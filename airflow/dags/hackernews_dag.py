"""HackerNews mentions ingestion DAG.

Queries the Algolia HN Search API (https://hn.algolia.com/api) for each
tracked company, captures the most relevant stories and comments, runs a
lightweight lexicon-based sentiment score, and loads the result into
``raw.hn_mentions``.

Algolia HN Search is free, requires no auth, and returns clean JSON.
A bundled-sample fallback is used when the API is unreachable so the
downstream demo dashboard always has data to render.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

log = logging.getLogger(__name__)

TRACKED_COMPANIES: list[dict[str, str]] = [
    # --- Workplace SaaS
    {"name": "Slack",     "slug": "slack",     "query": "Slack"},
    {"name": "Notion",    "slug": "notion",    "query": "Notion"},
    {"name": "Figma",     "slug": "figma",     "query": "Figma"},
    {"name": "Asana",     "slug": "asana",     "query": "Asana"},
    {"name": "Airtable",  "slug": "airtable",  "query": "Airtable"},
    # --- Fintech / payments (Satispay peer group)
    {"name": "Satispay",  "slug": "satispay",  "query": "Satispay"},
    {"name": "Nexi",      "slug": "nexi",      "query": "Nexi payments"},
    {"name": "Revolut",   "slug": "revolut",   "query": "Revolut"},
    {"name": "Klarna",    "slug": "klarna",    "query": "Klarna"},
    {"name": "N26",       "slug": "n26",       "query": "N26 bank"},
    # --- Capital markets / treasury (Murex peer group)
    {"name": "Murex",     "slug": "murex",     "query": "Murex MX.3"},
    {"name": "Finastra",  "slug": "finastra",  "query": "Finastra"},
    {"name": "Calypso",   "slug": "calypso",   "query": "Calypso trading"},
    {"name": "FIS",       "slug": "fis",       "query": "FIS Quantum"},
    {"name": "Bloomberg", "slug": "bloomberg", "query": "Bloomberg Terminal"},
]

ALGOLIA_URL = "https://hn.algolia.com/api/v1/search_by_date"
HN_ITEM_URL = "https://news.ycombinator.com/item?id={}"
USER_AGENT = "SaaSBIBot/1.0 (+https://example.com/bot)"

# Tags to scope each call. Stories give us titles and points; comments give us
# qualitative chatter where sentiment lives. We pull both.
HN_TAGS = "(story,comment)"
HITS_PER_PAGE = 50

# Window of HN content to consider, expressed in days. Algolia supports
# `numericFilters=created_at_i>...` for unix-timestamp filtering.
LOOKBACK_DAYS = 365 * 2

POSITIVE_WORDS = {
    "great", "excellent", "love", "amazing", "intuitive", "fast", "reliable",
    "powerful", "easy", "best", "fantastic", "brilliant", "seamless", "solid",
}
NEGATIVE_WORDS = {
    "slow", "bug", "buggy", "crash", "broken", "expensive", "terrible",
    "confusing", "hate", "worst", "poor", "glitch", "unreliable", "awful",
}

# Bundled fallback. Realistic HN-style mentions per company so the demo
# dashboard always has data even when the Algolia call fails.
SAMPLE_MENTIONS: list[dict[str, Any]] = [
    # --- Slack
    {"slug": "slack", "name": "Slack", "title": "Show HN: We replaced email with Slack",
     "body": "Slack is fast and reliable; the integrations are seamless. Search is great.",
     "points": 142, "author": "founder42", "date": "2025-09-12", "type": "story"},
    {"slug": "slack", "name": "Slack", "title": None,
     "body": "Per-seat pricing is getting expensive for large orgs. Otherwise solid.",
     "points": 18, "author": "vp_ops", "date": "2025-07-04", "type": "comment"},
    # --- Notion
    {"slug": "notion", "name": "Notion", "title": "Notion as a developer wiki",
     "body": "Powerful once you grok databases. Onboarding is confusing for new hires.",
     "points": 96, "author": "pm_jane", "date": "2025-08-19", "type": "story"},
    {"slug": "notion", "name": "Notion", "title": None,
     "body": "We dropped Confluence and Asana. Intuitive and fantastic for docs.",
     "points": 24, "author": "coo_org", "date": "2025-10-02", "type": "comment"},
    # --- Figma
    {"slug": "figma", "name": "Figma", "title": "Figma Dev Mode is a game changer",
     "body": "Brilliant collaborative design. The plugins ecosystem is excellent.",
     "points": 211, "author": "lead_design", "date": "2025-09-30", "type": "story"},
    {"slug": "figma", "name": "Figma", "title": None,
     "body": "Hand-off to engineering is now seamless. Love the variables feature.",
     "points": 31, "author": "fe_eng", "date": "2025-11-10", "type": "comment"},
    # --- Asana
    {"slug": "asana", "name": "Asana", "title": "Asana for cross-functional PM",
     "body": "Reliable for project tracking. UI can feel slow on large workspaces.",
     "points": 38, "author": "program_mgr", "date": "2025-08-01", "type": "story"},
    {"slug": "asana", "name": "Asana", "title": None,
     "body": "Reporting is weak for portfolio. Bug with recurring tasks.",
     "points": 9, "author": "pmo_lead", "date": "2025-09-22", "type": "comment"},
    # --- Airtable
    {"slug": "airtable", "name": "Airtable", "title": "Airtable as a no-code DB",
     "body": "Easy to build internal tools. Powerful automations.",
     "points": 73, "author": "ops_lead", "date": "2025-07-15", "type": "story"},
    {"slug": "airtable", "name": "Airtable", "title": None,
     "body": "Pricing per row hurts once you scale past a few thousand records. Expensive.",
     "points": 12, "author": "founder_x", "date": "2025-10-25", "type": "comment"},
    # --- Satispay
    {"slug": "satispay", "name": "Satispay", "title": "Satispay raises Series D",
     "body": "Italian payments app, fast and reliable. Best alternative to cards.",
     "points": 188, "author": "fintech_eu", "date": "2025-11-12", "type": "story"},
    {"slug": "satispay", "name": "Satispay", "title": None,
     "body": "Onboarding is seamless and the dashboard is intuitive. Customer support is excellent.",
     "points": 27, "author": "smb_owner", "date": "2025-10-18", "type": "comment"},
    {"slug": "satispay", "name": "Satispay", "title": None,
     "body": "Mostly reliable but had a buggy transfer last month. Was resolved fast.",
     "points": 6, "author": "freelance_it", "date": "2025-09-05", "type": "comment"},
    {"slug": "satispay", "name": "Satispay", "title": "Why Satispay won Italy",
     "body": "Powerful peer-to-peer payments, no fees between friends. Amazing UX.",
     "points": 134, "author": "student_mi", "date": "2025-12-01", "type": "story"},
    # --- Nexi
    {"slug": "nexi", "name": "Nexi", "title": "Nexi POS pricing is opaque",
     "body": "Hardware works fine, fees are confusing and the merchant portal is slow.",
     "points": 41, "author": "retail_mgr", "date": "2025-08-22", "type": "story"},
    {"slug": "nexi", "name": "Nexi", "title": None,
     "body": "Customer support is poor. Long waits, terrible escalation.",
     "points": 14, "author": "rest_owner", "date": "2025-10-09", "type": "comment"},
    {"slug": "nexi", "name": "Nexi", "title": None,
     "body": "Wide acceptance is the value. Reliable infrastructure across Italy.",
     "points": 22, "author": "cfo_smb", "date": "2025-11-04", "type": "comment"},
    # --- Revolut
    {"slug": "revolut", "name": "Revolut", "title": "Revolut multi-currency for travel",
     "body": "Brilliant FX, fast transfers. The app is intuitive and powerful.",
     "points": 156, "author": "traveler_eu", "date": "2025-11-15", "type": "story"},
    {"slug": "revolut", "name": "Revolut", "title": None,
     "body": "Customer service is slow when accounts get frozen. Otherwise fantastic.",
     "points": 18, "author": "crypto_user", "date": "2025-09-28", "type": "comment"},
    {"slug": "revolut", "name": "Revolut", "title": "Revolut Business: cheap and fast",
     "body": "Cheap, fast, reliable for our SMB. Love the API.",
     "points": 92, "author": "founder_uk", "date": "2025-10-30", "type": "story"},
    # --- Klarna
    {"slug": "klarna", "name": "Klarna", "title": "Klarna BNPL economics",
     "body": "Easy checkout, no hidden fees. Seamless on mobile.",
     "points": 87, "author": "ecom_an", "date": "2025-11-20", "type": "story"},
    {"slug": "klarna", "name": "Klarna", "title": None,
     "body": "Late fees can stack up. Expensive if you miss a payment.",
     "points": 15, "author": "consumer", "date": "2025-08-14", "type": "comment"},
    # --- N26
    {"slug": "n26", "name": "N26", "title": "N26 onboarding UX",
     "body": "Fast onboarding, intuitive UI. Reliable for daily use.",
     "points": 64, "author": "freelance_de", "date": "2025-10-05", "type": "story"},
    {"slug": "n26", "name": "N26", "title": None,
     "body": "Account freezes are scary - had mine frozen for compliance, terrible experience.",
     "points": 11, "author": "consultant", "date": "2025-07-19", "type": "comment"},
    # --- Murex
    {"slug": "murex", "name": "Murex", "title": "Murex MX.3 deep dive",
     "body": "Powerful platform for FO/MO/BO. Steep learning curve but reliable for capital markets.",
     "points": 108, "author": "quant_dev", "date": "2025-11-08", "type": "story"},
    {"slug": "murex", "name": "Murex", "title": None,
     "body": "Implementation is brutal. Best-in-class functionality but the rollout took two years. Expensive.",
     "points": 17, "author": "treasury_it", "date": "2025-09-14", "type": "comment"},
    {"slug": "murex", "name": "Murex", "title": None,
     "body": "MX.3 covers FX, rates, credit and commodities. Excellent risk engine.",
     "points": 21, "author": "risk_mgr", "date": "2025-10-26", "type": "comment"},
    {"slug": "murex", "name": "Murex", "title": None,
     "body": "Customization is powerful but slow. Anything is doable but timelines are long.",
     "points": 13, "author": "pm_bank", "date": "2025-08-30", "type": "comment"},
    # --- Finastra
    {"slug": "finastra", "name": "Finastra", "title": "Finastra for syndicated lending",
     "body": "Solid for lending. Reliable platform for syndicated loans. UI feels dated.",
     "points": 36, "author": "loan_ops", "date": "2025-09-02", "type": "story"},
    {"slug": "finastra", "name": "Finastra", "title": None,
     "body": "Modules don't talk to each other natively, integration is expensive.",
     "points": 8, "author": "bank_arch", "date": "2025-10-12", "type": "comment"},
    # --- Calypso
    {"slug": "calypso", "name": "Calypso", "title": "Calypso vs Murex on derivatives",
     "body": "Strong on derivatives. Powerful workflow engine.",
     "points": 52, "author": "deriv_trader", "date": "2025-08-25", "type": "story"},
    {"slug": "calypso", "name": "Calypso", "title": None,
     "body": "Functionally rich but version migrations are slow and buggy. Upgrades are a nightmare.",
     "points": 10, "author": "it_capmkts", "date": "2025-10-17", "type": "comment"},
    # --- FIS
    {"slug": "fis", "name": "FIS", "title": "FIS Quantum for treasury",
     "body": "Some products are excellent (Quantum), others feel legacy.",
     "points": 44, "author": "treasury_dir", "date": "2025-09-19", "type": "story"},
    {"slug": "fis", "name": "FIS", "title": None,
     "body": "Reliable for core banking. Battle-tested platform. Support is responsive.",
     "points": 19, "author": "bank_cio", "date": "2025-11-01", "type": "comment"},
    # --- Bloomberg
    {"slug": "bloomberg", "name": "Bloomberg", "title": "The Bloomberg Terminal is still the gold standard",
     "body": "Fast data, brilliant analytics, reliable. Expensive but worth it for FO.",
     "points": 287, "author": "pm_buyside", "date": "2025-11-22", "type": "story"},
    {"slug": "bloomberg", "name": "Bloomberg", "title": None,
     "body": "AIM is a powerful OMS for buy-side. Intuitive once you learn the keystrokes.",
     "points": 33, "author": "trader_buy", "date": "2025-10-08", "type": "comment"},
]


def _engine() -> Engine:
    """Build a SQLAlchemy engine from the APP_POSTGRES_URL env var."""
    url = os.environ.get("APP_POSTGRES_URL")
    if not url:
        raise RuntimeError("APP_POSTGRES_URL env var is not set")
    return create_engine(url, pool_pre_ping=True, future=True)


def _score_sentiment(text_: str) -> float:
    """Return a sentiment score in [-1.0, 1.0] using a small lexicon."""
    if not text_:
        return 0.0
    tokens = re.findall(r"[a-zA-Z']+", text_.lower())
    if not tokens:
        return 0.0
    pos = sum(1 for t in tokens if t in POSITIVE_WORDS)
    neg = sum(1 for t in tokens if t in NEGATIVE_WORDS)
    if pos + neg == 0:
        return 0.0
    return round((pos - neg) / (pos + neg), 4)


def _strip_html(raw: str | None) -> str:
    """HN comments come back with light HTML (<p>, <a>, <i>). Strip naively."""
    if not raw:
        return ""
    return re.sub(r"<[^>]+>", " ", raw).replace("&#x27;", "'").replace("&quot;", '"').strip()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
def _fetch_algolia(company: dict[str, str]) -> list[dict[str, Any]] | None:
    """Hit Algolia HN search for one company. Returns hits or None on hard error."""
    cutoff = int((datetime.now(tz=timezone.utc) - timedelta(days=LOOKBACK_DAYS)).timestamp())
    params = {
        "query": company["query"],
        "tags": HN_TAGS,
        "hitsPerPage": HITS_PER_PAGE,
        "numericFilters": f"created_at_i>{cutoff}",
    }
    resp = requests.get(
        ALGOLIA_URL, params=params, headers={"User-Agent": USER_AGENT}, timeout=20
    )
    if resp.status_code in (403, 404, 429):
        log.warning("Algolia returned %s for %s - skipping", resp.status_code, company["slug"])
        return None
    resp.raise_for_status()
    data = resp.json()
    return data.get("hits", []) or []


def _hits_to_rows(hits: list[dict[str, Any]], company: dict[str, str]) -> list[dict[str, Any]]:
    """Map Algolia hits to ``raw.hn_mentions`` rows."""
    rows: list[dict[str, Any]] = []
    for h in hits:
        object_id = str(h.get("objectID") or "")
        if not object_id:
            continue
        is_story = bool(h.get("title"))
        title = h.get("title") or h.get("story_title")
        body = h.get("story_text") if is_story else h.get("comment_text")
        body = _strip_html(body)
        # Comments often carry no title - synthesise from parent story if present.
        if not title:
            title = h.get("story_title")
        created_at = h.get("created_at")  # ISO 8601
        try:
            mention_date = datetime.fromisoformat(created_at.replace("Z", "+00:00")).date() if created_at else None
        except Exception:  # pragma: no cover
            mention_date = None
        rows.append({
            "company_name":    company["name"],
            "company_slug":    company["slug"],
            "mention_id":      object_id,
            "mention_type":    "story" if is_story else "comment",
            "title":           title,
            "body":            body or None,
            "points":          h.get("points") or 0,
            "author":          h.get("author"),
            "mention_date":    mention_date,
            "sentiment_score": _score_sentiment(body or title or ""),
            "source_url":      HN_ITEM_URL.format(object_id),
        })
    return rows


def _bundled_rows_for(slug: str) -> list[dict[str, Any]]:
    """Return bundled-sample rows for one company, shaped like _hits_to_rows output."""
    rows: list[dict[str, Any]] = []
    for idx, sample in enumerate(s for s in SAMPLE_MENTIONS if s["slug"] == slug):
        body = sample["body"]
        try:
            mention_date = datetime.strptime(sample["date"], "%Y-%m-%d").date()
        except Exception:
            mention_date = None
        rows.append({
            "company_name":    sample["name"],
            "company_slug":    sample["slug"],
            "mention_id":      f"{slug}-bundled-{idx}",
            "mention_type":    sample["type"],
            "title":           sample["title"],
            "body":            body,
            "points":          sample["points"],
            "author":          sample["author"],
            "mention_date":    mention_date,
            "sentiment_score": _score_sentiment(body),
            "source_url":      "bundled-sample",
        })
    return rows


def _upsert(engine: Engine, rows: list[dict[str, Any]]) -> int:
    """Insert rows into ``raw.hn_mentions`` with ON CONFLICT DO NOTHING."""
    if not rows:
        return 0
    sql = text("""
        INSERT INTO raw.hn_mentions (
            company_name, company_slug, mention_id, mention_type,
            title, body, points, author, mention_date,
            sentiment_score, source_url
        )
        VALUES (
            :company_name, :company_slug, :mention_id, :mention_type,
            :title, :body, :points, :author, :mention_date,
            :sentiment_score, :source_url
        )
        ON CONFLICT (company_slug, mention_id) DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def ingest_hn_mentions(**_: Any) -> None:
    """DAG entrypoint - loops tracked companies and loads HN mentions.

    For each company we hit Algolia first; if that returns nothing
    (rate limit, transient outage), we fall back to ``SAMPLE_MENTIONS``
    so the demo dashboard always has data.
    """
    engine = _engine()
    total = 0
    for company in TRACKED_COMPANIES:
        log.info("Querying HN/Algolia for %s", company["slug"])
        rows: list[dict[str, Any]] = []
        try:
            hits = _fetch_algolia(company)
            if hits:
                rows = _hits_to_rows(hits, company)
        except Exception as exc:
            log.exception("Unrecoverable Algolia error for %s: %s", company["slug"], exc)

        if not rows:
            rows = _bundled_rows_for(company["slug"])
            if rows:
                log.info("Using %d bundled mentions for %s", len(rows), company["slug"])
        else:
            log.info("Fetched %d live mentions for %s", len(rows), company["slug"])

        total += _upsert(engine, rows)

    log.info("HN ingestion complete - %d rows attempted", total)


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="hackernews_dag",
    description="Ingest HackerNews mentions (Algolia API) for tracked SaaS companies.",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ingest", "mentions", "hackernews"],
) as dag:
    PythonOperator(
        task_id="ingest_hn_mentions",
        python_callable=ingest_hn_mentions,
    )
