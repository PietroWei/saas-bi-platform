"""G2 reviews ingestion DAG.

Scrapes the public G2 product page for a set of tracked SaaS companies,
extracts reviews, runs a lightweight sentiment score, and loads the result
into ``raw.g2_reviews``.

G2 throttles aggressively; the DAG is tolerant of empty pages and HTTP 4xx -
in both cases it logs a warning and exits cleanly rather than failing the run.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

log = logging.getLogger(__name__)

TRACKED_COMPANIES: list[dict[str, str]] = [
    # --- Workplace SaaS (original cohort)
    {"name": "Slack",     "slug": "slack"},
    {"name": "Notion",    "slug": "notion"},
    {"name": "Figma",     "slug": "figma"},
    {"name": "Asana",     "slug": "asana"},
    {"name": "Airtable",  "slug": "airtable"},
    # --- Fintech / payments (Satispay peer group)
    {"name": "Satispay",  "slug": "satispay"},
    {"name": "Nexi",      "slug": "nexi"},
    {"name": "Revolut",   "slug": "revolut"},
    {"name": "Klarna",    "slug": "klarna"},
    {"name": "N26",       "slug": "n26"},
    # --- Capital markets / treasury software (Murex peer group)
    {"name": "Murex",     "slug": "murex"},
    {"name": "Finastra",  "slug": "finastra"},
    {"name": "Calypso",   "slug": "calypso"},
    {"name": "FIS",       "slug": "fis"},
    {"name": "Bloomberg", "slug": "bloomberg"},
]

G2_URL_TEMPLATE = "https://www.g2.com/products/{slug}/reviews"
USER_AGENT = (
    "Mozilla/5.0 (compatible; SaaSBIBot/1.0; +https://example.com/bot)"
)

# Bundled sample reviews used as fallback when G2 throttles or returns nothing.
# Demo-friendly: every tracked company has a handful of plausible reviews so
# the downstream sentiment / health-score pipeline always has data to chew on.
SAMPLE_REVIEWS: list[dict[str, Any]] = [
    # --- Slack
    {"slug": "slack", "name": "Slack", "title": "Best team chat we've used",
     "body": "Slack is fast, reliable and the integrations are seamless. Love the search.",
     "rating": 4.5, "role": "Engineering Manager", "date": "2025-09-12"},
    {"slug": "slack", "name": "Slack", "title": "Pricing is getting expensive",
     "body": "Great product but per-seat pricing feels expensive for large orgs.",
     "rating": 3.5, "role": "VP Operations", "date": "2025-07-04"},
    # --- Notion
    {"slug": "notion", "name": "Notion", "title": "Powerful but a learning curve",
     "body": "Once you grok databases it's amazing. Onboarding is confusing for new hires.",
     "rating": 4.2, "role": "Product Manager", "date": "2025-08-19"},
    {"slug": "notion", "name": "Notion", "title": "Replaces three other tools",
     "body": "We dropped Confluence and Asana. Intuitive and fantastic for docs.",
     "rating": 4.7, "role": "COO", "date": "2025-10-02"},
    # --- Figma
    {"slug": "figma", "name": "Figma", "title": "Industry standard for a reason",
     "body": "Brilliant collaborative design. Plugins ecosystem is excellent.",
     "rating": 4.8, "role": "Lead Designer", "date": "2025-09-30"},
    {"slug": "figma", "name": "Figma", "title": "Dev mode is a game changer",
     "body": "Hand-off to engineering is now seamless. Love the variables feature.",
     "rating": 4.6, "role": "Frontend Engineer", "date": "2025-11-10"},
    # --- Asana
    {"slug": "asana", "name": "Asana", "title": "Solid PM tool",
     "body": "Reliable for project tracking. UI can feel slow on large workspaces.",
     "rating": 4.0, "role": "Program Manager", "date": "2025-08-01"},
    {"slug": "asana", "name": "Asana", "title": "Reporting needs work",
     "body": "Good for tasks, weak for portfolio reporting. Bug with recurring tasks.",
     "rating": 3.4, "role": "PMO", "date": "2025-09-22"},
    # --- Airtable
    {"slug": "airtable", "name": "Airtable", "title": "Best no-code DB",
     "body": "Easy to build internal tools. Powerful automations.",
     "rating": 4.4, "role": "Operations Lead", "date": "2025-07-15"},
    {"slug": "airtable", "name": "Airtable", "title": "Pricing per row hurts",
     "body": "Great product but expensive once you scale past a few thousand records.",
     "rating": 3.6, "role": "Founder", "date": "2025-10-25"},
    # --- Satispay
    {"slug": "satispay", "name": "Satispay", "title": "Pagamenti istantanei perfetti",
     "body": "App fantastic, fast and reliable. Love the cashback. Best Italian payment app.",
     "rating": 4.7, "role": "Daily user", "date": "2025-11-12"},
    {"slug": "satispay", "name": "Satispay", "title": "Great UX for small merchants",
     "body": "Onboarding is seamless and the dashboard is intuitive. Customer support is excellent.",
     "rating": 4.5, "role": "Small business owner", "date": "2025-10-18"},
    {"slug": "satispay", "name": "Satispay", "title": "Occasional glitch on transfers",
     "body": "Mostly reliable but had a buggy transfer last month. Was resolved fast.",
     "rating": 4.0, "role": "Freelancer", "date": "2025-09-05"},
    {"slug": "satispay", "name": "Satispay", "title": "Best alternative to cards",
     "body": "Powerful peer-to-peer payments, no fees between friends. Amazing.",
     "rating": 4.8, "role": "Student", "date": "2025-12-01"},
    # --- Nexi
    {"slug": "nexi", "name": "Nexi", "title": "POS reliable but pricing opaque",
     "body": "Hardware works fine, fees are confusing and the merchant portal is slow.",
     "rating": 3.4, "role": "Retail manager", "date": "2025-08-22"},
    {"slug": "nexi", "name": "Nexi", "title": "Customer support is poor",
     "body": "Long waits, terrible escalation. The product itself is fine.",
     "rating": 2.8, "role": "Restaurant owner", "date": "2025-10-09"},
    {"slug": "nexi", "name": "Nexi", "title": "Wide acceptance, that's the value",
     "body": "Almost every merchant takes it in Italy. Reliable infrastructure.",
     "rating": 3.8, "role": "CFO", "date": "2025-11-04"},
    # --- Revolut
    {"slug": "revolut", "name": "Revolut", "title": "Multi-currency made easy",
     "body": "Brilliant FX, fast transfers. The app is intuitive and powerful.",
     "rating": 4.6, "role": "Frequent traveler", "date": "2025-11-15"},
    {"slug": "revolut", "name": "Revolut", "title": "Customer service is hit or miss",
     "body": "Fantastic product, but support is slow when accounts get frozen.",
     "rating": 3.7, "role": "Crypto trader", "date": "2025-09-28"},
    {"slug": "revolut", "name": "Revolut", "title": "Business plan is a steal",
     "body": "Cheap, fast, reliable for our SMB. Love the API.",
     "rating": 4.5, "role": "Startup founder", "date": "2025-10-30"},
    # --- Klarna
    {"slug": "klarna", "name": "Klarna", "title": "BNPL done right",
     "body": "Easy checkout, no hidden fees. Seamless on mobile.",
     "rating": 4.3, "role": "E-commerce shopper", "date": "2025-11-20"},
    {"slug": "klarna", "name": "Klarna", "title": "Late fees can stack up",
     "body": "Great when you pay on time, expensive otherwise. Be careful.",
     "rating": 3.2, "role": "Consumer", "date": "2025-08-14"},
    # --- N26
    {"slug": "n26", "name": "N26", "title": "Clean banking app",
     "body": "Fast onboarding, intuitive UI. Reliable for daily use.",
     "rating": 4.2, "role": "Freelancer", "date": "2025-10-05"},
    {"slug": "n26", "name": "N26", "title": "Account freezes are scary",
     "body": "Had my account frozen for compliance - terrible experience, slow resolution.",
     "rating": 2.5, "role": "Consultant", "date": "2025-07-19"},
    # --- Murex
    {"slug": "murex", "name": "Murex", "title": "Industry leader in trading risk",
     "body": "Powerful platform for FO/MO/BO. Steep learning curve but reliable for capital markets.",
     "rating": 4.4, "role": "Quant developer", "date": "2025-11-08"},
    {"slug": "murex", "name": "Murex", "title": "Implementation is brutal",
     "body": "Best-in-class functionality but the rollout took two years. Expensive.",
     "rating": 3.6, "role": "Head of Treasury IT", "date": "2025-09-14"},
    {"slug": "murex", "name": "Murex", "title": "MX.3 is comprehensive",
     "body": "Covers FX, rates, credit and commodities. Excellent risk engine.",
     "rating": 4.5, "role": "Risk manager", "date": "2025-10-26"},
    {"slug": "murex", "name": "Murex", "title": "Customization is powerful but slow",
     "body": "Anything is doable but timelines are long. The vendor team is brilliant.",
     "rating": 4.0, "role": "Project manager bank IT", "date": "2025-08-30"},
    # --- Finastra
    {"slug": "finastra", "name": "Finastra", "title": "Solid for lending",
     "body": "Reliable platform for syndicated loans. UI feels dated.",
     "rating": 3.7, "role": "Loan ops", "date": "2025-09-02"},
    {"slug": "finastra", "name": "Finastra", "title": "Integration headaches",
     "body": "Modules don't talk to each other natively, integration is expensive.",
     "rating": 3.0, "role": "Bank architect", "date": "2025-10-12"},
    # --- Calypso
    {"slug": "calypso", "name": "Calypso", "title": "Great cross-asset coverage",
     "body": "Strong on derivatives. Powerful workflow engine.",
     "rating": 4.1, "role": "Derivatives trader", "date": "2025-08-25"},
    {"slug": "calypso", "name": "Calypso", "title": "Upgrades are a nightmare",
     "body": "Functionally rich but version migrations are slow and buggy.",
     "rating": 3.3, "role": "IT lead capital markets", "date": "2025-10-17"},
    # --- FIS
    {"slug": "fis", "name": "FIS", "title": "Broad suite, mixed quality",
     "body": "Some products are excellent (Quantum), others feel legacy.",
     "rating": 3.5, "role": "Treasury director", "date": "2025-09-19"},
    {"slug": "fis", "name": "FIS", "title": "Reliable for core banking",
     "body": "Battle-tested platform. Support is responsive.",
     "rating": 3.9, "role": "Bank CIO", "date": "2025-11-01"},
    # --- Bloomberg
    {"slug": "bloomberg", "name": "Bloomberg", "title": "Terminal is the gold standard",
     "body": "Fast data, brilliant analytics, reliable. Expensive but worth it for FO.",
     "rating": 4.8, "role": "Portfolio manager", "date": "2025-11-22"},
    {"slug": "bloomberg", "name": "Bloomberg", "title": "AIM is powerful",
     "body": "Great OMS for buy-side. Intuitive once you learn the keystrokes.",
     "rating": 4.4, "role": "Buy-side trader", "date": "2025-10-08"},
]

# Tiny hand-crafted sentiment lexicon. The real pipeline would swap this for a
# transformer-based scorer; we stay dependency-light so the DAG runs offline.
POSITIVE_WORDS = {
    "great", "excellent", "love", "amazing", "intuitive", "fast", "reliable",
    "powerful", "easy", "best", "fantastic", "brilliant", "seamless",
}
NEGATIVE_WORDS = {
    "slow", "bug", "buggy", "crash", "broken", "expensive", "terrible",
    "confusing", "hate", "worst", "poor", "glitch", "unreliable",
}


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


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
def _fetch_page(url: str) -> str | None:
    """Fetch a G2 listing page. Returns HTML or ``None`` if blocked/empty."""
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
    if resp.status_code in (403, 404, 429):
        log.warning("G2 returned %s for %s - skipping", resp.status_code, url)
        return None
    resp.raise_for_status()
    return resp.text


def _parse_reviews(html: str, company: dict[str, str]) -> list[dict[str, Any]]:
    """Extract a list of review dicts from a G2 HTML page.

    G2's DOM changes often; the parser is intentionally tolerant -
    missing fields become None rather than raising.
    """
    soup = BeautifulSoup(html, "lxml")
    out: list[dict[str, Any]] = []
    cards = soup.select("div[itemprop='review'], article.paper--white")

    for idx, card in enumerate(cards):
        title_el = card.select_one("[itemprop='name'], h3")
        body_el = card.select_one("[itemprop='reviewBody'], div.pre-wrap")
        rating_el = card.select_one("[itemprop='ratingValue']")
        date_el = card.select_one("[itemprop='datePublished'], time")
        role_el = card.select_one(".c-midnight-70")

        body_text = body_el.get_text(" ", strip=True) if body_el else ""
        try:
            rating = float(rating_el.get_text(strip=True)) if rating_el else None
        except ValueError:
            rating = None

        review_date = None
        if date_el is not None:
            raw_date = date_el.get("datetime") or date_el.get_text(strip=True)
            try:
                review_date = pd.to_datetime(raw_date, errors="coerce").date()
            except Exception:  # pragma: no cover
                review_date = None

        out.append({
            "company_name":    company["name"],
            "company_slug":    company["slug"],
            "review_id":       f"{company['slug']}-{idx}-{(review_date or datetime.utcnow().date()).isoformat()}",
            "review_title":    title_el.get_text(strip=True) if title_el else None,
            "review_body":     body_text or None,
            "rating":          rating,
            "reviewer_role":   role_el.get_text(strip=True) if role_el else None,
            "reviewer_size":   None,
            "review_date":     review_date,
            "sentiment_score": _score_sentiment(body_text),
            "source_url":      G2_URL_TEMPLATE.format(slug=company["slug"]),
        })

    return out


def _upsert(engine: Engine, rows: list[dict[str, Any]]) -> int:
    """Insert rows into ``raw.g2_reviews`` with ON CONFLICT DO NOTHING."""
    if not rows:
        return 0
    sql = text("""
        INSERT INTO raw.g2_reviews (
            company_name, company_slug, review_id, review_title, review_body,
            rating, reviewer_role, reviewer_size, review_date,
            sentiment_score, source_url
        )
        VALUES (
            :company_name, :company_slug, :review_id, :review_title, :review_body,
            :rating, :reviewer_role, :reviewer_size, :review_date,
            :sentiment_score, :source_url
        )
        ON CONFLICT (company_slug, review_id) DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def _bundled_rows_for(slug: str) -> list[dict[str, Any]]:
    """Return bundled-sample rows for one company, shaped like _parse_reviews output."""
    rows: list[dict[str, Any]] = []
    for idx, sample in enumerate(s for s in SAMPLE_REVIEWS if s["slug"] == slug):
        body = sample["body"]
        review_date = pd.to_datetime(sample["date"], errors="coerce").date()
        rows.append({
            "company_name":    sample["name"],
            "company_slug":    sample["slug"],
            "review_id":       f"{slug}-bundled-{idx}",
            "review_title":    sample["title"],
            "review_body":     body,
            "rating":          sample["rating"],
            "reviewer_role":   sample["role"],
            "reviewer_size":   None,
            "review_date":     review_date,
            "sentiment_score": _score_sentiment(body),
            "source_url":      "bundled-sample",
        })
    return rows


def ingest_g2_reviews(**_: Any) -> None:
    """DAG task entrypoint - loops tracked companies and loads reviews.

    For each company we try a live G2 scrape first; if that returns nothing
    (rate limit, layout change, ...), we fall back to ``SAMPLE_REVIEWS`` so
    the demo dashboard always has data.
    """
    engine = _engine()
    total = 0
    for company in TRACKED_COMPANIES:
        url = G2_URL_TEMPLATE.format(slug=company["slug"])
        log.info("Fetching %s", url)
        rows: list[dict[str, Any]] = []
        try:
            html = _fetch_page(url)
            if html:
                rows = _parse_reviews(html, company)
        except Exception as exc:
            log.exception("Unrecoverable fetch error for %s: %s", company["slug"], exc)

        if not rows:
            rows = _bundled_rows_for(company["slug"])
            if rows:
                log.info("Using %d bundled reviews for %s", len(rows), company["slug"])
        else:
            log.info("Parsed %d live reviews for %s", len(rows), company["slug"])

        total += _upsert(engine, rows)

    log.info("G2 ingestion complete - %d rows attempted", total)


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="g2_reviews_dag",
    description="Scrape G2 reviews for tracked SaaS companies.",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ingest", "reviews", "g2"],
) as dag:
    PythonOperator(
        task_id="ingest_g2_reviews",
        python_callable=ingest_g2_reviews,
    )
