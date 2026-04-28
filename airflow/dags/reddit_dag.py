"""Reddit mentions ingestion DAG.

For each tracked company we hit the Reddit public JSON search endpoint
(no auth required) across a curated list of fintech / startup subreddits,
score the post text with TextBlob, and load the result into
``raw.reddit_mentions`` on Supabase.

The DAG always succeeds: if Reddit is rate-limited or unreachable for a
given company we fall back to ``SAMPLE_DATA`` so the demo pipeline still
has rows to chew on.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

try:
    from textblob import TextBlob
except ImportError:  # pragma: no cover
    TextBlob = None  # type: ignore[assignment]

log = logging.getLogger(__name__)

TRACKED_COMPANIES: list[dict[str, str]] = [
    # Workplace SaaS
    {"name": "Slack",     "slug": "slack",     "query": "Slack"},
    {"name": "Notion",    "slug": "notion",    "query": "Notion"},
    {"name": "Figma",     "slug": "figma",     "query": "Figma"},
    {"name": "Asana",     "slug": "asana",     "query": "Asana"},
    {"name": "Airtable",  "slug": "airtable",  "query": "Airtable"},
    # Fintech / payments (Satispay peer group)
    {"name": "Satispay",  "slug": "satispay",  "query": "Satispay"},
    {"name": "Nexi",      "slug": "nexi",      "query": "Nexi payments"},
    {"name": "Revolut",   "slug": "revolut",   "query": "Revolut"},
    {"name": "Klarna",    "slug": "klarna",    "query": "Klarna"},
    {"name": "N26",       "slug": "n26",       "query": "N26 bank"},
    # Capital markets / treasury (Murex peer group)
    {"name": "Murex",     "slug": "murex",     "query": "Murex MX.3"},
    {"name": "Finastra",  "slug": "finastra",  "query": "Finastra"},
    {"name": "Calypso",   "slug": "calypso",   "query": "Calypso trading"},
    {"name": "FIS",       "slug": "fis",       "query": "FIS Quantum"},
    {"name": "Bloomberg", "slug": "bloomberg", "query": "Bloomberg Terminal"},
]

SUBREDDITS: list[str] = [
    "fintech",
    "eupersonalfinance",
    "personalfinance",
    "investing",
    "startups",
]

SEARCH_URL = "https://www.reddit.com/r/{subreddit}/search.json"
USER_AGENT = "SaaSBIBot/1.0 (+https://example.com/bot)"
LIMIT = 100
TIME_RANGE = "year"
PER_REQUEST_SLEEP = 1.0  # seconds, keeps us well under Reddit's anonymous limits

# Bundled fallback used when Reddit is unreachable. Three companies, three
# posts each, so the demo dashboard always has data to render.
SAMPLE_DATA: list[dict[str, Any]] = [
    # Satispay
    {"slug": "satispay", "post_id": "sample_satispay_1", "subreddit": "fintech",
     "title": "Satispay raises 60M to expand across Europe",
     "selftext": "The Italian payments app is rolling out in France and Germany. Fast and reliable, the UX is great.",
     "score": 184, "num_comments": 42,
     "post_date": "2026-03-12T10:14:00", "url": "https://reddit.com/r/fintech/sample_satispay_1"},
    {"slug": "satispay", "post_id": "sample_satispay_2", "subreddit": "eupersonalfinance",
     "title": "Switched from PayPal to Satispay, here is what I learned",
     "selftext": "Lower fees, instant transfers, intuitive interface. Customer support has been excellent so far.",
     "score": 96, "num_comments": 27,
     "post_date": "2026-02-28T08:02:00", "url": "https://reddit.com/r/eupersonalfinance/sample_satispay_2"},
    {"slug": "satispay", "post_id": "sample_satispay_3", "subreddit": "personalfinance",
     "title": "Anyone using Satispay for SMB payments?",
     "selftext": "Considering it for our shop in Milan. The reviews are mostly positive but I wanted real feedback.",
     "score": 31, "num_comments": 14,
     "post_date": "2026-04-05T17:45:00", "url": "https://reddit.com/r/personalfinance/sample_satispay_3"},
    # Revolut
    {"slug": "revolut", "post_id": "sample_revolut_1", "subreddit": "fintech",
     "title": "Revolut hits 50M users, profits up 60%",
     "selftext": "Brilliant FX rates and seamless multi-currency accounts make it the go-to neobank for travellers.",
     "score": 412, "num_comments": 118,
     "post_date": "2026-03-21T09:30:00", "url": "https://reddit.com/r/fintech/sample_revolut_1"},
    {"slug": "revolut", "post_id": "sample_revolut_2", "subreddit": "personalfinance",
     "title": "Revolut Premium vs Metal worth it?",
     "selftext": "Tried both for a year. Premium is enough for most, Metal pays off only if you travel heavily.",
     "score": 78, "num_comments": 64,
     "post_date": "2026-02-10T19:11:00", "url": "https://reddit.com/r/personalfinance/sample_revolut_2"},
    {"slug": "revolut", "post_id": "sample_revolut_3", "subreddit": "investing",
     "title": "Revolut crypto fees are honestly terrible",
     "selftext": "Spreads are wide and you cannot move coins out. Fine for a quick punt, awful as a real wallet.",
     "score": 142, "num_comments": 89,
     "post_date": "2026-04-02T22:00:00", "url": "https://reddit.com/r/investing/sample_revolut_3"},
    # Murex
    {"slug": "murex", "post_id": "sample_murex_1", "subreddit": "startups",
     "title": "Working on a Murex MX.3 integration as a vendor, AMA",
     "selftext": "Powerful platform once you understand the data model. Implementation timelines are long but the risk engine is excellent.",
     "score": 54, "num_comments": 31,
     "post_date": "2026-03-08T15:22:00", "url": "https://reddit.com/r/startups/sample_murex_1"},
    {"slug": "murex", "post_id": "sample_murex_2", "subreddit": "fintech",
     "title": "Murex vs Calypso for derivatives, what would you pick?",
     "selftext": "Coverage on FX, rates and credit is solid in both. Murex is reliable for capital markets, Calypso wins on workflow.",
     "score": 87, "num_comments": 56,
     "post_date": "2026-02-19T12:40:00", "url": "https://reddit.com/r/fintech/sample_murex_2"},
    {"slug": "murex", "post_id": "sample_murex_3", "subreddit": "investing",
     "title": "Murex consultants are paid like rockstars right now",
     "selftext": "Day rates above 1500 EUR for senior MX.3 specialists. The talent shortage is real.",
     "score": 39, "num_comments": 22,
     "post_date": "2026-04-12T07:55:00", "url": "https://reddit.com/r/investing/sample_murex_3"},
]


def _engine() -> Engine:
    """Build a SQLAlchemy engine from DATABASE_URL and verify the connection.

    Raises immediately with a clear error if the URL is missing or the
    connection check fails. Normalizes ``postgresql+asyncpg://`` URLs to
    the sync ``postgresql://`` form so the psycopg2 driver is used.
    """
    url = os.environ.get("DATABASE_URL") or os.environ.get("APP_POSTGRES_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL env var is not set. The DAG cannot run without "
            "a Supabase / Postgres connection string."
        )
    if url.startswith("postgresql+asyncpg://"):
        url = "postgresql://" + url[len("postgresql+asyncpg://"):]
    try:
        engine = create_engine(url, pool_pre_ping=True, future=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        raise RuntimeError(
            f"Failed to connect to the database with DATABASE_URL: {exc}"
        ) from exc
    log.info("Connected to database successfully")
    return engine


def _score_sentiment(text_: str) -> float:
    """Return a TextBlob polarity score in [-1.0, 1.0]. 0.0 if TextBlob unavailable."""
    if not text_ or TextBlob is None:
        return 0.0
    try:
        polarity = TextBlob(text_).sentiment.polarity
        return round(float(polarity), 4)
    except Exception:  # pragma: no cover
        return 0.0


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
def _fetch_subreddit_search(subreddit: str, query: str) -> list[dict[str, Any]]:
    """Hit the Reddit JSON search for one (subreddit, query) pair."""
    url = SEARCH_URL.format(subreddit=subreddit)
    params = {"q": query, "sort": "new", "limit": LIMIT, "t": TIME_RANGE,
              "restrict_sr": "on"}
    resp = requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=20)
    if resp.status_code in (403, 404, 429):
        log.warning("Reddit %s for r/%s q=%s, skipping", resp.status_code, subreddit, query)
        return []
    resp.raise_for_status()
    payload = resp.json()
    children = payload.get("data", {}).get("children", []) or []
    return [c.get("data", {}) for c in children if isinstance(c, dict)]


def _post_to_row(post: dict[str, Any], company: dict[str, str]) -> dict[str, Any] | None:
    """Map a Reddit search result into a ``raw.reddit_mentions`` row."""
    post_id = post.get("id") or post.get("name")
    if not post_id:
        return None
    title = (post.get("title") or "").strip() or None
    selftext = (post.get("selftext") or "").strip() or None
    created_utc = post.get("created_utc")
    try:
        post_date = (
            datetime.fromtimestamp(float(created_utc), tz=timezone.utc).replace(tzinfo=None)
            if created_utc else None
        )
    except Exception:
        post_date = None
    body = " ".join(filter(None, [title, selftext]))
    return {
        "company_slug":    company["slug"],
        "post_id":         f"reddit_{post_id}",
        "subreddit":       (post.get("subreddit") or "").lower() or None,
        "title":           title,
        "selftext":        selftext,
        "score":           int(post.get("score") or 0),
        "num_comments":    int(post.get("num_comments") or 0),
        "sentiment_score": _score_sentiment(body),
        "post_date":       post_date,
        "url":             "https://reddit.com" + post.get("permalink", "")
                            if post.get("permalink") else post.get("url"),
    }


def _bundled_rows_for(slug: str) -> list[dict[str, Any]]:
    """Return SAMPLE_DATA rows for one slug, shaped like _post_to_row output."""
    rows: list[dict[str, Any]] = []
    for sample in SAMPLE_DATA:
        if sample["slug"] != slug:
            continue
        body = " ".join(filter(None, [sample.get("title"), sample.get("selftext")]))
        try:
            post_date = datetime.fromisoformat(sample["post_date"])
        except Exception:
            post_date = None
        rows.append({
            "company_slug":    sample["slug"],
            "post_id":         sample["post_id"],
            "subreddit":       sample["subreddit"],
            "title":           sample.get("title"),
            "selftext":        sample.get("selftext"),
            "score":           int(sample.get("score") or 0),
            "num_comments":    int(sample.get("num_comments") or 0),
            "sentiment_score": _score_sentiment(body),
            "post_date":       post_date,
            "url":             sample.get("url"),
        })
    return rows


def _upsert(engine: Engine, rows: list[dict[str, Any]]) -> int:
    """Insert rows into ``raw.reddit_mentions``. Raises on database error."""
    if not rows:
        return 0
    sql = text("""
        INSERT INTO raw.reddit_mentions (
            company_slug, post_id, subreddit, title, selftext,
            score, num_comments, sentiment_score, post_date, url
        )
        VALUES (
            :company_slug, :post_id, :subreddit, :title, :selftext,
            :score, :num_comments, :sentiment_score, :post_date, :url
        )
        ON CONFLICT (post_id) DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    log.info("Inserted %d rows into raw.reddit_mentions", len(rows))
    return len(rows)


def ingest_reddit_mentions(**_: Any) -> None:
    """DAG entrypoint: loop tracked companies, query each subreddit, load rows.

    Reddit network failures fall back to ``SAMPLE_DATA`` and the bundled
    rows are written to the database with the same engine. Database errors
    propagate so Airflow marks the task failed.
    """
    engine = _engine()
    total = 0
    for company in TRACKED_COMPANIES:
        log.info("Fetching Reddit mentions for %s", company["slug"])
        company_rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        for subreddit in SUBREDDITS:
            try:
                posts = _fetch_subreddit_search(subreddit, company["query"])
            except requests.RequestException as exc:
                log.warning("Reddit r/%s fetch failed for %s: %s",
                            subreddit, company["slug"], exc)
                posts = []
            for post in posts:
                row = _post_to_row(post, company)
                if row and row["post_id"] not in seen:
                    seen.add(row["post_id"])
                    company_rows.append(row)
            time.sleep(PER_REQUEST_SLEEP)

        if not company_rows:
            log.warning("API failed for %s, using sample data", company["slug"])
            company_rows = _bundled_rows_for(company["slug"])

        if not company_rows:
            log.warning("No SAMPLE_DATA available for %s, skipping", company["slug"])
            continue

        log.info("Fetched %d rows for %s", len(company_rows), company["slug"])
        total += _upsert(engine, company_rows)

    log.info("Reddit ingestion complete, %d total rows attempted", total)


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="reddit_dag",
    description="Ingest Reddit mentions for tracked SaaS / fintech companies.",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["saas-bi", "ingest", "reddit"],
) as dag:
    PythonOperator(
        task_id="ingest_reddit_mentions",
        python_callable=ingest_reddit_mentions,
    )
