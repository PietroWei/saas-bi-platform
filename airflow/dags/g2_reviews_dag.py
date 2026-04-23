"""G2 reviews ingestion DAG.

Scrapes the public G2 product page for a set of tracked SaaS companies,
extracts reviews, runs a lightweight sentiment score, and loads the result
into ``raw.g2_reviews``.

G2 throttles aggressively; the DAG is tolerant of empty pages and HTTP 4xx —
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
    {"name": "Slack",     "slug": "slack"},
    {"name": "Notion",    "slug": "notion"},
    {"name": "Figma",     "slug": "figma"},
    {"name": "Asana",     "slug": "asana"},
    {"name": "Airtable",  "slug": "airtable"},
]

G2_URL_TEMPLATE = "https://www.g2.com/products/{slug}/reviews"
USER_AGENT = (
    "Mozilla/5.0 (compatible; SaaSBIBot/1.0; +https://example.com/bot)"
)

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
        log.warning("G2 returned %s for %s — skipping", resp.status_code, url)
        return None
    resp.raise_for_status()
    return resp.text


def _parse_reviews(html: str, company: dict[str, str]) -> list[dict[str, Any]]:
    """Extract a list of review dicts from a G2 HTML page.

    G2's DOM changes often; the parser is intentionally tolerant —
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


def ingest_g2_reviews(**_: Any) -> None:
    """DAG task entrypoint — loops tracked companies and loads reviews."""
    engine = _engine()
    total = 0
    for company in TRACKED_COMPANIES:
        url = G2_URL_TEMPLATE.format(slug=company["slug"])
        log.info("Fetching %s", url)
        try:
            html = _fetch_page(url)
        except Exception as exc:
            log.exception("Unrecoverable fetch error for %s: %s", company["slug"], exc)
            continue
        if not html:
            continue

        rows = _parse_reviews(html, company)
        log.info("Parsed %d reviews for %s", len(rows), company["slug"])
        total += _upsert(engine, rows)

    log.info("G2 ingestion complete — %d rows attempted", total)


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
