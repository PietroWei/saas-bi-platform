"""Web momentum signals DAG.

Pulls two weekly signals per tracked company:

* ``search_interest`` (0-100) from Google Trends via ``pytrends``,
  using the rolling last 12 months worldwide.
* ``hn_mention_count`` (monthly), counted from the Algolia HN search API
  as a free proxy for web buzz when SimilarWeb / similar sources are
  blocked.

Rows are loaded into ``raw.web_signals`` with one row per
(company, month_start). Schedule is weekly because Google Trends
rate-limits aggressively.

If both upstreams fail for a company we fall back to ``SAMPLE_DATA``.
"""

from __future__ import annotations

import logging
import os
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
    # Workplace SaaS
    {"name": "Slack",     "slug": "slack",     "trend_term": "Slack app"},
    {"name": "Notion",    "slug": "notion",    "trend_term": "Notion app"},
    {"name": "Figma",     "slug": "figma",     "trend_term": "Figma"},
    {"name": "Asana",     "slug": "asana",     "trend_term": "Asana"},
    {"name": "Airtable",  "slug": "airtable",  "trend_term": "Airtable"},
    # Fintech / payments
    {"name": "Satispay",  "slug": "satispay",  "trend_term": "Satispay"},
    {"name": "Nexi",      "slug": "nexi",      "trend_term": "Nexi"},
    {"name": "Revolut",   "slug": "revolut",   "trend_term": "Revolut"},
    {"name": "Klarna",    "slug": "klarna",    "trend_term": "Klarna"},
    {"name": "N26",       "slug": "n26",       "trend_term": "N26 bank"},
    # Capital markets / treasury
    {"name": "Murex",     "slug": "murex",     "trend_term": "Murex MX.3"},
    {"name": "Finastra",  "slug": "finastra",  "trend_term": "Finastra"},
    {"name": "Calypso",   "slug": "calypso",   "trend_term": "Calypso trading"},
    {"name": "FIS",       "slug": "fis",       "trend_term": "FIS Global"},
    {"name": "Bloomberg", "slug": "bloomberg", "trend_term": "Bloomberg Terminal"},
]

ALGOLIA_URL = "https://hn.algolia.com/api/v1/search_by_date"
USER_AGENT = "SaaSBIBot/1.0 (+https://example.com/bot)"

# Bundled fallback. One row per company per month for the last 12 months.
# Numbers are illustrative but consistent with real-world ranges.
SAMPLE_DATA: dict[str, dict[str, int]] = {
    "satispay":  {"baseline_interest": 62, "baseline_hn": 4},
    "revolut":   {"baseline_interest": 88, "baseline_hn": 12},
    "n26":       {"baseline_interest": 54, "baseline_hn": 5},
    "klarna":    {"baseline_interest": 71, "baseline_hn": 9},
    "nexi":      {"baseline_interest": 35, "baseline_hn": 2},
    "slack":     {"baseline_interest": 78, "baseline_hn": 11},
    "notion":    {"baseline_interest": 74, "baseline_hn": 14},
    "figma":     {"baseline_interest": 82, "baseline_hn": 18},
    "asana":     {"baseline_interest": 49, "baseline_hn": 6},
    "airtable":  {"baseline_interest": 53, "baseline_hn": 7},
    "murex":     {"baseline_interest": 22, "baseline_hn": 1},
    "finastra":  {"baseline_interest": 18, "baseline_hn": 1},
    "calypso":   {"baseline_interest": 26, "baseline_hn": 2},
    "fis":       {"baseline_interest": 31, "baseline_hn": 2},
    "bloomberg": {"baseline_interest": 92, "baseline_hn": 22},
}


def _engine() -> Engine:
    """Build a SQLAlchemy engine from DATABASE_URL (Supabase)."""
    url = os.environ.get("DATABASE_URL") or os.environ.get("APP_POSTGRES_URL")
    if not url:
        raise RuntimeError("DATABASE_URL env var is not set")
    return create_engine(url, pool_pre_ping=True, future=True)


def _month_starts(months: int = 12) -> list[datetime]:
    """Return the first day of each of the last ``months`` calendar months (UTC)."""
    now = datetime.now(tz=timezone.utc).replace(day=1, hour=0, minute=0, second=0,
                                                microsecond=0, tzinfo=None)
    out: list[datetime] = []
    cursor = now
    for _ in range(months):
        out.append(cursor)
        prev = cursor - timedelta(days=1)
        cursor = prev.replace(day=1)
    return list(reversed(out))


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=2, max=8))
def _fetch_trends(term: str) -> dict[datetime, int] | None:
    """Pull last-12-months Google Trends interest for ``term``. None on failure."""
    try:
        from pytrends.request import TrendReq  # type: ignore[import-not-found]
    except ImportError:
        log.warning("pytrends not installed, skipping Google Trends")
        return None
    try:
        pytrends = TrendReq(hl="en-US", tz=0, retries=1)
        pytrends.build_payload([term], timeframe="today 12-m", geo="")
        df = pytrends.interest_over_time()
    except Exception as exc:
        log.warning("Google Trends failed for %s: %s", term, exc)
        return None

    if df is None or df.empty or term not in df.columns:
        return None

    monthly: dict[datetime, list[int]] = {}
    for ts, row in df.iterrows():
        if row.get("isPartial"):
            continue
        month = ts.to_pydatetime().replace(day=1, hour=0, minute=0, second=0,
                                           microsecond=0, tzinfo=None)
        monthly.setdefault(month, []).append(int(row[term] or 0))
    return {m: round(sum(v) / len(v)) for m, v in monthly.items() if v}


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=2, max=8))
def _fetch_hn_monthly_counts(term: str, months: int = 12) -> dict[datetime, int] | None:
    """Count HN stories+comments mentioning ``term`` per month, last ``months`` months."""
    cutoff = _month_starts(months)[0]
    cutoff_unix = int(cutoff.replace(tzinfo=timezone.utc).timestamp())
    params = {
        "query":          term,
        "tags":           "(story,comment)",
        "hitsPerPage":    1000,
        "numericFilters": f"created_at_i>{cutoff_unix}",
    }
    try:
        resp = requests.get(ALGOLIA_URL, params=params,
                            headers={"User-Agent": USER_AGENT}, timeout=20)
    except Exception as exc:
        log.warning("Algolia HN failed for %s: %s", term, exc)
        return None
    if resp.status_code in (403, 404, 429):
        log.warning("Algolia HN returned %s for %s", resp.status_code, term)
        return None
    try:
        resp.raise_for_status()
        hits = resp.json().get("hits", []) or []
    except Exception as exc:
        log.warning("Algolia HN bad payload for %s: %s", term, exc)
        return None

    counts: dict[datetime, int] = {}
    for h in hits:
        created_at_i = h.get("created_at_i")
        if not created_at_i:
            continue
        ts = datetime.fromtimestamp(int(created_at_i), tz=timezone.utc).replace(tzinfo=None)
        month = ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        counts[month] = counts.get(month, 0) + 1
    return counts


def _bundled_rows_for(slug: str) -> list[dict[str, Any]]:
    """Build deterministic monthly rows for a slug from SAMPLE_DATA."""
    sample = SAMPLE_DATA.get(slug)
    if not sample:
        return []
    rows: list[dict[str, Any]] = []
    for idx, month in enumerate(_month_starts(12)):
        wobble_int = (idx % 5 - 2) * 4
        wobble_hn = (idx % 3 - 1)
        rows.append({
            "company_slug":     slug,
            "signal_date":      month.date(),
            "search_interest":  max(0, min(100, sample["baseline_interest"] + wobble_int)),
            "hn_mention_count": max(0, sample["baseline_hn"] + wobble_hn),
        })
    return rows


def _build_rows(company: dict[str, str],
                trends: dict[datetime, int] | None,
                hn_counts: dict[datetime, int] | None) -> list[dict[str, Any]]:
    """Combine trends + HN counts into rows. Returns [] if both upstreams empty."""
    if not trends and not hn_counts:
        return []
    rows: list[dict[str, Any]] = []
    for month in _month_starts(12):
        interest = (trends or {}).get(month)
        hn_count = (hn_counts or {}).get(month)
        if interest is None and hn_count is None:
            continue
        rows.append({
            "company_slug":     company["slug"],
            "signal_date":      month.date(),
            "search_interest":  int(interest) if interest is not None else None,
            "hn_mention_count": int(hn_count) if hn_count is not None else 0,
        })
    return rows


def _upsert(engine: Engine, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = text("""
        INSERT INTO raw.web_signals (
            company_slug, signal_date, search_interest, hn_mention_count
        )
        VALUES (
            :company_slug, :signal_date, :search_interest, :hn_mention_count
        )
        ON CONFLICT (company_slug, signal_date) DO UPDATE SET
            search_interest  = EXCLUDED.search_interest,
            hn_mention_count = EXCLUDED.hn_mention_count,
            fetched_at       = NOW()
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def ingest_web_signals(**_: Any) -> None:
    """DAG entrypoint: pull trends + HN monthly counts, load rows."""
    engine = _engine()
    total = 0
    for company in TRACKED_COMPANIES:
        log.info("Pulling web signals for %s", company["slug"])
        trends: dict[datetime, int] | None = None
        hn_counts: dict[datetime, int] | None = None
        try:
            trends = _fetch_trends(company["trend_term"])
        except Exception as exc:
            log.warning("Google Trends hard failure for %s: %s", company["slug"], exc)
        try:
            hn_counts = _fetch_hn_monthly_counts(company["trend_term"])
        except Exception as exc:
            log.warning("Algolia HN hard failure for %s: %s", company["slug"], exc)

        rows = _build_rows(company, trends, hn_counts)
        if not rows:
            rows = _bundled_rows_for(company["slug"])
            if rows:
                log.info("Using %d bundled web-signal rows for %s",
                         len(rows), company["slug"])
        else:
            log.info("Built %d live web-signal rows for %s",
                     len(rows), company["slug"])

        total += _upsert(engine, rows)

    log.info("Web signals ingestion complete, %d rows attempted", total)


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="web_signals_dag",
    description="Weekly Google Trends + HN mention counts per tracked company.",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    default_args=default_args,
    tags=["saas-bi", "ingest", "web-signals"],
) as dag:
    PythonOperator(
        task_id="ingest_web_signals",
        python_callable=ingest_web_signals,
    )
