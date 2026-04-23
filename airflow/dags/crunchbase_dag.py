"""Crunchbase funding ingestion DAG.

Crunchbase's full API is paid; for the demo we hit a free public mirror
(``https://api.crunchbase.com/api/v4`` by default, configurable through
``CRUNCHBASE_API_BASE``). If no API key is configured the task still runs —
it falls back to a small bundled sample so the downstream pipeline has data
to chew on.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

log = logging.getLogger(__name__)

TRACKED_COMPANIES: list[dict[str, str]] = [
    {"name": "Slack",    "slug": "slack"},
    {"name": "Notion",   "slug": "notion-so"},
    {"name": "Figma",    "slug": "figma"},
    {"name": "Asana",    "slug": "asana"},
    {"name": "Airtable", "slug": "airtable"},
]

# Fallback sample used when no API key is set — deterministic & dependency-free.
SAMPLE_ROUNDS: list[dict[str, Any]] = [
    {"company_name": "Slack",    "company_slug": "slack",
     "round_id": "slack-seriesH", "round_type": "Series H",
     "announced_on": "2019-06-20", "amount_usd": 427_000_000,
     "lead_investor": "SoftBank", "investors": "SoftBank, Accel"},
    {"company_name": "Notion",   "company_slug": "notion-so",
     "round_id": "notion-seriesC", "round_type": "Series C",
     "announced_on": "2021-10-08", "amount_usd": 275_000_000,
     "lead_investor": "Coatue", "investors": "Coatue, Sequoia"},
    {"company_name": "Figma",    "company_slug": "figma",
     "round_id": "figma-seriesE", "round_type": "Series E",
     "announced_on": "2021-06-24", "amount_usd": 200_000_000,
     "lead_investor": "Durable Capital", "investors": "Durable, Morgan Stanley"},
    {"company_name": "Asana",    "company_slug": "asana",
     "round_id": "asana-ipo", "round_type": "IPO",
     "announced_on": "2020-09-30", "amount_usd": 1_500_000_000,
     "lead_investor": None, "investors": "Public markets"},
    {"company_name": "Airtable", "company_slug": "airtable",
     "round_id": "airtable-seriesF", "round_type": "Series F",
     "announced_on": "2021-12-14", "amount_usd": 735_000_000,
     "lead_investor": "XN", "investors": "XN, Thrive Capital"},
]


def _engine() -> Engine:
    url = os.environ.get("APP_POSTGRES_URL")
    if not url:
        raise RuntimeError("APP_POSTGRES_URL env var is not set")
    return create_engine(url, pool_pre_ping=True, future=True)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
def _fetch_company_rounds(slug: str, api_key: str, api_base: str) -> list[dict[str, Any]]:
    """Call the Crunchbase API for a single company's funding rounds."""
    url = f"{api_base.rstrip('/')}/entities/organizations/{slug}"
    params = {
        "user_key": api_key,
        "card_ids": "raised_funding_rounds",
    }
    resp = requests.get(url, params=params, timeout=20)
    if resp.status_code == 404:
        log.warning("Crunchbase 404 for %s — skipping", slug)
        return []
    resp.raise_for_status()
    payload = resp.json()

    rounds_raw = (
        payload.get("cards", {})
               .get("raised_funding_rounds", [])
    )
    out: list[dict[str, Any]] = []
    for r in rounds_raw:
        props = r.get("properties", {})
        out.append({
            "round_id":      r.get("uuid") or f"{slug}-{props.get('announced_on')}",
            "round_type":    props.get("investment_type"),
            "announced_on":  props.get("announced_on"),
            "amount_usd":    (props.get("money_raised") or {}).get("value_usd"),
            "lead_investor": props.get("lead_investor_identifiers", [{}])[0].get("value")
                              if props.get("lead_investor_identifiers") else None,
            "investors":     ", ".join(
                i.get("value", "") for i in props.get("investor_identifiers", []) or []
            ) or None,
            "currency":      (props.get("money_raised") or {}).get("currency", "USD"),
            "source_url":    url,
        })
    return out


def _clean(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Cast types + drop malformed rows."""
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["announced_on"] = pd.to_datetime(df["announced_on"], errors="coerce").dt.date
    df["amount_usd"] = pd.to_numeric(df["amount_usd"], errors="coerce")
    df = df.dropna(subset=["round_id", "announced_on"])
    return df


def _upsert(engine: Engine, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    sql = text("""
        INSERT INTO raw.crunchbase_funding (
            company_name, company_slug, round_id, round_type, announced_on,
            amount_usd, lead_investor, investors, currency, source_url
        )
        VALUES (
            :company_name, :company_slug, :round_id, :round_type, :announced_on,
            :amount_usd, :lead_investor, :investors, :currency, :source_url
        )
        ON CONFLICT (company_slug, round_id) DO NOTHING
    """)
    rows = df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def ingest_funding(**_: Any) -> None:
    """DAG task: pull funding rounds for tracked companies."""
    engine = _engine()
    api_key = os.environ.get("CRUNCHBASE_API_KEY", "").strip()
    api_base = os.environ.get("CRUNCHBASE_API_BASE", "https://api.crunchbase.com/api/v4")

    all_rows: list[dict[str, Any]] = []
    if not api_key:
        log.warning("CRUNCHBASE_API_KEY not set — loading bundled sample.")
        for sample in SAMPLE_ROUNDS:
            row = dict(sample)
            row["currency"] = "USD"
            row["source_url"] = "bundled-sample"
            all_rows.append(row)
    else:
        for company in TRACKED_COMPANIES:
            try:
                rounds = _fetch_company_rounds(company["slug"], api_key, api_base)
            except Exception as exc:
                log.exception("Fetch failed for %s: %s", company["slug"], exc)
                continue
            for r in rounds:
                r["company_name"] = company["name"]
                r["company_slug"] = company["slug"]
                all_rows.append(r)

    df = _clean(all_rows)
    count = _upsert(engine, df)
    log.info("Crunchbase ingestion complete — %d rows written", count)


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="crunchbase_dag",
    description="Pull funding rounds for tracked SaaS companies.",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ingest", "funding", "crunchbase"],
) as dag:
    PythonOperator(
        task_id="ingest_funding",
        python_callable=ingest_funding,
    )
