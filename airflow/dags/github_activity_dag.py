"""GitHub activity ingestion DAG.

For each tracked SaaS company we resolve its canonical GitHub org, pull the
top public repositories, and compute a daily health snapshot (stars, forks,
open issues, 30-day commit count, unique 30-day contributor count).

Without a GITHUB_TOKEN the public API allows 60 req/h which is often enough
for a small tracked-company list. With a token the limit is 5 000 req/h.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
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
    {"name": "Slack",    "slug": "slack",    "org": "slackhq"},
    {"name": "Notion",   "slug": "notion-so","org": "makenotion"},
    {"name": "Figma",    "slug": "figma",    "org": "figma"},
    {"name": "Asana",    "slug": "asana",    "org": "asana"},
    {"name": "Airtable", "slug": "airtable", "org": "airtable"},
]

API_ROOT = "https://api.github.com"
TOP_REPOS_PER_ORG = 3  # keep call volume low by default


def _engine() -> Engine:
    url = os.environ.get("APP_POSTGRES_URL")
    if not url:
        raise RuntimeError("APP_POSTGRES_URL env var is not set")
    return create_engine(url, pool_pre_ping=True, future=True)


def _headers() -> dict[str, str]:
    h = {"Accept": "application/vnd.github+json", "User-Agent": "SaaSBIBot/1.0"}
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=10))
def _gh_get(path: str, params: dict[str, Any] | None = None) -> Any:
    url = f"{API_ROOT}{path}"
    resp = requests.get(url, headers=_headers(), params=params or {}, timeout=20)
    if resp.status_code == 403 and "rate limit" in resp.text.lower():
        log.warning("GitHub rate limit hit on %s", url)
        return None
    if resp.status_code == 404:
        log.warning("GitHub 404 for %s", url)
        return None
    resp.raise_for_status()
    return resp.json()


def _top_repos(org: str) -> list[dict[str, Any]]:
    data = _gh_get(f"/orgs/{org}/repos", params={
        "per_page": 50, "sort": "updated", "type": "public",
    })
    if not data:
        return []
    ranked = sorted(
        (r for r in data if not r.get("archived")),
        key=lambda r: r.get("stargazers_count") or 0,
        reverse=True,
    )
    return ranked[:TOP_REPOS_PER_ORG]


def _commit_count_30d(full_name: str) -> int:
    since = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    data = _gh_get(f"/repos/{full_name}/commits",
                   params={"since": since, "per_page": 100})
    return len(data) if isinstance(data, list) else 0


def _contributors_30d(full_name: str) -> int:
    since = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    data = _gh_get(f"/repos/{full_name}/commits",
                   params={"since": since, "per_page": 100})
    if not isinstance(data, list):
        return 0
    return len({
        (c.get("author") or {}).get("login")
        for c in data
        if (c.get("author") or {}).get("login")
    })


def _collect(company: dict[str, str]) -> list[dict[str, Any]]:
    repos = _top_repos(company["org"])
    if not repos:
        log.warning("No repos returned for org %s", company["org"])
        return []

    today = datetime.now(timezone.utc).date()
    out: list[dict[str, Any]] = []
    for repo in repos:
        full_name = repo["full_name"]
        out.append({
            "company_name":     company["name"],
            "company_slug":     company["slug"],
            "org_login":        company["org"],
            "repo_full_name":   full_name,
            "snapshot_date":    today,
            "stars":            repo.get("stargazers_count"),
            "forks":            repo.get("forks_count"),
            "open_issues":      repo.get("open_issues_count"),
            "watchers":         repo.get("subscribers_count") or repo.get("watchers_count"),
            "commits_last_30d": _commit_count_30d(full_name),
            "contributors_30d": _contributors_30d(full_name),
            "primary_language": repo.get("language"),
            "source_url":       repo.get("html_url"),
        })
    return out


def _clean(rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for col in ("stars", "forks", "open_issues", "watchers",
                "commits_last_30d", "contributors_30d"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date
    return df


def _upsert(engine: Engine, df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    sql = text("""
        INSERT INTO raw.github_activity (
            company_name, company_slug, org_login, repo_full_name, snapshot_date,
            stars, forks, open_issues, watchers,
            commits_last_30d, contributors_30d, primary_language, source_url
        )
        VALUES (
            :company_name, :company_slug, :org_login, :repo_full_name, :snapshot_date,
            :stars, :forks, :open_issues, :watchers,
            :commits_last_30d, :contributors_30d, :primary_language, :source_url
        )
        ON CONFLICT (repo_full_name, snapshot_date) DO NOTHING
    """)
    rows = df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(sql, rows)
    return len(rows)


def ingest_github_activity(**_: Any) -> None:
    """DAG task: pull GitHub snapshot for tracked companies."""
    engine = _engine()
    all_rows: list[dict[str, Any]] = []
    for company in TRACKED_COMPANIES:
        try:
            all_rows.extend(_collect(company))
        except Exception as exc:
            log.exception("Failed to collect GitHub stats for %s: %s",
                          company["slug"], exc)
            continue

    df = _clean(all_rows)
    written = _upsert(engine, df)
    log.info("GitHub ingestion complete — %d repo snapshots written", written)


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="github_activity_dag",
    description="Snapshot GitHub activity for tracked SaaS companies.",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["ingest", "github"],
) as dag:
    PythonOperator(
        task_id="ingest_github_activity",
        python_callable=ingest_github_activity,
    )
