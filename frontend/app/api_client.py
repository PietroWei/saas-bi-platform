"""Thin HTTP wrapper around the FastAPI backend.

The base URL is read from the ``BACKEND_URL`` environment variable so
the same image runs locally (defaults to ``http://localhost:8000``)
and on Railway (set to the FastAPI service's public URL).

When the backend is unreachable or the database has not been seeded
yet, the public helpers fall back to bundled sample data so the
dashboards still render something meaningful.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import requests
import streamlit as st
from dotenv import load_dotenv

from sample_data import (
    SAMPLE_COMPANIES,
    filter_companies,
    sample_health_breakdown,
    sample_industries,
    sample_sentiment_trend,
)

load_dotenv()

log = logging.getLogger(__name__)

BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8000").rstrip("/")
TIMEOUT = 15

# Force the dashboard to render bundled samples even if the backend is up.
# Useful for local UI work or when the deployed Supabase is empty on purpose.
USE_SAMPLE_DATA = os.environ.get("USE_SAMPLE_DATA", "").lower() in {"1", "true", "yes"}


def _get(path: str, params: dict[str, Any] | None = None) -> Any:
    """Call the backend and raise for non-2xx responses.

    Returns ``None`` if the backend is unreachable or returns 404, so
    callers can transparently fall back to bundled sample data.
    """
    url = f"{BACKEND_URL}{path}"
    try:
        resp = requests.get(url, params=params or {}, timeout=TIMEOUT)
    except requests.RequestException as exc:
        log.warning("Backend unreachable at %s: %s", url, exc)
        return None
    if resp.status_code == 404:
        return None
    if resp.status_code >= 500:
        log.warning("Backend %s returned %s", url, resp.status_code)
        return None
    resp.raise_for_status()
    return resp.json()


@st.cache_data(ttl=60)
def list_companies(
    q: str | None = None, industry: str | None = None, limit: int = 50
) -> list[dict]:
    if not USE_SAMPLE_DATA:
        params: dict[str, Any] = {"limit": limit}
        if q:
            params["q"] = q
        if industry:
            params["industry"] = industry
        rows = _get("/companies", params)
        if rows:
            return rows
    return filter_companies(SAMPLE_COMPANIES, q=q, industry=industry)[:limit]


@st.cache_data(ttl=60)
def search_companies(q: str, limit: int = 10) -> list[dict]:
    if not USE_SAMPLE_DATA:
        rows = _get("/companies/search", {"q": q, "limit": limit})
        if rows:
            return rows
    return filter_companies(SAMPLE_COMPANIES, q=q)[:limit]


@st.cache_data(ttl=300)
def list_industries() -> list[str]:
    if not USE_SAMPLE_DATA:
        rows = _get("/companies/industries")
        if rows:
            return rows
    return sample_industries()


@st.cache_data(ttl=60)
def get_health_score(name: str) -> dict | None:
    if not USE_SAMPLE_DATA:
        row = _get(f"/companies/{name}/health-score")
        if row:
            return row
    return sample_health_breakdown(name.lower())


@st.cache_data(ttl=60)
def get_mentions(name: str, limit: int = 50) -> list[dict]:
    return _get(f"/companies/{name}/mentions", {"limit": limit}) or []


@st.cache_data(ttl=60)
def get_funding(name: str) -> list[dict]:
    return _get(f"/companies/{name}/funding") or []


@st.cache_data(ttl=60)
def get_sentiment_trend(name: str) -> list[dict]:
    if not USE_SAMPLE_DATA:
        rows = _get(f"/companies/{name}/sentiment-trend")
        if rows:
            return rows
    return sample_sentiment_trend(name.lower())


def compare_companies(slugs: list[str]) -> list[dict]:
    """Side-by-side breakdown for 2-5 companies. Order matches the input."""
    if not slugs or len(slugs) < 2:
        return []
    if not USE_SAMPLE_DATA:
        rows = _get("/companies/compare", {"slug": slugs})
        if rows:
            return rows
    out: list[dict] = []
    for slug in slugs:
        bd = sample_health_breakdown(slug.lower())
        if bd:
            out.append(bd)
    return out
