"""Thin HTTP wrapper around the FastAPI backend."""

from __future__ import annotations

import os
from typing import Any

import requests
import streamlit as st

BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8000").rstrip("/")
TIMEOUT = 15


def _get(path: str, params: dict[str, Any] | None = None) -> Any:
    """Call the backend and raise for non-2xx responses."""
    url = f"{BACKEND_URL}{path}"
    resp = requests.get(url, params=params or {}, timeout=TIMEOUT)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


@st.cache_data(ttl=60)
def list_companies(
    q: str | None = None, industry: str | None = None, limit: int = 50
) -> list[dict]:
    params: dict[str, Any] = {"limit": limit}
    if q:
        params["q"] = q
    if industry:
        params["industry"] = industry
    return _get("/companies", params) or []


@st.cache_data(ttl=60)
def search_companies(q: str, limit: int = 10) -> list[dict]:
    return _get("/companies/search", {"q": q, "limit": limit}) or []


@st.cache_data(ttl=300)
def list_industries() -> list[str]:
    return _get("/companies/industries") or []


@st.cache_data(ttl=60)
def get_health_score(name: str) -> dict | None:
    return _get(f"/companies/{name}/health-score")


@st.cache_data(ttl=60)
def get_reviews(name: str, limit: int = 50) -> list[dict]:
    return _get(f"/companies/{name}/reviews", {"limit": limit}) or []


@st.cache_data(ttl=60)
def get_funding(name: str) -> list[dict]:
    return _get(f"/companies/{name}/funding") or []


@st.cache_data(ttl=60)
def get_sentiment_trend(name: str) -> list[dict]:
    return _get(f"/companies/{name}/sentiment-trend") or []


def compare_companies(slugs: list[str]) -> list[dict]:
    """Side-by-side breakdown for 2-5 companies. Order matches the input."""
    if not slugs or len(slugs) < 2:
        return []
    # requests serialises lists by repeating the param: slug=a&slug=b...
    return _get("/companies/compare", {"slug": slugs}) or []
