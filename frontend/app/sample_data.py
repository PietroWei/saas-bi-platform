"""Bundled sample data for the Streamlit frontend.

Used as a graceful fallback when the FastAPI backend is unreachable
(e.g. Supabase still empty) so the dashboards always have something
to render. The numbers are illustrative, not real.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

_NOW = datetime.now(tz=timezone.utc)


SAMPLE_COMPANIES: list[dict[str, Any]] = [
    # --- Workplace SaaS
    {"company_slug": "slack",     "company_name": "Slack",     "industry": "Workplace SaaS",         "country": "USA",     "health_score": 78.4},
    {"company_slug": "notion",    "company_name": "Notion",    "industry": "Workplace SaaS",         "country": "USA",     "health_score": 72.9},
    {"company_slug": "figma",     "company_name": "Figma",     "industry": "Workplace SaaS",         "country": "USA",     "health_score": 81.1},
    {"company_slug": "asana",     "company_name": "Asana",     "industry": "Workplace SaaS",         "country": "USA",     "health_score": 61.3},
    {"company_slug": "airtable",  "company_name": "Airtable",  "industry": "Workplace SaaS",         "country": "USA",     "health_score": 64.8},
    # --- Fintech / payments
    {"company_slug": "satispay",  "company_name": "Satispay",  "industry": "Fintech / Payments",     "country": "Italy",   "health_score": 74.6},
    {"company_slug": "nexi",      "company_name": "Nexi",      "industry": "Fintech / Payments",     "country": "Italy",   "health_score": 55.2},
    {"company_slug": "revolut",   "company_name": "Revolut",   "industry": "Fintech / Payments",     "country": "UK",      "health_score": 76.0},
    {"company_slug": "klarna",    "company_name": "Klarna",    "industry": "Fintech / Payments",     "country": "Sweden",  "health_score": 65.7},
    {"company_slug": "n26",       "company_name": "N26",       "industry": "Fintech / Payments",     "country": "Germany", "health_score": 58.9},
    # --- Capital Markets / Treasury
    {"company_slug": "murex",     "company_name": "Murex",     "industry": "Capital Markets / Treasury", "country": "France", "health_score": 68.4},
    {"company_slug": "finastra",  "company_name": "Finastra",  "industry": "Capital Markets / Treasury", "country": "UK",     "health_score": 51.8},
    {"company_slug": "calypso",   "company_name": "Calypso",   "industry": "Capital Markets / Treasury", "country": "USA",    "health_score": 56.3},
    {"company_slug": "fis",       "company_name": "FIS",       "industry": "Capital Markets / Treasury", "country": "USA",    "health_score": 60.1},
    {"company_slug": "bloomberg", "company_name": "Bloomberg", "industry": "Capital Markets / Treasury", "country": "USA",    "health_score": 84.7},
]


SAMPLE_FOUNDED_YEAR = {
    "slack": 2009, "notion": 2016, "figma": 2012, "asana": 2008, "airtable": 2012,
    "satispay": 2013, "nexi": 1939, "revolut": 2015, "klarna": 2005, "n26": 2013,
    "murex": 1986, "finastra": 2017, "calypso": 1997, "fis": 1968, "bloomberg": 1981,
}

# Per-company score breakdowns and totals. Keep numbers internally consistent
# with SAMPLE_COMPANIES.health_score (rough average of components).
_BREAKDOWNS: dict[str, dict[str, Any]] = {
    "slack":     {"sentiment": 75.0, "funding": 82.0, "github": 78.0, "mentions180": 142, "raised": 1_400_000_000, "last_round": "2018-08-21", "stars": 28000, "commits": 220, "contrib": 35},
    "notion":    {"sentiment": 70.0, "funding": 80.0, "github": 68.0, "mentions180": 110, "raised": 343_000_000,   "last_round": "2021-10-08", "stars": 14500, "commits": 180, "contrib": 28},
    "figma":     {"sentiment": 82.0, "funding": 85.0, "github": 76.0, "mentions180": 198, "raised": 333_000_000,   "last_round": "2021-06-24", "stars": 22000, "commits": 260, "contrib": 42},
    "asana":     {"sentiment": 55.0, "funding": 70.0, "github": 60.0, "mentions180": 64,  "raised": 213_000_000,   "last_round": "2018-11-05", "stars": 9000,  "commits": 140, "contrib": 22},
    "airtable":  {"sentiment": 64.0, "funding": 78.0, "github": 52.0, "mentions180": 88,  "raised": 1_360_000_000, "last_round": "2021-12-14", "stars": 6200,  "commits": 95,  "contrib": 14},
    "satispay":  {"sentiment": 78.0, "funding": 76.0, "github": 70.0, "mentions180": 96,  "raised": 422_000_000,   "last_round": "2025-09-18", "stars": 4800,  "commits": 165, "contrib": 24},
    "nexi":      {"sentiment": 48.0, "funding": 62.0, "github": 56.0, "mentions180": 41,  "raised": 0,             "last_round": None,         "stars": 1100,  "commits": 60,  "contrib": 9},
    "revolut":   {"sentiment": 77.0, "funding": 84.0, "github": 67.0, "mentions180": 156, "raised": 1_700_000_000, "last_round": "2021-07-15", "stars": 7400,  "commits": 195, "contrib": 31},
    "klarna":    {"sentiment": 62.0, "funding": 80.0, "github": 55.0, "mentions180": 87,  "raised": 3_700_000_000, "last_round": "2022-07-11", "stars": 3300,  "commits": 110, "contrib": 17},
    "n26":       {"sentiment": 55.0, "funding": 72.0, "github": 50.0, "mentions180": 55,  "raised": 1_700_000_000, "last_round": "2021-10-18", "stars": 2800,  "commits": 80,  "contrib": 12},
    "murex":     {"sentiment": 70.0, "funding": 60.0, "github": 75.0, "mentions180": 38,  "raised": 0,             "last_round": None,         "stars": 5200,  "commits": 240, "contrib": 38},
    "finastra":  {"sentiment": 50.0, "funding": 58.0, "github": 47.0, "mentions180": 22,  "raised": 0,             "last_round": None,         "stars": 900,   "commits": 55,  "contrib": 10},
    "calypso":   {"sentiment": 56.0, "funding": 60.0, "github": 53.0, "mentions180": 28,  "raised": 0,             "last_round": None,         "stars": 1600,  "commits": 70,  "contrib": 12},
    "fis":       {"sentiment": 60.0, "funding": 64.0, "github": 56.0, "mentions180": 31,  "raised": 0,             "last_round": None,         "stars": 2100,  "commits": 90,  "contrib": 16},
    "bloomberg": {"sentiment": 86.0, "funding": 88.0, "github": 80.0, "mentions180": 287, "raised": 0,             "last_round": None,         "stars": 18000, "commits": 320, "contrib": 55},
}


def _company_meta(slug: str) -> dict[str, Any]:
    for c in SAMPLE_COMPANIES:
        if c["company_slug"] == slug:
            return c
    return {}


def sample_industries() -> list[str]:
    return sorted({c["industry"] for c in SAMPLE_COMPANIES})


def sample_health_breakdown(slug: str) -> dict[str, Any] | None:
    """Mirror the shape returned by ``GET /companies/{name}/health-score``."""
    meta = _company_meta(slug)
    bd = _BREAKDOWNS.get(slug)
    if not meta or not bd:
        return None
    last_round = (
        date.fromisoformat(bd["last_round"]) if bd.get("last_round") else None
    )
    return {
        "company_slug":          meta["company_slug"],
        "company_name":          meta["company_name"],
        "industry":              meta["industry"],
        "country":               meta["country"],
        "founded_year":          SAMPLE_FOUNDED_YEAR.get(slug),
        "health_score":          meta["health_score"],
        "sentiment_score_0_100": bd["sentiment"],
        "funding_score_0_100":   bd["funding"],
        "github_score_0_100":    bd["github"],
        "mention_count_180d":    bd["mentions180"],
        "total_raised_usd":      bd["raised"],
        "last_round_date":       last_round.isoformat() if last_round else None,
        "total_stars":           bd["stars"],
        "total_commits_30d":     bd["commits"],
        "total_contributors_30d": bd["contrib"],
        "computed_at":           _NOW.isoformat(),
    }


def sample_sentiment_trend(slug: str) -> list[dict[str, Any]]:
    """12 months of synthetic sentiment for the chart."""
    bd = _BREAKDOWNS.get(slug)
    if not bd:
        return []
    base = (bd["sentiment"] - 50.0) / 50.0
    today = date.today().replace(day=1)
    points: list[dict[str, Any]] = []
    rolling: list[float] = []
    for i in range(11, -1, -1):
        month = today - timedelta(days=30 * i)
        month = month.replace(day=1)
        wobble = ((i % 5) - 2) * 0.05
        avg = max(-1.0, min(1.0, round(base + wobble, 4)))
        rolling.append(avg)
        rolling = rolling[-3:]
        points.append({
            "month_start":       month.isoformat(),
            "avg_sentiment":     avg,
            "sentiment_3mo_avg": round(sum(rolling) / len(rolling), 4),
            "mention_count":     max(1, bd["mentions180"] // 12 + (i % 3)),
            "avg_points":        round(40 + (bd["sentiment"] / 4) + (i % 4) * 3, 2),
        })
    return points


def filter_companies(
    rows: list[dict[str, Any]],
    q: str | None = None,
    industry: str | None = None,
) -> list[dict[str, Any]]:
    """Apply the same filters the API would apply, locally."""
    out = rows
    if industry:
        out = [r for r in out if (r.get("industry") or "").lower() == industry.lower()]
    if q:
        needle = q.lower()
        out = [
            r for r in out
            if needle in (r.get("company_slug") or "").lower()
            or needle in (r.get("company_name") or "").lower()
        ]
    return out
