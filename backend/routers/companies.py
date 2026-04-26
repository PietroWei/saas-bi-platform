"""/companies endpoints - listing, search, and multi-company compare."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_session
from schemas import Company, HealthScoreBreakdown

router = APIRouter(prefix="/companies", tags=["companies"])


@router.get("", response_model=list[Company], summary="List / search companies")
async def list_companies(
    q: Optional[str] = Query(None, description="Case-insensitive substring search on slug/name"),
    industry: Optional[str] = Query(None, description="Filter by industry (case-insensitive exact match)"),
    limit: int = Query(50, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> list[Company]:
    """Return companies that currently have a health score computed.

    Optional filters: ``q`` (substring search) and ``industry`` (exact match).
    """
    sql = """
        SELECT company_slug, company_name, industry, country, health_score
          FROM marts.company_health_score
         WHERE (CAST(:q AS TEXT) IS NULL
                OR company_slug ILIKE '%' || :q || '%'
                OR company_name ILIKE '%' || :q || '%')
           AND (CAST(:industry AS TEXT) IS NULL
                OR lower(industry) = lower(:industry))
         ORDER BY health_score DESC NULLS LAST, company_name ASC
         LIMIT :limit
    """
    rows = (await session.execute(
        text(sql), {"q": q, "industry": industry, "limit": limit}
    )).mappings().all()
    return [Company(**row) for row in rows]


@router.get("/search", response_model=list[Company], summary="Simple company search")
async def search_companies(
    q: str = Query(..., min_length=1, description="Search term"),
    limit: int = Query(10, ge=1, le=100),
    session: AsyncSession = Depends(get_session),
) -> list[Company]:
    """Lightweight autocomplete-friendly endpoint."""
    sql = """
        SELECT company_slug, company_name, industry, country, health_score
          FROM marts.company_health_score
         WHERE company_slug ILIKE '%' || :q || '%'
            OR company_name ILIKE '%' || :q || '%'
         ORDER BY company_name ASC
         LIMIT :limit
    """
    rows = (await session.execute(text(sql), {"q": q, "limit": limit})).mappings().all()
    return [Company(**row) for row in rows]


@router.get(
    "/industries",
    response_model=list[str],
    summary="Distinct industries (used by the Compare filter)",
)
async def list_industries(session: AsyncSession = Depends(get_session)) -> list[str]:
    sql = """
        SELECT DISTINCT industry
          FROM marts.company_health_score
         WHERE industry IS NOT NULL
         ORDER BY industry
    """
    rows = (await session.execute(text(sql))).all()
    return [r[0] for r in rows]


@router.get(
    "/compare",
    response_model=list[HealthScoreBreakdown],
    summary="Side-by-side health score breakdown for 2-5 companies",
)
async def compare_companies(
    slug: list[str] = Query(
        ...,
        min_length=2,
        max_length=5,
        description="Repeat the parameter, e.g. ?slug=satispay&slug=nexi&slug=revolut",
    ),
    session: AsyncSession = Depends(get_session),
) -> list[HealthScoreBreakdown]:
    """Return one breakdown per requested slug, in the same order as the input.

    Matches by slug *or* name (case-insensitive). Raises 404 if any slug is unknown.
    """
    stmt = text("""
        SELECT company_slug, company_name, industry, country, founded_year,
               health_score, sentiment_score_0_100, funding_score_0_100,
               github_score_0_100, review_count_180d, total_raised_usd,
               last_round_date, total_stars, total_commits_30d,
               total_contributors_30d, computed_at
          FROM marts.company_health_score
         WHERE lower(company_slug) IN :keys
            OR lower(company_name) IN :keys
    """).bindparams(bindparam("keys", expanding=True))
    keys = [s.lower() for s in slug]
    rows = (await session.execute(stmt, {"keys": keys})).mappings().all()

    by_key: dict[str, dict] = {}
    for row in rows:
        by_key[row["company_slug"].lower()] = dict(row)
        by_key[row["company_name"].lower()] = dict(row)

    out: list[HealthScoreBreakdown] = []
    for key in keys:
        row = by_key.get(key)
        if row is None:
            raise HTTPException(status_code=404, detail=f"Unknown company '{key}'")
        out.append(HealthScoreBreakdown(**row))
    return out
