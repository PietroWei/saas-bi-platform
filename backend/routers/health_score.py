"""/companies/{name}/health-score - composite score + breakdown."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_session
from schemas import HealthScoreBreakdown

router = APIRouter(prefix="/companies", tags=["health"])


@router.get(
    "/{name}/health-score",
    response_model=HealthScoreBreakdown,
    summary="Composite 0-100 health score with component breakdown",
)
async def get_health_score(
    name: str = Path(..., description="Company slug or name (case-insensitive)"),
    session: AsyncSession = Depends(get_session),
) -> HealthScoreBreakdown:
    """Look up the latest health score for a single company.

    Matches either the canonical ``company_slug`` or ``company_name`` (case-insensitive).
    """
    sql = """
        SELECT company_slug, company_name, industry, country, founded_year,
               health_score, sentiment_score_0_100, funding_score_0_100,
               github_score_0_100, mention_count_180d, total_raised_usd,
               last_round_date, total_stars, total_commits_30d,
               total_contributors_30d, computed_at
          FROM marts.company_health_score
         WHERE lower(company_slug) = lower(:n)
            OR lower(company_name) = lower(:n)
         LIMIT 1
    """
    row = (await session.execute(text(sql), {"n": name})).mappings().first()
    if row is None:
        raise HTTPException(status_code=404, detail=f"No health score for '{name}'")
    return HealthScoreBreakdown(**row)
