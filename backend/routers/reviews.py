"""Review + funding + sentiment-trend endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_session
from schemas import FundingRound, Review, SentimentTrendPoint

router = APIRouter(prefix="/companies", tags=["reviews"])


async def _resolve_slug(session: AsyncSession, name: str) -> str:
    """Map a free-form name/slug to the canonical company_slug, raise 404 otherwise."""
    sql = """
        SELECT company_slug
          FROM marts.company_health_score
         WHERE lower(company_slug) = lower(:n)
            OR lower(company_name) = lower(:n)
         LIMIT 1
    """
    row = (await session.execute(text(sql), {"n": name})).first()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Unknown company '{name}'")
    return row[0]


@router.get(
    "/{name}/reviews",
    response_model=list[Review],
    summary="Recent reviews with sentiment",
)
async def get_reviews(
    name: str = Path(...),
    limit: int = Query(50, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> list[Review]:
    slug = await _resolve_slug(session, name)
    sql = """
        SELECT review_id, review_title, review_body, rating,
               reviewer_role, review_date, sentiment_score
          FROM staging.stg_reviews
         WHERE company_slug = :slug
         ORDER BY review_date DESC NULLS LAST, review_id
         LIMIT :limit
    """
    rows = (await session.execute(text(sql), {"slug": slug, "limit": limit})).mappings().all()
    return [Review(**row) for row in rows]


@router.get(
    "/{name}/funding",
    response_model=list[FundingRound],
    summary="Funding timeline",
)
async def get_funding(
    name: str = Path(...),
    session: AsyncSession = Depends(get_session),
) -> list[FundingRound]:
    slug = await _resolve_slug(session, name)
    sql = """
        SELECT round_id, round_type, announced_on, amount_usd,
               lead_investor, investors, currency
          FROM staging.stg_funding
         WHERE company_slug = :slug
         ORDER BY announced_on ASC
    """
    rows = (await session.execute(text(sql), {"slug": slug})).mappings().all()
    return [FundingRound(**row) for row in rows]


@router.get(
    "/{name}/sentiment-trend",
    response_model=list[SentimentTrendPoint],
    summary="Monthly sentiment trend (from marts.sentiment_trend)",
)
async def get_sentiment_trend(
    name: str = Path(...),
    session: AsyncSession = Depends(get_session),
) -> list[SentimentTrendPoint]:
    slug = await _resolve_slug(session, name)
    sql = """
        SELECT month_start, avg_sentiment, sentiment_3mo_avg,
               review_count, avg_rating
          FROM marts.sentiment_trend
         WHERE company_slug = :slug
         ORDER BY month_start ASC
    """
    rows = (await session.execute(text(sql), {"slug": slug})).mappings().all()
    return [SentimentTrendPoint(**row) for row in rows]
