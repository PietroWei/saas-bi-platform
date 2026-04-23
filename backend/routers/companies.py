"""/companies endpoints — listing and searching."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_session
from schemas import Company

router = APIRouter(prefix="/companies", tags=["companies"])


@router.get("", response_model=list[Company], summary="List / search companies")
async def list_companies(
    q: Optional[str] = Query(None, description="Case-insensitive substring search on slug/name"),
    limit: int = Query(50, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> list[Company]:
    """Return companies that currently have a health score computed.

    Pass ``?q=`` to filter by name/slug substring.
    """
    sql = """
        SELECT company_slug, company_name, health_score
          FROM marts.company_health_score
         WHERE (:q IS NULL
                OR company_slug ILIKE '%' || :q || '%'
                OR company_name ILIKE '%' || :q || '%')
         ORDER BY health_score DESC NULLS LAST, company_name ASC
         LIMIT :limit
    """
    rows = (await session.execute(text(sql), {"q": q, "limit": limit})).mappings().all()
    return [Company(**row) for row in rows]


@router.get("/search", response_model=list[Company], summary="Simple company search")
async def search_companies(
    q: str = Query(..., min_length=1, description="Search term"),
    limit: int = Query(10, ge=1, le=100),
    session: AsyncSession = Depends(get_session),
) -> list[Company]:
    """Lightweight autocomplete-friendly endpoint."""
    sql = """
        SELECT company_slug, company_name, health_score
          FROM marts.company_health_score
         WHERE company_slug ILIKE '%' || :q || '%'
            OR company_name ILIKE '%' || :q || '%'
         ORDER BY company_name ASC
         LIMIT :limit
    """
    rows = (await session.execute(text(sql), {"q": q, "limit": limit})).mappings().all()
    return [Company(**row) for row in rows]
