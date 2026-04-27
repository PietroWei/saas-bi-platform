"""Async SQLAlchemy engine + session factory.

The engine is built from ``Settings.async_database_url`` so the same
backend image runs locally (Docker Compose) and against Supabase on
Railway with no code changes - only the env var differs.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from settings import get_settings

settings = get_settings()

engine = create_async_engine(
    settings.async_database_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    future=True,
)

SessionLocal = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    autoflush=False,
)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency - yields an async DB session scoped to one request."""
    async with SessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()
