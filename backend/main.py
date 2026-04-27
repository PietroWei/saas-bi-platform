"""FastAPI entrypoint for the SaaS BI Platform."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from database import engine
from routers import companies, health_score, mentions
from settings import get_settings

settings = get_settings()

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
log = logging.getLogger("bi.backend")


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Verify DB connectivity on startup, dispose the engine on shutdown.

    A connection failure is logged but does not prevent the process from
    starting. This keeps ``/health`` reachable so platform health checks
    (Railway, Kubernetes) can surface a clear status while the operator
    fixes the DATABASE_URL.
    """
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        log.info("Database connection OK.")
    except Exception as exc:
        log.exception(
            "Database connection failed at startup. "
            "Check DATABASE_URL points at a reachable Postgres / Supabase. "
            "Underlying error: %s",
            exc,
        )
    yield
    await engine.dispose()


app = FastAPI(
    title="SaaS BI Platform API",
    description="Serves marts data for the Streamlit frontend.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origin_list or ["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health", tags=["meta"])
async def health() -> dict[str, str]:
    """Liveness + DB-readiness probe."""
    db_status = "ok"
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as exc:
        log.warning("Health check could not reach the database: %s", exc)
        db_status = "unreachable"
    return {"status": "ok", "env": settings.app_env, "database": db_status}


app.include_router(companies.router)
app.include_router(health_score.router)
app.include_router(mentions.router)
