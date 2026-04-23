"""FastAPI entrypoint for the SaaS BI Platform."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from database import engine
from routers import companies, health_score, reviews
from settings import get_settings

settings = get_settings()

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
log = logging.getLogger("bi.backend")


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Verify DB connectivity on startup, dispose the engine on shutdown."""
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        log.info("Database connection OK.")
    except Exception as exc:  # pragma: no cover — diagnostic only
        log.exception("Database connection failed at startup: %s", exc)
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
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/health", tags=["meta"])
async def health() -> dict[str, str]:
    """Liveness probe."""
    return {"status": "ok", "env": settings.app_env}


app.include_router(companies.router)
app.include_router(health_score.router)
app.include_router(reviews.router)
