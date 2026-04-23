"""Backend configuration — driven entirely by environment variables."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration loaded from environment / .env file."""

    app_env: str = "development"
    log_level: str = "INFO"

    # Full async SQLAlchemy URL, e.g. postgresql+asyncpg://user:pw@host/db
    database_url: str = (
        "postgresql+asyncpg://bi_user:change_me_strong_password@postgres:5432/bi_platform"
    )

    marts_schema: str = "marts"
    raw_schema: str = "raw"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings instance."""
    return Settings()
