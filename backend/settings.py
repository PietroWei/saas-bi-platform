"""Backend configuration loaded from environment variables.

The ``DATABASE_URL`` is normalised so the same value works locally
(via Docker Compose), against Supabase, and on Railway. Supabase
hands out a ``postgresql://...`` URL which is then upgraded to the
async driver expected by SQLAlchemy + asyncpg.
"""

from __future__ import annotations

import logging
from functools import lru_cache
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()

log = logging.getLogger(__name__)

LOCAL_DEFAULT_DATABASE_URL = (
    "postgresql+asyncpg://bi_user:change_me_strong_password@postgres:5432/bi_platform"
)


def _coerce_async_url(raw: str) -> str:
    """Return a SQLAlchemy async URL compatible with asyncpg.

    Accepts the formats Supabase / Railway / local Compose hand out:
      * postgres://user:pw@host:port/db
      * postgresql://user:pw@host:port/db
      * postgresql+asyncpg://user:pw@host:port/db (passes through)

    Strips ``sslmode``/``channel_binding`` query params (libpq only;
    asyncpg uses a different SSL flag) and re-emits the URL with the
    asyncpg driver.
    """
    if not raw:
        raise RuntimeError(
            "DATABASE_URL is empty. Set it to your Supabase or Postgres connection "
            "string (see backend/.env.example)."
        )

    parts = urlsplit(raw.strip())
    scheme = parts.scheme.lower()

    if scheme in {"postgres", "postgresql"}:
        scheme = "postgresql+asyncpg"
    elif scheme == "postgresql+asyncpg":
        pass
    else:
        raise RuntimeError(
            f"Unsupported DATABASE_URL scheme '{parts.scheme}'. "
            "Expected postgres:// or postgresql://."
        )

    drop_keys = {"sslmode", "channel_binding"}
    query_pairs = [(k, v) for k, v in parse_qsl(parts.query) if k.lower() not in drop_keys]
    new_query = urlencode(query_pairs)

    return urlunsplit((scheme, parts.netloc, parts.path, new_query, parts.fragment))


class Settings(BaseSettings):
    """Runtime configuration loaded from environment / .env file."""

    app_env: str = "development"
    log_level: str = "INFO"

    database_url: str = LOCAL_DEFAULT_DATABASE_URL

    marts_schema: str = "marts"
    raw_schema: str = "raw"

    cors_origins: str = "*"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @property
    def async_database_url(self) -> str:
        return _coerce_async_url(self.database_url)

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings instance."""
    return Settings()
