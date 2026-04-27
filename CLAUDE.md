# CLAUDE.md

Guidance for Claude Code when working in this repository.

## Project

SaaS BI Platform - ingests public data about SaaS companies (HackerNews mentions via Algolia, Crunchbase funding, GitHub activity), transforms it with dbt, serves it via FastAPI, and visualizes it in Streamlit. Everything is containerized.

## Stack

| Layer            | Tech                          | Port |
|------------------|-------------------------------|------|
| Ingest / orchestr| Airflow 2.9 (LocalExecutor)   | 8080 |
| Storage          | Postgres 15                   | 5432 |
| Transform        | dbt-postgres 1.8              | -    |
| API              | FastAPI + SQLAlchemy async    | 8000 |
| Frontend         | Streamlit + Plotly            | 8501 |

## Directory map

```
saas-bi-platform/
├── airflow/   DAGs + custom Dockerfile
├── dbt/       project, profile, staging + marts models
├── backend/   FastAPI app + routers
├── frontend/  Streamlit multi-page app
├── database/  Postgres init SQL (schemas, raw tables)
└── docker-compose.yml
```

Data flow: `DAGs → raw.* → dbt staging → dbt marts → FastAPI → Streamlit`.

## Conventions

- Python 3.12. Type hints on all public functions. Docstrings where intent is not obvious.
- Never hardcode credentials - use env vars through `python-dotenv` / `pydantic-settings`.
- Raw tables live in schema `raw`, transformed tables in schema `marts`.
- dbt materializes staging as views, marts as tables.
- All DAGs idempotent: `raw` tables have `UNIQUE` keys, loaders use `INSERT ... ON CONFLICT DO NOTHING`.
- The `company_health_score` mart is the canonical 0-100 health metric; changes to weighting must update `dbt/models/marts/company_health_score.sql`.

## Running locally

```bash
cp .env.example .env       # edit values
docker compose up --build
# Airflow  → http://localhost:8080  (admin/admin)
# API      → http://localhost:8000/docs
# Streamlit→ http://localhost:8501
# dbt      → docker compose run --rm dbt-runner dbt run
```

For local coding without Docker use the conda env:
```bash
conda env create -f environment.yml
conda activate saas_bi
```

## When modifying pipelines

1. Update the raw DDL in `database/init/02_create_raw_tables.sql` **and** the loader's `ON CONFLICT` target.
2. Update the matching `stg_*.sql` so downstream marts stay in sync.
3. Re-run dbt: `docker compose run --rm dbt-runner dbt build`.
