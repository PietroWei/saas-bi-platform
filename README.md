# SaaS BI Platform

A self-hosted Business Intelligence platform that aggregates **public signals about SaaS, fintech and capital-markets vendors** - HackerNews mentions (via Algolia), Crunchbase funding rounds, and GitHub activity - and turns them into three views:

- **Company view** (*lato azienda*) - "how is my company perceived?"
- **Analyst view** (*lato analyst*) - "is this company healthy? should I bet on it?"
- **Compare view** - side-by-side radar comparison across 2-5 peers (e.g. Satispay vs. Nexi vs. Revolut, or Murex vs. Calypso vs. Finastra).

The whole stack comes up with a single `docker-compose up --build`.

## Dashboards

> Screenshots live in [`docs/screenshots/`](docs/screenshots/). If the images
> below don't render, the local stack hasn't been captured yet - see
> [`docs/screenshots/README.md`](docs/screenshots/README.md) for how to
> regenerate them.

### Landing - top performers + industry filter
![Landing](docs/screenshots/landing.png)

### Company View - single-company gauge + sentiment trend
![Company View](docs/screenshots/company_view.png)

### Analyst View - full due-diligence dossier
![Analyst View](docs/screenshots/analyst_view.png)

### Compare View - radar chart across 2-5 peers
![Compare View](docs/screenshots/compare_view.png)

---

## Architecture

```
┌────────────┐   ┌───────────┐   ┌──────────┐   ┌──────────┐   ┌────────────┐
│  Airflow   │──▶│ Postgres  │──▶│   dbt    │──▶│ FastAPI  │──▶│ Streamlit  │
│  (ingest)  │   │  raw.*    │   │ marts.*  │   │  :8000   │   │   :8501    │
└────────────┘   └───────────┘   └──────────┘   └──────────┘   └────────────┘
```

| Service             | Role                                  | Port  |
|---------------------|---------------------------------------|-------|
| `postgres`          | Data warehouse (schemas `raw`, `marts`) | 5432  |
| `airflow-webserver` | DAG UI                                | 8080  |
| `airflow-scheduler` | DAG execution                         | -     |
| `dbt-runner`        | Transformations on demand             | -     |
| `fastapi-backend`   | REST API                              | 8000  |
| `streamlit-frontend`| Dashboards                            | 8501  |

Data flow: **DAGs** write to `raw.*` tables → **dbt** materializes `staging` views and `marts` tables → **FastAPI** serves JSON from `marts` → **Streamlit** consumes the API.

---

## Quick start

### 1. Clone and configure

```bash
git clone <this-repo> saas-bi-platform
cd saas-bi-platform
cp .env.example .env
```

Generate the two Airflow secrets and paste them into `.env`:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python -c "import secrets; print(secrets.token_hex(32))"
```

Optional: add a `GITHUB_TOKEN` to `.env` to raise the GitHub rate limit from 60 to 5 000 req/h.

### 2. Start the stack

```bash
docker compose up --build
```

First boot takes ~2 min (image builds, Airflow metadata init, Postgres seed).

| URL                          | Credentials        |
|------------------------------|--------------------|
| http://localhost:8080        | `admin` / `admin`  |
| http://localhost:8000/docs   | -                  |
| http://localhost:8501        | -                  |

### 3. Trigger the pipelines

In Airflow, unpause and trigger:

1. `hackernews_dag`
2. `crunchbase_dag`
3. `github_activity_dag`

Then run the transformations:

```bash
docker compose run --rm dbt-runner dbt build
```

Open Streamlit at http://localhost:8501 - the dashboards will populate.

---

## Local development without Docker

A conda environment is provided (mirrors the `repair_gui` setup with Python 3.12):

```bash
conda env create -f environment.yml
conda activate saas_bi
```

You can now run unit tests, lint with `ruff`, and iterate on DAG / dbt / FastAPI code without rebuilding containers.

```bash
# Run the API alone against a local Postgres
uvicorn backend.main:app --reload --port 8000

# Render Streamlit alone
streamlit run frontend/app/main.py

# Parse a DAG file for syntax errors
python airflow/dags/hackernews_dag.py
```

---

## Project structure

```
saas-bi-platform/
├── docker-compose.yml
├── .env.example
├── environment.yml            ← conda env (Python 3.12)
├── README.md
├── CLAUDE.md
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       ├── hackernews_dag.py       ← queries Algolia HN search (requests)
│       ├── crunchbase_dag.py       ← pulls funding events
│       └── github_activity_dag.py  ← pulls repo stats
├── dbt/
│   ├── Dockerfile
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/                ← stg_mentions, stg_funding, stg_github + schema.yml
│       └── marts/                  ← dim_companies, company_health_score, sentiment_trend, hiring_momentum + schema.yml
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py
│   └── routers/                    ← companies (incl. /compare), health_score, mentions
├── frontend/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py
│       ├── theme.py                ← shared CSS + palette
│       ├── pages/                  ← company_view, analyst_view, compare_view
│       └── components/             ← health_score_card, sentiment_chart, funding_timeline
├── database/
│   └── init/
│       ├── 01_create_schemas.sql
│       └── 02_create_raw_tables.sql
└── docs/
    └── screenshots/                ← landing / company / analyst / compare PNGs
```

---

## Tracked companies

The platform ships with three editorial peer groups, mostly so the Compare view
has interesting baselines out of the box. Every DAG carries a bundled-sample
fallback, so the demo works **even without API keys or successful scraping**.

| Industry                       | Companies                                                 |
|--------------------------------|-----------------------------------------------------------|
| Workplace SaaS                 | Slack, Notion, Figma, Asana, Airtable                     |
| Fintech / Payments             | **Satispay**, Nexi, Revolut, Klarna, N26                  |
| Capital Markets / Treasury     | **Murex**, Finastra, Calypso, FIS, Bloomberg              |

The list lives in two places - keep them in sync when adding a new company:

- `airflow/dags/*.py` (`TRACKED_COMPANIES`, plus the bundled `SAMPLE_*` rows)
- `dbt/models/marts/dim_companies.sql` (industry / country / founded year)

---

## Data model

### Raw layer (schema `raw`)

| Table                    | Grain                                | Source            |
|--------------------------|--------------------------------------|-------------------|
| `raw.hn_mentions`        | one row per HN story or comment      | Algolia HN Search |
| `raw.crunchbase_funding` | one row per funding round            | Crunchbase API    |
| `raw.github_activity`    | one row per repo / day snapshot      | GitHub API        |

### Marts (schema `marts`)

- `dim_companies` - static dimension: slug, name, industry, country, founded year.
- `company_health_score` - composite 0-100 score combining sentiment, funding recency, GitHub activity (joined with `dim_companies`).
- `sentiment_trend` - monthly rolling sentiment per company (for the time-series chart).
- `hiring_momentum` - derived signal from GitHub contributor growth, used as a hiring proxy.

Weighting of the health score lives in `dbt/models/marts/company_health_score.sql` - change it there, nowhere else.

---

## API cheatsheet

| Method | Path                                    | Purpose                                              |
|--------|-----------------------------------------|------------------------------------------------------|
| GET    | `/health`                               | liveness probe                                       |
| GET    | `/companies`                            | list companies (`q=` search, `industry=` filter)     |
| GET    | `/companies/search`                     | autocomplete-friendly search                         |
| GET    | `/companies/industries`                 | distinct industries (used by the Compare filter)     |
| GET    | `/companies/compare?slug=a&slug=b…`     | side-by-side breakdown for 2-5 companies             |
| GET    | `/companies/{name}/health-score`        | single 0-100 score + breakdown                       |
| GET    | `/companies/{name}/mentions`            | HN mentions + sentiment                              |
| GET    | `/companies/{name}/funding`             | funding timeline                                     |
| GET    | `/companies/{name}/sentiment-trend`     | monthly sentiment series                             |

Interactive docs at http://localhost:8000/docs (Swagger UI, auto-generated).

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `AIRFLOW_FERNET_KEY` complaint at boot | Generate a real Fernet key and put it in `.env`. |
| Streamlit shows empty charts | Trigger DAGs, then `docker compose run --rm dbt-runner dbt build`. |
| HN DAG returns 0 rows | Algolia is rate-limited or down - the DAG falls back to bundled samples and logs a warning. |
| Port already in use | Change the left-hand port in `docker-compose.yml` (e.g. `8001:8000`). |

Logs:
```bash
docker compose logs -f airflow-scheduler
docker compose logs -f fastapi-backend
```

---

## License

MIT
