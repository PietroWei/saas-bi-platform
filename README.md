# SaaS BI Platform

A self-hosted Business Intelligence platform that aggregates **public signals about SaaS, fintech and capital-markets vendors** (Reddit posts, mobile app reviews, Google Trends search interest) and turns them into three views:

- **Company view** (*lato azienda*): "how is my company perceived?"
- **Analyst view** (*lato analyst*): "is this company healthy? should I bet on it?"
- **Compare view**: side-by-side radar comparison across 2-5 peers (e.g. Satispay vs. Nexi vs. Revolut, or Murex vs. Calypso vs. Finastra).

The whole stack comes up with a single `docker compose up --build`.

## Dashboards

> Screenshots live in [`docs/screenshots/`](docs/screenshots/). If the images
> below don't render, the local stack hasn't been captured yet, see
> [`docs/screenshots/README.md`](docs/screenshots/README.md) for how to
> regenerate them.

### Landing, top performers + industry filter
![Landing](docs/screenshots/landing.png)

### Company View, single-company gauge + sentiment trend
![Company View](docs/screenshots/company_view.png)

### Analyst View, full due-diligence dossier
![Analyst View](docs/screenshots/analyst_view.png)

### Compare View, radar chart across 2-5 peers
![Compare View](docs/screenshots/compare_view.png)

---

## Architecture

```
+------------+   +-----------+   +----------+   +----------+   +------------+
|  Airflow   |-->| Postgres  |-->|   dbt    |-->| FastAPI  |-->| Streamlit  |
|  (ingest)  |   |  raw.*    |   | marts.*  |   |  :8000   |   |   :8501    |
+------------+   +-----------+   +----------+   +----------+   +------------+
```

| Service             | Role                                  | Port  |
|---------------------|---------------------------------------|-------|
| `postgres`          | Data warehouse (schemas `raw`, `marts`) | 5432  |
| `airflow-webserver` | DAG UI                                | 8080  |
| `airflow-scheduler` | DAG execution                         | -     |
| `dbt-runner`        | Transformations on demand             | -     |
| `fastapi-backend`   | REST API                              | 8000  |
| `streamlit-frontend`| Dashboards                            | 8501  |

Data flow: **DAGs** write to `raw.*` tables, **dbt** materializes `staging` views and `marts` tables, **FastAPI** serves JSON from `marts`, **Streamlit** consumes the API.

The target database for production is **Supabase Postgres**. DAGs read the connection URL from the `DATABASE_URL` env var.

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

Set `DATABASE_URL` in `.env` to your Supabase connection string (Project Settings, Database, Connection string, URI):

```
DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@db.YOUR_REF.supabase.co:5432/postgres
```

### 2. Create the new raw tables on Supabase

Run, in the Supabase SQL Editor:

1. `database/init/01_create_schemas.sql`
2. `database/init/02_create_raw_tables.sql` (legacy raw layer, optional)
3. `database/migrations/002_new_raw_tables.sql` (the four new tables used by the current DAGs)

### 3. Start the stack

```bash
docker compose up --build
```

First boot takes a couple of minutes (image builds, Airflow metadata init).

| URL                          | Credentials        |
|------------------------------|--------------------|
| http://localhost:8080        | `admin` / `admin`  |
| http://localhost:8000/docs   | -                  |
| http://localhost:8501        | -                  |

### 4. Trigger the pipelines

In Airflow, unpause and trigger:

1. `reddit_dag`        (daily, Reddit JSON API + TextBlob sentiment)
2. `appstore_dag`      (daily, iOS + Android reviews and ratings)
3. `web_signals_dag`   (weekly, Google Trends + HN mention proxy)

Then run the transformations:

```bash
docker compose run --rm dbt-runner dbt build
```

Open Streamlit at http://localhost:8501, the dashboards will populate.

---

## Local development without Docker

A conda environment is provided (Python 3.12):

```bash
conda env create -f environment.yml
conda activate saas_bi
```

```bash
# Run the API alone against a local Postgres
uvicorn backend.main:app --reload --port 8000

# Render Streamlit alone
streamlit run frontend/app/main.py

# Parse a DAG file for syntax errors
python airflow/dags/reddit_dag.py
```

---

## Project structure

```
saas-bi-platform/
├── docker-compose.yml
├── .env.example
├── environment.yml
├── README.md
├── CLAUDE.md
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       ├── reddit_dag.py        Reddit JSON API + TextBlob sentiment
│       ├── appstore_dag.py      app-store-scraper + google-play-scraper
│       └── web_signals_dag.py   pytrends Google Trends + HN proxy
├── dbt/
│   ├── Dockerfile
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       └── marts/
├── backend/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py
│   └── routers/
├── frontend/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
├── database/
│   ├── init/
│   │   ├── 01_create_schemas.sql
│   │   └── 02_create_raw_tables.sql
│   └── migrations/
│       └── 002_new_raw_tables.sql   reddit_mentions, app_reviews, app_ratings, web_signals
└── docs/
    └── screenshots/
```

---

## Tracked companies

The platform ships with three editorial peer groups, mostly so the Compare view
has interesting baselines out of the box. Every DAG carries a `SAMPLE_DATA`
fallback, so the demo works **even without API keys or successful scraping**.

| Industry                       | Companies                                                 |
|--------------------------------|-----------------------------------------------------------|
| Workplace SaaS                 | Slack, Notion, Figma, Asana, Airtable                     |
| Fintech / Payments             | **Satispay**, Nexi, Revolut, Klarna, N26                  |
| Capital Markets / Treasury     | **Murex**, Finastra, Calypso, FIS, Bloomberg              |

The list lives in two places, keep them in sync when adding a new company:

- `airflow/dags/*.py` (`TRACKED_COMPANIES`, plus the bundled `SAMPLE_DATA` rows)
- `dbt/models/marts/dim_companies.sql` (industry / country / founded year)

---

## Data model

### Raw layer (schema `raw`)

| Table                  | Grain                                       | Source                                |
|------------------------|---------------------------------------------|---------------------------------------|
| `raw.reddit_mentions`  | one row per Reddit post                     | reddit.com JSON search                |
| `raw.app_reviews`      | one row per iOS / Android review            | app-store-scraper, google-play-scraper |
| `raw.app_ratings`      | one row per (company, platform, day)        | app-store-scraper, google-play-scraper |
| `raw.web_signals`      | one row per (company, month)                | pytrends + Algolia HN search          |

### Marts (schema `marts`)

- `dim_companies`: static dimension (slug, name, industry, country, founded year).
- `company_health_score`: composite 0-100 score combining sentiment, app rating signals and search momentum.
- `sentiment_trend`: monthly rolling sentiment per company (used by the time-series chart).

Weighting of the health score lives in `dbt/models/marts/company_health_score.sql`, change it there, nowhere else.

---

## API cheatsheet

| Method | Path                                    | Purpose                                              |
|--------|-----------------------------------------|------------------------------------------------------|
| GET    | `/health`                               | liveness probe                                       |
| GET    | `/companies`                            | list companies (`q=` search, `industry=` filter)     |
| GET    | `/companies/search`                     | autocomplete-friendly search                         |
| GET    | `/companies/industries`                 | distinct industries (used by the Compare filter)     |
| GET    | `/companies/compare?slug=a&slug=b...`   | side-by-side breakdown for 2-5 companies             |
| GET    | `/companies/{name}/health-score`        | single 0-100 score + breakdown                       |
| GET    | `/companies/{name}/mentions`            | Reddit mentions + sentiment                          |
| GET    | `/companies/{name}/sentiment-trend`     | monthly sentiment series                             |

Interactive docs at http://localhost:8000/docs.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `AIRFLOW_FERNET_KEY` complaint at boot | Generate a real Fernet key and put it in `.env`. |
| Streamlit shows empty charts | Trigger DAGs, then `docker compose run --rm dbt-runner dbt build`. |
| Reddit DAG returns 0 rows | Reddit is rate-limited or down, the DAG falls back to `SAMPLE_DATA` and logs a warning. |
| App Store DAG gets 0 reviews for a slug | The company has no consumer app (B2B vendor), expected. |
| Google Trends 429 | pytrends rate-limited, the DAG falls back to bundled monthly samples. |
| Port already in use | Change the left-hand port in `docker-compose.yml` (e.g. `8001:8000`). |

Logs:
```bash
docker compose logs -f airflow-scheduler
docker compose logs -f fastapi-backend
```

---

## License

MIT
