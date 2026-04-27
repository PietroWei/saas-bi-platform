# Deploy on Railway with a Supabase database

This guide walks through hosting only the **FastAPI backend** and the
**Streamlit frontend** on Railway, with **Supabase** standing in for the
local Postgres container. Airflow and dbt stay on your machine; you do
not deploy them.

End state:

```
[ Streamlit on Railway ]  --HTTPS-->  [ FastAPI on Railway ]  --SSL-->  [ Supabase Postgres ]
```

There are seven steps. Plan on roughly thirty minutes the first time.

---

## 1. Create a free Supabase project

1. Sign up at https://supabase.com with your GitHub account.
2. From the dashboard click **New project**:
   - **Name:** `saas-bi-platform`
   - **Database password:** generate a strong one and store it in your
     password manager. You will need it in step 3.
   - **Region:** pick the one closest to your Railway region (typically
     `eu-central-1` for Europe).
3. Wait until the project finishes provisioning (about two minutes).
4. Open **Project Settings -> Database -> Connection string -> URI**
   and copy the value. It looks like:
   ```
   postgresql://postgres:YOUR_PASSWORD@db.YOUR_REF.supabase.co:5432/postgres
   ```
   This is your `DATABASE_URL`. The backend handles the
   `postgresql://` -> `postgresql+asyncpg://` conversion automatically,
   so paste it as-is into Railway later.

> **Pooler vs. direct connection.** The **Direct connection**
> (`db.YOUR_REF.supabase.co:5432`) is fine for this app. If you prefer
> Supabase's transaction pooler (`*.pooler.supabase.com:6543`), use it
> as well, the backend strips the libpq-only `sslmode` query parameter
> automatically.

---

## 2. Create the schemas and raw tables

Open the Supabase dashboard, go to **SQL Editor -> New query**, and run
the two init scripts in this order. You can copy / paste their content
directly from this repo.

### 2.1 Run `database/init/01_create_schemas.sql`

> Trim the trailing `CREATE DATABASE airflow` block, you do not need it
> on Supabase. The relevant part is:

```sql
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

COMMENT ON SCHEMA raw     IS 'Landing zone - untyped ingest from Airflow DAGs.';
COMMENT ON SCHEMA staging IS 'dbt staging views - cleaned + typed.';
COMMENT ON SCHEMA marts   IS 'dbt marts - business-ready aggregates.';
```

### 2.2 Run `database/init/02_create_raw_tables.sql`

Paste the whole file. It is idempotent (`CREATE TABLE IF NOT EXISTS`),
and creates `raw.hn_mentions`, `raw.crunchbase_funding`, and
`raw.github_activity`. They will stay empty in production, which is
fine: the demo dashboard reads from `marts.*`, not `raw.*`.

### 2.3 Create the marts tables

dbt is not deployed, so the mart tables it would normally produce must
be created by hand. Run this in the SQL editor:

```sql
-- ====================================================================
-- marts.dim_companies
-- ====================================================================
CREATE TABLE IF NOT EXISTS marts.dim_companies (
    company_slug   TEXT PRIMARY KEY,
    company_name   TEXT NOT NULL,
    industry       TEXT,
    country        TEXT,
    founded_year   INTEGER
);

-- ====================================================================
-- marts.company_health_score
-- ====================================================================
CREATE TABLE IF NOT EXISTS marts.company_health_score (
    company_slug             TEXT PRIMARY KEY,
    company_name             TEXT NOT NULL,
    industry                 TEXT,
    country                  TEXT,
    founded_year             INTEGER,
    sentiment_score_0_100    NUMERIC(6,2) NOT NULL,
    funding_score_0_100      NUMERIC(6,2) NOT NULL,
    github_score_0_100       NUMERIC(6,2) NOT NULL,
    health_score             NUMERIC(6,2) NOT NULL CHECK (health_score BETWEEN 0 AND 100),
    mention_count_180d       INTEGER,
    total_raised_usd         NUMERIC(18,2),
    last_round_date          DATE,
    total_stars              INTEGER,
    total_commits_30d        INTEGER,
    total_contributors_30d   INTEGER,
    computed_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ====================================================================
-- marts.sentiment_trend
-- ====================================================================
CREATE TABLE IF NOT EXISTS marts.sentiment_trend (
    company_slug       TEXT NOT NULL,
    company_name       TEXT NOT NULL,
    month_start        DATE NOT NULL,
    avg_sentiment      NUMERIC(6,4),
    sentiment_3mo_avg  NUMERIC(6,4),
    mention_count      INTEGER NOT NULL DEFAULT 0,
    avg_points         NUMERIC(8,2),
    PRIMARY KEY (company_slug, month_start)
);

-- ====================================================================
-- staging.stg_mentions, staging.stg_funding (used by /mentions, /funding)
-- They are normally dbt views over raw.*; we recreate them as views so
-- the API endpoints do not 500 when nothing is loaded.
-- ====================================================================
CREATE OR REPLACE VIEW staging.stg_mentions AS
SELECT
    company_name, company_slug, mention_id, mention_type,
    NULLIF(BTRIM(title), '')                AS title,
    NULLIF(BTRIM(body),  '')                AS body,
    GREATEST(COALESCE(points, 0), 0)        AS points,
    NULLIF(BTRIM(author), '')               AS author,
    mention_date, sentiment_score, source_url, ingested_at
FROM raw.hn_mentions
WHERE mention_id IS NOT NULL;

CREATE OR REPLACE VIEW staging.stg_funding AS
SELECT
    company_name, company_slug, round_id, round_type,
    announced_on, amount_usd, lead_investor, investors,
    COALESCE(currency, 'USD') AS currency, source_url, ingested_at
FROM raw.crunchbase_funding;
```

---

## 3. Seed the marts with sample data

This is what makes the dashboard render something meaningful without
running Airflow + dbt. Run the three blocks below in the Supabase SQL
editor.

### 3.1 `marts.dim_companies` (15 companies, three peer groups)

```sql
INSERT INTO marts.dim_companies (company_slug, company_name, industry, country, founded_year) VALUES
  -- Workplace SaaS
  ('slack',     'Slack',     'Workplace SaaS',             'USA',     2009),
  ('notion',    'Notion',    'Workplace SaaS',             'USA',     2016),
  ('figma',     'Figma',     'Workplace SaaS',             'USA',     2012),
  ('asana',     'Asana',     'Workplace SaaS',             'USA',     2008),
  ('airtable',  'Airtable',  'Workplace SaaS',             'USA',     2012),
  -- Fintech / Payments (Satispay peer group)
  ('satispay',  'Satispay',  'Fintech / Payments',         'Italy',   2013),
  ('nexi',      'Nexi',      'Fintech / Payments',         'Italy',   1939),
  ('revolut',   'Revolut',   'Fintech / Payments',         'UK',      2015),
  ('klarna',    'Klarna',    'Fintech / Payments',         'Sweden',  2005),
  ('n26',       'N26',       'Fintech / Payments',         'Germany', 2013),
  -- Capital Markets / Treasury (Murex peer group)
  ('murex',     'Murex',     'Capital Markets / Treasury', 'France',  1986),
  ('finastra',  'Finastra',  'Capital Markets / Treasury', 'UK',      2017),
  ('calypso',   'Calypso',   'Capital Markets / Treasury', 'USA',     1997),
  ('fis',       'FIS',       'Capital Markets / Treasury', 'USA',     1968),
  ('bloomberg', 'Bloomberg', 'Capital Markets / Treasury', 'USA',     1981)
ON CONFLICT (company_slug) DO NOTHING;
```

### 3.2 `marts.company_health_score`

```sql
INSERT INTO marts.company_health_score (
    company_slug, company_name, industry, country, founded_year,
    sentiment_score_0_100, funding_score_0_100, github_score_0_100, health_score,
    mention_count_180d, total_raised_usd, last_round_date,
    total_stars, total_commits_30d, total_contributors_30d
) VALUES
  ('slack',     'Slack',     'Workplace SaaS',             'USA',     2009, 75.0, 82.0, 78.0, 78.4, 142, 1400000000, '2018-08-21', 28000, 220, 35),
  ('notion',    'Notion',    'Workplace SaaS',             'USA',     2016, 70.0, 80.0, 68.0, 72.9, 110,  343000000, '2021-10-08', 14500, 180, 28),
  ('figma',     'Figma',     'Workplace SaaS',             'USA',     2012, 82.0, 85.0, 76.0, 81.1, 198,  333000000, '2021-06-24', 22000, 260, 42),
  ('asana',     'Asana',     'Workplace SaaS',             'USA',     2008, 55.0, 70.0, 60.0, 61.3,  64,  213000000, '2018-11-05',  9000, 140, 22),
  ('airtable',  'Airtable',  'Workplace SaaS',             'USA',     2012, 64.0, 78.0, 52.0, 64.8,  88, 1360000000, '2021-12-14',  6200,  95, 14),
  ('satispay',  'Satispay',  'Fintech / Payments',         'Italy',   2013, 78.0, 76.0, 70.0, 74.6,  96,  422000000, '2025-09-18',  4800, 165, 24),
  ('nexi',      'Nexi',      'Fintech / Payments',         'Italy',   1939, 48.0, 62.0, 56.0, 55.2,  41,          0, NULL,         1100,  60,  9),
  ('revolut',   'Revolut',   'Fintech / Payments',         'UK',      2015, 77.0, 84.0, 67.0, 76.0, 156, 1700000000, '2021-07-15',  7400, 195, 31),
  ('klarna',    'Klarna',    'Fintech / Payments',         'Sweden',  2005, 62.0, 80.0, 55.0, 65.7,  87, 3700000000, '2022-07-11',  3300, 110, 17),
  ('n26',       'N26',       'Fintech / Payments',         'Germany', 2013, 55.0, 72.0, 50.0, 58.9,  55, 1700000000, '2021-10-18',  2800,  80, 12),
  ('murex',     'Murex',     'Capital Markets / Treasury', 'France',  1986, 70.0, 60.0, 75.0, 68.4,  38,          0, NULL,         5200, 240, 38),
  ('finastra',  'Finastra',  'Capital Markets / Treasury', 'UK',      2017, 50.0, 58.0, 47.0, 51.8,  22,          0, NULL,          900,  55, 10),
  ('calypso',   'Calypso',   'Capital Markets / Treasury', 'USA',     1997, 56.0, 60.0, 53.0, 56.3,  28,          0, NULL,         1600,  70, 12),
  ('fis',       'FIS',       'Capital Markets / Treasury', 'USA',     1968, 60.0, 64.0, 56.0, 60.1,  31,          0, NULL,         2100,  90, 16),
  ('bloomberg', 'Bloomberg', 'Capital Markets / Treasury', 'USA',     1981, 86.0, 88.0, 80.0, 84.7, 287,          0, NULL,        18000, 320, 55)
ON CONFLICT (company_slug) DO UPDATE SET
    sentiment_score_0_100 = EXCLUDED.sentiment_score_0_100,
    funding_score_0_100   = EXCLUDED.funding_score_0_100,
    github_score_0_100    = EXCLUDED.github_score_0_100,
    health_score          = EXCLUDED.health_score,
    mention_count_180d    = EXCLUDED.mention_count_180d,
    total_raised_usd      = EXCLUDED.total_raised_usd,
    last_round_date       = EXCLUDED.last_round_date,
    total_stars           = EXCLUDED.total_stars,
    total_commits_30d     = EXCLUDED.total_commits_30d,
    total_contributors_30d = EXCLUDED.total_contributors_30d,
    computed_at           = now();
```

### 3.3 `marts.sentiment_trend` (12 months per company)

```sql
WITH months AS (
    SELECT generate_series(
        date_trunc('month', current_date) - interval '11 month',
        date_trunc('month', current_date),
        interval '1 month'
    )::date AS month_start
),
companies AS (
    SELECT company_slug, company_name, sentiment_score_0_100
    FROM marts.company_health_score
)
INSERT INTO marts.sentiment_trend (
    company_slug, company_name, month_start,
    avg_sentiment, sentiment_3mo_avg, mention_count, avg_points
)
SELECT
    c.company_slug,
    c.company_name,
    m.month_start,
    -- map [0..100] back to [-1..+1] with a small monthly wobble
    GREATEST(-1.0, LEAST(1.0,
        round(((c.sentiment_score_0_100 - 50) / 50.0
            + ((extract(month from m.month_start)::int % 5 - 2) * 0.05))::numeric, 4)
    ))                                                          AS avg_sentiment,
    -- 3-month rolling average computed downstream by the chart, store the same
    GREATEST(-1.0, LEAST(1.0,
        round(((c.sentiment_score_0_100 - 50) / 50.0)::numeric, 4)
    ))                                                          AS sentiment_3mo_avg,
    GREATEST(1, (50 + (extract(month from m.month_start)::int % 7) * 3))::int AS mention_count,
    round((40 + c.sentiment_score_0_100 / 4
            + (extract(month from m.month_start)::int % 4) * 3)::numeric, 2)  AS avg_points
FROM companies c CROSS JOIN months m
ON CONFLICT (company_slug, month_start) DO NOTHING;
```

After this, the API endpoints used by the three dashboard views are
fully populated:

| View          | Source mart / view                                |
|---------------|---------------------------------------------------|
| Landing       | `marts.company_health_score`                      |
| Company View  | `marts.company_health_score`, `marts.sentiment_trend` |
| Analyst View  | same plus `staging.stg_funding` (empty, frontend handles) |
| Compare View  | `marts.company_health_score`, `marts.sentiment_trend` |

The frontend has a bundled-sample fallback for endpoints that come back
empty, so even if a step in section 3 is skipped the dashboard still
renders.

---

## 4. Connect the GitHub repo to Railway

1. Sign up at https://railway.app (free hobby plan is enough).
2. Click **New Project -> Deploy from GitHub repo** and authorise the
   GitHub app to read the repo.
3. Pick `saas-bi-platform`. Railway will detect the root `railway.json`.

You now need to add **two services**, one per Dockerfile.

### 4.1 Add the FastAPI service

1. In the project, click **+ New -> GitHub Repo -> saas-bi-platform**.
2. Open the new service and go to **Settings**:
   - **Service name:** `fastapi-backend`
   - **Root Directory:** `backend`
   - **Builder:** Dockerfile (auto-detected)
   - **Dockerfile path:** `Dockerfile` (relative to `backend/`)
   - **Start command:** leave blank, the Dockerfile `CMD` already
     honours `$PORT`.
3. Under **Networking** click **Generate Domain**. Copy the public URL,
   you will paste it into the frontend service in step 6.

### 4.2 Add the Streamlit service

1. Click **+ New -> GitHub Repo -> saas-bi-platform** again.
2. Settings:
   - **Service name:** `streamlit-frontend`
   - **Root Directory:** `frontend`
   - **Builder:** Dockerfile
   - **Dockerfile path:** `Dockerfile`
3. **Networking -> Generate Domain.** This is the public URL you will
   share. Streamlit needs no extra config to listen on `$PORT`, the
   Dockerfile passes it through.

> If you prefer Railway's native multi-service config, the root
> `railway.json` declares both services with their Dockerfile paths.
> You can also manage them entirely from the dashboard, both ways
> work.

---

## 5. Set environment variables on each service

Open each service in Railway, go to **Variables**, and add the keys
below. Click **Deploy** after each save.

### 5.1 `fastapi-backend`

| Variable        | Value                                                                 |
|-----------------|-----------------------------------------------------------------------|
| `DATABASE_URL`  | The Supabase URI from step 1.4 (`postgresql://...supabase.co:5432/postgres`) |
| `APP_ENV`       | `production`                                                          |
| `LOG_LEVEL`     | `INFO`                                                                |
| `CORS_ORIGINS`  | The Streamlit public URL from step 4.2 (no trailing slash)            |

> Tip: never commit the Supabase password to the repo. Railway
> Variables are encrypted at rest and exposed to the container as env
> vars only, which is exactly what `Settings` reads.

### 5.2 `streamlit-frontend`

| Variable          | Value                                                              |
|-------------------|--------------------------------------------------------------------|
| `BACKEND_URL`     | The FastAPI public URL from step 4.1 (no trailing slash, https://) |
| `USE_SAMPLE_DATA` | `false` (omit, or set to `true` to force bundled samples)          |

---

## 6. Link the two services

Hardcoding the FastAPI URL in `BACKEND_URL` works. The cleaner pattern
on Railway is to use a **reference variable** so the frontend always
points at the latest backend domain even after redeploys:

1. Open `streamlit-frontend -> Variables`.
2. Click **+ New Variable -> Add Reference**.
3. Pick `fastapi-backend` -> `RAILWAY_PUBLIC_DOMAIN`.
4. Save the reference into a helper variable, e.g.
   `FASTAPI_DOMAIN = ${{fastapi-backend.RAILWAY_PUBLIC_DOMAIN}}`.
5. Set `BACKEND_URL = https://${{fastapi-backend.RAILWAY_PUBLIC_DOMAIN}}`.

Railway re-injects the value at deploy time, so promoting a new backend
domain is zero-touch on the frontend.

---

## 7. Verify the deploy

### 7.1 Backend health

Open the FastAPI public URL with `/health` appended:

```
https://fastapi-backend-production.up.railway.app/health
```

You should see:

```json
{ "status": "ok", "env": "production", "database": "ok" }
```

If `database` is `unreachable`, the backend is up but cannot reach
Supabase. Most common causes:
- Wrong `DATABASE_URL` (typo, wrong project ref, wrong password).
- Supabase project paused (free projects pause after a week of
  inactivity, click **Restore** in the dashboard).
- Network egress blocked, redeploy the service.

### 7.2 Backend data

Hit the `/companies` endpoint to confirm the seed worked:

```
https://fastapi-backend-production.up.railway.app/companies?limit=5
```

Expect a JSON array with five rows from `marts.company_health_score`.

### 7.3 Frontend dashboards

Open the Streamlit URL. You should see, in order:

1. **Landing** with a top-3 leaderboard and the 15-row table.
2. **Company View** -> sidebar -> pick `Satispay`, the gauge and
   sentiment trend should render.
3. **Analyst View** -> type `murex`, pick the result, the dossier
   should render with red-flag warnings.
4. **Compare View** -> pick `Satispay`, `Nexi`, `Revolut` -> the radar
   should overlay three traces.

If the Landing page shows the bundled-sample data even though the API
should be live, double-check `BACKEND_URL` on the Streamlit service:
it must point at the FastAPI public URL with `https://` and no
trailing slash. The sidebar of the Landing page prints the resolved
backend URL for quick troubleshooting.

---

## Local development is unaffected

- `docker compose up --build` keeps working: the backend defaults to
  the local `bi_user@postgres:5432/bi_platform` URL, the frontend
  defaults to `http://localhost:8000`, and the Dockerfiles fall back
  to ports 8000 / 8501 when `$PORT` is not set.
- `airflow/` and `dbt/` are not deployed and continue to run locally
  against the Postgres container as before.

---

## Cost notes

- Supabase free tier: 500 MB database, 2 projects, projects pause
  after 7 days of inactivity. Plenty for this demo.
- Railway hobby plan: 5 USD trial credit each month, both services
  fit comfortably within it for an interview-grade demo.
