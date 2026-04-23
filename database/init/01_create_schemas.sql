-- ======================================================================
-- SaaS BI Platform — schema bootstrap
-- Executed automatically by the postgres image on first boot.
-- ======================================================================

-- Analytical schemas.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

COMMENT ON SCHEMA raw     IS 'Landing zone — untyped ingest from Airflow DAGs.';
COMMENT ON SCHEMA staging IS 'dbt staging views — cleaned + typed.';
COMMENT ON SCHEMA marts   IS 'dbt marts — business-ready aggregates.';

-- Airflow needs its own logical database (not just a schema) so that its
-- metadata migrations can own the public schema without colliding with ours.
-- We create it here, idempotently, using a DO block.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = current_setting('app.airflow_db', true)) THEN
        -- fallback to default name if GUC is not set
        PERFORM 1;
    END IF;
END $$;

-- Simpler, robust path: create the Airflow DB unconditionally if it doesn't exist.
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')
\gexec
