-- ======================================================================
-- SaaS BI Platform - raw landing tables
-- One table per upstream source. Natural keys are enforced with UNIQUE
-- constraints so that Airflow loaders can safely use ON CONFLICT DO NOTHING.
-- ======================================================================

-- --------------------------------------------------------------------
-- raw.hn_mentions - HackerNews mentions (stories + comments) per company
-- Source: Algolia HN Search API (free, no auth).
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.hn_mentions (
    id              BIGSERIAL PRIMARY KEY,
    company_name    TEXT        NOT NULL,
    company_slug    TEXT        NOT NULL,
    mention_id      TEXT        NOT NULL,                  -- HN objectID
    mention_type    TEXT        NOT NULL CHECK (mention_type IN ('story', 'comment')),
    title           TEXT,                                  -- story title (nullable for comments)
    body            TEXT,                                  -- story_text or comment_text (HTML stripped)
    points          INTEGER     NOT NULL DEFAULT 0,        -- HN upvotes (comments are 0)
    author          TEXT,
    mention_date    DATE,
    sentiment_score NUMERIC(5,4),
    source_url      TEXT,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_hn_mentions UNIQUE (company_slug, mention_id)
);

CREATE INDEX IF NOT EXISTS ix_hn_mentions_company ON raw.hn_mentions (company_slug);
CREATE INDEX IF NOT EXISTS ix_hn_mentions_date    ON raw.hn_mentions (mention_date);

COMMENT ON TABLE raw.hn_mentions IS 'One row per HackerNews story or comment mentioning a tracked company.';

-- --------------------------------------------------------------------
-- raw.crunchbase_funding - funding rounds
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.crunchbase_funding (
    id              BIGSERIAL PRIMARY KEY,
    company_name    TEXT        NOT NULL,
    company_slug    TEXT        NOT NULL,
    round_id        TEXT        NOT NULL,
    round_type      TEXT,
    announced_on    DATE,
    amount_usd      NUMERIC(18,2),
    lead_investor   TEXT,
    investors       TEXT,
    currency        TEXT DEFAULT 'USD',
    source_url      TEXT,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_cb_funding UNIQUE (company_slug, round_id)
);

CREATE INDEX IF NOT EXISTS ix_cb_funding_company ON raw.crunchbase_funding (company_slug);
CREATE INDEX IF NOT EXISTS ix_cb_funding_date    ON raw.crunchbase_funding (announced_on);

COMMENT ON TABLE raw.crunchbase_funding IS 'One row per funding round for tracked companies.';

-- --------------------------------------------------------------------
-- raw.github_activity - daily repo snapshot
-- --------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.github_activity (
    id                BIGSERIAL PRIMARY KEY,
    company_name      TEXT        NOT NULL,
    company_slug      TEXT        NOT NULL,
    org_login         TEXT        NOT NULL,
    repo_full_name    TEXT        NOT NULL,
    snapshot_date     DATE        NOT NULL,
    stars             INTEGER,
    forks             INTEGER,
    open_issues       INTEGER,
    watchers          INTEGER,
    commits_last_30d  INTEGER,
    contributors_30d  INTEGER,
    primary_language  TEXT,
    source_url        TEXT,
    ingested_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_gh_activity UNIQUE (repo_full_name, snapshot_date)
);

CREATE INDEX IF NOT EXISTS ix_gh_activity_company ON raw.github_activity (company_slug);
CREATE INDEX IF NOT EXISTS ix_gh_activity_date    ON raw.github_activity (snapshot_date);

COMMENT ON TABLE raw.github_activity IS 'Daily snapshot of GitHub repo health per tracked company.';
