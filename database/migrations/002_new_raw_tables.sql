-- ======================================================================
-- Migration 002: new raw landing tables for the refreshed ingestion stack.
--
-- Run this on Supabase (SQL Editor) once. Idempotent: every CREATE uses
-- IF NOT EXISTS so re-running is safe.
--
-- Tables created:
--   raw.reddit_mentions  (replaces raw.hn_mentions)
--   raw.app_reviews      (per-review rows from iOS / Android stores)
--   raw.app_ratings      (daily aggregate rating snapshot per app)
--   raw.web_signals      (weekly Google Trends + HN mention proxy)
-- ======================================================================

CREATE SCHEMA IF NOT EXISTS raw;

-- ----------------------------------------------------------------------
-- raw.reddit_mentions: one row per Reddit post mentioning a tracked company.
-- Source: https://www.reddit.com/r/{subreddit}/search.json (no auth).
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.reddit_mentions (
    id              SERIAL      PRIMARY KEY,
    company_slug    TEXT        NOT NULL,
    post_id         TEXT        NOT NULL UNIQUE,
    subreddit       TEXT        NOT NULL,
    title           TEXT,
    selftext        TEXT,
    score           INTEGER     NOT NULL DEFAULT 0,
    num_comments    INTEGER     NOT NULL DEFAULT 0,
    sentiment_score FLOAT,
    post_date       TIMESTAMP,
    url             TEXT,
    fetched_at      TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_reddit_mentions_company   ON raw.reddit_mentions (company_slug);
CREATE INDEX IF NOT EXISTS ix_reddit_mentions_subreddit ON raw.reddit_mentions (subreddit);
CREATE INDEX IF NOT EXISTS ix_reddit_mentions_date      ON raw.reddit_mentions (post_date);

COMMENT ON TABLE raw.reddit_mentions IS 'Reddit posts mentioning a tracked company. Loaded daily by reddit_dag.';

-- ----------------------------------------------------------------------
-- raw.app_reviews: one row per individual app review (iOS or Android).
-- Source: app-store-scraper, google-play-scraper.
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.app_reviews (
    id            SERIAL      PRIMARY KEY,
    company_slug  TEXT        NOT NULL,
    platform      TEXT        NOT NULL CHECK (platform IN ('ios', 'android')),
    review_id     TEXT        NOT NULL,
    rating        INTEGER     CHECK (rating BETWEEN 1 AND 5),
    title         TEXT,
    content       TEXT,
    author        TEXT,
    review_date   TIMESTAMP,
    fetched_at    TIMESTAMP   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_app_reviews UNIQUE (platform, review_id)
);

CREATE INDEX IF NOT EXISTS ix_app_reviews_company ON raw.app_reviews (company_slug);
CREATE INDEX IF NOT EXISTS ix_app_reviews_date    ON raw.app_reviews (review_date);

COMMENT ON TABLE raw.app_reviews IS 'Individual mobile app reviews (iOS + Android) for tracked companies.';

-- ----------------------------------------------------------------------
-- raw.app_ratings: aggregate rating snapshot per app per day.
-- One row per (company_slug, platform, snapshot_date).
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.app_ratings (
    id             SERIAL      PRIMARY KEY,
    company_slug   TEXT        NOT NULL,
    platform       TEXT        NOT NULL CHECK (platform IN ('ios', 'android')),
    avg_rating     FLOAT,
    total_ratings  INTEGER,
    rating_1       INTEGER,
    rating_2       INTEGER,
    rating_3       INTEGER,
    rating_4       INTEGER,
    rating_5       INTEGER,
    snapshot_date  DATE        NOT NULL DEFAULT CURRENT_DATE,
    fetched_at     TIMESTAMP   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_app_ratings UNIQUE (company_slug, platform, snapshot_date)
);

CREATE INDEX IF NOT EXISTS ix_app_ratings_company ON raw.app_ratings (company_slug);
CREATE INDEX IF NOT EXISTS ix_app_ratings_date    ON raw.app_ratings (snapshot_date);

COMMENT ON TABLE raw.app_ratings IS 'Daily aggregate ratings (avg + 1..5 distribution) per company per platform.';

-- ----------------------------------------------------------------------
-- raw.web_signals: weekly snapshot of search interest + HN mention count.
-- Source: pytrends (Google Trends), Algolia HN search as fallback proxy.
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.web_signals (
    id                SERIAL      PRIMARY KEY,
    company_slug      TEXT        NOT NULL,
    signal_date       DATE        NOT NULL,
    search_interest   INTEGER,
    hn_mention_count  INTEGER,
    fetched_at        TIMESTAMP   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_web_signals UNIQUE (company_slug, signal_date)
);

CREATE INDEX IF NOT EXISTS ix_web_signals_company ON raw.web_signals (company_slug);
CREATE INDEX IF NOT EXISTS ix_web_signals_date    ON raw.web_signals (signal_date);

COMMENT ON TABLE raw.web_signals IS 'Weekly web momentum signals: Google Trends search interest + HN mention proxy.';
