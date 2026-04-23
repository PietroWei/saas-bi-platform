{{ config(materialized='view') }}

-- Cleaned view over raw.github_activity.
-- - one row per (repo, snapshot_date)
-- - non-negative int coercions already enforced at ingest, but we gate again
with source as (
    select * from {{ source('raw', 'github_activity') }}
),

cleaned as (
    select
        company_name,
        company_slug,
        org_login,
        repo_full_name,
        snapshot_date,
        greatest(coalesce(stars,            0), 0) as stars,
        greatest(coalesce(forks,            0), 0) as forks,
        greatest(coalesce(open_issues,      0), 0) as open_issues,
        greatest(coalesce(watchers,         0), 0) as watchers,
        greatest(coalesce(commits_last_30d, 0), 0) as commits_last_30d,
        greatest(coalesce(contributors_30d, 0), 0) as contributors_30d,
        nullif(trim(primary_language), '') as primary_language,
        source_url,
        ingested_at
    from source
)

select * from cleaned
