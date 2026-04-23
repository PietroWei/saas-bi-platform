{{ config(materialized='table') }}

-- ===================================================================
-- company_health_score
-- Produces a 0-100 health score per tracked company by blending:
--   * sentiment      — mean of review sentiment in the last 180 days
--   * funding        — recency-decayed sum of last funding rounds
--   * github         — commits + contributors + star log, last snapshot
-- Weights live in dbt_project.yml so they can be tuned per env.
-- ===================================================================

with sentiment as (
    select
        company_slug,
        max(company_name)                       as company_name,
        avg(sentiment_score) filter (
            where review_date >= current_date - interval '180 day'
        )                                       as avg_sentiment_180d,
        count(*) filter (
            where review_date >= current_date - interval '180 day'
        )                                       as review_count_180d
    from {{ ref('stg_reviews') }}
    group by company_slug
),

funding as (
    select
        company_slug,
        max(announced_on)                       as last_round_date,
        sum(amount_usd)                         as total_raised_usd,
        count(*)                                as num_rounds
    from {{ ref('stg_funding') }}
    where announced_on >= current_date - interval '{{ var("funding_recency_years") }} year'
    group by company_slug
),

github_latest as (
    -- one snapshot per repo (the most recent) then aggregate to company
    select distinct on (repo_full_name)
        company_slug,
        repo_full_name,
        stars,
        commits_last_30d,
        contributors_30d
    from {{ ref('stg_github') }}
    order by repo_full_name, snapshot_date desc
),

github as (
    select
        company_slug,
        sum(stars)            as total_stars,
        sum(commits_last_30d) as total_commits_30d,
        sum(contributors_30d) as total_contributors_30d
    from github_latest
    group by company_slug
),

companies as (
    select company_slug from sentiment
    union select company_slug from funding
    union select company_slug from github
),

scored as (
    select
        c.company_slug,
        coalesce(s.company_name, c.company_slug) as company_name,

        -- sentiment component: map [-1, 1] → [0, 100]. null → 50 (neutral).
        round(
            coalesce((s.avg_sentiment_180d + 1) * 50.0, 50.0)::numeric,
            2
        )                                        as sentiment_score_0_100,

        -- funding component: log-scaled amount + recency boost (capped 100).
        round(
            least(
                100.0,
                coalesce(ln(greatest(f.total_raised_usd, 1)) * 5.0, 0)
                + case
                    when f.last_round_date is null then 0
                    when f.last_round_date >= current_date - interval '1 year'  then 20
                    when f.last_round_date >= current_date - interval '2 year'  then 10
                    else 0
                  end
            )::numeric,
            2
        )                                        as funding_score_0_100,

        -- github component: weighted blend of commits, contributors, log(stars).
        round(
            least(
                100.0,
                coalesce(g.total_commits_30d, 0) * 0.5
              + coalesce(g.total_contributors_30d, 0) * 4.0
              + coalesce(ln(greatest(g.total_stars, 1)) * 3.0, 0)
            )::numeric,
            2
        )                                        as github_score_0_100,

        s.review_count_180d,
        f.total_raised_usd,
        f.last_round_date,
        g.total_stars,
        g.total_commits_30d,
        g.total_contributors_30d
    from companies c
    left join sentiment s using (company_slug)
    left join funding   f using (company_slug)
    left join github    g using (company_slug)
)

select
    company_slug,
    company_name,
    sentiment_score_0_100,
    funding_score_0_100,
    github_score_0_100,
    round((
        sentiment_score_0_100 * {{ var('sentiment_weight') }}
      + funding_score_0_100   * {{ var('funding_weight')   }}
      + github_score_0_100    * {{ var('github_weight')    }}
    )::numeric, 2)                               as health_score,
    review_count_180d,
    total_raised_usd,
    last_round_date,
    total_stars,
    total_commits_30d,
    total_contributors_30d,
    current_timestamp                            as computed_at
from scored
