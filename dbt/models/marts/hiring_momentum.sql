{{ config(materialized='table') }}

-- ===================================================================
-- hiring_momentum
-- Proxy for hiring velocity using GitHub contributor growth.
-- For each company, compares 30-day contributor count now vs. 60-day ago
-- snapshot; positive delta = likely headcount growth.
-- ===================================================================

with per_day as (
    select
        company_slug,
        max(company_name)            as company_name,
        snapshot_date,
        sum(contributors_30d)        as contributors_30d,
        sum(commits_last_30d)        as commits_30d,
        sum(stars)                   as stars
    from {{ ref('stg_github') }}
    group by company_slug, snapshot_date
),

latest as (
    select distinct on (company_slug)
        company_slug, company_name,
        snapshot_date, contributors_30d, commits_30d, stars
    from per_day
    order by company_slug, snapshot_date desc
),

prior as (
    select distinct on (company_slug)
        company_slug,
        snapshot_date          as prior_snapshot_date,
        contributors_30d       as contributors_30d_prior,
        commits_30d            as commits_30d_prior
    from per_day
    where snapshot_date <= current_date - interval '60 day'
    order by company_slug, snapshot_date desc
)

select
    l.company_slug,
    l.company_name,
    l.snapshot_date,
    l.contributors_30d,
    l.commits_30d,
    l.stars,
    p.contributors_30d_prior,
    p.commits_30d_prior,
    coalesce(l.contributors_30d - p.contributors_30d_prior, 0)   as contributor_delta,
    coalesce(l.commits_30d       - p.commits_30d_prior,       0) as commit_delta,
    case
        when p.contributors_30d_prior is null                    then 'unknown'
        when l.contributors_30d > p.contributors_30d_prior * 1.1 then 'hiring_up'
        when l.contributors_30d < p.contributors_30d_prior * 0.9 then 'hiring_down'
        else 'steady'
    end                                                          as momentum_flag
from latest l
left join prior p using (company_slug)
