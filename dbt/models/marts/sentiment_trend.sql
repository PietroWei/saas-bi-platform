{{ config(materialized='table') }}

-- ===================================================================
-- sentiment_trend
-- Monthly rolling sentiment per company, derived from HackerNews
-- mentions. Used by the company-view time-series chart
-- ("how has perception changed over time?").
-- ===================================================================

with monthly as (
    select
        company_slug,
        max(company_name)                  as company_name,
        date_trunc('month', mention_date)  as month_start,
        avg(sentiment_score)               as avg_sentiment,
        count(*)                           as mention_count,
        avg(points)                        as avg_points
    from {{ ref('stg_mentions') }}
    where mention_date is not null
    group by company_slug, date_trunc('month', mention_date)
),

with_rolling as (
    select
        company_slug,
        company_name,
        month_start::date                  as month_start,
        avg_sentiment,
        mention_count,
        avg_points,
        avg(avg_sentiment) over (
            partition by company_slug
            order by month_start
            rows between 2 preceding and current row
        )                                  as sentiment_3mo_avg
    from monthly
)

select * from with_rolling
order by company_slug, month_start
