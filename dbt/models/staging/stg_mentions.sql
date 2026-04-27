{{ config(materialized='view') }}

-- Cleaned, typed view over raw.hn_mentions.
-- - trims text
-- - clamps points to >= 0
-- - keeps sentiment score as-is ([-1, 1])
with source as (
    select * from {{ source('raw', 'hn_mentions') }}
),

cleaned as (
    select
        company_name,
        company_slug,
        mention_id,
        mention_type,
        nullif(trim(title), '')                                          as title,
        nullif(trim(body),  '')                                          as body,
        greatest(coalesce(points, 0), 0)                                 as points,
        nullif(trim(author), '')                                         as author,
        mention_date,
        sentiment_score,
        source_url,
        ingested_at
    from source
    where mention_id is not null
)

select * from cleaned
