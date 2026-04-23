{{ config(materialized='view') }}

-- Cleaned, typed view over raw.g2_reviews.
-- - trims text
-- - coerces rating into [1,5]
-- - keeps sentiment score as-is ([-1, 1])
with source as (
    select * from {{ source('raw', 'g2_reviews') }}
),

cleaned as (
    select
        company_name,
        company_slug,
        review_id,
        nullif(trim(review_title), '')                                   as review_title,
        nullif(trim(review_body),  '')                                   as review_body,
        case
            when rating between 1 and 5 then rating
            else null
        end                                                              as rating,
        nullif(trim(reviewer_role), '')                                  as reviewer_role,
        nullif(trim(reviewer_size), '')                                  as reviewer_size,
        review_date,
        sentiment_score,
        source_url,
        ingested_at
    from source
    where review_id is not null
)

select * from cleaned
