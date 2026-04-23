{{ config(materialized='view') }}

-- Cleaned view over raw.crunchbase_funding.
with source as (
    select * from {{ source('raw', 'crunchbase_funding') }}
),

cleaned as (
    select
        company_name,
        company_slug,
        round_id,
        nullif(trim(round_type), '')     as round_type,
        announced_on,
        case
            when amount_usd > 0 then amount_usd
            else null
        end                              as amount_usd,
        nullif(trim(lead_investor), '')  as lead_investor,
        nullif(trim(investors), '')      as investors,
        coalesce(nullif(trim(currency), ''), 'USD') as currency,
        source_url,
        ingested_at
    from source
    where announced_on is not null
),

ranked as (
    select
        *,
        row_number() over (
            partition by company_slug, round_id
            order by ingested_at desc
        ) as rn
    from cleaned
)

-- de-dupe multiple ingests of the same round
select
    company_name, company_slug, round_id, round_type, announced_on,
    amount_usd, lead_investor, investors, currency, source_url, ingested_at
from ranked
where rn = 1
