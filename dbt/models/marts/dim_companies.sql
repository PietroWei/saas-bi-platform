{{ config(materialized='table') }}

-- ===================================================================
-- dim_companies
-- Static dimension table - one row per tracked company with
-- editorial metadata (industry, country, founded year). Used to
-- group peers in the Compare view and enable industry filters.
--
-- Keep this list in sync with TRACKED_COMPANIES in the airflow DAGs.
-- ===================================================================

with raw_companies as (
    select cast(company_slug as text) as company_slug,
           cast(company_name as text) as company_name,
           cast(industry     as text) as industry,
           cast(country      as text) as country,
           cast(founded_year as int)  as founded_year
    from (
        values
            -- --- Workplace SaaS
            ('slack',     'Slack',     'Workplace SaaS',         'USA',     2009),
            ('notion',    'Notion',    'Workplace SaaS',         'USA',     2016),
            ('figma',     'Figma',     'Workplace SaaS',         'USA',     2012),
            ('asana',     'Asana',     'Workplace SaaS',         'USA',     2008),
            ('airtable',  'Airtable',  'Workplace SaaS',         'USA',     2012),
            -- --- Fintech / payments
            ('satispay',  'Satispay',  'Fintech / Payments',     'Italy',   2013),
            ('nexi',      'Nexi',      'Fintech / Payments',     'Italy',   1939),
            ('revolut',   'Revolut',   'Fintech / Payments',     'UK',      2015),
            ('klarna',    'Klarna',    'Fintech / Payments',     'Sweden',  2005),
            ('n26',       'N26',       'Fintech / Payments',     'Germany', 2013),
            -- --- Capital markets / treasury software
            ('murex',     'Murex',     'Capital Markets / Treasury', 'France', 1986),
            ('finastra',  'Finastra',  'Capital Markets / Treasury', 'UK',     2017),
            ('calypso',   'Calypso',   'Capital Markets / Treasury', 'USA',    1997),
            ('fis',       'FIS',       'Capital Markets / Treasury', 'USA',    1968),
            ('bloomberg', 'Bloomberg', 'Capital Markets / Treasury', 'USA',    1981)
    ) as t(company_slug, company_name, industry, country, founded_year)
)

select * from raw_companies
