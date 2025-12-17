-- SELECT
--     id as coin_id,
--     symbol,
--     current_price as price_usd,
--     market_cap,
--     CAST(loaded_at as DATE) as report_date
-- FROM {{ source('crypto_raw', 'daily_market') }}
-- -- This deduplicates data if we ran the script twice in one day
-- QUALIFY ROW_NUMBER() OVER (PARTITION BY id, CAST(loaded_at as DATE) ORDER BY loaded_at DESC) = 1



{{
    config(
        materialized='view'
    )
}}

SELECT
    id as coin_id,
    symbol,
    name,
    current_price as price_usd,
    market_cap,
    total_volume, -- <--- Added this column!
    last_updated,
    CAST(loaded_at as DATE) as report_date
FROM {{ source('crypto_raw', 'daily_market') }}
-- We remove the incremental logic because Sandbox doesn't support it
QUALIFY ROW_NUMBER() OVER (PARTITION BY id, CAST(loaded_at as DATE) ORDER BY loaded_at DESC) = 1