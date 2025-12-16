SELECT
    id as coin_id,
    symbol,
    current_price as price_usd,
    market_cap,
    CAST(loaded_at as DATE) as report_date
FROM {{ source('crypto_raw', 'daily_market') }}
-- This deduplicates data if we ran the script twice in one day
QUALIFY ROW_NUMBER() OVER (PARTITION BY id, CAST(loaded_at as DATE) ORDER BY loaded_at DESC) = 1