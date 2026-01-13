{{ config(materialized='view') }}

SELECT
    -- Identifiers
    id as coin_id,
    symbol,
    name,
    image as logo_url,
    market_cap_rank as rank,

    -- Price Metrics
    current_price as price_usd,
    high_24h,
    low_24h,
    price_change_percentage_24h as change_24h_pct,
    ath as all_time_high,
    ath_change_percentage as drawdown_pct,

    -- Volume & Supply
    market_cap,
    total_volume,
    circulating_supply,
    total_supply,

    -- Metadata
    last_updated as source_updated_at,
    CAST(loaded_at as DATE) as report_date

FROM {{ source('crypto_raw', 'daily_market') }}

-- Deduplication
QUALIFY ROW_NUMBER() OVER (PARTITION BY id, CAST(loaded_at as DATE) ORDER BY loaded_at DESC) = 1