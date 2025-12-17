{{ config(materialized='view') }}

SELECT
    report_date,
    coin_id,
    symbol,
    name,
    logo_url,
    rank,
    
    -- Price Data
    price_usd,
    high_24h,
    low_24h,
    change_24h_pct,
    all_time_high,
    drawdown_pct,

    -- Volume Data
    market_cap,
    total_volume,
    circulating_supply,
    
    -- Calculated: Daily Volatility (High - Low)
    (high_24h - low_24h) as daily_range_usd,

    -- Moving Average
    AVG(price_usd) OVER (
        PARTITION BY symbol 
        ORDER BY report_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7d

FROM {{ ref('stg_crypto') }}