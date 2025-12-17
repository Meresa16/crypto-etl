-- SELECT
--     report_date,
--     symbol,
--     price_usd,
--     -- Calculate generic moving average
--     AVG(price_usd) OVER (
--         PARTITION BY symbol 
--         ORDER BY report_date 
--         ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
--     ) as moving_avg_7d
-- FROM {{ ref('stg_crypto') }}

{{
    config(
        materialized='view'
    )
}}

SELECT
    report_date,
    symbol,
    name,
    price_usd,
    market_cap,  -- <--- ADD THIS LINE HERE
    total_volume, -- <--- Might as well add volume too!
    -- Calculate generic moving average
    AVG(price_usd) OVER (
        PARTITION BY symbol 
        ORDER BY report_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as moving_avg_7d
FROM {{ ref('stg_crypto') }}