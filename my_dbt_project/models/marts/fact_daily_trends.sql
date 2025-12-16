SELECT
    report_date,
    symbol,
    price_usd,
    -- Calculate generic moving average
    AVG(price_usd) OVER (
        PARTITION BY symbol 
        ORDER BY report_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as 7_day_moving_avg
FROM {{ ref('stg_crypto') }}