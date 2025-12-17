{{ config(materialized='view') }}

WITH base AS (
    SELECT
        rp.date,
        rp.symbol,
        rp.close,
        rp.volume
    FROM {{ ref('stg_prices_raw') }} AS rp
),

returns AS (
    SELECT
        date,
        symbol,
        close,
        LAG(close) OVER (
            PARTITION BY symbol
            ORDER BY date
        ) AS prev_close
    FROM base
),

returns_calc AS (
    SELECT
        date,
        symbol,
        close,
        CASE 
            WHEN prev_close IS NULL OR prev_close = 0
                THEN NULL
            ELSE (close - prev_close) / prev_close * 100.0
        END AS daily_return_pct
    FROM returns
),

moving_avg AS (
    SELECT
        date,
        symbol,
        close,
        daily_return_pct,
        AVG(close) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS ma_7d,
        AVG(close) OVER (
            PARTITION BY symbol 
            ORDER BY date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS ma_30d
    FROM returns_calc
)

SELECT
    date,
    symbol,
    close,
    daily_return_pct,
    ma_7d,
    ma_30d
FROM moving_avg
