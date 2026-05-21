
  
    

  create  table "crypto_db"."public_marts"."mart_technical_indicators__dbt_tmp"
  
  
    as
  
  (
    

WITH base AS (
    SELECT * FROM "crypto_db"."public_staging"."stg_ohlcv"
),

moving_averages AS (
    SELECT
        candle_time, symbol, open, high, low, close, volume,
        candle_type, pct_change,

        -- Simple Moving Average (SMA)
        AVG(close) OVER (PARTITION BY symbol
            ORDER BY candle_time
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS sma_20,

        AVG(close) OVER (PARTITION BY symbol
            ORDER BY candle_time
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS sma_50,

        -- Bollinger Bands std
        STDDEV(close) OVER (PARTITION BY symbol
            ORDER BY candle_time
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS std_20,

        -- Rolling volume average
        AVG(volume) OVER (PARTITION BY symbol
            ORDER BY candle_time
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS avg_volume_20

    FROM base
)

SELECT
    candle_time, symbol, open, high, low, close, volume,
    candle_type, pct_change,
    sma_20, sma_50,
    -- Bollinger Bands
    sma_20 + (2 * std_20) AS bb_upper,
    sma_20                AS bb_middle,
    sma_20 - (2 * std_20) AS bb_lower,
    std_20,
    -- Volume ratio (vs 20-period average)
    volume / NULLIF(avg_volume_20, 0) AS volume_ratio,
    -- Golden cross signal
    CASE
        WHEN sma_20 > sma_50 THEN 'bullish_cross'
        WHEN sma_20 < sma_50 THEN 'bearish_cross'
        ELSE 'neutral'
    END AS ma_signal

FROM moving_averages
ORDER BY symbol, candle_time
  );
  