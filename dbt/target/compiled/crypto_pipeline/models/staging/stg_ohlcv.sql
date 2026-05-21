-- Staging model: raw ohlcv_1min → clean view
-- Hanya rename dan cast, tidak ada business logic



SELECT
    time                          AS candle_time,
    symbol,
    open::NUMERIC(18,8)           AS open,
    high::NUMERIC(18,8)           AS high,
    low::NUMERIC(18,8)            AS low,
    close::NUMERIC(18,8)          AS close,
    volume::NUMERIC(18,8)         AS volume,
    trade_count,
    -- Derived columns
    (high - low)                  AS candle_range,
    (close - open)                AS candle_body,
    CASE
        WHEN open < close THEN 'bullish'
        WHEN open > close THEN 'bearish'
        ELSE 'doji'
    END                           AS candle_type,
    close / NULLIF(LAG(close) OVER (
        PARTITION BY symbol ORDER BY time
    ), 0) - 1                     AS pct_change

FROM "crypto_db"."public"."ohlcv_1min"
WHERE time >= NOW() - INTERVAL '7 days'  -- rolling window
ORDER BY symbol, time