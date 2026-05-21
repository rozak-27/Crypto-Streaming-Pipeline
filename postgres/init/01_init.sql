CREATE TABLE price_ticks (
  time    TIMESTAMPTZ      NOT NULL,   -- timestamp dengan timezone
  symbol  TEXT             NOT NULL,   -- BTC-USD, ETH-USD, dll
  price   DOUBLE PRECISION NOT NULL,   -- harga saat ini
  volume  DOUBLE PRECISION NOT NULL,   -- volume transaksi
  bid     DOUBLE PRECISION,            -- harga beli terbaik
  ask     DOUBLE PRECISION,            -- harga jual terbaik
  source  TEXT DEFAULT 'simulator'     -- sumber data
);

-- Jadikan hypertable (partisi per 7 hari otomatis)
SELECT create_hypertable('price_ticks', 'time');

-- Index untuk query per symbol
CREATE INDEX idx_price_ticks_symbol ON price_ticks (symbol, time DESC);

-- Tabel OHLCV 1 menit (diisi oleh Spark)
CREATE TABLE IF NOT EXISTS ohlcv_1min (
  time        TIMESTAMPTZ      NOT NULL,
  symbol      TEXT             NOT NULL,
  open        DOUBLE PRECISION NOT NULL,
  high        DOUBLE PRECISION NOT NULL,
  low         DOUBLE PRECISION NOT NULL,
  close       DOUBLE PRECISION NOT NULL,
  volume      DOUBLE PRECISION NOT NULL,
  trade_count BIGINT
);

SELECT create_hypertable('ohlcv_1min', 'time', if_not_exists => TRUE);
CREATE INDEX idx_ohlcv_symbol ON ohlcv_1min (symbol, time DESC);

-- Database khusus Airflow
CREATE DATABASE airflow_db;
