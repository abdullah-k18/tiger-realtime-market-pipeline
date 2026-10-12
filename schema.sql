-- Hypertable for raw price ticks, with columnstore compression segmented by symbol
CREATE TABLE crypto_ticks (
    "time" TIMESTAMPTZ,
    symbol TEXT,
    price DOUBLE PRECISION,
    day_volume NUMERIC
) WITH (
   tsdb.hypertable,
   tsdb.segmentby='symbol',
   tsdb.orderby='time DESC'
);

-- Relational metadata for each asset
CREATE TABLE crypto_assets (
    symbol TEXT UNIQUE,
    "name" TEXT
);

INSERT INTO crypto_assets VALUES
  ('BTC/USD', 'Bitcoin'), ('ETH/USD', 'Ethereum'), ('MSFT', 'Microsoft'), ('AAPL', 'Apple');

-- Daily OHLCV candles, computed incrementally in the background
CREATE MATERIALIZED VIEW one_day_candle
WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 day', time) AS bucket,
        symbol,
        FIRST(price, time) AS "open",
        MAX(price) AS high,
        MIN(price) AS low,
        LAST(price, time) AS "close",
        LAST(day_volume, time) AS day_volume
    FROM crypto_ticks
    GROUP BY bucket, symbol;

SELECT add_continuous_aggregate_policy('one_day_candle',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day');
