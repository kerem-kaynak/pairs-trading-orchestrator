CREATE TABLE IF NOT EXISTS raw.etf_daily_ohlc (
    ticker VARCHAR(10) NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    unix_msec_timestamp BIGINT NOT NULL,
    volume NUMERIC,
    volume_weighted_price NUMERIC,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ DEFAULT NULL,
    PRIMARY KEY (ticker, unix_msec_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_etf_aggregates_unix_msec_timestamp ON raw.etf_daily_ohlc (unix_msec_timestamp);

CREATE INDEX IF NOT EXISTS idx_etf_aggregates_ticker ON raw.etf_daily_ohlc (ticker);