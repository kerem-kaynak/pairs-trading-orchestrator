CREATE TABLE IF NOT EXISTS silver.etf_daily_ohlc (
    ticker VARCHAR(10) NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    date DATE NOT NULL,
    volume NUMERIC,
    volume_weighted_price NUMERIC,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ DEFAULT NULL,
    PRIMARY KEY (ticker, date)
);

CREATE INDEX IF NOT EXISTS idx_etf_aggregates_date ON silver.etf_daily_ohlc (date);

CREATE INDEX IF NOT EXISTS idx_etf_aggregates_ticker ON silver.etf_daily_ohlc (ticker);