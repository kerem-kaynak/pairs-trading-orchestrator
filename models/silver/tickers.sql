CREATE TABLE IF NOT EXISTS silver.tickers (
    ticker VARCHAR(10) PRIMARY KEY,
    active BOOLEAN,
    currency_name TEXT,
    locale TEXT,
    market TEXT,
    type TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tickers_type ON silver.tickers (type);