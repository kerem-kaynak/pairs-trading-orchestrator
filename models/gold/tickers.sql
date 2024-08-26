CREATE TABLE IF NOT EXISTS gold.tickers (
    ticker VARCHAR(10) PRIMARY KEY,
    name TEXT,
    active BOOLEAN,
    type TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tickers_type ON gold.tickers (type);