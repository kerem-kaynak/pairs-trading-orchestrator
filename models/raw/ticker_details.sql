CREATE TABLE IF NOT EXISTS raw.ticker_details (
    ticker VARCHAR(10) PRIMARY KEY,
    name TEXT,
    composite_figi TEXT,
    share_class_figi TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ticker_details_name ON raw.ticker_details (name);