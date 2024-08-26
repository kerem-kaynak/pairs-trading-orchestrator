CREATE TABLE IF NOT EXISTS silver.ticker_news (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    polygon_id TEXT,
    ticker VARCHAR(10) NOT NULL,
    publisher TEXT,
    author TEXT,
    source_url TEXT,
    description TEXT,
    published_at TIMESTAMPTZ DEFAULT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_ticker_news_ticker ON silver.ticker_news (ticker);

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_polygon_id_ticker ON silver.ticker_news (polygon_id, ticker);