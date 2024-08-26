CREATE TABLE IF NOT EXISTS gold.news_mentions (
    id UUID PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    publisher TEXT,
    author TEXT,
    source_url TEXT,
    short_summary TEXT,
    publish_time TIMESTAMPTZ DEFAULT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ticker_news_ticker ON gold.news_mentions (ticker);