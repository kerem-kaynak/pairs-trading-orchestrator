CREATE TABLE IF NOT EXISTS gold.model_trades (
    ticker_1 VARCHAR(10) NOT NULL,
    ticker_2 VARCHAR(10) NOT NULL,
    spread NUMERIC,
    predicted_trend NUMERIC,
    signal TEXT,
    position NUMERIC,
    budget NUMERIC,
    ticker_1_price NUMERIC,
    ticker_2_price NUMERIC,
    date DATE,
    PRIMARY KEY (ticker_1, ticker_2, date)
);

CREATE INDEX IF NOT EXISTS idx_model_success_metrics_ticker_1 ON gold.model_success_metrics (ticker_1);

CREATE INDEX IF NOT EXISTS idx_model_success_metrics_ticker_2 ON gold.model_success_metrics (ticker_2);