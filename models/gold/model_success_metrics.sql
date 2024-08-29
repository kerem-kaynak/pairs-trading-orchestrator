CREATE TABLE IF NOT EXISTS gold.model_success_metrics (
    ticker_1 VARCHAR(10) NOT NULL,
    ticker_2 VARCHAR(10) NOT NULL,
    total_return NUMERIC,
    annualized_return NUMERIC,
    max_drawdown NUMERIC,
    PRIMARY KEY (ticker_1, ticker_2)
);

CREATE INDEX IF NOT EXISTS idx_model_success_metrics_ticker_1 ON gold.model_success_metrics (ticker_1);

CREATE INDEX IF NOT EXISTS idx_model_success_metrics_ticker_2 ON gold.model_success_metrics (ticker_2);