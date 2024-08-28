CREATE TABLE IF NOT EXISTS gold.suggested_pairs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker_1 VARCHAR(10) NOT NULL,
    ticker_2 VARCHAR(10) NOT NULL,
    spread_intercept NUMERIC,
    spread_slope NUMERIC,
    cointegration_p_value NUMERIC,
    half_life NUMERIC,
    mean_crossings NUMERIC,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);