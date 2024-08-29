CREATE TABLE IF NOT EXISTS gold.model_chosen_pairs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker_1 VARCHAR(10),
    ticker_2 VARCHAR(10)
);