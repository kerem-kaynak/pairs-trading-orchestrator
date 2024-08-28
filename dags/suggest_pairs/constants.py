# QUANT_SERVICE_HOST = "https://pairs-trading-quant-service-a56kh2a5na-ez.a.run.app"
QUANT_SERVICE_HOST = "http://localhost:8000"

UPDATE_COLUMNS = [
    "ticker_1",
    "ticker_2",
    "spread_intercept",
    "spread_slope",
    "cointegration_p_value",
    "half_life",
    "mean_crossings",
]

TARGET_TABLE = "suggested_pairs"
TARGET_SCHEMA = "gold"