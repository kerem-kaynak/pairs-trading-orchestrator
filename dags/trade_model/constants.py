QUANT_SERVICE_HOST = "https://pairs-trading-quant-service-a56kh2a5na-ez.a.run.app"

TRADES_UPDATE_COLUMNS = [
    "ticker_1",
    "ticker_2",
    "date",
    "spread",
    "predicted_trend",
    "signal",
    "position",
    "budget",
    "ticker_1_price",
    "ticker_2_price",
]

SUCCESS_UPDATE_COLUMNS = [
    "ticker_1",
    "ticker_2",
    "total_return",
    "annualized_return",
    "max_drawdown",
]

TRADES_TARGET_TABLE = "model_trades"
TRADES_TARGET_SCHEMA = "gold"

SUCCESS_TARGET_TABLE = "model_success_metrics"
SUCCESS_TARGET_SCHEMA = "gold"