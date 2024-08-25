ETF_AGGREGATES_COLUMN_MAP = {
    "T": "ticker",
    "c": "close",
    "h": "high",
    "l": "low",
    "o": "open",
    "t": "unix_msec_timestamp",
    "v": "volume",
    "vw": "volume_weighted_price",
}

UPDATE_COLUMNS = [
    "ticker",
    "close",
    "high",
    "low",
    "open",
    "unix_msec_timestamp",
    "volume",
    "volume_weighted_price"
]

TARGET_TABLE = "etf_daily_ohlc"
TARGET_SCHEMA = "raw"