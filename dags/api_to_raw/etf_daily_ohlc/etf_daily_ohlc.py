from airflow.decorators import dag, task
from api_to_raw.etf_daily_ohlc.constants import ETF_AGGREGATES_COLUMN_MAP, TARGET_SCHEMA, TARGET_TABLE, UPDATE_COLUMNS
import pendulum
from api_to_raw.etf_daily_ohlc.utils import get_all_etf_tickers, get_previous_close_ohlc
from plugins.database import upsert_values
from plugins.logger import logger

SCHEDULE_INTERVAL = "0 11 * * *"

@task
def etf_ohlc_polygon_to_postgres():
    try:
        logger.info("Starting etf_ohlc_polygon_to_postgres task")
        tickers = list(set(get_all_etf_tickers()))
        previous_close_list = []
        progress = 0
        total = len(tickers)
        for ticker in tickers:
            progress += 1
            logger.info(f"{progress}/{total}")
            previous_close_ohlc = get_previous_close_ohlc(ticker=ticker)
            if previous_close_ohlc:
                renamed_previous_close = {ETF_AGGREGATES_COLUMN_MAP.get(key, key): value for key, value in previous_close_ohlc.items()}
                previous_close_list.append(renamed_previous_close)
        logger.info(f"Sample record: {previous_close_list[0] if previous_close_list else 'No records'}")
        upsert_values(
            data=previous_close_list,
            columns=UPDATE_COLUMNS,
            table=TARGET_TABLE,
            schema=TARGET_SCHEMA,
            conflict_columns=["ticker", "unix_msec_timestamp"]
        )
        logger.info("Upsert completed")
    except Exception as e:
        logger.error(f"An error occurred in etf_ohlc_polygon_to_postgres task: {str(e)}", exc_info=True)
        raise

@dag(
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['etfs']
)
def etf_daily_ohlc_api_to_raw():
    etf_ohlc_polygon_to_postgres()

dag = etf_daily_ohlc_api_to_raw()