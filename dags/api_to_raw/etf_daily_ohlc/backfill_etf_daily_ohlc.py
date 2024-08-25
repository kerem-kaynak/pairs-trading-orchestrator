from airflow.decorators import dag, task
from api_to_raw.etf_daily_ohlc.constants import ETF_AGGREGATES_COLUMN_MAP, TARGET_SCHEMA, TARGET_TABLE, UPDATE_COLUMNS
import pendulum
from api_to_raw.etf_daily_ohlc.utils import get_aggregates, get_all_etf_tickers
from utils.database import upsert_values
from utils.logger import logger

@task
def polygon_to_postgres():
    try:
        logger.info("Starting polygon_to_postgres task")
        tickers = list(set(get_all_etf_tickers()))
        aggregates_list = []
        progress = 0
        total = len(tickers)
        for ticker in tickers:
            progress += 1
            logger.info(f"{progress}/{total}")
            aggregates = get_aggregates(ticker=ticker)
            if aggregates:
                renamed_aggregates = [{ETF_AGGREGATES_COLUMN_MAP.get(key, key): value for key, value in aggregate.items()} for aggregate in aggregates]
                aggregates_list.extend(renamed_aggregates)
        logger.info(f"Sample record: {aggregates_list[0] if aggregates_list else 'No records'}")
        upsert_values(
            data=aggregates_list,
            columns=UPDATE_COLUMNS,
            table=TARGET_TABLE,
            schema=TARGET_SCHEMA,
            conflict_columns=["ticker", "unix_msec_timestamp"]
        )
        logger.info("Upsert completed")
    except Exception as e:
        logger.error(f"An error occurred in polygon_to_postgres task: {str(e)}", exc_info=True)
        raise

@dag(
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['etfs']
)
def backfill_etf_daily_ohlc_api_to_raw():
    polygon_to_postgres()

dag = backfill_etf_daily_ohlc_api_to_raw()