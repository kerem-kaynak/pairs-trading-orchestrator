from airflow.decorators import dag, task
from dags.raw_to_silver.etf_daily_ohlc.constants import TARGET_SCHEMA, TARGET_TABLE, UPDATE_COLUMNS
import pendulum
from dags.utils.database import run_query, upsert_values
from dags.utils.logger import logger


SCHEDULE_INTERVAL = "0 12 * * *"

@task
def etf_daily_ohlc_raw_to_silver_task():
    try:
        logger.info("Starting etf_daily_ohlc_raw_to_silver task")
        start_date = pendulum.now().subtract(days=1)
        start_unix_msec_timestamp = int(start_date.timestamp() * 1000)
        query = f"""
            SELECT
                ticker,
                open,
                high,
                low,
                close,
                (TO_TIMESTAMP(unix_msec_timestamp / 1000.0) AT TIME ZONE 'UTC')::DATE AS date,
                volume,
                volume_weighted_price
            FROM raw.etf_daily_ohlc WHERE unix_msec_timestamp >= {start_unix_msec_timestamp}
        """
        etf_daily_ohlc_data = run_query(query)
        logger.info(f"Sample record: {etf_daily_ohlc_data[0] if etf_daily_ohlc_data else 'No records'}")
        if etf_daily_ohlc_data:
            upsert_values(
                data=etf_daily_ohlc_data,
                columns=UPDATE_COLUMNS,
                table=TARGET_TABLE,
                schema=TARGET_SCHEMA,
                conflict_columns=["ticker", "date"]
            )
        logger.info("Task completed")
    except Exception as e:
        logger.error(f"An error occurred in etf_daily_ohlc_raw_to_silver task: {str(e)}", exc_info=True)
        raise

@dag(
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    tags=['etfs']
)
def etf_daily_ohlc_raw_to_silver():
    etf_daily_ohlc_raw_to_silver_task()

dag = etf_daily_ohlc_raw_to_silver()