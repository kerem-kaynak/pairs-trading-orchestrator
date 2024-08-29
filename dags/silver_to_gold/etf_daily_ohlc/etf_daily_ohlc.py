from airflow.decorators import dag, task
from dags.silver_to_gold.etf_daily_ohlc.constants import TARGET_SCHEMA, TARGET_TABLE, UPDATE_COLUMNS
import pendulum
from dags.utils.database import run_query, upsert_values
from dags.utils.logger import logger


SCHEDULE_INTERVAL = "0 13 * * *"

@task
def etf_daily_ohlc_silver_to_gold_task():
    try:
        logger.info("Starting etf_daily_ohlc_silver_to_gold task")
        start_date = pendulum.now().subtract(days=1).format("YYYY-MM-DD")
        query = f"""
            SELECT
                edo.ticker AS ticker,
                td.name AS name,
                edo.open AS open,
                edo.high AS high,
                edo.low AS low,
                edo.close AS close,
                edo.date AS date,
                edo.volume AS volume,
                edo.volume_weighted_price AS volume_weighted_price
            FROM silver.etf_daily_ohlc edo LEFT JOIN silver.ticker_details td ON edo.ticker = td.ticker WHERE edo.date >= '{start_date}';
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
        logger.error(f"An error occurred in etf_daily_ohlc_silver_to_gold task: {str(e)}", exc_info=True)
        raise

@dag(
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    tags=['etfs']
)
def etf_daily_ohlc_silver_to_gold():
    etf_daily_ohlc_silver_to_gold_task()

dag = etf_daily_ohlc_silver_to_gold()
