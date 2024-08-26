from airflow.decorators import dag, task
from dags.silver_to_gold.tickers.constants import TARGET_SCHEMA, TARGET_TABLE, UPDATE_COLUMNS
import pendulum
from dags.utils.database import run_query, upsert_values
from dags.utils.logger import logger

SCHEDULE_INTERVAL = "0 10 * * *"

@task
def tickers_silver_to_gold_task():
    try:
        logger.info("Starting tickers_silver_to_gold task")
        query = f"""
            SELECT
               t.ticker AS ticker,
               t.active AS active,
               t.type AS type,
               td.name AS name
            FROM silver.tickers t
            LEFT JOIN silver.ticker_details td ON t.ticker = td.ticker;
        """
        tickers = run_query(query)
        if tickers:
            upsert_values(
                data=tickers,
                columns=UPDATE_COLUMNS,
                table=TARGET_TABLE,
                schema=TARGET_SCHEMA,
                conflict_columns="ticker"
            )
        logger.info("Upsert completed")
    except Exception as e:
        logger.error(f"An error occurred in tickers_silver_to_gold task: {str(e)}", exc_info=True)
        raise

@dag(
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    tags=['etfs']
)
def tickers_silver_to_gold():
    tickers_silver_to_gold_task()

dag = tickers_silver_to_gold()