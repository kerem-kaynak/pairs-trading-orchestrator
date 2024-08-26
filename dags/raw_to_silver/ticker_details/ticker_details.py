from airflow.decorators import dag, task
from dags.raw_to_silver.ticker_details.constants import TARGET_SCHEMA, TARGET_TABLE, UPDATE_COLUMNS
import pendulum
from dags.utils.database import run_query, upsert_values
from dags.utils.logger import logger

SCHEDULE_INTERVAL = "0 10 * * 1"

@task
def ticker_details_raw_to_silver_task():
    try:
        logger.info("Starting ticker_details_raw_to_silver task")
        query = f"""
            SELECT
                ticker,
                name,
                composite_figi,
                share_class_figi
            FROM raw.ticker_details;
        """
        ticker_details = run_query(query)
        logger.info(f"Sample record: {ticker_details[0] if ticker_details else 'No records'}")
        if ticker_details:
            upsert_values(
                data=ticker_details,
                columns=UPDATE_COLUMNS,
                table=TARGET_TABLE,
                schema=TARGET_SCHEMA,
                conflict_columns="ticker"
            )
            logger.info("Upsert completed")
        else:
            logger.warning("No ticker details to upsert")
    except Exception as e:
        logger.error(f"An error occurred in ticker_details_raw_to_silver task: {str(e)}", exc_info=True)
        raise

@dag(
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    tags=['etfs']
)
def ticker_details_raw_to_silver():
    ticker_details_raw_to_silver_task()

dag = ticker_details_raw_to_silver()