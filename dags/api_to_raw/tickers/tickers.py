from airflow.decorators import dag, task
from dags.api_to_raw.tickers.constants import TARGET_SCHEMA, TARGET_TABLE, UPDATE_COLUMNS
import pendulum
from dags.api_to_raw.tickers.utils import get_etf_tickers
from dags.utils.database import run_query, upsert_values
from dags.utils.logger import logger

SCHEDULE_INTERVAL = "0 9 * * *"

@task
def tickers_polygon_to_postgres():
    try:
        tickers = get_etf_tickers()
        upsert_values(
            data=tickers,
            columns=UPDATE_COLUMNS,
            table=TARGET_TABLE,
            schema=TARGET_SCHEMA,
            conflict_columns="ticker"
        )
        logger.info("Upsert completed")
    except Exception as e:
        logger.error(f"An error occurred in tickers_polygon_to_postgres task: {str(e)}", exc_info=True)
        raise

@dag(
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    tags=['etfs']
)
def tickers_api_to_raw():
    tickers_polygon_to_postgres()

dag = tickers_api_to_raw()