from airflow.decorators import dag, task
from dags.api_to_raw.ticker_details.constants import TARGET_SCHEMA, TARGET_TABLE, UPDATE_COLUMNS
import pendulum
from dags.api_to_raw.ticker_details.utils import get_ticker_details
from plugins.database import run_query, upsert_values
from plugins.logger import logger

SCHEDULE_INTERVAL = "0 9 * * 1"

@task
def ticker_details_polygon_to_postgres():
    query = "SELECT ticker FROM raw.tickers"
    try:
        logger.info("Starting ticker_details_polygon_to_postgres task")
        tickers = run_query(query)
        tickers_details = []
        progress = 0
        total = len(tickers)
        for ticker in tickers:
            progress += 1
            logger.info(f"{progress}/{total}")
            ticker_details = get_ticker_details(ticker=ticker["ticker"])
            if ticker_details:
                tickers_details.append(ticker_details)
        logger.info(f"Sample record: {tickers_details[0] if tickers_details else 'No records'}")
        if tickers_details:
            upsert_values(
                data=tickers_details,
                columns=UPDATE_COLUMNS,
                table=TARGET_TABLE,
                schema=TARGET_SCHEMA,
                conflict_columns="ticker"
            )
            logger.info("Upsert completed")
        else:
            logger.warning("No ticker details to upsert")
    except Exception as e:
        logger.error(f"An error occurred in ticker_news_polygon_to_postgres task: {str(e)}", exc_info=True)
        raise

@dag(
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['etfs']
)
def ticker_details_api_to_raw():
    ticker_details_polygon_to_postgres()

dag = ticker_details_api_to_raw()