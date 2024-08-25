from airflow.decorators import dag, task
from api_to_raw.ticker_news.constants import TARGET_SCHEMA, TARGET_TABLE, UPDATE_COLUMNS
import pendulum
from api_to_raw.ticker_news.utils import backfill_ticker_news
from plugins.database import run_query, upsert_values
from plugins.logger import logger

@task
def ticker_news_polygon_to_postgres():
    query = "SELECT ticker FROM raw.tickers"
    try:
        logger.info("Starting ticker_news_polygon_to_postgres task")
        tickers = run_query(query)
        tickers_news = []
        progress = 0
        total = len(tickers)
        for ticker in tickers:
            progress += 1
            logger.info(f"{progress}/{total}")
            ticker_news = backfill_ticker_news(ticker=ticker["ticker"])
            if ticker_news:
                tickers_news.extend(ticker_news)
        logger.info(f"Sample record: {tickers_news[0] if tickers_news else 'No records'}")
        upsert_values(
            data=tickers_news,
            columns=UPDATE_COLUMNS,
            table=TARGET_TABLE,
            schema=TARGET_SCHEMA,
            conflict_columns=["polygon_id", "ticker"]
        )
        logger.info("Upsert completed")
    except Exception as e:
        logger.error(f"An error occurred in ticker_news_polygon_to_postgres task: {str(e)}", exc_info=True)
        raise

@dag(
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['etfs']
)
def backfill_ticker_news_api_to_raw():
    ticker_news_polygon_to_postgres()

dag = backfill_ticker_news_api_to_raw()