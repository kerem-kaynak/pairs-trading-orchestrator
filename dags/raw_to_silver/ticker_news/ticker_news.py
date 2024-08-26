from airflow.decorators import dag, task
from dags.raw_to_silver.ticker_news.constants import TARGET_SCHEMA, TARGET_TABLE, UPDATE_COLUMNS
import pendulum
from dags.utils.database import run_query, upsert_values
from dags.utils.logger import logger

SCHEDULE_INTERVAL = "0 10 * * *"

@task
def ticker_news_raw_to_silver_task():
    try:
        logger.info("Starting ticker_news_raw_to_silver task")
        start_date = pendulum.now().subtract(days=1).start_of('day')
        query = f"""
            SELECT
                id,
                polygon_id,
                ticker,
                publisher,
                author,
                source_url,
                description,
                published_at
            FROM raw.ticker_news WHERE updated_at >= '{start_date}'::timestamptz;
        """
        ticker_news = run_query(query)
        logger.info(f"Sample record: {ticker_news[0] if ticker_news else 'No records'}")
        if ticker_news:
            upsert_values(
                data=ticker_news,
                columns=UPDATE_COLUMNS,
                table=TARGET_TABLE,
                schema=TARGET_SCHEMA,
                conflict_columns=["polygon_id", "ticker"]
            )
        logger.info("Upsert completed")
    except Exception as e:
        logger.error(f"An error occurred in ticker_news_raw_to_silver task: {str(e)}", exc_info=True)
        raise

@dag(
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    tags=['etfs']
)
def ticker_news_raw_to_silver():
    ticker_news_raw_to_silver_task()

dag = ticker_news_raw_to_silver()