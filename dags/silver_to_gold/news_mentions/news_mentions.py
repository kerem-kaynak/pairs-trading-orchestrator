from airflow.decorators import dag, task
from dags.silver_to_gold.news_mentions.constants import TARGET_SCHEMA, TARGET_TABLE, UPDATE_COLUMNS
import pendulum
from dags.utils.database import run_query, upsert_values
from dags.utils.logger import logger

SCHEDULE_INTERVAL = "0 11 * * *"

@task
def news_mentions_silver_to_gold_task():
    try:
        logger.info("Starting news_mentions_silver_to_gold task")
        start_date = pendulum.now().subtract(days=1).start_of('day')
        query = f"""
            SELECT
                tn.id AS id,
                tn.ticker AS ticker,
                tn.publisher AS publisher,
                tn.author AS author,
                tn.source_url AS source_url,
                tn.description AS short_summary,
                tn.published_at AS publish_date
            FROM silver.ticker_news tn WHERE updated_at >= '{start_date}'::timestamptz;
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
        logger.error(f"An error occurred in news_mentions_silver_to_gold task: {str(e)}", exc_info=True)
        raise

@dag(
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    tags=['etfs']
)
def news_mentions_silver_to_gold():
    news_mentions_silver_to_gold_task()

dag = news_mentions_silver_to_gold()