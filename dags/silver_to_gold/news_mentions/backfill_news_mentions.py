from airflow.decorators import dag, task
from dags.silver_to_gold.news_mentions.constants import TARGET_SCHEMA, TARGET_TABLE, UPDATE_COLUMNS
import pendulum
from dags.utils.database import run_query, upsert_values
from dags.utils.logger import logger


@task
def backfill_news_mentions_silver_to_gold_task():
    try:
        logger.info("Starting backfill_news_mentions_silver_to_gold task")
        query = f"""
            SELECT
                tn.id AS id,
                tn.ticker AS ticker,
                tn.publisher AS publisher,
                tn.author AS author,
                tn.source_url AS source_url,
                tn.description AS short_summary,
                tn.published_at AS publish_time
            FROM silver.ticker_news tn;
        """
        ticker_news = run_query(query)
        logger.info(f"Sample record: {ticker_news[0] if ticker_news else 'No records'}")
        if ticker_news:
            upsert_values(
                data=ticker_news,
                columns=UPDATE_COLUMNS,
                table=TARGET_TABLE,
                schema=TARGET_SCHEMA,
                conflict_columns="id"
            )
        logger.info("Upsert completed")
    except Exception as e:
        logger.error(f"An error occurred in backfill_news_mentions_silver_to_gold task: {str(e)}", exc_info=True)
        raise

@dag(
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    tags=['etfs']
)
def backfill_news_mentions_silver_to_gold():
    backfill_news_mentions_silver_to_gold_task()

dag = backfill_news_mentions_silver_to_gold()