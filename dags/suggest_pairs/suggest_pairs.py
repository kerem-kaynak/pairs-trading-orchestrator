import json
import os
from airflow.decorators import dag, task
import requests
from dags.suggest_pairs.constants import QUANT_SERVICE_HOST, UPDATE_COLUMNS, TARGET_TABLE, TARGET_SCHEMA
import pendulum
from dags.utils.database import execute_sql, run_query, upsert_values, json_serialize
from dags.utils.logger import logger

SCHEDULE_INTERVAL = "0 3 * * *"

QUANT_SERVICE_API_TOKEN = os.environ.get("QUANT_SERVICE_API_TOKEN")

@task
def compute_and_upsert_suggested_pairs_task():
    try:
        logger.info("Starting tickers_silver_to_gold task")
        query = f"""
            SELECT
                ticker,
                close AS price,
                date
            FROM gold.etf_daily_ohlc;
        """
        price_series = run_query(query)
        if price_series:
            data = {
                "data": price_series
            }
            json_data = json.dumps(data, default=json_serialize)
            res = requests.post(f"{QUANT_SERVICE_HOST}/ml/pairs", data = json_data, headers = {
                'Authorization': f'Bearer {QUANT_SERVICE_API_TOKEN}',
                'Content-Type': 'application/json'
            })
            if res.status_code == 200:
                response_data = res.json()
                if 'suggested_pairs' in response_data:
                    pairs_data = response_data['suggested_pairs']
                    if isinstance(pairs_data, list):
                        upsert_data = [{
                            "ticker_1": pair["ticker_1"],
                            "ticker_2": pair["ticker_2"],
                            "spread_intercept": pair["spread_statistics"]["intercept"],
                            "spread_slope": pair["spread_statistics"]["slope"],
                            "cointegration_p_value": pair["cointegration_critical_value"],
                            "half_life": pair["half_life"],
                            "mean_crossings": pair["mean_crossings"],
                        } for pair in pairs_data]
                        execute_sql("TRUNCATE gold.suggested_pairs;")
                        upsert_values(
                            data=upsert_data,
                            columns=UPDATE_COLUMNS,
                            table=TARGET_TABLE,
                            schema=TARGET_SCHEMA,
                            conflict_columns="id"
                        )
                        logger.info("Upsert completed")
                    else:
                        logger.error(f"Expected 'suggested_pairs' to be a list, but got {type(pairs_data)}")
                else:
                    logger.error(f"'suggested_pairs' key not found in API response. Response: {response_data}")
            else:
                logger.error(f"API request failed with status code {res.status_code}. Response: {res.text}")
        else:
            logger.warning("No price series data retrieved from the database")
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
def suggest_pairs():
    compute_and_upsert_suggested_pairs_task()

dag = suggest_pairs()