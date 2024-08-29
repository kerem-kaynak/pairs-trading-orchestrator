import json
import os
from airflow.decorators import dag, task
import requests
from dags.trade_model.constants import QUANT_SERVICE_HOST, TRADES_UPDATE_COLUMNS, SUCCESS_UPDATE_COLUMNS, TRADES_TARGET_TABLE, TRADES_TARGET_SCHEMA, SUCCESS_TARGET_TABLE, SUCCESS_TARGET_SCHEMA
import pendulum
from dags.utils.database import execute_sql, run_query, upsert_values, json_serialize
from dags.utils.logger import logger

SCHEDULE_INTERVAL = "0 6 * * *"

QUANT_SERVICE_API_TOKEN = os.environ.get("QUANT_SERVICE_API_TOKEN")

@task
def compute_trade_statistics_task():
    try:
        logger.info("Starting compute_trade_statistics task")
        pairs_query = f"""
            SELECT
                ticker_1,
                ticker_2
            FROM gold.model_chosen_pairs;
        """
        pairs_result = run_query(pairs_query)
        if pairs_result:
            execute_sql("TRUNCATE gold.model_success_metrics;")
            for pair in pairs_result:
                logger.info("")
                pair_ohlc_query = f"""
                    SELECT
                        ticker,
                        close AS price,
                        date
                    FROM gold.etf_daily_ohlc WHERE ticker IN ('{pair["ticker_1"]}', '{pair["ticker_2"]}');
                """
                pair_ohlc_result = run_query(pair_ohlc_query)
                if pair_ohlc_result:
                    data = {
                        "data": pair_ohlc_result
                    }
                    json_data = json.dumps(data, default=json_serialize)
                    res = requests.post(f"{QUANT_SERVICE_HOST}/trading/trade_with_model", data = json_data, headers = {
                        'Authorization': f'Bearer {QUANT_SERVICE_API_TOKEN}',
                        'Content-Type': 'application/json'
                    })
                    if res.status_code == 200:
                        response_data = res.json()
                        daily_trades = response_data["results"]
                        pair_daily_trades = [
                            {
                                "date": daily_trade.get("date"),
                                "ticker_1": pair["ticker_1"],
                                "ticker_2": pair["ticker_2"],
                                "spread": daily_trade.get("spread"),
                                "predicted_trend": daily_trade.get("predicted_trend"),
                                "signal": daily_trade.get("signal"),
                                "position": daily_trade.get("position"),
                                "budget": daily_trade.get("budget"),
                                "ticker_1_price": daily_trade.get(pair["ticker_1"]),
                                "ticker_2_price": daily_trade.get(pair["ticker_2"]),
                            }
                            for daily_trade in daily_trades
                            if daily_trade is not None and daily_trade.get("date") is not None
                        ]
                        success_metrics = [{
                            "ticker_1": pair["ticker_1"],
                            "ticker_2": pair["ticker_2"],
                            "total_return": response_data["total_return"],
                            "annualized_return": response_data["annualized_return"],
                            "max_drawdown": response_data["max_drawdown"]
                        }]

                        logger.info("Beginning upsert for daily trades for pair")
                        upsert_values(
                            data=pair_daily_trades,
                            columns=TRADES_UPDATE_COLUMNS,
                            table=TRADES_TARGET_TABLE,
                            schema=TRADES_TARGET_SCHEMA,
                            conflict_columns=["ticker_1", "ticker_2", "date"]
                        )
                        logger.info("Completed upsert for daily trades for pair")

                        logger.info("Beginning upsert for daily success metrics for pair")
                        upsert_values(
                            data=success_metrics,
                            columns=SUCCESS_UPDATE_COLUMNS,
                            table=SUCCESS_TARGET_TABLE,
                            schema=SUCCESS_TARGET_SCHEMA,
                            conflict_columns=["ticker_1", "ticker_2"]
                        )
                        logger.info("Completed upsert for daily success metrics for pair")
                    else:
                        logger.warning(f"Request to quant service responded with an error: {res.status_code}")
                        error_detail = res.json()
                        logger.error(f"Parsed error response: {json.dumps(error_detail, indent=2)}")
                else:
                    logger.warning("OHLC data for pair could not be retrieved")
        else:
            logger.warning("No pairs retrieved from the database")
        logger.info("Upserts completed")
    except Exception as e:
        logger.error(f"An error occurred in compute_trade_statistics task: {str(e)}", exc_info=True)
        raise

@dag(
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    tags=['etfs']
)
def compute_trade_statistics():
    compute_trade_statistics_task()

dag = compute_trade_statistics()