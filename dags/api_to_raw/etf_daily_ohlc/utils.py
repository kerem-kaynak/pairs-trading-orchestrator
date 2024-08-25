import pendulum
import requests
import os
from typing import Any, Dict, List, Optional
from dags.utils.logger import logger


POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")

def get_all_etf_tickers() -> List[str]:
    req_url = f"https://api.polygon.io/v3/reference/tickers?type=ETF&market=stocks&exchange=XNAS&active=true&?date=2023-01-01&limit=1000&apiKey={POLYGON_API_KEY}"
    try:
        response = requests.get(req_url)
    except Exception as e:
        logger.error(str(e))
        raise
    if response.status_code == 200:
        tickers = [etf["ticker"] for etf in response.json()["results"]]
        return tickers
    else:
        logger.error(
            f"Failed to get ETF tickers\nStatus code: {response.status_code}\nResponse: {response.text}"
        )
        return None

def get_previous_close_ohlc(ticker: str) -> Optional[Dict[str, Any]]:
    req_url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={POLYGON_API_KEY}"
    response = requests.get(req_url)
    if response.status_code == 200:
        previous_close_ohlc = response.json()["results"][0]
        return previous_close_ohlc
    else:
        logger.error(
            f"Failed to get previous close OHLC data for {ticker}\nStatus code: {response.status_code}\nResponse: {response.text}"
        )
        return None
    
def get_aggregates(ticker: str, start_date: str = "2024-01-01", end_date: str = pendulum.yesterday().format('YYYY-MM-DD')) -> Optional[List[Dict[str, Any]]]:
    req_url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&limit=50000&apiKey={POLYGON_API_KEY}"
    response = requests.get(req_url)
    if response.status_code == 200:
        results = response.json()["results"]
        aggregates = [{**item, "T": ticker} for item in results]
        return aggregates
    else:
        logger.error(
            f"Failed to get aggregates data for {ticker}\nStatus code: {response.status_code}\nResponse: {response.text}"
        )
        return None