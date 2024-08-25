import pendulum
import requests
import os
from typing import Any, Dict, List, Optional
from plugins.logger import logger


POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")

def get_ticker_news(ticker: str) -> Optional[List[Dict[str, Any]]]:
    start_date = pendulum.yesterday().format('YYYY-MM-DD')
    req_url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&published_utc.gte={start_date}&limit=50&apiKey={POLYGON_API_KEY}"
    response = requests.get(req_url)
    if response.status_code == 200:
        ticker_news = response.json()["results"]
        formatted_ticker_news = [{
            "polygon_id": result.get("id"),
            "ticker": ticker,
            "publisher": result.get("publisher", {}).get("name"),
            "author": result.get("author"),
            "published_at": result.get("published_utc"),
            "source_url": result.get("article_url"),
            "description": result.get("description")
        } for result in ticker_news]
        return formatted_ticker_news
    else:
        logger.error(
            f"Failed to get ticker news for {ticker}\nStatus code: {response.status_code}\nResponse: {response.text}"
        )
        return None
    
def backfill_ticker_news(ticker: str) -> Optional[List[Dict[str, Any]]]:
    req_url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&limit=50&apiKey={POLYGON_API_KEY}"
    response = requests.get(req_url)
    if response.status_code == 200:
        ticker_news = response.json()["results"]
        formatted_ticker_news = [{
            "polygon_id": result.get("id"),
            "ticker": ticker,
            "publisher": result.get("publisher", {}).get("name"),
            "author": result.get("author"),
            "published_at": result.get("published_utc"),
            "source_url": result.get("article_url"),
            "description": result.get("description")
        } for result in ticker_news]
        return formatted_ticker_news
    else:
        logger.error(
            f"Failed to get ticker news for {ticker}\nStatus code: {response.status_code}\nResponse: {response.text}"
        )
        return None