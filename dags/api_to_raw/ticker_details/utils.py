import requests
import os
from typing import Any, Dict, List, Optional
from plugins.logger import logger


POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")

def get_ticker_details(ticker: str) -> Optional[Dict[str, Any]]:
    req_url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={POLYGON_API_KEY}"
    response = requests.get(req_url)
    if response.status_code == 200:
        ticker_details = response.json()["results"]
        return ticker_details
    else:
        logger.error(
            f"Failed to get ticker details for {ticker}\nStatus code: {response.status_code}\nResponse: {response.text}"
        )
        return None