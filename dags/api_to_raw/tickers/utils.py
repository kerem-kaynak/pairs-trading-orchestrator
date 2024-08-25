import pendulum
import requests
import os
from typing import Any, Dict, List, Optional
from plugins.logger import logger


POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")

def get_etf_tickers() -> Optional[List[Dict[str, Any]]]:
    req_url = f"https://api.polygon.io/v3/reference/tickers?type=ETF&market=stocks&exchange=XNAS&active=true&?date=2023-01-01&limit=1000&apiKey={POLYGON_API_KEY}"
    response = requests.get(req_url)
    if response.status_code == 200:
        tickers = response.json()["results"]
        return tickers
    else:
        logger.error(
            f"Failed to get ETF tickers\nStatus code: {response.status_code}\nResponse: {response.text}"
        )
        return None