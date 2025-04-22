import requests
import time
import os
from itertools import cycle
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Fetch and rotate API keys
api_keys_raw = os.getenv("ALPHA_VANTAGE_KEYS")
if not api_keys_raw:
    raise ValueError("ALPHA_VANTAGE_KEYS not found in .env file.")

API_KEYS = api_keys_raw.split(",")
key_cycle = cycle(API_KEYS)


def fetch_intraday_data(symbol="AAPL", interval="1min"):
    base_url = "https://www.alphavantage.co/query"

    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": interval,
        "apikey": next(key_cycle),
        "datatype": "json",
    }

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        print(f"Data fetched for {symbol} at {interval} interval.")
        return response.json()
    else:
        print("API Error:", response.status_code)
        return None
