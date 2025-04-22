import os
import time
from itertools import cycle

import requests
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


from datetime import datetime

from pyspark.sql import Row


def parse_alpha_vantage_json(json_data, symbol, spark):
    time_series = json_data.get("Time Series (1min)", {})

    if not time_series:
        raise ValueError("No 'Time Series (1min)' found")

    rows = []
    for timestamp_str, values in time_series.items():
        row = Row(
            symbol=symbol,
            timestamp=datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S"),
            open=float(values.get("1. open", 0)),
            high=float(values.get("2. high", 0)),
            low=float(values.get("3. low", 0)),
            close=float(values.get("4. close", 0)),
            volume=float(values.get("5. volume", 0)),
        )
        rows.append(row)

    df = spark.createDataFrame(rows)
    return df
