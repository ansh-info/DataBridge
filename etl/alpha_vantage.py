import os
import time
from itertools import cycle
import json
import requests
from dotenv import load_dotenv
from datetime import datetime

from pyspark.sql import Row

# Load environment variables
load_dotenv()

# Fetch and rotate API keys
api_keys_raw = os.getenv("ALPHA_VANTAGE_KEYS")
if not api_keys_raw:
    raise ValueError("ALPHA_VANTAGE_KEYS not found in .env file.")

API_KEYS = api_keys_raw.split(",")
key_cycle = cycle(API_KEYS)


def fetch_intraday_data(symbol="AAPL", interval="1min", max_attempts=4):
    base_url = "https://www.alphavantage.co/query"

    for attempt in range(max_attempts):
        api_key = next(key_cycle)
        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": interval,
            "apikey": api_key,
            "datatype": "json",
        }

        print(f"[INFO] Attempting with API key: {api_key}")
        response = requests.get(base_url, params=params)

        if response.status_code == 200:
            data = response.json()
            # Check if actual data exists
            if "Time Series (1min)" in data:
                print(f"[SUCCESS] Data fetched using API key: {api_key}")
                return data
            else:
                print(f"[WARN] Rate limit hit or data not available for key: {api_key}")
                print("[DEBUG] Response:", json.dumps(data, indent=2))
        else:
            print(
                f"[ERROR] API call failed with status {response.status_code} using key {api_key}"
            )

        time.sleep(1)  # Slight delay before trying next key

    raise RuntimeError("All API keys failed or exceeded rate limits.")


def parse_alpha_vantage_json(json_data, symbol, spark):

    if "Time Series (1min)" not in json_data:
        print("[DEBUG] Alpha Vantage response:\n", json.dumps(json_data, indent=2))
        raise ValueError("No 'Time Series (1min)' found")

    time_series = json_data["Time Series (1min)"]

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


def write_to_bigquery(df, table_name, dataset, project, gcs_bucket):
    df.write.format("bigquery").option(
        "table", f"{project}.{dataset}.{table_name}"
    ).option("temporaryGcsBucket", gcs_bucket).mode("append").save()
