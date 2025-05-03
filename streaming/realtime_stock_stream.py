#!/usr/bin/env python3
"""
Real-time stock data pipeline.
Fetches the latest intraday data point for each symbol every 5 minutes
and appends it to BigQuery.
"""
import os
import sys
import time
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession, Row
from google.cloud import bigquery
import pandas as pd

# Configure Spark local IP (for macOS)
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Ensure project root is on Python path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# Project-specific imports
from config.gcp_config import setup_gcp_auth
from config.env_config import PROJECT_ID, DATASET_NAME
from etl.alpha_vantage import fetch_intraday_data

# Step 1: Authentication and environment
setup_gcp_auth()
load_dotenv()
STOCK_SYMBOLS = os.getenv("STOCK_SYMBOLS", "AAPL").split(",")
print(f"[INFO] Real-time pipeline symbols: {', '.join(STOCK_SYMBOLS)}")

# Step 2: Spark session for any DataFrame construction
spark = (
    SparkSession.builder.appName("RealTimeStockStream")
    .config(
        "spark.jars.packages",
        ",".join([
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
        ])
    )
    .config("spark.executor.userClassPathFirst", "true")
    .config("spark.driver.userClassPathFirst", "true")
    .getOrCreate()
)

def generate_live_data(symbol):
    """
    Fetch the most recent intraday data point for a symbol using Alpha Vantage.
    Returns a Spark DataFrame with a single row.
    """
    data = fetch_intraday_data(symbol)
    if not data or "Time Series (1min)" not in data:
        raise RuntimeError(f"No time series data for {symbol}")

    time_series = data["Time Series (1min)"]
    # Get the latest timestamp
    latest_ts = max(time_series.keys())
    values = time_series[latest_ts]
    ts = datetime.strptime(latest_ts, "%Y-%m-%d %H:%M:%S")

    row = Row(
        symbol=symbol,
        timestamp=ts,
        open=float(values.get("1. open", 0)),
        high=float(values.get("2. high", 0)),
        low=float(values.get("3. low", 0)),
        close=float(values.get("4. close", 0)),
        volume=int(float(values.get("5. volume", 0))),
    )
    return spark.createDataFrame([row])

def main():
    # BigQuery client and settings
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_NAME}.streaming_stock_data"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("open", "FLOAT"),
            bigquery.SchemaField("high", "FLOAT"),
            bigquery.SchemaField("low", "FLOAT"),
            bigquery.SchemaField("close", "FLOAT"),
            bigquery.SchemaField("volume", "INTEGER"),
        ],
        write_disposition="WRITE_APPEND",
    )

    print(f"[INFO] Starting real-time pipeline, writing to {table_id}")
    while True:
        for symbol in STOCK_SYMBOLS:
            try:
                df = generate_live_data(symbol)
                pd_df = df.toPandas()
                # Ensure volume is integer
                pd_df["volume"] = pd_df["volume"].astype(int)
                job = client.load_table_from_dataframe(
                    pd_df, table_id, job_config=job_config
                )
                job.result()
                print(
                    f"[SUCCESS] Appended 1 row for {symbol} at {pd_df['timestamp'][0]}"
                )
            except Exception as e:
                print(f"[ERROR] Failed to append for {symbol}: {e}")
        print("[INFO] Sleeping for 5 minutes...")
        time.sleep(5 * 60)

if __name__ == "__main__":
    main()