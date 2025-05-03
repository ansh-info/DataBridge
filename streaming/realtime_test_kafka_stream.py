#!/usr/bin/env python3
"""
Test real-time stock data pipeline.
Generates a new test data point every 5 minutes for each symbol and appends to BigQuery.
"""
import os
import sys
import time
import random
from datetime import datetime, timedelta

# Load env
from dotenv import load_dotenv

load_dotenv()

# Setup Python path to project root
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# GCP and Spark imports
from config.gcp_config import setup_gcp_auth
from config.env_config import PROJECT_ID, DATASET_NAME
from kafka_utils.producer_config import get_kafka_producer, get_kafka_topic
import json
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, to_date, date_format
from google.cloud import bigquery

# Configure Spark local IP for Mac
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Authenticate to GCP
setup_gcp_auth()
# Initialize Kafka producer
producer = get_kafka_producer()
KAFKA_TOPIC = get_kafka_topic()
print(f"[INFO] Kafka producer initialized, sending to topic '{KAFKA_TOPIC}'")

# Read symbols
STOCK_SYMBOLS = os.getenv("STOCK_SYMBOLS", "AAPL").split(",")
print(f"[INFO] Realtime pipeline symbols: {', '.join(STOCK_SYMBOLS)}")

# Initialize price state for each symbol
price_state = {sym: 150.0 for sym in STOCK_SYMBOLS}

# Build Spark session
spark = (
    SparkSession.builder.appName("TestRealtimeStockStream")
    .config(
        "spark.jars.packages",
        ",".join(
            [
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2",
            ]
        ),
    )
    .config("spark.executor.userClassPathFirst", "true")
    .config("spark.driver.userClassPathFirst", "true")
    .getOrCreate()
)


def generate_live_test_data(symbol, interval_minutes=5):
    """
    Generate a single test data point at the nearest past interval for a symbol.
    """
    # Determine timestamp floor to interval
    now = datetime.now().replace(second=0, microsecond=0)
    minute_mod = now.minute % interval_minutes
    ts = now - timedelta(minutes=minute_mod)
    # Simulate price update
    current = price_state.get(symbol, 150.0)
    change = current * 0.005 * (random.random() * 2 - 1)
    current = max(0.01, current + change)
    price_state[symbol] = current
    open_price = current * (1 + random.uniform(-0.002, 0.002))
    high_price = max(open_price, current * (1 + random.uniform(0, 0.005)))
    low_price = min(open_price, current * (1 - random.uniform(0, 0.005)))
    close_price = current
    volume = int(random.uniform(10000, 1000000))
    # Build DataFrame
    row = Row(
        symbol=symbol,
        timestamp=ts,
        open=open_price,
        high=high_price,
        low=low_price,
        close=close_price,
        volume=volume,
    )
    df = spark.createDataFrame([row])
    # Split date/time
    df = df.withColumn("date", to_date(col("timestamp"))).withColumn(
        "time", date_format(col("timestamp"), "HH:mm:ss")
    )
    return df


def main():
    # BigQuery settings
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET_NAME}.realtime_test_stock_data"
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("time", "STRING"),
            bigquery.SchemaField("open", "FLOAT"),
            bigquery.SchemaField("high", "FLOAT"),
            bigquery.SchemaField("low", "FLOAT"),
            bigquery.SchemaField("close", "FLOAT"),
            bigquery.SchemaField("volume", "INTEGER"),
        ],
        write_disposition="WRITE_APPEND",
    )
    print(f"[INFO] Starting test real-time pipeline, writing to {table_id}")
    # Loop indefinitely
    while True:
        for symbol in STOCK_SYMBOLS:
            try:
                df = generate_live_test_data(symbol)
                pd_df = df.toPandas()
                # Send record to Kafka via confluent-kafka
                # Convert to dict and serialize datetime/date fields
                record = pd_df.to_dict(orient="records")[0]
                # Serialize timestamp to ISO format
                record["timestamp"] = record["timestamp"].isoformat()
                # Ensure date/time columns are strings
                record["date"] = str(record.get("date"))
                record["time"] = str(record.get("time"))
                # Produce JSON message to Kafka
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=record["symbol"],
                    value=json.dumps(record),
                )
                # Ensure delivery
                producer.flush()
                print(f"[INFO] Produced to Kafka for {symbol}: {record}")
                # Append to BigQuery as before
                job = client.load_table_from_dataframe(
                    pd_df, table_id, job_config=job_config
                )
                job.result()
                print(
                    f"[SUCCESS] Appended 1 row for {symbol} at {pd_df['timestamp'][0]}"
                )
            except Exception as e:
                print(f"[ERROR] Failed to append for {symbol}: {e}")
        # Wait until next interval
        print(f"[INFO] Sleeping for 5 minutes...")
        time.sleep(5 * 60)


if __name__ == "__main__":
    main()
