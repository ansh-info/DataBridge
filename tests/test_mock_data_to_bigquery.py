import os
import sys
from datetime import datetime, timedelta
import random
import pandas as pd
from google.cloud import bigquery

# Use localhost for Spark
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Setup path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from pyspark.sql import SparkSession, Row
from config.gcp_config import setup_gcp_auth
from config.env_config import PROJECT_ID, DATASET_NAME, GCS_BUCKET
# Import write utility (not directly used here)
from etl.alpha_vantage import write_to_bigquery
# Load configured stock symbols from environment
STOCK_SYMBOLS = os.getenv("STOCK_SYMBOLS", "AAPL").split(",")

# Step 1: Auth
setup_gcp_auth()

# Step 2: Spark session
spark = (
    SparkSession.builder.appName("MockDataToBigQuery")
    .config(
        "spark.jars.packages",
        ",".join(
            [
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2",
            ]
        ),
    )
    # Remove problematic exclusion and use a cleaner approach
    .config(
        "spark.executor.userClassPathFirst", "true"
    )  # Prioritize user-specified dependencies
    .config("spark.driver.userClassPathFirst", "true")
    .config(
        "spark.hadoop.fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    )
    .config(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    )
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        "config/dbt-user-creds.json",
    )
    .getOrCreate()
)


def generate_mock_stock_data(symbol, num_days=15):
    """
    Generate mock daily stock data for the past N days.

    Args:
        symbol (str): Stock symbol (e.g., 'AAPL')
        num_days (int): Number of days (including today) to generate

    Returns:
        PySpark DataFrame with mock daily stock data
    """
    print(f"[INFO] Generating {num_days} mock daily records for {symbol}")

    # Start date (today at midnight) and work backwards by 1-day intervals
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # Create random but realistic daily stock data
    base_price = 150.0
    volatility = 0.02  # 2% daily volatility
    rows = []
    current_price = base_price

    for i in range(num_days):
        timestamp = end_date - timedelta(days=i)
        # Random daily price change
        price_change = current_price * volatility * (random.random() * 2 - 1)
        current_price = max(0.01, current_price + price_change)
        open_price = current_price * (1 + random.uniform(-0.005, 0.005))
        high_price = max(open_price, current_price * (1 + random.uniform(0, 0.01)))
        low_price = min(open_price, current_price * (1 - random.uniform(0, 0.01)))
        close_price = current_price
        volume = int(random.uniform(100000, 5000000))

        row = Row(
            symbol=symbol,
            timestamp=timestamp,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
        )
        rows.append(row)

    return spark.createDataFrame(rows)


# Step 3 & 4: Generate mock daily data for the past 15 days and write to BigQuery per stock
for symbol in STOCK_SYMBOLS:
    df = generate_mock_stock_data(symbol, num_days=15)  # 15 days of daily data

    # Preview the data
    print(f"[INFO] Preview of mock data for {symbol}:")
    df.show(5)
    df.printSchema()

    # Write to BigQuery directly using BigQuery client library
    try:
        pandas_df = df.toPandas()
        print(f"[INFO] Successfully converted Spark DataFrame to Pandas ({len(pandas_df)} rows) for {symbol}")

        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_NAME}.mock_stock_data"
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

        job = client.load_table_from_dataframe(pandas_df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete

        print(f"[SUCCESS] Loaded {len(pandas_df)} rows into {table_id} for {symbol}")
    except Exception as e:
        print(f"[ERROR] Failed to write to BigQuery for {symbol}: {e}")
        print("[INFO] Saving data to CSV as fallback...")

        csv_path = f"mock_stock_data_{symbol}.csv"
        df.toPandas().to_csv(csv_path, index=False)
        print(f"[INFO] Data saved to {csv_path} for inspection")
