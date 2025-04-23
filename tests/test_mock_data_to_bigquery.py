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
from etl.alpha_vantage import write_to_bigquery

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


def generate_mock_stock_data(symbol, num_records=100):
    """
    Generate mock stock market data similar to Alpha Vantage intraday data.

    Args:
        symbol (str): Stock symbol (e.g., 'AAPL')
        num_records (int): Number of records to generate

    Returns:
        PySpark DataFrame with mock stock data
    """
    print(f"[INFO] Generating {num_records} mock records for {symbol}")

    # Start time (now) and work backwards by 1-minute intervals
    end_time = datetime.now().replace(second=0, microsecond=0)

    # Create random but realistic stock data
    base_price = 150.0  # Starting price for AAPL-like stock
    price_volatility = 0.005  # 0.5% volatility between records

    rows = []
    current_price = base_price

    for i in range(num_records):
        # Calculate timestamp (going backward in time)
        timestamp = end_time - timedelta(minutes=i)

        # Generate realistic price movement (random walk with drift)
        price_change = current_price * price_volatility * (random.random() * 2 - 1)
        current_price = max(
            0.01, current_price + price_change
        )  # Ensure price stays positive

        # Calculate other values based on current_price
        open_price = current_price * (1 + random.uniform(-0.001, 0.001))
        high_price = current_price * (1 + random.uniform(0, 0.002))
        low_price = current_price * (1 - random.uniform(0, 0.002))
        close_price = current_price
        volume = int(random.uniform(10000, 1000000))

        # Create row
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

    # Create DataFrame from the generated data
    df = spark.createDataFrame(rows)
    return df


# Step 3: Generate mock data
symbol = "AAPL"
df = generate_mock_stock_data(symbol, num_records=120)  # 2 hours of minute data

# Preview the data
print("[INFO] Preview of mock data:")
df.show(5)
df.printSchema()

# Step 4: Write to BigQuery directly using BigQuery client library
try:
    # Convert Spark DataFrame to Pandas
    pandas_df = df.toPandas()
    print(
        f"[INFO] Successfully converted Spark DataFrame to Pandas ({len(pandas_df)} rows)"
    )

    # Initialize BigQuery client
    client = bigquery.Client(project=PROJECT_ID)

    # Define table reference
    table_id = f"{PROJECT_ID}.{DATASET_NAME}.mock_stock_data"

    # Configure the load job
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

    # Load the data
    job = client.load_table_from_dataframe(pandas_df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"[SUCCESS] Loaded {len(pandas_df)} rows into {table_id}")

except Exception as e:
    print(f"[ERROR] Failed to write to BigQuery: {e}")
    print("[INFO] Saving data to CSV as fallback...")

    # Save to CSV as a fallback
    csv_path = "mock_stock_data.csv"
    df.toPandas().to_csv(csv_path, index=False)
    print(f"[INFO] Data saved to {csv_path} for inspection")
