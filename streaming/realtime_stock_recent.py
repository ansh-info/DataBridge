import os
import sys
import time
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

# Setup environment
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Setup path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config.gcp_config import setup_gcp_auth
from config.env_config import PROJECT_ID, DATASET_NAME
from etl.alpha_vantage import fetch_intraday_data, parse_alpha_vantage_json

# Step 1: Auth
setup_gcp_auth()

# Load environment variables to get stock symbols
load_dotenv()
STOCK_SYMBOLS = os.getenv("STOCK_SYMBOLS", "AAPL").split(",")
print(f"[INFO] Configured to monitor the following stocks: {', '.join(STOCK_SYMBOLS)}")

# Step 2: Spark session - simplified configuration
spark = (
    SparkSession.builder.appName("StockDataStream")
    .config(
        "spark.jars.packages",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
    )
    .config("spark.executor.userClassPathFirst", "true")
    .config("spark.driver.userClassPathFirst", "true")
    .getOrCreate()
)


def write_to_bigquery_direct(df, table_name, dataset, project):
    """
    Write a Spark DataFrame to BigQuery using the BigQuery Python client.
    """
    try:
        # Convert Spark DataFrame to Pandas
        pandas_df = df.toPandas()
        if pandas_df.empty:
            print("[INFO] No new data to write to BigQuery")
            return 0

        print(f"[INFO] Writing {len(pandas_df)} rows to BigQuery")

        # Initialize BigQuery client
        client = bigquery.Client(project=project)

        # Define table reference
        table_id = f"{project}.{dataset}.{table_name}"

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
        job = client.load_table_from_dataframe(
            pandas_df, table_id, job_config=job_config
        )
        job.result()  # Wait for the job to complete

        print(f"[SUCCESS] Loaded {len(pandas_df)} rows into {table_id}")
        return len(pandas_df)

    except Exception as e:
        print(f"[ERROR] Failed to write to BigQuery: {e}")
        print("[INFO] Saving data to CSV as fallback...")

        # Save to CSV as a fallback
        csv_path = f"{table_name}_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.toPandas().to_csv(csv_path, index=False)
        print(f"[INFO] Data saved to {csv_path} for inspection")
        return 0


def get_most_recent_data(df, num_records=5):
    """
    Get the most recent records by timestamp, regardless of how old they are.

    Args:
        df: Spark DataFrame with timestamp column
        num_records: Number of most recent records to return

    Returns:
        Spark DataFrame with only the most recent records
    """
    from pyspark.sql.functions import desc

    # Sort by timestamp in descending order and take the N most recent records
    recent_df = df.orderBy(desc("timestamp")).limit(num_records)

    return recent_df


def process_stock_data(symbol):
    """
    Process a single stock symbol.
    Returns the number of records written to BigQuery.
    """
    try:
        print(f"[INFO] Fetching real-time data for {symbol}...")
        json_data = fetch_intraday_data(symbol)

        if json_data and "Time Series (1min)" in json_data:
            # Parse JSON into Spark DataFrame
            df = parse_alpha_vantage_json(json_data, symbol, spark)

            # Get the 5 most recent records regardless of timestamp
            recent_df = get_most_recent_data(df, num_records=5)

            count_recent = recent_df.count()
            print(
                f"[INFO] Retrieved {df.count()} total records, taking {count_recent} most recent records"
            )

            if count_recent > 0:
                print("[INFO] Preview of recent data:")
                recent_df.show(5)

                # Write to BigQuery
                return write_to_bigquery_direct(
                    recent_df,
                    table_name="streaming_stock_data",
                    dataset=DATASET_NAME,
                    project=PROJECT_ID,
                )
            else:
                print(f"[WARN] No data available for {symbol}")
                return 0
        else:
            print(f"[WARN] Could not fetch time series data for {symbol}")
            return 0

    except RuntimeError as e:
        print(f"[ERROR] {e}")
        print(
            "[INFO] Consider waiting for API rate limit to reset or adding more API keys."
        )
        return 0
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred while processing {symbol}: {e}")
        return 0


def run_pipeline():
    """
    Main function to run the pipeline for all configured stocks.
    """
    total_records = 0
    for symbol in STOCK_SYMBOLS:
        try:
            records = process_stock_data(symbol)
            total_records += records

            # Wait a bit between API calls to avoid rate limits
            if symbol != STOCK_SYMBOLS[-1]:  # Don't wait after the last symbol
                print(f"[INFO] Waiting 2 seconds before processing next stock...")
                time.sleep(2)

        except Exception as e:
            print(f"[ERROR] Failed to process {symbol}: {e}")

    print(f"[INFO] Pipeline run complete. Total records processed: {total_records}")


if __name__ == "__main__":
    print(f"[INFO] Starting stock data streaming pipeline at {datetime.now()}")
    run_pipeline()
