import os
import sys
from pyspark.sql import SparkSession
import pandas as pd
from google.cloud import bigquery

# Setup environment
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Setup path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config.gcp_config import setup_gcp_auth
from config.env_config import PROJECT_ID, DATASET_NAME, GCS_BUCKET
from etl.alpha_vantage import fetch_intraday_data, parse_alpha_vantage_json

# Step 1: Auth
setup_gcp_auth()

# Step 2: Spark session - simplified configuration to avoid dependency conflicts
spark = (
    SparkSession.builder.appName("AlphaVantageToBigQuery")
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
    Write a Spark DataFrame to BigQuery using the BigQuery Python client
    instead of the Spark BigQuery connector.

    Args:
        df: Spark DataFrame to write
        table_name: BigQuery table name
        dataset: BigQuery dataset name
        project: GCP project ID
    """
    try:
        # Convert Spark DataFrame to Pandas
        pandas_df = df.toPandas()
        print(
            f"[INFO] Successfully converted Spark DataFrame to Pandas ({len(pandas_df)} rows)"
        )

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

    except Exception as e:
        print(f"[ERROR] Failed to write to BigQuery: {e}")
        print("[INFO] Saving data to CSV as fallback...")

        # Save to CSV as a fallback
        csv_path = f"{table_name}_data.csv"
        df.toPandas().to_csv(csv_path, index=False)
        print(f"[INFO] Data saved to {csv_path} for inspection")


# Step 3: Fetch + transform data from Alpha Vantage
try:
    symbol = "AAPL"
    print(f"[INFO] Fetching real-time data for {symbol}...")
    json_data = fetch_intraday_data(symbol)

    if json_data:
        # Parse JSON into Spark DataFrame
        df = parse_alpha_vantage_json(json_data, symbol, spark)
        print("[INFO] Preview of fetched data:")
        df.show(5)
        df.printSchema()

        # Step 4: Write to BigQuery using direct method
        write_to_bigquery_direct(
            df,
            table_name="realtime_stock_data",
            dataset=DATASET_NAME,
            project=PROJECT_ID,
        )
    else:
        print("[ERROR] Could not fetch data from Alpha Vantage.")
except RuntimeError as e:
    print(f"[ERROR] {e}")
    print(
        "[INFO] Consider waiting for API rate limit to reset or adding more API keys."
    )
except Exception as e:
    print(f"[ERROR] An unexpected error occurred: {e}")
