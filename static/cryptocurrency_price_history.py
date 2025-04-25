#!/usr/bin/env python3
"""
Static pipeline for the Cryptocurrency Price History dataset.

Downloads the latest cryptocurrency price history dataset from Kaggle,
loads all CSV files into a Spark DataFrame, and writes the combined data to BigQuery.
"""
import os
import sys
from datetime import datetime

# Load environment variables from .env if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("[WARN] python-dotenv not installed; skipping .env loading")

# Ensure project root is in path for imports
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config.gcp_config import setup_gcp_auth
from config.env_config import PROJECT_ID, DATASET_NAME, GCS_BUCKET
from static.static_pipeline import load_kaggle_dataset, read_csv_to_spark, write_to_bigquery

# Authenticate to GCP
setup_gcp_auth()

def run_pipeline():
    """
    Main entrypoint for the cryptocurrency static data pipeline.
    """
    dataset_ref = "sudalairajkumar/cryptocurrencypricehistory"

    # 1. Download the dataset
    data_path = load_kaggle_dataset(dataset_ref)

    # 2. Load all CSV files into a single Spark DataFrame
    df = read_csv_to_spark(data_path)
    print("[INFO] Preview of loaded crypto data:")
    df.show(5)
    df.printSchema()

    # 3. Write DataFrame to BigQuery
    table_name = "crypto_price_history"
    rows_written = write_to_bigquery(df, table_name=table_name)
    print(f"[INFO] Pipeline complete. Total rows written: {rows_written}")

if __name__ == "__main__":
    print(f"[INFO] Starting cryptocurrency static pipeline at {datetime.now()}")
    run_pipeline()