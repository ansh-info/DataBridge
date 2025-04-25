#!/usr/bin/env python3
"""
Static data pipeline.

Downloads a dataset from Kaggle, loads it into a Spark DataFrame,
and writes the data into BigQuery using the Spark BigQuery connector.
"""
import os
import sys
from datetime import datetime

# Load environment variables from .env if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("[WARN] python-dotenv not installed; skipping .env loading")
    # .env support is optional; ensure required env vars are set
    pass
import glob
from pyspark.sql import SparkSession

# Setup environment
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Ensure project root is in path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config.gcp_config import setup_gcp_auth
from config.env_config import PROJECT_ID, DATASET_NAME, GCS_BUCKET

# Authenticate to GCP
setup_gcp_auth()

# Initialize Spark session with GCS connector for BigQuery temp bucket support
spark = (
    SparkSession.builder.appName("StaticDataPipeline")
    .config(
        "spark.jars.packages",
        ",".join([
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2",
        ]),
    )
    .config("spark.executor.userClassPathFirst", "true")
    .config("spark.driver.userClassPathFirst", "true")
    # Enable GCS filesystem support
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    # Use service account for GCS auth
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "config/dbt-user-creds.json"),
    )
    .getOrCreate()
)

def load_kaggle_dataset(dataset_ref: str) -> str:
    """
    Download the latest version of the Kaggle dataset using kagglehub.
    Returns the path to the extracted dataset directory.
    """
    try:
        import kagglehub
    except ImportError as e:
        raise ImportError(
            "kagglehub is required for the static pipeline; please install it via `pip install kagglehub kaggle`."
        ) from e
    path = kagglehub.dataset_download(dataset_ref)
    print(f"[INFO] Dataset downloaded to: {path}")
    return path

def read_csv_to_spark(path: str):
    """
    Read all CSV files under the given directory into a Spark DataFrame.
    Only files ending with .csv (case-insensitive) are loaded.
    """
    # Discover CSV files via glob
    pattern = os.path.join(path, '**', '*.csv')
    csv_files = glob.glob(pattern, recursive=True)
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found under '{path}' to load.")
    print(f"[INFO] Found {len(csv_files)} CSV files under {path}")
    df = (
        spark.read
             .option("header", "true")
             .option("inferSchema", "true")
             .csv(csv_files)
    )
    return df

def write_to_bigquery(df, table_name: str) -> int:
    """
    Write Spark DataFrame to BigQuery using the Spark BigQuery connector.
    """
    destination = f"{PROJECT_ID}.{DATASET_NAME}.{table_name}"
    print(f"[INFO] Writing DataFrame to BigQuery at {destination}")
    df.write.format("bigquery") \
        .option("table", destination) \
        .option("temporaryGcsBucket", GCS_BUCKET) \
        .mode("append") \
        .save()
    count = df.count()
    print(f"[SUCCESS] Wrote {count} rows to {destination}")
    return count

def run_pipeline():
    """
    Main entrypoint for the static data pipeline.
    """
    dataset_ref = "camnugent/sandp500"

    # 1. Download the dataset
    data_path = load_kaggle_dataset(dataset_ref)

    # 2. Load CSV(s) into Spark DataFrame
    df = read_csv_to_spark(data_path)
    print("[INFO] Preview of loaded data:")
    df.show(5)
    df.printSchema()

    # 3. Write to BigQuery
    rows_written = write_to_bigquery(df, table_name="sandp500_data")
    print(f"[INFO] Pipeline complete. Total rows written: {rows_written}")

if __name__ == "__main__":
    print(f"[INFO] Starting static data pipeline at {datetime.now()}")
    run_pipeline()