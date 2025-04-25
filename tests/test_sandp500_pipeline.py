#!/usr/bin/env python3
"""
Test for the S&P 500 static pipeline: verifies CSV loading and BigQuery write.
"""
import os
import sys
import tempfile
import shutil
import pandas as pd

# Ensure Spark uses localhost
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
# Add project root to path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config.gcp_config import setup_gcp_auth
from config.env_config import PROJECT_ID, DATASET_NAME, GCS_BUCKET
from static.static_pipeline import read_csv_to_spark, write_to_bigquery

# Step 1: Authenticate to GCP
setup_gcp_auth()

# Step 2: Create temporary directory with sample CSV files
temp_dir = tempfile.mkdtemp(prefix='sandp500_test_')
try:
    # Create sample data
    sample = pd.DataFrame([
        {"symbol": "AAA", "price": 100.0},
        {"symbol": "BBB", "price": 200.0},
    ])
    # Write multiple CSVs to simulate multiple files
    sample.to_csv(os.path.join(temp_dir, 'data1.csv'), index=False)
    sample.to_csv(os.path.join(temp_dir, 'data2.csv'), index=False)

    # Step 3: Load CSVs into Spark DataFrame
    df = read_csv_to_spark(temp_dir)
    print("[INFO] Preview of loaded S&P 500 test DataFrame:")
    df.show()
    df.printSchema()

    # Step 4: Write to BigQuery
    table_name = "sandp500_test_data"
    written = write_to_bigquery(df, table_name=table_name)
    print(f"[INFO] Wrote {written} rows to {PROJECT_ID}.{DATASET_NAME}.{table_name}")
finally:
    # Clean up temporary files
    shutil.rmtree(temp_dir)