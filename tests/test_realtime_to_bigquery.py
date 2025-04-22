import os
import sys
from pyspark.sql import SparkSession
import os

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
# Setup path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config.gcp_config import setup_gcp_auth
from config.env_config import PROJECT_ID, DATASET_NAME, GCS_BUCKET
from etl.alpha_vantage import (
    fetch_intraday_data,
    parse_alpha_vantage_json,
    write_to_bigquery,
)
from pyspark.sql import SparkSession

# Step 1: Auth
setup_gcp_auth()

# Step 2: Spark session


spark = (
    SparkSession.builder.appName("AlphaVantageToBigQuery")
    .config(
        "spark.jars.packages",
        ",".join(
            [
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
                "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.2",
            ]
        ),
    )
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


# Step 3: Fetch + transform
symbol = "AAPL"
json_data = fetch_intraday_data(symbol)
if json_data:
    df = parse_alpha_vantage_json(json_data, symbol, spark)
    df.show(5)

    # Step 4: Load to BigQuery
    write_to_bigquery(
        df,
        table_name="realtime_stock_data",
        dataset=DATASET_NAME,
        project=PROJECT_ID,
        gcs_bucket=GCS_BUCKET,
    )
    print(" Data written to BigQuery!")
else:
    print(" Could not fetch data.")
