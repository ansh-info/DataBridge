import os
import sys

import os

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
# Ensure project root is in path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from config.gcp_config import setup_gcp_auth
from config.env_config import PROJECT_ID
from etl.alpha_vantage import fetch_intraday_data, parse_alpha_vantage_json
from pyspark.sql import SparkSession

# Step 1: Auth
setup_gcp_auth()

# Step 2: Init Spark
spark = (
    SparkSession.builder.appName("AlphaVantageToDF")
    .config(
        "spark.jars.packages",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
    )
    .getOrCreate()
)

# Step 3: Fetch real-time data
symbol = "AAPL"
json_data = fetch_intraday_data(symbol)

# Step 4: Parse JSON into PySpark DataFrame
if json_data:
    df = parse_alpha_vantage_json(json_data, symbol, spark)
    print(" DataFrame created successfully!")
    df.printSchema()
    df.show(10)
else:
    print(" Failed to fetch or parse data.")
