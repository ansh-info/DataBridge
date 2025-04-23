from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import sys
import os

# Add your project root to Python path so we can import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Optional: Import your script directly to call functions
# from streaming.realtime_stock_stream import run_pipeline

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "stock_data_pipeline",
    default_args=default_args,
    description="Fetch stock data from Alpha Vantage and load to BigQuery",
    schedule="*/5 * * * *",  # Run every 5 minutes
    start_date=datetime(2025, 4, 23),
    catchup=False,
)

# Method 1: Run the script directly using BashOperator
fetch_stock_data = BashOperator(
    task_id="fetch_stock_data",
    bash_command=(
        "cd /Users/anshkumar/Developer/code/git/DataManagement2 && "
        "conda run -n data_env python streaming/realtime_stock_recent.py"
    ),
    dag=dag,
)

# Uncomment this section if you prefer to call your function directly
# # Method 2: Call the function directly using PythonOperator
# def run_stock_pipeline_wrapper():
#     from streaming.realtime_stock_stream import run_pipeline
#     run_pipeline()
#
# fetch_stock_data = PythonOperator(
#     task_id='fetch_stock_data',
#     python_callable=run_stock_pipeline_wrapper,
#     dag=dag,
# )
