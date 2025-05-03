#!/usr/bin/env python3
"""Airflow DAG to run the realtime_test_stream.py long‐running process"""
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Compute project root dynamically (two levels up from this file)
DAG_FOLDER = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(DAG_FOLDER, "..", ".."))
SCRIPT_PATH = os.path.join(PROJECT_ROOT, "streaming", "realtime_test_stream.py")

default_args = {
    "owner": "anshkumar",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="realtime_test_stream",
    default_args=default_args,
    description="Launch the realtime_test_stream.py process",
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    run_realtime = BashOperator(
        task_id="run_realtime_test_stream",
        bash_command=f"python3 {SCRIPT_PATH}",
    )

    run_realtime
