from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add src directory to Python path
sys.path.insert(0, '/Users/eserogheneoghojafor/SkyLogix/src')

# Import the main functions from your scripts
from ingestion import run_ingestion
from mongo_to_postgres import run_transformation

default_args = {
    "owner": "skylogix",
    "depends_on_past": False,
    "start_date": datetime(2025, 12, 27),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def task_fetch_and_upsert_raw():
    """Call ingestion script"""
    return run_ingestion()

def task_transform_and_load_postgres():
    """Call transformation script"""
    return run_transformation()

with DAG(
    "weather_pipeline",
    default_args=default_args,
    schedule="*/15 * * * *",  # Changed from schedule_interval to schedule
    catchup=False,
    tags=['weather', 'etl']
) as dag:

    t1 = PythonOperator(
        task_id="fetch_and_upsert_raw",
        python_callable=task_fetch_and_upsert_raw
    )

    t2 = PythonOperator(
        task_id="transform_and_load_postgres",
        python_callable=task_transform_and_load_postgres
    )

    t1 >> t2