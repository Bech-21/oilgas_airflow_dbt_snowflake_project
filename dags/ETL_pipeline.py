import os
import sys
import csv
import logging
from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.python import PythonOperator
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipeline.bronze_layer import bronze_etl
from pipeline.bronze_counties_layer import counties_bronze_etl
from pipeline.bronze_earthquake_layer import earthquake_bronze_etl


default_args = {
    'owner': 'bech',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

def postgres_to_s3():
    print("This is where the logic to move data from Postgres to S3 would go.")

with DAG(
    dag_id="ETL_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 10,13 ),
    schedule_interval='@daily'
) as dag:
    bronze_rig_count_pipeline = PythonOperator(
        task_id="bronze_rig_count_pipeline",
        python_callable=bronze_etl
    ),
    load_counties_task  = PythonOperator(
        task_id="load_counties_to_snowflake",
        python_callable=counties_bronze_etl
    ),
    load_earthquake_task  = PythonOperator(
        task_id="load_earthquake_to_snowflake",
        python_callable=earthquake_bronze_etl
    ),
    [bronze_rig_count_pipeline, load_counties_task,load_earthquake_task] 