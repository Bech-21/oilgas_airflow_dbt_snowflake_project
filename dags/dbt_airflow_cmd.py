from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="running_oilandgas_dbt",
    default_args=default_args,
    description="Run dbt snapshots for SCD2",
    schedule_interval="@daily",     # or "@hourly" depending on your needs
    start_date=datetime(2025, 10,15),
    catchup=False,
    tags=["dbt", "snapshots"],
) as dag:

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/oilandgas_dbt && dbt seed --profiles-dir /home/airflow/oilandgas_dbt/.dbt"
        )
    dbt_run = BashOperator(
        task_id="dbt_run_files",
        bash_command="cd /opt/airflow/oilandgas_dbt && dbt run  --profiles-dir /home/airflow/oilandgas_dbt/.dbt"
    )


    dbt_seed >> dbt_run