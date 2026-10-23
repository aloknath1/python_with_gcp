from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from access_api import access_api, run_pipeline

default_args = {
    "owner": "alok",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="classic_python_operator_example",
    default_args=default_args,
    description="Fetch API data, upload CSV to GCS, and validate records",
    start_date=datetime(2026, 10, 19),
    schedule=None,
    catchup=False,
    tags=["api","gcs","csv","validation"]
) as dag:
    
    fetch_and_upload_csv = PythonOperator(
        task_id="fetch_api_and_upload_csv",
        python_callable=access_api,
        depends_on_past=False,
    )
    

    run_pipeline = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
        depends_on_past=False,
    )
