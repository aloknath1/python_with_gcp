
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import (
    GCSObjectExistenceSensor,
)

# GCP configuration
PROJECT_ID = "test-project-511411"
REGION = "asia-south2"
CLUSTER = "cluster-d2f4"

BQ_DATASET = "dataset001"
BQ_TABLE = "employee_processed"

RAW_BUCKET = "raw-data-bucket01"
PROCESSED_BUCKET = "processed-data-bucket01"

INPUT_FILE = "employee_data.csv"
PYSPARK_FILE = f"gs://{RAW_BUCKET}/scripts/employee_etl.py"
PROCESSED_OBJECT = "employee_data_processed/*.parquet"

OUTPUT_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


# XCom producer
def push_input_path(**context):
    input_file = f"gs://{RAW_BUCKET}/{INPUT_FILE}"

    context["ti"].xcom_push(
        key="input_file_path",
        value=input_file,
    )

    print(f"Pushed input file path: {input_file}")


# XCom consumer
def read_xcom(**context):
    file_path = context["ti"].xcom_pull(
        task_ids="push_input_path",
        key="input_file_path",
    )

    print(f"Received from XCom: {file_path}")


with DAG(
    dag_id="gcp_employee_etl_pipeline",
    default_args=default_args,
    description=(
        "End-to-end GCS, Dataproc and BigQuery employee ETL pipeline"
    ),
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["gcp", "dataproc", "bigquery", "composer"],
) as dag:

    # Task 1: Wait for the input CSV in GCS
    check_gcs_file = GCSObjectExistenceSensor(
        task_id="check_gcs_file",
        bucket=RAW_BUCKET,
        object=INPUT_FILE,
        timeout=300,
        poke_interval=10,
        mode="poke",
    )

    # Task 2: Push the input path to XCom
    push_xcom_task = PythonOperator(
        task_id="push_input_path",
        python_callable=push_input_path,
    )

    # Task 3: Submit the PySpark job to Dataproc
    pyspark_job = {
        "reference": {
            "project_id": PROJECT_ID,
        },
        "placement": {
            "cluster_name": CLUSTER,
        },
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_FILE,
            "args": [
                f"gs://{RAW_BUCKET}/{INPUT_FILE}",
                f"gs://{PROCESSED_BUCKET}/employee_data_processed/",
            ],
        },
    }

    run_dataproc_job = DataprocSubmitJobOperator(
        task_id="run_dataproc_pyspark_job",
        project_id=PROJECT_ID,
        region=REGION,
        job=pyspark_job,
    )

    # Task 4: Load processed Parquet files into BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery",
        bucket=PROCESSED_BUCKET,
        source_objects=[PROCESSED_OBJECT],
        destination_project_dataset_table=OUTPUT_TABLE,
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    # Task 5: Validate the loaded data
    validation_query = BigQueryInsertJobOperator(
        task_id="bigquery_validation_query",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    SELECT
                        department,
                        COUNT(*) AS employee_count,
                        AVG(salary) AS avg_salary
                    FROM `{OUTPUT_TABLE}`
                    GROUP BY department
                    ORDER BY avg_salary DESC
                """,
                "useLegacySql": False,
            }
        },
    )

    # Task 6: Read the path stored in XCom
    read_xcom_task = PythonOperator(
        task_id="read_xcom_task",
        python_callable=read_xcom,
    )

    # Define the execution order
    (
        check_gcs_file
        >> push_xcom_task
        >> read_xcom_task
        >> run_dataproc_job
        >> load_to_bigquery
        >> validation_query
    )
