import airflow
from airflow import DAG
import datetime
from datetime import timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'start_date': datetime.datetime(2000, 1, 1),
    'retries': 0,
}

dag = DAG(
    'stopvm',
    default_args=default_args,
    description='Stopping the VM',
    schedule=None,
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=20),
)

t1 = BashOperator(
    task_id='db_backup_dag',
    bash_command='gcloud compute instances stop test-instance --zone=asia-south2-b',
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31 - 1,
    do_xcom_push=False)

t2 = BashOperator(
    task_id='create_image',
    bash_command='gcloud compute machine-images create --source-instance=test-instance --source-zone=asia-south2-b',
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31 - 1,
    do_xcom_push=False)

 # Ensure image creation runs after the instance is stopped
 stop_vm >> create_image