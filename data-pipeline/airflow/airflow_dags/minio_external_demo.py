"""
MinIO External Connection Demo DAG
Uses minio library to connect via external port (6000) when not on same network
"""

import sys
import os
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
import pendulum

# Add airflow_jobs to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow_jobs.minio_external_job import (
    task_ensure_bucket,
    task_upload_json,
    task_upload_csv,
    task_list_objects,
)


# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Ho_Chi_Minh'),
}

# DAG definition
with DAG(
    dag_id='minio_external_demo',
    default_args=default_args,
    description='Demo DAG using minio library with external port 6000',
    schedule=None,
    catchup=False,
    tags=['demo', 'minio', 'external'],
) as dag:

    start = EmptyOperator(task_id='start')

    create_bucket = PythonOperator(
        task_id='ensure_bucket_exists',
        python_callable=task_ensure_bucket,
    )

    upload_json = PythonOperator(
        task_id='upload_json_data',
        python_callable=task_upload_json,
    )

    upload_csv = PythonOperator(
        task_id='upload_csv_data',
        python_callable=task_upload_csv,
    )

    list_objects = PythonOperator(
        task_id='list_uploaded_objects',
        python_callable=task_list_objects,
    )

    end = EmptyOperator(task_id='end')

    # Pipeline flow
    start >> create_bucket >> [upload_json, upload_csv] >> list_objects >> end
