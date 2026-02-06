from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.bash import BashOperator



with DAG(
    dag_id="test_config_spark",
    start_date=datetime(2026, 1, 12),
    schedule=None,
    catchup=False,
) as dag:
    
    SparkSubmitOperator(
    task_id="minio_task",
    application="/opt/spark/jobs/minio.py",
    conn_id="spark_default",
    verbose=True,
)



