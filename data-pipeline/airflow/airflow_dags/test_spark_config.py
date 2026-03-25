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
    task_id="test_spark_config",
    application="/opt/airflow/spark/jobs/test_pipeline/minio.py",
    conn_id="spark_default",
    verbose=True,
)



