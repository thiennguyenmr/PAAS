"""
HiLo Complete Data Pipeline DAG
Runs both Game Round and Chat pipelines in a coordinated manner
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
import pendulum


# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['alerts@hilochatbot.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=3),
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Ho_Chi_Minh'),
}

# DAG definition
dag = DAG(
    'etl_demo',
    default_args=default_args,
    description='Complete etl demo from sharepoint to postgres',
    start_date=datetime(2026, 2, 5, 15, 30),
    #schedule='0 1 * * *',  # Run daily at 1 AM
    catchup=False,
    tags=['hilo', 'complete', 'data-pipeline', 'spark', 'non-spark'],
)

# Base paths
AIRFLOW_BASE_DIR = "/opt/airflow/work-dir"
SPARK_BASE_DIR = "/opt/spark/work-dir"
NONSPARK = f"{AIRFLOW_BASE_DIR}/airflow_jobs"
SPARK = f"{SPARK_BASE_DIR}/spark_jobs"

# Common Spark config
spark_conf = {
    'spark.driver.memory': '2g',
    'spark.executor.memory': '4g',
    'spark.executor.cores': '2',
}

# ============================================================================
# non spark
# ============================================================================

# Task markers
start_game_round = EmptyOperator(task_id='start_etl_pipeline', dag=dag)
end_game_round = EmptyOperator(task_id='end_etl_pipeline', dag=dag)

# Game Round: Ingest CSV
etl_bronze = SparkSubmitOperator(
    task_id='tsk_1_file_to_bronze',
    application=f'{NONSPARK}/1_file_to_bronze.py',
    name='bronze',
    #conn_id='spark_default',
    verbose=True,
    #conf=spark_conf,
    dag=dag,
)

# ============================================================================
# spark
# ============================================================================

# Game Chat: Ingest CSV
etl_silver = SparkSubmitOperator(
    task_id='2_bronze_to_silver',
    application=f'{SPARK}/2_bronze_to_silver.py',
    name='silver',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

etl_db = SparkSubmitOperator(
    task_id='3_silver_to_db',
    application=f'{SPARK}/3_silver_to_db.py',
    name='db',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# ============================================================================
# PIPELINE DEPENDENCIES
# ============================================================================

# Game Round Pipeline Flow
start_game_round >> etl_bronze >> etl_silver >> etl_db >> end_game_round