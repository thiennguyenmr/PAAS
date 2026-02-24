"""
HiLo Game Chat Data Pipeline DAG
Orchestrates the complete chat data pipeline from ingestion to graph loading
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.python import PythonOperator
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
    'hilo_game_chat_pipeline',
    default_args=default_args,
    description='Complete HiLo chat message pipeline: Ingest → Merge → Quality Check → Clean → Transform → Vectorize → Graph',
    # schedule='0 3 * * *',  # Run daily at 3 AM (1 hour after game_round pipeline)
    catchup=False,
    tags=['hilo', 'chat', 'data-pipeline', 'spark'],
)

# Base path for Spark jobs
SPARK_BASE_DIR = "/opt/spark/work-dir"
SPARK_JOBS_DIR = f"{SPARK_BASE_DIR}/spark_jobs/game_chat"

# Common Spark config
spark_conf = {
    'spark.driver.memory': '2g',
    'spark.executor.memory': '4g',
    'spark.executor.cores': '2',
}

# Task 1a: Ingest CSV data
ingest_csv = SparkSubmitOperator(
    task_id='01a_ingest_csv',
    application=f'{SPARK_JOBS_DIR}/01a_ingest_csv.py',
    name='hilo-gamechat-ingest-csv',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Task 1b: Ingest Database data
ingest_db = SparkSubmitOperator(
    task_id='01b_ingest_db',
    application=f'{SPARK_JOBS_DIR}/01b_ingest_db.py',
    name='hilo-gamechat-ingest-db',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Task 2: Merge CSV and Database data
merge_data = SparkSubmitOperator(
    task_id='02_merge',
    application=f'{SPARK_JOBS_DIR}/02_merge.py',
    name='hilo-gamechat-merge',
    conn_id='spark_default',
    verbose=True,
    conf={
        'spark.driver.memory': '2g',
        'spark.executor.memory': '8g',
        'spark.executor.cores': '4',
    },
    dag=dag,
)

# Task 3: Quality Check
quality_check = SparkSubmitOperator(
    task_id='03_check_quality',
    application=f'{SPARK_JOBS_DIR}/03_check_quality.py',
    name='hilo-gamechat-quality-check',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Task 4: Clean data
clean_data = SparkSubmitOperator(
    task_id='04_clean',
    application=f'{SPARK_JOBS_DIR}/04_clean.py',
    name='hilo-gamechat-clean',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Task 5: Transform data
transform_data = SparkSubmitOperator(
    task_id='05_transform',
    application=f'{SPARK_JOBS_DIR}/05_transform.py',
    name='hilo-gamechat-transform',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Task 6: Vectorize data for Milvus
vectorize_data = SparkSubmitOperator(
    task_id='06_vectorize',
    application=f'{SPARK_JOBS_DIR}/06_vectorize.py',
    name='hilo-gamechat-vectorize',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Task 7: Load to Graph Database (Neo4j)
graph_load = SparkSubmitOperator(
    task_id='07_graph_load',
    application=f'{SPARK_JOBS_DIR}/07_graph_load.py',
    name='hilo-gamechat-graph-load',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Define task dependencies
# Both ingestion tasks run in parallel
ingest_csv >> ingest_db >> merge_data >> quality_check >> clean_data >> transform_data

# Vectorize and graph load can run in parallel after transform
transform_data >> [vectorize_data, graph_load]