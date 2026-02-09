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
    'hilo_complete_pipeline',
    default_args=default_args,
    description='Complete HiLo data pipeline for both game rounds and chat messages',
    #schedule='0 1 * * *',  # Run daily at 1 AM
    catchup=False,
    tags=['hilo', 'complete', 'data-pipeline', 'spark'],
)

# Base paths
SPARK_BASE_DIR = "/opt/spark/work-dir"
GAME_ROUND_BASE = f"{SPARK_BASE_DIR}/spark_jobs/game_round"
GAME_CHAT_BASE = f"{SPARK_BASE_DIR}/spark_jobs/game_chat"

# Common Spark config
spark_conf = {
    'spark.driver.memory': '2g',
    'spark.executor.memory': '4g',
    'spark.executor.cores': '2',
}

# ============================================================================
# GAME ROUND PIPELINE
# ============================================================================

# Task markers
start_game_round = EmptyOperator(task_id='start_game_round_pipeline', dag=dag)
end_game_round = EmptyOperator(task_id='end_game_round_pipeline', dag=dag)

# Game Round: Ingest CSV
gr_ingest_csv = SparkSubmitOperator(
    task_id='gr_01a_ingest_csv',
    application=f'{GAME_ROUND_BASE}/01a_ingest_csv.py',
    name='hilo-gr-ingest-csv',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Game Round: Ingest DB
gr_ingest_db = SparkSubmitOperator(
    task_id='gr_01b_ingest_db',
    application=f'{GAME_ROUND_BASE}/01b_ingest_db.py',
    name='hilo-gr-ingest-db',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Game Round: Merge
gr_merge = SparkSubmitOperator(
    task_id='gr_02_merge',
    application=f'{GAME_ROUND_BASE}/02_merge.py',
    name='hilo-gr-merge',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Game Round: Quality Check
gr_quality = SparkSubmitOperator(
    task_id='gr_03_check_quality',
    application=f'{GAME_ROUND_BASE}/03_check_quality.py',
    name='hilo-gr-quality',
    conn_id='spark_default',
    verbose=True,
    conf={'spark.driver.memory': '2g', 'spark.executor.memory': '2g'},
    dag=dag,
)

# Game Round: Clean
gr_clean = SparkSubmitOperator(
    task_id='gr_04_clean',
    application=f'{GAME_ROUND_BASE}/04_clean.py',
    name='hilo-gr-clean',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Game Round: Transform
gr_transform = SparkSubmitOperator(
    task_id='gr_05_transform',
    application=f'{GAME_ROUND_BASE}/05_transform.py',
    name='hilo-gr-transform',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Game Round: Vectorize
gr_vectorize = SparkSubmitOperator(
    task_id='gr_06_vectorize',
    application=f'{GAME_ROUND_BASE}/06_vectorize.py',
    name='hilo-gr-vectorize',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Game Round: Graph Load
gr_graph = SparkSubmitOperator(
    task_id='gr_07_graph_load',
    application=f'{GAME_ROUND_BASE}/07_graph_load.py',
    name='hilo-gr-graph',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# ============================================================================
# GAME CHAT PIPELINE
# ============================================================================

# Task markers
start_game_chat = EmptyOperator(task_id='start_game_chat_pipeline', dag=dag)
end_game_chat = EmptyOperator(task_id='end_game_chat_pipeline', dag=dag)

# Game Chat: Ingest CSV
gc_ingest_csv = SparkSubmitOperator(
    task_id='gc_01a_ingest_csv',
    application=f'{GAME_CHAT_BASE}/01a_ingest_csv.py',
    name='hilo-gc-ingest-csv',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Game Chat: Ingest DB
gc_ingest_db = SparkSubmitOperator(
    task_id='gc_01b_ingest_db',
    application=f'{GAME_CHAT_BASE}/01b_ingest_db.py',
    name='hilo-gc-ingest-db',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Game Chat: Merge
gc_merge = SparkSubmitOperator(
    task_id='gc_02_merge',
    application=f'{GAME_CHAT_BASE}/02_merge.py',
    name='hilo-gc-merge',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Game Chat: Quality Check
gc_quality = SparkSubmitOperator(
    task_id='gc_03_check_quality',
    application=f'{GAME_CHAT_BASE}/03_check_quality.py',
    name='hilo-gc-quality',
    conn_id='spark_default',
    verbose=True,
    conf={'spark.driver.memory': '2g', 'spark.executor.memory': '2g'},
    dag=dag,
)

# Game Chat: Clean
gc_clean = SparkSubmitOperator(
    task_id='gc_04_clean',
    application=f'{GAME_CHAT_BASE}/04_clean.py',
    name='hilo-gc-clean',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Game Chat: Transform
gc_transform = SparkSubmitOperator(
    task_id='gc_05_transform',
    application=f'{GAME_CHAT_BASE}/05_transform.py',
    name='hilo-gc-transform',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Game Chat: Vectorize
gc_vectorize = SparkSubmitOperator(
    task_id='gc_06_vectorize',
    application=f'{GAME_CHAT_BASE}/06_vectorize.py',
    name='hilo-gc-vectorize',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# Game Chat: Graph Load
gc_graph = SparkSubmitOperator(
    task_id='gc_07_graph_load',
    application=f'{GAME_CHAT_BASE}/07_graph_load.py',
    name='hilo-gc-graph',
    conn_id='spark_default',
    verbose=True,
    conf=spark_conf,
    dag=dag,
)

# ============================================================================
# PIPELINE DEPENDENCIES
# ============================================================================

# Game Round Pipeline Flow
start_game_round >> [gr_ingest_csv, gr_ingest_db]
[gr_ingest_csv, gr_ingest_db] >> gr_merge >> gr_quality >> gr_clean >> gr_transform
gr_transform >> [gr_vectorize, gr_graph] >> end_game_round

# Game Chat Pipeline Flow
start_game_chat >> [gc_ingest_csv, gc_ingest_db]
[gc_ingest_csv, gc_ingest_db] >> gc_merge >> gc_quality >> gc_clean >> gc_transform
gc_transform >> [gc_vectorize, gc_graph] >> end_game_chat

# Both pipelines can run in parallel
# But you can also make chat wait for game round if needed:
# end_game_round >> start_game_chat
