from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum

# Default Args
default_args = {
    'owner': 'hilo_team',
    'start_date': pendulum.today('UTC').add(days=-1),
    'retries': 1,
}

# Env vars for Spark Jobs (Passed via env_vars is cleaner, 
# but SparkSubmitOperator puts them in driver/executor env if configured)
COMMON_ENV_VARS = {
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_ACCESS_KEY": "minioadmin",
    "MINIO_SECRET_KEY": "minioadmin",
    "PG_HOST": "app-postgres",
    "PG_PORT": "5432",
    "PG_USER": "postgres",
    "PG_PASS": "postgres",
    "MILVUS_HOST": "milvus",
    "NEO4J_URL": "bolt://neo4j:7687",
    "NEO4J_USER": "neo4j",
    "NEO4J_PASSWORD": "graph_secret_password"
}

# The Spark Jobs are mapped into the container at /opt/airflow/spark/jobs
# (Based on docker-compose volumes: ../data-pipeline-repo/spark:/opt/airflow/spark)
SPARK_JOBS_DIR = "/opt/airflow/spark/jobs/test_pipeline"

with DAG(
    dag_id='test_postgres_connection',
    default_args=default_args,
    #schedule='@daily',
    catchup=False,
    tags=['hilo_test', 'etl', 'spark']
) as dag:

    # 1. Ingest
    ingest_task = SparkSubmitOperator(
        task_id='01_ingest_postgres_to_raw',
        application=f"{SPARK_JOBS_DIR}/01_ingest.py",
        conn_id='spark_default', # Ensure this connection exists or use local spark
        verbose=True,
    )