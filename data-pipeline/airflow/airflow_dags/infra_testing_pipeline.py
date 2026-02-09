import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum

# Default Args
default_args = {
    'owner': 'hilo_team',
    'start_date': pendulum.today('UTC').add(days=-1),
    'retries': 1,
}
# The Spark Jobs and modules are mapped into the container at /opt/spark/work-dir
SPARK_BASE_DIR = "/opt/spark/work-dir"
SPARK_JOBS_DIR = f"{SPARK_BASE_DIR}/spark_jobs/test_pipeline"

with DAG(
    dag_id='infra_testing_pipeline',
    default_args=default_args,
    #schedule='@daily',
    catchup=False,
    tags=['hilo', 'etl', 'spark']
) as dag:

    # 1. Ingest
    ingest_task = SparkSubmitOperator(
        task_id='01_ingest_postgres_to_raw',
        application=f"{SPARK_JOBS_DIR}/01_ingest.py",
        conn_id='spark_default',
        verbose=True,
    )

    # 2. Clean
    clean_task = SparkSubmitOperator(
        task_id='02_clean_raw_to_bronze',
        application=f"{SPARK_JOBS_DIR}/02_clean.py",
        conn_id='spark_default',
        verbose=True,
    )

    # 3. Transform
    transform_task = SparkSubmitOperator(
        task_id='03_transform_bronze_to_silver',
        application=f"{SPARK_JOBS_DIR}/03_transform.py",
        conn_id='spark_default',
        verbose=True,
    )

    # 4. Vectorize
    vectorize_task = SparkSubmitOperator(
        task_id='04_vectorize_to_milvus',
        application=f"{SPARK_JOBS_DIR}/04_vectorize.py",
        conn_id='spark_default',
        verbose=True,
    )

    # 5. Graph Load
    graph_load_task = SparkSubmitOperator(
        task_id='05_load_to_neo4j',
        application=f"{SPARK_JOBS_DIR}/05_graph_load.py",
        conn_id='spark_default',
        verbose=True,
    )

    # Dependencies
    ingest_task >> clean_task >> transform_task
    transform_task >> [vectorize_task, graph_load_task]

