from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG
from datetime import datetime



with DAG(
    dag_id="spark_hello_world",
    start_date=datetime(2026, 1, 12),
    schedule=None,
    catchup=False,
) as dag:

    # spark_job = BashOperator(
    #     task_id="hello_world_task",
    #     bash_command="""
    #     /opt/spark/spark_submit_wrapper.sh \
    #     --master spark://spark-master:7077 \
    #     --deploy-mode client \
    #     /opt/spark/jobs/hello_world.py
    #     """,
    #     do_xcom_push=True,
    # )


    
    spark_job = SparkSubmitOperator(
        task_id="hello_world_task",
        application="/opt/spark/jobs/hello_world.py",
        conn_id="spark_default",
        deploy_mode="client",
        name="hello_world",
        verbose=True,
    )


