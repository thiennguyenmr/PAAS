from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.bash import BashOperator



with DAG(
    dag_id="spark_minio_connection_testing",
    start_date=datetime(2026, 1, 12),
    schedule=None,
    catchup=False,
) as dag:
    
    # spark_job = SparkSubmitOperator(
    #     task_id="minio_task",
    #     application="/opt/spark/jobs/minio.py",
    #     conn_id="spark_default",
    #     deploy_mode="client",
    #     name="minio",
    #     verbose=True,
    # )

    SparkSubmitOperator(
    task_id="spark_minio",
    application="/opt/airflow/spark/jobs/test_pipeline/minio.py",
    conn_id="spark_default",
    conf={
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.access.key": "minioadmin",
        "spark.hadoop.fs.s3a.secret.key": "minioadmin",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            # 🔥 FIX NumberFormatException
        "spark.hadoop.fs.s3a.connection.timeout": "60000",
        "spark.hadoop.fs.s3a.connection.establish.timeout": "60000",
        "spark.hadoop.fs.s3a.retry.interval": "5000",
        "spark.hadoop.fs.s3a.threads.keepalivetime":"60000",
        "spark.hadoop.fs.s3a.multipart.purge.age":"86400000",
    },
    verbose=True
)



