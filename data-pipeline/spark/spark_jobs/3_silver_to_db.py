#init
import sys
import os

# Ensure we can import from the spark directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from spark_modules.minio import MinioStorage
from spark_modules.postgres import PostgresConnector

#---------Config paths

def main():
    print("--- SILVER TO DB PROCESSING STARTED ---")
    
    # 1. Initialize Connections
    minio = MinioStorage()
    postgres = PostgresConnector()
    
    try:
        # Load MinIO config
        print(">>> [Init] Connecting to Silver Layer...")
        minio.load_config("minio_silver")
        
        # Load Postgres config
        print(">>> [Init] Connecting to PostgreSQL...")
        postgres.load_config("postgres", "server_postgres")
        
        # Initialize Spark Session (reusing MinIO libs and adding Postgres if needed)
        # Note: PostgresConnector.init_spark and MinioStorage.init_spark handle different jars.
        # We need a session that has BOTH.
        # Let's manually init spark with both jars if possible, or use one and add config.
        
        # Since they use different jars, we merge the maven packages.
        maven_packages = f"{minio.storage_libs['maven_package']},{postgres.app_conf['maven_package']}"
        
        print(f">>> Initializing Spark with MinIO and PostgreSQL support...")
        from pyspark.sql import SparkSession
        spark = (
            SparkSession.builder
            .appName("Silver to DB Transformation")
            .master("local[*]")
            .config("spark.jars.packages", maven_packages)
            # MinIO configs
            .config("spark.hadoop.fs.s3a.endpoint", minio.storage_conn["endpoint"])
            .config("spark.hadoop.fs.s3a.access.key", minio.storage_conn["access_key"])
            .config("spark.hadoop.fs.s3a.secret.key", minio.storage_conn["secret_key"])
            .config("spark.hadoop.fs.s3a.path.style.access", str(minio.storage_libs["path_style_access"]).lower())
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(minio.storage_libs.get("ssl_enabled", "false")).lower())
            .config("spark.hadoop.fs.s3a.impl", minio.storage_libs["file_system_impl"])
            # Extra MinIO fixes
            .config("spark.hadoop.fs.s3a.connection.timeout", minio.storage_libs["connection_timeout"])
            .config("spark.hadoop.fs.s3a.socket.timeout", minio.storage_libs["socket_timeout"])
            .config("spark.hadoop.fs.s3a.retry.interval", minio.storage_libs["retry_interval"])
            .config("spark.hadoop.fs.s3a.attempts.maximum", minio.storage_libs["max_attempts"])
            .config("spark.hadoop.fs.s3a.threads.keepalivetime", minio.storage_libs["keep_alive_time"])
            .config("spark.hadoop.fs.s3a.multipart.purge.age", minio.storage_libs["multipart_purge_age"])
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", minio.storage_libs["establish_timeout"])
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", minio.storage_libs["aws_credentials_provider"])
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        
        # Share the spark session with connectors
        minio.spark = spark
        postgres.spark = spark
        
        # 2. Read from Silver and Write to DB (Raw Tables)
        bucket = minio.storage_conn["bucket"]
        
        # Table 1: data_sample
        PATH_SAMPLE = "sample_data/sample_data"
        print(f">>> [Process] Reading Parquet from silver/{PATH_SAMPLE}...")
        df_sample = spark.read.parquet(f"s3a://{bucket}/{PATH_SAMPLE}")
        print(f">>> [Process] Writing to PostgreSQL table: data_sample")
        postgres.write_to_table(df_sample, "data_sample")
        
        # Table 2: data_sample1
        PATH_SAMPLE1 = "sample_data/sample_data1"
        print(f">>> [Process] Reading Parquet from silver/{PATH_SAMPLE1}...")
        df_sample1 = spark.read.parquet(f"s3a://{bucket}/{PATH_SAMPLE1}")
        print(f">>> [Process] Writing to PostgreSQL table: data_sample1")
        postgres.write_to_table(df_sample1, "data_sample1")
        
        # 3. Spark SQL Transformation (Aggregation)
        print(">>> [Process] Executing Spark SQL Aggregation on data_sample1...")
        df_sample1.createOrReplaceTempView("data_sample1")
        
        sql_query = """
        SELECT count(consultation_id) as complaint_cnt, status
        FROM data_sample1
        GROUP BY status
        ORDER BY complaint_cnt DESC
        """
        
        df_result = spark.sql(sql_query)
        
        # Preview Results
        print(">>> Aggregation Results:")
        df_result.show()
        
        # 4. Write Aggregated Results to PostgreSQL
        TABLE_NAME = "cust_top_reason"
        print(f">>> [Process] Writing aggregated results to PostgreSQL table: {TABLE_NAME}")
        postgres.write_to_table(df_result, TABLE_NAME)
        
    except Exception as e:
        print(f"!!! Error in execution: {e}")
    finally:
        if 'spark' in locals():
            spark.stop()
            print(">>> Spark Session stopped.")
        print("--- PROGRAM FINISHED ---")

if __name__ == "__main__":
    main()
