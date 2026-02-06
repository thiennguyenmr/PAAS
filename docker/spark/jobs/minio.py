from pyspark.sql import SparkSession
import os

# ==============================
# MinIO config
# ==============================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "datalake"

# ==============================
# Spark session
# ==============================
spark = SparkSession.builder \
    .appName("HelloWorldToMinIO_DEBUG") \
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.509"
    ) \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

try:
    # ==============================
    # 🔍 PRINT ALL S3A CONFIG
    # ==============================
    print("\n========== DEBUG: Hadoop S3A CONFIG ==========")
    hconf = spark.sparkContext._jsc.hadoopConfiguration()

    s3a_conf = []
    it = hconf.iterator()
    while it.hasNext():
        entry = it.next()
        k = entry.getKey()
        v = entry.getValue()
        if "s3a" in k.lower():
            s3a_conf.append((k, v))

    # Sort cho dễ đọc
    for k, v in sorted(s3a_conf):
        print(f"{k} = {v}")

    print("==============================================\n")

    # ==============================
    # Test write
    # ==============================
    print("--- Creating DataFrame ---")
    df = spark.createDataFrame(
        [
            ("Hello", "World", 1),
            ("MinIO", "Storage", 2),
            ("Spark", "Processing", 3)
        ],
        ["col1", "col2", "id"]
    )

    df.show()

    output_path = f"s3a://{BUCKET_NAME}/hello_world_parquet"
    print(f"--- Writing to: {output_path} ---")

    df.write.mode("overwrite").parquet(output_path)

    print("===SPARK_JOB_SUCCESS===")

except Exception as e:
    print("===SPARK_JOB_FAILED===")
    print(f"Error: {str(e)}")
    raise

finally:
    spark.stop()
            