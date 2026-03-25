from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
import sys

def validate_record_count(spark, source_count, output_path, job_name):
    """
    Validate that the number of records written matches the source count.

    Args:
        spark: Spark session
        source_count: Expected number of records from source
        output_path: Path where data was written
        job_name: Name of the job for logging

    Raises:
        ValueError: If record counts don't match
    """
    print(f"\n{'='*60}")
    print(f"RECORD COUNT VALIDATION: {job_name}")
    print(f"{'='*60}")

    try:
        # Read back the written data and count
        df_written = MinIOConnector.read_parquet(spark, output_path)
        written_count = df_written.count()

        print(f"Source records:  {source_count:,}")
        print(f"Written records: {written_count:,}")

        if source_count != written_count:
            error_msg = (
                f"RECORD COUNT MISMATCH!\n"
                f"  Expected: {source_count:,} records\n"
                f"  Found:    {written_count:,} records\n"
                f"  Missing:  {abs(source_count - written_count):,} records\n"
                f"  Location: {output_path}"
            )
            print(f"\n❌ ERROR: {error_msg}")
            raise ValueError(error_msg)

        print(f"✅ Validation PASSED: All {source_count:,} records written successfully")
        print(f"{'='*60}\n")

        return True

    except Exception as e:
        if isinstance(e, ValueError):
            raise
        error_msg = f"Failed to validate record count: {str(e)}"
        print(f"❌ ERROR: {error_msg}")
        raise ValueError(error_msg)

def main():
    spark = get_spark_session("01_CSV_Ingest_GameChat_to_MinIO_Raw")

    # Define schema for chat messages CSV
    schema = StructType([
        StructField("message_id", IntegerType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("username", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("message_type", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("message_content", StringType(), False),
        StructField("recipient_id", StringType(), True),  # Can be empty string
        StructField("recipient_username", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("status", StringType(), False),
        StructField("language", StringType(), False)
    ])

    # CSV file path from S3 source bucket
    csv_path = "s3a://source/hilo_chat_data.csv"

    print(f"Reading CSV from {csv_path}...")
    try:
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(schema) \
            .csv(csv_path)

        # Handle empty strings as nulls for optional fields
        df = df \
            .withColumn("recipient_id",
                       F.when(F.col("recipient_id") == "", None).otherwise(F.col("recipient_id").cast(IntegerType()))) \
            .withColumn("recipient_username",
                       F.when(F.col("recipient_username") == "", None).otherwise(F.col("recipient_username"))) \
            .withColumn("session_id",
                       F.when(F.col("session_id") == "", None).otherwise(F.col("session_id")))

        # Count source records
        source_count = df.count()

        print("DEBUG: CSV DataFrame Schema:")
        df.printSchema()
        print("DEBUG: CSV Data Sample:")
        df.show(5, truncate=False)
        print(f"DEBUG: Total Source Records: {source_count:,}")

    except Exception as e:
        print(f"ERROR: Failed to read CSV: {e}")
        spark.stop()
        sys.exit(1)

    print("\nWriting to MinIO Raw (CSV source)...")
    output_path = "s3a://raw/csv_chat_messages"

    try:
        MinIOConnector.write_parquet(df, output_path, mode="overwrite")
        print(f"✓ Data written to {output_path}")

        # Validate record count
        validate_record_count(spark, source_count, output_path, "Chat CSV Ingestion")

        print(f"\n{'='*60}")
        print(f"CSV INGESTION COMPLETED SUCCESSFULLY")
        print(f"{'='*60}")
        print(f"Total records ingested: {source_count:,}")
        print(f"Output location: {output_path}")
        print(f"{'='*60}\n")

    except ValueError as e:
        print(f"\n❌ INGESTION FAILED: {e}")
        spark.stop()
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR during write or validation: {e}")
        spark.stop()
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    main()