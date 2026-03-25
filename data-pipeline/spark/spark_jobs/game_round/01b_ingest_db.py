from spark_modules.session_manager import get_spark_session
from spark_modules.postgres_connector import PostgresConnector
from spark_modules.minio_connector import MinIOConnector
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
    spark = get_spark_session("01_Ingest_GameRound_Postgres_to_MinIO_Raw")

    print("DEBUG: Spark Version:", spark.version)

    print("Reading game rounds from Postgres...")
    try:
        connector = PostgresConnector()
        df = connector.read_table(spark, "hilo_game_rounds")

        # Count source records
        source_count = df.count()

        print("DEBUG: Game Rounds DataFrame Schema:")
        df.printSchema()
        print("DEBUG: Game Rounds Data Sample:")
        df.show(5, truncate=False)
        print(f"DEBUG: Total Source Records: {source_count:,}")
    except Exception as e:
        print(f"ERROR: Failed to read from Postgres: {e}")
        spark.stop()
        sys.exit(1)

    print("\nWriting to MinIO Raw...")
    output_path = "s3a://raw/db_hilo_game_rounds"

    try:
        MinIOConnector.write_parquet(df, output_path, mode="overwrite")
        print(f"✓ Data written to {output_path}")

        # Validate record count
        validate_record_count(spark, source_count, output_path, "Game Round PostgreSQL Ingestion")

        print(f"\n{'='*60}")
        print(f"DATABASE INGESTION COMPLETED SUCCESSFULLY")
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