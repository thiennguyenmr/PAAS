from spark_modules.session_manager import get_spark_session
from spark_modules.postgres_connector import PostgresConnector
from spark_modules.minio_connector import MinIOConnector
import sys

def main():
    spark = get_spark_session("01_Ingest_Postgres_to_MinIO_Raw")
    
    print("DEBUG: Spark Version:", spark.version)
    # print("DEBUG: Java Classpath:", spark.sparkContext._jvm.java.lang.System.getProperty("java.class.path"))

    print("Reading from Postgres...")
    try:
        connector = PostgresConnector()
        df = connector.read_table(spark, "hilo_game_data")
        
        print("DEBUG: Postgres DataFrame Schema:")
        df.printSchema()
        print("DEBUG: Postgres Data Sample:")
        df.show(5)
    except Exception as e:
        print(f"ERROR: Failed to read from Postgres: {e}")
        spark.stop()
        sys.exit(1)

    print("Writing to MinIO Raw...")
    output_path = "s3a://raw/hilo_game_data"
    
    MinIOConnector.write_parquet(df, output_path, mode="append")
    
    print(f"Ingestion completed to {output_path}")
    spark.stop()

if __name__ == "__main__":
    main()
