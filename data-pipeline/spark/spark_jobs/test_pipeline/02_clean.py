from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
import sys
import traceback

def main():
    spark = get_spark_session("02_Clean_Raw_to_Bronze")

    input_path = "s3a://raw/hilo_game_data"
    output_path = "s3a://bronze/hilo_game_data"

    print(f"Reading from {input_path}")
    # Read raw data
    try:
        df = MinIOConnector.read_parquet(spark, input_path)
        print("DEBUG: Raw Data Schema:")
        df.printSchema()
        print("DEBUG: Raw Data Count:", df.count())
        print("DEBUG: Raw Data Sample:")
        df.show(5)
        
        # Transformations: Deduplicate based on 'id' and drop nulls in critical columns
        # Assuming 'id' is unique key
        df_clean = df.dropDuplicates(["id"]).dropna(subset=["id", "user_id"])
        
        print("DEBUG: Clean Data Schema:")
        df_clean.printSchema()
        df_clean.show(5)
        print("DEBUG: Clean Data Count:", df_clean.count())

        print(f"Writing to {output_path}")
        MinIOConnector.write_parquet(df_clean, output_path, mode="overwrite")

        print("Cleaning completed.")
    except Exception as e:
        print(f"ERROR: Failed during cleaning process: {e}")
        traceback.print_exc()
        spark.stop()
        sys.exit(1)
    spark.stop()

if __name__ == "__main__":
    main()
