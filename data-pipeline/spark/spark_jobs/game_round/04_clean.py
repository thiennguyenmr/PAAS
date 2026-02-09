from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
from pyspark.sql import functions as F
import sys
import traceback

def main():
    spark = get_spark_session("02_Clean_Merged_GameRound_Raw_to_Bronze")

    # Read from merged data (output of 03_merge_data.py)
    input_path = "s3a://raw/merged_hilo_game_rounds"
    output_path = "s3a://bronze/hilo_game_rounds"

    print(f"Reading merged data from {input_path}")
    try:
        df = MinIOConnector.read_parquet(spark, input_path)
        print("DEBUG: Merged Raw Game Rounds Schema:")
        df.printSchema()
        print("DEBUG: Raw Data Count:", df.count())
        print("DEBUG: Raw Data Sample:")
        df.show(5, truncate=False)

        # Show data source distribution
        if "data_source" in df.columns:
            print("\nData source distribution:")
            df.groupBy("data_source").count().show()

        # Cleaning transformations
        # 1. Remove duplicates based on round_id (should already be done in merge, but double-check)
        # 2. Drop null values in critical columns
        # 3. Ensure data types are correct
        # 4. Filter out invalid dice values (should be 1-6)
        # 5. Ensure total_points is between 3-18
        # 6. Validate data integrity

        print("\nApplying cleaning transformations...")

        df_clean = (df
            .dropDuplicates(["round_id"])
            .dropna(subset=["round_id", "user_id", "timestamp"])
            .filter(
                (F.col("die1").between(1, 6)) &
                (F.col("die2").between(1, 6)) &
                (F.col("die3").between(1, 6)) &
                (F.col("total_points").between(3, 18)) &
                (F.col("bet_amount") > 0)
            )
            # Validate dice sum equals total
            .filter(F.col("total_points") == (F.col("die1") + F.col("die2") + F.col("die3")))
            # Keep data_source column for tracking
        )

        print("DEBUG: Clean Game Rounds Schema:")
        df_clean.printSchema()
        df_clean.show(5, truncate=False)
        print("DEBUG: Clean Data Count:", df_clean.count())

        # Show cleaning impact
        records_removed = df.count() - df_clean.count()
        print(f"\nCleaning impact: {records_removed:,} records removed")

        if "data_source" in df_clean.columns:
            print("\nCleaned data source distribution:")
            df_clean.groupBy("data_source").count().show()

        print(f"\nWriting to {output_path}")
        MinIOConnector.write_parquet(df_clean, output_path, mode="overwrite")

        print("Game rounds cleaning completed.")
        print(f"Final clean records: {df_clean.count():,}")

    except Exception as e:
        print(f"ERROR: Failed during cleaning process: {e}")
        traceback.print_exc()
        spark.stop()
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    main()