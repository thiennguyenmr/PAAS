from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
from pyspark.sql import functions as F
import sys
import traceback

def main():
    spark = get_spark_session("02_Clean_GameRound_Raw_to_Bronze")

    input_path = "s3a://raw/hilo_game_rounds"
    output_path = "s3a://bronze/hilo_game_rounds"

    print(f"Reading from {input_path}")
    try:
        df = MinIOConnector.read_parquet(spark, input_path)
        print("DEBUG: Raw Game Rounds Schema:")
        df.printSchema()
        print("DEBUG: Raw Data Count:", df.count())
        print("DEBUG: Raw Data Sample:")
        df.show(5, truncate=False)

        # Cleaning transformations
        # 1. Remove duplicates based on round_id
        # 2. Drop null values in critical columns
        # 3. Ensure data types are correct
        # 4. Filter out invalid dice values (should be 1-6)
        # 5. Ensure total_points is between 3-18

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
        )

        print("DEBUG: Clean Game Rounds Schema:")
        df_clean.printSchema()
        df_clean.show(5, truncate=False)
        print("DEBUG: Clean Data Count:", df_clean.count())

        print(f"Writing to {output_path}")
        MinIOConnector.write_parquet(df_clean, output_path, mode="overwrite")

        print("Game rounds cleaning completed.")
    except Exception as e:
        print(f"ERROR: Failed during cleaning process: {e}")
        traceback.print_exc()
        spark.stop()
        sys.exit(1)
    spark.stop()

if __name__ == "__main__":
    main()