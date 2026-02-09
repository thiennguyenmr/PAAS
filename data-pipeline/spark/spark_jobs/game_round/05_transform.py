from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
import traceback

def main():
    spark = get_spark_session("03_Transform_GameRound_Bronze_to_Silver")

    input_path = "s3a://bronze/hilo_game_rounds"
    output_path = "s3a://silver/hilo_game_rounds"

    print(f"Reading from {input_path}")
    try:
        df = MinIOConnector.read_parquet(spark, input_path)
        print("DEBUG: Bronze Game Rounds Schema:")
        df.printSchema()
        print("DEBUG: Bronze Data Count:", df.count())

        # Feature engineering transformations
        # 1. Extract date parts from timestamp
        # 2. Calculate win rate per user
        # 3. Add cumulative statistics
        # 4. Calculate house edge metrics

        # Window specifications
        user_window = Window.partitionBy("user_id").orderBy("timestamp")
        session_window = Window.partitionBy("session_id").orderBy("timestamp")

        df_transformed = (df
            # Extract date/time features
            .withColumn("game_date", F.to_date("timestamp"))
            .withColumn("game_hour", F.hour("timestamp"))
            .withColumn("day_of_week", F.dayofweek("timestamp"))

            # Calculate cumulative metrics per user
            .withColumn("user_total_rounds", F.count("*").over(user_window))
            .withColumn("user_total_wagered", F.sum("bet_amount").over(user_window))
            .withColumn("user_cumulative_profit", F.sum("net_profit").over(user_window))

            # Session metrics
            .withColumn("rounds_in_session", F.count("*").over(session_window))
            .withColumn("session_total_wagered", F.sum("bet_amount").over(session_window))
            .withColumn("session_profit", F.sum("net_profit").over(session_window))

            # Win indicator (1 for win, 0 for loss)
            .withColumn("win_flag", F.when(F.col("result") == "Win", 1).otherwise(0))

            # Bet type category
            .withColumn("bet_category",
                F.when(F.col("bet_type").isin(["High", "Low"]), "Basic")
                 .when(F.col("bet_type").startswith("Total_"), "Specific_Total")
                 .when(F.col("bet_type").startswith("Triple_"), "Triple")
                 .otherwise("Other"))

            # ROI (Return on Investment) for each round
            .withColumn("roi", F.round((F.col("net_profit") / F.col("bet_amount")) * 100, 2))
        )

        print("DEBUG: Transformed Game Rounds Schema:")
        df_transformed.printSchema()
        df_transformed.show(5, truncate=False)
        print("DEBUG: Transformed Data Count:", df_transformed.count())

        print(f"Writing to {output_path}")
        MinIOConnector.write_parquet(df_transformed, output_path, mode="overwrite")

        print("Game rounds transformation completed.")
    except Exception as e:
        print(f"ERROR: Failed during transformation process: {e}")
        traceback.print_exc()
        spark.stop()
        sys.exit(1)
    spark.stop()

if __name__ == "__main__":
    main()