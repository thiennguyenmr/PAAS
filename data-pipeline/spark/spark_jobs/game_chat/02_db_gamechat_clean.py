from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
from pyspark.sql import functions as F
import sys
import traceback

def main():
    spark = get_spark_session("02_Clean_GameChat_Raw_to_Bronze")

    input_path = "s3a://raw/chat_messages"
    output_path = "s3a://bronze/chat_messages"

    print(f"Reading from {input_path}")
    try:
        df = MinIOConnector.read_parquet(spark, input_path)
        print("DEBUG: Raw Chat Messages Schema:")
        df.printSchema()
        print("DEBUG: Raw Data Count:", df.count())
        print("DEBUG: Raw Data Sample:")
        df.show(5, truncate=False)

        # Cleaning transformations
        # 1. Remove duplicates based on message_id
        # 2. Drop null values in critical columns
        # 3. Filter out empty messages
        # 4. Ensure valid message types
        # 5. Clean message content (trim whitespace)

        valid_message_types = ['public', 'private', 'system', 'game_announcement', 'support']
        valid_statuses = ['sent', 'delivered', 'read']
        valid_languages = ['en', 'vi', 'th']

        df_clean = (df
            .dropDuplicates(["message_id"])
            .dropna(subset=["message_id", "user_id", "timestamp", "message_content"])
            .filter(
                (F.col("message_type").isin(valid_message_types)) &
                (F.col("status").isin(valid_statuses)) &
                (F.col("language").isin(valid_languages)) &
                (F.length(F.trim(F.col("message_content"))) > 0)
            )
            # Clean message content
            .withColumn("message_content", F.trim(F.col("message_content")))
        )

        print("DEBUG: Clean Chat Messages Schema:")
        df_clean.printSchema()
        df_clean.show(5, truncate=False)
        print("DEBUG: Clean Data Count:", df_clean.count())

        print(f"Writing to {output_path}")
        MinIOConnector.write_parquet(df_clean, output_path, mode="overwrite")

        print("Chat messages cleaning completed.")
    except Exception as e:
        print(f"ERROR: Failed during cleaning process: {e}")
        traceback.print_exc()
        spark.stop()
        sys.exit(1)
    spark.stop()

if __name__ == "__main__":
    main()