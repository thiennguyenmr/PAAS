from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
from pyspark.sql import functions as F
import sys
import traceback

def main():
    spark = get_spark_session("02_Clean_Merged_GameChat_Raw_to_Bronze")

    # Read from merged data (output of 03_merge_data.py)
    input_path = "s3a://raw/merged_chat_messages"
    output_path = "s3a://bronze/chat_messages"

    print(f"Reading merged data from {input_path}")
    try:
        df = MinIOConnector.read_parquet(spark, input_path)
        print("DEBUG: Merged Raw Chat Messages Schema:")
        df.printSchema()
        print("DEBUG: Raw Data Count:", df.count())
        print("DEBUG: Raw Data Sample:")
        df.show(5, truncate=False)

        # Show data source distribution
        if "data_source" in df.columns:
            print("\nData source distribution:")
            df.groupBy("data_source").count().show()

        # Cleaning transformations
        # 1. Remove duplicates based on message_id (should already be done, but double-check)
        # 2. Drop null values in critical columns
        # 3. Filter out empty messages
        # 4. Ensure valid message types
        # 5. Clean message content (trim whitespace)

        print("\nApplying cleaning transformations...")

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
            # Keep data_source column for tracking
        )

        print("DEBUG: Clean Chat Messages Schema:")
        df_clean.printSchema()
        df_clean.show(5, truncate=False)
        print("DEBUG: Clean Data Count:", df_clean.count())

        # Show cleaning impact
        records_removed = df.count() - df_clean.count()
        print(f"\nCleaning impact: {records_removed:,} records removed")

        if "data_source" in df_clean.columns:
            print("\nCleaned data source distribution:")
            df_clean.groupBy("data_source").count().show()

        # Show message type distribution
        print("\nMessage type distribution in cleaned data:")
        df_clean.groupBy("message_type").count().orderBy(F.desc("count")).show()

        print(f"\nWriting to {output_path}")
        MinIOConnector.write_parquet(df_clean, output_path, mode="overwrite")

        print("Chat messages cleaning completed.")
        print(f"Final clean records: {df_clean.count():,}")

    except Exception as e:
        print(f"ERROR: Failed during cleaning process: {e}")
        traceback.print_exc()
        spark.stop()
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    main()