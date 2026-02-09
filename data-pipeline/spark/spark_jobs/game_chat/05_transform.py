from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import sys
import traceback

def main():
    spark = get_spark_session("03_Transform_GameChat_Bronze_to_Silver")

    input_path = "s3a://bronze/chat_messages"
    output_path = "s3a://silver/chat_messages"

    print(f"Reading from {input_path}")
    try:
        df = MinIOConnector.read_parquet(spark, input_path)
        print("DEBUG: Bronze Chat Messages Schema:")
        df.printSchema()
        print("DEBUG: Bronze Data Count:", df.count())

        # Feature engineering transformations
        # 1. Extract date/time features
        # 2. Calculate message length and word count
        # 3. Add user activity metrics
        # 4. Identify conversation threads
        # 5. Sentiment indicators (simple keyword-based)

        # Window specifications
        user_window = Window.partitionBy("user_id").orderBy("timestamp")
        channel_window = Window.partitionBy("channel").orderBy("timestamp")
        conversation_window = Window.partitionBy("user_id", "recipient_id").orderBy("timestamp")

        # Positive and negative keyword patterns (simple sentiment)
        positive_keywords = ["win", "won", "lucky", "good", "great", "congrats", "nice", "excellent", "awesome"]
        negative_keywords = ["loss", "lost", "lose", "bad", "rough", "tough", "everything", "fail"]

        df_transformed = (df
            # Extract date/time features
            .withColumn("message_date", F.to_date("timestamp"))
            .withColumn("message_hour", F.hour("timestamp"))
            .withColumn("day_of_week", F.dayofweek("timestamp"))

            # Message characteristics
            .withColumn("message_length", F.length(F.col("message_content")))
            .withColumn("word_count", F.size(F.split(F.col("message_content"), "\\s+")))

            # User activity metrics
            .withColumn("user_message_count", F.count("*").over(user_window))
            .withColumn("user_messages_in_channel",
                       F.count("*").over(Window.partitionBy("user_id", "channel").orderBy("timestamp")))

            # Channel activity
            .withColumn("channel_message_count", F.count("*").over(channel_window))

            # Time since last message (in seconds)
            .withColumn("seconds_since_last_msg",
                       F.unix_timestamp("timestamp") -
                       F.lag(F.unix_timestamp("timestamp"), 1).over(user_window))

            # Conversation thread for private messages
            .withColumn("conversation_message_count",
                       F.when(F.col("message_type") == "private",
                             F.count("*").over(conversation_window))
                        .otherwise(None))

            # Simple sentiment indicators
            .withColumn("has_positive_sentiment",
                       F.when(F.lower(F.col("message_content"))
                             .rlike("|".join(positive_keywords)), True)
                        .otherwise(False))
            .withColumn("has_negative_sentiment",
                       F.when(F.lower(F.col("message_content"))
                             .rlike("|".join(negative_keywords)), True)
                        .otherwise(False))

            # Is user active (non-system user)
            .withColumn("is_player_message", F.when(F.col("user_id") > 0, True).otherwise(False))

            # Message category for analytics
            .withColumn("message_category",
                F.when(F.col("message_type") == "system", "System")
                 .when(F.col("message_type") == "game_announcement", "Game_Event")
                 .when(F.col("message_type") == "support", "Support")
                 .when((F.col("message_type") == "public") & (F.col("channel") == "VIP_Lounge"), "VIP_Chat")
                 .when(F.col("message_type") == "public", "Public_Chat")
                 .when(F.col("message_type") == "private", "Private_Message")
                 .otherwise("Other"))
        )

        print("DEBUG: Transformed Chat Messages Schema:")
        df_transformed.printSchema()
        df_transformed.show(5, truncate=False)
        print("DEBUG: Transformed Data Count:", df_transformed.count())

        print(f"Writing to {output_path}")
        MinIOConnector.write_parquet(df_transformed, output_path, mode="overwrite")

        print("Chat messages transformation completed.")
    except Exception as e:
        print(f"ERROR: Failed during transformation process: {e}")
        traceback.print_exc()
        spark.stop()
        sys.exit(1)
    spark.stop()

if __name__ == "__main__":
    main()