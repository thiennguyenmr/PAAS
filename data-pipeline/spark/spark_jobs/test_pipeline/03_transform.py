from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
from pyspark.sql.functions import col, from_json, schema_of_json

def main():
    spark = get_spark_session("03_Transform_Bronze_to_Silver")

    input_path = "s3a://bronze/hilo_game_data"
    output_path = "s3a://silver/hilo_game_data"

    print(f"Reading from {input_path}")
    df = MinIOConnector.read_parquet(spark, input_path)

    # 1. Infer schema for json column 'game_info'
    json_sample = df.select("game_info").limit(1).collect()[0][0]
    if json_sample:
        json_schema = schema_of_json(json_sample)
        
        df_transformed = df.withColumn("game_details", from_json(col("game_info"), json_schema)) \
                           .select(
                               col("id"),
                               col("user_id"),
                               col("created_at"),
                               col("game_details.*")  # Flatten the struct
                           )
    else:
        # Fallback if empty or null
        df_transformed = df

    print(f"Writing to {output_path}")
    MinIOConnector.write_parquet(df_transformed, output_path, mode="overwrite")

    print("Transformation completed.")
    spark.stop()

if __name__ == "__main__":
    main()
