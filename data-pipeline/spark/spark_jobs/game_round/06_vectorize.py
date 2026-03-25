from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
from spark_modules.milvus_connector import MilvusConnector
from pyspark.sql.functions import col, concat_ws, pandas_udf
from pyspark.sql.types import ArrayType, FloatType
import os
import pandas as pd

ASSEMBLYAI_API_KEY = os.getenv("ASSEMBLYAI_API_KEY", "")
EMBEDDING_DIM = 768

def get_embedding(text_list):
    """
    Pandas UDF to get embeddings in batch.
    In production, replace with actual embedding service (OpenAI, AssemblyAI, etc.)
    """
    # Mock embedding for demo - replace with actual API call
    embeddings = [[0.1] * EMBEDDING_DIM for _ in text_list]
    return pd.Series(embeddings)

@pandas_udf(ArrayType(FloatType()))
def vectorize_udf(text_series: pd.Series) -> pd.Series:
    if not ASSEMBLYAI_API_KEY or ASSEMBLYAI_API_KEY == "":
        # Return mock embeddings if no API key
        return text_series.apply(lambda x: [0.0] * EMBEDDING_DIM)
    return get_embedding(text_series)

def main():
    spark = get_spark_session("04_Vectorize_GameRound_Load_Milvus")

    input_path = "s3a://silver/hilo_game_rounds"
    df = MinIOConnector.read_parquet(spark, input_path)

    print("DEBUG: Creating text representation for vectorization")

    # Create a text representation combining key features for semantic search
    df_text = df.withColumn(
        "round_description",
        concat_ws(" ",
            col("username"),
            col("bet_type"),
            col("bet_amount"),
            col("result"),
            col("outcome_type"),
            col("bet_category"),
            col("session_id")
        )
    )

    print("DEBUG: Applying vectorization UDF")
    df_vect = df_text.withColumn("vector", vectorize_udf(col("round_description")))

    # Select relevant columns for Milvus
    df_final = df_vect.select(
        "round_id",
        "vector",
        "round_description"
    )

    print("DEBUG: Connecting to Milvus")
    MilvusConnector.connect()
    collection_name = "game_round_vectors"
    collection = MilvusConnector.create_collection(collection_name, EMBEDDING_DIM)

    # Process in batches using toLocalIterator() instead of collect()
    # This streams data instead of loading all to memory
    BATCH_SIZE = 5000
    batch_round_ids = []
    batch_vectors = []
    batch_descriptions = []
    total_inserted = 0

    print("DEBUG: Starting batch insertion to Milvus")

    # Use toLocalIterator to stream rows without loading all to memory
    for row in df_final.toLocalIterator():
        batch_round_ids.append(row.round_id)
        batch_vectors.append(row.vector)
        batch_descriptions.append(row.round_description)

        # When batch is full, insert to Milvus
        if len(batch_round_ids) >= BATCH_SIZE:
            entities = [batch_round_ids, batch_vectors, batch_descriptions]
            collection.insert(entities)
            total_inserted += len(batch_round_ids)
            print(f"Inserted batch: {total_inserted} records")

            # Clear batch
            batch_round_ids = []
            batch_vectors = []
            batch_descriptions = []

    # Insert remaining records
    if batch_round_ids:
        entities = [batch_round_ids, batch_vectors, batch_descriptions]
        collection.insert(entities)
        total_inserted += len(batch_round_ids)
        print(f"Inserted final batch: {total_inserted} total records")

    # Flush once at the end
    collection.flush()
    print(f"Successfully inserted {total_inserted} game round vectors to Milvus.")

    spark.stop()

if __name__ == "__main__":
    main()