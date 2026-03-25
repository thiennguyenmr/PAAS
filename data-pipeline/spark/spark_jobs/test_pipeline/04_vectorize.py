from spark_modules.session_manager import get_spark_session
from spark_modules.minio_connector import MinIOConnector
from spark_modules.milvus_connector import MilvusConnector
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import ArrayType, FloatType
import os
import pandas as pd

ASSEMBLYAI_API_KEY = os.getenv("ASSEMBLYAI_API_KEY", "f44ccc009fb4555b65360454e23f999")
EMBEDDING_DIM = 768 

def get_embedding(text_list):
    """
    Pandas UDF to get embeddings in batch.
    """
    # Mock embedding for demo
    embeddings = [[0.1] * EMBEDDING_DIM for _ in text_list]
    return pd.Series(embeddings)

@pandas_udf(ArrayType(FloatType()))
def vectorize_udf(text_series: pd.Series) -> pd.Series:
    if not ASSEMBLYAI_API_KEY or ASSEMBLYAI_API_KEY == "your_api_key_here":
        return text_series.apply(lambda x: [0.0] * EMBEDDING_DIM)
    return get_embedding(text_series)

def main():
    spark = get_spark_session("04_Vectorize_and_Load_Milvus")

    input_path = "s3a://silver/hilo_game_data"
    df = MinIOConnector.read_parquet(spark, input_path)

    if "status" in df.columns:
        df_vect = df.withColumn("vector", vectorize_udf(col("status")))
        rows = df_vect.select("id", "vector", "status").collect()
        
        MilvusConnector.connect()
        collection_name = "hilo_game_vectors"
        collection = MilvusConnector.create_collection(collection_name, EMBEDDING_DIM)
            
        # Insert
        ids = [row.id for row in rows]
        vectors = [row.vector for row in rows]
        statuses = [row.status for row in rows]
        
        if ids:
            entities = [ids, vectors, statuses]
            MilvusConnector.insert(collection, entities)
            print(f"Inserted {len(ids)} vectors to Milvus.")
        else:
            print("No data to insert.")
    else:
        print("Column 'status' not found for vectorization.")

    spark.stop()

if __name__ == "__main__":
    main()
