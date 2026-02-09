from pyspark.sql import DataFrame, SparkSession

class MinIOConnector:
    @staticmethod
    def read_parquet(spark: SparkSession, path: str) -> DataFrame:
        """
        Reads a parquet file from MinIO.
        """
        return spark.read.parquet(path)

    @staticmethod
    def write_parquet(df: DataFrame, path: str, mode: str = "overwrite"):
        """
        Writes a DataFrame to MinIO in parquet format.
        """
        df.write.mode(mode).parquet(path)
