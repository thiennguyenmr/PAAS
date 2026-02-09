from pyspark.sql import DataFrame
from spark_configs.settings import NEO4J_URL, NEO4J_USER, NEO4J_PASSWORD

class Neo4jConnector:
    # Batch size for Neo4j writes - prevents OOM with large datasets
    BATCH_SIZE = 5000

    @staticmethod
    def write_nodes(df: DataFrame, labels: str, keys: str):
        """
        Writes nodes to Neo4j with batching for large datasets.
        """
        df.write \
            .format("org.neo4j.spark.DataSource") \
            .mode("Overwrite") \
            .option("url", NEO4J_URL) \
            .option("authentication.type", "basic") \
            .option("authentication.basic.username", NEO4J_USER) \
            .option("authentication.basic.password", NEO4J_PASSWORD) \
            .option("labels", labels) \
            .option("node.keys", keys) \
            .option("batch.size", Neo4jConnector.BATCH_SIZE) \
            .option("transaction.codes.fail", "Neo.ClientError.Schema.ConstraintValidationFailed") \
            .save()

    @staticmethod
    def write_relationship(df: DataFrame, query: str):
        """
        Writes relationships using a Cypher query with batching for large datasets.
        """
        df.dropna().write \
            .format("org.neo4j.spark.DataSource") \
            .mode("Append") \
            .option("url", NEO4J_URL) \
            .option("authentication.type", "basic") \
            .option("authentication.basic.username", NEO4J_USER) \
            .option("authentication.basic.password", NEO4J_PASSWORD) \
            .option("query", query) \
            .option("batch.size", Neo4jConnector.BATCH_SIZE) \
            .option("transaction.codes.fail", "Neo.ClientError.Schema.ConstraintValidationFailed") \
            .save()
