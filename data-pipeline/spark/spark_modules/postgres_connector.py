from pyspark.sql import DataFrame, SparkSession
from spark_configs.settings import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS

class PostgresConnector:
    def __init__(self, dbname=None):
        """
        Initializes connection settings.
        """
        self.host = PG_HOST
        self.port = PG_PORT
        self.user = PG_USER
        self.password = PG_PASS
        self.dbname = dbname if dbname is not None else PG_DB
        self.jdbc_url = f"jdbc:postgresql://{self.host}:{self.port}/{self.dbname}"

    def read_table(self, spark: SparkSession, table_name: str) -> DataFrame:
        """
        Reads a table from Postgres into a Spark DataFrame using JDBC.
        """
        return spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

    def write_table(self, df: DataFrame, table_name: str, mode: str = "append"):
        """
        Writes a Spark DataFrame to Postgres using JDBC.
        """
        df.write \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", "org.postgresql.Driver") \
            .mode(mode) \
            .save()

    def execute_query(self, spark: SparkSession, sql: str) -> DataFrame:
        """
        Executes a custom SQL query in Postgres and returns a Spark DataFrame.
        Note: The query is executed in a subquery by Spark.
        """
        return spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("query", sql) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", "org.postgresql.Driver") \
            .option("driver", "org.postgresql.Driver") \
            .load()
