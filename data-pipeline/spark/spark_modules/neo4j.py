import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from spark_modules.reader import read_yaml
from spark_modules.paths import CONFIG_DIR


class Neo4jConnector:
    def __init__(self, db_config_path=None):
        self.APP_CONFIG_PATH = os.path.join(CONFIG_DIR, "app_config.yaml")
        self.DB_CONFIG_PATH = db_config_path if db_config_path else os.path.join(CONFIG_DIR, "db_configs.yaml")
        self.spark = None
        self.app_conf = None

    def load_config(self, spark_config_name="neo4j", db_config_name="neo4j"):
        """Load and merge configurations."""
        try:
            print(">>> [Module: Config] Loading Neo4j configurations...")
            app_conf_raw = read_yaml(self.APP_CONFIG_PATH)
            db_conf_raw = read_yaml(self.DB_CONFIG_PATH)
            
            # 1. Get General Spark Config (Maven Package)
            spark_conf = app_conf_raw[spark_config_name]
            
            # 2. Get DB Specific Config (Connection Info)
            db_conf = db_conf_raw[db_config_name]
            
            # 3. Create merged config for Spark Session init
            self.app_conf = {**spark_conf, **db_conf}
            
            print(">>> Neo4j configuration loaded successfully.")
        except Exception as e:
            print(f"!!! Error loading Neo4j config: {e}")
            sys.exit(1)

    def init_spark(self, app_name="Neo4jConnector"):
        """Initialize Spark with Neo4j support."""
        print(f">>> Initializing Spark with {app_name} support...")
        try:
            self.spark = (
                SparkSession.builder
                .appName(app_name)
                .master("local[*]")
                .config("spark.jars.packages", self.app_conf["maven_package"])
                .getOrCreate()
            )
            
            # Suppress logging
            self.spark.sparkContext.setLogLevel("ERROR")
            print(">>> Spark Session initialized.")
        except Exception as e:
            print(f"!!! Error init Spark: {e}")
            sys.exit(1)

    def write_to_neo4j(self, df, label=None, relationship=None, mode="Overwrite"):
        """Write DataFrame to Neo4j."""
        try:
            print(f"    -> Writing to Neo4j...")
            
            writer = df.write.format("org.neo4j.spark.DataSource") \
                .option("url", self.app_conf["url"]) \
                .option("authentication.type", "basic") \
                .option("authentication.basic.username", self.app_conf["user"]) \
                .option("authentication.basic.password", self.app_conf["password"]) \
                .mode(mode)
            
            if label:
                writer.option("labels", label)
            if relationship:
                writer.option("relationship", relationship)
                
            writer.save()
            print(">>> [Success] Data successfully loaded to Neo4j!")
        except Exception as e:
            print(f"!!! Error writing to Neo4j: {e}")
            raise

    def read_from_neo4j(self, query=None, label=None):
        """Read DataFrame from Neo4j."""
        try:
            print(f"    -> Reading from Neo4j...")
            
            reader = self.spark.read.format("org.neo4j.spark.DataSource") \
                .option("url", self.app_conf["url"]) \
                .option("authentication.type", "basic") \
                .option("authentication.basic.username", self.app_conf["user"]) \
                .option("authentication.basic.password", self.app_conf["password"])
                
            if query:
                reader.option("query", query)
            elif label:
                reader.option("labels", label)
            else:
                raise ValueError("Either query or label must be provided for Neo4j reading")
                
            df = reader.load()
            print(">>> [Success] Data read from Neo4j")
            return df
        except Exception as e:
            print(f"!!! Error reading from Neo4j: {e}")
            raise

    def stop_spark(self):
        """Stop Spark session."""
        if self.spark:
            self.spark.stop()
            print(">>> Spark Session stopped.")
