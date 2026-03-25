import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from spark_modules.reader import read_yaml
from spark_modules.paths import CONFIG_DIR


class MilvusConnector:
    def __init__(self, db_config_path=None):
        self.APP_CONFIG_PATH = os.path.join(CONFIG_DIR, "app_config.yaml")
        self.DB_CONFIG_PATH = db_config_path if db_config_path else os.path.join(CONFIG_DIR, "db_configs.yaml")
        self.spark = None
        self.app_conf = None

    def load_config(self, spark_config_name="milvus", db_config_name="milvus"):
        """Load and merge configurations."""
        try:
            print(">>> [Module: Config] Loading Milvus configurations...")
            app_conf_raw = read_yaml(self.APP_CONFIG_PATH)
            db_conf_raw = read_yaml(self.DB_CONFIG_PATH)
            
            # 1. Get General Spark Config (Maven Package)
            spark_conf = app_conf_raw[spark_config_name]
            
            # 2. Get DB Specific Config (Connection Info)
            db_conf = db_conf_raw[db_config_name]
            
            # 3. Create merged config for Spark Session init
            self.app_conf = {**spark_conf, **db_conf}
            
            print(">>> Milvus configuration loaded successfully.")
        except Exception as e:
            print(f"!!! Error loading Milvus config: {e}")
            sys.exit(1)

    def init_spark(self, app_name="MilvusConnector"):
        """Initialize Spark with Milvus support."""
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

    def write_to_milvus(self, df, collection_name, mode="append"):
        """Write DataFrame to Milvus collection."""
        try:
            print(f"    -> Writing to Milvus collection '{collection_name}'...")
            
            df.write.format("io.milvus.spark.MilvusSource") \
                .option("milvus.host", self.app_conf["host"]) \
                .option("milvus.port", self.app_conf["port"]) \
                .option("milvus.collection.name", collection_name) \
                .mode(mode) \
                .save()
                
            print(f">>> [Success] Data successfully loaded to Milvus collection '{collection_name}'!")
        except Exception as e:
            print(f"!!! Error writing to Milvus: {e}")
            raise

    def read_from_milvus(self, collection_name):
        """Read DataFrame from Milvus collection."""
        try:
            print(f"    -> Reading from Milvus collection '{collection_name}'...")
            
            df = self.spark.read.format("io.milvus.spark.MilvusSource") \
                .option("milvus.host", self.app_conf["host"]) \
                .option("milvus.port", self.app_conf["port"]) \
                .option("milvus.collection.name", collection_name) \
                .load()
                
            print(f">>> [Success] Data read from Milvus collection '{collection_name}'")
            return df
        except Exception as e:
            print(f"!!! Error reading from Milvus: {e}")
            raise

    def stop_spark(self):
        """Stop Spark session."""
        if self.spark:
            self.spark.stop()
            print(">>> Spark Session stopped.")
