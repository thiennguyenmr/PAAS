import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from spark_modules.reader import read_yaml
from spark_modules.paths import CONFIG_DIR


class SqlServerConnector:
    def __init__(self, db_config_path=None):
        self.APP_CONFIG_PATH = os.path.join(CONFIG_DIR, "app_config.yaml")
        self.DB_CONFIG_PATH = db_config_path if db_config_path else os.path.join(CONFIG_DIR, "db_configs.yaml")
        self.spark = None
        self.app_conf = None
        self.db_conf = None

    def load_config(self, spark_config_name="sqlserver", db_config_name="server_sqlserver"):
        """Load and merge configurations."""
        try:
            print(">>> [Module: Config] Loading configurations...")
            app_conf_raw = read_yaml(self.APP_CONFIG_PATH)
            db_conf_raw = read_yaml(self.DB_CONFIG_PATH)
            
            # 1. Get General Spark Config (Driver & Maven Package)
            spark_conf = app_conf_raw[spark_config_name]
            
            # 2. Get DB Specific Config (Connection Info)
            db_conf = db_conf_raw[db_config_name]
            
            # 3. Create merged config for Spark Session init
            self.app_conf = {**spark_conf, **db_conf}
            
            print(">>> Configuration loaded successfully.")
        except Exception as e:
            print(f"!!! Error loading config: {e}")
            sys.exit(1)

    def init_spark(self, app_name="SqlServerConnector"):
        """Initialize Spark with SQL Server support."""
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

    def check_spark_alive(self):
        """Check if Spark session is alive."""
        if self.spark is None:
            return False
        try:
            # Simple operation to verify connection
            self.spark.sql("SELECT 1")
            print(">>> Spark connection verified.")
            return True
        except Exception as e:
            print(f"!!! Spark connection check failed: {e}")
            return False

    def build_csv_schema(self, columns_config):
        """Build Spark schema from column configuration."""
        type_mapping = {
            'int': IntegerType(),
            'integer': IntegerType(),
            'string': StringType(),
            'str': StringType(),
            'datetime': TimestampType(),
            'timestamp': TimestampType()
        }
        
        fields = []
        for col in columns_config:
            col_name = col['name']
            col_type = col.get('type', 'string').lower()
            spark_type = type_mapping.get(col_type, StringType())
            fields.append(StructField(col_name, spark_type, True))
        
        return StructType(fields)

    def read_csv(self, csv_path, file_conf):
        """Read CSV file with configured options."""
        try:
            print(f"    -> Reading file: {csv_path}")
            
            # Build CSV reader options from config
            csv_options = {
                'header': str(file_conf.get('header', True)).lower(),
                'sep': file_conf.get('delimiter', ','),
                'encoding': file_conf.get('encoding', 'utf-8'),
                'quote': file_conf.get('quote_char', '"'),
                'escape': file_conf.get('escape_char', '\\')
            }
            
            # Build schema if columns are defined
            schema = None
            if 'columns' in file_conf and file_conf['columns']:
                schema = self.build_csv_schema(file_conf['columns'])
                print(f"    -> Using defined schema with {len(file_conf['columns'])} columns")
            
            # Read CSV with all options
            reader = self.spark.read.options(**csv_options)
            if schema:
                df = reader.schema(schema).csv(csv_path)
            else:
                df = reader.csv(csv_path, inferSchema=True)
            
            # Apply skip_rows if needed
            skip_rows = file_conf.get('skip_rows', 0)
            if skip_rows > 0:
                df = df.limit(df.count() - skip_rows).offset(skip_rows)
            
            return df
        except AnalysisException as e:
            print(f"!!! Error reading CSV file: {e}")
            raise

    def write_to_table(self, df, table_name, mode="overwrite"):
        """Write DataFrame to database table."""
        try:
            print(f"    -> Writing to table '{table_name}'...")
            
            # Inject driver class from spark config into db properties
            db_props = self.app_conf["db_properties"].copy()
            db_props["driver"] = self.app_conf["driver_class"]
            
            df.write.jdbc(
                url=self.app_conf["jdbc_url"], 
                table=table_name, 
                mode=mode, 
                properties=db_props
            )
            print(">>> [Success] Data successfully loaded to SQL Server!")
        except Exception as e:
            print(f"!!! Error writing to Database: {e}")
            raise

    def read_from_table(self, table_name):
        """Read DataFrame from database table."""
        try:
            print(f"    -> Reading from table '{table_name}'...")
            
            db_props = self.app_conf["db_properties"].copy()
            db_props["driver"] = self.app_conf["driver_class"]
            
            df = self.spark.read.jdbc(
                url=self.app_conf["jdbc_url"],
                table=table_name,
                properties=db_props
            )
            print(f">>> [Success] Data read from table '{table_name}'")
            return df
        except Exception as e:
            print(f"!!! Error reading from Database: {e}")
            raise

    def exec_sql(self, sql_query):
        """Execute arbitrary SQL query against the database."""
        try:
            print(f"    -> Executing SQL: {sql_query}")
            
            db_props = self.app_conf["db_properties"].copy()
            db_props["driver"] = self.app_conf["driver_class"]
            
            # Use subquery to execute arbitrary SQL
            query = f"({sql_query}) as tmp_table"
            
            df = self.spark.read.jdbc(
                url=self.app_conf["jdbc_url"],
                table=query,
                properties=db_props
            )
            print(">>> [Success] SQL executed successfully.")
            return df
        except Exception as e:
            print(f"!!! Error executing SQL: {e}")
            raise

    def table_exists(self, table_name):
        """Check if a table exists in the database."""
        try:
            print(f"    -> Checking if table '{table_name}' exists...")
            # Using LIMIT 0 to avoid fetching any data, just verify existence
            # SQL Server might optimize this differently but it should work
            self.exec_sql(f"SELECT TOP 0 1 FROM {table_name}")
            print(f">>> [Response] Table '{table_name}' exists.")
            return True
        except Exception:
            print(f">>> [Response] Table '{table_name}' does not exist.")
            return False

    def get_table_schema(self, table_name):
        """Get the schema of a table."""
        try:
            print(f"    -> Getting schema for table '{table_name}'...")
            df = self.read_from_table(table_name)
            return df.schema
        except Exception as e:
            print(f"!!! Error getting table schema: {e}")
            raise

    def check_db_connection(self):
        """Check if the database connection is alive."""
        try:
            print(">>> Checking database connection...")
            # Use simple query to test connection
            self.exec_sql("SELECT 1")
            print(">>> [Success] Database connection is alive.")
            return True
        except Exception as e:
            print(f"!!! Database connection check failed: {e}")
            return False

    def stop_spark(self):
        """Stop Spark session."""
        if self.spark:
            self.spark.stop()
            print(">>> Spark Session stopped.")
