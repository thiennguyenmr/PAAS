import sys
import os
from pyspark.sql import SparkSession
from spark_modules.reader import read_yaml
from spark_modules.paths import CONFIG_DIR

class MinioStorage:
    def __init__(self):
        self.STORAGE_CONFIG_PATH = os.path.join(CONFIG_DIR, "app_config.yaml")
        self.STORAGE_CONN_PATH = os.path.join(CONFIG_DIR, "storage_config.yaml")
        self.spark = None
        self.storage_libs = None
        self.storage_conn = None

    def load_config(self, config_name="demo_minio"):
        # 1. Load Configurations
        try:
            self.storage_libs = read_yaml(self.STORAGE_CONFIG_PATH)['minio']
            self.storage_conn = read_yaml(self.STORAGE_CONN_PATH)[config_name]
            print(">>> Configuration loaded.")
        except Exception as e:
            print(f"!!! Error loading config: {e}")
            sys.exit(1)

    def init_client(self, app_name="MinIO"):
        # 2. Initialize Spark with MinIO/S3 support
        print(f">>> Initializing Spark with {app_name} support...")
        try:
            self.spark = (
                SparkSession.builder
                .appName(app_name)
                .master("local[*]")
                .config("spark.jars.packages", self.storage_libs["maven_package"])
                .config("spark.hadoop.fs.s3a.endpoint", self.storage_conn["endpoint"])
                .config("spark.hadoop.fs.s3a.access.key", self.storage_conn["access_key"])
                .config("spark.hadoop.fs.s3a.secret.key", self.storage_conn["secret_key"])
                .config("spark.hadoop.fs.s3a.path.style.access", str(self.storage_libs["path_style_access"]).lower())
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(self.storage_libs.get("ssl_enabled", "false")).lower())
                .config("spark.hadoop.fs.s3a.impl", self.storage_libs["file_system_impl"])
                
                #FIX NumberFormatException
                .config("spark.hadoop.fs.s3a.connection.timeout", self.storage_libs["connection_timeout"])
                .config("spark.hadoop.fs.s3a.socket.timeout", self.storage_libs["socket_timeout"])
                .config("spark.hadoop.fs.s3a.retry.interval", self.storage_libs["retry_interval"])
                .config("spark.hadoop.fs.s3a.attempts.maximum", self.storage_libs["max_attempts"])
                .config("spark.hadoop.fs.s3a.threads.keepalivetime", self.storage_libs["keep_alive_time"])
                .config("spark.hadoop.fs.s3a.multipart.purge.age", self.storage_libs["multipart_purge_age"])
                .config("spark.hadoop.fs.s3a.connection.establish.timeout", self.storage_libs["establish_timeout"])
                #FIX ClassNotFoundException
                .config(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    self.storage_libs["aws_credentials_provider"]
                )
                .getOrCreate()
            )
            
            # Suppress logging
            self.spark.sparkContext.setLogLevel("ERROR")
            print(">>> Spark Session initialized.")
        except Exception as e:
            print(f"!!! Error init Spark: {e}")
            sys.exit(1)

    def file_to_bucket(self, path, new_name=None, mode="overwrite"):
        # 3. Process Data
        try:
            # Read CSV
            print(f">>> Reading file: {path}")
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(path)
            df.show(5)
            
            # Write to MinIO
            bucket = self.storage_conn["bucket"]
            
            # Determine output name
            if new_name:
                output_name = new_name
            else:
                output_name = os.path.basename(path)
                
            output_path = f"s3a://{bucket}/{output_name}"
            print(f">>> Writing to MinIO: {output_path} (mode: {mode})")
            
            df.write.option("header", "true").mode(mode).csv(output_path)
            print(">>> Success! Data written to MinIO.")
            
        except Exception as e:
            print(f"!!! Error processing data: {e}")

    def folder_to_bucket(self, path, new_folder, mode="overwrite"):
        print(f">>> Copying folder: {path} -> {new_folder} (mode: {mode})")
        if not os.path.isdir(path):
            print(f"!!! Error: {path} is not a directory.")
            return False

        # Pre-check: count files before copy
        expected_count = self.pre_check(path)
        
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            if os.path.isfile(file_path):
                # Construct new name as "new_folder/filename"
                new_name = f"{new_folder}/{filename}"
                print(f"    -> Processing file: {filename}")
                self.copy_file(file_path, new_name, mode)
        
        # Post-check: validate files copied to MinIO
        return self.post_check(new_folder, expected_count)

    def pre_check(self, path):
        """Count files in local directory before copy."""
        if not os.path.isdir(path):
            print(f"!!! Error: {path} is not a directory.")
            return 0
        
        file_count = 0
        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            if os.path.isfile(file_path):
                file_count += 1
        
        print(f">>> Pre-check: Found {file_count} files in {path}")
        return file_count

    def post_check(self, minio_folder, expected_count):
        """Verify files copied to MinIO match expected count."""
        try:
            bucket = self.storage_conn["bucket"]
            s3_path = f"s3a://{bucket}/{minio_folder}"
            
            # Use Hadoop FileSystem to list files
            hadoop_conf = self.spark._jsc.hadoopConfiguration()
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jvm.java.net.URI(f"s3a://{bucket}"),
                hadoop_conf
            )
            
            path_obj = self.spark._jvm.org.apache.hadoop.fs.Path(s3_path)
            
            if not fs.exists(path_obj):
                print(f"!!! Post-check FAILED: MinIO path does not exist: {s3_path}")
                return False
            
            # Count directories (each file becomes a directory with part files)
            file_statuses = fs.listStatus(path_obj)
            minio_count = 0
            for status in file_statuses:
                if status.isDirectory():
                    dir_name = status.getPath().getName()
                    # Skip hidden directories
                    if not dir_name.startswith('_') and not dir_name.startswith('.'):
                        minio_count += 1
            
            print(f">>> Post-check: Found {minio_count} files in MinIO ({minio_folder})")
            
            if minio_count == expected_count:
                print(f">>> Validation PASSED: {minio_count}/{expected_count} files copied successfully!")
                return True
            else:
                print(f"!!! Validation FAILED: Expected {expected_count} files, found {minio_count}")
                return False
                
        except Exception as e:
            print(f"!!! Error in post-check: {e}")
            return False

    def read_file(self, file_path, options=None):
        """Read a single file from MinIO."""
        try:
            bucket = self.storage_conn["bucket"]
            s3_path = f"s3a://{bucket}/{file_path}"
            print(f">>> Reading file from MinIO: {s3_path}")
            
            # Default options
            read_options = {
                "header": "true",
                "inferSchema": "true"
            }
            if options:
                read_options.update(options)
            
            df = self.spark.read.options(**read_options).csv(s3_path)
            return df
        except Exception as e:
            print(f"!!! Error reading file from MinIO: {e}")
            return None

    def write_file(self, df, file_path, format="parquet", mode="overwrite"):
        """Write a DataFrame to MinIO."""
        try:
            bucket = self.storage_conn["bucket"]
            s3_path = f"s3a://{bucket}/{file_path}"
            print(f">>> Writing data to MinIO: {s3_path} (format: {format}, mode: {mode})")
            
            df.write.option("header", "true").mode(mode).format(format).save(s3_path)
            print(">>> Success! Data written to MinIO.")
            return True
        except Exception as e:
            print(f"!!! Error writing data to MinIO: {e}")
            return False

    def list_files(self, folder_path):
        """List all files in a MinIO folder (non-recursive for top-level checks)."""
        files = []
        try:
            bucket = self.storage_conn["bucket"]
            s3_path = f"s3a://{bucket}/{folder_path}"
            
            # Use Hadoop FileSystem
            hadoop_conf = self.spark._jsc.hadoopConfiguration()
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jvm.java.net.URI(f"s3a://{bucket}"),
                hadoop_conf
            )
            
            path_obj = self.spark._jvm.org.apache.hadoop.fs.Path(s3_path)
            
            if not fs.exists(path_obj):
                print(f"!!! MinIO path does not exist: {s3_path}")
                return files
            
            file_statuses = fs.listStatus(path_obj)
            for status in file_statuses:
                # We expect "files" (which are actually directories in Parquet/CSV on S3)
                # or real files (if uploaded directly).
                # For this specific requirement (reading CSVs uploaded folder style),
                # we look for "directories" that represent the files, OR actual files.
                # However, your current flow uploads FOLDERS of files.
                # Let's assume the structure is bucket/folder/file.csv
                
                # Check if it's a directory (which might be a "file" in some contexts like parity with local)
                # or a file.
                
                name = status.getPath().getName()
                if not name.startswith('_') and not name.startswith('.'):
                    files.append({
                        'name': name,
                        'size': status.getLen(),
                        'full_path': str(status.getPath())
                    })
            
            return files
        except Exception as e:
            print(f"!!! Error listing MinIO files: {e}")
            return files

    def stop_spark(self):
        if self.spark:
            self.spark.stop()
            print(">>> Spark Session stopped.")

