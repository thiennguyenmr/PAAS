import sys
import os
from minio import Minio
from minio.commonconfig import CopySource
from airflow_modules.airflow_reader import read_yaml
from airflow_modules.airflow_paths import AIRFLOW_CONFIG_DIR

class AirflowMinioStorage:
    def __init__(self):
        # Target spark config by default
        self.STORAGE_CONFIG_PATH = os.path.join(AIRFLOW_CONFIG_DIR, "app_config.yaml")
        self.STORAGE_CONN_PATH = os.path.join(AIRFLOW_CONFIG_DIR, "storage_config.yaml")
        self.client = None
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

    def init_client(self):
        # 2. Initialize MinIO Client
        print(f">>> Initializing MinIO Client for {self.storage_conn['endpoint']}...")
        try:
            # MinIO python client needs endpoint without http/https prefix for host parameter
            endpoint = self.storage_conn["endpoint"]
            
            self.client = Minio(
                endpoint,
                access_key=self.storage_conn["access_key"],
                secret_key=self.storage_conn["secret_key"],
                secure=self.storage_libs.get("ssl_enabled", False),
            )
            print(">>> MinIO Client initialized.")
        except Exception as e:
            print(f"!!! Error init MinIO Client: {e}")
            sys.exit(1)

    def file_to_bucket(self, path, new_name=None, mode="overwrite"):
        # 3. Process Data
        try:
            bucket = self.storage_conn["bucket"]
            
            # Determine output name
            if new_name:
                output_name = new_name
            else:
                output_name = os.path.basename(path)
            
            # Check if bucket exists
            if not self.client.bucket_exists(bucket):
                print(f">>> Creating bucket: {bucket}")
                self.client.make_bucket(bucket)

            print(f">>> Uploading to MinIO: {bucket}/{output_name}")
            
            # Pure python doesn't handle "mode" directly like Spark does for single files
            # but we can check if file exists if we wanted to mimic it strictly.
            # For simplicity, we just use fput_object which overwrites by default.
            
            self.client.fput_object(
                bucket_name=bucket,
                object_name=output_name,
                file_path=path
            )
            print(">>> Success! File uploaded to MinIO.")
            
        except Exception as e:
            print(f"!!! Error uploading file: {e}")

    def folder_to_bucket(self, path, new_folder, mode="overwrite"):
        print(f">>> Copying folder: {path} -> {new_folder} (mode: {mode})")
        if not os.path.isdir(path):
            print(f"!!! Error: {path} is not a directory.")
            return False

        # Pre-check: count files before copy
        expected_count = self.pre_check(path)
        
        # In MinIO, we might want to "clear" the folder if mode is overwrite
        # but the Spark version just writes files into it.
        # If we want to mimic Spark's mode='overwrite', we'd need to delete existing objects.
        if mode == "overwrite":
            print(f">>> Mode is overwrite. Clearing existing objects in {new_folder}...")
            self._delete_objects(new_folder)

        for filename in os.listdir(path):
            file_path = os.path.join(path, filename)
            if os.path.isfile(file_path):
                # Construct new name as "new_folder/filename"
                new_name = f"{new_folder}/{filename}"
                print(f"    -> Uploading file: {filename}")
                self.file_to_bucket(file_path, new_name, mode)
        
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
            
            # List objects in the prefix
            objects = self.client.list_objects(bucket, prefix=minio_folder, recursive=True)
            minio_count = sum(1 for _ in objects)
            
            print(f">>> Post-check: Found {minio_count} objects in MinIO ({minio_folder})")
            
            if minio_count == expected_count:
                print(f">>> Validation PASSED: {minio_count}/{expected_count} files copied successfully!")
                return True
            else:
                print(f"!!! Validation FAILED: Expected {expected_count} files, found {minio_count}")
                return False
                
        except Exception as e:
            print(f"!!! Error in post-check: {e}")
            return False

    def files_list(self, folder_path, bucket=None):
        """List all files in a MinIO folder."""
        files = []
        try:
            target_bucket = bucket if bucket else self.storage_conn["bucket"]
            objects = self.client.list_objects(target_bucket, prefix=folder_path, recursive=False)
            
            for obj in objects:
                # Filter out "directories" if needed, but minio list_objects returns Object items
                files.append({
                    'name': os.path.basename(obj.object_name),
                    'size': obj.size,
                    'full_path': obj.object_name
                })
            
            return files
        except Exception as e:
            print(f"!!! Error listing MinIO files: {e}")
            return files

    def _delete_objects(self, prefix):
        """Internal helper to delete objects with a prefix."""
        try:
            bucket = self.storage_conn["bucket"]
            objects = self.client.list_objects(bucket, prefix=prefix, recursive=True)
            for obj in objects:
                self.client.remove_object(bucket, obj.object_name)
        except Exception as e:
            print(f"!!! Error deleting objects: {e}")

    def bucket_to_bucket(self, source_bucket, target_bucket, source_prefix="", target_prefix=""):
        """Copy objects from one bucket to another."""
        try:
            print(f">>> Copying from bucket '{source_bucket}' (prefix: '{source_prefix}') to '{target_bucket}' (prefix: '{target_prefix}')...")
            
            objects = self.client.list_objects(source_bucket, prefix=source_prefix, recursive=True)
            
            count = 0
            for obj in objects:
                if obj.is_dir:
                    continue
                
                # Construct target name
                # If source_prefix is "data/", and obj is "data/file.txt"
                # relative path is "file.txt"
                relative_path = os.path.relpath(obj.object_name, source_prefix) if source_prefix else obj.object_name
                
                # If target_prefix is "sample_data", target path is "sample_data/file.txt"
                target_object_name = os.path.join(target_prefix, relative_path).replace("\\", "/") # Ensure forward slashes
                
                print(f"    -> Copying: {obj.object_name} -> {target_object_name}")
                
                self.client.copy_object(
                    target_bucket,
                    target_object_name,
                    CopySource(source_bucket, obj.object_name)
                )
                count += 1
            
            print(f">>> Success! {count} objects copied.")
            return True
        except Exception as e:
            print(f"!!! Error copying between buckets: {e}")
            return False