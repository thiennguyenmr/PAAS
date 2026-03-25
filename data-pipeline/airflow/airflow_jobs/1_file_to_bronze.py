#init
import sys
import os

# Ensure we can import from the airflow directory
# airflow/airflow_process/demo_minio.py -> airflow/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow_modules.airflow_paths import AIRFLOW_CONFIG_DIR
from airflow_modules.airflow_minio import AirflowMinioStorage

#---------Maincode

def main():
    print("--- DEMO AIRFLOW MINIO STARTED (Bucket-to-Bucket) ---")
    storage = AirflowMinioStorage()
    
    try:
        # 1. Load configuration (using minio_bronze as base for client init)
        storage.load_config("minio_bronze")
        storage.init_client()
        
        # 2. Define source and target details (referring to airflow/1_minio_bronze.py)
        source_bucket = "sharepoint"
        source_prefix = "" # Files are in root
        
        target_bucket = "bronze"
        target_prefix = "sample_data"
        
        # 3. Perform copy using the new bucket_to_bucket method
        print(f"\n>>> Copying from bucket '{source_bucket}' to '{target_bucket}/{target_prefix}'...")
        
        if storage.bucket_to_bucket(
            source_bucket=source_bucket,
            target_bucket=target_bucket,
            source_prefix=source_prefix,
            target_prefix=target_prefix
        ):
            print(">>> Demo: bucket_to_bucket finished successfully!")
        else:
            print(">>> Demo: bucket_to_bucket failed!")
            
        # 4. Verification: List files in bronze/sample_data
        print(f"\n>>> Verifying files in {target_bucket}/{target_prefix}...")
        files = storage.files_list(target_prefix, bucket=target_bucket)
        for f in files:
            print(f"    - {f['name']} ({f['size']} bytes)")

    except Exception as e:
        print(f"!!! Error in execution: {e}")
    finally:
        print("\n--- PROGRAM FINISHED ---")

if __name__ == "__main__":
    main()
