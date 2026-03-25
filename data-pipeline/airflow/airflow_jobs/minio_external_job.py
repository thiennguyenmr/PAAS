"""
MinIO External Connection Job
Uses AirflowMinioStorage with external config (minio_external)
"""

import sys
import os
from datetime import datetime

# Ensure we can import from the airflow directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow_modules.airflow_minio import AirflowMinioStorage


# Config name for external access
CONFIG_NAME = "minio_external"


def get_storage():
    """Get configured AirflowMinioStorage instance"""
    storage = AirflowMinioStorage()
    storage.load_config(CONFIG_NAME)
    storage.init_client()
    return storage


# =============================================================================
# Task Functions (called by Airflow DAG)
# =============================================================================

def task_ensure_bucket(**context):
    """Airflow task: Ensure bucket exists"""
    storage = get_storage()
    return storage.ensure_bucket()


def task_upload_json(prefix: str = "demo", **context):
    """Airflow task: Upload sample JSON data"""
    storage = get_storage()
    storage.ensure_bucket()

    # Sample data with context info
    sample_data = {
        "pipeline_run": context.get("run_id", "manual"),
        "execution_date": str(context.get("execution_date", datetime.now())),
        "records": [
            {"id": 1, "name": "Record 1", "value": 100},
            {"id": 2, "name": "Record 2", "value": 200},
            {"id": 3, "name": "Record 3", "value": 300},
        ],
        "metadata": {
            "source": "airflow_minio_external",
            "created_at": datetime.now().isoformat(),
        }
    }

    ds = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
    object_name = f"{prefix}/airflow_output/{ds}/sample_data.json"

    return storage.upload_json(object_name, sample_data)


def task_upload_csv(prefix: str = "demo", **context):
    """Airflow task: Upload sample CSV data"""
    storage = get_storage()
    storage.ensure_bucket()

    csv_content = """id,name,value,category
                    1,Item A,100,electronics
                    2,Item B,200,clothing
                    3,Item C,150,electronics
                    4,Item D,300,furniture
                    5,Item E,250,clothing
                """

    ds = context.get("ds", datetime.now().strftime("%Y-%m-%d"))
    object_name = f"{prefix}/airflow_output/{ds}/sample_data.csv"

    return storage.upload_csv(object_name, csv_content)


def task_list_objects(prefix: str = "demo/", **context):
    """Airflow task: List objects in bucket"""
    storage = get_storage()
    files = storage.files_list(prefix)
    for f in files:
        print(f"    - {f['full_path']} ({f['size']} bytes)")
    return files


# =============================================================================
# Main (for standalone testing)
# =============================================================================

def main():
    """Test the MinIO external connection"""
    print("=" * 60)
    print("MinIO External Connection Test")
    print("=" * 60)

    try:
        storage = get_storage()
        storage.ensure_bucket()

        # Upload test data
        test_data = {"test": True, "timestamp": datetime.now().isoformat()}
        storage.upload_json("test/connection_test.json", test_data)

        # List objects
        files = storage.files_list("test/")
        print(f"\n>>> Found {len(files)} files")

        print("\n>>> All tests passed!")

    except Exception as e:
        print(f"!!! Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
