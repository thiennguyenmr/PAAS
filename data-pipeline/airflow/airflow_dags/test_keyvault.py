from datetime import timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.bases.hook import BaseHook
import pendulum

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['alerts@hilochatbot.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10),
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Ho_Chi_Minh'),
}

# DAG definition
dag = DAG(
    'test_keyvault',
    default_args=default_args,
    description='Test Vault integration - fetch secrets via Airflow connections',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'vault', 'keyvault', 'security'],
)


def test_vault_connections():
    """
    Test fetching all connections from Vault.
    When Vault backend is configured, BaseHook.get_connection()
    automatically fetches from Vault instead of Airflow metadata DB.
    """
    connections_to_test = [
        'minio_bronze',
        'minio_silver',
        'pg_app',
        'neo4j_default',
        'milvus_default',
        'spark_default',
    ]

    print("=" * 60)
    print("VAULT CONNECTION TEST")
    print("=" * 60)

    results = {}
    for conn_id in connections_to_test:
        try:
            conn = BaseHook.get_connection(conn_id)
            results[conn_id] = {
                'status': 'OK',
                'host': conn.host,
                'port': conn.port,
                'login': conn.login,
                'schema': conn.schema,
                'has_password': bool(conn.password),
            }
            print(f"[OK] {conn_id}: {conn.host}:{conn.port} (user: {conn.login})")
        except Exception as e:
            results[conn_id] = {'status': 'FAILED', 'error': str(e)}
            print(f"[FAILED] {conn_id}: {e}")

    print("=" * 60)

    # Fail if any connection failed
    failed = [k for k, v in results.items() if v['status'] == 'FAILED']
    if failed:
        raise Exception(f"Failed to fetch connections: {failed}")

    return results


def test_minio_connection():
    """
    Test MinIO connection using the new load_config_from_connection method.
    This demonstrates how to use Vault-backed credentials in actual code.
    """
    from airflow_modules.airflow_minio import AirflowMinioStorage

    print("=" * 60)
    print("MINIO CONNECTION TEST (via Vault)")
    print("=" * 60)

    # Initialize storage with Vault-backed connection
    storage = AirflowMinioStorage()
    storage.load_config_from_connection("minio_bronze")
    storage.init_client()

    # Test: list buckets
    print("Listing buckets...")
    buckets = storage.client.list_buckets()
    print(f"Found {len(buckets)} buckets:")
    for bucket in buckets:
        print(f"  - {bucket.name}")

    # Test: ensure bucket exists
    storage.ensure_bucket("bronze")

    print("=" * 60)
    print("MinIO connection test PASSED")
    print("=" * 60)

    return True


def test_postgres_connection():
    """
    Test PostgreSQL connection using Vault-backed credentials.
    """
    import psycopg2

    print("=" * 60)
    print("POSTGRESQL CONNECTION TEST (via Vault)")
    print("=" * 60)

    # Get connection from Vault
    conn_info = BaseHook.get_connection("pg_app")

    print(f"Connecting to {conn_info.host}:{conn_info.port}/{conn_info.schema}...")

    try:
        conn = psycopg2.connect(
            host=conn_info.host,
            port=conn_info.port,
            database=conn_info.schema,
            user=conn_info.login,
            password=conn_info.password,
        )

        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"Connected! PostgreSQL version: {version}")

        cursor.close()
        conn.close()

        print("=" * 60)
        print("PostgreSQL connection test PASSED")
        print("=" * 60)

        return True

    except Exception as e:
        print(f"Connection failed: {e}")
        raise


def test_neo4j_connection():
    """
    Test Neo4j connection using Vault-backed credentials.
    """
    from neo4j import GraphDatabase

    print("=" * 60)
    print("NEO4J CONNECTION TEST (via Vault)")
    print("=" * 60)

    # Get connection from Vault
    conn_info = BaseHook.get_connection("neo4j_default")

    uri = f"bolt://{conn_info.host}:{conn_info.port}"
    print(f"Connecting to {uri}...")

    try:
        driver = GraphDatabase.driver(
            uri,
            auth=(conn_info.login, conn_info.password)
        )

        with driver.session() as session:
            result = session.run("RETURN 1 as test")
            record = result.single()
            print(f"Query result: {record['test']}")

        driver.close()

        print("=" * 60)
        print("Neo4j connection test PASSED")
        print("=" * 60)

        return True

    except Exception as e:
        print(f"Connection failed: {e}")
        raise


# Task 1: Test all Vault connections
task_test_connections = PythonOperator(
    task_id='test_vault_connections',
    python_callable=test_vault_connections,
    dag=dag,
)

# Task 2: Test MinIO with Vault credentials
task_test_minio = PythonOperator(
    task_id='test_minio_connection',
    python_callable=test_minio_connection,
    dag=dag,
)

# Task 3: Test PostgreSQL with Vault credentials
task_test_postgres = PythonOperator(
    task_id='test_postgres_connection',
    python_callable=test_postgres_connection,
    dag=dag,
)

# Task 4: Test Neo4j with Vault credentials
task_test_neo4j = PythonOperator(
    task_id='test_neo4j_connection',
    python_callable=test_neo4j_connection,
    dag=dag,
)

# Define task dependencies
task_test_connections >> [task_test_minio, task_test_postgres, task_test_neo4j]
