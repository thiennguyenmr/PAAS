#!/bin/sh
# Vault Secrets Initialization Script
# This script stores all system and Airflow connection credentials in Vault

set -e

export VAULT_ADDR="http://127.0.0.1:8200"
# Use existing VAULT_TOKEN if set (production mode), otherwise use dev token
if [ -z "$VAULT_TOKEN" ]; then
  export VAULT_TOKEN="${VAULT_DEV_ROOT_TOKEN_ID:-root-token-change-in-prod}"
fi

echo "=== Vault Secrets Initialization ==="
echo "Waiting for Vault to be ready..."
until vault status > /dev/null 2>&1; do
  sleep 2
done
echo "Vault is ready."

# Enable KV-v2 secrets engine
echo "Enabling KV-v2 secrets engine..."
vault secrets enable -path=secret kv-v2 2>/dev/null || echo "KV engine already enabled"

# ==========================================
# SYSTEM CREDENTIALS
# ==========================================
echo ""
echo "=== Storing System Credentials ==="

echo "  -> MinIO credentials"
vault kv put secret/system/minio \
  root_user="${MINIO_ROOT_USER:-minioadmin}" \
  root_password="${MINIO_ROOT_PASSWORD:-minioadmin}" \
  endpoint="minio:9000"

echo "  -> PostgreSQL App credentials"
vault kv put secret/system/postgres_app \
  user="${POSTGRES_APP_USER:-app_admin}" \
  password="${POSTGRES_APP_PASSWORD:-zyneric}" \
  host="app-postgres-db" \
  port="5432" \
  database="${POSTGRES_APP_DB:-app_data}"

echo "  -> PostgreSQL Airflow credentials"
vault kv put secret/system/postgres_airflow \
  user="${AIRFLOW_POSTGRES_USER:-airflow}" \
  password="${AIRFLOW_POSTGRES_PASSWORD:-airflow}" \
  host="airflow-postgres" \
  port="5432" \
  database="${AIRFLOW_POSTGRES_DB:-airflow}"

echo "  -> Neo4j credentials"
vault kv put secret/system/neo4j \
  user="neo4j" \
  password="${NEO4J_PASSWORD:-graph_secret_password}" \
  uri="bolt://neo4j:7687"

echo "  -> Airflow admin credentials"
vault kv put secret/system/airflow \
  admin_username="${AIRFLOW_ADMIN_USERNAME:-admin}" \
  admin_password="${AIRFLOW_ADMIN_PASSWORD:-admin123}" \
  admin_email="${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" \
  fernet_key="${AIRFLOW__CORE__FERNET_KEY}" \
  secret_key="${AIRFLOW__API__SECRET_KEY}"

# ==========================================
# AIRFLOW CONNECTIONS (Vault Backend Format)
# ==========================================
echo ""
echo "=== Storing Airflow Connections ==="

echo "  -> minio_bronze"
vault kv put secret/airflow/connections/minio_bronze \
  conn_type="generic" \
  host="minio" \
  port="9000" \
  login="${MINIO_ROOT_USER:-minioadmin}" \
  password="${MINIO_ROOT_PASSWORD:-minioadmin}" \
  schema="bronze" \
  extra='{"ssl_enabled": false}'

echo "  -> minio_silver"
vault kv put secret/airflow/connections/minio_silver \
  conn_type="generic" \
  host="minio" \
  port="9000" \
  login="${MINIO_ROOT_USER:-minioadmin}" \
  password="${MINIO_ROOT_PASSWORD:-minioadmin}" \
  schema="silver" \
  extra='{"ssl_enabled": false}'

echo "  -> minio_external"
vault kv put secret/airflow/connections/minio_external \
  conn_type="generic" \
  host="${MINIO_EXTERNAL_HOST:-10.30.0.11}" \
  port="${MINIO_EXTERNAL_PORT:-6000}" \
  login="${MINIO_ROOT_USER:-minioadmin}" \
  password="${MINIO_ROOT_PASSWORD:-minioadmin}" \
  schema="datalake" \
  extra='{"ssl_enabled": false}'

echo "  -> pg_app"
vault kv put secret/airflow/connections/pg_app \
  conn_type="postgres" \
  host="app-postgres-db" \
  port="5432" \
  login="${POSTGRES_APP_USER:-app_admin}" \
  password="${POSTGRES_APP_PASSWORD:-zyneric}" \
  schema="${POSTGRES_APP_DB:-app_data}"

echo "  -> neo4j_default"
vault kv put secret/airflow/connections/neo4j_default \
  conn_type="generic" \
  host="neo4j" \
  port="7687" \
  login="neo4j" \
  password="${NEO4J_PASSWORD:-graph_secret_password}" \
  extra='{"protocol": "bolt"}'

echo "  -> milvus_default"
vault kv put secret/airflow/connections/milvus_default \
  conn_type="generic" \
  host="milvus" \
  port="19530"

echo "  -> spark_default"
vault kv put secret/airflow/connections/spark_default \
  conn_type="spark" \
  host="spark://spark-master" \
  port="7077"

# ==========================================
# MLFLOW
# ==========================================
echo ""
echo "=== Storing MLflow Credentials ==="

echo "  -> MLflow PostgreSQL credentials"
vault kv put secret/system/mlflow_postgres \
  user="${MLFLOW_POSTGRES_USER:-mlflow}" \
  password="${MLFLOW_POSTGRES_PASSWORD:-mlflow}" \
  host="mlflow-postgres" \
  port="5432" \
  database="${MLFLOW_POSTGRES_DB:-mlflow}"

echo "  -> mlflow_tracking (Airflow connection)"
vault kv put secret/airflow/connections/mlflow_tracking \
  conn_type="http" \
  host="mlflow-server" \
  port="5000" \
  extra='{"endpoint": "api"}'

# ==========================================
# API KEYS
# ==========================================
echo ""
echo "=== Storing API Keys ==="

echo "  -> Brevo SMTP"
vault kv put secret/api_keys/brevo_smtp \
  login="${BREVO_SMTP_LOGIN}" \
  key="${BREVO_SMTP_KEY}" \
  from_email="${ALERT_EMAIL_FROM}" \
  to_email="${ALERT_EMAIL_TO}"

echo ""
echo "=== Vault secrets initialization complete! ==="
echo ""
echo "To verify, run:"
echo "  vault kv list secret/airflow/connections"
echo "  vault kv get secret/system/minio"
