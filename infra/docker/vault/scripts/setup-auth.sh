#!/bin/bash
# Vault AppRole Authentication Setup
# Configures AppRole auth method for Airflow and other services

set -e

export VAULT_ADDR="http://127.0.0.1:8200"
export VAULT_TOKEN="${VAULT_DEV_ROOT_TOKEN:-root-token}"

echo "=== Vault AppRole Authentication Setup ==="

# Enable AppRole auth method
echo "Enabling AppRole authentication..."
vault auth enable approle 2>/dev/null || echo "AppRole already enabled"

# Create and apply Airflow policy
echo "Creating Airflow policy..."
vault policy write airflow-policy /vault/policies/airflow-policy.hcl

# Create Airflow AppRole
echo "Creating Airflow AppRole..."
vault write auth/approle/role/airflow \
  token_policies="airflow-policy" \
  token_ttl=1h \
  token_max_ttl=4h \
  secret_id_ttl=0 \
  secret_id_num_uses=0

# Get Role ID and Secret ID for Airflow
echo ""
echo "=== Airflow AppRole Credentials ==="
ROLE_ID=$(vault read -field=role_id auth/approle/role/airflow/role-id)
SECRET_ID=$(vault write -field=secret_id -force auth/approle/role/airflow/secret-id)

echo "VAULT_ROLE_ID=${ROLE_ID}"
echo "VAULT_SECRET_ID=${SECRET_ID}"

# Store these values in Vault for reference
vault kv put secret/auth/airflow-approle \
  role_id="${ROLE_ID}" \
  secret_id="${SECRET_ID}"

echo ""
echo "=== Grafana AppRole Setup ==="

# Create Grafana policy (read-only for monitoring secrets)
vault policy write grafana-policy - <<EOF
path "secret/data/system/*" {
  capabilities = ["read"]
}
path "secret/data/api_keys/*" {
  capabilities = ["read"]
}
EOF

# Create Grafana AppRole
vault write auth/approle/role/grafana \
  token_policies="grafana-policy" \
  token_ttl=1h \
  token_max_ttl=4h \
  secret_id_ttl=0 \
  secret_id_num_uses=0

GRAFANA_ROLE_ID=$(vault read -field=role_id auth/approle/role/grafana/role-id)
GRAFANA_SECRET_ID=$(vault write -field=secret_id -force auth/approle/role/grafana/secret-id)

echo "VAULT_GRAFANA_ROLE_ID=${GRAFANA_ROLE_ID}"
echo "VAULT_GRAFANA_SECRET_ID=${GRAFANA_SECRET_ID}"

vault kv put secret/auth/grafana-approle \
  role_id="${GRAFANA_ROLE_ID}" \
  secret_id="${GRAFANA_SECRET_ID}"

echo ""
echo "=== AppRole setup complete! ==="
echo ""
echo "For production, update your .env with these values and switch from token auth to AppRole."
