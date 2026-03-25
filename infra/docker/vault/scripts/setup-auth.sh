#!/bin/sh
# Vault AppRole Authentication Setup
# Configures AppRole auth method for Airflow and other services
# Idempotent: safe to run on every startup
#
# If VAULT_AIRFLOW_SECRET_ID (etc.) env vars are set, registers them as
# custom secret IDs so the same credentials work across Vault restarts.
# If not set, generates new credentials and saves them to a file.

set -e

export VAULT_ADDR="http://127.0.0.1:8200"
if [ -z "$VAULT_TOKEN" ]; then
  export VAULT_TOKEN="${VAULT_DEV_ROOT_TOKEN_ID:-root-token-change-in-prod}"
fi

APPROLE_KEYS_FILE="/vault/data/.approle-keys"

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

# -------------------------------------------------------
# Register AppRole credentials
# If env vars are set (from .env), register them as custom
# secret IDs so the same values always work.
# Otherwise, generate new ones and save to file.
# -------------------------------------------------------
echo ""

# --- Airflow ---
AIRFLOW_ROLE_ID=$(vault read -field=role_id auth/approle/role/airflow/role-id)

if [ -n "$VAULT_AIRFLOW_SECRET_ID" ]; then
  echo "Registering Airflow custom secret_id from env..."
  vault write auth/approle/role/airflow/custom-secret-id \
    secret_id="$VAULT_AIRFLOW_SECRET_ID" > /dev/null 2>&1 || echo "  (already registered)"
  AIRFLOW_SECRET_ID="$VAULT_AIRFLOW_SECRET_ID"
else
  echo "Generating new Airflow secret_id..."
  AIRFLOW_SECRET_ID=$(vault write -field=secret_id -force auth/approle/role/airflow/secret-id)
fi

# --- Grafana ---
GRAFANA_ROLE_ID=$(vault read -field=role_id auth/approle/role/grafana/role-id)

if [ -n "$VAULT_GRAFANA_SECRET_ID" ]; then
  echo "Registering Grafana custom secret_id from env..."
  vault write auth/approle/role/grafana/custom-secret-id \
    secret_id="$VAULT_GRAFANA_SECRET_ID" > /dev/null 2>&1 || echo "  (already registered)"
  GRAFANA_SECRET_ID="$VAULT_GRAFANA_SECRET_ID"
else
  echo "Generating new Grafana secret_id..."
  GRAFANA_SECRET_ID=$(vault write -field=secret_id -force auth/approle/role/grafana/secret-id)
fi

# Save to persistent file
cat > "$APPROLE_KEYS_FILE" <<KEYS
VAULT_AIRFLOW_ROLE_ID=${AIRFLOW_ROLE_ID}
VAULT_AIRFLOW_SECRET_ID=${AIRFLOW_SECRET_ID}
VAULT_GRAFANA_ROLE_ID=${GRAFANA_ROLE_ID}
VAULT_GRAFANA_SECRET_ID=${GRAFANA_SECRET_ID}
KEYS
chmod 600 "$APPROLE_KEYS_FILE"

echo ""
echo "AppRole credentials:"
echo "  VAULT_AIRFLOW_ROLE_ID=${AIRFLOW_ROLE_ID}"
echo "  VAULT_AIRFLOW_SECRET_ID=${AIRFLOW_SECRET_ID}"
echo "  VAULT_GRAFANA_ROLE_ID=${GRAFANA_ROLE_ID}"
echo "  VAULT_GRAFANA_SECRET_ID=${GRAFANA_SECRET_ID}"

# Store in Vault for reference
vault kv put secret/auth/airflow-approle \
  role_id="${AIRFLOW_ROLE_ID}" \
  secret_id="${AIRFLOW_SECRET_ID}" > /dev/null

vault kv put secret/auth/grafana-approle \
  role_id="${GRAFANA_ROLE_ID}" \
  secret_id="${GRAFANA_SECRET_ID}" > /dev/null

echo ""
echo "=== AppRole setup complete! ==="
