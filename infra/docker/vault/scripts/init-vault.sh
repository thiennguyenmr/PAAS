#!/bin/sh
# Vault Initialization and Unseal Script (Production Mode)
# This script initializes Vault on first run and unseals it on subsequent runs

set -e

export VAULT_ADDR="http://127.0.0.1:8200"
KEYS_FILE="/vault/data/.vault-keys"

echo "=== Vault Production Mode Initialization ==="

# Wait for Vault to be ready
echo "Waiting for Vault to start..."
until vault status 2>&1 | grep -q "Sealed"; do
  sleep 1
done
echo "Vault is running."

# Check if Vault is already initialized
if vault status 2>&1 | grep -q "Initialized.*false"; then
  echo ""
  echo "=== First Time Setup: Initializing Vault ==="

  # Initialize Vault with 1 key share and 1 threshold (simple setup)
  # For production, use 5 shares with 3 threshold
  vault operator init -key-shares=1 -key-threshold=1 -format=json > /tmp/vault-init.json

  # Extract keys
  UNSEAL_KEY=$(cat /tmp/vault-init.json | grep -o '"unseal_keys_b64":\s*\[[^]]*\]' | grep -o '"[^"]*"' | head -1 | tr -d '"')
  ROOT_TOKEN=$(cat /tmp/vault-init.json | grep -o '"root_token":\s*"[^"]*"' | cut -d'"' -f4)

  # Save keys securely (in production, store these in a secure location!)
  echo "VAULT_UNSEAL_KEY=${UNSEAL_KEY}" > ${KEYS_FILE}
  echo "VAULT_ROOT_TOKEN=${ROOT_TOKEN}" >> ${KEYS_FILE}
  chmod 600 ${KEYS_FILE}

  echo ""
  echo "=============================================="
  echo "  IMPORTANT: Save these credentials securely!"
  echo "=============================================="
  echo "VAULT_UNSEAL_KEY=${UNSEAL_KEY}"
  echo "VAULT_ROOT_TOKEN=${ROOT_TOKEN}"
  echo "=============================================="
  echo ""
  echo "Keys saved to: ${KEYS_FILE}"

  # Clean up temp file
  rm -f /tmp/vault-init.json

else
  echo "Vault is already initialized."
fi

# Unseal Vault
if vault status 2>&1 | grep -q "Sealed.*true"; then
  echo ""
  echo "=== Unsealing Vault ==="

  if [ -f ${KEYS_FILE} ]; then
    # Read unseal key from file
    UNSEAL_KEY=$(grep VAULT_UNSEAL_KEY ${KEYS_FILE} | cut -d'=' -f2)
    vault operator unseal ${UNSEAL_KEY}
    echo "Vault unsealed successfully."
  else
    echo "ERROR: No unseal key found at ${KEYS_FILE}"
    echo "Please provide VAULT_UNSEAL_KEY environment variable or run init first."
    exit 1
  fi
else
  echo "Vault is already unsealed."
fi

# Export root token for subsequent scripts
if [ -f ${KEYS_FILE} ]; then
  export VAULT_TOKEN=$(grep VAULT_ROOT_TOKEN ${KEYS_FILE} | cut -d'=' -f2)
fi

echo ""
echo "=== Vault Status ==="
vault status

echo ""
echo "=== Vault initialization complete ==="
