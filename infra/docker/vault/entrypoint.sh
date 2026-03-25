#!/bin/sh
# Vault Entrypoint with Auto-Initialization
# Supports both dev mode and production mode

export VAULT_ADDR="http://127.0.0.1:8200"
KEYS_FILE="/vault/data/.vault-keys"
INITIALIZED_FLAG="/vault/data/.initialized"

echo "=== Vault Entrypoint ==="
echo "Mode: ${VAULT_MODE:-dev}"

# ============================================
# PRODUCTION MODE
# ============================================
if [ "${VAULT_MODE}" = "production" ] || [ "${VAULT_MODE}" = "prod" ]; then
  echo "Starting Vault in PRODUCTION mode..."

  # Start Vault server in background
  vault server -config=/vault/config/vault.hcl &
  VAULT_PID=$!

  # Wait for Vault to start responding
  echo "Waiting for Vault to start..."
  sleep 5
  for i in 1 2 3 4 5 6 7 8 9 10; do
    if vault status 2>&1 | grep -qE "(Initialized|Sealed)"; then
      echo "Vault is responding."
      break
    fi
    echo "Waiting... ($i)"
    sleep 2
  done

  # Check current status
  echo ""
  echo "=== Current Vault Status ==="
  vault status 2>&1 || true
  echo ""

  # Check if initialization is needed
  INIT_STATUS=$(vault status 2>&1 | grep "Initialized" | awk '{print $2}')
  echo "Initialized status: $INIT_STATUS"

  if [ "$INIT_STATUS" = "false" ]; then
    echo ""
    echo "=== First Time Setup: Initializing Vault ==="

    # Initialize Vault
    vault operator init -key-shares=1 -key-threshold=1 > /tmp/vault-init.txt 2>&1

    echo "Init output:"
    cat /tmp/vault-init.txt

    # Parse keys from output
    UNSEAL_KEY=$(grep "Unseal Key 1:" /tmp/vault-init.txt | awk '{print $4}')
    ROOT_TOKEN=$(grep "Initial Root Token:" /tmp/vault-init.txt | awk '{print $4}')

    echo ""
    echo "Parsed UNSEAL_KEY: $UNSEAL_KEY"
    echo "Parsed ROOT_TOKEN: $ROOT_TOKEN"

    if [ -n "$UNSEAL_KEY" ] && [ -n "$ROOT_TOKEN" ]; then
      # Save keys
      echo "VAULT_UNSEAL_KEY=${UNSEAL_KEY}" > ${KEYS_FILE}
      echo "VAULT_ROOT_TOKEN=${ROOT_TOKEN}" >> ${KEYS_FILE}
      chmod 600 ${KEYS_FILE}

      echo ""
      echo "=============================================="
      echo "  SAVE THESE CREDENTIALS SECURELY!"
      echo "=============================================="
      echo "VAULT_UNSEAL_KEY=${UNSEAL_KEY}"
      echo "VAULT_ROOT_TOKEN=${ROOT_TOKEN}"
      echo "=============================================="
    else
      echo "ERROR: Failed to parse initialization output"
    fi

    rm -f /tmp/vault-init.txt
  fi

  # Check if unseal is needed
  SEAL_STATUS=$(vault status 2>&1 | grep "Sealed" | awk '{print $2}')
  echo "Sealed status: $SEAL_STATUS"

  if [ "$SEAL_STATUS" = "true" ]; then
    echo "Unsealing Vault..."

    # Get unseal key (use sed to extract value after first =)
    if [ -f ${KEYS_FILE} ]; then
      UNSEAL_KEY=$(grep VAULT_UNSEAL_KEY ${KEYS_FILE} | sed 's/^VAULT_UNSEAL_KEY=//' | tr -d '[:space:]')
      echo "Using unseal key from file: ${UNSEAL_KEY}"
    fi

    if [ -n "$UNSEAL_KEY" ]; then
      vault operator unseal "$UNSEAL_KEY" || echo "Unseal command returned error"
      echo "Vault unsealed."
    else
      echo "ERROR: No unseal key available"
      exit 1
    fi
  fi

  # Set token for subsequent operations
  if [ -f ${KEYS_FILE} ]; then
    export VAULT_TOKEN=$(grep VAULT_ROOT_TOKEN ${KEYS_FILE} | sed 's/^VAULT_ROOT_TOKEN=//' | tr -d '[:space:]')
    echo "Token set from keys file."
  fi

  # Run initialization scripts on every startup
  # (both scripts are idempotent - safe to re-run)
  echo ""
  echo "=== Running initialization scripts ==="

  # Wait for Vault API to be fully ready after unseal
  echo "Waiting for Vault API to be fully ready..."
  sleep 3
  for i in 1 2 3 4 5; do
    if vault secrets list > /dev/null 2>&1; then
      echo "Vault API is ready."
      break
    fi
    echo "Vault API not ready yet... ($i)"
    sleep 2
  done

  if [ -f /vault/scripts/init-secrets.sh ]; then
    echo "Running init-secrets.sh..."
    sh /vault/scripts/init-secrets.sh || echo "WARNING: init-secrets.sh had errors"
  fi

  if [ -f /vault/scripts/setup-auth.sh ]; then
    echo "Running setup-auth.sh..."
    sh /vault/scripts/setup-auth.sh || echo "WARNING: setup-auth.sh had errors"
  fi

  echo "Initialization complete."

  echo ""
  echo "=== Vault Production Mode Ready ==="
  vault status

# ============================================
# DEV MODE (default)
# ============================================
else
  echo "Starting Vault in DEV mode..."

  # Start Vault dev server in background
  vault server -dev \
    -dev-root-token-id="${VAULT_DEV_ROOT_TOKEN_ID:-root-token-change-in-prod}" \
    -dev-listen-address="${VAULT_DEV_LISTEN_ADDRESS:-0.0.0.0:8200}" &
  VAULT_PID=$!

  # Set token for dev mode
  export VAULT_TOKEN="${VAULT_DEV_ROOT_TOKEN_ID:-root-token-change-in-prod}"

  # Wait for Vault
  echo "Waiting for Vault to start..."
  sleep 3
  for i in 1 2 3 4 5 6 7 8 9 10; do
    if vault status > /dev/null 2>&1; then
      echo "Vault is ready."
      break
    fi
    sleep 1
  done

  # Always run init scripts in dev mode (since it's in-memory)
  if [ -f /vault/scripts/init-secrets.sh ]; then
    echo "Running init-secrets.sh..."
    sh /vault/scripts/init-secrets.sh || echo "Warning: init-secrets.sh had errors"
  fi

  if [ -f /vault/scripts/setup-auth.sh ]; then
    echo "Running setup-auth.sh..."
    sh /vault/scripts/setup-auth.sh || echo "Warning: setup-auth.sh had errors"
  fi

  echo ""
  echo "=== Vault Dev Mode Ready ==="
fi

echo "Vault is running with PID $VAULT_PID"

# Wait for Vault process (keep container running)
wait $VAULT_PID
