#!/bin/bash

# SSH Tunneling Script for LLM Ops Stack
# Maps remote ports to localhost for browser/client access

# --- Server Configuration ---
SERVER_USER="felix"
SERVER_HOST="185.23.199.196"
SERVER_PORT="22311"
SERVER_PASS=""  # Leave empty to use SSH keys or manual password prompt

# --- Port Mappings (LocalPort:RemotePort:ServiceName:Hostname:Protocol) ---
TUNNELS=(
    # Storage
    "6000:6000:MinIO API:minio:http"
    "6001:6001:MinIO Console:minio:http"
    "6003:6003:App Postgres:postgres:postgresql"
    "6005:6005:Milvus gRPC:milvus:grpc"
    "6006:6006:Milvus Health:milvus:http"
    "6007:6007:Neo4j Browser:neo4j:http"
    "6008:6008:Neo4j Bolt:neo4j:bolt"
    # Processing
    "6020:6020:Spark Master:spark:http"
    "6022:6022:Spark History:spark:http"
    "6030:6030:Airflow Postgres:postgres:postgresql"
    "6031:6031:Airflow:airflow:http"
    # ML Tracking
    "6035:6035:MLflow:mlflow:http"
    "6036:6036:MLflow SSO:mlflow:http"
    # Monitoring
    "6051:6051:Prometheus:prometheus:http"
    "6052:6052:Grafana:grafana:http"
    # Security
    "6060:6060:Vault:vault:http"
    "6061:6061:Keycloak:keycloak:http"
)

# --- Check sshpass (only if password is set) ---
check_sshpass() {
    if ! command -v sshpass >/dev/null 2>&1; then
        echo "sshpass not found. Install it:"
        echo "  macOS:  brew install hudochenkov/sshpass/sshpass"
        echo "  Ubuntu: sudo apt install sshpass"
        exit 1
    fi
}

# --- Extract unique hostnames for /etc/hosts ---
HOSTNAMES=$(printf '%s\n' "${TUNNELS[@]}" | cut -d':' -f4 | sort -u | tr '\n' ' ')

# --- Setup /etc/hosts if needed ---
setup_hosts() {
    local missing=""
    for host in $HOSTNAMES; do
        if ! grep -q "127.0.0.1.*\b${host}\b" /etc/hosts 2>/dev/null; then
            missing="$missing $host"
        fi
    done

    if [[ -n "$missing" ]]; then
        echo "Missing /etc/hosts entries:$missing"
        read -rp "Add them now? (y/n) " response
        if [[ "$response" =~ ^[yY]$ ]]; then
            echo "127.0.0.1 $HOSTNAMES" | sudo tee -a /etc/hosts
            echo "Added successfully."
        fi
        echo ""
    fi
}

# --- Build SSH Args ---
SSH_ARGS="-N -o StrictHostKeyChecking=no -p $SERVER_PORT"
for tunnel in "${TUNNELS[@]}"; do
    IFS=':' read -r local_port remote_port name host proto <<< "$tunnel"
    SSH_ARGS="$SSH_ARGS -L $local_port:localhost:$remote_port"
done

# --- Main ---
setup_hosts

echo "========================================"
echo "  SSH Tunnel -> $SERVER_HOST:$SERVER_PORT"
echo "========================================"
echo ""
printf "%-18s %-30s\n" "SERVICE" "URL"
echo "----------------------------------------"
for tunnel in "${TUNNELS[@]}"; do
    IFS=':' read -r local_port remote_port name host proto <<< "$tunnel"
    printf "%-18s %s://%s:%s\n" "$name" "$proto" "$host" "$local_port"
done
echo "----------------------------------------"
echo ""
echo "Press Ctrl+C to close tunnels."
echo ""

# --- Execute (with or without password) ---
if [[ -n "$SERVER_PASS" ]]; then
    check_sshpass
    exec sshpass -p "$SERVER_PASS" ssh $SSH_ARGS "$SERVER_USER@$SERVER_HOST"
else
    exec ssh $SSH_ARGS "$SERVER_USER@$SERVER_HOST"
fi
