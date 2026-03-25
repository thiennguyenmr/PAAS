#!/bin/bash
# fix-permissions.sh - Quick fix for PostgreSQL and Prometheus permission issues
# Use this when moving infrastructure to a new server

set -e

echo "==================================================================="
echo "     PostgreSQL & Prometheus Permission Fix"
echo "==================================================================="
echo ""
echo "This script will fix common issues when deploying to a new server:"
echo "  - PostgreSQL database corruption"
echo "  - Prometheus permission denied errors"
echo ""
echo "WARNING: This will delete existing PostgreSQL and Prometheus data!"
echo ""
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Fix cancelled."
    exit 1
fi

# Change to infra-repo directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/.."

echo ""
echo "==================================================================="
echo "Fixing PostgreSQL Issues"
echo "==================================================================="

echo ">> Stopping PostgreSQL containers..."
docker compose down app-postgres-db airflow-postgres

echo ">> Cleaning app-postgres data directory..."
docker run --rm -v $(pwd)/mnt/data_drive/postgres_data:/data alpine sh -c "rm -rf /data/*"
echo "   ✓ Cleaned: mnt/data_drive/postgres_data"

echo ">> Cleaning airflow-postgres data directory..."
docker run --rm -v $(pwd)/mnt/data_drive/airflow_data/airflow_db:/data alpine sh -c "rm -rf /data/*"
echo "   ✓ Cleaned: mnt/data_drive/airflow_data/airflow_db"

echo ">> Starting PostgreSQL containers..."
docker compose up -d app-postgres-db airflow-postgres

echo ">> Waiting for PostgreSQL to initialize (15s)..."
sleep 15

echo ">> Checking PostgreSQL status..."
docker compose ps app-postgres-db airflow-postgres

echo ""
echo "==================================================================="
echo "Fixing Prometheus Issues"
echo "==================================================================="

echo ">> Stopping Prometheus..."
docker compose down prometheus

echo ">> Cleaning and setting correct permissions (UID 65534)..."
docker run --rm -v $(pwd)/mnt/data_drive/prometheus_data:/data alpine sh -c "rm -rf /data/* && chown -R 65534:65534 /data"
echo "   ✓ Cleaned and set permissions: mnt/data_drive/prometheus_data"

echo ">> Starting Prometheus..."
docker compose up -d prometheus

echo ">> Waiting for Prometheus to start (10s)..."
sleep 10

echo ">> Checking Prometheus status..."
docker compose ps prometheus

echo ""
echo "==================================================================="
echo "Checking Service Logs"
echo "==================================================================="

echo ""
echo ">> App PostgreSQL logs:"
docker compose logs app-postgres-db --tail 10

echo ""
echo ">> Airflow PostgreSQL logs:"
docker compose logs airflow-postgres --tail 10

echo ""
echo ">> Prometheus logs:"
docker compose logs prometheus --tail 10

echo ""
echo "==================================================================="
echo "          Fix Complete!"
echo "==================================================================="
echo ""
echo "Service Status:"
docker compose ps app-postgres-db airflow-postgres prometheus

echo ""
echo "If services are healthy, you can now start dependent services:"
echo "  docker compose up -d airflow-webserver airflow-scheduler airflow-dag-processor"
echo "  docker compose up -d grafana"
echo ""
echo "To view full logs:"
echo "  docker compose logs -f <service-name>"
echo ""
