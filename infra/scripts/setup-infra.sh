#!/bin/bash
# setup-infra.sh - Complete infrastructure setup script for new servers
# This script cleans and initializes all data directories with correct permissions

set -e

echo "==================================================================="
echo "          Infrastructure Setup Script"
echo "==================================================================="
echo ""
echo "This script will:"
echo "  1. Stop all running containers"
echo "  2. Clean all data directories"
echo "  3. Set correct permissions for each service"
echo "  4. Build base images (Spark, Airflow)"
echo "  5. Start all services in the correct order"
echo ""
echo "WARNING: This will delete ALL existing data!"
echo ""
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Setup cancelled."
    exit 1
fi

# Change to infra-repo directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/.."

echo ""
echo "==================================================================="
echo "Step 1: Stopping all services"
echo "==================================================================="
docker compose down

echo ""
echo "==================================================================="
echo "Step 2: Cleaning data directories"
echo "==================================================================="

echo ">> Cleaning PostgreSQL directories..."
docker run --rm -v $(pwd)/mnt/data_drive/postgres_data:/data alpine sh -c "rm -rf /data/* && mkdir -p /data"
docker run --rm -v $(pwd)/mnt/data_drive/airflow_data/airflow_db:/data alpine sh -c "rm -rf /data/* && mkdir -p /data"

echo ">> Cleaning Prometheus directory (UID 65534)..."
docker run --rm -v $(pwd)/mnt/data_drive/prometheus_data:/data alpine sh -c "rm -rf /data/* && mkdir -p /data && chown -R 65534:65534 /data"

echo ">> Cleaning Grafana directory (UID 472)..."
docker run --rm -v $(pwd)/mnt/data_drive/grafana_data:/data alpine sh -c "rm -rf /data/* && mkdir -p /data && chown -R 472:472 /data"

echo ">> Cleaning MinIO directory..."
docker run --rm -v $(pwd)/mnt/data_drive/minio_data:/data alpine sh -c "rm -rf /data/* && mkdir -p /data"

echo ">> Cleaning Neo4j directories..."
docker run --rm -v $(pwd)/mnt/data_drive/neo4j_data:/data alpine sh -c "rm -rf /data/* && mkdir -p /data/data /data/logs /data/import /data/plugins"

echo ">> Cleaning Spark directories..."
docker run --rm -v $(pwd)/mnt/data_drive/spark_data:/data alpine sh -c "rm -rf /data/* && mkdir -p /data/spark_events"

echo ">> Cleaning Milvus directory..."
docker run --rm -v $(pwd)/mnt/data_drive/milvus_data:/data alpine sh -c "rm -rf /data/* && mkdir -p /data"

echo ">> Cleaning Etcd directory..."
docker run --rm -v $(pwd)/mnt/data_drive/etcd_data:/data alpine sh -c "rm -rf /data/* && mkdir -p /data"

echo ""
echo "==================================================================="
echo "Step 3: Building base images"
echo "==================================================================="

echo ">> Building spark-base..."
docker compose build spark-base

echo ">> Building airflow-base..."
docker compose build airflow-base

echo ""
echo ">> Verifying images are built..."
docker images | grep -E "chatbot-spark|chatbot-airflow" || echo "Warning: Base images not found"

echo ""
echo "==================================================================="
echo "Step 4: Starting services in correct order"
echo "==================================================================="

# Stage 1: Storage services
echo ""
echo ">> Stage 1: Starting storage services..."
echo "   - MinIO (S3)"
echo "   - App PostgreSQL"
echo "   - Etcd (for Milvus)"
docker compose up -d minio app-postgres-db etcd

echo ">> Waiting for storage services to be healthy (30s)..."
sleep 30

# Stage 2: Vector and Graph databases
echo ""
echo ">> Stage 2: Starting vector and graph databases..."
echo "   - Milvus (vector DB)"
echo "   - Neo4j (graph DB)"
docker compose up -d milvus neo4j

echo ">> Waiting for databases to initialize (30s)..."
sleep 30

# Stage 3: Spark cluster
echo ""
echo ">> Stage 3: Starting Spark cluster..."
echo "   - Spark Master"
docker compose up -d spark-master

echo ">> Waiting for Spark Master to be healthy (15s)..."
sleep 15

echo "   - Spark Workers"
docker compose up -d spark-worker-1 spark-worker-2

echo "   - Spark History Server"
docker compose up -d spark-history

echo ">> Waiting for Spark cluster to stabilize (10s)..."
sleep 10

# Stage 4: Airflow
echo ""
echo ">> Stage 4: Starting Airflow..."
echo "   - Airflow PostgreSQL"
docker compose up -d airflow-postgres

echo ">> Waiting for Airflow DB to be healthy (15s)..."
sleep 15

echo "   - Airflow services"
docker compose up -d airflow-webserver airflow-scheduler airflow-dag-processor

echo ">> Waiting for Airflow to initialize (20s)..."
sleep 20

# Stage 5: Monitoring
echo ""
echo ">> Stage 5: Starting monitoring services..."
echo "   - cAdvisor"
docker compose up -d cadvisor

echo ">> Waiting for cAdvisor (5s)..."
sleep 5

echo "   - Prometheus"
docker compose up -d prometheus

echo ">> Waiting for Prometheus (10s)..."
sleep 10

echo "   - Grafana Renderer"
docker compose up -d renderer

echo "   - Grafana"
docker compose up -d grafana

echo ">> Waiting for monitoring stack to stabilize (10s)..."
sleep 10

echo ""
echo "==================================================================="
echo "          Setup Complete!"
echo "==================================================================="
echo ""
echo "Checking service status..."
docker compose ps

echo ""
echo "==================================================================="
echo "          Service Access URLs"
echo "==================================================================="
echo ""
echo "Storage:"
echo "  MinIO Console:     http://localhost:6001"
echo ""
echo "Databases:"
echo "  App PostgreSQL:    localhost:6003"
echo "  Airflow PostgreSQL: localhost:6030"
echo "  Neo4j Browser:     http://localhost:6007"
echo "  Milvus:            localhost:6005"
echo ""
echo "Processing:"
echo "  Spark Master:      http://localhost:6020"
echo "  Spark History:     http://localhost:6022"
echo ""
echo "Orchestration:"
echo "  Airflow:           http://localhost:6031"
echo ""
echo "Monitoring:"
echo "  cAdvisor:          http://localhost:6050"
echo "  Prometheus:        http://localhost:6051"
echo "  Grafana:           http://localhost:6052"
echo ""
echo "==================================================================="
echo ""
echo "Useful commands:"
echo "  - View all services:        docker compose ps"
echo "  - View logs:                docker compose logs -f <service>"
echo "  - Restart service:          docker compose restart <service>"
echo "  - Stop all:                 docker compose down"
echo ""
echo "For troubleshooting, see: TROUBLESHOOTING.md"
echo ""
