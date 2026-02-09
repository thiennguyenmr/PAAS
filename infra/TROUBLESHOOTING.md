# Infrastructure Troubleshooting Guide

## Common Issues When Deploying to a New Server

This guide covers how to fix PostgreSQL and Prometheus permission/corruption issues when running `docker compose` on a new server.

---

## Issue 1: PostgreSQL Database Corruption

### Symptoms
```
FATAL: could not open directory "pg_notify": No such file or directory
LOG: database system is shut down
```

### Affected Services
- `app-postgres-db` (App PostgreSQL)
- `airflow-postgres` (Airflow PostgreSQL)

### Root Cause
- Corrupted or incomplete database directory from previous installations
- Improper shutdown or migration of data directories
- Permission issues from different server environments

### Solution

#### Quick Fix (Clean Start - Recommended for Development)
```bash
cd /srv/chatbot/shared/chatbot/infra-repo

# Stop affected containers
docker compose down app-postgres-db airflow-postgres

# Clean corrupted directories using Docker (no sudo needed)
docker run --rm -v $(pwd)/mnt/data_drive/postgres_data:/data alpine sh -c "rm -rf /data/*"
docker run --rm -v $(pwd)/mnt/data_drive/airflow_data/airflow_db:/data alpine sh -c "rm -rf /data/*"

# Restart containers - they will reinitialize
docker compose up -d app-postgres-db airflow-postgres

# Verify they're healthy
docker compose ps app-postgres-db airflow-postgres
docker compose logs -f app-postgres-db airflow-postgres
```

#### With Backup (Production)
```bash
cd /srv/chatbot/shared/chatbot/infra-repo

# Backup corrupted directories first
BACKUP_DATE=$(date +%Y%m%d_%H%M%S)
sudo cp -r ./mnt/data_drive/postgres_data ./mnt/data_drive/postgres_data.backup.$BACKUP_DATE
sudo cp -r ./mnt/data_drive/airflow_data/airflow_db ./mnt/data_drive/airflow_data/airflow_db.backup.$BACKUP_DATE

# Then proceed with cleanup
docker compose down app-postgres-db airflow-postgres
docker run --rm -v $(pwd)/mnt/data_drive/postgres_data:/data alpine sh -c "rm -rf /data/*"
docker run --rm -v $(pwd)/mnt/data_drive/airflow_data/airflow_db:/data alpine sh -c "rm -rf /data/*"
docker compose up -d app-postgres-db airflow-postgres
```

---

## Issue 2: Prometheus Permission Denied

### Symptoms
```
Error opening query log file: permission denied
panic: Unable to create mmap-ed active query log
```

### Affected Services
- `prometheus`

### Root Cause
- Prometheus runs as user `nobody` (UID 65534)
- Data directory has incorrect ownership from previous server or manual creation
- File permissions not set correctly during volume mount

### Solution

#### Quick Fix
```bash
cd /srv/chatbot/shared/chatbot/infra-repo

# Stop Prometheus
docker compose down prometheus

# Fix permissions using Docker (no sudo needed)
docker run --rm -v $(pwd)/mnt/data_drive/prometheus_data:/data alpine sh -c "chown -R 65534:65534 /data"

# Restart Prometheus
docker compose up -d prometheus

# Verify it's healthy
docker compose ps prometheus
docker compose logs -f prometheus
```

#### Clean Start (if you don't need existing metrics)
```bash
cd /srv/chatbot/shared/chatbot/infra-repo

# Stop Prometheus
docker compose down prometheus

# Clean and set correct permissions
docker run --rm -v $(pwd)/mnt/data_drive/prometheus_data:/data alpine sh -c "rm -rf /data/* && chown -R 65534:65534 /data"

# Restart Prometheus
docker compose up -d prometheus
```

---

## Complete Server Setup Script

Use this script when setting up on a **brand new server**:

```bash
#!/bin/bash
# setup-infra.sh - Complete infrastructure setup script

set -e

echo "=== Infrastructure Setup Script ==="
echo "This will clean and initialize all data directories"
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
fi

cd /srv/chatbot/shared/chatbot/infra-repo

echo ">> Stopping all services..."
docker compose down

echo ">> Cleaning PostgreSQL directories..."
docker run --rm -v $(pwd)/mnt/data_drive/postgres_data:/data alpine sh -c "rm -rf /data/* && mkdir -p /data"
docker run --rm -v $(pwd)/mnt/data_drive/airflow_data/airflow_db:/data alpine sh -c "rm -rf /data/* && mkdir -p /data"

echo ">> Cleaning and setting Prometheus permissions..."
docker run --rm -v $(pwd)/mnt/data_drive/prometheus_data:/data alpine sh -c "rm -rf /data/* && mkdir -p /data && chown -R 65534:65534 /data"

echo ">> Cleaning Grafana directory..."
docker run --rm -v $(pwd)/mnt/data_drive/grafana_data:/data alpine sh -c "rm -rf /data/* && mkdir -p /data && chown -R 472:472 /data"

echo ">> Building base images..."
docker compose build spark-base airflow-base

echo ">> Verifying images are built..."
docker images | grep -E "chatbot-spark|chatbot-airflow"

echo ">> Starting infrastructure services in order..."

# 1. Start storage services
echo ">> Starting storage services (MinIO, PostgreSQL, Etcd)..."
docker compose up -d minio app-postgres-db etcd

# 2. Wait for storage to be ready
echo ">> Waiting for storage services to be healthy..."
sleep 10

# 3. Start vector and graph databases
echo ">> Starting Milvus and Neo4j..."
docker compose up -d milvus neo4j

# 4. Start Spark cluster
echo ">> Starting Spark cluster..."
docker compose up -d spark-master
sleep 15
docker compose up -d spark-worker-1 spark-worker-2 spark-history

# 5. Start Airflow
echo ">> Starting Airflow services..."
docker compose up -d airflow-postgres
sleep 10
docker compose up -d airflow-webserver airflow-scheduler airflow-dag-processor

# 6. Start monitoring
echo ">> Starting monitoring services..."
docker compose up -d cadvisor
sleep 5
docker compose up -d prometheus
sleep 5
docker compose up -d renderer grafana

echo ""
echo "=== Setup Complete ==="
echo "Checking service status..."
docker compose ps

echo ""
echo "=== Service URLs ==="
echo "MinIO:           http://localhost:9001"
echo "Neo4j:           http://localhost:7474"
echo "Spark Master:    http://localhost:8080"
echo "Spark History:   http://localhost:18080"
echo "Airflow:         http://localhost:8081"
echo "Prometheus:      http://localhost:9090"
echo "Grafana:         http://localhost:6052"
echo ""
echo "Run 'docker compose logs -f <service>' to view logs"
```

---

## Prevention Tips

### 1. Use Version Control for Data Directories
Add to `.gitignore`:
```
mnt/data_drive/*/
!mnt/data_drive/.gitkeep
```

### 2. Create Proper Directory Structure
```bash
# Create all required directories with correct ownership
mkdir -p mnt/data_drive/{postgres_data,airflow_data/airflow_db,prometheus_data,grafana_data,minio_data,neo4j_data,spark_data,milvus_data,etcd_data}

# Set Prometheus permissions
docker run --rm -v $(pwd)/mnt/data_drive/prometheus_data:/data alpine sh -c "chown -R 65534:65534 /data"

# Set Grafana permissions
docker run --rm -v $(pwd)/mnt/data_drive/grafana_data:/data alpine sh -c "chown -R 472:472 /data"
```

### 3. Health Check Before Deployment
```bash
# Check all services health status
docker compose ps

# Check specific service logs
docker compose logs <service-name> --tail 50

# Check all unhealthy services
docker compose ps | grep -E "unhealthy|Restarting"
```

### 4. Regular Cleanup Script
Create `scripts/cleanup-data.sh`:
```bash
#!/bin/bash
# Cleanup all data directories for fresh start

cd /srv/chatbot/shared/chatbot/infra-repo

echo "WARNING: This will delete all data!"
read -p "Are you sure? (type 'yes' to confirm): " confirm

if [ "$confirm" != "yes" ]; then
    echo "Cancelled."
    exit 1
fi

docker compose down

echo "Cleaning all data directories..."
for dir in postgres_data airflow_data prometheus_data grafana_data minio_data neo4j_data spark_data milvus_data etcd_data; do
    echo "  - $dir"
    docker run --rm -v $(pwd)/mnt/data_drive/$dir:/data alpine sh -c "rm -rf /data/*"
done

echo "Setting correct permissions..."
docker run --rm -v $(pwd)/mnt/data_drive/prometheus_data:/data alpine sh -c "chown -R 65534:65534 /data"
docker run --rm -v $(pwd)/mnt/data_drive/grafana_data:/data alpine sh -c "chown -R 472:472 /data"

echo "Cleanup complete!"
```

---

## Quick Reference

### Check Service Health
```bash
docker compose ps
```

### View Service Logs
```bash
docker compose logs <service-name> --tail 50 -f
```

### Restart Unhealthy Service
```bash
docker compose restart <service-name>
```

### Complete Rebuild
```bash
docker compose down
docker compose build --no-cache
docker compose up -d
```

### Fix All Permission Issues at Once
```bash
cd /srv/chatbot/shared/chatbot/infra-repo

# PostgreSQL
docker run --rm -v $(pwd)/mnt/data_drive/postgres_data:/data alpine sh -c "rm -rf /data/*"
docker run --rm -v $(pwd)/mnt/data_drive/airflow_data/airflow_db:/data alpine sh -c "rm -rf /data/*"

# Prometheus
docker run --rm -v $(pwd)/mnt/data_drive/prometheus_data:/data alpine sh -c "rm -rf /data/* && chown -R 65534:65534 /data"

# Grafana
docker run --rm -v $(pwd)/mnt/data_drive/grafana_data:/data alpine sh -c "chown -R 472:472 /data"

# Restart all
docker compose up -d
```

---

## Container User IDs Reference

- **PostgreSQL**: UID 999 (postgres)
- **Prometheus**: UID 65534 (nobody)
- **Grafana**: UID 472 (grafana)
- **Airflow**: UID 50000 (airflow)
- **Spark**: UID 185 (spark)

---

## Additional Resources

- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [PostgreSQL Docker Documentation](https://hub.docker.com/_/postgres)
- [Prometheus Docker Documentation](https://hub.docker.com/r/prom/prometheus)
- [Grafana Docker Documentation](https://grafana.com/docs/grafana/latest/setup-grafana/installation/docker/)
