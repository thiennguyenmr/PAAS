# Infrastructure Scripts

Utility scripts for managing the infrastructure deployment.

## Available Scripts

### 1. `setup-infra.sh` - Complete Infrastructure Setup

**Use Case**: Setting up infrastructure on a **brand new server** or when you want a **complete fresh start**.

**What it does**:
- Stops all services
- Cleans all data directories
- Sets correct permissions for all services
- Builds base images (Spark, Airflow)
- Starts all services in the correct dependency order

**Usage**:
```bash
cd /srv/chatbot/shared/chatbot/infra-repo
./scripts/setup-infra.sh
```

**Time**: ~5-10 minutes depending on your system

---

### 2. `fix-permissions.sh` - Quick Permission Fix

**Use Case**: Fixing **PostgreSQL** and **Prometheus** issues when moving to a new server.

**What it does**:
- Fixes PostgreSQL database corruption
- Fixes Prometheus permission denied errors
- Restarts affected services

**Usage**:
```bash
cd /srv/chatbot/shared/chatbot/infra-repo
./scripts/fix-permissions.sh
```

**Time**: ~1-2 minutes

---

## Common Scenarios

### Scenario 1: Deploying to a New Server (First Time)

Use the complete setup script:
```bash
./scripts/setup-infra.sh
```

### Scenario 2: Moving Infrastructure Between Servers

If you're experiencing PostgreSQL or Prometheus issues:
```bash
./scripts/fix-permissions.sh
```

### Scenario 3: Partial Issues (Only One Service)

For PostgreSQL only:
```bash
docker compose down app-postgres-db airflow-postgres
docker run --rm -v $(pwd)/mnt/data_drive/postgres_data:/data alpine sh -c "rm -rf /data/*"
docker run --rm -v $(pwd)/mnt/data_drive/airflow_data/airflow_db:/data alpine sh -c "rm -rf /data/*"
docker compose up -d app-postgres-db airflow-postgres
```

For Prometheus only:
```bash
docker compose down prometheus
docker run --rm -v $(pwd)/mnt/data_drive/prometheus_data:/data alpine sh -c "rm -rf /data/* && chown -R 65534:65534 /data"
docker compose up -d prometheus
```

---

## Troubleshooting

For detailed troubleshooting information, see: [TROUBLESHOOTING.md](../TROUBLESHOOTING.md)

### Quick Checks

Check service status:
```bash
docker compose ps
```

View service logs:
```bash
docker compose logs -f <service-name>
```

Check for unhealthy services:
```bash
docker compose ps | grep -E "unhealthy|Restarting"
```

---

## Service Startup Order

The scripts follow this startup order to respect service dependencies:

1. **Storage**: MinIO, PostgreSQL, Etcd
2. **Databases**: Milvus, Neo4j
3. **Processing**: Spark (master → workers → history)
4. **Orchestration**: Airflow (postgres → webserver/scheduler/dag-processor)
5. **Monitoring**: cAdvisor → Prometheus → Grafana

---

## Script Maintenance

### Adding New Services

When adding new services, update:
1. `setup-infra.sh` - Add to appropriate startup stage
2. `TROUBLESHOOTING.md` - Add common issues and fixes
3. This README - Update service list

### Testing Scripts

Always test scripts in a development environment first:
```bash
# Dry run (without actually executing)
bash -n ./scripts/setup-infra.sh

# Test in development
./scripts/setup-infra.sh
```
