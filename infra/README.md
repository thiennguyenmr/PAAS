<h1 align="center">Local LLM Ops Data Stack</h1>

<p align="center">A Modern Data Stack environment running entirely on Docker, optimized for RAG (Retrieval Augmented Generation) pipelines and LLM Fine-tuning workflows. It provides a full-lifecycle platform from data ingestion to observability with centralized identity management and secret storage.</p>

---

## Architecture Overview

The system integrates best-in-class tools for each stage of the data lifecycle:

### I. Data Ingestion & Processing
*   **Apache Spark** (2 workers, 8 cores / 16GB each): Distributed data processing and transformation.
*   **Apache Airflow 3.x**: Workflow orchestrator with GPU-enabled scheduler for ML pipelines.

### II. Data Storage
*   **MinIO**: High-performance, S3-compatible object storage (Data Lake).
*   **PostgreSQL 16**: Relational database for application and metadata storage.
*   **Milvus + Etcd**: Vector database for storage and retrieval of AI embeddings.
*   **Neo4j 5.x**: Graph database for representing complex relationships and knowledge graphs.

### III. ML Experiment Tracking
*   **MLflow 3.x**: Experiment tracking, model registry, and artifact management for ML/LLM workflows.

### IV. System Monitoring & Observability
*   **cAdvisor**: Analyzes resource usage and performance of running containers.
*   **Prometheus**: Time-series database for collecting and storing system metrics.
*   **Alertmanager**: Alert routing and notification via email (Brevo SMTP).
*   **Grafana**: Visualization platform for metrics and operational dashboards.
*   **Grafana Image Renderer**: Server-side rendering of dashboard panels for alerts and reports.

### V. Security & Identity
*   **Keycloak**: Centralized identity and access management (SSO/OIDC) for Airflow, Grafana, and Spark.
*   **HashiCorp Vault**: Secret management for storing credentials, API keys, and connection strings.
*   **OAuth2 Proxy**: Authentication proxy for services without native OAuth support (Spark Master & History Server).

---

## Project Structure

```text
infra-repo/
├── docker/                       # Custom Dockerfiles and configurations
│   ├── airflow/                  # Airflow custom build, entrypoints & configs
│   │   ├── _config/              # webserver_config.py, spark-defaults.conf, init_connections.sh
│   │   ├── dockerfile
│   │   └── entrypoint-with-init.sh
│   ├── spark/                    # Spark custom build (Python/Java support)
│   │   ├── _config/              # requirements.txt, spark-defaults.conf
│   │   └── dockerfile
│   ├── monitoring/               # Prometheus, Alertmanager & Grafana configs
│   │   ├── prometheus.yml
│   │   ├── alert.rules.yml
│   │   └── alertmanager.yml
│   ├── vault/                    # Vault server config, policies & init scripts
│   │   ├── config/vault.hcl
│   │   ├── policies/airflow-policy.hcl
│   │   ├── scripts/              # init-vault.sh, init-secrets.sh, setup-auth.sh
│   │   └── entrypoint.sh
│   ├── keycloak/                 # Keycloak realm import
│   │   └── import/paas-realm.json
│   └── storages/                 # DB init scripts (Postgres, Milvus)
├── scripts/                      # Operational scripts
│   ├── setup-infra.sh            # Full infrastructure setup (clean + init)
│   ├── setup-keycloak.sh         # Keycloak realm configuration
│   ├── fix-permissions.sh        # Fix volume permissions
│   └── ssh-tunnel.sh             # Remote access SSH tunneling
├── mnt/data_drive/               # Persisted data volumes (mapped to host)
│   ├── airflow_data/             # Airflow DB and logs
│   ├── minio_data/               # S3 buckets
│   ├── milvus_data/              # Vector collections
│   ├── neo4j_data/               # Graph data
│   ├── vault_data/               # Vault persistent storage
│   ├── keycloak_data/            # Keycloak PostgreSQL data
│   ├── prometheus_data/          # Prometheus TSDB
│   ├── grafana_data/             # Grafana dashboards & config
│   └── spark_data/               # Spark event logs
├── docker-compose.yaml           # Core infrastructure orchestration
├── .env.example                  # Environment variables template
└── TROUBLESHOOTING.md            # Common issues and fixes
```

---

## Prerequisites

Due to the resource-intensive nature of these services, your machine should meet these requirements:

*   **RAM**: Minimum 32GB (64GB recommended for full stack).
*   **Docker Memory**: Allocate at least **24GB** to Docker.
    - Neo4j: 12GB (6GB heap + 4GB pagecache + overhead)
    - Milvus: 8-12GB for vector operations
    - Spark Workers: 16GB each
*   **CPU**: 8 Cores or more.
*   **GPU**: NVIDIA GPU (optional, for Airflow scheduler ML tasks).
*   **Disk**: SSD with at least 50GB of free space.
*   **Software**: Docker, Docker Compose, NVIDIA Container Toolkit (if using GPU).

---

## Installation & Setup

### I. Configure Environment Variables

1. Copy the example environment file:
    ```bash
    cp .env.example .env
    ```
2. Update all placeholder values in `.env` (passwords, secrets, Fernet keys, etc.).

### II. Automated Setup (Recommended)

Run the full setup script which handles permissions, image builds, and ordered service startup:

```bash
./scripts/setup-infra.sh
```

This script will:
1. Stop all running containers
2. Clean and initialize all data directories with correct permissions
3. Build base images (Spark, Airflow)
4. Start services in the correct dependency order (6 stages)
5. Initialize Vault secrets and Keycloak realm

### III. Manual Setup

If you prefer to start services manually:

```bash
# 1. Build base images
docker compose build spark-base airflow-base

# 2. Start storage layer
docker compose up -d minio app-postgres-db

# 3. Start databases
docker compose up -d milvus neo4j

# 4. Start security services
docker compose up -d vault keycloak-postgres keycloak

# 5. Start processing
docker compose up -d spark-master spark-worker-1 spark-worker-2 spark-history

# 6. Start orchestration
docker compose up -d airflow-postgres airflow-webserver airflow-scheduler airflow-dag-processor

# 7. Start monitoring
docker compose up -d cadvisor alertmanager prometheus renderer grafana

# 8. Start OAuth2 proxies (optional)
docker compose up -d spark-oauth2-proxy spark-history-oauth2-proxy
```

---

## Access Points

### Port Allocation Scheme

| Range       | Category    |
| :---------- | :---------- |
| 6000 - 6019 | Storage     |
| 6020 - 6034 | Processing  |
| 6035 - 6039 | ML Tracking |
| 6050 - 6059 | Monitoring  |
| 6060 - 6069 | Security    |

### Service URLs

| Service | Host URL | Internal URL (Docker) | Auth |
| :--- | :--- | :--- | :--- |
| **MinIO Console** | http://localhost:6001 | `minio:9001` | `.env` credentials |
| **MinIO API (S3)** | http://localhost:6000 | `minio:9000` | `.env` credentials |
| **App PostgreSQL** | `localhost:6003` | `app-postgres:5432` | `.env` credentials |
| **Milvus gRPC** | `localhost:6005` | `milvus-standalone:19530` | N/A |
| **Neo4j Browser** | http://localhost:6007 | `neo4j-server:7474` | `.env` `NEO4J_AUTH` |
| **Neo4j Bolt** | `localhost:6008` | `neo4j-server:7687` | `.env` `NEO4J_AUTH` |
| **Spark Master** | http://localhost:6020 | `spark-master:8080` | N/A (or via OAuth2 Proxy :6025) |
| **Spark History** | http://localhost:6022 | `spark-history:18080` | N/A (or via OAuth2 Proxy :6026) |
| **MLflow UI** | http://localhost:6035 | `mlflow-server:5000` | N/A (or via OAuth2 Proxy :6036) |
| **Airflow Web** | http://localhost:6031 | `airflow-webserver:8080` | Keycloak SSO |
| **cAdvisor** | http://localhost:6050 | `cadvisor:8080` | N/A |
| **Prometheus** | http://localhost:6051 | `prometheus:9090` | N/A |
| **Grafana** | http://localhost:6052 | `grafana:3000` | Keycloak SSO |
| **Alertmanager** | http://localhost:6054 | `alertmanager:9093` | N/A |
| **Vault** | http://localhost:6060 | `vault:8200` | Root token |
| **Keycloak** | http://localhost:6061 | `keycloak:8080` | `.env` admin credentials |

---

## Security Architecture

### Authentication Flow

All user-facing services authenticate through **Keycloak** (OIDC/OAuth2):

- **Airflow**: Native Keycloak Auth Manager (`KeycloakAuthManager`)
- **Grafana**: Generic OAuth integration with role mapping (`grafana_roles`)
- **Spark UIs**: OAuth2 Proxy sidecar containers with Keycloak backend
- **MLflow UI**: OAuth2 Proxy sidecar container with Keycloak backend

### Secret Management

**HashiCorp Vault** stores all sensitive credentials and is integrated with Airflow's secrets backend:

- Vault mode: `production` (persistent file storage) or `dev` (in-memory)
- Airflow reads connections and variables from Vault at `secret/airflow/connections` and `secret/airflow/variables`
- Secrets are initialized automatically on first startup via `init-secrets.sh`

### Keycloak Realm

The `paas` realm is auto-imported on first startup with pre-configured clients:
- `airflow` - Airflow SSO
- `grafana` - Grafana SSO with role mapping
- `spark` - Spark OAuth2 Proxy
- `mlflow` - MLflow OAuth2 Proxy

---

## Remote Access (SSH Tunneling)

If the stack is running on a remote server, use the provided tunneling script:

```bash
./scripts/ssh-tunnel.sh
```

This script:
- Forwards all service ports to your local machine
- Optionally adds hostname entries to `/etc/hosts`
- Displays a service URL table on connection

### Manual Port Forwarding

```bash
ssh -L 6031:localhost:6031 -L 6001:localhost:6001 -L 6000:localhost:6000 \
    -L 6020:localhost:6020 -L 6022:localhost:6022 -L 6052:localhost:6052 \
    -L 6051:localhost:6051 -L 6007:localhost:6007 -L 6005:localhost:6005 \
    -L 6035:localhost:6035 -L 6036:localhost:6036 \
    -L 6060:localhost:6060 -L 6061:localhost:6061 \
    -p <port> user@your-server-ip
```

### Custom Hostnames (Optional)

Add to `/etc/hosts` on your local machine:
```text
127.0.0.1  minio airflow spark grafana neo4j prometheus milvus vault keycloak mlflow
```

---

## Configuration Notes

### 1. Internal Networking

All services run within the `llm-ops-net` bridge network. Use **container names** (not `localhost`) for inter-service communication:

| Service | Internal Endpoint |
| :--- | :--- |
| MinIO | `http://minio:9000` |
| Milvus | `milvus-standalone:19530` |
| PostgreSQL (App) | `app-postgres:5432` |
| PostgreSQL (Airflow) | `airflow-postgres:5432` |
| Vault | `http://vault:8200` |
| Keycloak | `http://keycloak:8080` |
| MLflow | `http://mlflow-server:5000` |

### 2. MinIO S3 Configuration
*   **Region**: `us-east-1`
*   **Force Path Style**: `True` (mandatory for MinIO)
*   **Bucket**: `datalake` (created automatically)

### 3. Data Persistence
All data is persisted in `./mnt/data_drive/`. Deleting these folders will result in **data loss**.

### 4. Airflow Auth Manager Options

Three authentication modes are available (configured in `.env`):

| Mode | Config Value | Description |
| :--- | :--- | :--- |
| **Keycloak Native** (default) | `KeycloakAuthManager` | SSO via Keycloak OIDC |
| **FAB Auth Manager** | `FabAuthManager` | Keycloak via `webserver_config.py` |
| **Simple Auth** | `SimpleAuthManager` | Local username/password |

---

## Maintenance

### Start/Stop Services
```bash
docker compose up -d --build <service_name>
docker compose stop <service_name>
```

### Checking Logs
```bash
docker logs -f <container_name>
```

### Health Check Status
```bash
docker compose ps
```

### Vault Operations
```bash
# List secrets
docker compose exec vault vault kv list secret/airflow/connections

# Get a secret
docker compose exec vault vault kv get secret/system/minio

# Re-initialize secrets
docker compose exec vault sh /vault/scripts/init-secrets.sh
```

---

## Troubleshooting

### Spark "Initial job has not accepted any resources"
- Check executor cores: `spark.executor.cores` must be <= worker cores (default: 8)
- Check executor memory: `spark.executor.memory` should be <= 16GB
- Kill stuck apps: Visit Spark Master UI -> Running Applications -> Kill

### Neo4j "Invalid memory configuration - exceeds physical memory"
- Increase Docker memory allocation to at least 24GB
- Current config requires ~12GB (heap + pagecache + overhead)

### Milvus Container Crashes
- Ensure Docker has 24GB+ memory allocated
- Avoid custom `milvus.yaml` in standalone mode (causes port conflicts)
- Container uses ~8-12GB during vector operations

### Vault Sealed After Restart
- In production mode, Vault seals on restart and must be unsealed
- Check `mnt/data_drive/vault_data/.vault-keys` for unseal keys
- The entrypoint script handles auto-unseal on container start

### Keycloak Realm Not Imported
- Realm import only happens on first startup with an empty database
- To re-import: stop Keycloak, clean `mnt/data_drive/keycloak_data/`, restart

### Permission Issues on Data Directories
```bash
./scripts/fix-permissions.sh
```

For more details, see [TROUBLESHOOTING.md](TROUBLESHOOTING.md).
