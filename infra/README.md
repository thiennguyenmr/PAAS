<h1 align="center">🚀 Local LLM Ops Data Stack</h1>

<p align="center">This is a Modern Data Stack environment running entirely on Docker Local, optimized for RAG (Retrieval Augmented Generation) pipelines and LLM Fine-tuning workflows. It provides a full-lifecycle platform from data ingestion to observability.</p>

---

## 🏗️ Architecture Overview

The system integrates best-in-class tools for each stage of the data lifecycle:

### I. Data Ingestion & Processing
*   **Apache Spark**: Large-scale distributed data processing and transformation.
*   **Apache Airflow**: Workflow orchestrator for managing and monitoring data pipelines.

### II. Data Storage
*   **MinIO**: High-performance, S3-compatible object storage (Data Lake).
*   **PostgreSQL**: Relational database for application and metadata storage.
*   **Milvus + Etcd**: Vector database for storage and retrieval of AI embeddings.
*   **Neo4j**: Graph database for representing complex relationships and knowledge graphs.

### III. System Monitoring & Observability
*   **cAdvisor**: Analyzes resource usage and performance of running containers.
*   **Prometheus**: Time-series database for collecting and storing system metrics.
*   **Grafana**: Visualization platform for metrics and operational dashboards.

### IV. Security & Logging (Upcoming)
*   **LDAP**: Centralized authentication and user management.
*   **ELK Stack** (Elasticsearch, Logstash, Kibana): Distributed logging and analytics system.

---

## 📁 Project Structure

```text
infra-repo/
├── docker/                 # Custom Dockerfiles and configurations
│   ├── airflow/            # Airflow custom build & entrypoints
│   ├── spark/              # Spark custom build (Python/Java support)
│   ├── monitoring/         # Prometheus & Grafana configs
│   └── storages/           # DB init scripts (Postgres, etc.)
├── mnt/data_drive/         # Persisted data volumes (mapped to host)
│   ├── airflow_data/       # Airflow DB and logs
│   ├── minio_data/         # S3 buckets
│   ├── milvus_data/        # Vector collections
│   ├── neo4j_data/         # Graph data
│   └── ...                 # Other service volumes
├── docker-compose.yaml     # Core infrastructure orchestration
└── .env                    # Environment variables & secrets
```

---

## 📋 Prerequisites

Due to the resource-intensive nature of these services, your machine should meet these requirements:

*   **RAM**: Minimum 16GB (32GB recommended for complex RAG pipelines).
*   **Docker Memory**: Allocate at least **16GB** to Docker Desktop (Settings → Resources).
    - Neo4j requires 10GB (6GB heap + 4GB pagecache)
    - Milvus requires 8-12GB for vector operations
*   **CPU**: 4 Cores or more.
*   **Disk**: SSD with at least 20GB of free space.
*   **Software**: Docker Desktop & Docker Compose.

---

## 🛠️ Installation & Setup

### I. Configure Environment Variables
1.  Create a `.env` file from the provided template or ensure it contains the necessary variables (Airflow UID, DB passwords, etc.).
2.  Grant permissions for Neo4j to mount volumes:
    ```bash
    sudo chown -R 7474:7474 ./mnt/data_drive/neo4j_*
    sudo chmod -R 777 ./mnt/data_drive/neo4j_*
    ```

### II. Start the Stack
1.  Pull images and start all services in detached mode:
    ```bash
    docker compose up -d --build
    ```
    *Note: The first run may take 10-15 minutes to download all images.*

2.  **Neo4j Initial Config**:
    - **Connect URL**: `bolt://localhost:6008`
    - **User**: `neo4j`
    - **Password**: `graph_secret_password` (or as set in `.env`)

---

## 🔗 Access Points

| Service | Host URL | Internal URL (Docker) | Username | Password |
| :--- | :--- | :--- | :--- | :--- |
| **Airflow Web** | [http://localhost:6031](http://localhost:6031) | `airflow-webserver:8080` | `admin` | `admin123` |
| **MinIO Console**| [http://localhost:6001](http://localhost:6001) | `minio:9001` | `minioadmin` | `minioadmin` |
| **MinIO API (S3)**| [http://localhost:6000](http://localhost:6000) | `minio:9000` | `minioadmin` | `minioadmin` |
| **Spark Master** | [http://localhost:6020](http://localhost:6020) | `spark-master:8080` | N/A | N/A |
| **Spark History**| [http://localhost:6022](http://localhost:6022) | `spark-history:18080`| N/A | N/A |
| **Grafana** | [http://localhost:6052](http://localhost:6052) | `grafana:3000` | `admin` | `admin` |
| **Prometheus** | [http://localhost:6051](http://localhost:6051) | `prometheus:9090` | N/A | N/A |
| **PrometheusTargets**| [http://localhost:6051/targets](http://localhost:6051/targets) | `prometheus:9090/targets` | N/A | N/A |
| **Milvus API** | `localhost:6005` | `milvus-standalone:19530`| N/A | N/A |
| **Neo4j Browser**| [http://localhost:6007](http://localhost:6007) | `neo4j-server:7474` | `neo4j` | `graph_secret_password` |

---

## 🌐 Remote Access (SSH Tunneling)

If the stack is running on a remote server, use **SSH Tunneling** to access the UIs securely on your local machine.

### 1. Simple Port Forwarding
Run this command on your **local machine** to forward all service UIs:

```bash
ssh -L 6031:localhost:6031 -L 6001:localhost:6001 -L 6000:localhost:6000 \
    -L 6020:localhost:6020 -L 6022:localhost:6022 -L 6052:localhost:6052 \
    -L 6051:localhost:6051 -L 6007:localhost:6007 -L 6005:localhost:6005 \
    user@your-server-ip
```

### 2. Port Mapping Breakdown
| Service | Local URL | Port to Forward |
| :--- | :--- | :--- |
| **Airflow** | [http://localhost:6031](http://localhost:6031) | `6031` |
| **MinIO Console** | [http://localhost:6001](http://localhost:6001) | `6001` |
| **Grafana** | [http://localhost:6052](http://localhost:6052) | `6052` |
| **Spark Master** | [http://localhost:6020](http://localhost:6020) | `6020` |
| **Neo4j** | [http://localhost:6007](http://localhost:6007) | `6007` |

After running the SSH command, you can open the **Local URL** in your browser just as if the services were running locally.

### 3. Using Custom Hostnames (Advanced)
To avoid using `localhost`, map the service names to `127.0.0.1` on your local machine.

**On Mac/Linux:**
1. Open your hosts file: `sudo nano /etc/hosts`
2. Add the following line:
   ```text
   127.0.0.1  minio airflow spark grafana neo4j prometheus milvus
   ```
3. Save and exit.
4. Now you can access your services at:
   - [http://minio:6001](http://minio:6001)
   - [http://airflow:6031](http://airflow:6031)
   - [http://grafana:6052](http://grafana:6052)

---

## ⚙️ Configuration Notes

### 1. Networking (internal vs External)
Since all services run within the `llm-ops-net` internal network, **DO NOT** use `localhost` when configuring connections between services (e.g., Spark to MinIO).
*   **MinIO Endpoint**: `http://minio:9000`
*   **Milvus Endpoint**: `milvus-standalone:19530`
*   **Postgres**: `app-postgres:5432`

### 2. MinIO S3 Specifics
*   **Region**: `us-east-1`
*   **Force Path Style**: `True` (Mandatory for MinIO)
*   **Bucket**: `datalake` (created automatically via env)

### 3. Data Persistence
Your data is persisted in physical directories under `./mnt/data_drive/`. Deleting these folders will result in data loss.
*   `minio_data`: S3 objects and buckets.
*   `postgres_data`: Application metadata.
*   `milvus_data`: Vector database files.

---

## 🛠️ Maintenance

### Start/Stop Specific Services
```bash
docker compose up -d --build <service_name>
docker compose stop <service_name>
```

### Checking Logs
```bash
docker logs -f <container_name>  # e.g., docker logs -f airflow-webserver
```

### Health Check Status
```bash
docker compose ps
```

---

## 🔧 Troubleshooting

### Spark "Initial job has not accepted any resources"
This error means the task is requesting more resources than available:
- Check executor cores: `spark.executor.cores` must be ≤ worker cores (default: 4)
- Check executor memory: `spark.executor.memory` should be ≤ 6GB
- Kill stuck apps: Visit Spark Master UI → Running Applications → Kill

### Neo4j "Invalid memory configuration - exceeds physical memory"
- Increase Docker memory allocation to at least 16GB
- Current config requires ~10GB (heap + pagecache)

### Milvus Container Crashes
- Ensure Docker has 16GB+ memory allocated
- Avoid custom milvus.yaml in standalone mode (causes port conflicts)
- Container uses ~8-12GB during vector operations

