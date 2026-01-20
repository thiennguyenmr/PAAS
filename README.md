<h1 align="center">🚀 Local LLM Ops Data Stack</h1>

<p align="center">This is a Modern Data Stack environment running entirely on Docker Local, optimized for RAG (Retrieval Augmented Generation) pipelines and LLM Fine-tuning workflows.</p>


## The system integrates best-in-class tools for each stage of the data lifecycle:

1. **MinIO**: Data Lake (S3-compatible Object Storage).

2. **Postgre**: Application database.

3. **Apache Spark**: Data Processing (Large-scale transformation).

4. **Milvus**: Vector Database (Storage for AI Embeddings).

5. **Apache Airflow**: Orchestrator (Manage the entire pipeline).

## 📋 Prerequisites
 Due to the resource-intensive nature of these Java-based services, your machine must meet the following requirements:

* **RAM**: Minimum 16GB (Recommended 32GB for smooth operation).

* **CPU**: 4 Cores or more.

* **Disk**: SSD with at least 20GB of free space.

* **Software**: Docker Desktop & Docker Compose.

## 🛠️ Installation & Setup

### 1. Configure Environment Variables

Create a `.env` file in the same directory as your `docker-compose.yaml` and ensure it has the necessary configurations (Airbyte DB, Airflow UID).

### 2. Start the Stack

Run the following command to pull images and start the containers:

```bash
docker compose up -d
```
*Note: The first run may take 10-15 minutes to download all Docker images.*

## 🔗 Access Points

Keep this table handy to access your services:

| Service | URL | Username | Password |
| :--- | :--- | :--- | :--- |
| **Airflow** | [http://localhost:8081](http://localhost:8081) | `admin` | `admin` |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` | `minioadmin` |
| **Spark Master** | [http://localhost:8080](http://localhost:8080) | N/A | N/A |
| **Milvus** | `localhost:19530` | N/A | N/A |
| **MinIO API** | `http://localhost:9000` | `minioadmin` | `minioadmin` |

## ⚙️ Configuration Notes

### 1. Connecting to MinIO (S3) from Airbyte/Spark

Since all services run within the internal Docker network (`llm-ops-net`), **DO NOT** use `localhost` when configuring internal connections. Use the following settings:

* **Endpoint URL:** `http://minio:9000`
* **Access Key:** `minioadmin`
* **Secret Key:** `minioadmin`
* **Region:** `us-east-1` (Default)
* **Force Path Style:** `True` (Mandatory for MinIO)

### 2. MinIO Version Pinning

This stack uses a specific release of MinIO to ensure stability and reproducibility:
* **Image:** `minio/minio:RELEASE.2025-09-07T16-13-09Z`

### 3. Data Persistence (Volumes)

Your data is persisted in the following Docker Volumes:

* `minio_data`: S3 bucket objects.
* `milvus_data`: Vector embeddings.
* `./dags`: Local directory for Airflow Python DAGs (Mapped from host to container).


