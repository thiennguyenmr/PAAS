<h1 align="center">📊 Data Pipeline Repository</h1>

<p align="center">This repository contains the ETL (Extract, Transform, Load) logic, Spark jobs, and Airflow orchestration for the LLM Ops platform.</p>

---

## 🏗️ Architecture: Medallion Flow

The pipeline follows a simplified **Medallion Architecture** to ensure data quality and traceability:

1.  **Raw Layer (Inbound)**: Raw data extracted from source systems (e.g., PostgreSQL) and stored in MinIO as Parquet files.
2.  **Bronze Layer (Cleaned)**: Data with basic cleaning, schema normalization, and deduplication.
3.  **Silver Layer (Transformed)**: Business logic, aggregations, and feature engineering applied.
4.  **Gold/Storage Layer (Downstream)**: Final data loaded into specialized databases:
    - **Milvus**: For Vector Search (RAG).
    - **Neo4j**: For Knowledge Graphs.

---

## 📁 Repository Structure

```text
data-pipeline-repo/
├── airflow/                   # Orchestration Layer
│   ├── airflow_dags/          # Airflow DAG definitions (Python)
│   ├── airflow_configs/       # Connection profiles and DAG configs
│   └── airflow_modules/       # Shared Airflow helper utilities
└── spark/                     # Processing Layer
    ├── spark_jobs/            # Spark Application scripts
    │   ├── game_round/        # Game round data pipeline jobs
    │   ├── game_chat/         # Chat message pipeline jobs
    │   └── test_pipeline/     # Test scripts for validation
    ├── spark_modules/         # Core processing logic & connectors
    └── spark_configs/         # Spark setup and JAR profiles
```

---

## 💻 Local Development Setup

When developing locally on your host machine (outside of Docker), you need to set the `PYTHONPATH` so that Python can find the `modules/` and `configs/` folders.

### macOS / Linux / Windows (Git Bash)
Run the following command from the `spark/` directory:
```bash
source setup_dev.sh
```

### Windows (PowerShell)
Run the following command from the `spark/` directory:
```powershell
. .\setup_dev.ps1
```

---

## 🚀 Available Pipelines

Two main data pipelines are available in `airflow/airflow_dags/`:

- **`hilo_game_round_pipeline`**: Processes game round data (bets, outcomes, sessions)
- **`hilo_game_chat_pipeline`**: Processes chat message data (user messages, channels)

Both pipelines follow the Medallion Architecture stages and output to Milvus (vectors) and Neo4j (knowledge graph).

---

## 🛠️ Development & Execution

### Running Spark Jobs Locally (Manual)
If you have access to the Spark Master container, you can submit jobs manually for debugging:
```bash
docker exec -it spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/spark_jobs/game_round/01a_ingest_csv.py
```

### Airflow Integration
DAGs are automatically synced from the `airflow/airflow_dags` folder to the Airflow container.
- **Base Directory**: `/opt/airflow/dags` (Mapped from host).
- **Environment**: Ensure `PYTHONPATH` includes `/opt/spark/work-dir` for module access.

> [!TIP]
> **Remote Access**: If running on a remote server, use SSH Tunneling (e.g., `ssh -L 6031:localhost:6031 ...`) to access Airflow and Spark UIs locally. See the [Infrastructure README](../infra-repo/README.md#🌐-remote-access-ssh-tunneling) for a full guide.

---

## 📝 Dependencies
- **PySpark**: 3.5.8
- **Airflow**: 3.1.4
- **Connectors**: JDBC (Postgres), S3A (MinIO), Milvus Python SDK, Neo4j Driver.
