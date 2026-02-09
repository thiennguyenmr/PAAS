<h1 align="center">PAAS - Platform as a Stack</h1>

<p align="center">A self-hosted Modern Data Stack running entirely on Docker, designed for RAG (Retrieval Augmented Generation) pipelines and LLM Fine-tuning workflows. From raw data ingestion to vector search and knowledge graphs — all orchestrated locally.</p>

---

## Overview

PAAS combines infrastructure provisioning and data pipeline development into a single monorepo. It provides a complete data lifecycle platform with distributed processing, multi-model storage (relational, object, vector, graph), workflow orchestration, and observability.

The project is split into two main areas:

| Directory | Purpose |
| :--- | :--- |
| **[`infra/`](infra/)** | Docker services, networking, monitoring, and all infrastructure configuration |
| **[`data-pipeline/`](data-pipeline/)** | Spark jobs, Airflow DAGs, ETL logic, and connector modules |

---

## Architecture

```text
                        ┌──────────────────────────────────┐
                        │        Apache Airflow             │
                        │     (Workflow Orchestration)       │
                        └──────────┬───────────────────────┘
                                   │ triggers
                        ┌──────────▼───────────────────────┐
                        │        Apache Spark               │
                        │   (Distributed Data Processing)   │
                        │    1 Master  +  2 Workers          │
                        └──┬───────┬───────┬───────┬───────┘
                           │       │       │       │
              ┌────────────▼──┐ ┌──▼────┐ ┌▼─────┐ ┌▼──────┐
              │  MinIO (S3)   │ │Postgre│ │Milvus│ │ Neo4j │
              │  Data Lake    │ │  SQL  │ │Vector│ │ Graph │
              └───────────────┘ └───────┘ └──────┘ └───────┘
                           │
              ┌────────────▼──────────────────────┐
              │  Prometheus + Grafana + cAdvisor   │
              │         (Observability)            │
              └───────────────────────────────────┘
```

### Technology Stack

| Layer | Technology | Role |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow 3.1.4 | DAG scheduling, pipeline management |
| **Processing** | Apache Spark 3.5.8 | Distributed ETL, data transformation |
| **Object Storage** | MinIO | S3-compatible Data Lake (Parquet files) |
| **Relational DB** | PostgreSQL 16 | Application data, Airflow metadata |
| **Vector DB** | Milvus 2.6 + Etcd | Embedding storage for RAG |
| **Graph DB** | Neo4j 5.x | Knowledge graph relationships |
| **Monitoring** | Prometheus, Grafana, cAdvisor | Metrics, dashboards, container stats |

---

## Data Pipeline: Medallion Architecture

The ETL pipelines follow a **Medallion Architecture** for data quality and traceability:

```text
Source Systems ──► Raw (Inbound) ──► Bronze (Cleaned) ──► Silver (Transformed) ──► Gold (Downstream)
  CSV / DB             MinIO            MinIO                MinIO               Milvus / Neo4j
```

1. **Raw**: Extract from CSV files and PostgreSQL, store as Parquet in MinIO
2. **Bronze**: Schema normalization, deduplication, basic cleaning
3. **Silver**: Business logic, aggregations, feature engineering
4. **Gold**: Load into Milvus (vector search) and Neo4j (knowledge graph)

### Available Pipelines

| Pipeline | Description |
| :--- | :--- |
| `hilo_complete_pipeline` | Orchestrates game round + chat pipelines in parallel |
| `hilo_game_round_pipeline` | Processes game round data (bets, outcomes, sessions) |
| `hilo_game_chat_pipeline` | Processes chat message data (user messages, channels) |

Each pipeline runs through sequential Spark jobs: **ingest → merge → quality check → clean → transform → vectorize → graph load**.

---

## Quick Start

### Prerequisites

- **RAM**: 16GB minimum (32GB recommended)
- **CPU**: 4+ cores
- **Disk**: 20GB+ SSD free space
- **Docker Desktop**: 16GB memory allocated to Docker

### Setup

1. Configure environment variables:
   ```bash
   cp infra/.env.example infra/.env   # edit with your settings
   ```

2. Start the infrastructure:
   ```bash
   cd infra
   docker compose up -d --build
   ```

3. Access the services:

   | Service | URL | Credentials |
   | :--- | :--- | :--- |
   | Airflow | [http://localhost:6031](http://localhost:6031) | `admin` / `admin123` |
   | MinIO Console | [http://localhost:6001](http://localhost:6001) | `minioadmin` / `minioadmin` |
   | Spark Master | [http://localhost:6020](http://localhost:6020) | — |
   | Grafana | [http://localhost:6052](http://localhost:6052) | `admin` / `admin` |
   | Neo4j Browser | [http://localhost:6007](http://localhost:6007) | `neo4j` / `graph_secret_password` |

---

## Project Structure

```text
PAAS/
├── infra/                          # Infrastructure
│   ├── docker/                     # Custom Dockerfiles & configs
│   │   ├── airflow/                #   Airflow image + entrypoints
│   │   ├── spark/                  #   Spark image (Python/Java)
│   │   ├── monitoring/             #   Prometheus & Grafana configs
│   │   └── storages/               #   DB init scripts (PostgreSQL)
│   ├── mnt/data_drive/             # Persistent volume mounts
│   ├── scripts/                    # Setup & maintenance scripts
│   ├── docker-compose.yaml         # Service orchestration
│   ├── .env                        # Environment variables
│   ├── README.md                   # Infrastructure docs
│   └── TROUBLESHOOTING.md          # Common issues & fixes
│
└── data-pipeline/                  # Data Processing
    ├── airflow/                    # Orchestration layer
    │   ├── airflow_dags/           #   DAG definitions
    │   ├── airflow_configs/        #   Connection profiles
    │   └── airflow_modules/        #   Shared utilities
    └── spark/                      # Processing layer
        ├── spark_jobs/             #   Pipeline job scripts
        │   ├── game_round/         #     Game round ETL (7 stages)
        │   ├── game_chat/          #     Chat message ETL (7 stages)
        │   └── test_pipeline/      #     Validation scripts
        ├── spark_modules/          #   Connectors & processing libs
        └── spark_configs/          #   Spark setup & JAR profiles
```

---

## Documentation

- **[Infrastructure README](infra/README.md)** — Service configuration, networking, access points, remote SSH tunneling, troubleshooting
- **[Data Pipeline README](data-pipeline/README.md)** — Pipeline architecture, local development setup, Spark job execution, Airflow integration
- **[Troubleshooting Guide](infra/TROUBLESHOOTING.md)** — PostgreSQL recovery, permission issues, container debugging

---

## Networking

All services communicate over the `llm-ops-net` Docker bridge network. Use **service names** (not `localhost`) for inter-service connections:

| Service | Internal Address |
| :--- | :--- |
| MinIO S3 API | `minio:9000` |
| PostgreSQL | `app-postgres:5432` |
| Milvus | `milvus-standalone:19530` |
| Neo4j Bolt | `neo4j-server:7687` |
| Spark Master | `spark-master:7077` |

External access uses the `6000-6059` port range. For remote servers, use SSH tunneling — see the [infra README](infra/README.md#-remote-access-ssh-tunneling).
