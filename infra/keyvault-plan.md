# Secret Management Plan: Airflow Connections + HashiCorp Vault

## Overview

This plan implements a two-phase approach to secret management:
- **Phase 1:** Airflow Connections (immediate, simpler)
- **Phase 2:** HashiCorp Vault integration (enterprise-ready)

---

## Current State

```
┌─────────────────────────────────────────────────────────────┐
│  Current: YAML Files (gitignored)                           │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  storage_config.yaml                                │    │
│  │  db_configs.yaml                                    │    │
│  │  (credentials in plain text on disk)                │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

**Problems:**
- Credentials stored in plain text
- No audit trail
- Manual sync across environments
- Risk of accidental commit

---

## Phase 1: Airflow Connections

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Airflow Metadata DB                      │
│                     (airflow-postgres)                      │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  Table: connection                                  │    │
│  │  ───────────────────────────────────────────────────│    │
│  │  conn_id      │ host   │ login      │ password      │    │
│  │  ───────────────────────────────────────────────────│    │
│  │  minio_bronze │ minio  │ minioadmin │ [encrypted]   │    │
│  │  minio_silver │ minio  │ minioadmin │ [encrypted]   │    │
│  │  pg_app       │ app-pg │ app_admin  │ [encrypted]   │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                             │
│  Encryption: AIRFLOW__CORE__FERNET_KEY                      │
└─────────────────────────────────────────────────────────────┘
```

### Connection Mapping

| Service | conn_id | host | port | login | password | schema |
|---------|---------|------|------|-------|----------|--------|
| MinIO Bronze | `minio_bronze` | minio | 9000 | minioadmin | minioadmin | bronze |
| MinIO Silver | `minio_silver` | minio | 9000 | minioadmin | minioadmin | silver |
| MinIO External | `minio_external` | 10.30.0.11 | 6000 | minioadmin | minioadmin | datalake |
| PostgreSQL App | `pg_app` | 10.130.0.3 | 6003 | app_admin | zyneric | app_data |
| Neo4j | `neo4j_default` | neo4j | 7687 | neo4j | graph_secret_password | - |
| Milvus | `milvus_default` | milvus | 19530 | - | - | - |

### Implementation Steps

#### Step 1: Update `init_connections.sh`

```bash
#!/bin/bash
# infra-repo/docker/airflow/_config/init_connections.sh

set -e
echo "Initializing Airflow connections..."

# Spark
airflow connections add spark_default \
  --conn-type spark \
  --conn-host spark://spark-master \
  --conn-port 7077 \
  || echo "spark_default already exists"

# MinIO Bronze
airflow connections add minio_bronze \
  --conn-type generic \
  --conn-host minio \
  --conn-port 9000 \
  --conn-login "${MINIO_ACCESS_KEY:-minioadmin}" \
  --conn-password "${MINIO_SECRET_KEY:-minioadmin}" \
  --conn-schema bronze \
  --conn-extra '{"ssl_enabled": false}' \
  || echo "minio_bronze already exists"

# MinIO Silver
airflow connections add minio_silver \
  --conn-type generic \
  --conn-host minio \
  --conn-port 9000 \
  --conn-login "${MINIO_ACCESS_KEY:-minioadmin}" \
  --conn-password "${MINIO_SECRET_KEY:-minioadmin}" \
  --conn-schema silver \
  --conn-extra '{"ssl_enabled": false}' \
  || echo "minio_silver already exists"

# MinIO External
airflow connections add minio_external \
  --conn-type generic \
  --conn-host "${MINIO_EXTERNAL_HOST:-10.30.0.11}" \
  --conn-port "${MINIO_EXTERNAL_PORT:-6000}" \
  --conn-login "${MINIO_ACCESS_KEY:-minioadmin}" \
  --conn-password "${MINIO_SECRET_KEY:-minioadmin}" \
  --conn-schema datalake \
  --conn-extra '{"ssl_enabled": false}' \
  || echo "minio_external already exists"

# PostgreSQL App
airflow connections add pg_app \
  --conn-type postgres \
  --conn-host "${PG_HOST:-10.130.0.3}" \
  --conn-port "${PG_PORT:-6003}" \
  --conn-login "${PG_USER:-app_admin}" \
  --conn-password "${PG_PASSWORD:-zyneric}" \
  --conn-schema "${PG_DB:-app_data}" \
  || echo "pg_app already exists"

# Neo4j
airflow connections add neo4j_default \
  --conn-type generic \
  --conn-host neo4j \
  --conn-port 7687 \
  --conn-login "${NEO4J_USER:-neo4j}" \
  --conn-password "${NEO4J_PASSWORD:-graph_secret_password}" \
  --conn-extra '{"protocol": "bolt"}' \
  || echo "neo4j_default already exists"

# Milvus
airflow connections add milvus_default \
  --conn-type generic \
  --conn-host milvus \
  --conn-port 19530 \
  || echo "milvus_default already exists"

echo "All connections initialized!"
```

#### Step 2: Add method to `airflow_minio.py`

```python
def load_config_from_connection(self, conn_id):
    """
    Load MinIO configuration from Airflow Connection.

    Connection field mapping:
        - host:port  -> endpoint
        - login      -> access_key
        - password   -> secret_key
        - schema     -> bucket
        - extra      -> additional settings (JSON)
    """
    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(conn_id)

        # Build endpoint from host:port
        endpoint = conn.host
        if conn.port:
            endpoint = f"{conn.host}:{conn.port}"

        self.storage_conn = {
            "endpoint": endpoint,
            "access_key": conn.login,
            "secret_key": conn.password,
            "bucket": conn.schema
        }

        # Parse extra for ssl_enabled
        ssl_enabled = False
        if conn.extra:
            import json
            extra = json.loads(conn.extra) if isinstance(conn.extra, str) else conn.extra
            ssl_enabled = extra.get("ssl_enabled", False)

        self.storage_libs = {"ssl_enabled": ssl_enabled}

        print(f">>> Configuration loaded from Airflow connection '{conn_id}'.")

    except Exception as e:
        print(f"!!! Error loading from connection: {e}")
        raise
```

#### Step 3: Usage in DAG/Job

```python
from airflow_modules.airflow_minio import AirflowMinioStorage

storage = AirflowMinioStorage()
storage.load_config_from_connection("minio_bronze")
storage.init_client()

# Use storage methods
storage.upload_json("path/to/file.json", data)
```

---

## Phase 2: HashiCorp Vault Integration

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   HashiCorp Vault                           │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  Secret Engine: kv-v2                               │    │
│  │  Path: secret/airflow/connections/                  │    │
│  │                                                     │    │
│  │  secret/airflow/connections/minio_bronze            │    │
│  │  {                                                  │    │
│  │    "conn_type": "generic",                          │    │
│  │    "host": "minio",                                 │    │
│  │    "port": 9000,                                    │    │
│  │    "login": "minioadmin",                           │    │
│  │    "password": "minioadmin",                        │    │
│  │    "schema": "bronze"                               │    │
│  │  }                                                  │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                             │
│  Auth: AppRole / Kubernetes / Token                         │
│  Audit: Full access logging                                 │
│  Rotation: Automatic credential rotation                    │
└─────────────────────────────────────────────────────────────┘
          │
          │ Airflow fetches secrets automatically
          ▼
┌─────────────────────────────────────────────────────────────┐
│                      Airflow                                │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  airflow.cfg or Environment Variables               │    │
│  │                                                     │    │
│  │  [secrets]                                          │    │
│  │  backend = airflow.providers.hashicorp.secrets.     │    │
│  │            vault.VaultBackend                       │    │
│  │  backend_kwargs = {                                 │    │
│  │    "connections_path": "airflow/connections",       │    │
│  │    "variables_path": "airflow/variables",           │    │
│  │    "url": "http://vault:8200",                      │    │
│  │    "auth_type": "approle",                          │    │
│  │    "role_id": "...",                                │    │
│  │    "secret_id": "..."                               │    │
│  │  }                                                  │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                             │
│  Code: BaseHook.get_connection("minio_bronze")              │
│        → Same API, but fetches from Vault!                  │
└─────────────────────────────────────────────────────────────┘
```

### Implementation Steps

#### Step 1: Add Vault to Docker Compose

```yaml
# infra-repo/docker-compose.yaml

vault:
  image: hashicorp/vault:1.15
  container_name: vault
  restart: unless-stopped
  ports:
    - "8200:8200"
  environment:
    VAULT_DEV_ROOT_TOKEN_ID: ${VAULT_TOKEN:-root-token}
    VAULT_DEV_LISTEN_ADDRESS: 0.0.0.0:8200
  cap_add:
    - IPC_LOCK
  volumes:
    - ./mnt/data_drive/vault_data:/vault/data
    - ./docker/vault/init-secrets.sh:/vault/init-secrets.sh
  networks:
    - llm-ops-net
  healthcheck:
    test: ["CMD", "vault", "status"]
    interval: 30s
    timeout: 10s
    retries: 3
```

#### Step 2: Create Vault Init Script

```bash
#!/bin/bash
# infra-repo/docker/vault/init-secrets.sh

export VAULT_ADDR="http://127.0.0.1:8200"
export VAULT_TOKEN="${VAULT_TOKEN:-root-token}"

# Enable KV secrets engine
vault secrets enable -path=secret kv-v2 2>/dev/null || true

# Store Airflow connections
vault kv put secret/airflow/connections/minio_bronze \
  conn_type="generic" \
  host="minio" \
  port="9000" \
  login="minioadmin" \
  password="minioadmin" \
  schema="bronze"

vault kv put secret/airflow/connections/minio_silver \
  conn_type="generic" \
  host="minio" \
  port="9000" \
  login="minioadmin" \
  password="minioadmin" \
  schema="silver"

vault kv put secret/airflow/connections/pg_app \
  conn_type="postgres" \
  host="10.130.0.3" \
  port="6003" \
  login="app_admin" \
  password="zyneric" \
  schema="app_data"

echo "Vault secrets initialized!"
```

#### Step 3: Configure Airflow to Use Vault

```bash
# Add to infra-repo/.env

# Vault Configuration
AIRFLOW__SECRETS__BACKEND=airflow.providers.hashicorp.secrets.vault.VaultBackend
AIRFLOW__SECRETS__BACKEND_KWARGS={"connections_path": "airflow/connections", "variables_path": "airflow/variables", "url": "http://vault:8200", "token": "root-token", "mount_point": "secret"}
```

#### Step 4: Install Vault Provider

```txt
# infra-repo/docker/airflow/_config/requirements.txt
apache-airflow-providers-hashicorp>=3.0.0
```

---

## Comparison: Phase 1 vs Phase 2

| Feature | Phase 1: Airflow Connections | Phase 2: Vault |
|---------|------------------------------|----------------|
| **Storage Location** | Airflow Metadata DB | Vault Server |
| **Encryption** | Fernet (symmetric) | Vault (unsealed) |
| **Access Control** | Airflow RBAC | Vault Policies |
| **Audit Trail** | None | Full logging |
| **Credential Rotation** | Manual | Automatic |
| **Multi-Service** | Airflow only | All services |
| **Complexity** | Low | Medium |
| **Setup Time** | 1 hour | 4-8 hours |

---

## Recommended Rollout

```
Week 1: Phase 1 - Airflow Connections
├── Update init_connections.sh
├── Add load_config_from_connection() method
├── Migrate existing DAGs to use connections
└── Test all pipelines

Week 2-3: Phase 2 - Vault (Optional)
├── Deploy Vault container
├── Initialize secrets
├── Configure Airflow secrets backend
├── Test Vault integration
└── Enable audit logging
```

---

## Files to Modify

### Phase 1
| File | Action |
|------|--------|
| `infra-repo/docker/airflow/_config/init_connections.sh` | Update with all connections |
| `data-pipeline-repo/airflow/airflow_modules/airflow_minio.py` | Add `load_config_from_connection()` |
| `data-pipeline-repo/airflow/airflow_jobs/minio_external_job.py` | Use new method |

### Phase 2 (Additional)
| File | Action |
|------|--------|
| `infra-repo/docker-compose.yaml` | Add Vault service |
| `infra-repo/docker/vault/init-secrets.sh` | Create secret init script |
| `infra-repo/.env` | Add Vault config variables |
| `infra-repo/docker/airflow/_config/requirements.txt` | Add hashicorp provider |

---

## Security Considerations

1. **Fernet Key:** Store `AIRFLOW__CORE__FERNET_KEY` securely (not in git)
2. **Vault Token:** Use AppRole auth in production (not root token)
3. **Network:** Vault should only be accessible from internal network
4. **Backup:** Regular backup of Vault data and unseal keys
5. **Rotation:** Set up automatic credential rotation for production

---

## Phase 2 Implementation (Completed)

### Overview

Phase 2 extends the secret management with:
- **HashiCorp Vault** (Port 6060): Centralized system credential storage
- **Keycloak** (Port 6061): User authentication with SSO for Airflow and Grafana

### Architecture Diagram

```
┌────────────────────────────────────────────────────────────────────────┐
│                       PAAS Platform Security                           │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌─────────────────────┐              ┌─────────────────────┐          │
│  │   HashiCorp Vault   │              │      Keycloak       │          │
│  │     (Port 6060)     │              │     (Port 6061)     │          │
│  ├─────────────────────┤              ├─────────────────────┤          │
│  │ System Credentials  │              │ User Authentication │          │
│  │ ─────────────────── │              │ ─────────────────── │          │
│  │ • MinIO keys        │              │ • OIDC Provider     │          │
│  │ • PostgreSQL creds  │              │ • SSO for services  │          │
│  │ • Neo4j password    │              │ • Role-based access │          │
│  │ • API keys          │              │ • User management   │          │
│  │ • Airflow secrets   │              │                     │          │
│  └──────────┬──────────┘              └──────────┬──────────┘          │
│             │                                    │                     │
│             │ Secrets Backend                    │ OIDC Auth           │
│             ▼                                    ▼                     │
│  ┌─────────────────────┐              ┌─────────────────────┐          │
│  │      Airflow        │              │      Grafana        │          │
│  │    (Port 6031)      │              │    (Port 6052)      │          │
│  │  VaultBackend ✓     │              │  OAuth2/OIDC ✓      │          │
│  └─────────────────────┘              └─────────────────────┘          │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

### Files Created

#### Vault Configuration

| File | Purpose |
|------|---------|
| `infra/docker/vault/policies/airflow-policy.hcl` | Vault policy for Airflow read access |
| `infra/docker/vault/scripts/init-secrets.sh` | Initialize all secrets in Vault |
| `infra/docker/vault/scripts/setup-auth.sh` | Configure AppRole authentication |

#### Keycloak Configuration

| File | Purpose |
|------|---------|
| `infra/docker/keycloak/import/paas-realm.json` | Realm with clients, roles, and users |

### Files Modified

| File | Changes |
|------|---------|
| `infra/docker-compose.yaml` | Added Vault, Keycloak-Postgres, Keycloak services |
| `infra/.env` | Added Vault and Keycloak environment variables |
| `infra/docker/airflow/_config/requirements.txt` | Added `apache-airflow-providers-hashicorp>=3.7.0` |
| `infra/docker/airflow/entrypoint-with-init.sh` | Added Vault readiness check |

### Vault Secrets Structure

```
secret/
├── airflow/
│   └── connections/
│       ├── minio_bronze      # MinIO bronze bucket
│       ├── minio_silver      # MinIO silver bucket
│       ├── minio_external    # External MinIO
│       ├── pg_app            # PostgreSQL app database
│       ├── neo4j_default     # Neo4j graph database
│       ├── milvus_default    # Milvus vector database
│       └── spark_default     # Spark master
├── system/
│   ├── minio                 # MinIO root credentials
│   ├── postgres_app          # PostgreSQL app credentials
│   ├── postgres_airflow      # Airflow database credentials
│   ├── neo4j                 # Neo4j credentials
│   └── airflow               # Airflow admin & Fernet key
└── api_keys/
    └── brevo_smtp            # SMTP credentials for alerts
```

### Keycloak Realm: `paas`

#### Clients

| Client | Purpose | Redirect URIs |
|--------|---------|---------------|
| `airflow` | Airflow SSO | `http://localhost:6031/*` |
| `grafana` | Grafana SSO | `http://localhost:6052/*` |

#### Roles

| Role | Description | Grafana Mapping |
|------|-------------|-----------------|
| `admin` | Platform Administrator | Admin |
| `data_engineer` | Pipeline access | Editor |
| `viewer` | Read-only access | Viewer |

#### Default Users

| Username | Password | Role | Notes |
|----------|----------|------|-------|
| `admin` | `admin123` | admin | Temporary password |
| `engineer` | `engineer123` | data_engineer | Temporary password |
| `viewer` | `viewer123` | viewer | Temporary password |

### Environment Variables Added

```bash
# Vault
VAULT_HOST_PORT=6060
VAULT_DEV_ROOT_TOKEN=root-token-change-in-prod
VAULT_ADDR=http://vault:8200

# Keycloak
KEYCLOAK_HOST_PORT=6061
KEYCLOAK_HOSTNAME=localhost
KEYCLOAK_ADMIN_USER=admin
KEYCLOAK_ADMIN_PASSWORD=admin
KEYCLOAK_DB_USER=keycloak
KEYCLOAK_DB_PASSWORD=keycloak
KEYCLOAK_DB_NAME=keycloak

# Client Secrets
KEYCLOAK_AIRFLOW_CLIENT_SECRET=airflow-client-secret-change-in-prod
KEYCLOAK_GRAFANA_CLIENT_SECRET=grafana-client-secret-change-in-prod

# Airflow Vault Backend
AIRFLOW__SECRETS__BACKEND=airflow.providers.hashicorp.secrets.vault.VaultBackend
AIRFLOW__SECRETS__BACKEND_KWARGS={"connections_path": "airflow/connections", ...}
```

### Deployment Steps

```bash
cd /path/to/infra

# 1. Rebuild Airflow image (includes hashicorp provider)
docker compose build airflow-base

# 2. Start Vault
docker compose up -d vault

# 3. Initialize Vault secrets (run once)
docker compose exec vault sh /vault/scripts/init-secrets.sh

# 4. (Optional) Setup AppRole auth for production
docker compose exec vault sh /vault/scripts/setup-auth.sh

# 5. Start Keycloak
docker compose up -d keycloak-postgres keycloak

# 6. Start Airflow with Vault backend
docker compose up -d airflow-webserver airflow-scheduler airflow-dag-processor

# 7. Restart Grafana with OIDC
docker compose up -d grafana
```

### Verification Commands

```bash
# Vault status
docker compose exec vault vault status

# List Vault secrets
docker compose exec vault vault kv list secret/airflow/connections

# Get a specific secret
docker compose exec vault vault kv get secret/system/minio

# Test Airflow connection from Vault
docker compose exec airflow-webserver airflow connections get minio_bronze

# Keycloak health
curl http://localhost:6061/health/ready
```

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Vault UI | http://localhost:6060 | Token: `root-token-change-in-prod` |
| Keycloak Admin | http://localhost:6061 | admin / admin |
| Grafana (SSO) | http://localhost:6052 | Via Keycloak |
| Airflow | http://localhost:6031 | admin / admin123 |

### Production Hardening Checklist

- [ ] Change `VAULT_DEV_ROOT_TOKEN` to a secure value
- [ ] Switch Vault from dev mode to production mode
- [ ] Configure Vault auto-unseal (AWS KMS, Azure Key Vault, etc.)
- [ ] Switch to AppRole authentication (run `setup-auth.sh`)
- [ ] Enable Vault audit logging
- [ ] Change Keycloak admin password
- [ ] Enable HTTPS for Keycloak
- [ ] Change all client secrets in Keycloak
- [ ] Configure Keycloak email verification
- [ ] Set up Vault backup procedures
- [ ] Configure credential rotation policies

### Rollback Procedure

If issues occur, disable Phase 2 integration:

```bash
# 1. Comment out Vault backend in .env
# AIRFLOW__SECRETS__BACKEND=...
# AIRFLOW__SECRETS__BACKEND_KWARGS=...

# 2. Disable Grafana OIDC in docker-compose.yaml
# GF_AUTH_GENERIC_OAUTH_ENABLED: "false"

# 3. Stop Phase 2 services
docker compose stop vault keycloak keycloak-postgres

# 4. Restart affected services
docker compose up -d airflow-webserver airflow-scheduler grafana
```

---

## Summary: Phase 1 vs Phase 2

| Aspect | Phase 1 | Phase 2 |
|--------|---------|---------|
| **System Credentials** | Airflow Connections DB | HashiCorp Vault |
| **User Authentication** | SimpleAuthManager | Keycloak OIDC |
| **SSO** | None | Grafana, Airflow (optional) |
| **Audit Trail** | None | Vault audit logs |
| **Role-Based Access** | Airflow only | Centralized in Keycloak |
| **Secret Rotation** | Manual | Automatic (Vault) |
| **Complexity** | Low | Medium |
| **Services** | 0 additional | 3 (Vault, Keycloak, Keycloak-PG) |

---

## How Vault and Keycloak Work with PAAS Services

### Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                              PAAS Platform                                   │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                    SYSTEM CREDENTIALS (Vault)                          │  │
│  │  ┌─────────────┐                                                       │  │
│  │  │    Vault    │◄──── Stores all service credentials                   │  │
│  │  │  (6060)     │      (MinIO, PostgreSQL, Neo4j, API keys)             │  │
│  │  └──────┬──────┘                                                       │  │
│  │         │                                                              │  │
│  │         │ VaultBackend (auto-fetch secrets)                            │  │
│  │         ▼                                                              │  │
│  │  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐               │  │
│  │  │   Airflow   │────►│    MinIO    │     │  PostgreSQL │               │  │
│  │  │   (6031)    │     │   (6000)    │     │   (6003)    │               │  │
│  │  └─────────────┘     └─────────────┘     └─────────────┘               │  │
│  │         │                                                              │  │
│  │         │ Spark jobs use credentials from Vault                        │  │
│  │         ▼                                                              │  │
│  │  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐               │  │
│  │  │    Spark    │────►│    Neo4j    │     │   Milvus    │               │  │
│  │  │   (6020)    │     │   (6007)    │     │   (6005)    │               │  │
│  │  └─────────────┘     └─────────────┘     └─────────────┘               │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                    USER AUTHENTICATION (Keycloak)                      │  │
│  │  ┌─────────────┐                                                       │  │
│  │  │  Keycloak   │◄──── Central Identity Provider                        │  │
│  │  │   (6061)    │      (Users, Roles, SSO)                              │  │
│  │  └──────┬──────┘                                                       │  │
│  │         │                                                              │  │
│  │         │ OIDC/OAuth2 Authentication                                   │  │
│  │         ▼                                                              │  │
│  │  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐               │  │
│  │  │   Grafana   │     │   Airflow   │     │  Future     │               │  │
│  │  │   (6052)    │     │   (6031)    │     │  Services   │               │  │
│  │  │  SSO ✓      │     │  (optional) │     │             │               │  │
│  │  └─────────────┘     └─────────────┘     └─────────────┘               │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

### 1. HashiCorp Vault - System Credentials Flow

#### What Vault Stores

```
secret/
├── airflow/connections/          # Airflow uses VaultBackend to fetch these
│   ├── minio_bronze             → {host, port, login, password, schema}
│   ├── minio_silver             → {host, port, login, password, schema}
│   ├── pg_app                   → {host, port, login, password, schema}
│   ├── neo4j_default            → {host, port, login, password}
│   ├── milvus_default           → {host, port}
│   └── spark_default            → {host, port}
│
├── system/                       # Raw credentials for direct access
│   ├── minio                    → {root_user, root_password, endpoint}
│   ├── postgres_app             → {user, password, host, port, database}
│   ├── neo4j                    → {user, password, uri}
│   └── airflow                  → {admin_username, admin_password, fernet_key}
│
└── api_keys/                     # External API credentials
    └── brevo_smtp               → {login, key, from_email, to_email}
```

#### How Airflow Fetches Secrets from Vault

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Airflow + Vault Integration                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. STARTUP                                                                 │
│  ┌──────────────────┐                                                       │
│  │ Airflow starts   │                                                       │
│  │ (webserver/      │                                                       │
│  │  scheduler)      │                                                       │
│  └────────┬─────────┘                                                       │
│           │                                                                 │
│           ▼                                                                 │
│  ┌──────────────────┐     ┌──────────────────┐                              │
│  │ entrypoint.sh    │────►│ Wait for Vault   │                              │
│  │ checks Vault     │     │ API to be ready  │                              │
│  └────────┬─────────┘     └──────────────────┘                              │
│           │                                                                 │
│           ▼                                                                 │
│  ┌──────────────────┐                                                       │
│  │ Load VaultBackend│                                                       │
│  │ from env vars:   │                                                       │
│  │ AIRFLOW__SECRETS_│                                                       │
│  │ _BACKEND         │                                                       │
│  └────────┬─────────┘                                                       │
│           │                                                                 │
│  2. DAG EXECUTION                                                           │
│           ▼                                                                 │
│  ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐     │
│  │ DAG calls:       │────►│ VaultBackend     │────►│ Vault API        │     │
│  │ BaseHook.get_    │     │ intercepts       │     │ GET /v1/secret/  │     │
│  │ connection(      │     │                  │     │ data/airflow/    │     │
│  │ "minio_bronze")  │     │                  │     │ connections/     │     │
│  └──────────────────┘     └──────────────────┘     │ minio_bronze     │     │
│                                                    └────────┬─────────┘     │
│                                                             │               │
│                                                             ▼               │
│                                              ┌──────────────────────────┐   │
│                                              │ Returns:                 │   │
│                                              │ {                        │   │
│                                              │   "host": "minio",       │   │
│                                              │   "port": 9000,          │   │
│                                              │   "login": "minioadmin", │   │
│                                              │   "password": "****",    │   │
│                                              │   "schema": "bronze"     │   │
│                                              │ }                        │   │
│                                              └──────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Code Example: DAG Using Vault Secrets

```python
# In your DAG file
from airflow.hooks.base import BaseHook

# This automatically fetches from Vault (not local DB)
conn = BaseHook.get_connection("minio_bronze")

# Use the connection
minio_client = Minio(
    endpoint=f"{conn.host}:{conn.port}",
    access_key=conn.login,        # From Vault
    secret_key=conn.password,     # From Vault (decrypted)
    secure=False
)
```

---

### 2. Keycloak - User Authentication Flow

#### Keycloak Realm Structure

```
Realm: paas
├── Clients
│   ├── grafana          → OAuth2 client for Grafana SSO
│   └── airflow          → OAuth2 client for Airflow (optional)
│
├── Roles
│   ├── admin            → Full platform access
│   ├── data_engineer    → Pipeline and data tools access
│   └── viewer           → Read-only dashboards
│
├── Groups
│   ├── /admins          → Members get "admin" role
│   ├── /engineers       → Members get "data_engineer" role
│   └── /viewers         → Members get "viewer" role
│
└── Users
    ├── admin            → admin role, /admins group
    ├── engineer         → data_engineer role, /engineers group
    └── viewer           → viewer role, /viewers group
```

#### User Login Flow: Grafana SSO

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     User Login Flow (Grafana + Keycloak)                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. USER ACCESSES GRAFANA                                                   │
│  ┌──────────┐     ┌──────────────────┐                                      │
│  │  User    │────►│ http://localhost │                                      │
│  │ Browser  │     │ :6052            │                                      │
│  └──────────┘     └────────┬─────────┘                                      │
│                            │                                                │
│                            ▼                                                │
│  2. GRAFANA REDIRECTS TO KEYCLOAK                                           │
│  ┌──────────────────────────────────────────────────────────────────┐       │
│  │ Redirect to:                                                     │       │
│  │ http://localhost:6061/realms/paas/protocol/openid-connect/auth   │       │
│  │ ?client_id=grafana                                               │       │
│  │ &redirect_uri=http://localhost:6052/login/generic_oauth          │       │
│  │ &response_type=code                                              │       │
│  │ &scope=openid email profile roles                                │       │
│  └──────────────────────────────────────────────────────────────────┘       │
│                            │                                                │
│                            ▼                                                │
│  3. USER SEES KEYCLOAK LOGIN PAGE                                           │
│  ┌──────────────────┐                                                       │
│  │  ┌────────────┐  │                                                       │
│  │  │ PAAS       │  │                                                       │
│  │  │ Platform   │  │                                                       │
│  │  ├────────────┤  │                                                       │
│  │  │ Username:  │  │                                                       │
│  │  │ [admin   ] │  │                                                       │
│  │  │ Password:  │  │                                                       │
│  │  │ [********] │  │                                                       │
│  │  │ [Login]    │  │                                                       │
│  │  └────────────┘  │                                                       │
│  └──────────────────┘                                                       │
│                            │                                                │
│                            ▼                                                │
│  4. KEYCLOAK VALIDATES CREDENTIALS                                          │
│  ┌──────────────────┐     ┌──────────────────┐                              │
│  │ Keycloak checks  │────►│ PostgreSQL       │                              │
│  │ username/password│     │ (keycloak-db)    │                              │
│  └────────┬─────────┘     └──────────────────┘                              │
│           │                                                                 │ 
│           ▼                                                                 │
│  5. KEYCLOAK ISSUES TOKENS                                                  │
│  ┌───────────────────────────────────────────────────────────────────┐      │
│  │ Authorization Code: abc123...                                     │      │
│  │ Redirect to: http://localhost:6052/login/generic_oauth?code=abc123│      │
│  └───────────────────────────────────────────────────────────────────┘      │
│                            │                                                │
│                            ▼                                                │
│  6. GRAFANA EXCHANGES CODE FOR TOKENS                                       │
│  ┌──────────────────┐                      ┌──────────────────┐             │
│  │ Grafana backend  │─── POST ────────────►│ Keycloak         │             │
│  │ /token endpoint  │    code + secret     │ /token           │             │
│  └────────┬─────────┘                      └────────┬─────────┘             │
│           │                                         │                       │
│           │◄────────────────────────────────────────┘                       │
│           │  Returns:                                                       │
│           │  {                                                              │
│           │    "access_token": "eyJhbG...",                                 │
│           │    "id_token": "eyJhbG...",                                     │
│           │    "refresh_token": "eyJhbG..."                                 │
│           │  }                                                              │
│           │                                                                 │
│           ▼                                                                 │
│  7. GRAFANA DECODES TOKEN & MAPS ROLES                                      │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │ ID Token payload:                                                     │  │
│  │ {                                                                     │  │
│  │   "sub": "user-uuid",                                                 │  │
│  │   "preferred_username": "admin",                                      │  │
│  │   "email": "admin@paas.local",                                        │  │
│  │   "roles": ["admin"],           ◄─── Mapped to Grafana "Admin"        │  │
│  │   "groups": ["/admins"]                                               │  │
│  │ }                                                                     │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                            │                                                │
│                            ▼                                                │
│  8. USER IS LOGGED INTO GRAFANA                                             │
│  ┌──────────────────┐                                                       │
│  │ Grafana Dashboard│                                                       │
│  │ ┌──────────────┐ │                                                       │
│  │ │ Welcome,     │ │                                                       │
│  │ │ admin!       │ │                                                       │
│  │ │ Role: Admin  │ │                                                       │
│  │ └──────────────┘ │                                                       │
│  └──────────────────┘                                                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### Role Mapping Configuration

In `docker-compose.yaml`, Grafana uses this JMESPath expression to map Keycloak roles:

```yaml
GF_AUTH_GENERIC_OAUTH_ROLE_ATTRIBUTE_PATH: "contains(roles[*], 'admin') && 'Admin' || contains(roles[*], 'data_engineer') && 'Editor' || 'Viewer'"
```

This maps:
- Keycloak `admin` role → Grafana `Admin`
- Keycloak `data_engineer` role → Grafana `Editor`
- Any other user → Grafana `Viewer`

---

### 3. Complete System Interaction

```
┌────────────────────────────────────────────────────────────────────────┐
│                    Complete Authentication Flow                        │
├────────────────────────────────────────────────────────────────────────┤
│                                                                        │
│  ┌─────────┐                                                           │
│  │  User   │                                                           │
│  └────┬────┘                                                           │
│       │                                                                │
│       │ 1. Access Grafana (http://localhost:6052)                      │
│       ▼                                                                │
│  ┌─────────────────┐                                                   │
│  │    Grafana      │                                                   │
│  │    (6052)       │                                                   │
│  └────────┬────────┘                                                   │
│           │                                                            │
│           │ 2. Redirect to Keycloak for login                          │
│           ▼                                                            │
│  ┌─────────────────┐     ┌─────────────────┐                           │
│  │    Keycloak     │────►│ Keycloak        │                           │
│  │    (6061)       │     │ PostgreSQL      │                           │
│  └────────┬────────┘     └─────────────────┘                           │
│           │                                                            │
│           │ 3. User logs in, gets JWT token with roles                 │
│           ▼                                                            │
│  ┌─────────────────┐                                                   │
│  │    Grafana      │ 4. User sees dashboards based on role             │
│  │  (logged in)    │                                                   │
│  └────────┬────────┘                                                   │
│           │                                                            │
│           │ 5. Dashboard queries Prometheus/data sources               │
│           ▼                                                            │
│  ┌─────────────────┐                                                   │
│  │   Prometheus    │                                                   │
│  │    (6051)       │                                                   │
│  └────────┬────────┘                                                   │
│           │                                                            │
│           │ 6. Prometheus scrapes metrics from services                │
│           ▼                                                            │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Data Platform Services                       │   │
│  │                                                                 │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐             │   │
│  │  │ Airflow │  │  Spark  │  │  MinIO  │  │  Neo4j  │             │   │
│  │  └────┬────┘  └─────────┘  └────┬────┘  └────┬────┘             │   │
│  │       │                         │            │                  │   │
│  │       │ 7. Airflow DAGs fetch   │            │                  │   │
│  │       │    credentials from     │            │                  │   │
│  │       │    Vault                │            │                  │   │
│  │       ▼                         │            │                  │   │
│  │  ┌─────────────────┐            │            │                  │   │
│  │  │     Vault       │────────────┴────────────┘                  │   │
│  │  │    (6060)       │  8. Vault provides credentials             │   │
│  │  │                 │     to access MinIO, Neo4j, etc.           │   │
│  │  └─────────────────┘                                            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

---

### 4. Service Integration Matrix

| Component | Uses Vault | Uses Keycloak | Purpose |
|-----------|------------|---------------|---------|
| **Airflow** | Yes (VaultBackend) | Optional | Fetches connection credentials for DAGs |
| **Grafana** | No | Yes (SSO) | User login with role-based dashboards |
| **Spark** | Indirect (via Airflow) | No | Jobs get credentials from Airflow |
| **MinIO** | No (target) | No | Receives connections using Vault secrets |
| **PostgreSQL** | No (target) | No | Receives connections using Vault secrets |
| **Neo4j** | No (target) | No | Receives connections using Vault secrets |
| **Milvus** | No (target) | No | Receives connections using Vault secrets |
| **Prometheus** | No | No | Scrapes metrics, no auth needed internally |

---

### 5. Key Benefits

#### Vault Benefits
1. **Centralized Secret Management** - Change password in one place, all services get updated
2. **Audit Trail** - Every secret access is logged
3. **Dynamic Secrets** - Can generate short-lived credentials automatically
4. **Encryption at Rest** - All secrets encrypted in storage
5. **Access Policies** - Fine-grained control over who can access what

#### Keycloak Benefits
1. **Single Sign-On (SSO)** - Users log in once, access multiple services
2. **Centralized User Management** - Add/remove users in one place
3. **Role-Based Access Control (RBAC)** - Roles map to application permissions
4. **Identity Federation** - Can connect to LDAP, Active Directory, social logins
5. **Session Management** - Control session timeouts, force logout

---

### 6. Configuration Reference

#### Vault Environment Variables (in `.env`)

```bash
# Vault server
VAULT_HOST_PORT=6060
VAULT_DEV_ROOT_TOKEN=root-token-change-in-prod
VAULT_ADDR=http://vault:8200

# Airflow Vault Backend
AIRFLOW__SECRETS__BACKEND=airflow.providers.hashicorp.secrets.vault.VaultBackend
AIRFLOW__SECRETS__BACKEND_KWARGS={"connections_path": "airflow/connections", "url": "http://vault:8200", "auth_type": "token", "token": "root-token-change-in-prod", "mount_point": "secret"}
```

#### Keycloak Environment Variables (in `.env`)

```bash
# Keycloak server
KEYCLOAK_HOST_PORT=6061
KEYCLOAK_HOSTNAME=localhost
KEYCLOAK_ADMIN_USER=admin
KEYCLOAK_ADMIN_PASSWORD=admin

# Database
KEYCLOAK_DB_USER=keycloak
KEYCLOAK_DB_PASSWORD=keycloak
KEYCLOAK_DB_NAME=keycloak

# Client secrets for SSO
KEYCLOAK_AIRFLOW_CLIENT_SECRET=airflow-client-secret-change-in-prod
KEYCLOAK_GRAFANA_CLIENT_SECRET=grafana-client-secret-change-in-prod
```

#### Grafana OIDC Configuration (in `docker-compose.yaml`)

```yaml
environment:
  GF_AUTH_GENERIC_OAUTH_ENABLED: "true"
  GF_AUTH_GENERIC_OAUTH_NAME: "Keycloak"
  GF_AUTH_GENERIC_OAUTH_CLIENT_ID: "grafana"
  GF_AUTH_GENERIC_OAUTH_CLIENT_SECRET: ${KEYCLOAK_GRAFANA_CLIENT_SECRET}
  GF_AUTH_GENERIC_OAUTH_SCOPES: "openid email profile roles"
  GF_AUTH_GENERIC_OAUTH_AUTH_URL: http://localhost:${KEYCLOAK_HOST_PORT}/realms/paas/protocol/openid-connect/auth
  GF_AUTH_GENERIC_OAUTH_TOKEN_URL: http://keycloak:8080/realms/paas/protocol/openid-connect/token
  GF_AUTH_GENERIC_OAUTH_API_URL: http://keycloak:8080/realms/paas/protocol/openid-connect/userinfo
  GF_AUTH_GENERIC_OAUTH_ROLE_ATTRIBUTE_PATH: "contains(roles[*], 'admin') && 'Admin' || contains(roles[*], 'data_engineer') && 'Editor' || 'Viewer'"
```

---

## AppRole Authentication (Production Recommended)

### Overview

AppRole is Vault's recommended authentication method for machine-to-machine authentication. Unlike token-based auth, AppRole provides:

- **Role-based access**: Each service gets its own role with specific permissions
- **Secret ID rotation**: Secret IDs can be rotated without changing the role
- **Audit trail**: Clear identification of which service accessed which secret
- **TTL management**: Automatic token expiration and renewal

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     AppRole Authentication Flow                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. SETUP (one-time, by admin)                                              │
│  ┌──────────────────┐                                                       │
│  │ Admin runs       │                                                       │
│  │ setup-auth.sh    │                                                       │
│  └────────┬─────────┘                                                       │
│           │                                                                 │
│           ▼                                                                 │
│  ┌──────────────────┐     ┌──────────────────┐                              │
│  │ Vault creates:   │     │ Outputs:         │                              │
│  │ - airflow role   │────►│ - ROLE_ID        │                              │
│  │ - airflow policy │     │ - SECRET_ID      │                              │
│  └──────────────────┘     └──────────────────┘                              │
│                                                                             │
│  2. RUNTIME (automatic, by Airflow)                                         │
│  ┌──────────────────┐                                                       │
│  │ Airflow starts   │                                                       │
│  │ with AppRole     │                                                       │
│  │ credentials      │                                                       │
│  └────────┬─────────┘                                                       │
│           │                                                                 │
│           │ POST /v1/auth/approle/login                                     │
│           │ {role_id, secret_id}                                            │
│           ▼                                                                 │
│  ┌──────────────────┐                                                       │
│  │ Vault returns    │                                                       │
│  │ short-lived      │──────► Token TTL: 1 hour                              │
│  │ client token     │        Max TTL: 4 hours                               │
│  └────────┬─────────┘                                                       │
│           │                                                                 │
│           │ Use token for secret access                                     │
│           ▼                                                                 │
│  ┌──────────────────┐                                                       │
│  │ Access secrets   │                                                       │
│  │ per policy       │                                                       │
│  └──────────────────┘                                                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Important: Dev Mode vs Production Mode

> **⚠️ Dev Mode Limitation**
>
> Vault is currently running in **dev mode** which uses in-memory storage. This means:
> - All secrets and AppRole credentials are **lost on container restart**
> - AppRole credentials (role_id, secret_id) **regenerate on each Vault startup**
> - After Vault restarts, you must update `.env` with new credentials and restart Airflow
>
> **Auto-Initialization:** The custom entrypoint (`docker/vault/entrypoint.sh`) automatically runs `init-secrets.sh` and `setup-auth.sh` on Vault startup, so secrets and AppRole are recreated. However, the credentials will be different each time.

#### Dev Mode Workflow (After Vault Restart)

```bash
# 1. Check new credentials from Vault logs
docker compose logs vault | grep -E "VAULT_(AIRFLOW|GRAFANA)_(ROLE|SECRET)_ID"

# 2. Update .env with new credentials
# Edit VAULT_AIRFLOW_ROLE_ID, VAULT_AIRFLOW_SECRET_ID
# Edit AIRFLOW__SECRETS__BACKEND_KWARGS with new role_id/secret_id

# 3. Restart Airflow to pick up new credentials
docker compose up -d airflow-webserver airflow-scheduler airflow-dag-processor
```

#### Production Mode (Implemented)

Production mode uses file-based storage for persistence. Set in `.env`:

```bash
VAULT_MODE=production
```

**How it works:**
1. On first startup, Vault is automatically initialized
2. Unseal key and root token are saved to `/vault/data/.vault-keys`
3. Vault auto-unseals on subsequent restarts using saved keys
4. Secrets and AppRole credentials persist across restarts

**First Time Setup:**
```bash
# 1. Set production mode in .env
VAULT_MODE=production

# 2. Clear any existing vault data (if switching from dev mode)
rm -rf ./mnt/data_drive/vault_data/*

# 3. Start Vault
docker compose up -d vault

# 4. Check logs for generated credentials
docker compose logs vault | grep -E "(UNSEAL_KEY|ROOT_TOKEN|ROLE_ID|SECRET_ID)"

# 5. Save the credentials securely and update .env
```

**Files created:**
- `docker/vault/config/vault.hcl` - Vault configuration
- `docker/vault/entrypoint.sh` - Handles both dev and production modes
- `mnt/data_drive/vault_data/.vault-keys` - Auto-generated unseal keys (production)
- `mnt/data_drive/vault_data/.initialized` - Flag to prevent re-initialization

**Production mode benefits:**
- Secrets persist across restarts
- AppRole credentials remain valid (no need to update `.env`)
- Automatic unsealing using saved keys
- Data stored in `/vault/data` volume

---

### Setup Steps

#### Step 1: Run AppRole Setup Script

```bash
cd /path/to/infra-repo
docker compose exec vault sh /vault/scripts/setup-auth.sh
```

This script will:
1. Enable AppRole auth method
2. Create `airflow-policy` with read access to connections
3. Create `airflow` AppRole
4. Generate and output `ROLE_ID` and `SECRET_ID`

**Output example:**
```
=== Airflow AppRole Credentials ===
VAULT_AIRFLOW_ROLE_ID=b18732dc-b7eb-e773-29fa-9e766ba87681
VAULT_AIRFLOW_SECRET_ID=16582ee9-cef8-379d-5833-2a5c28d15245

=== Grafana AppRole Credentials ===
VAULT_GRAFANA_ROLE_ID=1e2d5628-5762-3438-37d4-aef76c2569de
VAULT_GRAFANA_SECRET_ID=07dee21c-612d-44dc-2d59-43de297c87b7
```

#### Step 2: Update .env with AppRole Credentials

```bash
# In infra-repo/.env

# Vault section - AppRole credentials for each service
VAULT_AIRFLOW_ROLE_ID=<airflow-role-id>
VAULT_AIRFLOW_SECRET_ID=<airflow-secret-id>
VAULT_GRAFANA_ROLE_ID=<grafana-role-id>
VAULT_GRAFANA_SECRET_ID=<grafana-secret-id>

# Airflow secrets backend (change auth_type from "token" to "approle")
AIRFLOW__SECRETS__BACKEND=airflow.providers.hashicorp.secrets.vault.VaultBackend
AIRFLOW__SECRETS__BACKEND_KWARGS={"connections_path": "airflow/connections", "variables_path": "airflow/variables", "url": "http://vault:8200", "auth_type": "approle", "role_id": "<airflow-role-id>", "secret_id": "<airflow-secret-id>", "mount_point": "secret"}
```

#### Step 3: Restart Airflow Services

```bash
docker compose up -d airflow-webserver airflow-scheduler airflow-dag-processor
```

#### Step 4: Verify AppRole Authentication

```bash
# Check Airflow can fetch connections
docker compose exec airflow-webserver airflow connections get minio_bronze

# Or trigger test DAG
docker compose exec airflow-webserver airflow dags trigger test_keyvault
```

### Airflow Policy (airflow-policy.hcl)

```hcl
# Read Airflow connections
path "secret/data/airflow/connections/*" {
  capabilities = ["read"]
}

# List connections
path "secret/metadata/airflow/connections/*" {
  capabilities = ["list"]
}

# Read Airflow variables
path "secret/data/airflow/variables/*" {
  capabilities = ["read"]
}
```

### Token vs AppRole Comparison

| Feature | Token Auth | AppRole Auth |
|---------|------------|--------------|
| **Security** | Static token, never expires | Short-lived tokens (1h TTL) |
| **Rotation** | Manual token rotation | Automatic token renewal |
| **Audit** | Generic "token" identity | Clear service identification |
| **Policy** | Token inherits all permissions | Role-specific permissions |
| **Best For** | Development | Production |

### Troubleshooting

#### "permission denied" when accessing secrets

1. Check if AppRole is enabled:
   ```bash
   docker compose exec vault vault auth list
   ```

2. Verify role exists:
   ```bash
   docker compose exec vault vault read auth/approle/role/airflow
   ```

3. Check policy is attached:
   ```bash
   docker compose exec vault vault policy read airflow-policy
   ```

#### "invalid role_id" or "invalid secret_id"

1. Regenerate credentials:
   ```bash
   docker compose exec vault sh /vault/scripts/setup-auth.sh
   ```

2. Update `.env` with new credentials

3. Restart Airflow services

#### Token expired

AppRole tokens have a 1-hour TTL. The Airflow Vault provider automatically renews tokens. If you see token expiration errors:

1. Verify Vault is accessible:
   ```bash
   docker compose exec vault vault status
   ```

2. Check network connectivity between Airflow and Vault

---

## How Vault Security Components Work Together

### Overview: The Three Layers of Vault Security

Vault has a layered security model where different credentials serve different purposes:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Vault Security Hierarchy                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  LAYER 1: UNSEAL KEY (Unlock the Vault)                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  🔐 Unseal Key                                                          │    │
│  │  ─────────────────────────────────────────────────────────────────────  │    │
│  │  Purpose: Decrypt the master encryption key                             │    │
│  │  When used: Only at Vault startup (after restart)                       │    │
│  │  Who has it: System administrator (kept offline/secure)                 │    │
│  │                                                                         │    │
│  │  Think of it as: The key to open a bank vault door                      │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                           │                                                     │
│                           │ Unseals                                             │
│                           ▼                                                     │
│  LAYER 2: ROOT TOKEN (Master Access)                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  👑 Root Token                                                          │    │
│  │  ─────────────────────────────────────────────────────────────────────  │    │
│  │  Purpose: Full administrative access to Vault                           │    │
│  │  Capabilities: Create policies, enable auth methods, manage secrets     │    │
│  │  When used: Initial setup, emergency access, admin tasks                │    │
│  │  Who has it: Platform administrator (use sparingly!)                    │    │
│  │                                                                         │    │
│  │  Think of it as: The master key that opens every safety deposit box     │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                           │                                                     │
│                           │ Creates                                             │
│                           ▼                                                     │
│  LAYER 3: AppRole CREDENTIALS (Service Access)                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  🤖 AppRole (Role ID + Secret ID)                                       │    │
│  │  ─────────────────────────────────────────────────────────────────────  │    │
│  │  Purpose: Machine-to-machine authentication with limited permissions    │    │
│  │  Capabilities: Only what the attached policy allows (e.g., read-only)   │    │
│  │  When used: Runtime operations by services (Airflow, Grafana)           │    │
│  │  Who has it: Each service has its own Role ID + Secret ID               │    │
│  │                                                                         │    │
│  │  Think of it as: Employee badges with restricted area access            │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Detailed Component Breakdown

#### 1. Unseal Key - "Opening the Vault"

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Unseal Key Mechanism                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  WHAT HAPPENS AT VAULT STARTUP:                                                 │
│                                                                                 │
│  ┌──────────────┐                      ┌──────────────────────────────────────┐ │
│  │ Vault Server │                      │ Encrypted Storage                    │ │
│  │  (Sealed)    │                      │ ┌──────────────────────────────────┐ │ │
│  │              │                      │ │ 🔒 Encrypted Master Key          │ │ │
│  │  Cannot      │                      │ │    (AES-256-GCM encrypted)       │ │ │
│  │  access      │─────────────────────►│ └──────────────────────────────────┘ │ │
│  │  secrets     │                      │ ┌──────────────────────────────────┐ │ │
│  │              │                      │ │ 🔒 Encrypted Secrets             │ │ │
│  └──────────────┘                      │ │    (MinIO, PostgreSQL, etc.)     │ │ │
│                                        │ └──────────────────────────────────┘ │ │
│         │                              └──────────────────────────────────────┘ │
│         │                                                                       │
│         │  Apply Unseal Key                                                     │
│         ▼                                                                       │
│  ┌──────────────┐                      ┌──────────────────────────────────────┐ │
│  │ Vault Server │                      │ Decrypted in Memory                  │ │
│  │  (Unsealed)  │                      │ ┌──────────────────────────────────┐ │ │
│  │              │                      │ │ 🔓 Master Key (in RAM only)      │ │ │
│  │  Can now     │─────────────────────►│ │    Used to decrypt/encrypt       │ │ │
│  │  access      │                      │ │    all secrets on-the-fly        │ │ │
│  │  secrets     │                      │ └──────────────────────────────────┘ │ │
│  └──────────────┘                      └──────────────────────────────────────┘ │
│                                                                                 │
│  KEY POINTS:                                                                    │
│  • Unseal key is generated ONCE during `vault operator init`                    │
│  • We use 1 key share, 1 threshold (simple setup)                               │
│  • Production: Use 5 shares, 3 threshold (Shamir's secret sharing)              │
│  • Unseal key is NEVER stored in Vault - keep it offline!                       │
│  • Our production mode auto-saves it to /vault/data/.vault-keys                 │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

#### 2. Root Token - "The Master Key"

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Root Token Usage                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  WHAT ROOT TOKEN CAN DO:                                                        │
│                                                                                 │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                          Root Token Powers                               │   │
│  │                                                                          │   │
│  │  ✅ Enable/disable secret engines    (vault secrets enable kv-v2)       │   │
│  │  ✅ Enable/disable auth methods      (vault auth enable approle)        │   │
│  │  ✅ Create/delete policies           (vault policy write airflow-policy)│   │
│  │  ✅ Create/delete AppRoles           (vault write auth/approle/role/...) │   │
│  │  ✅ Read/write ALL secrets           (vault kv put secret/...)          │   │
│  │  ✅ Manage Vault configuration       (vault operator ...)               │   │
│  │  ✅ Create new root tokens           (vault operator generate-root)     │   │
│  │  ✅ Revoke any token                 (vault token revoke)               │   │
│  │                                                                          │   │
│  │  ⚠️  TOO POWERFUL FOR DAILY USE!                                         │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  WHEN TO USE ROOT TOKEN:                                                        │
│                                                                                 │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐                     │
│  │ Initial      │     │ Create       │     │ Emergency    │                     │
│  │ Setup        │     │ AppRoles     │     │ Recovery     │                     │
│  │              │     │              │     │              │                     │
│  │ • Enable KV  │     │ • Create     │     │ • Rotate     │                     │
│  │ • Enable     │     │   policies   │     │   compromised│                     │
│  │   AppRole    │     │ • Generate   │     │   credentials│                     │
│  │ • Store      │     │   role_id    │     │ • Revoke     │                     │
│  │   initial    │     │ • Generate   │     │   tokens     │                     │
│  │   secrets    │     │   secret_id  │     │              │                     │
│  └──────────────┘     └──────────────┘     └──────────────┘                     │
│        ▲                    ▲                    ▲                               │
│        │                    │                    │                               │
│        └────────────────────┴────────────────────┘                               │
│                   Uses Root Token                                               │
│                                                                                 │
│  OUR SETUP:                                                                     │
│  • Root token saved in /vault/data/.vault-keys (production mode)                │
│  • Used by init-secrets.sh and setup-auth.sh scripts                            │
│  • After setup, services use AppRole instead of root token                      │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

#### 3. AppRole - "Employee Badges for Services"

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          AppRole Authentication                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  TWO-PART CREDENTIAL SYSTEM:                                                    │
│                                                                                 │
│  ┌─────────────────────────────┐     ┌─────────────────────────────┐            │
│  │        Role ID              │     │       Secret ID              │            │
│  │  (The "username")           │     │  (The "password")            │            │
│  ├─────────────────────────────┤     ├─────────────────────────────┤            │
│  │                             │     │                             │            │
│  │  • Identifies the role      │     │  • Proves identity          │            │
│  │  • Tied to a policy         │     │  • Can be rotated           │            │
│  │  • Relatively static        │     │  • Can have TTL             │            │
│  │  • Can be shared in config  │     │  • Should be kept secret    │            │
│  │                             │     │                             │            │
│  │  Example:                   │     │  Example:                   │            │
│  │  0d469a99-a88d-ba7e-...     │     │  d73ca8d0-9e87-804c-...     │            │
│  │                             │     │                             │            │
│  └─────────────────────────────┘     └─────────────────────────────┘            │
│              │                                   │                              │
│              └───────────────┬───────────────────┘                              │
│                              │                                                  │
│                              ▼                                                  │
│                    ┌─────────────────┐                                          │
│                    │ Login to Vault  │                                          │
│                    │ POST /v1/auth/  │                                          │
│                    │ approle/login   │                                          │
│                    └────────┬────────┘                                          │
│                             │                                                   │
│                             ▼                                                   │
│                    ┌─────────────────────────────────────────┐                  │
│                    │ Vault Returns Short-Lived Client Token  │                  │
│                    │                                         │                  │
│                    │  {                                      │                  │
│                    │    "client_token": "hvs.CAESI...",      │                  │
│                    │    "token_ttl": 3600,     (1 hour)      │                  │
│                    │    "token_max_ttl": 14400 (4 hours)     │                  │
│                    │  }                                      │                  │
│                    └────────┬────────────────────────────────┘                  │
│                             │                                                   │
│                             ▼                                                   │
│                    ┌─────────────────────────────────────────┐                  │
│                    │ Use Client Token for Secret Access      │                  │
│                    │                                         │                  │
│                    │ GET /v1/secret/data/airflow/connections │                  │
│                    │ Authorization: Bearer hvs.CAESI...      │                  │
│                    └─────────────────────────────────────────┘                  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Complete Lifecycle: From Vault Startup to Secret Access

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Complete Vault Security Lifecycle                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  PHASE 1: VAULT INITIALIZATION (First Time Only)                                │
│  ════════════════════════════════════════════════                               │
│                                                                                 │
│  ┌────────────────┐                                                             │
│  │ vault operator │──────► Generates:                                           │
│  │ init           │        • Unseal Key(s)                                      │
│  └────────────────┘        • Root Token                                         │
│          │                                                                      │
│          ▼                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐             │
│  │ /vault/data/.vault-keys                                        │             │
│  │ ──────────────────────────────────────────────────────────────│             │
│  │ VAULT_UNSEAL_KEY=<base64-encoded-key>                          │             │
│  │ VAULT_ROOT_TOKEN=hvs.xxxxxxxxxxxxx                             │             │
│  └────────────────────────────────────────────────────────────────┘             │
│                                                                                 │
│  PHASE 2: VAULT STARTUP (Every Restart)                                         │
│  ═══════════════════════════════════════                                        │
│                                                                                 │
│  ┌────────────────┐     ┌────────────────┐     ┌────────────────┐              │
│  │ Vault starts   │────►│ Vault is       │────►│ Vault is       │              │
│  │ (Sealed)       │     │ SEALED         │     │ UNSEALED       │              │
│  └────────────────┘     └───────┬────────┘     └────────────────┘              │
│                                 │                                               │
│                    ┌────────────┴────────────┐                                  │
│                    │ vault operator unseal   │                                  │
│                    │ <unseal-key>            │                                  │
│                    └─────────────────────────┘                                  │
│                                                                                 │
│  PHASE 3: INITIAL SETUP (First Time After Init)                                 │
│  ══════════════════════════════════════════════                                 │
│                                                                                 │
│  Using ROOT TOKEN:                                                              │
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │ init-secrets.sh                           setup-auth.sh               │     │
│  │ ─────────────────                         ──────────────              │     │
│  │                                                                        │     │
│  │ 1. Enable KV-v2 engine                    1. Enable AppRole auth      │     │
│  │    vault secrets enable kv-v2                vault auth enable approle│     │
│  │                                                                        │     │
│  │ 2. Store secrets                          2. Create policies           │     │
│  │    vault kv put secret/system/minio          vault policy write ...    │     │
│  │    vault kv put secret/airflow/...                                     │     │
│  │                                           3. Create AppRoles           │     │
│  │                                              vault write auth/approle/ │     │
│  │                                              role/airflow ...          │     │
│  │                                                                        │     │
│  │                                           4. Generate credentials      │     │
│  │                                              ROLE_ID=xxx               │     │
│  │                                              SECRET_ID=yyy             │     │
│  └────────────────────────────────────────────────────────────────────────┘     │
│                                                                                 │
│  PHASE 4: RUNTIME OPERATIONS (Ongoing)                                          │
│  ═════════════════════════════════════                                          │
│                                                                                 │
│  Using AppRole (NOT Root Token):                                                │
│                                                                                 │
│  ┌──────────────────┐                                                           │
│  │ Airflow starts   │                                                           │
│  │ with AppRole     │                                                           │
│  │ credentials      │                                                           │
│  └────────┬─────────┘                                                           │
│           │                                                                     │
│           │  1. Login with Role ID + Secret ID                                  │
│           ▼                                                                     │
│  ┌──────────────────┐                                                           │
│  │ Vault returns    │                                                           │
│  │ client token     │                                                           │
│  │ (1 hour TTL)     │                                                           │
│  └────────┬─────────┘                                                           │
│           │                                                                     │
│           │  2. Fetch secrets using client token                                │
│           ▼                                                                     │
│  ┌──────────────────┐     ┌──────────────────┐                                  │
│  │ GET connection   │────►│ MinIO credential │                                  │
│  │ minio_bronze     │     │ {login, password}│                                  │
│  └──────────────────┘     └──────────────────┘                                  │
│           │                                                                     │
│           │  3. Token auto-renews before expiry                                 │
│           ▼                                                                     │
│  ┌──────────────────┐                                                           │
│  │ Continue working │                                                           │
│  │ (token renewed)  │                                                           │
│  └──────────────────┘                                                           │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Security Best Practices

| Component | Storage Location | Who Should Access | Rotation Frequency |
|-----------|-----------------|-------------------|-------------------|
| **Unseal Key** | Offline/HSM/secure backup | Only sysadmins (emergency) | Never (unless compromised) |
| **Root Token** | Secure vault, revoke after setup | Platform admin only | Use once, revoke if possible |
| **AppRole Role ID** | Config files, env vars | Deployment system | Rarely (tied to role) |
| **AppRole Secret ID** | Secure env vars, injected at runtime | Service only | Monthly or on compromise |
| **Client Token** | Memory only (never persisted) | Service runtime | Auto-renewed (1h TTL) |

### Our Production Setup Summary

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     PAAS Platform Vault Configuration                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Storage: File-based (/vault/data)                                              │
│  Mode: Production (VAULT_MODE=production)                                       │
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │ Auto-saved Credentials                                                 │     │
│  │ Location: /vault/data/.vault-keys                                      │     │
│  │                                                                         │     │
│  │ VAULT_UNSEAL_KEY=<auto-generated>                                       │     │
│  │ VAULT_ROOT_TOKEN=<auto-generated>                                       │     │
│  └────────────────────────────────────────────────────────────────────────┘     │
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────┐     │
│  │ Service AppRoles (from .env)                                           │     │
│  │                                                                         │     │
│  │ Airflow:                                                                │     │
│  │   VAULT_AIRFLOW_ROLE_ID=<from setup-auth.sh>                            │     │
│  │   VAULT_AIRFLOW_SECRET_ID=<from setup-auth.sh>                          │     │
│  │   Policy: airflow-policy (read airflow/connections/*)                   │     │
│  │                                                                         │     │
│  │ Grafana:                                                                │     │
│  │   VAULT_GRAFANA_ROLE_ID=<from setup-auth.sh>                            │     │
│  │   VAULT_GRAFANA_SECRET_ID=<from setup-auth.sh>                          │     │
│  │   Policy: grafana-policy (read system/*, api_keys/*)                    │     │
│  └────────────────────────────────────────────────────────────────────────┘     │
│                                                                                 │
│  Automatic Behaviors:                                                           │
│  ✅ Auto-init on first startup                                                  │
│  ✅ Auto-unseal on restart (uses saved unseal key)                              │
│  ✅ Auto-run init-secrets.sh (once, creates .initialized flag)                  │
│  ✅ Auto-run setup-auth.sh (once, generates AppRole credentials)                │
│  ✅ Secrets persist across restarts                                             │
│  ✅ AppRole credentials persist across restarts                                 │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Keycloak Per-Service Role-Based Access Control (RBAC)

### Overview

Each service has its own set of roles defined as **client roles** in Keycloak. This allows granular access control where a user can have different permission levels across different services.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    Per-Service Role Assignment Example                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  User: mixed_user                                                               │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                                                                          │   │
│  │  Airflow:  [Admin]     ─── Full access to DAGs, connections, admin       │   │
│  │  Grafana:  [Viewer]    ─── Read-only access to dashboards                │   │
│  │  MinIO:    [readonly]  ─── Can only read objects                         │   │
│  │  Spark:    [viewer]    ─── Can only view job status                      │   │
│  │                                                                          │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                 │
│  Same user, different permissions per service!                                  │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Client Roles by Service

#### Airflow Roles
| Role | Description | Permissions |
|------|-------------|-------------|
| `Admin` | Full administrator | Manage connections, variables, users, DAGs |
| `Op` | Operator | Trigger DAGs, view logs, pause/unpause |
| `User` | Standard user | View DAGs, trigger allowed DAGs |
| `Viewer` | Read-only | View DAG status and history |
| `Public` | Minimal | Very limited access |

#### Grafana Roles
| Role | Description | Permissions |
|------|-------------|-------------|
| `Admin` | Full administrator | Manage users, data sources, dashboards |
| `Editor` | Dashboard editor | Create and edit dashboards |
| `Viewer` | Read-only | View dashboards only |

#### MinIO Roles
| Role | Description | Permissions |
|------|-------------|-------------|
| `admin` | Full access | All bucket and object operations |
| `readwrite` | Read/Write | Read and write objects |
| `readonly` | Read-only | Read objects only |

#### Spark Roles
| Role | Description | Permissions |
|------|-------------|-------------|
| `admin` | Full access | Submit and manage all jobs |
| `user` | Standard user | Submit and view own jobs |
| `viewer` | Read-only | View job status only |

### Predefined User Groups

Groups automatically assign roles across all services:

| Group | Airflow | Grafana | MinIO | Spark |
|-------|---------|---------|-------|-------|
| `/platform-admins` | Admin | Admin | admin | admin |
| `/data-engineers` | Op | Editor | readwrite | user |
| `/analysts` | Viewer | Editor | readonly | viewer |
| `/viewers` | Viewer | Viewer | readonly | viewer |

### Default Users

| Username | Password | Group | Special Notes |
|----------|----------|-------|---------------|
| `admin` | `admin123` | platform-admins | Full access everywhere |
| `engineer` | `engineer123` | data-engineers | Operator access |
| `analyst` | `analyst123` | analysts | Viewer in Airflow, Editor in Grafana |
| `viewer` | `viewer123` | viewers | Read-only everywhere |
| `mixed_user` | `mixed123` | (none) | Admin in Airflow, Viewer in Grafana |

### Token Structure

When a user logs in, the JWT token contains service-specific role claims:

```json
{
  "preferred_username": "mixed_user",
  "email": "mixed@paas.local",
  "airflow_roles": ["Admin"],
  "grafana_roles": ["Viewer"],
  "policy": ["readonly"],
  "spark_roles": ["viewer"],
  "groups": []
}
```

### Service Integration

#### Grafana Integration (Implemented)

Grafana uses the `grafana_roles` claim for role mapping:

```yaml
# docker-compose.yaml
GF_AUTH_GENERIC_OAUTH_ROLE_ATTRIBUTE_PATH: "contains(grafana_roles[*], 'Admin') && 'Admin' || contains(grafana_roles[*], 'Editor') && 'Editor' || 'Viewer'"
```

#### Airflow Integration (Optional)

To enable Keycloak SSO for Airflow, add to docker-compose.yaml:

```yaml
# Airflow webserver environment
environment:
  # Enable OAuth
  AIRFLOW__WEBSERVER__AUTH_BACKEND: airflow.providers.fab.auth_manager.fab_auth_manager
  AIRFLOW__WEBSERVER__AUTHENTICATE: "True"

  # OAuth Configuration (using Flask-AppBuilder)
  AIRFLOW__WEBSERVER__RBAC: "True"
  AUTH_TYPE: AUTH_OAUTH
  OAUTH_PROVIDERS: |
    [{
      "name": "keycloak",
      "token_key": "access_token",
      "icon": "fa-key",
      "remote_app": {
        "client_id": "airflow",
        "client_secret": "${KEYCLOAK_AIRFLOW_CLIENT_SECRET}",
        "api_base_url": "http://keycloak:8080/realms/paas/protocol/openid-connect",
        "access_token_url": "http://keycloak:8080/realms/paas/protocol/openid-connect/token",
        "authorize_url": "http://localhost:6061/realms/paas/protocol/openid-connect/auth",
        "request_token_url": null,
        "client_kwargs": {"scope": "openid email profile"}
      }
    }]
```

And create `webserver_config.py`:

```python
from flask_appbuilder.security.manager import AUTH_OAUTH
import os

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Viewer"

OAUTH_PROVIDERS = [{
    'name': 'keycloak',
    'token_key': 'access_token',
    'icon': 'fa-key',
    'remote_app': {
        'client_id': 'airflow',
        'client_secret': os.environ.get('KEYCLOAK_AIRFLOW_CLIENT_SECRET'),
        'api_base_url': 'http://keycloak:8080/realms/paas/protocol/openid-connect',
        'access_token_url': 'http://keycloak:8080/realms/paas/protocol/openid-connect/token',
        'authorize_url': 'http://localhost:6061/realms/paas/protocol/openid-connect/auth',
        'client_kwargs': {'scope': 'openid email profile airflow_roles'},
    }
}]

# Map Keycloak roles to Airflow roles
AUTH_ROLES_MAPPING = {
    "Admin": ["Admin"],
    "Op": ["Op"],
    "User": ["User"],
    "Viewer": ["Viewer"],
    "Public": ["Public"],
}

# Sync roles on each login
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_USER_REGISTRATION_ROLE_JMESPATH = "airflow_roles[0]"
```

#### MinIO Integration

MinIO uses the `policy` claim to map OIDC roles to MinIO policies:

```bash
# Configure MinIO OIDC
mc admin config set myminio identity_openid \
  config_url="http://keycloak:8080/realms/paas/.well-known/openid-configuration" \
  client_id="minio" \
  client_secret="${KEYCLOAK_MINIO_CLIENT_SECRET}" \
  claim_name="policy" \
  redirect_uri="http://localhost:6001/oauth_callback"
```

### Managing Users

#### Add New User via Keycloak Admin Console

1. Access Keycloak Admin: `http://localhost:6061/admin`
2. Login with admin credentials
3. Select `paas` realm
4. Go to `Users` → `Add User`
5. Fill in user details
6. Go to `Role Mapping` tab
7. Select `Filter by clients`
8. Assign roles for each client (Airflow, Grafana, MinIO, Spark)

#### Add User to Group

1. Go to user's `Groups` tab
2. Click `Join Group`
3. Select the appropriate group
4. User inherits all group's client roles

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Keycloak Per-Service RBAC Flow                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌──────────────────┐                                                           │
│  │      User        │                                                           │
│  │   (mixed_user)   │                                                           │
│  └────────┬─────────┘                                                           │
│           │                                                                     │
│           │ 1. Login to Keycloak                                                │
│           ▼                                                                     │
│  ┌──────────────────┐                                                           │
│  │    Keycloak      │                                                           │
│  │  ┌────────────┐  │                                                           │
│  │  │ Client     │  │  Looks up user's roles per client:                        │
│  │  │ Roles DB   │  │  • airflow: Admin                                         │
│  │  │            │  │  • grafana: Viewer                                        │
│  │  └────────────┘  │  • minio: readonly                                        │
│  └────────┬─────────┘  • spark: viewer                                          │
│           │                                                                     │
│           │ 2. Issue JWT with client-specific role claims                       │
│           ▼                                                                     │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │                              JWT Token                                   │   │
│  │  {                                                                       │   │
│  │    "preferred_username": "mixed_user",                                   │   │
│  │    "airflow_roles": ["Admin"],                                           │   │
│  │    "grafana_roles": ["Viewer"],                                          │   │
│  │    "policy": ["readonly"],        ← MinIO uses this claim                │   │
│  │    "spark_roles": ["viewer"]                                             │   │
│  │  }                                                                       │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│           │                                                                     │
│           │ 3. User accesses services with JWT                                  │
│           ▼                                                                     │
│  ┌───────────────────┐  ┌───────────────────┐  ┌───────────────────┐           │
│  │     Airflow       │  │     Grafana       │  │      MinIO        │           │
│  │  ┌─────────────┐  │  │  ┌─────────────┐  │  │  ┌─────────────┐  │           │
│  │  │ Reads claim │  │  │  │ Reads claim │  │  │  │ Reads claim │  │           │
│  │  │ airflow_    │  │  │  │ grafana_    │  │  │  │ policy      │  │           │
│  │  │ roles       │  │  │  │ roles       │  │  │  │             │  │           │
│  │  └─────────────┘  │  │  └─────────────┘  │  │  └─────────────┘  │           │
│  │                   │  │                   │  │                   │           │
│  │  Result: Admin    │  │  Result: Viewer   │  │  Result: readonly │           │
│  │  (Full access)    │  │  (Read-only)      │  │  (Read-only)      │           │
│  └───────────────────┘  └───────────────────┘  └───────────────────┘           │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Deployment Steps

```bash
# 1. Remove existing Keycloak data to import new realm
docker compose down keycloak
rm -rf ./mnt/data_drive/keycloak_data/postgres/*

# 2. Start Keycloak services
docker compose up -d keycloak-postgres
docker compose up -d keycloak

# 3. Wait for realm import
docker compose logs -f keycloak | grep -i "import"

# 4. Restart Grafana to pick up new role mapping
docker compose restart grafana

# 5. Verify: Login to Grafana as different users
# - admin/admin123 → Should be Admin
# - analyst/analyst123 → Should be Editor
# - viewer/viewer123 → Should be Viewer
# - mixed_user/mixed123 → Should be Viewer (Airflow Admin but Grafana Viewer)
```

### Verification Commands

```bash
# Check Keycloak realm imported
docker compose exec keycloak /opt/keycloak/bin/kcadm.sh get realms/paas -s localhost:8080

# Get user's token and decode it
TOKEN=$(curl -s -X POST "http://localhost:6061/realms/paas/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "client_id=grafana" \
  -d "client_secret=grafana-client-secret-change-in-prod" \
  -d "username=mixed_user" \
  -d "password=mixed123" \
  -d "grant_type=password" | jq -r '.access_token')

# Decode JWT payload (base64)
echo $TOKEN | cut -d'.' -f2 | base64 -d 2>/dev/null | jq .
```

---

## Airflow 3.x Keycloak SSO Integration (Implemented)

### Summary

Successfully integrated Airflow 3.x with Keycloak SSO using the native **Keycloak Auth Manager** provider.

### Configuration Changes

#### 1. Added Keycloak Provider Package
```txt
# requirements.txt
apache-airflow-providers-keycloak>=0.3.0
```

#### 2. Environment Variables (.env)
```bash
# Auth Manager: Keycloak Auth Manager (Native SSO)
AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.keycloak.auth_manager.keycloak_auth_manager.KeycloakAuthManager
AIRFLOW__KEYCLOAK_AUTH_MANAGER__CLIENT_ID=${KEYCLOAK_AIRFLOW_CLIENT_ID}
AIRFLOW__KEYCLOAK_AUTH_MANAGER__CLIENT_SECRET=${KEYCLOAK_AIRFLOW_CLIENT_SECRET}
AIRFLOW__KEYCLOAK_AUTH_MANAGER__REALM=${KEYCLOAK_REALM}
AIRFLOW__KEYCLOAK_AUTH_MANAGER__SERVER_URL=http://<HOST_IP>:${KEYCLOAK_HOST_PORT}
```

#### 3. Docker Compose (x-airflow-env)
```yaml
AIRFLOW__KEYCLOAK_AUTH_MANAGER__CLIENT_ID: ${KEYCLOAK_AIRFLOW_CLIENT_ID}
AIRFLOW__KEYCLOAK_AUTH_MANAGER__CLIENT_SECRET: ${KEYCLOAK_AIRFLOW_CLIENT_SECRET}
AIRFLOW__KEYCLOAK_AUTH_MANAGER__REALM: ${KEYCLOAK_REALM}
AIRFLOW__KEYCLOAK_AUTH_MANAGER__SERVER_URL: http://10.30.0.11:${KEYCLOAK_HOST_PORT}
```

### Key Learnings

| Issue | Solution |
|-------|----------|
| FAB Auth Manager OAuth button not showing in Airflow 3.x | Use native Keycloak Auth Manager instead |
| `localhost` URL doesn't work from Docker container | Use host machine IP (e.g., `10.30.0.11`) |
| Config section is `keycloak_auth_manager`, not `keycloak` | Use `AIRFLOW__KEYCLOAK_AUTH_MANAGER__*` env vars |

### Auth Manager Options

```
Option 1: SimpleAuth (local users)
  AIRFLOW__CORE__AUTH_MANAGER=<not set or SimpleAuthManager>
  AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin

Option 2: FAB Auth Manager (Keycloak OAuth) - NOT RECOMMENDED for Airflow 3.x
  AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
  AIRFLOW__WEBSERVER__CONFIG_FILE=/opt/airflow/_config/webserver_config.py

Option 3: Keycloak Auth Manager (Native SSO) - RECOMMENDED for Airflow 3.x
  AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.keycloak.auth_manager.keycloak_auth_manager.KeycloakAuthManager
  AIRFLOW__KEYCLOAK_AUTH_MANAGER__CLIENT_ID=airflow
  AIRFLOW__KEYCLOAK_AUTH_MANAGER__CLIENT_SECRET=<secret>
  AIRFLOW__KEYCLOAK_AUTH_MANAGER__REALM=paas
  AIRFLOW__KEYCLOAK_AUTH_MANAGER__SERVER_URL=http://<HOST_IP>:6061
```

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     Airflow 3.x + Keycloak SSO Flow                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌──────────────┐                      ┌──────────────────┐                     │
│  │   Browser    │ ─── 1. Access ────▶  │  Airflow 3.x     │                     │
│  │              │                      │  (api-server)    │                     │
│  └──────────────┘                      └────────┬─────────┘                     │
│         │                                       │                               │
│         │                           2. Redirect to Keycloak                     │
│         │                                       │                               │
│         │                                       ▼                               │
│         │                              ┌──────────────────┐                     │
│         └──── 3. Login ──────────────▶ │    Keycloak      │                     │
│                                        │  (10.30.0.11:6061)│                     │
│         ┌──── 4. JWT Token ◀────────── │                  │                     │
│         │                              └──────────────────┘                     │
│         │                                                                       │
│         │ 5. Callback with token                                                │
│         ▼                                                                       │
│  ┌──────────────┐                      ┌──────────────────┐                     │
│  │   Browser    │ ─── 6. Authorized ─▶ │  Airflow 3.x     │                     │
│  │  (with JWT)  │                      │  (with roles)    │                     │
│  └──────────────┘                      └──────────────────┘                     │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Deployment Steps

```bash
# 1. Rebuild Airflow image with Keycloak provider
docker compose build airflow-base

# 2. Recreate Airflow containers
docker compose up -d --force-recreate airflow-webserver airflow-scheduler

# 3. Verify login redirects to Keycloak
curl -L http://localhost:6031/auth/login

# 4. Access Airflow UI in browser
# - Go to http://localhost:6031
# - Will redirect to Keycloak login
# - Login with Keycloak user (e.g., all_admin)
```

### Keycloak User Role Mapping

Ensure users in Keycloak have the `airflow` client roles assigned:

| Keycloak Role | Airflow Permission |
|--------------|-------------------|
| Admin | Full access to DAGs, connections, admin |
| Op | Trigger DAGs, view logs |
| User | Standard access |
| Viewer | Read-only |
| Public | Minimal access |

### Keycloak Authorization Services (Critical!)

Airflow's Keycloak Auth Manager uses Keycloak's **Authorization Services** to check permissions. Without proper configuration, users will get **403 Forbidden** errors even after successful login.

#### The Problem

```
Authentication ✓ → User logs in successfully via Keycloak SSO
Authorization ✗ → 403 Forbidden when accessing DAGs, pools, etc.
```

The realm JSON must include `authorizationSettings` for the airflow client with:
1. **Resources** - What can be accessed (Dag, Pool, Connection, etc.)
2. **Scopes** - Actions that can be performed (GET, POST, PUT, DELETE, LIST, MENU)
3. **Policies** - Who has access (role-based policies linked to client roles)
4. **Permissions** - Rules linking resources, scopes, and policies

#### Solution

The `paas-realm.json` now includes full `authorizationSettings` in the airflow client definition with:
- Role-based policies for Admin, Op, User, Viewer roles
- Permissions linking scopes to policies
- Default policy for authenticated users

#### Key Learnings - Authorization

| Issue | Root Cause | Solution |
|-------|-----------|----------|
| 403 Forbidden after login | Missing authorization policies in Keycloak | Add `authorizationSettings` to paas-realm.json with role-based policies |
| `airflow keycloak-auth-manager create-all` doesn't fix 403 | Command creates resources/permissions but NOT policies | Manually create role-based policies linking client roles to permissions |
| "Client does not support permissions" | `authorizationServicesEnabled` is false | Set `authorizationServicesEnabled: true` and `serviceAccountsEnabled: true` |

#### Manual Fix (if needed)

If you encounter 403 Forbidden after deploying:

```bash
# 1. Login to Keycloak Admin CLI
docker exec keycloak /opt/keycloak/bin/kcadm.sh config credentials \
  --server http://localhost:8080 --realm master --user admin --password admin

# 2. Get airflow client ID
AIRFLOW_CLIENT_ID=$(docker exec keycloak /opt/keycloak/bin/kcadm.sh get clients -r paas \
  -q clientId=airflow --fields id | grep '"id"' | cut -d'"' -f4)

# 3. Create Admin Role Policy
docker exec keycloak /opt/keycloak/bin/kcadm.sh create \
  clients/$AIRFLOW_CLIENT_ID/authz/resource-server/policy/role -r paas \
  -s name="Admin Role Policy" \
  -s description="Policy for users with Admin client role" \
  -s logic="POSITIVE" \
  -s 'roles=[{"id":"airflow/Admin","required":true}]'

# 4. Update Default Permission to use the policy
ADMIN_POLICY_ID=$(docker exec keycloak /opt/keycloak/bin/kcadm.sh get \
  clients/$AIRFLOW_CLIENT_ID/authz/resource-server/policy -r paas \
  -q name="Admin Role Policy" --fields id | grep '"id"' | cut -d'"' -f4)

docker exec keycloak /opt/keycloak/bin/kcadm.sh update \
  clients/$AIRFLOW_CLIENT_ID/authz/resource-server/permission/resource/DEFAULT_PERM_ID \
  -r paas -s "policies=[\"$ADMIN_POLICY_ID\"]"
```

### Troubleshooting

```bash
# Check Airflow auth manager configuration
docker exec airflow-webserver env | grep -i keycloak

# Check Airflow logs for auth errors
docker logs airflow-webserver 2>&1 | grep -i -E "(keycloak|auth|error)"

# Test Keycloak connectivity from container
docker exec airflow-webserver curl -s http://10.30.0.11:6061/realms/paas/.well-known/openid-configuration

# Check if authorization policies exist in Keycloak
docker exec keycloak /opt/keycloak/bin/kcadm.sh config credentials \
  --server http://localhost:8080 --realm master --user admin --password admin
AIRFLOW_CLIENT_ID=$(docker exec keycloak /opt/keycloak/bin/kcadm.sh get clients -r paas \
  -q clientId=airflow --fields id | grep '"id"' | cut -d'"' -f4)
docker exec keycloak /opt/keycloak/bin/kcadm.sh get \
  clients/$AIRFLOW_CLIENT_ID/authz/resource-server/policy -r paas

# If no role policies exist (Admin Role Policy, etc.), authorization is not properly configured
```

---

## MinIO SSO with Keycloak (OIDC) - Implemented

### Problem

MinIO does not support overriding individual OIDC endpoints (token, JWKS, userinfo).
It fetches the discovery document from `CONFIG_URL` and uses the returned URLs verbatim.
When Keycloak's `KC_HOSTNAME=http://localhost:6061` with `KC_HOSTNAME_STRICT=true`,
the discovery document returns `localhost:6061` URLs — unreachable from inside the MinIO container.

### Solution

Add `KC_HOSTNAME_BACKCHANNEL_DYNAMIC=true` to Keycloak while keeping `KC_HOSTNAME_STRICT=true`.
This makes Keycloak return URLs based on the request's Host header for backend (container-to-container) calls,
while browser requests still get `localhost:6061` URLs.

- **MinIO** calls `keycloak:8080` → discovery doc returns `keycloak:8080` endpoints (reachable via Docker DNS)
- **Browser** calls `localhost:6061` → discovery doc returns `localhost:6061` endpoints (reachable from host)

> **Important:** Setting `KC_HOSTNAME_STRICT=false` together with `KC_HOSTNAME_BACKCHANNEL_DYNAMIC=true`
> causes conflicts. Keep `KC_HOSTNAME_STRICT=true`.

### Keycloak Configuration Change

```yaml
# docker-compose.yaml - keycloak service
environment:
  KC_HOSTNAME: http://${KEYCLOAK_HOSTNAME}:${KEYCLOAK_HOST_PORT}
  KC_HOSTNAME_STRICT: "true"              # keep true
  KC_HOSTNAME_BACKCHANNEL_DYNAMIC: "true"  # added for container-to-container OIDC
```

### MinIO OIDC Configuration

```yaml
# docker-compose.yaml - minio service
environment:
  MINIO_IDENTITY_OPENID_CONFIG_URL: ${KEYCLOAK_BASE_URL}/realms/${KEYCLOAK_REALM}/.well-known/openid-configuration
  MINIO_IDENTITY_OPENID_CLIENT_ID: minio
  MINIO_IDENTITY_OPENID_CLIENT_SECRET: ${KEYCLOAK_MINIO_CLIENT_SECRET}
  MINIO_IDENTITY_OPENID_CLAIM_NAME: policy
  MINIO_IDENTITY_OPENID_DISPLAY_NAME: "Login with Keycloak"
  MINIO_IDENTITY_OPENID_SCOPES: "openid,email,profile"
  MINIO_IDENTITY_OPENID_REDIRECT_URI: http://localhost:${MINIO_UI_HOST_PORT}/oauth_callback
depends_on:
  keycloak:
    condition: service_healthy
```

### Keycloak Client Configuration (paas-realm.json)

- **Client ID:** `minio`
- **Client Secret:** `${KEYCLOAK_MINIO_CLIENT_SECRET}` (in .env)
- **Redirect URIs:** `http://localhost:6001/*`, `http://minio:9001/*`
- **Client Roles:** `consoleAdmin`, `readwrite`, `readonly` (match MinIO built-in policy names)
- **Protocol Mapper:** `minio-policy-claim` — maps client roles to `policy` claim in JWT token

### Role-to-Policy Mapping (Keycloak Groups → MinIO Policies)

| Keycloak Group     | MinIO Client Role | MinIO Policy   |
|--------------------|-------------------|----------------|
| platform-admins    | consoleAdmin      | Full admin     |
| data-engineers     | readwrite         | Read & write   |
| analysts           | readonly          | Read only      |
| viewers            | readonly          | Read only      |

### Startup Order

MinIO depends on Keycloak being healthy. In `setup-infra.sh`, MinIO starts in Stage 3b (after Keycloak in Stage 3).

### Test Users

| Username   | Password     | MinIO Policy   |
|------------|-------------|----------------|
| admin      | admin123    | consoleAdmin   |
| engineer   | engineer123 | readwrite      |
| analyst    | analyst123  | readonly       |
| viewer     | viewer123   | readonly       |

### Unsupported MinIO OIDC Env Vars (Do NOT Use)

These env vars do NOT exist in MinIO and will cause startup failure:
- `MINIO_IDENTITY_OPENID_TOKEN_ENDPOINT`
- `MINIO_IDENTITY_OPENID_USERINFO_ENDPOINT`
- `MINIO_IDENTITY_OPENID_JWKS_URL`
