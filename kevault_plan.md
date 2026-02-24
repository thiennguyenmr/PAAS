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
