# HashiCorp Vault + Keycloak Implementation Plan

## Overview

This plan implements centralized secret management and user authentication for the PAAS platform:

- **HashiCorp Vault** (Port 6060): Centralized system credential storage for all services
- **Keycloak** (Port 6061): User identity management with SSO for Grafana and Airflow

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│                         PAAS Platform Security                         │
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

---

## PAAS Platform Integration

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

## 1. HashiCorp Vault

### Vault Secrets Structure

```
secret/
├── airflow/connections/          # Airflow VaultBackend auto-fetches these
│   ├── minio_bronze             → {host, port, login, password, schema}
│   ├── minio_silver             → {host, port, login, password, schema}
│   ├── minio_external           → {host, port, login, password, schema}
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

### How Airflow Fetches Secrets

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

### DAG Usage Example

```python
from airflow.hooks.base import BaseHook

# Automatically fetches from Vault via VaultBackend
conn = BaseHook.get_connection("minio_bronze")

minio_client = Minio(
    endpoint=f"{conn.host}:{conn.port}",
    access_key=conn.login,     # From Vault
    secret_key=conn.password,  # From Vault (decrypted)
    secure=False
)
```

---

## 2. Keycloak

### Realm: `paas`

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

#### Clients

| Client | Purpose | Redirect URIs |
|--------|---------|---------------|
| `grafana` | Grafana SSO | `http://localhost:6052/*` |
| `airflow` | Airflow SSO | `http://localhost:6031/*` |

#### Roles

| Role | Description | Grafana Mapping |
|------|-------------|-----------------|
| `admin` | Platform Administrator | Admin |
| `data_engineer` | Pipeline access | Editor |
| `viewer` | Read-only access | Viewer |

#### Default Users

| Username | Password | Role |
|----------|----------|------|
| `admin` | `admin123` | admin |
| `engineer` | `engineer123` | data_engineer |
| `viewer` | `viewer123` | viewer |

### User Login Flow (Grafana SSO)

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

### Role Mapping (Grafana `docker-compose.yaml`)

```yaml
GF_AUTH_GENERIC_OAUTH_ROLE_ATTRIBUTE_PATH: >
  "contains(roles[*], 'admin') && 'Admin'
  || contains(roles[*], 'data_engineer') && 'Editor'
  || 'Viewer'"
```

| Keycloak Role | Grafana Role |
|---------------|-------------|
| `admin` | Admin |
| `data_engineer` | Editor |
| *(any other)* | Viewer |

---

## 3. Complete Authentication Flow

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

## 4. Service Integration Matrix

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

## 5. Files Created

### Vault

| File | Purpose |
|------|---------|
| `infra/docker/vault/policies/airflow-policy.hcl` | Vault policy for Airflow read access |
| `infra/docker/vault/scripts/init-secrets.sh` | Initialize all secrets in Vault |
| `infra/docker/vault/scripts/setup-auth.sh` | Configure AppRole authentication |

### Keycloak

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
| `infra/scripts/setup-infra.sh` | Full setup automation including Vault init + Keycloak start |

---

## 6. Configuration Reference

### Vault (`.env`)

```bash
VAULT_HOST_PORT=6060
VAULT_DEV_ROOT_TOKEN=root-token-change-in-prod
VAULT_ADDR=http://vault:8200

AIRFLOW__SECRETS__BACKEND=airflow.providers.hashicorp.secrets.vault.VaultBackend
AIRFLOW__SECRETS__BACKEND_KWARGS={"connections_path": "airflow/connections", "url": "http://vault:8200", "auth_type": "token", "token": "root-token-change-in-prod", "mount_point": "secret"}
```

### Keycloak (`.env`)

```bash
KEYCLOAK_HOST_PORT=6061
KEYCLOAK_HOSTNAME=localhost
KEYCLOAK_ADMIN_USER=admin
KEYCLOAK_ADMIN_PASSWORD=admin
KEYCLOAK_DB_USER=keycloak
KEYCLOAK_DB_PASSWORD=keycloak
KEYCLOAK_DB_NAME=keycloak

KEYCLOAK_AIRFLOW_CLIENT_SECRET=airflow-client-secret-change-in-prod
KEYCLOAK_GRAFANA_CLIENT_SECRET=grafana-client-secret-change-in-prod
```

### Grafana OIDC (`docker-compose.yaml`)

```yaml
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

## 7. Deployment

### Full Setup (via `setup-infra.sh`)

```bash
cd infra
bash scripts/setup-infra.sh
```

This script handles the full ordered startup automatically:
1. Stops all containers and cleans data directories
2. Builds Spark and Airflow base images
3. Starts storage services (MinIO, PostgreSQL, Etcd)
4. Starts databases (Milvus, Neo4j)
5. Starts Vault → initializes secrets
6. Starts Keycloak PostgreSQL → Keycloak
7. Starts Spark cluster
8. Starts Airflow (with VaultBackend)
9. Starts monitoring stack (Prometheus, Grafana)

### Manual Deployment

```bash
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

---

## 8. Verification

```bash
# Vault status
docker compose exec vault vault status

# List Vault secrets
docker compose exec vault vault kv list secret/airflow/connections

# Get a specific secret
docker compose exec vault vault kv get secret/system/minio

# Test Airflow connection fetched from Vault
docker compose exec airflow-webserver airflow connections get minio_bronze

# Keycloak health
curl http://localhost:6061/health/ready
```

---

## 9. Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Vault UI | http://localhost:6060 | Token: `VAULT_DEV_ROOT_TOKEN` from `.env` |
| Keycloak Admin | http://localhost:6061 | admin / admin |
| Grafana (SSO) | http://localhost:6052 | Via Keycloak |
| Airflow | http://localhost:6031 | admin / admin123 |

---

## 10. Production Hardening

- [ ] Change `VAULT_DEV_ROOT_TOKEN` to a secure value
- [ ] Switch Vault from dev mode to production mode
- [ ] Configure Vault auto-unseal (AWS KMS, Azure Key Vault, etc.)
- [ ] Switch to AppRole authentication (`setup-auth.sh`)
- [ ] Enable Vault audit logging
- [ ] Change Keycloak admin password
- [ ] Enable HTTPS for Keycloak
- [ ] Change all client secrets in Keycloak
- [ ] Configure Keycloak email verification
- [ ] Set up Vault backup procedures
- [ ] Configure credential rotation policies

---

## 11. Rollback

```bash
# 1. Comment out Vault backend in .env
# AIRFLOW__SECRETS__BACKEND=...
# AIRFLOW__SECRETS__BACKEND_KWARGS=...

# 2. Disable Grafana OIDC in docker-compose.yaml
# GF_AUTH_GENERIC_OAUTH_ENABLED: "false"

# 3. Stop security services
docker compose stop vault keycloak keycloak-postgres

# 4. Restart affected services
docker compose up -d airflow-webserver airflow-scheduler grafana
```
