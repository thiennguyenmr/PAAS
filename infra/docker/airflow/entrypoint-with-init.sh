#!/usr/bin/env bash
set -e

: "${AIRFLOW_HOME:=/opt/airflow}"

# Required variables
: "${AIRFLOW_POSTGRES_HOST:?AIRFLOW_POSTGRES_HOST is not set}"
: "${AIRFLOW_POSTGRES_PORT:?AIRFLOW_POSTGRES_PORT is not set}"
: "${AIRFLOW_POSTGRES_DB:?AIRFLOW_POSTGRES_DB is not set}"
: "${AIRFLOW_POSTGRES_USER:?AIRFLOW_POSTGRES_USER is not set}"
: "${AIRFLOW_POSTGRES_PASSWORD:?AIRFLOW_POSTGRES_PASSWORD is not set}"

: "${AIRFLOW_ADMIN_USERNAME:?AIRFLOW_ADMIN_USERNAME is not set}"
: "${AIRFLOW_ADMIN_PASSWORD:?AIRFLOW_ADMIN_PASSWORD is not set}"
: "${AIRFLOW_ADMIN_EMAIL:?AIRFLOW_ADMIN_EMAIL is not set}"
: "${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE:=/opt/airflow/simple_auth_manager_passwords.json.generated}"

PASSWORD_FILE="${AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE}"

# Only initialize when running webserver or api-server
if [ "$1" = "webserver" ] || [ "$1" = "api-server" ]; then

  echo "Waiting for PostgreSQL at ${AIRFLOW_POSTGRES_HOST}:${AIRFLOW_POSTGRES_PORT}..."
  until nc -z "$AIRFLOW_POSTGRES_HOST" "$AIRFLOW_POSTGRES_PORT"; do
    sleep 2
  done

  echo "Checking if database '${AIRFLOW_POSTGRES_DB}' exists..."
  export PGPASSWORD="${AIRFLOW_POSTGRES_PASSWORD}"

  DB_EXISTS=$(psql \
    -h "$AIRFLOW_POSTGRES_HOST" \
    -p "$AIRFLOW_POSTGRES_PORT" \
    -U "$AIRFLOW_POSTGRES_USER" \
    -d postgres \
    -tAc "SELECT 1 FROM pg_database WHERE datname='${AIRFLOW_POSTGRES_DB}'" || echo "")

  if [ "$DB_EXISTS" != "1" ]; then
    echo "Database '${AIRFLOW_POSTGRES_DB}' does NOT exist. Creating..."
    psql \
      -h "$AIRFLOW_POSTGRES_HOST" \
      -p "$AIRFLOW_POSTGRES_PORT" \
      -U "$AIRFLOW_POSTGRES_USER" \
      -d postgres \
      -c "CREATE DATABASE \"${AIRFLOW_POSTGRES_DB}\" OWNER \"${AIRFLOW_POSTGRES_USER}\";"
    echo "Database '${AIRFLOW_POSTGRES_DB}' created."
  else
    echo "Database '${AIRFLOW_POSTGRES_DB}' already exists."
  fi

  # Run DB migration once
  INIT_MARKER="${AIRFLOW_HOME}/.db-initialized"

  if [ ! -f "${INIT_MARKER}" ]; then
    echo "Running Airflow database migration..."
    airflow db migrate
    touch "${INIT_MARKER}"
    echo "Airflow DB initialization completed."
  else
    echo "Initialization skipped — database already initialized."
  fi

  # Always (re)write SimpleAuth password file with our credentials
  echo "Writing SimpleAuth password file at ${PASSWORD_FILE}..."
  mkdir -p "$(dirname "${PASSWORD_FILE}")"
  cat > "${PASSWORD_FILE}" <<EOF
{"${AIRFLOW_ADMIN_USERNAME}": "${AIRFLOW_ADMIN_PASSWORD}"}
EOF
  echo "Password file written."
fi

# init spark default connection
echo ">>> Init airflow connections"
bash /opt/airflow/connections/init_connections.sh || true

echo "Starting Airflow: airflow $*"
exec airflow "$@"
