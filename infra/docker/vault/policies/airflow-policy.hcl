# Vault Policy for Airflow
# Allows Airflow to read secrets from Vault

# Allow Airflow to read its connections
path "secret/data/airflow/connections/*" {
  capabilities = ["read", "list"]
}

# Allow Airflow to read variables
path "secret/data/airflow/variables/*" {
  capabilities = ["read", "list"]
}

# Allow reading system credentials
path "secret/data/system/*" {
  capabilities = ["read"]
}

# Allow reading API keys
path "secret/data/api_keys/*" {
  capabilities = ["read"]
}

# Allow metadata operations for listing
path "secret/metadata/airflow/*" {
  capabilities = ["list"]
}

path "secret/metadata/airflow/connections/*" {
  capabilities = ["list"]
}

path "secret/metadata/airflow/variables/*" {
  capabilities = ["list"]
}
