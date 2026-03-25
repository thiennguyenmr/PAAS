#!/bin/bash
# Setup Keycloak realm with groups, roles, and users
# This script uses Keycloak Admin CLI to configure the paas realm

set -e

# Load environment variables
if [ -f .env ]; then
    source .env
fi

KEYCLOAK_URL="http://localhost:${KEYCLOAK_HOST_PORT:-6061}"
ADMIN_USER="${KEYCLOAK_ADMIN_USER:-admin}"
ADMIN_PASSWORD="${KEYCLOAK_ADMIN_PASSWORD:-admin}"
REALM="paas"

echo "=== Keycloak Setup Script ==="
echo "URL: $KEYCLOAK_URL"
echo "Realm: $REALM"

# Get admin access token
echo ""
echo "1. Getting admin access token..."
ACCESS_TOKEN=$(curl -s -X POST "${KEYCLOAK_URL}/realms/master/protocol/openid-connect/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=${ADMIN_USER}" \
  -d "password=${ADMIN_PASSWORD}" \
  -d "grant_type=password" \
  -d "client_id=admin-cli" | jq -r '.access_token')

if [ "$ACCESS_TOKEN" == "null" ] || [ -z "$ACCESS_TOKEN" ]; then
    echo "ERROR: Failed to get access token. Check admin credentials."
    exit 1
fi
echo "   Access token obtained."

# Function to make API calls
keycloak_api() {
    local method=$1
    local endpoint=$2
    local data=$3

    if [ -n "$data" ]; then
        curl -s -X "$method" "${KEYCLOAK_URL}/admin/realms/${REALM}${endpoint}" \
          -H "Authorization: Bearer $ACCESS_TOKEN" \
          -H "Content-Type: application/json" \
          -d "$data"
    else
        curl -s -X "$method" "${KEYCLOAK_URL}/admin/realms/${REALM}${endpoint}" \
          -H "Authorization: Bearer $ACCESS_TOKEN" \
          -H "Content-Type: application/json"
    fi
}

# Check if realm exists, create if not
echo ""
echo "2. Checking/Creating realm..."
REALM_EXISTS=$(curl -s -o /dev/null -w "%{http_code}" "${KEYCLOAK_URL}/admin/realms/${REALM}" \
  -H "Authorization: Bearer $ACCESS_TOKEN")

if [ "$REALM_EXISTS" != "200" ]; then
    echo "   Creating realm: $REALM"
    curl -s -X POST "${KEYCLOAK_URL}/admin/realms" \
      -H "Authorization: Bearer $ACCESS_TOKEN" \
      -H "Content-Type: application/json" \
      -d '{
        "realm": "paas",
        "enabled": true,
        "displayName": "PAAS Platform",
        "registrationAllowed": false,
        "loginWithEmailAllowed": true,
        "resetPasswordAllowed": true
      }'
    echo "   Realm created."
else
    echo "   Realm '$REALM' already exists."
fi

# Create realm role
echo ""
echo "3. Creating realm roles..."
keycloak_api POST "/roles" '{"name": "platform_admin", "description": "Platform Administrator"}'
echo "   Created: platform_admin"

# Get client IDs and create client roles
echo ""
echo "4. Creating clients and client roles..."

# Create Airflow client
echo "   Creating client: airflow"
keycloak_api POST "/clients" '{
  "clientId": "airflow",
  "name": "Apache Airflow",
  "enabled": true,
  "clientAuthenticatorType": "client-secret",
  "secret": "'"${KEYCLOAK_AIRFLOW_CLIENT_SECRET:-airflow-client-secret-change-in-prod}"'",
  "redirectUris": ["http://localhost:6031/*"],
  "webOrigins": ["http://localhost:6031"],
  "standardFlowEnabled": true,
  "directAccessGrantsEnabled": true,
  "publicClient": false,
  "protocol": "openid-connect"
}'

# Get airflow client ID
AIRFLOW_CLIENT_ID=$(keycloak_api GET "/clients?clientId=airflow" | jq -r '.[0].id')
echo "   Airflow client ID: $AIRFLOW_CLIENT_ID"

# Create Airflow roles
for role in Admin Op User Viewer Public; do
    keycloak_api POST "/clients/${AIRFLOW_CLIENT_ID}/roles" "{\"name\": \"$role\"}"
    echo "   Created airflow role: $role"
done

# Create Grafana client
echo "   Creating client: grafana"
keycloak_api POST "/clients" '{
  "clientId": "grafana",
  "name": "Grafana Monitoring",
  "enabled": true,
  "clientAuthenticatorType": "client-secret",
  "secret": "'"${KEYCLOAK_GRAFANA_CLIENT_SECRET:-grafana-client-secret-change-in-prod}"'",
  "redirectUris": ["http://localhost:6052/*"],
  "webOrigins": ["http://localhost:6052"],
  "standardFlowEnabled": true,
  "publicClient": false,
  "protocol": "openid-connect"
}'

GRAFANA_CLIENT_ID=$(keycloak_api GET "/clients?clientId=grafana" | jq -r '.[0].id')
echo "   Grafana client ID: $GRAFANA_CLIENT_ID"

for role in Admin Editor Viewer; do
    keycloak_api POST "/clients/${GRAFANA_CLIENT_ID}/roles" "{\"name\": \"$role\"}"
    echo "   Created grafana role: $role"
done

# Create MinIO client
echo "   Creating client: minio"
keycloak_api POST "/clients" '{
  "clientId": "minio",
  "name": "MinIO Object Storage",
  "enabled": true,
  "clientAuthenticatorType": "client-secret",
  "secret": "'"${KEYCLOAK_MINIO_CLIENT_SECRET:-minio-client-secret-change-in-prod}"'",
  "redirectUris": ["http://localhost:6001/*"],
  "webOrigins": ["http://localhost:6001"],
  "standardFlowEnabled": true,
  "directAccessGrantsEnabled": true,
  "publicClient": false,
  "protocol": "openid-connect"
}'

MINIO_CLIENT_ID=$(keycloak_api GET "/clients?clientId=minio" | jq -r '.[0].id')
echo "   MinIO client ID: $MINIO_CLIENT_ID"

for role in consoleAdmin readwrite readonly; do
    keycloak_api POST "/clients/${MINIO_CLIENT_ID}/roles" "{\"name\": \"$role\"}"
    echo "   Created minio role: $role"
done

# Create Spark client
echo "   Creating client: spark"
keycloak_api POST "/clients" '{
  "clientId": "spark",
  "name": "Apache Spark",
  "enabled": true,
  "clientAuthenticatorType": "client-secret",
  "secret": "'"${KEYCLOAK_SPARK_CLIENT_SECRET:-spark-client-secret-change-in-prod}"'",
  "redirectUris": ["http://localhost:6020/*", "http://localhost:6025/*", "http://localhost:6026/*"],
  "webOrigins": ["http://localhost:6020", "http://localhost:6025", "http://localhost:6026"],
  "standardFlowEnabled": true,
  "directAccessGrantsEnabled": true,
  "publicClient": false,
  "protocol": "openid-connect"
}'

SPARK_CLIENT_ID=$(keycloak_api GET "/clients?clientId=spark" | jq -r '.[0].id')
echo "   Spark client ID: $SPARK_CLIENT_ID"

for role in admin user viewer; do
    keycloak_api POST "/clients/${SPARK_CLIENT_ID}/roles" "{\"name\": \"$role\"}"
    echo "   Created spark role: $role"
done

# Create groups
echo ""
echo "5. Creating groups..."
for group in platform-admins data-engineers analysts viewers; do
    keycloak_api POST "/groups" "{\"name\": \"$group\"}"
    echo "   Created group: $group"
done

# Create users
echo ""
echo "6. Creating users..."

create_user() {
    local username=$1
    local email=$2
    local firstName=$3
    local lastName=$4
    local password=$5

    echo "   Creating user: $username"
    keycloak_api POST "/users" "{
      \"username\": \"$username\",
      \"email\": \"$email\",
      \"firstName\": \"$firstName\",
      \"lastName\": \"$lastName\",
      \"enabled\": true,
      \"emailVerified\": true,
      \"credentials\": [{
        \"type\": \"password\",
        \"value\": \"$password\",
        \"temporary\": false
      }]
    }"

    # Get user ID
    local user_id=$(keycloak_api GET "/users?username=$username" | jq -r '.[0].id')
    echo "   User ID: $user_id"
    echo "$user_id"
}

# Create admin user
ADMIN_USER_ID=$(create_user "admin" "admin@paas.local" "Platform" "Admin" "admin123")

# Create engineer user
ENGINEER_USER_ID=$(create_user "engineer" "engineer@paas.local" "Data" "Engineer" "engineer123")

# Create analyst user
ANALYST_USER_ID=$(create_user "analyst" "analyst@paas.local" "Data" "Analyst" "analyst123")

# Create viewer user
VIEWER_USER_ID=$(create_user "viewer" "viewer@paas.local" "Dashboard" "Viewer" "viewer123")

# Setup Airflow Authorization Services
echo ""
echo "7. Setting up Airflow Authorization Services..."

# Enable authorization services for Airflow client
echo "   Enabling authorization services..."
keycloak_api PUT "/clients/${AIRFLOW_CLIENT_ID}" '{
  "serviceAccountsEnabled": true,
  "authorizationServicesEnabled": true
}'

# Create role-based policies for Airflow
echo "   Creating authorization policies..."

# Get authorization resource server settings
AUTH_SETTINGS=$(keycloak_api GET "/clients/${AIRFLOW_CLIENT_ID}/authz/resource-server")

if [ -n "$AUTH_SETTINGS" ]; then
    # Create Admin Role Policy
    keycloak_api POST "/clients/${AIRFLOW_CLIENT_ID}/authz/resource-server/policy/role" '{
      "name": "Admin Role Policy",
      "description": "Policy for users with Admin client role - full access",
      "logic": "POSITIVE",
      "decisionStrategy": "UNANIMOUS",
      "roles": [{"id": "airflow/Admin", "required": true}]
    }'
    echo "   Created: Admin Role Policy"

    # Create Op Role Policy
    keycloak_api POST "/clients/${AIRFLOW_CLIENT_ID}/authz/resource-server/policy/role" '{
      "name": "Op Role Policy",
      "description": "Policy for users with Op client role - operator access",
      "logic": "POSITIVE",
      "decisionStrategy": "UNANIMOUS",
      "roles": [{"id": "airflow/Op", "required": true}]
    }'
    echo "   Created: Op Role Policy"

    # Create User Role Policy
    keycloak_api POST "/clients/${AIRFLOW_CLIENT_ID}/authz/resource-server/policy/role" '{
      "name": "User Role Policy",
      "description": "Policy for users with User client role - basic user access",
      "logic": "POSITIVE",
      "decisionStrategy": "UNANIMOUS",
      "roles": [{"id": "airflow/User", "required": true}]
    }'
    echo "   Created: User Role Policy"

    # Create Viewer Role Policy
    keycloak_api POST "/clients/${AIRFLOW_CLIENT_ID}/authz/resource-server/policy/role" '{
      "name": "Viewer Role Policy",
      "description": "Policy for users with Viewer client role - read-only access",
      "logic": "POSITIVE",
      "decisionStrategy": "UNANIMOUS",
      "roles": [{"id": "airflow/Viewer", "required": true}]
    }'
    echo "   Created: Viewer Role Policy"

    # Create Default Policy (grants access to all authenticated users)
    keycloak_api POST "/clients/${AIRFLOW_CLIENT_ID}/authz/resource-server/policy/js" '{
      "name": "Default Policy",
      "description": "A policy that grants access only for users within this realm",
      "logic": "POSITIVE",
      "decisionStrategy": "AFFIRMATIVE",
      "code": "// by default, grants any permission associated with this policy\n$evaluation.grant();\n"
    }'
    echo "   Created: Default Policy"

    echo "   Authorization policies created successfully."
    echo ""
    echo "   NOTE: Run 'airflow keycloak-auth-manager create-all' inside the Airflow container"
    echo "         to create the required resources and permissions, then link the policies."
else
    echo "   WARNING: Could not access authorization settings. You may need to configure manually."
fi

echo ""
echo "=== Keycloak Setup Complete ==="
echo ""
echo "Users created (password = username + '123'):"
echo "  - admin (Platform Admin)"
echo "  - engineer (Data Engineer)"
echo "  - analyst (Analyst)"
echo "  - viewer (Viewer)"
echo ""
echo "Next steps:"
echo "  1. Go to ${KEYCLOAK_URL}/admin"
echo "  2. Select 'paas' realm"
echo "  3. Assign roles to users via Users -> [user] -> Role mapping"
echo "  4. Or assign users to groups via Users -> [user] -> Groups"
echo ""
echo "For Airflow Keycloak Auth Manager:"
echo "  1. Authorization policies have been created automatically"
echo "  2. Run this command in Airflow container to create resources/permissions:"
echo "     docker exec airflow-webserver airflow keycloak-auth-manager create-all \\"
echo "       --username admin --password admin --user-realm master --client-id admin-cli"
echo "  3. Then link policies to permissions in Keycloak Admin Console:"
echo "     Clients -> airflow -> Authorization -> Permissions -> Edit each permission -> Add policies"
echo ""
