# Vault Production Configuration
# Uses file storage for persistence across restarts

# Storage backend - file-based for persistence
storage "file" {
  path = "/vault/data"
}

# Listener configuration
listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = true  # Enable TLS in production with proper certs
}

# API address for Vault cluster communication
api_addr = "http://vault:8200"

# Disable memory locking (enable in production with proper capabilities)
disable_mlock = true

# UI access
ui = true

# Log level
log_level = "info"
