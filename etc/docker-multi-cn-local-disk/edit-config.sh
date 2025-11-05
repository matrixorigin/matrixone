#!/bin/bash
# Interactive configuration editor for specific services
# Usage: ./edit-config.sh [SERVICE]
# SERVICE: cn1, cn2, proxy, log, tn, or common (for common settings)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get service prefix
get_prefix() {
    case "$1" in
        cn1) echo "CN1" ;;
        cn2) echo "CN2" ;;
        proxy) echo "PROXY" ;;
        log) echo "LOG" ;;
        tn) echo "TN" ;;
        common) echo "" ;;
        *) echo "" ;;
    esac
}

# Get service name in uppercase
get_service_upper() {
    case "$1" in
        cn1) echo "CN1" ;;
        cn2) echo "CN2" ;;
        proxy) echo "PROXY" ;;
        log) echo "LOG" ;;
        tn) echo "TN" ;;
        common) echo "COMMON" ;;
        *) echo "$1" | tr '[:lower:]' '[:upper:]' ;;
    esac
}

# Get configuration default value
get_default() {
    case "$1" in
        LOG_LEVEL) echo "info" ;;
        LOG_FORMAT) echo "console" ;;
        LOG_MAX_SIZE) echo "512" ;;
        MEMORY_CACHE) echo "512MB" ;;
        DISK_CACHE) echo "8GB" ;;
        CACHE_DIR) echo "mo-data/file-service-cache" ;;
        DISABLE_TRACE) echo "true" ;;
        DISABLE_METRIC) echo "true" ;;
        *) echo "" ;;
    esac
}

# Get configuration description
get_description() {
    case "$1" in
        LOG_LEVEL) echo "Log verbosity (debug, info, warn, error, fatal)" ;;
        LOG_FORMAT) echo "Log output format (console, json)" ;;
        LOG_MAX_SIZE) echo "Log file max size in MB" ;;
        MEMORY_CACHE) echo "FileService memory cache size (e.g., 512MB, 2GB, 4GB)" ;;
        DISK_CACHE) echo "FileService disk cache size (e.g., 8GB, 20GB, 50GB)" ;;
        CACHE_DIR) echo "FileService cache directory path" ;;
        DISABLE_TRACE) echo "Disable distributed tracing (true, false)" ;;
        DISABLE_METRIC) echo "Disable metrics collection (true, false)" ;;
        *) echo "" ;;
    esac
}

# Parse arguments
SERVICE="${1:-}"
if [ -z "$SERVICE" ]; then
    echo "Usage: $0 <service>"
    echo ""
    echo "Available services:"
    echo "  cn1      - Configure CN1 (Compute Node 1)"
    echo "  cn2      - Configure CN2 (Compute Node 2)"
    echo "  proxy    - Configure Proxy service"
    echo "  log      - Configure Log service"
    echo "  tn       - Configure TN (Transaction Node)"
    echo "  common   - Configure common settings (applies to all services)"
    echo ""
    echo "Example: $0 cn1"
    exit 1
fi

# Validate service
case "$SERVICE" in
    cn1|cn2|proxy|log|tn|common) ;;
    *)
        echo "Error: Invalid service '$SERVICE'"
        echo "Valid services: cn1, cn2, proxy, log, tn, common"
        exit 1
        ;;
esac

PREFIX=$(get_prefix "$SERVICE")
SERVICE_UPPER=$(get_service_upper "$SERVICE")

# Create temporary file for editing
TEMP_FILE=$(mktemp)
trap "rm -f $TEMP_FILE" EXIT

# Configuration keys
CONFIG_KEYS="LOG_LEVEL LOG_FORMAT LOG_MAX_SIZE MEMORY_CACHE DISK_CACHE CACHE_DIR DISABLE_TRACE DISABLE_METRIC"

# Generate header
cat > "$TEMP_FILE" << EOF
# Configuration Editor for $SERVICE_UPPER
# ============================================================
# 
# Instructions:
#   - Lines starting with # are DISABLED (default values shown)
#   - Remove # to ENABLE and customize the value
#   - Save and close to apply changes
# 
# Configuration Priority:
#   1. Service-specific (${PREFIX}_*) - highest priority
#   2. Common configuration (no prefix) - applies to all services
#   3. Default values (shown in comments) - fallback
# ============================================================

EOF

# Add service-specific note
if [ "$SERVICE" != "common" ]; then
    cat >> "$TEMP_FILE" << EOF
# NOTE: These settings apply ONLY to $SERVICE_UPPER
# To set common defaults for all services, use: ./edit-config.sh common
# 
EOF
fi

# Read current config.env if exists
if [ -f "config.env" ]; then
    # Load into temporary variables
    while IFS='=' read -r key value; do
        # Skip comments and empty lines
        [[ "$key" =~ ^#.*$ ]] && continue
        [[ -z "$key" ]] && continue
        # Remove quotes
        value="${value%\"}"
        value="${value#\"}"
        # Export for use in subshells
        export "CURRENT_$key=$value"
    done < config.env
fi

# Generate configuration lines for each key
for key in $CONFIG_KEYS; do
    default=$(get_default "$key")
    description=$(get_description "$key")
    
    # Determine the full variable name
    if [ "$SERVICE" = "common" ]; then
        var_name="$key"
    else
        var_name="${PREFIX}_${key}"
    fi
    
    # Check if this config is already set
    current_var="CURRENT_$var_name"
    current_value="${!current_var:-}"
    
    echo "" >> "$TEMP_FILE"
    echo "# $description" >> "$TEMP_FILE"
    
    if [ -n "$current_value" ]; then
        # Already set - show without comment
        echo "${var_name}=${current_value}" >> "$TEMP_FILE"
    else
        # Not set - show as comment with default value
        echo "# Default: $default" >> "$TEMP_FILE"
        echo "#${var_name}=$default" >> "$TEMP_FILE"
    fi
done

# Add footer with examples
cat >> "$TEMP_FILE" << EOF


# ============================================================
# Examples:
# ============================================================
EOF

if [ "$SERVICE" = "common" ]; then
    cat >> "$TEMP_FILE" << EOF
# 
# Example 1: Enable debug logging for all services
# LOG_LEVEL=debug
# LOG_FORMAT=json
# 
# Example 2: Increase cache for all services
# MEMORY_CACHE=2GB
# DISK_CACHE=20GB
# 
# Example 3: Enable monitoring
# DISABLE_TRACE=false
# DISABLE_METRIC=false
EOF
else
    cat >> "$TEMP_FILE" << EOF
# 
# Example 1: Increase memory cache for $SERVICE_UPPER only
# ${PREFIX}_MEMORY_CACHE=2GB
# ${PREFIX}_DISK_CACHE=20GB
# 
# Example 2: Enable debug logging for $SERVICE_UPPER only
# ${PREFIX}_LOG_LEVEL=debug
# ${PREFIX}_LOG_FORMAT=json
# 
# Example 3: Mix with common settings
# LOG_LEVEL=info              (applies to all services)
# ${PREFIX}_LOG_LEVEL=debug   (overrides for $SERVICE_UPPER only)
EOF
fi

echo "" >> "$TEMP_FILE"
echo "# Save and close this file to apply changes" >> "$TEMP_FILE"

# Open in editor
EDITOR="${EDITOR:-vim}"
if ! command -v "$EDITOR" &> /dev/null; then
    EDITOR="vi"
fi

echo -e "${BLUE}Opening configuration editor for $SERVICE_UPPER...${NC}"
echo -e "${YELLOW}Editor: $EDITOR${NC}"
echo ""

"$EDITOR" "$TEMP_FILE"

# Parse edited file and update config.env
echo ""
echo -e "${BLUE}Processing changes...${NC}"

# Create backup
if [ -f "config.env" ]; then
    cp config.env config.env.bak
fi

# Create new config file
NEW_CONFIG=$(mktemp)

# Add header
cat > "$NEW_CONFIG" << EOF
# MatrixOne Configuration
# Generated by edit-config.sh
# Last edited: $(date)

EOF

# Copy existing config, excluding settings for this service
if [ -f "config.env" ]; then
    while IFS= read -r line; do
        # Skip empty lines and pure comments
        if [[ -z "$line" ]] || [[ "$line" =~ ^[[:space:]]*# ]]; then
            continue
        fi
        
        # Extract variable name
        if [[ "$line" =~ ^([A-Z0-9_]+)= ]]; then
            var="${BASH_REMATCH[1]}"
            
            # Skip if this variable belongs to the service we're editing
            keep_line=true
            if [ "$SERVICE" = "common" ]; then
                # For common, skip base variables (no prefix)
                for key in $CONFIG_KEYS; do
                    if [ "$var" = "$key" ]; then
                        keep_line=false
                        break
                    fi
                done
            else
                # For specific service, skip if it matches the prefix
                if [[ "$var" =~ ^${PREFIX}_ ]]; then
                    keep_line=false
                fi
            fi
            
            if [ "$keep_line" = true ]; then
                echo "$line" >> "$NEW_CONFIG"
            fi
        fi
    done < config.env
    
    echo "" >> "$NEW_CONFIG"
fi

# Add section header
if [ "$SERVICE" = "common" ]; then
    echo "# Common Configuration (applies to all services)" >> "$NEW_CONFIG"
else
    echo "# $SERVICE_UPPER Configuration" >> "$NEW_CONFIG"
fi

# Parse edited temp file and add active settings
CHANGES_MADE=false
while IFS= read -r line; do
    # Skip comments and empty lines
    if [[ "$line" =~ ^[[:space:]]*# ]] || [[ -z "$line" ]]; then
        continue
    fi
    
    # This is an active configuration line
    if [[ "$line" =~ ^([A-Z0-9_]+)=(.*)$ ]]; then
        CHANGES_MADE=true
        echo "$line" >> "$NEW_CONFIG"
    fi
done < "$TEMP_FILE"

# Replace config.env with new version
mv "$NEW_CONFIG" config.env

if [ "$CHANGES_MADE" = true ]; then
    echo -e "${GREEN}✓ Configuration updated successfully${NC}"
    echo ""
    echo "Active settings for $SERVICE_UPPER:"
    echo "=================================="
    
    # Show what was configured
    if [ "$SERVICE" = "common" ]; then
        grep -E "^[A-Z0-9_]+=" config.env | grep -v "^CN1_\|^CN2_\|^PROXY_\|^LOG_\|^TN_" || echo "  (none - all settings commented out)"
    else
        grep "^${PREFIX}_" config.env || echo "  (none - all settings commented out)"
    fi
    
    echo ""
    echo -e "${BLUE}Generating TOML configuration files...${NC}"
    if ./generate-config.sh > /dev/null 2>&1; then
        echo -e "${GREEN}✓ TOML files generated successfully${NC}"
    else
        echo -e "${YELLOW}⚠ Warning: Failed to generate TOML files${NC}"
        echo "Please run manually: make dev-config"
    fi
    
    echo ""
    echo "Next steps:"
    if [ "$SERVICE" != "common" ]; then
        echo "  1. Restart service:  make dev-restart-${SERVICE}  (only restart $SERVICE_UPPER)"
        echo "  2. Or restart all:   make dev-restart"
        echo "  3. Check logs:       make dev-logs-${SERVICE}"
    else
        echo "  1. Restart all:      make dev-restart"
        echo "  2. Check logs:       make dev-logs"
    fi
    
    if [ -f "config.env.bak" ]; then
        echo ""
        echo "Backup saved: config.env.bak"
    fi
else
    echo -e "${YELLOW}⚠ No changes made (all settings remain commented out)${NC}"
    # Restore original if no changes
    if [ -f "config.env.bak" ]; then
        mv config.env.bak config.env
    fi
fi

echo ""
