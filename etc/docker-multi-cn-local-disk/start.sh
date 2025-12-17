#!/bin/bash
# MatrixOne Docker Compose Startup Script
# Automatically sets user permissions and provides convenient options

set -e

# Get current directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Default values
IMAGE_VERSION="local"
EXTRA_MOUNTS=""
SHOW_HELP=false
CHECK_GRAFANA=false

# Parse custom arguments
DOCKER_COMPOSE_ARGS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--version)
            IMAGE_VERSION="$2"
            shift 2
            ;;
        -m|--mount)
            EXTRA_MOUNTS="$2"
            shift 2
            ;;
        -h|--help)
            SHOW_HELP=true
            shift
            ;;
        check-grafana|--check-grafana)
            CHECK_GRAFANA=true
            shift
            ;;
        *)
            DOCKER_COMPOSE_ARGS+=("$1")
            shift
            ;;
    esac
done

# Show help
if [ "$SHOW_HELP" = true ]; then
    cat << 'EOF'
MatrixOne Docker Compose Startup Script

Usage: ./start.sh [OPTIONS] [DOCKER_COMPOSE_COMMANDS]

Options:
  -v, --version VERSION    Specify image version (default: local)
                          Examples: local, latest, nightly, 1.0.0, 3.0
  -m, --mount PATH        Add extra mount directory (e.g., ../../test:/test:ro)
  -h, --help             Show this help message

Examples:
  # Start with local build (default)
  ./start.sh up -d

  # Use official latest version
  ./start.sh -v latest up -d

  # Use specific version
  ./start.sh -v 3.0 up -d
  ./start.sh -v nightly up -d

  # Mount test directory for SQL files
  ./start.sh -m "../../test:/test:ro" up -d

  # Combine options
  ./start.sh -v latest -m "../../test:/test:ro" up -d

  # Build local image
  ./start.sh build

  # Other docker compose commands
  ./start.sh ps
  ./start.sh logs -f
  ./start.sh down
  ./start.sh restart

  # Check Grafana service status
  ./start.sh check-grafana        # Check if Grafana is ready (ports 3000 and 3001)

Environment Variables:
  DOCKER_UID              Automatically set to $(id -u)
  DOCKER_GID              Automatically set to $(id -g)
  IMAGE_NAME              Set based on --version option
  SKIP_SUDO_FIX           Set to 'true' to skip sudo-based ownership fixes
                          (useful if you don't have sudo access or prefer manual fixes)

EOF
    exit 0
fi

# Function to check if a port is listening and responding
check_port() {
    local port=$1
    local name=$2
    local url=$3
    
    # Check if port is listening
    if command -v nc >/dev/null 2>&1; then
        if ! nc -z localhost "$port" 2>/dev/null; then
            echo "  ✗ $name (port $port): Not listening"
            return 1
        fi
    elif command -v ss >/dev/null 2>&1; then
        if ! ss -lnt | grep -q ":$port "; then
            echo "  ✗ $name (port $port): Not listening"
            return 1
        fi
    fi
    
    # Try to connect via HTTP/HTTPS
    if [ -n "$url" ]; then
        if command -v curl >/dev/null 2>&1; then
            if curl -sf --max-time 2 "$url" >/dev/null 2>&1; then
                echo "  ✓ $name (port $port): Ready and responding"
                return 0
            else
                echo "  ⏳ $name (port $port): Listening but not ready yet"
                return 1
            fi
        elif command -v wget >/dev/null 2>&1; then
            if wget -q --spider --timeout=2 "$url" 2>/dev/null; then
                echo "  ✓ $name (port $port): Ready and responding"
                return 0
            else
                echo "  ⏳ $name (port $port): Listening but not ready yet"
                return 1
            fi
        else
            echo "  ? $name (port $port): Listening (cannot verify HTTP response)"
            return 0
        fi
    else
        echo "  ✓ $name (port $port): Listening"
        return 0
    fi
}

# Function to check Grafana services
check_grafana_services() {
    echo "Checking Grafana services..."
    echo ""
    
    local all_ready=true
    
    # Check if containers are running
    local grafana_running=false
    local grafana_local_running=false
    
    if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^mo-grafana$'; then
        grafana_running=true
    fi
    
    if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^mo-grafana-local$'; then
        grafana_local_running=true
    fi
    
    # Check Grafana (cluster monitoring) on port 3000
    if [ "$grafana_running" = false ]; then
        echo "  ⚠ Grafana (Cluster) (port 3000): Container not running"
        echo "    Start with: make dev-up-grafana"
        all_ready=false
    elif check_port 3000 "Grafana (Cluster)" "http://localhost:3000/api/health"; then
        echo "    URL: http://localhost:3000 (admin/admin)"
    else
        all_ready=false
    fi
    echo ""
    
    # Check Grafana Local on port 3001
    if [ "$grafana_local_running" = false ]; then
        echo "  ⚠ Grafana (Local) (port 3001): Container not running"
        echo "    Start with: make dev-up-grafana-local"
        all_ready=false
    elif check_port 3001 "Grafana (Local)" "http://localhost:3001/api/health"; then
        echo "    URL: http://localhost:3001 (admin/admin)"
    else
        all_ready=false
    fi
    echo ""
    
    if [ "$all_ready" = true ]; then
        echo "✓ All Grafana services are ready!"
        return 0
    else
        echo "⚠ Some Grafana services are not ready yet."
        if [ "$grafana_running" = true ] || [ "$grafana_local_running" = true ]; then
            echo "  Grafana may take a while to start, especially on first run."
            echo "  Keep checking with: make dev-check-grafana"
        fi
        return 1
    fi
}

# Handle check-grafana command
if [ "$CHECK_GRAFANA" = true ]; then
    check_grafana_services
    exit $?
fi

# Auto-detect current user's UID and GID
export DOCKER_UID=$(id -u)
export DOCKER_GID=$(id -g)

# Set image name based on version
if [ "$IMAGE_VERSION" = "local" ]; then
    export IMAGE_NAME="matrixorigin/matrixone:local"
elif [ "$IMAGE_VERSION" = "latest" ]; then
    export IMAGE_NAME="matrixorigin/matrixone:latest"
elif [ "$IMAGE_VERSION" = "nightly" ]; then
    export IMAGE_NAME="matrixorigin/matrixone:nightly"
else
    export IMAGE_NAME="matrixorigin/matrixone:$IMAGE_VERSION"
fi

# Handle extra mounts by creating a temporary docker-compose override
COMPOSE_FILES=(-f docker-compose.yml)
if [ -n "$EXTRA_MOUNTS" ]; then
    TEMP_OVERRIDE="/tmp/docker-compose.override.$$.yml"
    cat > "$TEMP_OVERRIDE" << EOF
services:
  mo-cn1:
    volumes:
      - $EXTRA_MOUNTS
  mo-cn2:
    volumes:
      - $EXTRA_MOUNTS
  mo-proxy:
    volumes:
      - $EXTRA_MOUNTS
EOF
    trap "rm -f $TEMP_OVERRIDE" EXIT
    COMPOSE_FILES+=(-f "$TEMP_OVERRIDE")
fi

# Check if configuration files exist, generate if missing
REQUIRED_CONFIGS=("cn1.toml" "cn2.toml" "log.toml" "tn.toml" "proxy.toml")
MISSING_CONFIGS=false
for config in "${REQUIRED_CONFIGS[@]}"; do
    if [ ! -f "$config" ]; then
        MISSING_CONFIGS=true
        break
    fi
done

# Generate configuration files if missing or if config.env exists
if [ "$MISSING_CONFIGS" = true ]; then
    echo "Configuration files not found. Generating with defaults..."
    ./generate-config.sh
    echo ""
elif [ -f "config.env" ]; then
    echo "Regenerating configuration from config.env..."
    ./generate-config.sh
    echo ""
fi

# Function to get file owner UID (cross-platform)
get_owner_uid() {
    local file="$1"
    # Try Linux stat first
    if stat -c '%u' "$file" 2>/dev/null; then
        return 0
    fi
    # Try macOS/BSD stat
    if stat -f '%u' "$file" 2>/dev/null; then
        return 0
    fi
    # Fallback: assume not root
    echo "1000"
}

    # Pre-create directories with correct permissions (before docker creates them as root)
    if [[ " ${DOCKER_COMPOSE_ARGS[*]} " =~ " up " ]]; then
        echo "Pre-creating directories with correct permissions..."
        DATA_DIRS=("../../mo-data" "../../logs" "../../prometheus-data" "../../prometheus-local-data" "../../grafana-data" "../../grafana-local-data")
        
        # Create directories and set ownership immediately to prevent root ownership
        for dir in "${DATA_DIRS[@]}"; do
            mkdir -p "$dir"
            # Try to set ownership immediately (may fail if dir exists and is root-owned, but that's OK)
            chown "$DOCKER_UID:$DOCKER_GID" "$dir" 2>/dev/null || true
        done
        
        # Pre-create subdirectories that Grafana might create
        mkdir -p ../../grafana-data/dashboards ../../grafana-local-data/dashboards 2>/dev/null || true
        chown -R "$DOCKER_UID:$DOCKER_GID" ../../grafana-data/dashboards ../../grafana-local-data/dashboards 2>/dev/null || true
        
        # Fix ownership for all directories (in case they exist but owned by root)
        # Recursively check and fix all subdirectories, not just top-level
        # Skip sudo if SKIP_SUDO_FIX environment variable is set
        NEEDS_FIX=false
        SKIP_SUDO=false
        if [ "${SKIP_SUDO_FIX:-false}" = "true" ]; then
            SKIP_SUDO=true
            echo "Skipping sudo-based ownership fixes (SKIP_SUDO_FIX=true)"
        fi
    for dir in "${DATA_DIRS[@]}"; do
        if [ -d "$dir" ]; then
            # Check if directory is already writable by current user
            if [ -w "$dir" ]; then
                dir_uid=$(get_owner_uid "$dir")
                # If owned by current user, no fix needed
                if [ "$dir_uid" -eq "$DOCKER_UID" ]; then
                    continue
                fi
            fi
            
            # Check if any files/directories are owned by root (UID 0)
            HAS_ROOT_FILES=false
            if find "$dir" -user root 2>/dev/null | head -1 | grep -q .; then
                HAS_ROOT_FILES=true
            fi
            
            # Check if top-level directory is owned by root or not writable
            dir_uid=$(get_owner_uid "$dir")
            NEEDS_FIX_DIR=false
            if [ "$HAS_ROOT_FILES" = true ] || [ "$dir_uid" -eq 0 ] || [ ! -w "$dir" ]; then
                NEEDS_FIX_DIR=true
            fi
            
            if [ "$NEEDS_FIX_DIR" = true ]; then
                NEEDS_FIX=true
                echo "Fixing ownership for $dir (found root-owned files)..."
                
                # Try without sudo first (in case we have write access)
                if chown -R "$DOCKER_UID:$DOCKER_GID" "$dir" 2>/dev/null; then
                    echo "  ✓ Ownership fixed (no sudo required)"
                elif sudo -n chown -R "$DOCKER_UID:$DOCKER_GID" "$dir" 2>/dev/null; then
                    echo "  ✓ Ownership fixed (no password required)"
                else
                    # If sudo -n fails, check if user wants to skip
                    if [ "$SKIP_SUDO" = false ]; then
                        echo "  ⚠ Files in $dir were previously created by Docker as root"
                        echo "  To fix ownership, sudo is required (you'll be prompted for password)"
                        echo "  If you prefer to skip this, set SKIP_SUDO_FIX=true and fix manually later"
                        if sudo chown -R "$DOCKER_UID:$DOCKER_GID" "$dir" 2>/dev/null; then
                            echo "  ✓ Ownership fixed"
                        else
                            echo "  ⚠ Could not fix ownership for $dir"
                            echo "  You can manually fix it later with:"
                            echo "    sudo chown -R $DOCKER_UID:$DOCKER_GID $dir"
                            echo "  Or skip this check by setting: SKIP_SUDO_FIX=true"
                            SKIP_SUDO=true
                        fi
                    else
                        echo "  ⚠ Skipping sudo fix. You can manually fix with:"
                        echo "    sudo chown -R $DOCKER_UID:$DOCKER_GID $dir"
                    fi
                fi
            fi
        fi
    done
    
    # Set permissions to ensure user can delete files
    for dir in "${DATA_DIRS[@]}"; do
        if [ -d "$dir" ] && [ -w "$dir" ]; then
            chmod -R u+rwX "$dir" 2>/dev/null || true
        fi
    done
    
    if [ "$NEEDS_FIX" = true ]; then
        echo "Directory permissions have been fixed."
    fi
fi

# Show configuration
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "MatrixOne Docker Compose Configuration"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "User Permissions: UID=$DOCKER_UID GID=$DOCKER_GID"
echo "Image:           $IMAGE_NAME"
if [ -n "$EXTRA_MOUNTS" ]; then
    echo "Extra Mounts:    $EXTRA_MOUNTS"
fi
echo "Command:         docker compose ${DOCKER_COMPOSE_ARGS[*]}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Add --profile matrixone if up command is used and no profile is specified
# Profile must be before the 'up' command, not after
if [[ " ${DOCKER_COMPOSE_ARGS[*]} " =~ " up " ]] && [[ ! " ${DOCKER_COMPOSE_ARGS[*]} " =~ " --profile " ]]; then
    # Find the position of 'up' and insert --profile matrixone before it
    NEW_ARGS=()
    for arg in "${DOCKER_COMPOSE_ARGS[@]}"; do
        if [ "$arg" = "up" ]; then
            NEW_ARGS+=("--profile" "matrixone")
        fi
        NEW_ARGS+=("$arg")
    done
    DOCKER_COMPOSE_ARGS=("${NEW_ARGS[@]}")
fi

# Pass arguments to docker compose
docker compose "${COMPOSE_FILES[@]}" "${DOCKER_COMPOSE_ARGS[@]}"

