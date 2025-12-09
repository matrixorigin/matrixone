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

Environment Variables:
  DOCKER_UID              Automatically set to $(id -u)
  DOCKER_GID              Automatically set to $(id -g)
  IMAGE_NAME              Set based on --version option

EOF
    exit 0
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
    COMPOSE_FILES="-f docker-compose.yml -f $TEMP_OVERRIDE"
else
    COMPOSE_FILES="-f docker-compose.yml"
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
    mkdir -p "${DATA_DIRS[@]}"
    
    # Pre-create subdirectories that Grafana might create
    mkdir -p ../../grafana-data/dashboards ../../grafana-local-data/dashboards 2>/dev/null || true
    
    # Fix ownership for all directories (in case they exist but owned by root)
    # Recursively check and fix all subdirectories, not just top-level
    NEEDS_FIX=false
    for dir in "${DATA_DIRS[@]}"; do
        if [ -d "$dir" ]; then
            # Check if any files/directories are owned by root (UID 0)
            if find "$dir" -user root 2>/dev/null | grep -q .; then
                NEEDS_FIX=true
                echo "Fixing ownership for $dir (found root-owned files)..."
                sudo chown -R "$DOCKER_UID:$DOCKER_GID" "$dir" 2>/dev/null || {
                    echo "Warning: Could not fix ownership for $dir. You may need to run:"
                    echo "  sudo chown -R $DOCKER_UID:$DOCKER_GID $dir"
                }
            else
                # Also check if top-level directory is not writable
                dir_uid=$(get_owner_uid "$dir")
                if [ "$dir_uid" -eq 0 ] || [ ! -w "$dir" ]; then
                    NEEDS_FIX=true
                    echo "Fixing ownership for $dir..."
                    sudo chown -R "$DOCKER_UID:$DOCKER_GID" "$dir" 2>/dev/null || {
                        echo "Warning: Could not fix ownership for $dir. You may need to run:"
                        echo "  sudo chown -R $DOCKER_UID:$DOCKER_GID $dir"
                    }
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

# Pass arguments to docker compose
docker compose $COMPOSE_FILES "${DOCKER_COMPOSE_ARGS[@]}"

