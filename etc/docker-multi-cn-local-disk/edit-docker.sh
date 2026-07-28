#!/usr/bin/env bash
# Interactive docker-compose resource editor for specific services
# Usage: ./edit-docker.sh [SERVICE]
# SERVICE: cn1, cn2, tn, log, proxy

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

OVERRIDE_FILE="docker-compose.override.yml"

get_docker_service() {
    case "$1" in
        cn1) echo "mo-cn1" ;;
        cn2) echo "mo-cn2" ;;
        tn) echo "mo-tn" ;;
        log) echo "mo-log" ;;
        proxy) echo "mo-proxy" ;;
        *) echo "" ;;
    esac
}

SERVICE="${1:-}"
if [ -z "$SERVICE" ]; then
    echo "Usage: $0 <service>"
    echo "Available services: cn1, cn2, tn, log, proxy"
    exit 1
fi

DOCKER_SERVICE=$(get_docker_service "$SERVICE")
if [ -z "$DOCKER_SERVICE" ]; then
    echo "Error: Invalid service '$SERVICE'"
    exit 1
fi

# Read current values from override file
CURRENT_CPU=""
CURRENT_MEMORY=""
if [ -f "$OVERRIDE_FILE" ]; then
    # Simple grep-based parsing
    IN_SERVICE=false
    while IFS= read -r line; do
        if [[ "$line" =~ ^[[:space:]]*${DOCKER_SERVICE}: ]]; then
            IN_SERVICE=true
        elif [[ "$line" =~ ^[[:space:]]*mo- ]] && [[ ! "$line" =~ ${DOCKER_SERVICE} ]]; then
            IN_SERVICE=false
        elif [ "$IN_SERVICE" = true ]; then
            if [[ "$line" =~ cpus:[[:space:]]*[\'\"]?([0-9.]+) ]]; then
                CURRENT_CPU="${BASH_REMATCH[1]}"
            elif [[ "$line" =~ memory:[[:space:]]*([0-9]+[GMgm]) ]]; then
                CURRENT_MEMORY="${BASH_REMATCH[1]}"
            fi
        fi
    done < "$OVERRIDE_FILE"
fi

TEMP_FILE=$(mktemp)
trap "rm -f $TEMP_FILE" EXIT

cat > "$TEMP_FILE" << EOF
# Docker Resource Configuration for $DOCKER_SERVICE
# ============================================================
#
# Instructions:
#   - Lines starting with # are DISABLED
#   - Remove # to ENABLE the setting
#   - Save and close to apply changes
#
# Examples:
#   cpus: '2'        # Limit to 2 CPU cores
#   cpus: '0.5'      # Limit to half a CPU core
#   memory: 4G       # Limit to 4 gigabytes
#   memory: 512M     # Limit to 512 megabytes
# ============================================================

EOF

if [ -n "$CURRENT_CPU" ]; then
    echo "cpus: '$CURRENT_CPU'" >> "$TEMP_FILE"
else
    echo "#cpus: '2'" >> "$TEMP_FILE"
fi

if [ -n "$CURRENT_MEMORY" ]; then
    echo "memory: $CURRENT_MEMORY" >> "$TEMP_FILE"
else
    echo "#memory: 4G" >> "$TEMP_FILE"
fi

EDITOR="${EDITOR:-vim}"
command -v "$EDITOR" &> /dev/null || EDITOR="vi"

echo -e "${BLUE}Opening docker resource editor for $DOCKER_SERVICE...${NC}"
"$EDITOR" "$TEMP_FILE"

# Parse edited values
NEW_CPU=""
NEW_MEMORY=""
while IFS= read -r line; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ -z "$line" ]] && continue
    if [[ "$line" =~ cpus:[[:space:]]*[\'\"]?([0-9.]+) ]]; then
        NEW_CPU="${BASH_REMATCH[1]}"
    elif [[ "$line" =~ memory:[[:space:]]*([0-9]+[GMgm]) ]]; then
        NEW_MEMORY="${BASH_REMATCH[1]}"
    fi
done < "$TEMP_FILE"

# Check if anything changed
if [ "$NEW_CPU" = "$CURRENT_CPU" ] && [ "$NEW_MEMORY" = "$CURRENT_MEMORY" ]; then
    echo -e "${YELLOW}No changes made${NC}"
    exit 0
fi

# Generate new override file
generate_override() {
    local target_svc="$1"
    local target_cpu="$2"
    local target_mem="$3"
    
    local tmp_out=$(mktemp)
    echo "services:" > "$tmp_out"
    
    for svc in mo-cn1 mo-cn2 mo-tn mo-log mo-proxy; do
        local cpu="" mem=""
        
        if [ "$svc" = "$target_svc" ]; then
            cpu="$target_cpu"
            mem="$target_mem"
        elif [ -f "$OVERRIDE_FILE" ]; then
            # Read existing values for other services
            local in_svc=false
            while IFS= read -r line; do
                if [[ "$line" =~ ^[[:space:]]*${svc}: ]]; then
                    in_svc=true
                elif [[ "$line" =~ ^[[:space:]]*mo- ]] && [[ ! "$line" =~ ${svc} ]]; then
                    in_svc=false
                elif [ "$in_svc" = true ]; then
                    if [[ "$line" =~ cpus:[[:space:]]*[\'\"]?([0-9.]+) ]]; then
                        cpu="${BASH_REMATCH[1]}"
                    elif [[ "$line" =~ memory:[[:space:]]*([0-9]+[GMgm]) ]]; then
                        mem="${BASH_REMATCH[1]}"
                    fi
                fi
            done < "$OVERRIDE_FILE"
        fi
        
        if [ -n "$cpu" ] || [ -n "$mem" ]; then
            echo "  $svc:" >> "$tmp_out"
            echo "    deploy:" >> "$tmp_out"
            echo "      resources:" >> "$tmp_out"
            echo "        limits:" >> "$tmp_out"
            [ -n "$cpu" ] && echo "          cpus: '$cpu'" >> "$tmp_out"
            [ -n "$mem" ] && echo "          memory: $mem" >> "$tmp_out"
        fi
    done
    
    # Check if any services configured
    if grep -q "^  mo-" "$tmp_out"; then
        mv "$tmp_out" "$OVERRIDE_FILE"
        return 0
    else
        rm -f "$tmp_out" "$OVERRIDE_FILE"
        return 1
    fi
}

if generate_override "$DOCKER_SERVICE" "$NEW_CPU" "$NEW_MEMORY"; then
    echo -e "${GREEN}✓ Updated $OVERRIDE_FILE${NC}"
    [ -n "$NEW_CPU" ] && echo "  cpus: $NEW_CPU"
    [ -n "$NEW_MEMORY" ] && echo "  memory: $NEW_MEMORY"
else
    echo -e "${GREEN}✓ Removed all resource limits${NC}"
fi
echo ""
echo "Restart to apply: make dev-restart-${SERVICE}"
