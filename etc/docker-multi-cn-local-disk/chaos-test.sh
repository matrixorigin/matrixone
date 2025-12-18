#!/bin/bash
# Chaos testing script for MatrixOne CN nodes
# Randomly stops and starts cn1 or cn2 containers

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default time ranges
UP_TIME_MIN=30
UP_TIME_MAX=120
DOWN_WAIT_TIME_MIN=10
DOWN_WAIT_TIME_MAX=20

# Default CN selection (empty means random)
# Can be a single container or comma-separated list (e.g., "cn1,cn2,tn")
SELECTED_CN=""
SELECTED_CNS=()  # Array to store multiple containers

# Network chaos settings
NETWORK_CHAOS_ENABLED=false
NETWORK_CHAOS_TYPE=""  # loss, delay, bandwidth, or combined
NETWORK_LOSS_PERCENT=10
NETWORK_DELAY_MS=100
NETWORK_BANDWIDTH_MBIT=10
NETWORK_BANDWIDTH_SET=false  # Track if bandwidth was explicitly set by user
NETWORK_ONLY_MODE=false  # Only inject network chaos, don't stop containers
NETWORK_SCENARIO=""  # Preset scenario: light, moderate, severe, inter-region, inter-continent, congestion, bandwidth, random
NETWORK_RANDOM=false  # Randomly choose delay or loss
NETWORK_CHAOS_DURATION=0  # Duration in seconds (0 = inject until manually stopped, default)

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Function to show usage
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Options:
  -c, --cn CN              Specify which container(s) to test
                           Single: cn1, cn2, or tn
                           Multiple: cn1,cn2,tn or cn1,tn (comma-separated, no spaces)
                           Default: random cn1/cn2
  -u, --up-time RANGE      Container running time before stopping (default: 30-120)
                           Format: min-max or min,max (e.g., 30-120 or 30,120)
  -d, --down-time RANGE    Wait time after stopping before starting (default: 10-20)
                           Format: min-max or min,max (e.g., 10-20 or 10,20)
  -n, --network TYPE       Enable network chaos injection
                           Types: loss, delay, bandwidth, combined
                           Examples: -n loss, -n delay, -n bandwidth, -n combined
  --loss PERCENT           Packet loss percentage (default: 10, used with -n loss/combined)
  --delay MS               Network delay in milliseconds (default: 100, used with -n delay/combined)
  --bandwidth MBIT         Bandwidth limit in Mbps (default: 10, used with -n bandwidth/combined)
  --network-only           Only inject network chaos, don't stop containers (requires -n or --scenario)
  --scenario SCENARIO      Use preset network chaos scenario (see scenarios below)
  --random                 Randomly choose delay or loss (used with --scenario or -n)
  --duration SECONDS      Network chaos duration in seconds (default: 0 = inject until manually stopped)
                          Examples: --duration 60 (inject for 60 seconds), --duration 0 (inject until Ctrl+C)
  -h, --help              Show this help message

Network Chaos Scenarios (--scenario):
  light           - Light network issues (10-30ms delay, 1-3% loss)
  moderate        - Moderate network issues (50-150ms delay, 5-10% loss)
  severe          - Severe network issues (200-400ms delay, 15-25% loss)
  inter-region    - Inter-region network (100-200ms delay, 2-5% loss)
  inter-continent - Inter-continent network (300-500ms delay, 5-10% loss)
  congestion      - Network congestion (50-100ms delay, 10-20% loss)
  bandwidth       - Bandwidth limitation (10Mbps)
  random          - Randomly select delay or loss

Workflow (default mode):
  1. Container runs for UP_TIME seconds (random in range)
  2. Stop container
  3. Wait for DOWN_TIME seconds (random in range)
  4. Start container
  5. Repeat

Workflow (--network-only mode):
  1. Inject network chaos
  2. If --duration is specified: Wait for duration, then restore network and exit
  3. If --duration is 0 (default): Inject until manually stopped (Ctrl+C), then restore network
  4. If duration > 0 and in loop mode: Repeat (containers remain running)

Examples:
  # Default mode: stop/start containers
  $0                                    # Use default: random CN, up 30-120s, down wait 10-20s
  $0 -c cn1                             # Test cn1 only
  
  # Network chaos with container restart
  $0 -c tn -n loss --loss 20            # Test tn with 20% packet loss
  $0 -c cn2 -n delay --delay 200        # Test cn2 with 200ms delay
  $0 -c cn1 -n bandwidth --bandwidth 5  # Test cn1 with 5Mbps bandwidth limit
  $0 -c cn2 -n combined --loss 15 --delay 150 --bandwidth 8  # Combined: delay + loss + bandwidth
  $0 -c cn1 -n combined --loss 20 --delay 100 --bandwidth 10  # Combined: delay + loss + bandwidth (10Mbps)
  $0 -c cn1 -n combined --loss 10 --delay 50  # Combined: delay + loss only (no bandwidth)
  
  # Network-only mode (containers keep running)
  $0 --network-only -c cn1 --scenario moderate    # Moderate network chaos on cn1 (inject until Ctrl+C)
  $0 --network-only -c cn2 --scenario severe       # Severe network chaos on cn2 (inject until Ctrl+C)
  $0 --network-only -c tn --scenario inter-region  # Inter-region network on tn (inject until Ctrl+C)
  $0 --network-only -c cn1 --random               # Random delay or loss on cn1 (inject until Ctrl+C)
  $0 --network-only -c cn2 -n delay --delay 100   # 100ms delay on cn2 (inject until Ctrl+C)
  $0 --network-only -c cn1,cn2 --scenario moderate  # Moderate chaos on cn1 and cn2 (inject until Ctrl+C)
  $0 --network-only -c cn1,tn -n delay --delay 100  # 100ms delay on cn1 and tn (inject until Ctrl+C)
  $0 --network-only -c cn1,cn2,tn --scenario severe # Severe chaos on all nodes (inject until Ctrl+C)
  
  # Network-only mode with duration (auto-restore after duration)
  $0 --network-only -c cn1 --scenario moderate --duration 60    # Moderate chaos for 60 seconds
  $0 --network-only -c cn1,cn2 -n delay --delay 100 --duration 120  # 100ms delay for 120 seconds
  $0 --network-only -c cn1,tn --scenario severe --duration 300  # Severe chaos for 5 minutes

EOF
}

# Function to parse time range
parse_time_range() {
    local range=$1
    local var_min=$2
    local var_max=$3
    
    # Support both "min-max" and "min,max" formats
    if [[ "$range" =~ ^([0-9]+)[-,]([0-9]+)$ ]]; then
        local min="${BASH_REMATCH[1]}"
        local max="${BASH_REMATCH[2]}"
        
        if [ "$min" -gt "$max" ]; then
            echo "Error: Minimum value ($min) cannot be greater than maximum value ($max)" >&2
            return 1
        fi
        
        eval "$var_min=$min"
        eval "$var_max=$max"
        return 0
    else
        echo "Error: Invalid time range format: $range. Use 'min-max' or 'min,max'" >&2
        return 1
    fi
}

# Function to generate random number between min and max (inclusive)
random_range() {
    local min=$1
    local max=$2
    echo $((RANDOM % (max - min + 1) + min))
}

# Function to apply network chaos scenario
apply_scenario() {
    local scenario=$1
    
    case "$scenario" in
        light)
            NETWORK_CHAOS_TYPE="combined"
            NETWORK_DELAY_MS=$(random_range 10 30)
            NETWORK_LOSS_PERCENT=$(random_range 1 3)
            echo -e "${BLUE}Scenario: Light network issues${NC}"
            echo -e "  Delay: ${NETWORK_DELAY_MS}ms, Loss: ${NETWORK_LOSS_PERCENT}%"
            ;;
        moderate)
            NETWORK_CHAOS_TYPE="combined"
            NETWORK_DELAY_MS=$(random_range 50 150)
            NETWORK_LOSS_PERCENT=$(random_range 5 10)
            echo -e "${BLUE}Scenario: Moderate network issues${NC}"
            echo -e "  Delay: ${NETWORK_DELAY_MS}ms, Loss: ${NETWORK_LOSS_PERCENT}%"
            ;;
        severe)
            NETWORK_CHAOS_TYPE="combined"
            NETWORK_DELAY_MS=$(random_range 200 400)
            NETWORK_LOSS_PERCENT=$(random_range 15 25)
            echo -e "${BLUE}Scenario: Severe network issues${NC}"
            echo -e "  Delay: ${NETWORK_DELAY_MS}ms, Loss: ${NETWORK_LOSS_PERCENT}%"
            ;;
        inter-region)
            NETWORK_CHAOS_TYPE="combined"
            NETWORK_DELAY_MS=$(random_range 100 200)
            NETWORK_LOSS_PERCENT=$(random_range 2 5)
            echo -e "${BLUE}Scenario: Inter-region network${NC}"
            echo -e "  Delay: ${NETWORK_DELAY_MS}ms, Loss: ${NETWORK_LOSS_PERCENT}%"
            ;;
        inter-continent)
            NETWORK_CHAOS_TYPE="combined"
            NETWORK_DELAY_MS=$(random_range 300 500)
            NETWORK_LOSS_PERCENT=$(random_range 5 10)
            echo -e "${BLUE}Scenario: Inter-continent network${NC}"
            echo -e "  Delay: ${NETWORK_DELAY_MS}ms, Loss: ${NETWORK_LOSS_PERCENT}%"
            ;;
        congestion)
            NETWORK_CHAOS_TYPE="combined"
            NETWORK_DELAY_MS=$(random_range 50 100)
            NETWORK_LOSS_PERCENT=$(random_range 10 20)
            echo -e "${BLUE}Scenario: Network congestion${NC}"
            echo -e "  Delay: ${NETWORK_DELAY_MS}ms, Loss: ${NETWORK_LOSS_PERCENT}%"
            ;;
        bandwidth)
            NETWORK_CHAOS_TYPE="bandwidth"
            NETWORK_BANDWIDTH_MBIT=10
            echo -e "${BLUE}Scenario: Bandwidth limitation${NC}"
            echo -e "  Bandwidth: ${NETWORK_BANDWIDTH_MBIT}Mbps"
            ;;
        random)
            # Randomly choose delay or loss
            if [ $((RANDOM % 2)) -eq 0 ]; then
                NETWORK_CHAOS_TYPE="delay"
                NETWORK_DELAY_MS=$(random_range 50 300)
                echo -e "${BLUE}Scenario: Random delay${NC}"
                echo -e "  Delay: ${NETWORK_DELAY_MS}ms"
            else
                NETWORK_CHAOS_TYPE="loss"
                NETWORK_LOSS_PERCENT=$(random_range 5 20)
                echo -e "${BLUE}Scenario: Random packet loss${NC}"
                echo -e "  Loss: ${NETWORK_LOSS_PERCENT}%"
            fi
            ;;
        *)
            echo "Error: Unknown scenario: $scenario" >&2
            echo "Available scenarios: light, moderate, severe, inter-region, inter-continent, congestion, bandwidth, random" >&2
            exit 1
            ;;
    esac
}

# Function to apply random network chaos
apply_random_chaos() {
    if [ $((RANDOM % 2)) -eq 0 ]; then
        NETWORK_CHAOS_TYPE="delay"
        NETWORK_DELAY_MS=$(random_range 50 300)
        echo -e "${BLUE}Randomly selected: Delay${NC}"
        echo -e "  Delay: ${NETWORK_DELAY_MS}ms"
    else
        NETWORK_CHAOS_TYPE="loss"
        NETWORK_LOSS_PERCENT=$(random_range 5 20)
        echo -e "${BLUE}Randomly selected: Packet loss${NC}"
        echo -e "  Loss: ${NETWORK_LOSS_PERCENT}%"
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--cn)
            if [ -z "$2" ]; then
                echo "Error: --cn requires a value (cn1, cn2, tn, or comma-separated list)" >&2
                show_usage
                exit 1
            fi
            # Parse comma-separated list of containers
            SELECTED_CN="$2"
            SELECTED_CNS=()
            IFS=',' read -ra CN_ARRAY <<< "$2"
            for cn in "${CN_ARRAY[@]}"; do
                cn=$(echo "$cn" | xargs)  # Trim whitespace
                if [ "$cn" != "cn1" ] && [ "$cn" != "cn2" ] && [ "$cn" != "tn" ]; then
                    echo "Error: Invalid container name '$cn'. Must be 'cn1', 'cn2', or 'tn'" >&2
                    show_usage
                    exit 1
                fi
                SELECTED_CNS+=("mo-$cn")
            done
            shift 2
            ;;
        -n|--network)
            if [ -z "$2" ]; then
                echo "Error: --network requires a type (loss, delay, bandwidth, or combined)" >&2
                show_usage
                exit 1
            fi
            if [ "$2" != "loss" ] && [ "$2" != "delay" ] && [ "$2" != "bandwidth" ] && [ "$2" != "combined" ]; then
                echo "Error: --network must be 'loss', 'delay', 'bandwidth', or 'combined'" >&2
                show_usage
                exit 1
            fi
            NETWORK_CHAOS_ENABLED=true
            NETWORK_CHAOS_TYPE="$2"
            shift 2
            ;;
        --loss)
            if [ -z "$2" ]; then
                echo "Error: --loss requires a percentage value" >&2
                show_usage
                exit 1
            fi
            if ! [[ "$2" =~ ^[0-9]+$ ]] || [ "$2" -lt 0 ] || [ "$2" -gt 100 ]; then
                echo "Error: --loss must be a number between 0 and 100" >&2
                exit 1
            fi
            NETWORK_LOSS_PERCENT="$2"
            shift 2
            ;;
        --delay)
            if [ -z "$2" ]; then
                echo "Error: --delay requires a value in milliseconds" >&2
                show_usage
                exit 1
            fi
            if ! [[ "$2" =~ ^[0-9]+$ ]] || [ "$2" -lt 0 ]; then
                echo "Error: --delay must be a positive number" >&2
                exit 1
            fi
            NETWORK_DELAY_MS="$2"
            shift 2
            ;;
        --bandwidth)
            if [ -z "$2" ]; then
                echo "Error: --bandwidth requires a value in Mbps" >&2
                show_usage
                exit 1
            fi
            if ! [[ "$2" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
                echo "Error: --bandwidth must be a positive number" >&2
                exit 1
            fi
            # Convert to integer for comparison (handles decimals by truncating)
            bw_int=${2%.*}
            if [ "$bw_int" -le 0 ]; then
                echo "Error: --bandwidth must be greater than 0" >&2
                exit 1
            fi
            NETWORK_BANDWIDTH_MBIT="$2"
            NETWORK_BANDWIDTH_SET=true  # Mark that bandwidth was explicitly set
            shift 2
            ;;
        -u|--up-time)
            if [ -z "$2" ]; then
                echo "Error: --up-time requires a value" >&2
                show_usage
                exit 1
            fi
            if ! parse_time_range "$2" UP_TIME_MIN UP_TIME_MAX; then
                exit 1
            fi
            shift 2
            ;;
        -d|--down-time)
            if [ -z "$2" ]; then
                echo "Error: --down-time requires a value" >&2
                show_usage
                exit 1
            fi
            if ! parse_time_range "$2" DOWN_WAIT_TIME_MIN DOWN_WAIT_TIME_MAX; then
                exit 1
            fi
            shift 2
            ;;
        --network-only)
            NETWORK_ONLY_MODE=true
            shift
            ;;
        --scenario)
            if [ -z "$2" ]; then
                echo "Error: --scenario requires a scenario name" >&2
                show_usage
                exit 1
            fi
            NETWORK_SCENARIO="$2"
            NETWORK_CHAOS_ENABLED=true
            shift 2
            ;;
        --random)
            NETWORK_RANDOM=true
            shift
            ;;
        --duration)
            if [ -z "$2" ]; then
                echo "Error: --duration requires a value in seconds (0 = inject until manually stopped)" >&2
                show_usage
                exit 1
            fi
            if ! [[ "$2" =~ ^[0-9]+$ ]] || [ "$2" -lt 0 ]; then
                echo "Error: --duration must be a non-negative integer (0 = inject until manually stopped)" >&2
                exit 1
            fi
            NETWORK_CHAOS_DURATION="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            echo "Error: Unknown option: $1" >&2
            show_usage
            exit 1
            ;;
    esac
done

# Apply scenario or random chaos if specified
if [ -n "$NETWORK_SCENARIO" ]; then
    apply_scenario "$NETWORK_SCENARIO"
elif [ "$NETWORK_RANDOM" = true ]; then
    apply_random_chaos
fi

# Validate network-only mode
if [ "$NETWORK_ONLY_MODE" = true ] && [ "$NETWORK_CHAOS_ENABLED" != true ]; then
    echo "Error: --network-only requires -n/--network or --scenario option" >&2
    show_usage
    exit 1
fi

# Function to check if container exists and is running
check_container() {
    local container=$1
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        return 0
    else
        return 1
    fi
}

# Function to stop container
stop_container() {
    local container=$1
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] Stopping ${container}...${NC}"
    docker stop "$container" > /dev/null 2>&1 || {
        echo -e "${RED}Error: Failed to stop ${container}${NC}"
        return 1
    }
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ${container} is DOWN${NC}"
}

# Function to start container
start_container() {
    local container=$1
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] Starting ${container}...${NC}"
    docker start "$container" > /dev/null 2>&1 || {
        echo -e "${RED}Error: Failed to start ${container}${NC}"
        return 1
    }
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] ${container} is UP${NC}"
}

# Function to wait for seconds
wait_seconds() {
    local seconds=$1
    local message=$2
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] ${message} (waiting ${seconds}s)...${NC}"
    sleep "$seconds"
}

# Function to install iproute2 in container (requires root or sudo)
install_iproute2() {
    local container=$1
    echo -e "${YELLOW}Attempting to install iproute2 in container ${container}...${NC}"
    
    # Try to install iproute2 (may require root)
    if docker exec "$container" sh -c "apt-get update && apt-get install -y iproute2 && rm -rf /var/lib/apt/lists/*" > /dev/null 2>&1; then
        echo -e "${GREEN}Successfully installed iproute2 in ${container}${NC}"
        return 0
    elif docker exec -u root "$container" sh -c "apt-get update && apt-get install -y iproute2 && rm -rf /var/lib/apt/lists/*" > /dev/null 2>&1; then
        echo -e "${GREEN}Successfully installed iproute2 in ${container} (as root)${NC}"
        return 0
    else
        echo -e "${RED}Failed to install iproute2. Container may not have apt-get or root access.${NC}"
        return 1
    fi
}

# Function to check if container has network admin capabilities
check_network_capabilities() {
    local container=$1
    local auto_install=${2:-false}
    
    # Check if tc command exists in container
    if ! docker exec "$container" which tc > /dev/null 2>&1; then
        echo -e "${RED}Error: 'tc' command not found in container ${container}${NC}"
        echo ""
        echo "The 'tc' command is required for network chaos testing."
        echo "It is provided by the 'iproute2' package."
        echo ""
        
        if [ "$auto_install" = true ]; then
            if install_iproute2 "$container"; then
                # Verify installation
                if docker exec "$container" which tc > /dev/null 2>&1; then
                    echo -e "${GREEN}tc command is now available${NC}"
                    return 0
                fi
            fi
        fi
        
        echo "Solutions:"
        echo "  1. Rebuild the Docker image with iproute2 installed:"
        echo "     make dev-build"
        echo ""
        echo "  2. Install iproute2 manually in the running container:"
        echo "     docker exec -u root ${container} apt-get update && docker exec -u root ${container} apt-get install -y iproute2"
        echo ""
        echo "  3. Or restart containers after rebuilding:"
        echo "     make dev-build && make dev-restart"
        return 1
    fi
    
    # Try to run a simple tc command to check permissions (as root)
    if ! docker exec -u root "$container" tc qdisc show > /dev/null 2>&1; then
        echo -e "${RED}Error: Container ${container} does not have NET_ADMIN capability${NC}"
        echo "Please restart container with NET_ADMIN capability."
        echo "The docker-compose.yml should already have cap_add: [NET_ADMIN] configured."
        echo "Try: make dev-restart"
        return 1
    fi
    return 0
}

# Function to get the main network interface in container
get_container_interface() {
    local container=$1
    # Get the default route interface
    local iface=$(docker exec "$container" ip route | grep default | awk '{print $5}' | head -1)
    if [ -z "$iface" ]; then
        # Fallback to eth0
        iface="eth0"
    fi
    echo "$iface"
}

# Function to inject network chaos
inject_network_chaos() {
    local container=$1
    local iface=$(get_container_interface "$container")
    
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] Injecting network chaos into ${container}...${NC}"
    echo -e "${BLUE}Using network interface: ${iface}${NC}"
    
    # Verify interface exists
    if ! docker exec "$container" ip link show "$iface" > /dev/null 2>&1; then
        echo -e "${RED}Error: Network interface ${iface} not found in container ${container}${NC}"
        echo -e "${YELLOW}Available interfaces:${NC}"
        docker exec "$container" ip link show
        return 1
    fi
    
    # Remove existing qdisc if any
    docker exec -u root "$container" tc qdisc del dev "$iface" root > /dev/null 2>&1 || true
    
    # Build netem parameters
    local netem_params=""
    
    if [ "$NETWORK_CHAOS_TYPE" = "loss" ]; then
        netem_params="loss ${NETWORK_LOSS_PERCENT}%"
        echo -e "  ${RED}Packet loss: ${NETWORK_LOSS_PERCENT}%${NC}"
    elif [ "$NETWORK_CHAOS_TYPE" = "delay" ]; then
        netem_params="delay ${NETWORK_DELAY_MS}ms"
        echo -e "  ${RED}Network delay: ${NETWORK_DELAY_MS}ms${NC}"
    elif [ "$NETWORK_CHAOS_TYPE" = "bandwidth" ]; then
        # For bandwidth only, use htb
        echo -e "  ${RED}Bandwidth limit: ${NETWORK_BANDWIDTH_MBIT}Mbps${NC}"
        
        # Step 1: Create root htb qdisc
        if ! docker exec -u root "$container" tc qdisc add dev "$iface" root handle 1: htb default 1 2>&1; then
            echo -e "${RED}Error: Failed to create root htb qdisc${NC}"
            docker exec -u root "$container" tc qdisc add dev "$iface" root handle 1: htb default 1
            return 1
        fi
        
        # Step 2: Create htb class for bandwidth
        if ! docker exec -u root "$container" tc class add dev "$iface" parent 1: classid 1:1 htb rate "${NETWORK_BANDWIDTH_MBIT}mbit" 2>&1; then
            echo -e "${RED}Error: Failed to create htb class${NC}"
            docker exec -u root "$container" tc class add dev "$iface" parent 1: classid 1:1 htb rate "${NETWORK_BANDWIDTH_MBIT}mbit"
            return 1
        fi
        
        echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] Network chaos injected into ${container}${NC}"
        return 0
    elif [ "$NETWORK_CHAOS_TYPE" = "combined" ]; then
        # For combined mode: apply delay + loss, and optionally bandwidth if explicitly set
        # Build netem parameters for delay and loss
        netem_params="delay ${NETWORK_DELAY_MS}ms loss ${NETWORK_LOSS_PERCENT}%"
        echo -e "  ${RED}Network delay: ${NETWORK_DELAY_MS}ms${NC}"
        echo -e "  ${RED}Packet loss: ${NETWORK_LOSS_PERCENT}%${NC}"
        
        # Apply bandwidth limit if explicitly set by user
        if [ "$NETWORK_BANDWIDTH_SET" = true ] && [ "${NETWORK_BANDWIDTH_MBIT}" != "0" ]; then
            echo -e "  ${RED}Bandwidth limit: ${NETWORK_BANDWIDTH_MBIT}Mbps${NC}"
            
            # Create htb for bandwidth, then add netem
            # Step 1: Create root htb qdisc
            if ! docker exec -u root "$container" tc qdisc add dev "$iface" root handle 1: htb default 1 2>&1; then
                echo -e "${RED}Error: Failed to create root htb qdisc${NC}"
                docker exec -u root "$container" tc qdisc add dev "$iface" root handle 1: htb default 1
                return 1
            fi
            
            # Step 2: Create htb class for bandwidth
            if ! docker exec -u root "$container" tc class add dev "$iface" parent 1: classid 1:1 htb rate "${NETWORK_BANDWIDTH_MBIT}mbit" 2>&1; then
                echo -e "${RED}Error: Failed to create htb class${NC}"
                docker exec -u root "$container" tc class add dev "$iface" parent 1: classid 1:1 htb rate "${NETWORK_BANDWIDTH_MBIT}mbit"
                return 1
            fi
            
            # Step 3: Add netem as child qdisc
            if ! docker exec -u root "$container" tc qdisc add dev "$iface" parent 1:1 handle 2: netem $netem_params 2>&1; then
                echo -e "${RED}Error: Failed to add netem qdisc${NC}"
                docker exec -u root "$container" tc qdisc add dev "$iface" parent 1:1 handle 2: netem $netem_params
                return 1
            fi
        else
            # Simple netem for delay+loss only (no bandwidth limit)
            if ! docker exec -u root "$container" tc qdisc add dev "$iface" root netem $netem_params 2>&1; then
                echo -e "${RED}Error: Failed to inject network chaos into ${container}${NC}"
                echo -e "${YELLOW}Attempting to show actual error:${NC}"
                docker exec -u root "$container" tc qdisc add dev "$iface" root netem $netem_params
                return 1
            fi
        fi
        
        echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] Network chaos injected into ${container}${NC}"
        return 0
    fi
    
    # For loss or delay only, use simple netem
    if [ -n "$netem_params" ]; then
        if ! docker exec -u root "$container" tc qdisc add dev "$iface" root netem $netem_params 2>&1; then
            echo -e "${RED}Error: Failed to inject network chaos into ${container}${NC}"
            echo -e "${YELLOW}Attempting to show actual error:${NC}"
            docker exec -u root "$container" tc qdisc add dev "$iface" root netem $netem_params
            return 1
        fi
        echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] Network chaos injected into ${container}${NC}"
        return 0
    fi
    
    return 1
}

# Function to restore network
restore_network() {
    local container=$1
    local iface=$(get_container_interface "$container")
    
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] Restoring network for ${container}...${NC}"
    
    # Remove all qdisc rules (as root)
    if docker exec -u root "$container" tc qdisc del dev "$iface" root > /dev/null 2>&1; then
        echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] Network restored for ${container}${NC}"
        return 0
    else
        # Try to remove child qdiscs first
        docker exec -u root "$container" tc qdisc del dev "$iface" parent 1:1 > /dev/null 2>&1 || true
        docker exec -u root "$container" tc qdisc del dev "$iface" root > /dev/null 2>&1 || true
        echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] Network restored for ${container}${NC}"
        return 0
    fi
}

# Main loop
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}MatrixOne CN Chaos Testing Script${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
if [ "$NETWORK_ONLY_MODE" = true ]; then
    if [ ${#SELECTED_CNS[@]} -gt 0 ]; then
        echo "This script will inject network chaos into ${SELECTED_CN} container(s) (containers will NOT be stopped)."
    else
        echo "This script will inject network chaos into random cn1 or cn2 containers (containers will NOT be stopped)."
    fi
else
    if [ ${#SELECTED_CNS[@]} -gt 0 ]; then
        echo "This script will stop and start ${SELECTED_CN} container(s)."
    else
        echo "This script will randomly stop and start cn1 or cn2 containers."
    fi
fi
echo -e "${BLUE}Configuration:${NC}"
if [ ${#SELECTED_CNS[@]} -gt 0 ]; then
    echo -e "  Target container(s): ${SELECTED_CN}"
else
    echo -e "  Target container: random (cn1 or cn2)"
fi
if [ "$NETWORK_ONLY_MODE" = true ]; then
    echo -e "  Mode: Network-only (containers remain running)"
    if [ "$NETWORK_CHAOS_DURATION" -eq 0 ]; then
        echo -e "  Chaos duration: Until manually stopped (Ctrl+C)"
    else
        echo -e "  Chaos duration: ${NETWORK_CHAOS_DURATION} seconds (auto-restore after duration)"
    fi
else
    echo -e "  Container up time: ${UP_TIME_MIN}-${UP_TIME_MAX} seconds (before stopping)"
    echo -e "  Down wait time: ${DOWN_WAIT_TIME_MIN}-${DOWN_WAIT_TIME_MAX} seconds (after stopping, before starting)"
fi
if [ "$NETWORK_CHAOS_ENABLED" = true ]; then
    echo -e "  ${RED}Network chaos: ${NETWORK_CHAOS_TYPE}${NC}"
    if [ "$NETWORK_CHAOS_TYPE" = "loss" ] || [ "$NETWORK_CHAOS_TYPE" = "combined" ]; then
        echo -e "    Packet loss: ${NETWORK_LOSS_PERCENT}%"
    fi
    if [ "$NETWORK_CHAOS_TYPE" = "delay" ] || [ "$NETWORK_CHAOS_TYPE" = "combined" ]; then
        echo -e "    Delay: ${NETWORK_DELAY_MS}ms"
    fi
    if [ "$NETWORK_CHAOS_TYPE" = "bandwidth" ]; then
        echo -e "    Bandwidth: ${NETWORK_BANDWIDTH_MBIT}Mbps"
    elif [ "$NETWORK_CHAOS_TYPE" = "combined" ] && [ "$NETWORK_BANDWIDTH_SET" = true ]; then
        echo -e "    Bandwidth: ${NETWORK_BANDWIDTH_MBIT}Mbps"
    fi
fi
echo ""
echo "Press Ctrl+C to stop."
echo ""

# Check if containers exist
if [ ${#SELECTED_CNS[@]} -gt 0 ]; then
    # Check if all specified containers exist
    missing_containers=()
    for container in "${SELECTED_CNS[@]}"; do
        if ! check_container "$container"; then
            missing_containers+=("$container")
        fi
    done
    if [ ${#missing_containers[@]} -gt 0 ]; then
        echo -e "${RED}Error: The following containers are not running: ${missing_containers[*]}${NC}"
        echo "Please start the containers first with: make dev-up"
        exit 1
    fi
    
    # Check network capabilities if network chaos is enabled
    if [ "$NETWORK_CHAOS_ENABLED" = true ]; then
        failed_containers=()
        for container in "${SELECTED_CNS[@]}"; do
            if ! check_network_capabilities "$container" false; then
                failed_containers+=("$container")
            fi
        done
        
        if [ ${#failed_containers[@]} -gt 0 ]; then
            echo ""
            read -p "Attempt to install iproute2 automatically? (y/N) " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                for container in "${failed_containers[@]}"; do
                    if ! check_network_capabilities "$container" true; then
                        echo -e "${YELLOW}Warning: Network chaos may not work properly on ${container}.${NC}"
                    fi
                done
            else
                echo -e "${YELLOW}Warning: Network chaos may not work on: ${failed_containers[*]}${NC}"
                read -p "Continue anyway? (y/N) " -n 1 -r
                echo
                if [[ ! $REPLY =~ ^[Yy]$ ]]; then
                    exit 1
                fi
            fi
        fi
    fi
else
    # Check if at least one container exists
    if ! check_container "mo-cn1" && ! check_container "mo-cn2"; then
        echo -e "${RED}Error: Neither mo-cn1 nor mo-cn2 containers are running.${NC}"
        echo "Please start the containers first with: make dev-up"
        exit 1
    fi
fi

# Counter for iterations
iteration=1

# Signal handler for graceful shutdown
cleanup() {
    echo -e "\n${YELLOW}Script interrupted. Cleaning up...${NC}"
    # Restore network for affected containers if network chaos was enabled
    if [ "$NETWORK_CHAOS_ENABLED" = true ]; then
        # If specific containers were selected, restore only those
        if [ ${#SELECTED_CNS[@]} -gt 0 ]; then
            for container in "${SELECTED_CNS[@]}"; do
                if check_container "$container"; then
                    restore_network "$container" 2>/dev/null || true
                fi
            done
        else
            # Otherwise restore all possible containers
            for container in mo-cn1 mo-cn2 mo-tn; do
                if check_container "$container"; then
                    restore_network "$container" 2>/dev/null || true
                fi
            done
        fi
    fi
    echo -e "${YELLOW}Cleanup complete. Exiting...${NC}"
    exit 0
}
trap cleanup INT TERM

# Main loop
while true; do
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}Iteration ${iteration}${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    # Select container(s) (use specified or random)
    if [ ${#SELECTED_CNS[@]} -gt 0 ]; then
        selected_containers=("${SELECTED_CNS[@]}")
    else
        # Randomly select cn1 or cn2
        if [ $((RANDOM % 2)) -eq 0 ]; then
            selected_containers=("mo-cn1")
        else
            selected_containers=("mo-cn2")
        fi
    fi
    
    echo -e "${YELLOW}Selected container(s): ${selected_containers[*]}${NC}"
    
    # Ensure containers are running
    for container in "${selected_containers[@]}"; do
        if ! check_container "$container"; then
            echo -e "${YELLOW}Warning: ${container} is not running. Starting it...${NC}"
            start_container "$container"
        fi
    done
    # Wait a bit for containers to be ready
    sleep 2
    
    if [ "$NETWORK_ONLY_MODE" = true ]; then
        # Network-only mode: only inject network chaos, don't stop containers
        # Apply scenario or random chaos if specified (re-apply each iteration for randomness)
        if [ -n "$NETWORK_SCENARIO" ]; then
            apply_scenario "$NETWORK_SCENARIO"
        elif [ "$NETWORK_RANDOM" = true ]; then
            apply_random_chaos
        fi
        
        # Inject network chaos on all selected containers (in parallel)
        if [ "$NETWORK_CHAOS_ENABLED" = true ]; then
            for container in "${selected_containers[@]}"; do
                if check_container "$container"; then
                    inject_network_chaos "$container" &
                fi
            done
            wait  # Wait for all background jobs to complete
        fi
        
        # Handle chaos duration
        if [ "$NETWORK_CHAOS_DURATION" -eq 0 ]; then
            # Duration = 0: Inject until manually stopped (Ctrl+C)
            echo -e "${GREEN}Network chaos active on ${#selected_containers[@]} container(s). Press Ctrl+C to stop...${NC}"
            # Wait indefinitely (until Ctrl+C triggers cleanup)
            while true; do
                sleep 1
            done
        else
            # Duration > 0: Inject for specified duration, then restore and exit
            echo -e "${GREEN}Network chaos active for ${NETWORK_CHAOS_DURATION} seconds on ${#selected_containers[@]} container(s)...${NC}"
            wait_seconds "$NETWORK_CHAOS_DURATION" "Network chaos is active"
            
            # Restore network on all selected containers (in parallel)
            if [ "$NETWORK_CHAOS_ENABLED" = true ]; then
                for container in "${selected_containers[@]}"; do
                    if check_container "$container"; then
                        restore_network "$container" &
                    fi
                done
                wait  # Wait for all background jobs to complete
                echo -e "${GREEN}Network chaos duration completed. Network restored.${NC}"
            fi
            
            # Exit after duration (single run mode)
            exit 0
        fi
    else
        # Default mode: stop/start containers
        # Note: In default mode, we only handle one container at a time to avoid complete cluster shutdown
        # If multiple containers are specified, we'll process them sequentially
        for selected_cn in "${selected_containers[@]}"; do
            # Step 1: Container runs for a random time (UP_TIME)
            up_time=$(random_range $UP_TIME_MIN $UP_TIME_MAX)
            echo -e "${GREEN}Container ${selected_cn} will run for ${up_time} seconds...${NC}"
            
            # Inject network chaos if enabled
            if [ "$NETWORK_CHAOS_ENABLED" = true ]; then
                if check_container "$selected_cn"; then
                    inject_network_chaos "$selected_cn"
                fi
            fi
            
            wait_seconds "$up_time" "Container is running"
            
            # Restore network before stopping
            if [ "$NETWORK_CHAOS_ENABLED" = true ]; then
                if check_container "$selected_cn"; then
                    restore_network "$selected_cn"
                fi
            fi
            
            # Step 2: Stop the container
            stop_container "$selected_cn"
            
            # Step 3: Wait for a random time before starting (DOWN_WAIT_TIME)
            down_wait_time=$(random_range $DOWN_WAIT_TIME_MIN $DOWN_WAIT_TIME_MAX)
            echo -e "${YELLOW}Waiting ${down_wait_time} seconds before starting...${NC}"
            wait_seconds "$down_wait_time" "Container is down, waiting before starting"
            
            # Step 4: Start the container
            start_container "$selected_cn"
            
            # Wait a bit for container to be ready before next iteration
            sleep 2
        done
    fi
    
    echo ""
    ((iteration++))
done
