#!/bin/bash
# Generate configuration files from templates with environment variable substitution

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Load config.env if it exists
if [ -f "config.env" ]; then
    echo "Loading configuration from config.env..."
    set -a
    source config.env
    set +a
fi

# Default values
LOG_LEVEL="${LOG_LEVEL:-info}"
LOG_FORMAT="${LOG_FORMAT:-console}"
LOG_MAX_SIZE="${LOG_MAX_SIZE:-512}"
MEMORY_CACHE="${MEMORY_CACHE:-512MB}"
DISK_CACHE="${DISK_CACHE:-8GB}"
CACHE_DIR="${CACHE_DIR:-mo-data/file-service-cache}"
DISABLE_TRACE="${DISABLE_TRACE:-true}"
DISABLE_METRIC="${DISABLE_METRIC:-true}"

echo "Configuration:"
echo "  Log Level:       $LOG_LEVEL"
echo "  Log Format:      $LOG_FORMAT"
echo "  Log Max Size:    $LOG_MAX_SIZE MB"
echo "  Memory Cache:    $MEMORY_CACHE"
echo "  Disk Cache:      $DISK_CACHE"
echo "  Disable Trace:   $DISABLE_TRACE"
echo "  Disable Metric:  $DISABLE_METRIC"
echo ""

# Generate CN1 config
cat > cn1.toml << EOF
service-type = "CN"
data-dir = "./mo-data"

[log]
level = "$LOG_LEVEL"
format = "$LOG_FORMAT"
max-size = $LOG_MAX_SIZE
filename = "/logs/cn1.log"

[hakeeper-client]
service-addresses = [
  "mo-log:32001",
]

[[fileservice]]
name = "LOCAL"
backend = "DISK"

[[fileservice]]
name = "SHARED"
backend = "DISK"
data-dir = "mo-data/shared"

[fileservice.cache]
memory-capacity = "$MEMORY_CACHE"
disk-capacity = "$DISK_CACHE"
disk-path = "$CACHE_DIR"

[[fileservice]]
name = "ETL"
backend = "DISK-ETL"

[observability]
disableTrace = $DISABLE_TRACE
disableMetric = $DISABLE_METRIC

[cn]
uuid = "dd1dccb4-4d3c-41f8-b482-5251dc7a41bf"
port-base = 18100
listen-address = "0.0.0.0:18101"
service-address = "mo-cn1:18101"
service-host = "mo-cn1"
sql-address = "mo-cn1:16001"

[cn.Engine]
type = "distributed-tae"

[cn.frontend]
port = 16001
host = "0.0.0.0"
proxy-enabled = true
EOF

# Generate CN2 config
cat > cn2.toml << EOF
service-type = "CN"
data-dir = "./mo-data"

[log]
level = "$LOG_LEVEL"
format = "$LOG_FORMAT"
max-size = $LOG_MAX_SIZE
filename = "/logs/cn2.log"

[hakeeper-client]
service-addresses = [
  "mo-log:32001",
]

[[fileservice]]
name = "LOCAL"
backend = "DISK"

[[fileservice]]
name = "SHARED"
backend = "DISK"
data-dir = "mo-data/shared"

[fileservice.cache]
memory-capacity = "$MEMORY_CACHE"
disk-capacity = "$DISK_CACHE"
disk-path = "$CACHE_DIR"

[[fileservice]]
name = "ETL"
backend = "DISK-ETL"

[observability]
disableTrace = $DISABLE_TRACE
disableMetric = $DISABLE_METRIC

[cn]
uuid = "dd2dccb4-4d3c-41f8-b482-5251dc7a41be"
port-base = 18200
listen-address = "0.0.0.0:18201"
service-address = "mo-cn2:18201"
service-host = "mo-cn2"
sql-address = "mo-cn2:16002"

[cn.Engine]
type = "distributed-tae"

[cn.frontend]
port = 16002
host = "0.0.0.0"
proxy-enabled = true
EOF

# Generate log service config
cat > log.toml << EOF
# service node type, [DN|CN|LOG]
service-type = "LOG"
data-dir = "./mo-data"

[log]
level = "$LOG_LEVEL"
format = "$LOG_FORMAT"
max-size = $LOG_MAX_SIZE
filename = "/logs/logservice.log"

[[fileservice]]
name = "LOCAL"
backend = "DISK"

[[fileservice]]
name = "SHARED"
backend = "DISK"
data-dir = "mo-data/shared"

[fileservice.cache]
memory-capacity = "$MEMORY_CACHE"
disk-capacity = "$DISK_CACHE"
disk-path = "$CACHE_DIR"

[[fileservice]]
name = "ETL"
backend = "DISK-ETL"

[observability]
status-port = 7001
EOF

# Generate TN config
cat > tn.toml << EOF
service-type = "DN"
data-dir = "./mo-data"

[log]
level = "$LOG_LEVEL"
format = "$LOG_FORMAT"
max-size = $LOG_MAX_SIZE
filename = "/logs/tn.log"

[hakeeper-client]
service-addresses = [
  "mo-log:32001",
]

[[fileservice]]
name = "LOCAL"
backend = "DISK"

[[fileservice]]
name = "SHARED"
backend = "DISK"
data-dir = "mo-data/shared"

[fileservice.cache]
memory-capacity = "$MEMORY_CACHE"
disk-capacity = "$DISK_CACHE"
disk-path = "$CACHE_DIR"

[[fileservice]]
name = "ETL"
backend = "DISK-ETL"

[observability]
disableTrace = $DISABLE_TRACE
disableMetric = $DISABLE_METRIC

[dn]
uuid = "dd4dccb4-4d3c-41f8-b482-5251dc7a41bd"
port-base = 19100
service-address = "mo-tn:19100"
service-host = "mo-tn"

[dn.Txn.Storage]
backend = "TAE"
log-backend = "logservice"

[dn.Ckp]
flush-interval = "60s"
min-count = 100
scan-interval = "5s"
incremental-interval = "60s"
global-interval = "100000s"
EOF

# Generate proxy config  
cat > proxy.toml << EOF
service-type = "PROXY"

[log]
level = "$LOG_LEVEL"
format = "$LOG_FORMAT"
max-size = $LOG_MAX_SIZE
filename = "/logs/proxy.log"

[hakeeper-client]
service-addresses = [
  "mo-log:32001",
]

[observability]
disableTrace = $DISABLE_TRACE
disableMetric = $DISABLE_METRIC

[proxy]
listen-address = "0.0.0.0:6009"
rebalance-interval = 30
rebalance-disabled = false
rebalance-tolerance = 0.3
EOF

echo "✓ Configuration files generated successfully"
echo ""
echo "Generated files:"
echo "  - cn1.toml"
echo "  - cn2.toml"
echo "  - log.toml"
echo "  - tn.toml"
echo "  - proxy.toml"

