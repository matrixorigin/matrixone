# Local Multi-CN Development Environment - Commands Quick Reference

> **Note**: These `make dev-*` commands are specifically for the **local multi-CN development environment** (`docker-multi-cn-local-disk`). They are NOT general Docker commands, but specialized shortcuts for managing this specific development setup.

## 🚀 Quick Start

```bash
# From project root
make dev-build && make dev-up
```

> **Note:** Configuration files (`*.toml`) are automatically generated on first startup. They are not tracked by git to avoid commit conflicts. To customize configurations, use `config.env` (see Configuration Management section).

## 📋 All Available Commands

### Basic Operations

| Command | Description |
|---------|-------------|
| `make dev-help` | Show all available commands |
| `make dev-build` | Build MatrixOne docker image (local tag) |
| `make dev-up` | Start multi-CN cluster (default: local image) |
| `make dev-down` | Stop multi-CN cluster |
| `make dev-restart` | Restart multi-CN cluster |
| `make dev-ps` | Show service status |
| `make dev-clean` | Stop and remove all data ⚠️ |

### Version Selection

| Command | Description |
|---------|-------------|
| `make dev-up` | Use local build (default) |
| `make dev-up-latest` | Use official latest release |
| `make dev-up-nightly` | Use nightly build |
| `make DEV_VERSION=3.0 dev-up` | Use specific version |

### With Test Directory / Custom Mounts

| Command | Description |
|---------|-------------|
| `make dev-up-test` | Start with test directory mounted at `/test` |
| `make DEV_MOUNT="../../test:/test:ro" dev-up` | Custom mount (test directory example) |
| `make DEV_MOUNT="/path/to/data:/data:ro" dev-up` | Custom mount (any directory) |
| `make DEV_VERSION=latest DEV_MOUNT="../../test:/test:ro" dev-up` | Combine version + mount |

### Logs

| Command | Description |
|---------|-------------|
| `make dev-logs` | View all logs (follow mode) |
| `make dev-logs-cn1` | View CN1 logs only |
| `make dev-logs-cn2` | View CN2 logs only |
| `make dev-logs-proxy` | View proxy logs only |
| `make dev-logs-tn` | View TN logs only |
| `make dev-logs-log` | View log service logs |

### Database Connection

| Command | Description |
|---------|-------------|
| `make dev-mysql` | Connect via proxy (recommended) |
| `make dev-mysql-cn1` | Connect directly to CN1 |
| `make dev-mysql-cn2` | Connect directly to CN2 |

Or manually:
```bash
mysql -h 127.0.0.1 -P 6009 -u root -p111   # Proxy
mysql -h 127.0.0.1 -P 16001 -u root -p111  # CN1
mysql -h 127.0.0.1 -P 16002 -u root -p111  # CN2
```

### Container Shell Access

| Command | Description |
|---------|-------------|
| `make dev-shell-cn1` | Enter CN1 container shell |
| `make dev-shell-cn2` | Enter CN2 container shell |

Or manually:
```bash
docker exec -it mo-cn1 sh
docker exec -it mo-cn2 sh
```

## 🔄 Common Workflows

### Development Workflow

```bash
# 1. Build and start
make dev-build
make dev-up

# 2. View logs during development
make dev-logs

# 3. Restart after changes
make dev-restart

# 4. Clean restart
make dev-clean
make dev-build && make dev-up
```

### Testing Workflow

```bash
# 1. Start with test directory
make dev-up-test

# 2. Connect and run tests
make dev-mysql
mysql> source /test/distributed/cases/your_test.sql;

# 3. View logs if errors occur
make dev-logs-cn1
```

### Quick Testing with Official Image

```bash
# Try latest without building
make dev-up-latest

# Or try nightly build
make dev-up-nightly
```

## 🛠️ Advanced Usage

### Custom Version
```bash
make DEV_VERSION=3.0 dev-up
make DEV_VERSION=v1.2.3 dev-up
```

### Custom Mount
```bash
make DEV_MOUNT="/path/to/data:/data:ro" dev-up
```

### Combine Options
```bash
# Use specific version + mount test dir
make DEV_VERSION=latest dev-up-test
```

## ⚙️ Configuration Management

### Quick Setup

```bash
# Create config from example
make dev-config-example

# Edit settings
vim etc/docker-multi-cn-local-disk/config.env

# Apply changes
make dev-config
make dev-restart
```

### Common Configurations

**Debug Logging (All Services):**
```bash
echo "LOG_LEVEL=debug" > etc/docker-multi-cn-local-disk/config.env
echo "LOG_FORMAT=json" >> etc/docker-multi-cn-local-disk/config.env
make dev-config && make dev-restart
```

**Increase Memory Cache for CNs Only:**
```bash
cat > etc/docker-multi-cn-local-disk/config.env << 'EOF'
CN1_MEMORY_CACHE=2GB
CN2_MEMORY_CACHE=2GB
EOF
make dev-config && make dev-restart
```

**Debug Logging for Proxy Only:**
```bash
cat > etc/docker-multi-cn-local-disk/config.env << 'EOF'
PROXY_LOG_LEVEL=debug
PROXY_LOG_FORMAT=json
EOF
make dev-config && make dev-restart
```

**High Performance (All Services):**
```bash
cat > etc/docker-multi-cn-local-disk/config.env << 'EOF'
MEMORY_CACHE=4GB
DISK_CACHE=50GB
LOG_LEVEL=warn
EOF
make dev-down && make dev-up
```

**Enable Monitoring (All Services):**
```bash
cat > etc/docker-multi-cn-local-disk/config.env << 'EOF'
DISABLE_TRACE=false
DISABLE_METRIC=false
EOF
make dev-config && make dev-restart
```

### Available Options

**Common Options (apply to all services):**

| Variable | Default | Description | Examples |
|----------|---------|-------------|----------|
| `LOG_LEVEL` | info | Log verbosity | debug, info, warn, error, fatal |
| `LOG_FORMAT` | console | Log output format | console, json |
| `LOG_MAX_SIZE` | 512 | Log file size (MB) | 512, 1024, 2048 |
| `MEMORY_CACHE` | 512MB | FileService memory cache | 512MB, 2GB, 4GB |
| `DISK_CACHE` | 8GB | FileService disk cache | 8GB, 20GB, 50GB |
| `DISABLE_TRACE` | true | Disable tracing | true, false |
| `DISABLE_METRIC` | true | Disable metrics | true, false |

**Service-Specific Options (override for specific service):**

Use service prefix to override for specific services:
- `CN1_*` - CN1 only (e.g., `CN1_MEMORY_CACHE=2GB`)
- `CN2_*` - CN2 only (e.g., `CN2_MEMORY_CACHE=2GB`)
- `PROXY_*` - Proxy only (e.g., `PROXY_LOG_LEVEL=debug`)
- `LOG_*` - Log service only (e.g., `LOG_LOG_LEVEL=debug`)
- `TN_*` - Transaction Node only (e.g., `TN_MEMORY_CACHE=4GB`)

**Examples:**
```bash
# Only CN1 gets larger cache
CN1_MEMORY_CACHE=2GB

# Only proxy gets debug logging
PROXY_LOG_LEVEL=debug

# Different cache for each service
CN1_MEMORY_CACHE=4GB
CN2_MEMORY_CACHE=2GB
TN_MEMORY_CACHE=1GB
```

## 🐛 Troubleshooting

### Check Status
```bash
make dev-ps
```

### View Logs
```bash
make dev-logs           # All services
make dev-logs-cn1       # Specific service
```

### Fix Permission Issues
```bash
# Stop services
make dev-down

# Fix permissions
sudo chown -R $(id -u):$(id -g) mo-data logs

# Restart
make dev-up
```

### Complete Reset
```bash
make dev-clean
sudo rm -rf mo-data logs
make dev-build && make dev-up
```

## 📝 Notes

- All commands should be run from the **project root directory**
- Data is stored in `mo-data/` and `logs/` directories
- Default password: `111`
- Default user: `root`
- Containers run with your user's UID/GID to avoid permission issues

