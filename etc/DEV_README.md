# MatrixOne Development Guide

This guide provides comprehensive instructions for developers working with MatrixOne, covering local standalone deployment, multi-CN cluster setup, monitoring, and all `make dev-*` commands.

## Table of Contents

- [Quick Start](#quick-start)
- [Local Development Methods](#local-development-methods)
  - [Method 1: Simple Launch (File System Storage)](#method-1-simple-launch-file-system-storage)
  - [Method 2: Launch with MinIO Storage](#method-2-launch-with-minio-storage)
  - [Method 3: Multi-CN Cluster (Docker Compose)](#method-3-multi-cn-cluster-docker-compose)
  - [Important: Data Directory Compatibility](#important-data-directory-compatibility)
- [Development Commands Overview](#development-commands-overview)
- [Standalone MatrixOne Setup](#standalone-matrixone-setup)
- [Multi-CN Cluster Setup (Docker Compose)](#multi-cn-cluster-setup-docker-compose)
- [Monitoring and Metrics](#monitoring-and-metrics)
- [Configuration Management](#configuration-management)
- [Troubleshooting](#troubleshooting)

---

## Quick Start

### Option 1: Multi-CN Cluster (Recommended for Development)

**Basic Setup:**
```bash
# Build and start local multi-CN cluster
make dev-build && make dev-up

# Connect via proxy (load balanced across CNs)
mysql -h 127.0.0.1 -P 6001 -u root -p111
```

**With Monitoring:**
```bash
# Build and start
make dev-build && make dev-up

# Enable metrics (edit configs - see Multi-CN Workflow section)
make dev-edit-cn1  # Enable metrics in each service config
make dev-restart

# Start monitoring
make dev-up-grafana
make dev-check-grafana              # Wait until ready
make dev-create-dashboard-cluster   # Create dashboards

# Access Grafana at http://localhost:3000 (admin/admin)
```

### Option 2: Standalone MatrixOne

**Basic Setup:**
```bash
# Build and launch standalone MatrixOne
make build
./mo-service -launch ./etc/launch/launch.toml

# Connect
mysql -h 127.0.0.1 -P 6001 -u root -p111
```

**With Monitoring:**
```bash
# Build
make build

# Enable metrics (edit etc/launch/cn.toml, tn.toml, log.toml)
# Set: disableMetric = false, enable-metric-to-prom = true, status-port = 7001

# Launch
./mo-service -launch ./etc/launch/launch.toml

# Start monitoring
make dev-up-grafana-local
make dev-check-grafana              # Wait until ready
make dev-create-dashboard-local     # Create dashboards

# Access Grafana at http://localhost:3001 (admin/admin)
```

### View All Available Commands

```bash
make dev-help
```

**For detailed workflows, see:**
- [Local Development Methods](#local-development-methods) - **Start here!** Three ways to run MatrixOne locally with complete setup, data directories, and cleanup instructions
- [Standalone MatrixOne Setup](#standalone-matrixone-setup) - Complete standalone workflow details
- [Multi-CN Cluster Setup](#multi-cn-cluster-setup-docker-compose) - Complete multi-CN workflow details

---

## Local Development Methods

MatrixOne provides three methods for local development, each with different storage backends and use cases. Choose the method that best fits your needs.

### Method 1: Simple Launch (File System Storage)

**Best for:** Quick testing, simple development, learning MatrixOne

**Storage:** Local file system (DISK backend)

#### Quick Start

```bash
# 1. Build MatrixOne
make build

# 2. Start MatrixOne
./mo-service -launch ./etc/launch/launch.toml

# 3. Connect (in another terminal)
mysql -h 127.0.0.1 -P 6001 -u root -p111
```

#### With Grafana Monitoring

```bash
# 1. Enable metrics in config files
# Edit: etc/launch/cn.toml, etc/launch/tn.toml, etc/launch/log.toml
# Set: disableMetric = false, enable-metric-to-prom = true, status-port = 7001

# 2. Start MatrixOne
./mo-service -launch ./etc/launch/launch.toml

# 3. Start Grafana (in another terminal)
make dev-up-grafana-local

# 4. Wait for Grafana to be ready
make dev-check-grafana

# 5. Create dashboards
make dev-create-dashboard-local

# 6. Access Grafana at http://localhost:3001 (admin/admin)
```

#### Data Directories

- **MatrixOne Data**: `./mo-data/` (project root directory)
- **Metrics Data**: `prometheus-local-data/` (project root directory)
- **Grafana Data**: `grafana-local-data/` (project root directory)

#### Stop Services

```bash
# Stop MatrixOne process
# Press Ctrl+C in the terminal where mo-service is running
# Or find and kill the process:
ps aux | grep mo-service
kill <PID>

# Stop Grafana and Prometheus
make dev-down-grafana-local
```

#### Clean Up Data

```bash
# Stop all services first
# Then remove data directories:
rm -rf ./mo-data
rm -rf prometheus-local-data
rm -rf grafana-local-data
```

---

### Method 2: Launch with MinIO Storage

**Best for:** Testing object storage, S3-compatible storage, distributed storage scenarios

**Storage:** MinIO (S3-compatible object storage) + local file system

#### Quick Start

```bash
# 1. Build MatrixOne
make build

# 2. Start MinIO service
make dev-up-minio-local

# 3. Start MatrixOne with MinIO storage
make launch-minio
# Or manually:
# ./mo-service -launch etc/launch-minio-local/launch.toml

# 4. Connect (in another terminal)
mysql -h 127.0.0.1 -P 6001 -u root -p111
```

#### With Grafana Monitoring

```bash
# 1. Enable metrics in config files
# Edit: etc/launch-minio-local/cn.toml, etc/launch-minio-local/tn.toml, etc/launch-minio-local/log.toml
# Set: disableMetric = false, enable-metric-to-prom = true, status-port = 7001

# 2. Start MinIO
make dev-up-minio-local

# 3. Start MatrixOne
./mo-service -launch etc/launch-minio-local/launch.toml

# 4. Start Grafana (in another terminal)
make dev-up-grafana-local

# 5. Wait for Grafana to be ready
make dev-check-grafana

# 6. Create dashboards
make dev-create-dashboard-local

# 7. Access Grafana at http://localhost:3001 (admin/admin)
```

#### Data Directories

- **MatrixOne Data**: `etc/launch-minio-local/mo-data/`
- **MinIO Data**: `etc/launch-minio-local/mo-data/minio-data/`
- **Metrics Data**: `prometheus-local-data/` (project root directory)
- **Grafana Data**: `grafana-local-data/` (project root directory)

#### Stop Services

```bash
# Stop MatrixOne process
# Press Ctrl+C in the terminal where mo-service is running
# Or find and kill the process:
ps aux | grep mo-service
kill <PID>

# Stop MinIO
make dev-down-minio-local

# Stop Grafana and Prometheus
make dev-down-grafana-local
```

#### Clean Up Data

```bash
# Stop all services first
# Then remove the entire directory (contains both MatrixOne and MinIO data):
rm -rf etc/launch-minio-local/

# Also clean up monitoring data if needed:
rm -rf prometheus-local-data
rm -rf grafana-local-data
```

---

### Method 3: Multi-CN Cluster (Docker Compose)

**Best for:** Testing multi-node scenarios, load balancing, distributed features, production-like setup

**Storage:** Local disk shared storage (DISK backend)

#### Quick Start

```bash
# 1. Build Docker image
make dev-build

# 2. Start multi-CN cluster
make dev-up

# 3. Connect via proxy (load balanced)
mysql -h 127.0.0.1 -P 6001 -u root -p111
```

#### With Grafana Monitoring

```bash
# 1. Start cluster with Grafana
make dev-up-grafana

# 2. Enable metrics (edit configs interactively)
make dev-edit-cn1  # Enable metrics in each service config
make dev-edit-cn2
make dev-edit-tn
make dev-edit-log
make dev-edit-proxy

# 3. Restart services to apply changes
make dev-restart

# 4. Wait for Grafana to be ready
make dev-check-grafana

# 5. Create dashboards
make dev-create-dashboard-cluster

# 6. Access Grafana at http://localhost:3000 (admin/admin)
```

#### Data Directories

- **MatrixOne Data**: `mo-data/` (project root directory)
  - Shared storage: `mo-data/shared/`
- **Logs**: `logs/` (project root directory)
- **Metrics Data**: `prometheus-data/` (project root directory)
- **Grafana Data**: `grafana-data/` (project root directory)

#### Stop Services

```bash
# Stop all cluster services (including Grafana if started with dev-up-grafana)
make dev-down

# Note: If you started Grafana separately, you can stop it with:
# cd etc/docker-multi-cn-local-disk && docker compose --profile prometheus down
```

#### Clean Up Data

```bash
# Interactive cleanup (recommended - shows what will be deleted)
make dev-cleanup

# Or quick cleanup (WARNING: deletes all data without confirmation!)
make dev-clean

# This removes:
# - mo-data/ directory
# - logs/ directory
# - prometheus-data/ directory (if exists)
# - grafana-data/ directory (if exists)
```

---

### Important: Data Directory Compatibility

⚠️ **CRITICAL:** The `mo-data` directories used by **launch methods** (Method 1 and Method 2) and **multi-CN method** (Method 3) are **NOT compatible**.

- **Launch methods** use `./mo-data/` at project root with a different data format
- **Multi-CN method** uses `mo-data/` at project root with Docker volume mounts and shared storage

**Do NOT mix them!** If you switch between methods:

1. **Stop all services** first
2. **Backup or remove** the existing `mo-data/` directory
3. **Start fresh** with the new method

**Example:**
```bash
# Switching from launch to multi-CN:
# 1. Stop launch method (Ctrl+C)
# 2. Backup or remove:
mv mo-data mo-data-launch-backup
# 3. Start multi-CN:
make dev-up

# Switching from multi-CN to launch:
# 1. Stop multi-CN:
make dev-down
# 2. Backup or remove:
mv mo-data mo-data-multicn-backup
# 3. Start launch method:
./mo-service -launch ./etc/launch/launch.toml
```

---

### Comparison Table

| Feature | Method 1: Launch | Method 2: Launch + MinIO | Method 3: Multi-CN |
|---------|------------------|--------------------------|-------------------|
| **Storage** | Local file system | MinIO (S3-compatible) | Local shared disk |
| **Nodes** | Single process | Single process | Multi-CN (2+ nodes) |
| **Proxy** | No | No | Yes (load balancer) |
| **Containerized** | No | MinIO only | Yes (all services) |
| **Best For** | Quick testing | Object storage testing | Production-like testing |
| **Data Dir** | `./mo-data/` | `etc/launch-minio-local/mo-data/` | `mo-data/` |
| **Grafana Port** | 3001 | 3001 | 3000 |
| **Stop Command** | Ctrl+C or kill | Ctrl+C + `make dev-down-minio-local` | `make dev-down` |
| **Clean Command** | `rm -rf ./mo-data` | `rm -rf etc/launch-minio-local/` | `make dev-clean` |

---

## Development Commands Overview

### Build Commands

| Command | Description |
|---------|-------------|
| `make dev-build` | Build MatrixOne Docker image with `local` tag |
| `make dev-help` | Show all available `dev-*` commands |

### Cluster Management

| Command | Description |
|---------|-------------|
| `make dev-up` | Start multi-CN cluster (default: local image) |
| `make dev-up-latest` | Start with official latest image from Docker Hub |
| `make dev-up-nightly` | Start with nightly build image |
| `make dev-up-test` | Start with test directory mounted at `/test` |
| `make dev-down` | Stop multi-CN cluster |
| `make dev-restart` | Restart all services |
| `make dev-ps` | Show service status |
| `make dev-cleanup` | Interactive cleanup (stops containers, removes data directories) |

### Service-Specific Restart

| Command | Description |
|---------|-------------|
| `make dev-restart-cn1` | Restart CN1 only |
| `make dev-restart-cn2` | Restart CN2 only |
| `make dev-restart-proxy` | Restart Proxy only |
| `make dev-restart-tn` | Restart TN only |
| `make dev-restart-log` | Restart Log service only |

### Monitoring Commands

| Command | Description |
|---------|-------------|
| `make dev-up-grafana` | Start cluster with Grafana monitoring (port 3000) |
| `make dev-up-grafana-local` | Start Grafana dashboard for local MatrixOne (port 3001) |
| `make dev-check-grafana` | Check if Grafana services are ready (ports 3000, 3001) |
| `make dev-restart-grafana` | Restart Grafana and Prometheus (cluster mode) |
| `make dev-restart-grafana-local` | Restart Grafana and Prometheus (local mode) |
| `make dev-down-grafana-local` | Stop and remove local monitoring services |

### Logs

| Command | Description |
|---------|-------------|
| `make dev-logs` | View all logs (tail -f) |
| `make dev-logs-cn1` | View CN1 logs |
| `make dev-logs-cn2` | View CN2 logs |
| `make dev-logs-proxy` | View proxy logs |
| `make dev-logs-tn` | View TN logs |
| `make dev-logs-log` | View Log service logs |
| `make dev-logs-grafana` | View Grafana logs (cluster mode) |
| `make dev-logs-grafana-local` | View Grafana logs (local mode) |

### Database Connection

| Command | Description |
|---------|-------------|
| `make dev-mysql` | Connect to database via proxy (port 6001) |
| `make dev-mysql-cn1` | Connect directly to CN1 (port 16001) |
| `make dev-mysql-cn2` | Connect directly to CN2 (port 16002) |

### Configuration

| Command | Description |
|---------|-------------|
| `make dev-edit-cn1` | Edit CN1 configuration interactively |
| `make dev-edit-cn2` | Edit CN2 configuration interactively |
| `make dev-edit-proxy` | Edit Proxy configuration interactively |
| `make dev-edit-tn` | Edit TN configuration interactively |
| `make dev-edit-log` | Edit Log service configuration interactively |
| `make dev-edit-common` | Edit common configuration (all services) |
| `make dev-config` | Generate config files from `config.env` |
| `make dev-config-example` | Create `config.env.example` file |

### Utilities

| Command | Description |
|---------|-------------|
| `make dev-clean` | Stop and remove all data (WARNING: destructive!) |
| `make dev-cleanup` | Interactive cleanup (stops containers, removes data directories with confirmation) |
| `make dev-shell-cn1` | Open shell in CN1 container |
| `make dev-shell-cn2` | Open shell in CN2 container |
| `make dev-setup-docker-mirror` | Configure Docker registry mirror (for faster pulls) |

### Dashboard Creation

| Command | Description |
|---------|-------------|
| `make dev-create-dashboard` | Create dashboard (default: local mode, port 3001) |
| `make dev-create-dashboard-local` | Create local dashboard (port 3001) |
| `make dev-create-dashboard-cluster` | Create cluster dashboard (port 3000, docker compose) |
| `make dev-create-dashboard-k8s` | Create K8S dashboard |
| `make dev-create-dashboard-cloud-ctrl` | Create cloud control-plane dashboard |

**Custom Options:**
```bash
# Using make (recommended)
DASHBOARD_PORT=3001 make dev-create-dashboard
DASHBOARD_HOST=localhost make dev-create-dashboard
DASHBOARD_MODE=cloud make dev-create-dashboard
DASHBOARD_USERNAME=admin DASHBOARD_PASSWORD=admin make dev-create-dashboard

# Or directly using mo-tool
./mo-tool dashboard --host localhost --port 3001 --mode local
./mo-tool dashboard --mode cloud --port 3000 --datasource Prometheus
./mo-tool dashboard --username admin --password mypass
```

---

## Standalone MatrixOne Setup

For development and testing, you can run MatrixOne in standalone mode (single process with all services).

### Prerequisites

- Go 1.22 or later
- GCC/Clang compiler
- Make

### Complete Standalone Workflow

Here's the complete workflow for building, launching, and monitoring standalone MatrixOne:

#### Step 1: Build MatrixOne

```bash
# Build mo-service binary
make build

# Or build with race detector and debug symbols (for debugging)
make debug
```

**Output:** The `mo-service` binary will be created in the project root directory.

#### Step 2: Launch MatrixOne

```bash
# Start with default configuration (recommended - launches all services)
./mo-service -launch ./etc/launch/launch.toml
```

**Alternative: Manual Service Startup**

If you prefer to start services individually (useful for debugging):

```bash
# Terminal 1: Start Log service (contains HAKeeper)
./mo-service -cfg ./etc/launch/log.toml

# Terminal 2: Start TN (Transaction Node)
./mo-service -cfg ./etc/launch/tn.toml

# Terminal 3: Start CN (Compute Node)
./mo-service -cfg ./etc/launch/cn.toml
```

**Note:** When using `launch.toml`, all services start in a single process. The manual approach requires three separate terminals.

#### Step 3: Connect to MatrixOne

```bash
# Default connection (if using launch.toml)
mysql -h 127.0.0.1 -P 6001 -u root -p111

# Or if connecting directly to CN (when started manually)
mysql -h 127.0.0.1 -P 18001 -u root -p111
```

#### Step 4: Enable Metrics (Optional but Recommended)

To enable metrics collection, edit the configuration files:

**For launch.toml mode:**
```bash
# Edit CN configuration
vim ./etc/launch/cn.toml

# Edit TN configuration
vim ./etc/launch/tn.toml

# Edit Log service configuration
vim ./etc/launch/log.toml
```

In each file, ensure the following section is present and configured:

```toml
[observability]
disableMetric = false
enable-metric-to-prom = true
status-port = 7001
```

**Then restart MatrixOne:**
```bash
# Stop current instance (Ctrl+C)
# Restart with updated configuration
./mo-service -launch ./etc/launch/launch.toml
```

#### Step 5: Start Metrics Monitoring (Grafana Dashboard)

After enabling metrics, start the Grafana dashboard:

```bash
# Start Grafana dashboard for local standalone MatrixOne
make dev-up-grafana-local

# Wait for Grafana to initialize (2-3 minutes on first startup)
# Check if ready:
make dev-check-grafana

# Access Grafana at http://localhost:3001
# Username: admin
# Password: admin
```

**Create Dashboards:**

After Grafana is ready, create the MatrixOne dashboards:

```bash
# Create dashboards for standalone MatrixOne
make dev-create-dashboard-local

# Or with custom options:
DASHBOARD_PORT=3001 make dev-create-dashboard
```

**Verify Metrics:**

You can verify metrics are working:

```bash
# Check metrics endpoint directly
curl http://localhost:7001/metrics

# Or query Prometheus (if port exposed)
curl http://localhost:9091/api/v1/query?query=mo_sql_statement_total
```

### Standalone Workflow Summary

**Quick Start (Without Metrics):**
```bash
make build                                    # Build binary
./mo-service -launch ./etc/launch/launch.toml  # Launch MatrixOne
mysql -h 127.0.0.1 -P 6001 -u root -p111      # Connect
```

**Complete Workflow (With Metrics):**
```bash
# 1. Build
make build

# 2. Enable metrics in config files (edit cn.toml, tn.toml, log.toml)
#    Set: disableMetric = false, enable-metric-to-prom = true, status-port = 7001

# 3. Launch
./mo-service -launch ./etc/launch/launch.toml

# 4. Start monitoring
make dev-up-grafana-local
make dev-check-grafana                        # Wait until ready
make dev-create-dashboard-local               # Create dashboards

# 5. Connect and use
mysql -h 127.0.0.1 -P 6001 -u root -p111

# 6. Access Grafana at http://localhost:3001 (admin/admin)
```

### Build Options

```bash
# Standard build
make build

# Build with race detector and debug symbols
make debug

# Clean build artifacts
make clean
```

---

## Multi-CN Cluster Setup (Docker Compose)

The multi-CN setup provides a complete distributed cluster with:
- **Multi-CN**: 2 CN nodes (scalable)
- **Local Disk**: Local disk shared storage (DISK backend)
- **With Proxy**: Load balancing proxy included
- **Docker Compose**: Containerized deployment, easy to manage
- **Bridge Network**: Bridge network with service isolation
- **Enhanced Script**: `start.sh` with auto permissions, versioning, and dynamic mounts
- **No Root Files**: All generated files use your user's permissions

### Architecture

```
┌─────────────────────────────────────────┐
│   MatrixOne Multi-CN Local Disk        │
├─────────────────────────────────────────┤
│                                         │
│  ┌────────┐  ┌────────┐  ┌──────────┐  │
│  │ mo-log │  │ mo-tn  │  │ mo-proxy │  │
│  │ :32001 │  │        │  │  :6001   │  │
│  └───┬────┘  └───┬────┘  └────┬─────┘  │
│      │           │             │        │
│  ┌───┴───────────┴─────────────┴────┐   │
│  │    Local Shared Disk Storage     │   │
│  │      (mo-data/shared)            │   │
│  └───┬───────────┬──────────────────┘   │
│      │           │                      │
│  ┌───┴────┐  ┌───┴────┐                │
│  │ mo-cn1 │  │ mo-cn2 │                │
│  │ :16001 │  │ :16002 │                │
│  └────────┘  └────────┘                │
│                                         │
└─────────────────────────────────────────┘
```

### Service Components

| Service | Container | Port | Description |
|---------|-----------|------|-------------|
| LogService | mo-log | 32001, 7001 | Log service and HAKeeper |
| TN | mo-tn | - | Transaction Node |
| CN1 | mo-cn1 | 16001 | Compute Node 1 |
| CN2 | mo-cn2 | 16002 | Compute Node 2 |
| Proxy | mo-proxy | 6001 | Load balancer proxy |

### Complete Multi-CN Workflow

Here's the complete workflow for building, launching, and monitoring a multi-CN cluster:

#### Step 1: Build Docker Image

```bash
# Build MatrixOne Docker image with 'local' tag
make dev-build
```

**Note:** This builds the image from source code. Build time: ~10-15 minutes depending on network and CPU.

**Alternative: Use Official Images**
```bash
# Skip build and use official latest image
make dev-up-latest

# Or use nightly build
make dev-up-nightly
```

#### Step 2: Start Multi-CN Cluster

```bash
# Start cluster with local image
make dev-up
```

**What happens:**
- Starts 2 CN nodes (mo-cn1, mo-cn2)
- Starts TN (Transaction Node)
- Starts Log service (HAKeeper)
- Starts Proxy (load balancer on port 6001)
- Auto-generates configuration files if not present

**Note:** On first startup, default TOML configuration files will be automatically generated. These files are not tracked by git to prevent accidental commits of your local configurations.

#### Step 3: Connect to Cluster

```bash
# Connect via proxy (load balanced across CNs)
mysql -h 127.0.0.1 -P 6001 -u root -p111

# Or connect directly to CN1
mysql -h 127.0.0.1 -P 16001 -u root -p111

# Or connect directly to CN2
mysql -h 127.0.0.1 -P 16002 -u root -p111
```

#### Step 4: Enable Metrics (Optional but Recommended)

Metrics are **disabled by default**. To enable:

**Method 1: Interactive Configuration (Recommended)**
```bash
# Edit CN1 configuration
make dev-edit-cn1

# In the editor, find and uncomment:
# [observability]
# disableMetric = false
# enable-metric-to-prom = true
# status-port = 7001

# Save and repeat for other services
make dev-edit-cn2
make dev-edit-tn
make dev-edit-log
make dev-edit-proxy

# Restart services to apply changes
make dev-restart
```

**Method 2: Manual Configuration**
```bash
# Edit configuration files directly
vim etc/docker-multi-cn-local-disk/cn1.toml
vim etc/docker-multi-cn-local-disk/cn2.toml
vim etc/docker-multi-cn-local-disk/tn.toml
vim etc/docker-multi-cn-local-disk/log.toml
vim etc/docker-multi-cn-local-disk/proxy.toml

# Add or uncomment in each file:
# [observability]
# disableMetric = false
# enable-metric-to-prom = true
# status-port = 7001

# Restart services
make dev-restart
```

#### Step 5: Start Metrics Monitoring (Grafana Dashboard)

After enabling metrics, start the Grafana monitoring dashboard:

```bash
# Start cluster with Grafana monitoring
make dev-up-grafana

# Wait for Grafana to initialize (2-3 minutes on first startup)
# Check if ready:
make dev-check-grafana

# Access Grafana at http://localhost:3000
# Username: admin
# Password: admin
```

**Create Dashboards:**

After Grafana is ready, create the MatrixOne dashboards:

```bash
# Create dashboards for multi-CN cluster
make dev-create-dashboard-cluster

# Or with custom options:
DASHBOARD_MODE=cloud DASHBOARD_PORT=3000 make dev-create-dashboard
```

**Verify Metrics:**

```bash
# Check metrics from a service
docker exec mo-cn1 curl http://localhost:7001/metrics

# Or query Prometheus (if port exposed)
curl http://localhost:9090/api/v1/query?query=mo_sql_statement_total
```

### Multi-CN Workflow Summary

**Quick Start (Without Metrics):**
```bash
make dev-build                              # Build Docker image
make dev-up                                 # Start cluster
mysql -h 127.0.0.1 -P 6001 -u root -p111   # Connect
```

**Complete Workflow (With Metrics):**
```bash
# 1. Build
make dev-build

# 2. Start cluster
make dev-up

# 3. Enable metrics (edit configs or use interactive editor)
make dev-edit-cn1    # Edit and enable metrics
make dev-edit-cn2    # Edit and enable metrics
make dev-edit-tn     # Edit and enable metrics
make dev-edit-log    # Edit and enable metrics
make dev-edit-proxy  # Edit and enable metrics

# 4. Restart to apply metrics configuration
make dev-restart

# 5. Start monitoring
make dev-up-grafana
make dev-check-grafana                      # Wait until ready
make dev-create-dashboard-cluster           # Create dashboards

# 6. Connect and use
mysql -h 127.0.0.1 -P 6001 -u root -p111

# 7. Access Grafana at http://localhost:3000 (admin/admin)
```

**Using Official Images (Skip Build):**
```bash
# Start with latest official image
make dev-up-latest

# Or with nightly build
make dev-up-nightly

# Then follow steps 3-7 above for metrics
```

### Quick Start

```bash
# 1. Build Docker image
make dev-build

# 2. Start cluster
make dev-up

# 3. Connect
mysql -h 127.0.0.1 -P 6001 -u root -p111
```

**Note:** On first startup, default TOML configuration files will be automatically generated. These files are not tracked by git to prevent accidental commits of your local configurations.

### Using Official Images

```bash
# Start with latest official image
make dev-up-latest

# Or use specific version
make DEV_VERSION=1.0.0 dev-up

# Or use nightly build
make dev-up-nightly
```

### Mount Test Directory

```bash
# Start with test directory mounted
make dev-up-test

# In MySQL client:
mysql> source /test/distributed/cases/your_test.sql;
```

### Custom Mounts

```bash
# Mount custom directory
make DEV_MOUNT="../../test:/test:ro" dev-up
make DEV_MOUNT="/path/to/data:/data:ro" dev-up

# Combine options (version + mount)
make DEV_VERSION=latest DEV_MOUNT="../../test:/test:ro" dev-up
```

### Using start.sh Script (Alternative Method)

The `start.sh` script provides an enhanced wrapper with automatic permission handling:

```bash
cd etc/docker-multi-cn-local-disk

# Show help
./start.sh -h

# Default: Build and start with local image
./start.sh build
./start.sh up -d

# Use official latest version
./start.sh -v latest up -d

# Use specific version
./start.sh -v 3.0 up -d
./start.sh -v nightly up -d

# Mount test directory for source command
./start.sh -m "../../test:/test:ro" up -d

# Combine options
./start.sh -v latest -m "../../test:/test:ro" up -d

# Other commands
./start.sh down
./start.sh logs -f
./start.sh ps
./start.sh restart
```

**start.sh Quick Reference:**

| Option | Description | Example |
|--------|-------------|---------|
| `-v, --version` | Specify image version | `./start.sh -v latest up -d` |
| | Default: `local` (built image) | `./start.sh -v nightly up -d` |
| | Options: `local`, `latest`, `nightly`, `3.0` | `./start.sh -v 1.2.3 up -d` |
| `-m, --mount` | Mount extra directory | `./start.sh -m "../../test:/test:ro" up -d` |
| | Useful for SQL test files | Multiple mounts: separate calls |
| `-h, --help` | Show help message | `./start.sh -h` |
| (no options) | Use local image + auto UID/GID | `./start.sh up -d` |

**Why use start.sh?**
- ✅ **Auto Permissions**: Automatically detects your UID/GID (no root-owned files!)
- ✅ **Default Local Image**: Uses local build by default, no version confusion
- ✅ **Easy Versioning**: Switch versions with `-v latest|nightly|3.0`
- ✅ **Dynamic Mounts**: Add test directory with `-m` option
- ✅ **Clear Output**: Shows configuration before executing
- ✅ **No Manual Config**: Just works out of the box

### Running SQL Scripts

**Method 1: Mount with start.sh or make (Easiest)**

```bash
# Using make
make dev-up-test

# Using start.sh
./start.sh -m "../../test:/test:ro" up -d

# Connect and run
mysql -h 127.0.0.1 -P 6001 -u root -p111
mysql> source /test/distributed/cases/your_test.sql;
```

**Method 2: Pipe SQL file directly**
```bash
mysql -h 127.0.0.1 -P 6001 -u root -p111 < test/distributed/cases/your_test.sql
```

**Method 3: Use mysql with -e option**
```bash
mysql -h 127.0.0.1 -P 6001 -u root -p111 -e "$(cat test/distributed/cases/your_test.sql)"
```

**Method 4: Copy file to container**
```bash
docker cp test/distributed/cases/your_test.sql mo-cn1:/tmp/test.sql
mysql -h 127.0.0.1 -P 16001 -u root -p111 -e "source /tmp/test.sql"
```

### Image Selection

#### Building from Source (Default - Recommended)

**Advantages:**
- ✅ Use latest code from repository
- ✅ Custom modifications
- ✅ Debug with source code
- ✅ Test unreleased features

**Build Process:**
```bash
# Build from source (creates matrixorigin/matrixone:local)
make dev-build

# Or using start.sh
cd etc/docker-multi-cn-local-disk
./start.sh build
```

**Build Requirements:**
- Docker with buildx support
- Go 1.22+
- ~10-15 minutes build time (depending on network and CPU)

**Go Proxy Configuration:**

The build uses Go proxy to speed up dependency downloads. Default proxies (in order):
1. `https://goproxy.cn` - Alibaba Cloud
2. `https://mirrors.aliyun.com/goproxy/` - Aliyun Mirror
3. `https://goproxy.io` - Global
4. `direct` - Fallback to source

**Available Go Proxies:**

| Provider | Proxy | Usage |
|----------|-------|-------|
| Alibaba | `https://goproxy.cn` | `GOPROXY="https://goproxy.cn,direct" docker compose build` |
| Aliyun | `https://mirrors.aliyun.com/goproxy/` | `GOPROXY="https://mirrors.aliyun.com/goproxy/,direct" docker compose build` |
| GoProxy.io | `https://goproxy.io` | `GOPROXY="https://goproxy.io,direct" docker compose build` |
| Official | `https://proxy.golang.org` | `GOPROXY="https://proxy.golang.org,direct" docker compose build` |
| Athens | `https://athens.azurefd.net` | `GOPROXY="https://athens.azurefd.net,direct" docker compose build` |

**For faster builds in different regions:**
```bash
# Use regional mirror (Alibaba)
export GOPROXY="https://goproxy.cn,direct"
make dev-build

# Or combine multiple proxies for better redundancy
export GOPROXY="https://goproxy.cn,https://mirrors.aliyun.com/goproxy/,direct"
make dev-build
```

#### Using Docker Hub Image (Alternative)

**Advantages:**
- ✅ No build time - start immediately
- ✅ Official stable releases
- ✅ Smaller download size
- ✅ Updated regularly

**Disadvantages:**
- ⚠️ May be slow in some regions (Docker Hub access limitations)
- ⚠️ Requires Docker mirror configuration for faster access

**Available Tags:**
- `matrixorigin/matrixone:latest` - Latest stable release
- `matrixorigin/matrixone:nightly` - Nightly builds
- `matrixorigin/matrixone:v1.0.0` - Specific version

**Usage:**
```bash
# Use latest
make dev-up-latest

# Use specific version
make DEV_VERSION=1.0.0 dev-up

# Use nightly build
make dev-up-nightly
```

### Data Storage

All containers share local disk storage:
- **Data Directory**: `mo-data/` (under project root)
- **Shared Storage**: `mo-data/shared/` (DISK backend)
- **Log Directory**: `logs/` (service-specific log files)

**File Permissions:**
- When using `make dev-*` or `./start.sh`: Files are automatically created with your user's permissions ✅
- When using `docker compose` directly: Set `DOCKER_UID` and `DOCKER_GID` environment variables
- Default fallback: `501:20` (macOS first user) - may need adjustment for Linux users

### Scale CN Nodes

To add more CN nodes:

1. Copy config: `cp etc/docker-multi-cn-local-disk/cn2.toml etc/docker-multi-cn-local-disk/cn3.toml`
2. Modify UUID, ports, etc. in `cn3.toml`
3. Add `mo-cn3` service in `docker-compose.yml`
4. Start: `docker compose up -d mo-cn3`

---

## Monitoring and Metrics

MatrixOne provides comprehensive monitoring through Prometheus and Grafana.

### Enable Metrics Collection

Metrics are **disabled by default**. To enable:

#### Method 1: Interactive Configuration (Recommended)

```bash
# Edit service configuration
make dev-edit-cn1

# In the editor, find and uncomment:
# [observability]
# disableMetric = false
# enable-metric-to-prom = true
# status-port = 7001

# Save and restart
make dev-restart-cn1
```

#### Method 2: Manual Configuration

Edit the configuration files directly:
- `etc/docker-multi-cn-local-disk/cn1.toml`
- `etc/docker-multi-cn-local-disk/cn2.toml`
- `etc/docker-multi-cn-local-disk/tn.toml`
- `etc/docker-multi-cn-local-disk/proxy.toml`
- `etc/docker-multi-cn-local-disk/log.toml`

Add or uncomment:

```toml
[observability]
disableMetric = false
enable-metric-to-prom = true
status-port = 7001
```

Then restart the services.

### Monitoring Scenarios

#### Scenario 1: Monitor Docker Compose Cluster

```bash
# Start cluster with Grafana
make dev-up-grafana

# Access Grafana
# URL: http://localhost:3000
# Username: admin
# Password: admin
```

**Important:** On first startup, Grafana needs a few minutes to initialize its database and services. You can check if Grafana is ready using:

```bash
# Check Grafana service status (recommended)
make dev-check-grafana
```

This command checks both ports 3000 (cluster) and 3001 (local) and verifies that Grafana is responding to HTTP requests. Alternatively, you can manually check by accessing http://localhost:3000 - if the login page loads, Grafana is ready.

**What's included:**
- Prometheus: Collects metrics from all MatrixOne services
- Grafana: Visualization dashboard with pre-configured MatrixOne dashboards
- Auto-discovery: Automatically connects to all services in the cluster

**Prometheus Configuration:**
- Config file: `etc/docker-multi-cn-local-disk/prometheus.yml`
- Scrapes: `mo-cn1:7001`, `mo-cn2:7001`, `mo-tn:7001`, `mo-proxy:7001`, `mo-log:7001`

#### Scenario 2: Monitor Local Standalone MatrixOne

If you're running MatrixOne locally (not in Docker):

```bash
# Start Grafana dashboard for local MatrixOne
make dev-up-grafana-local

# Access Grafana
# URL: http://localhost:3001
# Username: admin
# Password: admin
```

**Important:** On first startup, Grafana needs a few minutes to initialize its database and services. You can check if Grafana is ready using:

```bash
# Check Grafana service status (recommended)
make dev-check-grafana
```

This command checks both ports 3000 (cluster) and 3001 (local) and verifies that Grafana is responding to HTTP requests. Alternatively, you can manually check by accessing http://localhost:3001 - if the login page loads, Grafana is ready.

**What's included:**
- Prometheus: Collects metrics from local MatrixOne services
- Grafana: Visualization dashboard
- Host access: Uses `host.docker.internal` to reach local services

**Prometheus Configuration:**
- Config file: `etc/docker-multi-cn-local-disk/prometheus-local.yml`
- Scrapes: `host.docker.internal:7001` (or your configured status-port)

**Note for Linux:** If `host.docker.internal` doesn't work, edit `prometheus-local.yml` and replace with your host IP address.

### Creating Dashboards

**Important:** Wait for Grafana to be ready before creating dashboards. Grafana needs time to initialize its database and services on first startup (typically 2-3 minutes). 

**Check if Grafana is ready:**
```bash
# Recommended: Use the check command
make dev-check-grafana

# Or manually: Access the web UI - if the login page loads, it's ready
# Cluster: http://localhost:3000
# Local:   http://localhost:3001
```

After Grafana is ready, you need to create the dashboards. MatrixOne provides a convenient make command:

**For Local Standalone MatrixOne (port 3001):**
```bash
make dev-create-dashboard-local
# Or with custom port:
DASHBOARD_PORT=3001 make dev-create-dashboard
# Or directly using mo-tool:
./mo-tool dashboard --mode local --port 3001
```

**For Docker Compose Cluster (port 3000):**
```bash
make dev-create-dashboard-cluster
# Or:
DASHBOARD_MODE=cloud DASHBOARD_PORT=3000 make dev-create-dashboard
# Or directly using mo-tool:
./mo-tool dashboard --mode cloud --port 3000
```

**Other Modes:**
```bash
# K8S mode
make dev-create-dashboard-k8s

# Cloud control-plane mode
make dev-create-dashboard-cloud-ctrl
```

**Custom Options:**
```bash
# Custom host and port
DASHBOARD_HOST=localhost DASHBOARD_PORT=3001 make dev-create-dashboard

# Custom credentials
DASHBOARD_USERNAME=admin DASHBOARD_PASSWORD=yourpassword make dev-create-dashboard

# Custom datasource name (for cloud/k8s modes)
DASHBOARD_MODE=cloud DASHBOARD_DATASOURCE=Prometheus make dev-create-dashboard
```

**Note:** The dashboard creation tool connects to Grafana and creates all MatrixOne dashboards. Make sure Grafana is running and has finished initializing (wait 2-3 minutes after first startup) before executing this command.

### Accessing Metrics

#### Via Grafana (Recommended)

1. Create dashboards (see above)
2. Open Grafana: http://localhost:3000 (cluster) or http://localhost:3001 (local)
3. Navigate to **Dashboards** → **Browse**
4. Select MatrixOne dashboards from the folder (Matrixone for cluster, Matrixone-Standalone for local)

#### Via Prometheus Query API

If Prometheus port is exposed (uncomment in `docker-compose.yml`):

```bash
# Query total SQL statements
curl 'http://localhost:9090/api/v1/query?query=mo_sql_statement_total'

# Query CN1 CPU usage
curl 'http://localhost:9090/api/v1/query?query=process_cpu_seconds_total{instance="mo-cn1:7001"}'
```

#### Direct from MatrixOne

```bash
# Query metrics endpoint directly
curl http://localhost:7001/metrics

# Or from container
docker exec mo-cn1 curl http://localhost:7001/metrics
```

### Available Metrics

MatrixOne exposes various metrics including:

- **SQL Metrics**: `mo_sql_statement_total`, `mo_sql_statement_duration_seconds`
- **Transaction Metrics**: `mo_txn_total`, `mo_txn_duration_seconds`
- **Storage Metrics**: `mo_storage_size_bytes`, `mo_storage_io_total`
- **Connection Metrics**: `mo_connection_total`, `mo_connection_active`
- **System Metrics**: CPU, memory, disk usage

See `pkg/util/metric/` for complete metric definitions.

---

## Configuration Management

### Configuration Files

Configuration files are located in `etc/docker-multi-cn-local-disk/`:

- `cn1.toml` - CN1 configuration
- `cn2.toml` - CN2 configuration
- `tn.toml` - TN configuration
- `proxy.toml` - Proxy configuration
- `log.toml` - Log service configuration

### Interactive Configuration Editor

```bash
# Edit specific service config
make dev-edit-cn1      # Opens CN1 config in your default editor
make dev-edit-cn2      # Opens CN2 config
make dev-edit-proxy    # Opens Proxy config
make dev-edit-tn       # Opens TN config
make dev-edit-log      # Opens Log service config
make dev-edit-common   # Opens common config (affects all services)
```

The editor will:
1. Open the configuration file
2. Show commented examples
3. Auto-generate config on save (if using `config.env`)

### Environment-Based Configuration

You can use `config.env` for centralized configuration:

```bash
# Create config.env from example
make dev-config-example
# or: cp etc/docker-multi-cn-local-disk/config.env.example etc/docker-multi-cn-local-disk/config.env

# Edit configuration (uncomment and modify lines)
vim etc/docker-multi-cn-local-disk/config.env

# Generate config files
make dev-config

# Or start (will auto-generate TOML configs)
make dev-up
```

**How it works:**
- `*.toml` files are automatically generated from `config.env` (or defaults if no config.env exists)
- These generated files are ignored by git to avoid commit conflicts
- On first `make dev-up`, default configurations are generated automatically
- When `config.env` exists, configurations are regenerated on every startup

**Common Configuration Options:**
- `LOG_LEVEL` - debug, info, warn, error, fatal (default: info)
- `LOG_FORMAT` - console, json (default: console)
- `MEMORY_CACHE` - e.g., 512MB, 2GB, 4GB (default: 512MB)
- `DISK_CACHE` - e.g., 8GB, 20GB, 50GB (default: 8GB)
- `DISABLE_TRACE` - true, false (default: true)
- `DISABLE_METRIC` - true, false (default: true)

**Service-Specific Configuration:**
Use service prefixes to override settings for specific services:
- `CN1_*` - CN1 only (e.g., `CN1_MEMORY_CACHE=2GB`)
- `CN2_*` - CN2 only (e.g., `CN2_MEMORY_CACHE=2GB`)
- `PROXY_*` - Proxy only (e.g., `PROXY_LOG_LEVEL=debug`)
- `LOG_*` - Log service only (e.g., `LOG_LOG_LEVEL=debug`)
- `TN_*` - Transaction Node only (e.g., `TN_MEMORY_CACHE=4GB`)

**Configuration Priority:**
1. Service-specific (highest priority)
2. Common configuration
3. Default values

### Advanced Configuration Examples

#### Example 1: Increase Memory Cache for CNs Only

```bash
# Only increase cache for CN1 and CN2, keep others default
cat > etc/docker-multi-cn-local-disk/config.env << 'EOF'
CN1_MEMORY_CACHE=2GB
CN2_MEMORY_CACHE=2GB
EOF

# Apply and restart
make dev-config
make dev-restart
```

#### Example 2: Debug Logging for Proxy Only

```bash
# Enable debug logging only for proxy, others stay at info
cat > etc/docker-multi-cn-local-disk/config.env << 'EOF'
PROXY_LOG_LEVEL=debug
PROXY_LOG_FORMAT=json
EOF

# Apply
make dev-config
make dev-restart
```

#### Example 3: Different Cache Sizes Per Service

```bash
# Different cache sizes for each service
cat > etc/docker-multi-cn-local-disk/config.env << 'EOF'
CN1_MEMORY_CACHE=4GB
CN2_MEMORY_CACHE=2GB
TN_MEMORY_CACHE=1GB
EOF

# Apply
make dev-down
make dev-up
```

#### Example 4: Mixed - Common + Service-Specific

```bash
# Common log level for all, but debug for proxy
cat > etc/docker-multi-cn-local-disk/config.env << 'EOF'
LOG_LEVEL=info              # Default for all services
PROXY_LOG_LEVEL=debug        # Override for proxy only
CN1_MEMORY_CACHE=2GB         # Override for CN1 only
EOF

# Apply
make dev-config
make dev-restart
```

#### Example 5: High Performance Setup (All Services)

```bash
# Increase cache for all services
cat > etc/docker-multi-cn-local-disk/config.env << 'EOF'
MEMORY_CACHE=4GB
DISK_CACHE=50GB
LOG_LEVEL=warn
EOF

# Apply
make dev-down
make dev-up
```

#### Quick Config Changes

```bash
# One-liner: increase CN1 memory cache only
echo "CN1_MEMORY_CACHE=2GB" > etc/docker-multi-cn-local-disk/config.env
make dev-config && make dev-restart

# View current configs
cat etc/docker-multi-cn-local-disk/cn1.toml | grep -A3 "\[fileservice.cache\]"
```

### Common Configuration Options

#### Enable Metrics

```toml
[observability]
disableMetric = false
enable-metric-to-prom = true
status-port = 7001
```

#### Adjust Log Level

```toml
[log]
level = "debug"  # Options: debug, info, warn, error
```

#### Memory Configuration

```toml
[cn]
memory-capacity = "8GB"  # Adjust based on available RAM
```

#### Port Configuration

```toml
[cn]
port-base = 18000  # CN1 uses 18000, CN2 uses 18001, etc.
```

---

## Troubleshooting

### Docker Image Pull Failures

If `docker pull` is slow or fails:

```bash
# Configure Docker registry mirror
sudo make dev-setup-docker-mirror

# Or manually:
cd etc/docker-multi-cn-local-disk
sudo ./setup-docker-mirror.sh
```

### Services Not Starting

1. **Check logs:**
   ```bash
   make dev-logs
   ```

2. **Check service status:**
   ```bash
   make dev-ps
   ```

3. **Verify ports are not in use:**
   ```bash
   # Check if ports are occupied
   lsof -i :6001
   lsof -i :16001
   lsof -i :16002
   ```

4. **Check data directory permissions:**
   ```bash
   # Ensure mo-data and logs directories are writable
   ls -la mo-data logs
   ```

### Metrics Not Showing in Grafana

1. **Verify metrics are enabled:**
   ```bash
   # Check if metrics endpoint responds
   curl http://localhost:7001/metrics
   ```

2. **Check Prometheus targets:**
   - Open Prometheus UI (if port exposed): http://localhost:9090/targets
   - Verify all targets are "UP"

3. **Check Grafana data source:**
   - Open Grafana: http://localhost:3000
   - Go to **Configuration** → **Data Sources**
   - Verify Prometheus connection is working

4. **Restart monitoring services:**
   ```bash
   make dev-restart-grafana
   ```

### Connection Refused

1. **Verify services are running:**
   ```bash
   make dev-ps
   ```

2. **Check service logs:**
   ```bash
   make dev-logs-cn1
   ```

3. **Verify port mapping:**
   ```bash
   docker ps | grep mo-
   ```

### Permission Issues

If you see permission errors:

```bash
# Check file ownership
ls -la mo-data logs

# Fix ownership (if needed)
sudo chown -R $(id -u):$(id -g) mo-data logs
```

### Clean Start

If you need to start fresh:

```bash
# Option 1: Interactive cleanup (recommended - shows what will be deleted)
make dev-cleanup

# Option 2: Quick cleanup (WARNING: This deletes all data without confirmation!)
make dev-clean

# Then rebuild and start
make dev-build && make dev-up
```

**Note:** `make dev-cleanup` provides an interactive interface that shows all data directories and asks for confirmation before deletion. It also handles root-owned files that may require sudo.

### Build Failures

If `make dev-build` fails:

1. **Check Docker is running:**
   ```bash
   docker ps
   ```

2. **Check disk space:**
   ```bash
   df -h
   ```

3. **Check build logs:**
   ```bash
   cd etc/docker-multi-cn-local-disk
   ./start.sh build
   ```

4. **Try different Go proxy:**
   ```bash
   # Check Docker build context with verbose output
   cd etc/docker-multi-cn-local-disk
   docker compose build --progress=plain
   
   # Try different Go proxy
   GOPROXY="https://goproxy.io,direct" docker compose build
   
   # Or use pre-built image (if Docker Hub is accessible)
   docker compose up -d  # Uses matrixorigin/matrixone:latest
   ```

### Fix Existing Root-Owned Files

If you already ran containers and have root-owned files:

```bash
cd etc/docker-multi-cn-local-disk

# Stop containers
./start.sh down

# Fix permissions (need sudo because files are currently root-owned)
sudo chown -R $(id -u):$(id -g) ../../mo-data ../../logs

# Restart with correct permissions
./start.sh up -d
```

**Or clean restart (removes all data):**
```bash
./start.sh down
sudo rm -rf ../../mo-data ../../logs
./start.sh up -d  # Will create with your user permissions
```

### Image Not Found

If you see `image not found` error:

```bash
# Use official image
cd etc/docker-multi-cn-local-disk
docker compose up -d

# Or build from source
docker compose build
IMAGE_NAME=matrixorigin/matrixone:local docker compose up -d
```

### Docker Hub Access Issues

If you see "pull access denied" or timeout errors when using Docker Hub image:

**Solution 1: Use local build (recommended)**
```bash
make dev-build
make dev-up
```

**Solution 2: Configure Docker mirror**
```bash
sudo make dev-setup-docker-mirror
# Or manually:
cd etc/docker-multi-cn-local-disk
sudo ./setup-docker-mirror.sh
```

### Network Issues

If containers can't communicate:

```bash
# Check network
docker network ls
docker network inspect docker-multi-cn-local-disk_matrixone-net

# Verify container names resolve
docker exec mo-cn1 ping mo-log
```

---

## Comparison with Other Deployment Methods

| Method | Location | Features |
|--------|----------|----------|
| Standalone | `etc/launch/` | Single node, quick test |
| Manual Multi-CN | `etc/launch-multi-cn/` | Multi-CN, manual start |
| Manual with Proxy | `etc/launch-with-proxy/` | Proxy+Multi-CN, manual |
| **Docker Multi-CN** | `etc/docker-multi-cn-local-disk/` | **Containerized+Multi-CN+Local Storage** ✨ |

## Additional Resources

- **Main README**: See project root `README.md`
- **Build Guide**: See `BUILD.md` for detailed build instructions
- **Installation Guide**: See `INSTALLATION.md` for production deployment
- **Makefile Help**: Run `make help` or `make dev-help`
- **Docker Compose**: See `etc/docker-multi-cn-local-disk/docker-compose.yml` for service definitions
- **MatrixOne Documentation**: https://docs.matrixorigin.cn/
- **Docker Hub - MatrixOne**: https://hub.docker.com/r/matrixorigin/matrixone

---

## Quick Reference

### Most Common Workflows

**Daily Development:**
```bash
make dev-up                    # Start cluster
make dev-mysql                 # Connect to database
make dev-logs-cn1              # Check logs
make dev-restart-cn1           # Restart after config change
```

**With Monitoring:**
```bash
make dev-up-grafana            # Start with monitoring
# Access http://localhost:3000
```

**Testing:**
```bash
make dev-up-test               # Start with test directory
mysql -h 127.0.0.1 -P 6001 -u root -p111
mysql> source /test/distributed/cases/your_test.sql;
```

**Configuration Changes:**
```bash
make dev-edit-cn1              # Edit config
make dev-restart-cn1           # Apply changes
```

**Cleanup:**
```bash
make dev-down                  # Stop services
make dev-clean                 # Remove all data (WARNING!)
```

---

For more information, see the main project documentation or run `make dev-help` for a complete command list.
