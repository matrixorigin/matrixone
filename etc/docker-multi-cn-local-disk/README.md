# MatrixOne Docker Multi-CN Local Disk Deployment

## Features

- ✅ **Multi-CN**: 2 CN nodes (scalable)
- ✅ **Local Disk**: Local disk shared storage (DISK backend)
- ✅ **With Proxy**: Load balancing proxy included
- ✅ **Docker Compose**: Containerized deployment, easy to manage
- ✅ **Bridge Network**: Bridge network with service isolation
- ✅ **Enhanced Script**: `start.sh` with auto permissions, versioning, and dynamic mounts
- ✅ **No Root Files**: All generated files use your user's permissions

## start.sh Quick Reference

| Option | Description | Example |
|--------|-------------|---------|
| `-v, --version` | Specify image version | `./start.sh -v latest up -d` |
| | Default: `local` (built image) | `./start.sh -v nightly up -d` |
| | Options: `local`, `latest`, `nightly`, `3.0` | `./start.sh -v 1.2.3 up -d` |
| `-m, --mount` | Mount extra directory | `./start.sh -m "../../test:/test:ro" up -d` |
| | Useful for SQL test files | Multiple mounts: separate calls |
| `-h, --help` | Show help message | `./start.sh -h` |
| (no options) | Use local image + auto UID/GID | `./start.sh up -d` |

## Quick Start

The `docker-compose.yml` supports both local build (recommended) and Docker Hub image.

### Recommended: Use start.sh (Enhanced Wrapper Script)

The easiest and most powerful way to manage MatrixOne:

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

**Why use start.sh?**
- ✅ **Auto Permissions**: Automatically detects your UID/GID (no root-owned files!)
- ✅ **Default Local Image**: Uses local build by default, no version confusion
- ✅ **Easy Versioning**: Switch versions with `-v latest|nightly|3.0`
- ✅ **Dynamic Mounts**: Add test directory with `-m` option
- ✅ **Clear Output**: Shows configuration before executing
- ✅ **No Manual Config**: Just works out of the box

**Quick Examples:**

```bash
# Most common: local build + test directory mounted
./start.sh build
./start.sh -m "../../test:/test:ro" up -d

# Connect and run tests
mysql -h 127.0.0.1 -P 6009 -u root -p111
mysql> source /test/distributed/cases/your_test.sql;

# Try official latest without building
./start.sh -v latest up -d
```

### Option 1: Build from Source (Default - Recommended)

**Best for most users - uses Go proxy acceleration:**

```bash
cd etc/docker-multi-cn-local-disk

# Build from source (uses Go proxy, ~10-15 minutes first time)
docker compose build

# Start with local build
IMAGE_NAME=matrixorigin/matrixone:local docker compose up -d
```

**Build with custom Go proxy (for faster downloads):**

```bash
# Set Go proxy and build
GOPROXY="https://goproxy.cn,direct" docker compose build

# Then start
IMAGE_NAME=matrixorigin/matrixone:local docker compose up -d
```

### Option 2: Use Docker Hub Image (Alternative)

**Note: May be slow or fail in some regions due to Docker Hub access limitations.**

**If you have good Docker Hub access:**

```bash
cd etc/docker-multi-cn-local-disk

# Start (will pull from Docker Hub)
docker compose up -d
```

**Use a specific version:**

```bash
# Use specific version
IMAGE_NAME=matrixorigin/matrixone:v1.0.0 docker compose up -d

# Or use nightly build
IMAGE_NAME=matrixorigin/matrixone:nightly docker compose up -d
```

**If Docker Hub is slow, configure Docker mirror (optional):**

```bash
# Edit Docker daemon config
sudo mkdir -p /etc/docker
sudo tee /etc/docker/daemon.json <<EOF
{
  "registry-mirrors": [
    "https://docker.mirrors.sjtug.sjtu.edu.cn",
    "https://docker.nju.edu.cn"
  ]
}
EOF

# Restart Docker
sudo systemctl restart docker
```

### Connect to Database

**Via Proxy (Recommended):**
```bash
mysql -h 127.0.0.1 -P 6009 -u root -p111
```

**Direct to CN1:**
```bash
mysql -h 127.0.0.1 -P 16001 -u root -p111
```

**Direct to CN2:**
```bash
mysql -h 127.0.0.1 -P 16002 -u root -p111
```

### Running SQL Scripts

**Method 1: Mount with start.sh (Easiest)**

Use the `-m` option to mount test directory:

```bash
# Start with test directory mounted
./start.sh -m "../../test:/test:ro" up -d

# Connect and run
mysql -h 127.0.0.1 -P 6009 -u root -p111
mysql> source /test/distributed/cases/your_test.sql;
```

**Method 2: Pipe SQL file directly**
```bash
mysql -h 127.0.0.1 -P 6009 -u root -p111 < test/distributed/cases/your_test.sql
```

**Method 3: Use mysql with -e option**
```bash
mysql -h 127.0.0.1 -P 6009 -u root -p111 -e "$(cat test/distributed/cases/your_test.sql)"
```

**Method 4: Copy file to container**
```bash
docker cp test/distributed/cases/your_test.sql mo-cn1:/tmp/test.sql
mysql -h 127.0.0.1 -P 16001 -u root -p111 -e "source /tmp/test.sql"
```

## Architecture

```
┌─────────────────────────────────────────┐
│   MatrixOne Multi-CN Local Disk        │
├─────────────────────────────────────────┤
│                                         │
│  ┌────────┐  ┌────────┐  ┌──────────┐  │
│  │ mo-log │  │ mo-tn  │  │ mo-proxy │  │
│  │ :32001 │  │        │  │  :6009   │  │
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

## Service Components

| Service | Container | Port | Description |
|---------|-----------|------|-------------|
| LogService | mo-log | 32001, 7001 | Log service and HAKeeper |
| TN | mo-tn | - | Transaction Node |
| CN1 | mo-cn1 | 16001 | Compute Node 1 |
| CN2 | mo-cn2 | 16002 | Compute Node 2 |
| Proxy | mo-proxy | 6009 | Load balancer proxy |

## Image Selection

### Building from Source (Default - Recommended)

**Advantages:**
- ✅ Use latest code from repository
- ✅ Custom modifications
- ✅ Debug with source code
- ✅ Test unreleased features

**Build Process:**
```bash
# Build from source (creates matrixorigin/matrixone:local)
docker compose build

# Start with local build
IMAGE_NAME=matrixorigin/matrixone:local docker compose up -d

# Or force rebuild
IMAGE_NAME=matrixorigin/matrixone:local docker compose up -d --build
```

**Switch between local build and official image:**
```bash
# Use official latest from Docker Hub
docker compose up -d

# Use local build
IMAGE_NAME=matrixorigin/matrixone:local docker compose up -d
```

**Build Requirements:**
- Docker with buildx support
- Go 1.24+
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

### Using Docker Hub Image (Alternative)

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
# Default (uses latest)
docker compose up -d

# Use specific version
IMAGE_NAME=matrixorigin/matrixone:v1.0.0 docker compose up -d

# Use nightly build
IMAGE_NAME=matrixorigin/matrixone:nightly docker compose up -d
```

**Docker Mirror Configuration (for faster Docker Hub access):**

If Docker Hub is slow or inaccessible, configure Docker mirrors:

```bash
# Create/edit Docker daemon config
sudo mkdir -p /etc/docker
sudo tee /etc/docker/daemon.json <<EOF
{
  "registry-mirrors": [
    "https://docker.mirrors.sjtug.sjtu.edu.cn",
    "https://docker.nju.edu.cn"
  ]
}
EOF

# Restart Docker daemon
sudo systemctl restart docker

# Verify configuration
docker info | grep -A 5 "Registry Mirrors"
```

## Storage

All containers share local disk storage:
- **Data Directory**: `../../mo-data/` (under project root)
- **Shared Storage**: `mo-data/shared/` (DISK backend)
- **Log Directory**: `../../logs/`

**File Permissions:**
- When using `./start.sh`: Files are automatically created with your user's permissions ✅
- When using `docker compose` directly: Set `DOCKER_UID` and `DOCKER_GID` environment variables
- Default fallback: `501:20` (macOS first user) - may need adjustment for Linux users

## Common Commands

**Using start.sh (Recommended):**

```bash
# Show help and all options
./start.sh -h

# View status
./start.sh ps

# View logs
./start.sh logs -f
tail -f ../../logs/*.log

# Restart services
./start.sh restart

# Stop services
./start.sh down

# Clean restart (removes data)
./start.sh down
rm -rf ../../mo-data ../../logs  # No sudo needed!
./start.sh up -d

# Build and use local image (default)
./start.sh build
./start.sh up -d

# Switch to different versions
./start.sh -v latest up -d      # Official latest
./start.sh -v nightly up -d     # Nightly build
./start.sh -v 3.0 up -d         # Specific version

# With test directory mounted
./start.sh -m "../../test:/test:ro" up -d

# Advanced: combine everything
./start.sh -v latest -m "../../test:/test:ro" up -d
```

**Using docker compose directly (need to set DOCKER_UID/DOCKER_GID):**

```bash
# Set permissions first
export DOCKER_UID=$(id -u) DOCKER_GID=$(id -g)

# Then run commands
docker compose ps
docker compose logs -f
docker compose restart
docker compose down
```

## Scale CN Nodes

1. Copy config: `cp cn2.toml cn3.toml`
2. Modify UUID, ports, etc.
3. Add `mo-cn3` service in `docker-compose.yml`
4. Start: `docker compose up -d mo-cn3`

## Comparison with Other Deployment Methods

| Method | Location | Features |
|--------|----------|----------|
| Standalone | `etc/launch/` | Single node, quick test |
| Manual Multi-CN | `etc/launch-multi-cn/` | Multi-CN, manual start |
| Manual with Proxy | `etc/launch-with-proxy/` | Proxy+Multi-CN, manual |
| **Docker Multi-CN** | `etc/docker-multi-cn-local-disk/` | **Containerized+Multi-CN+Local Storage** ✨ |

## Troubleshooting

### Fix Existing Root-Owned Files

If you already ran containers and have root-owned files, here's how to fix:

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
docker compose up -d

# Or build from source
docker compose build
IMAGE_NAME=matrixorigin/matrixone:local docker compose up -d
```

### Build Failures

If local build fails:

```bash
# Check Docker build context with verbose output
docker compose build --progress=plain

# Try different Go proxy
GOPROXY="https://goproxy.io,direct" docker compose build

# Or use pre-built image (if Docker Hub is accessible)
docker compose up -d  # Uses matrixorigin/matrixone:latest
```

### Docker Hub Access Issues

If you see "pull access denied" or timeout errors when using Docker Hub image:

**Solution 1: Use local build (recommended)**
```bash
docker compose build
IMAGE_NAME=matrixorigin/matrixone:local docker compose up -d
```

**Solution 2: Configure Docker mirror**
```bash
# See "Docker Mirror Configuration" section above
sudo tee /etc/docker/daemon.json <<EOF
{
  "registry-mirrors": [
    "https://docker.mirrors.sjtug.sjtu.edu.cn"
  ]
}
EOF
sudo systemctl restart docker
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

## Advanced Configuration

### Custom Image Tag

To use a specific version:

```bash
# Use specific Docker Hub version
IMAGE_NAME=matrixorigin/matrixone:v1.0.0 docker compose up -d

# Or set environment variable
export IMAGE_NAME=matrixorigin/matrixone:nightly
docker compose up -d
```

### Build with Custom Options

**Custom Go Proxy:**

```bash
# Method 1: Environment variable (recommended)
export GOPROXY="https://goproxy.io,direct"
docker compose build

# Method 2: Build argument
docker compose build --build-arg GOPROXY="https://goproxy.cn,direct"

# Method 3: One-liner
GOPROXY="https://proxy.golang.org,direct" docker compose build
```

**For faster builds in different regions:**

```bash
# Use regional mirror (Alibaba)
export GOPROXY="https://goproxy.cn,direct"
docker compose build

# Or combine multiple proxies for better redundancy
export GOPROXY="https://goproxy.cn,https://mirrors.aliyun.com/goproxy/,direct"
docker compose build
```

**Troubleshooting slow builds:**

If the build is slow, try these proxies in order:
1. Regional: `https://goproxy.cn,direct` or `https://mirrors.aliyun.com/goproxy/,direct`
2. Global: `https://goproxy.io,direct`
3. Official: `https://proxy.golang.org,direct`
4. No proxy: `direct`

## References

- [MatrixOne Documentation](https://docs.matrixone.io/)
- [Docker Hub - MatrixOne](https://hub.docker.com/r/matrixorigin/matrixone)
- [MatrixOne GitHub](https://github.com/matrixorigin/matrixone)
