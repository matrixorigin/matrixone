# MatrixOne Installation Guide

This guide provides detailed instructions for installing MatrixOne on Linux and MacOS.

## Installation Methods

MatrixOne supports multiple installation methods:

- **[Using mo_ctl Tool](#using-moctl-tool)** - Recommended for quick deployment
- **[Building from Source](#building-from-source)** - For development and customization
- **[Using Docker](#using-docker)** - For containerized deployment

---

## Using mo_ctl Tool

[mo_ctl](https://github.com/matrixorigin/mo_ctl_standalone) is the official command-line tool for deploying, installing, and managing MatrixOne.

### Prerequisites

**Install Dependencies:**

1. **MySQL Client** (version 8.0.30 or later recommended)
   - Download from [MySQL Community Downloads](https://dev.mysql.com/downloads/mysql)
   - Configure MySQL client environment variables

### Installation Steps

**Step 1: Install mo_ctl tool**

```bash
wget https://raw.githubusercontent.com/matrixorigin/mo_ctl_standalone/main/install.sh && sudo -u $(whoami) bash +x ./install.sh
```

**Step 2: Configure deployment mode**

Choose between Git (source) or Docker deployment:

```bash
# Option A: Deploy from source
mo_ctl set_conf MO_PATH="yourpath"        # Set custom MatrixOne download path
mo_ctl set_conf MO_DEPLOY_MODE=git        # Set deployment method to git

# Option B: Deploy with Docker
mo_ctl set_conf MO_CONTAINER_DATA_HOST_PATH="/yourpath/mo/"  # Set data directory
mo_ctl set_conf MO_DEPLOY_MODE=docker     # Set deployment method to docker
```

**Step 3: Deploy MatrixOne**

```bash
# Deploy latest development version (main branch)
mo_ctl deploy main

# Or deploy specific stable version
mo_ctl deploy v1.2.0  # Replace with desired version
```

**Step 4: Start MatrixOne**

```bash
mo_ctl start
```

> **Note:** Initial startup takes approximately 20-30 seconds.

**Step 5: Connect to MatrixOne**

```bash
mo_ctl connect
```

### Common mo_ctl Commands

```bash
mo_ctl status          # Check MatrixOne status
mo_ctl stop            # Stop MatrixOne
mo_ctl restart         # Restart MatrixOne
mo_ctl upgrade         # Upgrade to latest version
mo_ctl uninstall       # Uninstall MatrixOne
```

For complete usage, see [mo_ctl Tool Documentation](https://docs.matrixorigin.cn/en/latest/MatrixOne/Reference/mo-tools/mo_ctl_standalone/).

---

## Building from Source

Build MatrixOne from source for development or customization.

### Prerequisites

**1. Install Go (version 1.22 required)**

Follow the [official Go installation guide](https://go.dev/doc/install).

**2. Install GCC/Clang**

Follow the [official GCC installation guide](https://gcc.gnu.org/install/).

**3. Install Git**

Install via the [official Git documentation](https://git-scm.com/download).

**4. Install MySQL Client**

Download from [MySQL Community Downloads](https://dev.mysql.com/downloads/mysql) and configure environment variables.

### Build Steps

**Step 1: Clone repository**

```bash
git clone https://github.com/matrixorigin/matrixone.git
cd matrixone
```

**Step 2: Prepare Dependencies**

```bash
go mod vendor
```

**Step 3: Build MatrixOne**

```bash
make build
```

**Step 4: Launch MatrixOne**

```bash
./mo-service -launch ./etc/launch/launch.toml
```

**Step 5: Connect using MySQL client**

```bash
mysql -h 127.0.0.1 -P 6001 -u root -p
# Default password: 111
```

---

## Using Docker

Quick deployment using Docker containers.

### Prerequisites

**1. Install Docker**

- Download from [Docker official website](https://docs.docker.com/get-docker/)
- Recommended version: 20.10.18 or later
- Maintain consistency between Docker client and server versions

**2. Install MySQL Client**

Download from [MySQL Community Downloads](https://dev.mysql.com/downloads/mysql) (version 8.0.30+ recommended).

### Deployment Steps

**Step 1: Pull and run MatrixOne**

```bash
docker run -d -p 6001:6001 --name matrixone matrixorigin/matrixone:latest
```

**Step 2: Connect to MatrixOne**

```bash
mysql -h 127.0.0.1 -P 6001 -u root -p111
```

### Docker Compose

For production deployment with persistent storage:

```yaml
version: '3'
services:
  matrixone:
    image: matrixorigin/matrixone:latest
    container_name: matrixone
    ports:
      - "6001:6001"
    volumes:
      - ./data:/var/lib/matrixone
    restart: unless-stopped
```

---

## Post-Installation

After installation, consider the following:

1. **Change Default Password**
   - See [Password Management](https://docs.matrixorigin.cn/en/latest/MatrixOne/Security/password-mgmt/)

2. **⚠️ Configure Performance (Important)**
   
   **Default cache size is too small for production workloads.** For better query performance, adjust the memory cache configuration:
   
   Edit your configuration file (`launch.toml` or `cn.toml`):
   ```toml
   [fileservice.cache]
   memory-capacity = "8GB"  # Adjust based on available memory
   ```
   
   **Recommended settings:**
   - Development: 2-4GB
   - Production: 8-32GB (depending on workload and available RAM)
   
   📖 **[Complete Configuration Guide →](https://docs.matrixorigin.cn/en/latest/MatrixOne/Reference/System-Parameters/standalone-configuration-settings/)**

3. **Install Python SDK**
   ```bash
   pip install matrixone-python-sdk
   ```

4. **Verify Installation**
   ```bash
   mysql -h 127.0.0.1 -P 6001 -u root -p111 -e "SELECT VERSION()"
   ```

---

## Troubleshooting

### Performance Issues

**Slow query performance?** The default cache size (512MB) is likely too small.

**Solution:** Increase memory cache in your configuration file:

```toml
[fileservice.cache]
memory-capacity = "8GB"  # Or higher based on your needs
```

Restart MatrixOne after configuration changes.

📖 **[Performance Configuration Guide →](https://docs.matrixorigin.cn/en/latest/MatrixOne/Reference/System-Parameters/standalone-configuration-settings/#default-parameters)**

### Connection Issues

- Verify MatrixOne is running: `mo_ctl status` (if using mo_ctl)
- Check port 6001 is not in use: `lsof -i :6001`
- Review logs: `mo_ctl watchdog` (if using mo_ctl)

### Build Issues

- Ensure Go version 1.22 is installed: `go version`
- Verify GCC is available: `gcc --version`
- Check disk space and memory availability

For more help, visit:
- [MatrixOne Documentation](https://docs.matrixorigin.cn/en/latest/)
- [GitHub Issues](https://github.com/matrixorigin/matrixone/issues)
- [Community Slack](http://matrixoneworkspace.slack.com)

