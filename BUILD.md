# Building MatrixOne from Source

This guide provides detailed instructions for building and running MatrixOne from source code using Make.

## Prerequisites

Before building MatrixOne, ensure you have the following installed:

### Required Tools

1. **Go** (version 1.22)
   - [Installation Guide](https://go.dev/doc/install)
   - Verify: `go version`

2. **GCC/Clang**
   - [GCC Installation](https://gcc.gnu.org/install/)
   - Verify: `gcc --version`

3. **Git**
   - [Git Installation](https://git-scm.com/download)
   - Verify: `git --version`

4. **Make**
   - Usually pre-installed on Linux/MacOS
   - Verify: `make --version`

5. **MySQL Client** (version 8.0.30+)
   - [MySQL Downloads](https://dev.mysql.com/downloads/mysql)
   - Verify: `mysql --version`

---

## Building MatrixOne

### Step 1: Clone Repository

```bash
git clone https://github.com/matrixorigin/matrixone.git
cd matrixone
```

### Step 2: Prepare Dependencies

```bash
# Download and vendor dependencies
go mod vendor
```

### Step 3: Build

```bash
# Build MatrixOne server
make build

# The binary will be created at: ./mo-service
```

### Step 4: Build Options

```bash
# Clean build artifacts
make clean

# Build with debug symbols
make debug

# Build specific components
make config          # Build configuration
make service         # Build service only

# Run tests
make test

# Run unit tests
make ut

# Check code style
make fmt-check
```

---

## Running MatrixOne

### Launch Server

```bash
# Default launch with local configuration
./mo-service -launch ./etc/launch/launch.toml
```

### Configuration

Edit `./etc/launch/launch.toml` to customize your MatrixOne deployment.

**⚠️ Important: Adjust Memory Cache for Better Performance**

The default cache size (512MB) is too small for production workloads. Update the configuration for optimal query performance:

```toml
[fileservice.cache]
memory-capacity = "8GB"  # Adjust based on your available memory
```

**Recommended memory-capacity settings:**
- **Development/Testing:** 2-4GB
- **Production:** 8-32GB (depending on workload and available RAM)
- **High-performance:** 32GB+ for large-scale analytics

**Other important settings:**
- `service-host` - Server listening address (default: 0.0.0.0)
- `service-port` - MySQL protocol port (default: 6001)
- `data-dir` - Data storage directory
- `log-level` - Logging level (debug, info, warn, error)

📖 **[Complete Configuration Reference →](https://docs.matrixorigin.cn/en/latest/MatrixOne/Reference/System-Parameters/standalone-configuration-settings/)**

### Connect to Server

```bash
mysql -h 127.0.0.1 -P 6001 -u root -p
# Default password: 111
```

---

## Development Workflow

### Quick Development Cycle

```bash
# 1. Make changes to source code

# 2. Update dependencies (only if go.mod changed)
go mod vendor

# 3. Rebuild
make build

# 4. Restart server
# Stop: Ctrl+C or kill process
# Start: ./mo-service -launch ./etc/launch/launch.toml
```

### Dependency Management

```bash
# Download dependencies
go mod download

# Vendor dependencies (required before build)
go mod vendor

# Clean up unused dependencies
go mod tidy

# Update specific dependency
go get -u github.com/package/name
go mod vendor
```

### Running Tests

```bash
# Run all tests
make test

# Run unit tests only
make ut

# Run integration tests
make bvt

# Run specific test package
go test ./pkg/your-package/...
```

---

## Build Targets

Common Make targets:

| Target | Description |
|--------|-------------|
| `make build` | Build MatrixOne server |
| `make clean` | Clean build artifacts |
| `make config` | Generate configuration |
| `make test` | Run all tests |
| `make ut` | Run unit tests |
| `make bvt` | Run BVT tests |
| `make fmt` | Format code |
| `make lint` | Run linters |

---

## Troubleshooting

### Build Failures

**Go version mismatch:**
```bash
go version  # Must be 1.22
```

**Missing dependencies:**
```bash
go mod download
go mod vendor  # Required before build
go mod tidy
```

**Vendor directory issues:**
```bash
# Remove vendor directory and re-vendor
rm -rf vendor/
go mod vendor
```

**Clean rebuild:**
```bash
make clean
go mod vendor  # Re-vendor dependencies
make build
```

### Runtime Issues

**Slow query performance:**

The default cache size (512MB) is too small. Increase memory cache in `./etc/launch/launch.toml`:

```toml
[fileservice.cache]
memory-capacity = "8GB"  # Adjust based on available memory
```

**Recommended:** 2-4GB for dev, 8-32GB for production.

📖 **[Configuration Guide →](https://docs.matrixorigin.cn/en/latest/MatrixOne/Reference/System-Parameters/standalone-configuration-settings/#default-parameters)**

**Port already in use:**
```bash
# Check what's using port 6001
lsof -i :6001

# Kill process if needed
kill -9 <PID>
```

**Permission issues:**
```bash
# Ensure proper permissions for data directories
chmod -R 755 ./data
```

---

## Advanced Build Options

_This section will be expanded with advanced build configurations, cross-compilation, optimization flags, etc._

---

## Contributing

For development and contribution guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md).

