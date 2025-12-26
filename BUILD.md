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
# Build MatrixOne server (default, no typecheck for performance)
make build

# The binary will be created at: ./mo-service
```

### Step 4: Build Options

#### Basic Build Commands

```bash
# Clean build artifacts
make clean

# Build with debug symbols and race detector
make debug

# Build static binary with musl
make musl

# Build mo-tool utility
make mo-tool
```

#### Typecheck Options

MatrixOne provides optional type checking for `ToSliceNoTypeCheck` and `ToSliceNoTypeCheck2` functions. By default, typecheck is disabled for optimal performance.

```bash
# Build with typecheck enabled (for non-performance testing scenarios)
make build-typecheck
# or
make build TYPECHECK=1

# Default build (no typecheck, performance mode)
make build
```

**When to use typecheck:**
- ✅ Non-performance testing scenarios
- ✅ Development and debugging
- ✅ CI/CD validation
- ❌ Performance testing (disabled by default)
- ❌ Production builds (disabled by default)

**Note:** When race detector is enabled (`make debug` or `go build -race`), typecheck is automatically enabled for safety.

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
# Run unit tests
make ut

# Run CI tests (BVT + optional UT)
make ci

# Run docker compose BVT tests
make compose

# Run specific test package
go test ./pkg/your-package/...
```

#### Test Configuration

```bash
# Run CI tests with custom settings
make ci \
  UT_PARALLEL=4 \
  ENABLE_UT="true" \
  LAUNCH="launch" \
  GOPROXY="https://proxy.golang.com.cn,direct"

# Skip specific tests
make ut SKIP_TEST="pkg/frontend"
```

---

## Build Targets

### Build Commands

| Target | Description |
|--------|-------------|
| `make build` | Build mo-service binary (default, no typecheck) |
| `make build-typecheck` | Build with typecheck enabled |
| `make build TYPECHECK=1` | Build with typecheck enabled (alternative) |
| `make debug` | Build with race detector and debug symbols |
| `make musl` | Build static binary with musl |
| `make mo-tool` | Build mo-tool utility |
| `make clean` | Clean build artifacts |
| `make config` | Generate configuration |
| `make vendor-build` | Build vendor directory |
| `make pb` | Generate protobuf files |

### Testing Commands

| Target | Description |
|--------|-------------|
| `make ut` | Run unit tests |
| `make ci` | Run CI tests (BVT + optional UT) |
| `make compose` | Run docker compose BVT tests |

### Code Quality Commands

| Target | Description |
|--------|-------------|
| `make fmt` | Format Go code |
| `make static-check` | Run static analysis |

### Development Environment Commands

| Target | Description |
|--------|-------------|
| `make dev-build` | Build docker image (typecheck enabled by default) |
| `make dev-build TYPECHECK=0` | Build without typecheck (for performance testing) |
| `make dev-build-force` | Force rebuild (typecheck enabled by default) |
| `make dev-config` | Generate config from config.env (default: check-fraction=1000) |
| `make dev-up` | Start multi-CN cluster |
| `make dev-help` | Show all dev-* commands |

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

## Testing and CI

### Unit Tests

Run unit tests locally:

```bash
# Run all unit tests
make ut

# Run with timeout (Linux)
make ut SKIP_TEST="pkg/frontend"

# Run specific test package
go test -v ./pkg/container/vector/...
```

### CI Tests

The CI test suite includes BVT (Basic Verification Tests) and optional unit tests:

```bash
# Run CI tests with default settings
make ci

# Run CI tests with custom configuration
make ci \
  UT_PARALLEL=4 \
  ENABLE_UT="true" \
  LAUNCH="launch" \
  GOPROXY="https://proxy.golang.com.cn,direct"
```

**CI Test Parameters:**
- `UT_PARALLEL`: Number of parallel test workers (default: 1)
- `ENABLE_UT`: Enable unit tests (default: "false")
- `LAUNCH`: Launch configuration (default: "launch")
- `GOPROXY`: Go proxy URL for dependency downloads

### Docker Compose BVT Tests

Run BVT tests using docker compose:

```bash
# Run docker compose BVT tests
make compose

# Run with custom launch configuration
COMPOSE_LAUNCH="launch-multi-cn" make compose

# Clean up after tests
make compose-clean
```

### Building Test Images

#### CI Test Image

Build the CI test image (used by `make ci`):

```bash
# Build with default GOPROXY
docker build -f optools/bvt_ut/Dockerfile . \
  -t matrixorigin/matrixone:local-ci \
  --build-arg GOPROXY="https://proxy.golang.com.cn,direct"

# Build with custom GOPROXY
docker build -f optools/bvt_ut/Dockerfile . \
  -t matrixorigin/matrixone:local-ci \
  --build-arg GOPROXY="http://goproxy.goproxy.svc.cluster.local"
```

#### Production Image

Build the production image (used by regression tests):

```bash
# Build without typecheck (default, performance mode)
docker build -f optools/images/Dockerfile . \
  -t matrixorigin/matrixone:latest \
  --build-arg GOPROXY="http://goproxy.goproxy.svc.cluster.local"

# Build with typecheck enabled (for non-performance testing)
docker build -f optools/images/Dockerfile . \
  -t matrixorigin/matrixone:latest \
  --build-arg GOPROXY="http://goproxy.goproxy.svc.cluster.local" \
  --build-arg TYPECHECK=1
```

### Regression Testing

Regression tests use `optools/images/Dockerfile` to build test images. Typecheck is optional and disabled by default for performance.

#### Using Docker Compose

```bash
# Build and run regression tests (default, no typecheck)
cd etc/launch-tae-compose
docker-compose -f compose.yaml --profile launch-multi-cn up -d --build

# Build and run with typecheck enabled
TYPECHECK=1 docker-compose -f compose.yaml --profile launch-multi-cn up -d --build
```

#### Direct Docker Build

```bash
# Default build (no typecheck)
docker build -f optools/images/Dockerfile . \
  -t matrixorigin/matrixone:test \
  --build-arg GOPROXY="http://goproxy.goproxy.svc.cluster.local"

# Build with typecheck
docker build -f optools/images/Dockerfile . \
  -t matrixorigin/matrixone:test \
  --build-arg GOPROXY="http://goproxy.goproxy.svc.cluster.local" \
  --build-arg TYPECHECK=1
```

### Typecheck in CI/CD

Typecheck can be enabled in CI/CD pipelines for additional safety:

**GitHub Actions / CI Pipeline:**
```yaml
- name: Build with typecheck
  run: |
    docker build -f optools/images/Dockerfile . \
      -t matrixorigin/matrixone:test \
      --build-arg GOPROXY="${{ env.GOPROXY }}" \
      --build-arg TYPECHECK=1
```

**Jenkins / Other CI:**
```bash
# Enable typecheck in CI builds
docker build -f optools/images/Dockerfile . \
  -t $IMAGE_TAG \
  --build-arg GOPROXY="$GOPROXY" \
  --build-arg TYPECHECK=1
```

**Note:** 
- Default behavior (TYPECHECK=0) maintains backward compatibility
- Typecheck is automatically enabled when race detector is used (`-race` flag)
- For performance-critical tests, keep typecheck disabled

---

## Development Environment Configuration

### Memory Allocation Check (check-fraction)

The `check-fraction` configuration controls the frequency of memory deallocation safety checks. It helps detect memory management errors like double free and missing free (memory leaks).

**Default Value:** `1000` (for development environment via `make dev-config`)

**How it works:**
- On average, 1 in `check-fraction` deallocations will be checked
- Lower values = more frequent checks (better error detection, higher overhead)
- Higher values = less frequent checks (better performance, may miss errors)
- Set to `0` to disable checks (maximum performance, no error detection)

**Configuration:**

```bash
# Generate config with default check-fraction=1000
make dev-config

# Or customize in config.env
echo "CHECK_FRACTION=100" >> etc/docker-multi-cn-local-disk/config.env
make dev-config

# Service-specific override
echo "CN1_CHECK_FRACTION=100" >> etc/docker-multi-cn-local-disk/config.env
make dev-config
```

**Recommended Values:**
- **Development/Testing:** `100-1000` (good balance of detection and performance)
- **Production:** `65536` or higher (minimal overhead)
- **Debugging memory issues:** `1-10` (maximum detection, significant overhead)
- **Performance testing:** `0` or very large values (disable checks)

**What it checks:**
- **Double free:** Same memory address freed twice
- **Missing free:** Allocated memory not freed (detected via finalizer)

**Note:** This is different from production defaults (65536). Development environment uses 1000 by default to catch errors more frequently during development.

---

## Advanced Build Options

### Typecheck Configuration

MatrixOne provides compile-time type checking for `ToSliceNoTypeCheck` and `ToSliceNoTypeCheck2` functions through build tags.

**Build Tags:**
- `typecheck`: Enable type checking (optional)
- `race`: Automatically enables type checking (required for safety)

**Usage:**

```bash
# Local build with typecheck
make build TYPECHECK=1
# or
make build-typecheck

# Docker build with typecheck
docker build -f optools/images/Dockerfile . \
  --build-arg TYPECHECK=1

# Development build (typecheck enabled by default)
make dev-build

# Development build without typecheck (performance testing)
make dev-build TYPECHECK=0
```

**When Typecheck is Enabled:**
- Race detector mode (`-race`): Always enabled
- Development builds (`make dev-build`): Enabled by default
- CI/CD validation: Optional (recommended)
- Performance testing: Disabled by default
- Production builds: Disabled by default

### Cross-Compilation

MatrixOne supports cross-compilation for different platforms. See the Makefile for platform-specific targets.

### Optimization Flags

For production builds, consider:
- Using release builds (default)
- Disabling debug symbols
- Using static linking with musl (`make musl`)

---

## Contributing

For development and contribution guidelines, see [CONTRIBUTING.md](CONTRIBUTING.md).

