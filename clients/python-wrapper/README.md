# MatrixOne Python SDK

[![PyPI version](https://badge.fury.io/py/matrixone-python-sdk.svg)](https://badge.fury.io/py/matrixone-python-sdk)
[![Python Support](https://img.shields.io/pypi/pyversions/matrixone-python-sdk.svg)](https://pypi.org/project/matrixone-python-sdk/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://github.com/matrixorigin/matrixone/workflows/CI/badge.svg)](https://github.com/matrixorigin/matrixone/actions)

A high-level Python SDK for MatrixOne that provides SQLAlchemy-like interface for database operations, snapshot management, PITR, restore operations, table cloning, and mo-ctl integration.

## ✨ Features

- 🚀 **High Performance**: Optimized for MatrixOne database operations
- 🔄 **Async Support**: Full async/await support with AsyncClient
- 📸 **Snapshot Management**: Create and manage database snapshots
- ⏰ **Point-in-Time Recovery**: PITR functionality for data recovery
- 🔄 **Table Cloning**: Clone databases and tables efficiently
- 👥 **Account Management**: User and role management
- 📊 **Pub/Sub**: Publication and subscription support
- 🔧 **Version Management**: Automatic backend version detection and compatibility checking
- 🛡️ **Type Safety**: Full type hints support
- 📚 **SQLAlchemy Integration**: Seamless SQLAlchemy integration

## 🚀 Installation

```bash
pip install matrixone-python-sdk
```

### Development Installation

#### Using Virtual Environment (Recommended)

```bash
git clone https://github.com/matrixorigin/matrixone.git
cd matrixone/clients/python-wrapper

# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate

# Quick setup with Makefile
make dev-setup

# Or manual setup
pip install -e .
```

#### Using Conda Environment

```bash
git clone https://github.com/matrixorigin/matrixone.git
cd matrixone/clients/python-wrapper

# Create conda environment
conda create -n matrixone-dev python=3.10
conda activate matrixone-dev

# Quick setup with Makefile
make dev-setup

# Or manual setup
pip install -e .
```

#### Direct Installation (Not Recommended)

```bash
git clone https://github.com/matrixorigin/matrixone.git
cd matrixone/clients/python-wrapper

# Quick setup with Makefile
make dev-setup

# Or manual setup
pip install -e .
```

## Quick Start

### Basic Usage

```python
from matrixone import Client

# Create and connect to MatrixOne
client = Client()
client.connect(
    host='localhost',
    port=6001,
    user='root',
    password='111',
    database='test'
)

# Execute queries
result = client.execute("SELECT 1 as test")
print(result.fetchall())

# Get backend version (auto-detected)
version = client.get_backend_version()
print(f"MatrixOne version: {version}")

client.disconnect()
```

### Async Usage

```python
import asyncio
from matrixone import AsyncClient

async def main():
    client = AsyncClient()
    await client.connect(
        host='localhost',
        port=6001,
        user='root',
        password='111',
        database='test'
    )
    
    result = await client.execute("SELECT 1 as test")
    print(await result.fetchall())
    
    await client.disconnect()

asyncio.run(main())
```

### Snapshot Management

```python
# Create a snapshot
snapshot = client.snapshots.create(
    name='my_snapshot',
    level='cluster',
    description='Backup before migration'
)

# List snapshots
snapshots = client.snapshots.list()
for snap in snapshots:
    print(f"Snapshot: {snap.name}, Created: {snap.created_at}")

# Clone database from snapshot
client.snapshots.clone_database(
    target_db='new_database',
    source_db='old_database',
    snapshot_name='my_snapshot'
)
```

### Version Management

```python
# Check if feature is available
if client.is_feature_available('snapshot_creation'):
    snapshot = client.snapshots.create('my_snapshot', 'cluster')
else:
    hint = client.get_version_hint('snapshot_creation')
    print(f"Feature not available: {hint}")

# Check version compatibility
if client.check_version_compatibility('3.0.0', '>='):
    print("Backend supports 3.0.0+ features")
```

## MatrixOne Version Support

The SDK automatically detects MatrixOne backend versions and handles compatibility:

- **Development Version**: `8.0.30-MatrixOne-v` → `999.0.0` (highest priority)
- **Release Version**: `8.0.30-MatrixOne-v3.0.0` → `3.0.0`
- **Legacy Format**: `MatrixOne 3.0.1` → `3.0.1`

```python
# Check if running development version
if client.is_development_version():
    print("Running development version - all features available")
else:
    print(f"Running release version: {client.get_backend_version()}")
```

## Advanced Features

### PITR (Point-in-Time Recovery)

```python
# Create PITR for cluster
pitr = client.pitr.create_cluster_pitr(
    name='cluster_pitr',
    range_value=7,
    range_unit='d'
)

# Restore to specific time
restore_point = datetime(2024, 1, 15, 10, 30, 0)
client.pitr.restore_to_time(restore_point)
```

### Account Management

```python
# Create user
user = client.account.create_user(
    username='newuser',
    password='password123',
    description='New user account'
)

# Create role
role = client.account.create_role(
    role_name='analyst',
    description='Data analyst role'
)

# Grant privileges
client.account.grant_privilege(
    user='newuser',
    role='analyst',
    privileges=['SELECT', 'INSERT']
)
```

### Pub/Sub Operations

```python
# Create publication
publication = client.pubsub.create_publication(
    name='data_changes',
    tables=['users', 'orders'],
    description='User and order changes'
)

# Create subscription
subscription = client.pubsub.create_subscription(
    name='data_sync',
    publication_name='data_changes',
    target_tables=['users_backup', 'orders_backup']
)
```

## Configuration

### Connection Parameters

```python
client = Client(
    connection_timeout=30,
    query_timeout=300,
    auto_commit=True,
    charset='utf8mb4',
    enable_performance_logging=True,
    enable_sql_logging=True
)
```

### Logging Configuration

```python
from matrixone import MatrixOneLogger

logger = MatrixOneLogger(
    level=logging.INFO,
    enable_performance_logging=True,
    enable_slow_sql_logging=True,
    slow_sql_threshold=1.0
)

client = Client(logger=logger)
```

## Error Handling

The SDK provides comprehensive error handling with helpful messages:

```python
from matrixone.exceptions import (
    ConnectionError,
    QueryError,
    VersionError,
    SnapshotError
)

try:
    snapshot = client.snapshots.create('test', 'cluster')
except VersionError as e:
    print(f"Version compatibility error: {e}")
except SnapshotError as e:
    print(f"Snapshot operation failed: {e}")
```

## Examples

Check out the `examples/` directory for comprehensive usage examples:

- `example_01_basic_operations.py` - Basic database operations
- `example_02_account_management.py` - User and role management
- `example_03_async_operations.py` - Async operations
- `example_04_transaction_management.py` - Transaction handling
- `example_05_snapshot_restore.py` - Snapshot and restore
- `example_06_sqlalchemy_integration.py` - SQLAlchemy integration
- `example_07_advanced_features.py` - Advanced features
- `example_08_pubsub_operations.py` - Pub/Sub operations
- `example_09_logger_integration.py` - Logging integration
- `example_10_version_management.py` - Version management
- `example_11_matrixone_version_demo.py` - MatrixOne version demo

### Running Examples

Run all examples:
```bash
make examples
```

Run specific examples:
```bash
make example-basic    # Basic connection example
make example-async    # Async operations example
make example-account  # Account management example
```

### Customizing Connection Parameters

You can override the default connection parameters using environment variables:

```bash
# Set custom connection parameters
export MATRIXONE_HOST=localhost
export MATRIXONE_PORT=6001
export MATRIXONE_USER=root
export MATRIXONE_PASSWORD=111
export MATRIXONE_DATABASE=test

# Run examples with custom parameters
make examples
```

Or set them inline:
```bash
MATRIXONE_HOST=localhost MATRIXONE_USER=admin make example-basic
```

Available environment variables:
- `MATRIXONE_HOST`: Database host (default: 127.0.0.1)
- `MATRIXONE_PORT`: Database port (default: 6001)
- `MATRIXONE_USER`: Database username (default: root)
- `MATRIXONE_PASSWORD`: Database password (default: 111)
- `MATRIXONE_DATABASE`: Database name (default: test)
- `MATRIXONE_CHARSET`: Character set (default: utf8mb4)
- `MATRIXONE_CONNECT_TIMEOUT`: Connection timeout (default: 30)
- `MATRIXONE_AUTOCOMMIT`: Auto-commit mode (default: true)

## 🧪 Testing & Development

### Python Environment Configuration

#### Configuring Python Path

The Makefile uses `python3` and `pip` by default, but you can override these:

```bash
# Method 1: Set environment variables
export PYTHON=/path/to/your/python3
export PIP=/path/to/your/pip
make test

# Method 2: Pass variables to make command
make test PYTHON=/path/to/your/python3 PIP=/path/to/your/pip

# Method 3: Use virtual environment (recommended)
source venv/bin/activate
make test  # Will use the activated environment's python/pip
```

#### Check Your Environment

```bash
# Check current Python configuration
make check-env

# Show project information
make info
```

### Virtual Environment Best Practices

#### Setting Up Development Environment

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On macOS/Linux
# venv\Scripts\activate   # On Windows

# Install development dependencies
make dev-setup

# Verify installation
python -c "import matrixone; print('MatrixOne SDK installed successfully')"
```

#### Working with Virtual Environment

```bash
# Activate virtual environment before development
source venv/bin/activate

# Run tests
make test

# Run examples
make example-basic

# Deactivate when done
deactivate
```

#### Virtual Environment Management

```bash
# Check virtual environment status
which python  # Should show venv path

# List installed packages
pip list

# Update dependencies
pip install -r requirements.txt --upgrade

# Clean virtual environment
make clean-all  # Removes venv/ directory
```

### Quick Commands (Makefile)

```bash
# View all available commands
make help

# Setup development environment
make dev-setup

# Run tests
make test                    # All tests
make test-fast              # Quick tests
make test-coverage          # With coverage report

# Code quality
make lint                   # Code quality check
make format                 # Format code
make type-check             # Type checking

# Examples
make examples               # Run all examples
make example-basic          # Basic connection example

# Build & Release
make build                  # Build package
make build-release          # Build release package (user README)
make publish-test           # Publish to test PyPI
make publish                # Publish to PyPI

# Cleanup
make clean                  # Clean build artifacts
```

### Manual Commands

```bash
# Run all tests
python -m pytest

# Run with coverage
python -m pytest --cov=matrixone --cov-report=html

# Format code
black matrixone tests examples

# Lint code
flake8 matrixone tests examples

# Type checking
mypy matrixone

# Build package
python -m build
```

## CLI Tool

The SDK includes a command-line interface:

```bash
# Show SDK version
matrixone-client --sdk-version

# Execute a query
matrixone-client -H localhost -P 6001 -u root -p 111 -d test -q "SELECT 1"

# Interactive mode
matrixone-client -H localhost -P 6001 -u root -p 111 -d test -i

# Show help
matrixone-client --help
```

## 📚 Documentation

- **Examples**: See `example_*.py` files for comprehensive usage examples
- **API Reference**: All classes and methods are fully documented with type hints
- **Version Management**: Automatic backend version detection and compatibility checking

## 🤝 Contributing

We welcome contributions! 

### Development Workflow

1. **Fork the repository**
2. **Set up development environment**
   ```bash
   git clone <your-fork-url>
   cd matrixone/clients/python-wrapper
   
   # Create virtual environment
   python -m venv venv
   source venv/bin/activate  # On macOS/Linux
   
   # Install development dependencies
   make dev-setup
   ```
3. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```
4. **Make your changes**
5. **Add tests** for new functionality
6. **Run pre-commit checks**
   ```bash
   make pre-commit  # format + lint + test
   ```
7. **Submit a pull request**

### Virtual Environment for Contributors

```bash
# Always work in virtual environment
source venv/bin/activate

# Before making changes
make test  # Ensure all tests pass

# After making changes
make format    # Format code
make lint      # Check code quality
make test      # Run tests
make examples  # Test examples

# Before submitting PR
make release-check  # Complete pre-release checks
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

- 📧 Email: dev@matrixone.cloud
- 🐛 Issues: [GitHub Issues](https://github.com/matrixorigin/matrixone/issues)
- 💬 Discussions: [GitHub Discussions](https://github.com/matrixorigin/matrixone/discussions)
- 📖 Documentation: [MatrixOne Docs](https://docs.matrixone.cloud/)

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and changes.

---

**MatrixOne Python SDK** - Making MatrixOne database operations simple and powerful in Python.