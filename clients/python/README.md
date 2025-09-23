# MatrixOne Python SDK

[![PyPI version](https://badge.fury.io/py/matrixone-python-sdk.svg)](https://badge.fury.io/py/matrixone-python-sdk)
[![Python Support](https://img.shields.io/pypi/pyversions/matrixone-python-sdk.svg)](https://pypi.org/project/matrixone-python-sdk/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://github.com/matrixorigin/matrixone/workflows/CI/badge.svg)](https://github.com/matrixorigin/matrixone/actions)

A comprehensive Python SDK for MatrixOne that provides SQLAlchemy-like interface for database operations, vector search, fulltext search, snapshot management, PITR, restore operations, table cloning, and mo-ctl integration.

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
- 🔍 **Fulltext Search**: Advanced fulltext indexing and search capabilities with TF-IDF and BM25 algorithms
- 🧮 **Vector Search**: High-performance vector similarity search with HNSW and IVF algorithms
- 📊 **Vector Indexes**: Create and manage vector indexes for AI/ML applications
- 🎯 **Multi-Modal Support**: Support for various vector dimensions and data types

## 🚀 Installation

```bash
pip install matrixone-python-sdk
```

### Development Installation

#### Using Virtual Environment (Recommended)

```bash
git clone https://github.com/matrixorigin/matrixone.git
cd matrixone/clients/python

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
cd matrixone/clients/python

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
cd matrixone/clients/python

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

- `example_01_basic_connection.py` - Basic database operations
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
- `example_12_vector_search.py` - Vector similarity search
- `example_13_vector_index.py` - Vector index creation and management
- `example_14_vector_index_orm.py` - Vector operations with SQLAlchemy ORM
- `example_15_vector_index_client_chain.py` - Chained vector operations
- `example_16_vector_comprehensive.py` - Comprehensive vector examples
- `example_17_hnsw_vector_index.py` - HNSW vector index examples
- `example_18_specialized_vector_indexes.py` - Specialized vector index types
- `example_fulltext_index.py` - Fulltext indexing and search
- `example_sql_logging.py` - SQL query logging

### Running Examples

Run all examples:
```bash
make examples
```

Run specific examples:
```bash
make example-basic              # Basic connection example
make example-async              # Async operations example
make example-account            # Account management example
make example-vector             # Vector search example
make example-vector-index       # Vector index example
make example-vector-comprehensive # Comprehensive vector examples
make example-hnsw               # HNSW vector index example
make example-fulltext           # Fulltext index example
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

### Testing Overview

The MatrixOne Python SDK includes comprehensive testing with two types of tests:

#### Offline Tests (Mock-based)
- **Purpose**: Test SDK functionality without requiring a database connection
- **Coverage**: 450+ tests covering all core functionality including vector and fulltext features
- **Speed**: Fast execution (typically 3-5 seconds)
- **Dependencies**: No external dependencies required
- **Command**: `make test-offline`

#### Online Tests (Database Integration)
- **Purpose**: Test real database operations and integration
- **Coverage**: 100+ tests covering database connectivity, vector search, fulltext search, and operations
- **Speed**: Slower execution (typically 20-30 seconds)
- **Dependencies**: Requires running MatrixOne database
- **Command**: `make test-online`

#### Specialized Test Suites
- **Vector Tests**: `make test-vector` - Vector search and similarity operations
- **Vector Index Tests**: `make test-vector-index` - Vector index creation and management
- **Fulltext Tests**: `make test-fulltext` - Fulltext indexing and search operations

### Online Testing Setup

#### Prerequisites for Online Tests

Before running online tests, ensure you have:

1. **MatrixOne Database Running**
   ```bash
   # Start MatrixOne database (example with Docker)
   docker run -d --name matrixone \
     -p 6001:6001 \
     matrixorigin/matrixone:latest
   ```

2. **Database Access**
   - Host: `localhost` (default)
   - Port: `6001` (default)
   - User: `root` (default)
   - Password: `111` (default)
   - Database: `test` (default)

#### Connection Check

The SDK automatically checks database connectivity before running online tests:

```bash
# Test connection manually
make test-online
# Output: 🔍 Checking MatrixOne database connection...
#         ✅ MatrixOne database connection successful!
#         Online tests can proceed.
```

#### Environment Variables for Online Tests

Customize connection parameters using environment variables:

```bash
# Set custom connection parameters
export MATRIXONE_HOST=localhost
export MATRIXONE_PORT=6001
export MATRIXONE_USER=root
export MATRIXONE_PASSWORD=111
export MATRIXONE_DATABASE=test

# Run online tests with custom parameters
make test-online
```

Available environment variables:
- `MATRIXONE_HOST`: Database host (default: localhost)
- `MATRIXONE_PORT`: Database port (default: 6001)
- `MATRIXONE_USER`: Database username (default: root)
- `MATRIXONE_PASSWORD`: Database password (default: 111)
- `MATRIXONE_DATABASE`: Database name (default: test)

#### Online Test Behavior

**When Database is Available:**
```bash
make test-online
# ✅ Connection successful → Runs all 67 online tests
# ✅ All tests pass → Shows success message
```

**When Database is Unavailable:**
```bash
make test-online
# ❌ Connection failed → Shows error message
# ❌ Exits with error code → No tests run
```

**Smart Test Execution:**
```bash
make test
# 1. Runs offline tests (always)
# 2. Checks database connection
# 3. If available → Runs online tests
# 4. If unavailable → Shows warning and continues
```

#### Online Test Categories

The online tests cover:

1. **Basic Connection Tests** (`test_basic_connection.py`)
   - Connection establishment
   - Basic query execution
   - Connection cleanup

2. **Account Management Tests** (`test_account_management.py`)
   - User creation and management
   - Role management
   - Privilege operations

3. **Advanced Features Tests** (`test_advanced_features.py`)
   - Snapshot operations
   - PITR (Point-in-Time Recovery)
   - Clone operations
   - MoCTL integration

4. **Pub/Sub Operations Tests** (`test_pubsub_operations.py`)
   - Publication creation
   - Subscription management
   - Data synchronization

5. **Snapshot & Restore Tests** (`test_snapshot_restore.py`)
   - Snapshot creation and management
   - Restore operations
   - Async snapshot operations

6. **SQLAlchemy Integration Tests** (`test_sqlalchemy_integration.py`)
   - SQLAlchemy engine integration
   - Session management
   - Transaction handling

7. **Logger Integration Tests** (`test_logger_integration.py`)
   - Logging configuration
   - Performance logging
   - SQL query logging

8. **Version Management Tests** (`test_version_management.py`)
   - Version detection
   - Feature availability checking
   - Compatibility validation

#### Troubleshooting Online Tests

**Common Issues and Solutions:**

1. **Connection Refused**
   ```bash
   # Error: Can't connect to MySQL server on 'localhost'
   # Solution: Start MatrixOne database
   docker run -d --name matrixone -p 6001:6001 matrixorigin/matrixone:latest
   ```

2. **Authentication Failed**
   ```bash
   # Error: Access denied for user 'root'
   # Solution: Check credentials
   export MATRIXONE_USER=root
   export MATRIXONE_PASSWORD=111
   ```

3. **Database Not Found**
   ```bash
   # Error: Unknown database 'test'
   # Solution: Create database or use existing one
   export MATRIXONE_DATABASE=your_database_name
   ```

4. **Port Already in Use**
   ```bash
   # Error: Address already in use
   # Solution: Use different port
   export MATRIXONE_PORT=6002
   ```

#### Coverage Reports

Generate coverage reports for online tests:

```bash
# Online tests with coverage
make coverage-online

# All tests with coverage
make coverage

# View coverage report
open htmlcov/index.html
```

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
make test                    # All tests (offline + online)
make test-offline           # Offline tests only (mock-based)
make test-online            # Online tests only (requires database)
make test-fast              # Quick tests
make coverage               # All tests with coverage report
make coverage-offline       # Offline tests with coverage
make coverage-online        # Online tests with coverage

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
   cd matrixone/clients/python
   
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

- 📧 Email: contact@matrixorigin.cn
- 🐛 Issues: [GitHub Issues](https://github.com/matrixorigin/matrixone/issues)
- 💬 Discussions: [GitHub Discussions](https://github.com/matrixorigin/matrixone/discussions)
- 📖 Documentation: 
  - [MatrixOne Docs (English)](https://docs.matrixorigin.cn/en)
  - [MatrixOne Docs (中文)](https://docs.matrixorigin.cn/)

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and changes.

---

**MatrixOne Python SDK** - Making MatrixOne database operations simple and powerful in Python.