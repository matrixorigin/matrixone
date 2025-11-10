# MatrixOne Python SDK Examples

This directory contains comprehensive examples demonstrating various features of the MatrixOne Python SDK.

## Running Examples

The SDK provides flexible ways to run examples, allowing you to use either:
- **Local source code** (for development and testing)
- **Installed package** (for production-like testing)

### Quick Start

```bash
# Run all examples using local source code (default)
make examples

# Run all examples using installed matrixone package
make examples-installed

# Run specific example
make example-basic        # Basic connection example
make example-async        # Async operations example
make example-vector       # Vector search example
make example-fulltext     # Fulltext search example
```

### Advanced Usage

```bash
# Use local source code explicitly
make examples-source

# Use installed package via parameter
make examples USE_SOURCE=0

# Use local source via parameter (same as default)
make examples USE_SOURCE=1

# Run specific example with installed package
USE_SOURCE=0 make example-basic
```

## Running Tests

The same `USE_SOURCE` mechanism applies to tests:

```bash
# Run all tests using local source code (default)
make test

# Run all tests using installed package
make test USE_SOURCE=0

# Run offline tests only
make test-offline                # Local source (default)
make test-offline USE_SOURCE=0   # Installed package

# Run online tests only (requires database)
make test-online                 # Local source (default)
make test-online USE_SOURCE=0    # Installed package

# Run coverage
make coverage                    # Local source (default)
make coverage USE_SOURCE=0       # Installed package

# Run matrix tests (SQLAlchemy 1.4 + 2.0)
make test-matrix                 # Local source (default)
make test-matrix USE_SOURCE=0    # Installed package
```

## How It Works

### Local Source Code Mode (Default)
```bash
make examples                    # or USE_SOURCE=1
make test                        # or USE_SOURCE=1
```
- Sets `PYTHONPATH=.` to prioritize local source code
- Useful for development and testing changes
- Changes are immediately reflected without reinstallation

### Installed Package Mode
```bash
make examples-installed          # or USE_SOURCE=0
make test USE_SOURCE=0
```
- Uses the matrixone package installed in your environment
- Useful for testing the installed package
- Simulates production usage

## Example Categories

### Basic Examples
- `example_01_basic_connection.py` - Connection and basic operations
- `example_02_account_management.py` - Account and user management
- `example_03_async_operations.py` - Asynchronous operations
- `example_04_transaction_management.py` - Transaction handling

### Advanced Features
- `example_05_snapshot_restore.py` - Snapshot and restore operations
- `example_06_sqlalchemy_integration.py` - SQLAlchemy integration
- `example_07_advanced_features.py` - Advanced SDK features
- `example_08_pubsub_operations.py` - Pub/Sub messaging

### Vector Search
- `example_12_vector_basics.py` - Vector basics
- `example_13_vector_indexes.py` - Vector index creation
- `example_14_vector_search.py` - Vector similarity search
- `example_15_vector_advanced.py` - Advanced vector operations

### Fulltext Search
- Fulltext search examples (check documentation)
- JSON Parser examples
- NGRAM Parser examples

### ORM Examples
- `example_18_snapshot_orm.py` - ORM with snapshots
- `example_19_sqlalchemy_style_orm.py` - SQLAlchemy-style ORM
- `example_20_sqlalchemy_engine_integration.py` - Engine integration
- `example_21_advanced_orm_features.py` - Advanced ORM features

### Other Examples
- `example_09_logger_integration.py` - Logging configuration
- `example_10_version_management.py` - Version management
- `example_22_unified_sql_builder.py` - SQL builder
- `example_24_query_update.py` - Query and update operations
- `example_25_metadata_operations.py` - Metadata operations
- `example_31_cdc_operations.py` - CDC task lifecycle operations

## Prerequisites

### For Local Source Mode
```bash
# Ensure dependencies are installed
pip install -r requirements.txt
```

### For Installed Package Mode
```bash
# Install the matrixone SDK first
pip install matrixone-python-sdk

# Or install from local build
pip install dist/matrixone_python_sdk-*.whl
```

## Database Connection

All examples require a running MatrixOne database. Configure connection in examples or use environment variables:

```bash
export MO_HOST=127.0.0.1
export MO_PORT=6001
export MO_USER=root
export MO_PASSWORD=111
export MO_DATABASE=test
```

## Troubleshooting

### Example fails with "ModuleNotFoundError"
- **Local source mode**: Run `pip install -r requirements.txt`
- **Installed mode**: Run `pip install matrixone-python-sdk`

### Example fails with "Connection refused"
- Ensure MatrixOne database is running
- Check connection parameters (host, port, user, password)

### Example uses old code after changes
- You're probably in installed package mode
- Switch to local source mode: `make examples-source`
- Or reinstall the package: `pip install -e .`

## Development Workflow

### Testing Local Changes
```bash
# 1. Make code changes to matrixone/
# 2. Run examples with local source to test changes immediately
make examples-source

# No need to reinstall the package!
```

### Testing Installed Package
```bash
# 1. Build the package
make build

# 2. Install it
pip install dist/matrixone_python_sdk-*.whl --force-reinstall

# 3. Test with installed package
make examples-installed
```

## Tips

1. **Development**: Always use `make examples` or `make examples-source` to test changes immediately
2. **Production Testing**: Use `make examples-installed` to test the actual installed package
3. **CI/CD**: Use `make examples-installed` in CI/CD pipelines to test the distributed package
4. **Debugging**: Use local source mode to add print statements and debug

## More Information

- See [../docs/](../docs/) for comprehensive documentation
- See [../TESTING.md](../TESTING.md) for testing guidelines
- See [../README.md](../README.md) for SDK overview

