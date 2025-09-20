# MatrixOne Python SDK Project Structure

## 📁 Directory Organization

This document describes the recommended project structure following industry best practices.

### 🏗️ Current Structure

```
clients/python-wrapper/
├── matrixone/                    # Main package directory
│   ├── __init__.py              # Package initialization
│   ├── client.py                # Synchronous client
│   ├── async_client.py          # Asynchronous client
│   ├── account.py               # Account management
│   ├── snapshot.py              # Snapshot operations
│   ├── pitr.py                  # Point-in-time recovery
│   ├── pubsub.py                # Publication/Subscription
│   ├── restore.py               # Restore operations
│   ├── version.py               # Version management
│   ├── logger.py                # Logging utilities
│   ├── exceptions.py            # Custom exceptions
│   ├── moctl.py                 # MoCtl integration
│   └── cli.py                   # Command-line interface
├── tests/                       # Unit tests directory
│   ├── __init__.py              # Test package initialization
│   ├── test_client.py           # Client tests
│   ├── test_async.py            # Async client tests
│   ├── test_account.py          # Account management tests
│   ├── test_snapshot.py         # Snapshot tests
│   └── ...                      # Other test files
├── examples/                    # Example scripts directory
│   ├── example_01_basic_connection.py
│   ├── example_02_account_management.py
│   ├── example_03_async_operations.py
│   └── ...                      # Other example files
├── docs/                        # Documentation directory
│   ├── conf.py                  # Sphinx configuration
│   ├── index.rst                # Main documentation
│   ├── installation.rst         # Installation guide
│   ├── quickstart.rst           # Quick start guide
│   ├── examples.rst             # Examples documentation
│   ├── contributing.rst         # Contributing guide
│   └── api/                     # API documentation
│       ├── index.rst
│       ├── client.rst
│       └── async_client.rst
├── requirements.txt             # Dependencies
├── pyproject.toml              # Project configuration
├── MANIFEST.in                 # Package manifest
├── pytest.ini                 # Test configuration
├── Makefile                    # Build automation
└── README.md                   # Project documentation
```

## 🎯 Industry Best Practices

### ✅ What We Follow

1. **Separate Directories**: Tests and examples are in dedicated directories
2. **Clean Package**: Main package only contains production code
3. **Standard Structure**: Follows Python packaging standards
4. **Documentation**: Comprehensive docs with Sphinx
5. **Automation**: Makefile for common tasks

### 📊 Comparison with Major Projects

| Project | Main Package | Tests | Examples | Docs |
|---------|-------------|-------|----------|------|
| **Requests** | `requests/` | `tests/` | `examples/` | `docs/` |
| **Flask** | `src/flask/` | `tests/` | `examples/` | `docs/` |
| **NumPy** | `numpy/` | `numpy/tests/` | `examples/` | `doc/` |
| **MatrixOne** | `matrixone/` | `tests/` | `examples/` | `docs/` |

## 🔧 Configuration Updates

### Makefile Changes
- Updated test paths: `test_*.py` → `tests/`
- Updated example paths: `example_*.py` → `examples/`
- Updated file statistics commands

### pytest.ini Changes
- Updated testpaths: `.` → `tests`

### MANIFEST.in Changes
- Updated example inclusion: `include example_*.py` → `recursive-include examples *.py`
- Updated test exclusion: `exclude test_*.py` → `recursive-exclude tests *.py`

## 🚀 Benefits of This Structure

### 1. **Clean Separation**
- Production code isolated in `matrixone/`
- Tests don't pollute the main package
- Examples are easily accessible but separate

### 2. **Industry Standard**
- Follows conventions used by major Python projects
- Familiar to developers from other projects
- Easy to navigate and understand

### 3. **Package Distribution**
- Only production code included in wheel
- Examples included for user experience
- Tests excluded from distribution

### 4. **Development Workflow**
- Clear separation of concerns
- Easy to run tests: `make test`
- Easy to run examples: `make examples`
- Easy to build docs: `make docs`

## 📋 File Statistics

- **Test files**: 23
- **Example files**: 12  
- **Core modules**: 13

## 🛠️ Common Commands

```bash
# Run tests
make test

# Run examples
make examples

# Generate documentation
make docs

# Build package
make build

# Install development environment
make dev-setup
```

## 📚 References

- [Python Packaging User Guide](https://packaging.python.org/)
- [Sphinx Documentation](https://www.sphinx-doc.org/)
- [pytest Documentation](https://docs.pytest.org/)
- [Industry Best Practices](https://docs.python-guide.org/writing/structure/)
