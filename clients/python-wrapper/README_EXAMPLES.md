# MatrixOne Python Client Examples

This directory contains comprehensive examples demonstrating all capabilities of the MatrixOne Python client. The examples have been reorganized and integrated for better clarity and completeness.

## 📚 Example Structure

### Core Examples (Recommended)

| Example File | Description | Key Features |
|--------------|-------------|--------------|
| `example_01_basic_connection.py` | Basic connection and login formats | All login formats, error handling, connection info |
| `example_02_account_management.py` | Account, user, and role management | Complete account lifecycle, role-based login |
| `example_03_async_operations.py` | Async operations and performance | Async connections, concurrent operations, performance comparison |
| `example_04_transaction_management.py` | Transaction operations | Commit/rollback, isolation, error handling, async transactions |
| `example_05_snapshot_restore.py` | Snapshot and restore operations | PITR, snapshot management, error handling |
| `example_06_sqlalchemy_integration.py` | SQLAlchemy integration | ORM operations, async SQLAlchemy, performance optimization |
| `example_07_advanced_features.py` | Advanced MatrixOne features | PubSub, Clone, MoCTL, performance monitoring |

### Legacy Examples (Deprecated)

The following examples are deprecated and will be removed in future versions. Use the core examples above instead:

- `example_account.py` → Use `example_02_account_management.py`
- `example_async*.py` → Use `example_03_async_operations.py`
- `example_complete_login.py` → Use `example_01_basic_connection.py`
- `example_corrected_login.py` → Use `example_01_basic_connection.py`
- `example_login_formats.py` → Use `example_01_basic_connection.py`
- `example_new_login.py` → Use `example_01_basic_connection.py`
- `example_role_*.py` → Use `example_02_account_management.py`
- `example_sqlalchemy_*.py` → Use `example_06_sqlalchemy_integration.py`
- `example_snapshot*.py` → Use `example_05_snapshot_restore.py`
- `example_transaction_*.py` → Use `example_04_transaction_management.py`
- `example_unified_transaction.py` → Use `example_04_transaction_management.py`

## 🚀 Quick Start

### 1. Basic Connection
```bash
python example_01_basic_connection.py
```

### 2. Account Management
```bash
python example_02_account_management.py
```

### 3. Async Operations
```bash
python example_03_async_operations.py
```

### 4. Transaction Management
```bash
python example_04_transaction_management.py
```

### 5. Snapshot and Restore
```bash
python example_05_snapshot_restore.py
```

### 6. SQLAlchemy Integration
```bash
python example_06_sqlalchemy_integration.py
```

### 7. Advanced Features
```bash
python example_07_advanced_features.py
```

## 📋 Feature Coverage

### Login Formats
- ✅ Legacy format (simple username)
- ✅ Direct format (account#user)
- ✅ User with role (separate parameters)
- ✅ Account with separate parameters
- ✅ Role-based login (account#user#role)
- ✅ Role-based login (separate parameters)
- ✅ Conflict detection and error handling

### Account Management
- ✅ Account creation and management
- ✅ User creation and management
- ✅ Role creation and assignment
- ✅ Permission management
- ✅ Multi-tenant scenarios
- ✅ Account cleanup and maintenance

### Async Operations
- ✅ Async connections
- ✅ Async query execution
- ✅ Async transaction management
- ✅ Concurrent operations
- ✅ Performance comparison
- ✅ Error handling

### Transaction Management
- ✅ Basic transaction operations
- ✅ Commit and rollback
- ✅ Transaction isolation
- ✅ Error handling
- ✅ Performance optimization
- ✅ Best practices

### Snapshot and Restore
- ✅ Snapshot creation
- ✅ Snapshot restoration
- ✅ Point-in-time recovery (PITR)
- ✅ Snapshot enumeration
- ✅ Snapshot cleanup
- ✅ Error handling

### SQLAlchemy Integration
- ✅ Basic SQLAlchemy setup
- ✅ ORM operations
- ✅ Transaction management
- ✅ Async SQLAlchemy
- ✅ Performance optimization
- ✅ MatrixOne-specific features

### Advanced Features
- ✅ PubSub operations
- ✅ Clone operations
- ✅ MoCTL integration
- ✅ Performance monitoring
- ✅ Custom configurations
- ✅ Advanced error handling

## 🔧 Prerequisites

### Required Dependencies
```bash
pip install pymysql aiomysql sqlalchemy
```

### MatrixOne Server
- MatrixOne server running on `127.0.0.1:6001`
- Default credentials: `root/111`
- Test database: `test`

## 📖 Usage Examples

### Basic Connection
```python
from matrixone import Client

client = Client()
client.connect('127.0.0.1', 6001, 'root', '111', 'test')
result = client.execute("SELECT 1")
print(result.fetchone())
client.disconnect()
```

### Account Management
```python
from matrixone import Client
from matrixone.account import AccountManager

client = Client()
client.connect('127.0.0.1', 6001, 'root', '111', 'test')
account_manager = AccountManager(client)

# Create account
account = account_manager.create_account('my_account', 'admin', 'password')
print(f"Created account: {account.name}")

client.disconnect()
```

### Async Operations
```python
import asyncio
from matrixone import AsyncClient

async def main():
    client = AsyncClient()
    await client.connect('127.0.0.1', 6001, 'root', '111', 'test')
    result = await client.execute("SELECT 1")
    print(result.fetchone())
    await client.disconnect()

asyncio.run(main())
```

### Transaction Management
```python
from matrixone import Client

client = Client()
client.connect('127.0.0.1', 6001, 'root', '111', 'test')

with client.transaction() as tx:
    tx.execute("INSERT INTO test_table VALUES (1, 'test')")
    # Transaction automatically commits

client.disconnect()
```

### SQLAlchemy Integration
```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from matrixone import Client

# Create SQLAlchemy engine
engine = create_engine('mysql+pymysql://root:111@127.0.0.1:6001/test')
SessionLocal = sessionmaker(bind=engine)

# Use with MatrixOne client
client = Client()
client.connect('127.0.0.1', 6001, 'root', '111', 'test')

# SQLAlchemy operations
session = SessionLocal()
# ... ORM operations ...
session.close()
client.disconnect()
```

## 🐛 Troubleshooting

### Common Issues

1. **Connection Failed**
   - Check MatrixOne server is running
   - Verify host, port, and credentials
   - Check network connectivity

2. **Login Format Errors**
   - Use correct format: `account#user#role`
   - Avoid mixing formats and parameters
   - Check role assignments

3. **Transaction Errors**
   - Some operations may not support transactions
   - Use proper error handling
   - Keep transactions short

4. **Async Errors**
   - Use `await` for async operations
   - Handle async exceptions properly
   - Use `asyncio.run()` for main functions

### Error Handling
All examples include comprehensive error handling and logging. Check the console output for detailed error messages and troubleshooting information.

## 📝 Contributing

When adding new examples:
1. Follow the naming convention: `example_XX_feature_name.py`
2. Include comprehensive error handling
3. Add logging and progress indicators
4. Include cleanup operations
5. Update this README with new features

## 🔄 Migration Guide

### From Legacy Examples

If you're using legacy examples, here's how to migrate:

1. **Login Examples** → `example_01_basic_connection.py`
2. **Account Examples** → `example_02_account_management.py`
3. **Async Examples** → `example_03_async_operations.py`
4. **Transaction Examples** → `example_04_transaction_management.py`
5. **Snapshot Examples** → `example_05_snapshot_restore.py`
6. **SQLAlchemy Examples** → `example_06_sqlalchemy_integration.py`
7. **Advanced Examples** → `example_07_advanced_features.py`

The new examples provide the same functionality with better organization, more test cases, and improved error handling.
