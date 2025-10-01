# MatrixOne Python SDK

[![PyPI version](https://badge.fury.io/py/matrixone-python-sdk.svg)](https://badge.fury.io/py/matrixone-python-sdk)
[![Python Support](https://img.shields.io/pypi/pyversions/matrixone-python-sdk.svg)](https://pypi.org/project/matrixone-python-sdk/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A comprehensive, high-level Python SDK for MatrixOne that provides SQLAlchemy-like interface for database operations, vector similarity search, fulltext search, snapshot management, PITR, restore operations, table cloning, and more.

## ✨ Features

- 🚀 **High Performance**: Optimized for MatrixOne database operations with connection pooling
- 🔄 **Async Support**: Full async/await support with AsyncClient for non-blocking operations
- 🧠 **Vector Search**: Advanced vector similarity search with HNSW and IVF indexing
  - Support for f32 and f64 precision vectors
  - Multiple distance metrics (L2, Cosine, Inner Product)
  - High-performance indexing for AI/ML applications
- 🔍 **Fulltext Search**: Powerful fulltext indexing and search with BM25 and TF-IDF
  - Natural language and boolean search modes
  - Multi-column indexes with relevance scoring
- 📊 **Metadata Analysis**: Table and column metadata analysis with statistics
- 📸 **Snapshot Management**: Create and manage database snapshots at multiple levels
- ⏰ **Point-in-Time Recovery**: PITR functionality for precise data recovery
- 🔄 **Table Cloning**: Clone databases and tables efficiently
- 👥 **Account Management**: Comprehensive user and role management
- 📊 **Pub/Sub**: Real-time publication and subscription support
- 🔧 **Version Management**: Automatic backend version detection and compatibility
- 🛡️ **Type Safety**: Full type hints support with comprehensive documentation
- 📚 **SQLAlchemy Integration**: Seamless SQLAlchemy ORM integration with enhanced features

## 🚀 Installation

### Using pip (Recommended)

```bash
pip install matrixone-python-sdk
```

### Using Virtual Environment (Best Practice)

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate

# Install MatrixOne SDK
pip install matrixone-python-sdk

# Verify installation
python -c "import matrixone; print('MatrixOne SDK installed successfully')"
```

### Using Conda

```bash
# Create conda environment
conda create -n matrixone python=3.10
conda activate matrixone

# Install MatrixOne SDK
pip install matrixone-python-sdk
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
    print(result.fetchall())
    
    await client.disconnect()

asyncio.run(main())
```

### Snapshot Management

```python
# Create a snapshot
snapshot = client.snapshots.create(
    'my_snapshot',
    'cluster',
    description='Backup before migration'
)

# List snapshots
snapshots = client.snapshots.list()
for snap in snapshots:
    print(f"Snapshot: {snap.name}, Created: {snap.created_at}")

# Clone database from snapshot
client.clone.clone_database(
    'new_database',
    'old_database',
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
    'cluster_pitr',
    range_value=7,
    range_unit='d'
)

# Restore cluster from snapshot
client.restore.restore_cluster('my_snapshot')
```

### Account Management

```python
# Create user
user = client.account.create_user(
    'newuser',
    'password123',
    comment='New user account'
)

# Create role
role = client.account.create_role(
    'analyst',
    comment='Data analyst role'
)

# Grant privileges
client.account.grant_privilege(
    'SELECT',
    'TABLE',
    '*',
    to_user='newuser'
)
```

### Vector Search Operations

```python
from matrixone.sqlalchemy_ext import create_vector_column
from sqlalchemy import Column, Integer, String, Text
from matrixone.orm import declarative_base
import numpy as np

# Define vector table
Base = declarative_base()

class Document(Base):
    __tablename__ = 'documents'
    id = Column(Integer, primary_key=True)
    title = Column(String(200))
    content = Column(Text)
    embedding = create_vector_column(384, 'f32')

# Create table
client.create_table(Document)

# Create HNSW index for high-accuracy search
client.vector_ops.enable_hnsw()
client.vector_ops.create_hnsw(
    'documents',
    'idx_embedding',
    'embedding',
    m=16,
    ef_construction=200
)

# Insert vector data
client.insert(Document, {
    'id': 1,
    'title': 'Machine Learning Guide',
    'content': 'Comprehensive ML tutorial...',
    'embedding': np.random.rand(384).tolist()
})

# Search similar documents
query_vector = np.random.rand(384).tolist()
results = client.vector_ops.similarity_search(
    'documents',
    'embedding',
    query_vector,
    limit=5,
    distance_type='cosine'
)

for row in results:
    print(f"Document: {row[1]}, Similarity: {row[-1]}")
```

### Fulltext Search Operations

```python
# Create fulltext index
client.fulltext_index.create(
    'documents',
    'ftidx_content',
    ['title', 'content'],
    algorithm='BM25'
)

# Natural language search using ORM
from matrixone.sqlalchemy_ext.fulltext_search import natural_match

results = client.query(Document).filter(
    natural_match('title', 'content', query='machine learning tutorial')
).all()

# Boolean search with operators using ORM
from matrixone.sqlalchemy_ext.fulltext_search import boolean_match

results = client.query(Document).filter(
    boolean_match('title', 'content')
    .must('machine')
    .must('learning')
    .discourage('basics')
).all()

for row in results:
    print(f"Title: {row[1]}, Score: {row[-1]}")
```

### Metadata Analysis

```python
# Analyze table metadata
metadata = client.metadata.scan(
    dbname='test',
    tablename='documents'
)

for row in metadata:
    print(f"Column: {row.col_name}")
    print(f"  Type: {row.data_type}")
    print(f"  Null count: {row.null_cnt}")
    print(f"  Rows count: {row.rows_cnt}")

# Get table statistics
stats = client.metadata.get_table_brief_stats(
    dbname='test',
    tablename='documents'
)
print(f"Total rows: {stats['total_rows']}")
print(f"Table size: {stats['total_size']} bytes")
```

### Pub/Sub Operations

```python
# Create subscription (publication must be created separately)
subscription = client.pubsub.create_subscription(
    'data_sync',
    'data_changes',
    'root'
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
    sql_log_mode='auto',  # 'off', 'simple', 'auto', 'full'
    slow_query_threshold=1.0
)
```

### Logging Configuration

```python
from matrixone import MatrixOneLogger

logger = MatrixOneLogger(
    level=logging.INFO,
    sql_log_mode='auto',  # 'off', 'simple', 'auto', 'full'
    slow_query_threshold=1.0,
    max_sql_display_length=500
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

## 📖 Documentation

For comprehensive documentation, visit:
- **PyPI Package**: https://pypi.org/project/matrixone-python-sdk/
- **GitHub Repository**: https://github.com/matrixorigin/matrixone/tree/main/clients/python
- **MatrixOne Docs**: https://docs.matrixorigin.cn/

### Online Examples

The SDK includes 25+ comprehensive examples covering all features:

**Getting Started:**
- Basic connection and database operations
- Async/await operations
- Transaction management
- SQLAlchemy ORM integration

**Vector Search:**
- Vector data types and distance functions
- IVF and HNSW index creation and tuning
- Similarity search operations
- Advanced vector optimizations

**Advanced Features:**
- Fulltext search with BM25/TF-IDF
- Table metadata analysis
- Snapshot and restore operations
- Account and permission management
- Pub/Sub operations
- Connection hooks and logging

### Quick Examples

Clone the repository to access all examples:
```bash
git clone https://github.com/matrixorigin/matrixone.git
cd matrixone/clients/python/examples

# Run basic example
python example_01_basic_connection.py

# Run vector search example
python example_12_vector_basics.py

# Run metadata analysis example
python example_25_metadata_operations.py
```


## Support

- 📧 Email: contact@matrixorigin.cn
- 🐛 Issues: [GitHub Issues](https://github.com/matrixorigin/matrixone/issues)
- 💬 Discussions: [GitHub Discussions](https://github.com/matrixorigin/matrixone/discussions)
- 📖 Documentation: 
  - [MatrixOne Docs (English)](https://docs.matrixorigin.cn/en)
  - [MatrixOne Docs (中文)](https://docs.matrixorigin.cn/)

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

---

**MatrixOne Python SDK** - Making MatrixOne database operations simple and powerful in Python.
