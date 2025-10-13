# MatrixOne Python SDK

[![PyPI version](https://badge.fury.io/py/matrixone-python-sdk.svg)](https://badge.fury.io/py/matrixone-python-sdk)
[![Python Support](https://img.shields.io/pypi/pyversions/matrixone-python-sdk.svg)](https://pypi.org/project/matrixone-python-sdk/)
[![Documentation Status](https://app.readthedocs.org/projects/matrixone/badge/?version=latest)](https://matrixone.readthedocs.io/en/latest/?badge=latest)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A comprehensive, high-level Python SDK for MatrixOne that provides SQLAlchemy-like interface for database operations, vector similarity search, fulltext search, snapshot management, PITR, restore operations, table cloning, and more.

---

## 📚 Documentation

**[📖 Complete Documentation on ReadTheDocs](https://matrixone.readthedocs.io/)** ⭐

**Quick Links:**
- 🚀 [Quick Start Guide](https://matrixone.readthedocs.io/en/latest/quickstart.html)
- 🧠 [Vector Search & IVF Index Monitoring](https://matrixone.readthedocs.io/en/latest/vector_guide.html)
- 📋 [Best Practices](https://matrixone.readthedocs.io/en/latest/best_practices.html)
- 📖 [API Reference](https://matrixone.readthedocs.io/en/latest/api/index.html)

---

## ✨ Features

- 🚀 **High Performance**: Optimized for MatrixOne database operations with connection pooling
- 🔄 **Async Support**: Full async/await support with AsyncClient for non-blocking operations
- 🧠 **Vector Search**: Advanced vector similarity search with HNSW and IVF indexing
  - Support for f32 and f64 precision vectors
  - Multiple distance metrics (L2, Cosine, Inner Product)
  - ⭐ **IVF Index Health Monitoring** with `get_ivf_stats()` - Critical for production!
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

### Install from test.pypi (Latest Pre-release)

```bash
pip install \
    --index-url https://test.pypi.org/simple/ \
    --extra-index-url https://pypi.org/simple/ \
    matrixone-python-sdk
```

**Note**: The `--extra-index-url` is required to install dependencies from the official PyPI.

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

> **📝 Connection Parameters**
> 
> The `connect()` method requires **keyword arguments** (not positional):
> - `database` - **Required**, no default value
> - `host` - Default: `'localhost'`
> - `port` - Default: `6001`
> - `user` - Default: `'root'`
> - `password` - Default: `'111'`
> 
> **Minimal connection** (uses all defaults):
> ```python
> client.connect(database='test')
> ```
> 
> By default, all features (IVF, HNSW, fulltext) are automatically enabled via `on_connect=[ConnectionAction.ENABLE_ALL]`.

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
from matrixone.account import AccountManager

# Initialize account manager
account_manager = AccountManager(client)

# Create user
user = account_manager.create_user('newuser', 'password123')
print(f"Created user: {user.name}")

# Create role  
role = account_manager.create_role('analyst')
print(f"Created role: {role.name}")

# Grant privileges on specific table (optional)
# Note: table must exist first
account_manager.grant_privilege(
    'SELECT',           # privilege
    'TABLE',            # object_type
    'users',       # object_name (database.table format)
    to_role='analyst'
)

# Grant role to user
account_manager.grant_role('analyst', 'newuser')
print(f"Granted role to user")

# List users
users = account_manager.list_users()
for user in users:
    print(f"User: {user.name}")
```

### Vector Search Operations

```python
from matrixone import Client
from matrixone.sqlalchemy_ext import create_vector_column
from matrixone.orm import declarative_base
from sqlalchemy import Column, BigInteger, String, Text
import numpy as np

# Create client and connect
client = Client()
client.connect(
    host='localhost',
    port=6001,
    user='root',
    password='111',
    database='test'
)

# Define vector table using MatrixOne ORM
Base = declarative_base()

class Document(Base):
    __tablename__ = 'documents'
    # IMPORTANT: HNSW index requires BigInteger (BIGINT) primary key
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    title = Column(String(200))
    content = Column(Text)
    embedding = create_vector_column(384, precision='f32')

# Create table using client API (not Base.metadata.create_all)
client.create_table(Document)

# Create HNSW index using SDK (not SQL)
client.vector_ops.enable_hnsw()
client.vector_ops.create_hnsw(
    'documents',  # table name or model - positional argument
    name='idx_embedding',
    column='embedding',
    m=16,
    ef_construction=200
)

# Insert vector data using client API
client.insert(Document, {
    'title': 'Machine Learning Guide',
    'content': 'Comprehensive ML tutorial...',
    'embedding': np.random.rand(384).tolist()
})

# Search similar documents using SDK
query_vector = np.random.rand(384).tolist()
results = client.vector_ops.similarity_search(
    'documents',  # table name or model - positional argument
    vector_column='embedding',
    query_vector=query_vector,
    limit=5,
    distance_type='cosine'
)

for row in results:
    print(f"Document: {row[1]}, Similarity: {row[-1]}")

# Cleanup
client.drop_table(Document)  # Use client API
client.disconnect()
```

### ⭐ IVF Index Health Monitoring (Production Critical)

**Monitor your IVF indexes to ensure optimal performance!**

```python
from matrixone import Client
import numpy as np

client = Client()
client.connect(host='localhost', port=6001, user='root', password='111', database='test')

# After creating IVF index and inserting data...

# Get IVF index statistics
stats = client.vector_ops.get_ivf_stats("documents", "embedding")

# Analyze index balance
counts = stats['distribution']['centroid_count']
total_centroids = len(counts)
total_vectors = sum(counts)
min_count = min(counts) if counts else 0
max_count = max(counts) if counts else 0
balance_ratio = max_count / min_count if min_count > 0 else float('inf')

print(f"📊 IVF Index Health Report:")
print(f"  - Total centroids: {total_centroids}")
print(f"  - Total vectors: {total_vectors}")
print(f"  - Balance ratio: {balance_ratio:.2f}")
print(f"  - Min vectors in centroid: {min_count}")
print(f"  - Max vectors in centroid: {max_count}")

# Check if index needs rebuilding
if balance_ratio > 2.5:
    print("⚠️  WARNING: Index is imbalanced and needs rebuilding!")
    print("   Rebuild the index for optimal performance:")
    
    # Rebuild process
    client.vector_ops.drop("documents", "idx_embedding")
    client.vector_ops.create_ivf(
        "documents",
        name="idx_embedding",
        column="embedding",
        lists=100
    )
    print("✅ Index rebuilt successfully")
else:
    print("✅ Index is healthy and well-balanced")

client.disconnect()
```

**Why IVF Stats Matter:**
- 🎯 **Performance**: Unbalanced indexes lead to slow searches
- 📊 **Load Distribution**: Identify hot spots and imbalances
- 🔄 **Rebuild Timing**: Know when to rebuild for optimal performance
- 📈 **Capacity Planning**: Understand data distribution patterns

**When to Rebuild:**
- Balance ratio > 2.5 (moderate imbalance)
- Balance ratio > 3.0 (severe imbalance - rebuild immediately)
- After bulk inserts (>20% of data)
- Performance degradation in searches

### Fulltext Search Operations

```python
from matrixone import Client
from matrixone.sqlalchemy_ext.fulltext_search import boolean_match
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, Text

# Create client and connect
client = Client()
client.connect(
    host='localhost',
    port=6001,
    user='root',
    password='111',
    database='test'
)

# Define model using MatrixOne ORM
Base = declarative_base()

class Article(Base):
    __tablename__ = 'articles'
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(200), nullable=False)
    content = Column(Text, nullable=False)
    category = Column(String(100))

# Create table using client API (not Base.metadata.create_all)
client.create_table(Article)

# Insert some data using client API
articles = [
    {'title': 'Machine Learning Guide', 
     'content': 'Comprehensive machine learning tutorial...', 
     'category': 'AI'},
    {'title': 'Python Programming', 
     'content': 'Learn Python programming basics', 
     'category': 'Programming'},
]
client.batch_insert(Article, articles)

# Create fulltext index using SDK (not SQL)
client.fulltext_index.create(
    'articles',  # table name - positional argument
    name='ftidx_content',
    columns=['title', 'content']
)

# Boolean search with encourage (like natural language)
results = client.query(
    Article.title,
    Article.content,
    boolean_match('title', 'content').encourage('machine learning tutorial')
).execute()

# Boolean search with must/should operators
results = client.query(
    Article.title,
    Article.content,
    boolean_match('title', 'content')
        .must('machine')
        .must('learning')
        .must_not('basics')
).execute()

# Results is a ResultSet object
for row in results.rows:
    print(f"Title: {row[0]}, Content: {row[1][:50]}...")

# Cleanup
client.drop_table(Article)  # Use client API
client.disconnect()
```

### Metadata Analysis

```python
from matrixone import Client

# Create client and connect
client = Client()
client.connect(
    host='localhost',
    port=6001,
    user='root',
    password='111',
    database='test'
)

# Analyze table metadata - returns structured MetadataRow objects
metadata_rows = client.metadata.scan(
    dbname='test',
    tablename='documents',
    columns='*'  # Get all columns
)

for row in metadata_rows:
    print(f"Column: {row.col_name}")
    print(f"  Rows count: {row.rows_cnt}")
    print(f"  Null count: {row.null_cnt}")
    print(f"  Size: {row.origin_size}")

# Get table brief statistics
brief_stats = client.metadata.get_table_brief_stats(
    dbname='test',
    tablename='documents'
)

table_stats = brief_stats['documents']
print(f"Total rows: {table_stats['row_cnt']}")
print(f"Total nulls: {table_stats['null_cnt']}")
print(f"Original size: {table_stats['original_size']}")
print(f"Compressed size: {table_stats['compress_size']}")

client.disconnect()
```

### Pub/Sub Operations

```python
# List publications
publications = client.pubsub.list_publications()
for pub in publications:
    print(f"Publication: {pub}")

# List subscriptions
subscriptions = client.pubsub.list_subscriptions()
for sub in subscriptions:
    print(f"Subscription: {sub}")

# Drop publication/subscription when needed
try:
    client.pubsub.drop_publication("test_publication")
    client.pubsub.drop_subscription("test_subscription")
except Exception as e:
    print(f"Cleanup: {e}")
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
from matrixone import Client
from matrixone.logger import create_default_logger
import logging

# Create custom logger
logger = create_default_logger(
    level=logging.INFO,
    sql_log_mode='auto',  # 'off', 'simple', 'auto', 'full'
    slow_query_threshold=1.0,
    max_sql_display_length=500
)

# Use custom logger with client
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

## 🔗 Links

- **📚 Full Documentation**: https://matrixone.readthedocs.io/
- **📦 PyPI Package**: https://pypi.org/project/matrixone-python-sdk/
- **💻 GitHub Repository**: https://github.com/matrixorigin/matrixone/tree/main/clients/python
- **🌐 MatrixOne Docs**: https://docs.matrixorigin.cn/

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
- ⭐ **IVF Index Health Monitoring** - Essential for production systems
- Similarity search operations
- Advanced vector optimizations and index rebuilding

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
