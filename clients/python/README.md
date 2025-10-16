# MatrixOne Python SDK

[![PyPI version](https://badge.fury.io/py/matrixone-python-sdk.svg)](https://badge.fury.io/py/matrixone-python-sdk)
[![Python Support](https://img.shields.io/pypi/pyversions/matrixone-python-sdk.svg)](https://pypi.org/project/matrixone-python-sdk/)
[![Documentation Status](https://app.readthedocs.org/projects/matrixone/badge/?version=latest)](https://matrixone.readthedocs.io/en/latest/?badge=latest)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A comprehensive Python SDK for MatrixOne that provides SQLAlchemy-like interface for database operations, vector search, fulltext search, snapshot management, PITR, restore operations, table cloning, and mo-ctl integration.

> **📚 [Complete Documentation](https://matrixone.readthedocs.io/)** | 
> **🚀 [Quick Start Guide](https://matrixone.readthedocs.io/en/latest/quickstart.html)** | 
> **🧠 [Vector Search & IVF Monitoring](https://matrixone.readthedocs.io/en/latest/vector_guide.html)** | 
> **📖 [API Reference](https://matrixone.readthedocs.io/en/latest/api/index.html)**

> **Note**: This SDK implementation is generated and optimized through AI-assisted development, leveraging advanced code generation techniques to ensure comprehensive API coverage, type safety, and modern Python best practices.

## ✨ Features

- 🚀 **High Performance**: Optimized for MatrixOne database operations with connection pooling
- 🔄 **Async Support**: Full async/await support with AsyncClient for non-blocking operations
- 🧠 **Vector Search**: Advanced vector similarity search with HNSW and IVF indexing algorithms
  - Support for f32 and f64 precision vectors
  - Multiple distance metrics (L2, Cosine, Inner Product)
  - ⭐ **IVF Index Health Monitoring** with `get_ivf_stats()` - Critical for production!
  - Configurable index parameters for performance tuning
- 🔍 **Fulltext Search**: Powerful fulltext indexing and search with BM25 and TF-IDF algorithms
  - Natural language and boolean search modes
  - Multi-column fulltext indexes
  - Relevance scoring and ranking
- 📊 **Metadata Analysis**: Comprehensive table and column metadata analysis with statistics
  - Table size and row count statistics
  - Column data distribution analysis
  - Index usage metrics
- 🔍 **Secondary Index Verification**: Verify consistency of secondary indexes with main table
  - Get all secondary index table names
  - Get specific index table by index name
  - Verify row counts across main table and all indexes in single query
- 📸 **Snapshot Management**: Create and manage database snapshots at multiple levels
- ⏰ **Point-in-Time Recovery**: PITR functionality for precise data recovery
- 🔄 **Table Cloning**: Clone databases and tables efficiently with data replication
- 👥 **Account Management**: Comprehensive user, role, and permission management
- 📊 **Pub/Sub**: Real-time publication and subscription support
- 🔧 **Version Management**: Automatic backend version detection and compatibility checking
- 🛡️ **Type Safety**: Full type hints support with comprehensive documentation
- 📚 **SQLAlchemy Integration**: Seamless SQLAlchemy integration with enhanced ORM features
- 🔗 **Enhanced Query Building**: Advanced query building with logical operations (logical_and, logical_or, logical_not)
- 🪝 **Connection Hooks**: Pre/post connection hooks for custom initialization logic
- 🛠️ **mo-diag CLI Tool**: Interactive diagnostic tool for MatrixOne database maintenance
  - Index health monitoring and verification
  - IVF/HNSW vector index status inspection
  - Table statistics and metadata analysis
  - Interactive shell with Tab completion and command history
  - Non-interactive mode for scripting and automation
  - Batch operations on tables and indexes

## 🚀 Installation

### From PyPI (Stable Release)

```bash
pip install matrixone-python-sdk
```

### From test.pypi (Latest Pre-release)

```bash
pip install \
    --index-url https://test.pypi.org/simple/ \
    --extra-index-url https://pypi.org/simple/ \
    matrixone-python-sdk
```

**Note**: The `--extra-index-url` is required to install dependencies (PyMySQL, SQLAlchemy, etc.) from the official PyPI.

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
    name='my_snapshot',
    level='cluster',
    description='Backup before migration'
)

# List snapshots
snapshots = client.snapshots.list()
for snap in snapshots:
    print(f"Snapshot: {snap.name}, Created: {snap.created_at}")

# Clone database from snapshot
client.clone.clone_database(
    target_db='new_database',
    source_db='old_database',
    snapshot_name='my_snapshot'
)
```

### Vector Search

```python
from matrixone.sqlalchemy_ext import create_vector_column
from sqlalchemy import Column, BigInteger, String, Text
from matrixone.orm import declarative_base
import numpy as np

# Define vector table model
Base = declarative_base()

class Document(Base):
    __tablename__ = 'documents'
    # IMPORTANT: HNSW index requires BigInteger primary key
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    title = Column(String(200))
    content = Column(Text)
    embedding = create_vector_column(384, precision='f32')  # 384-dim vector

# Create table using client API
client.create_table(Document)

# Enable and create HNSW index
client.vector_ops.enable_hnsw()
client.vector_ops.create_hnsw(
    'documents',  # table name - positional argument
    name='idx_embedding',
    column='embedding',
    m=16,
    ef_construction=200
)

# Insert document with vector using client API
client.insert(Document, {
    'title': 'AI Research Paper',
    'content': 'Advanced machine learning techniques...',
    'embedding': np.random.rand(384).tolist()
})

# Vector similarity search
query_vector = np.random.rand(384).tolist()
results = client.vector_ops.similarity_search(
    'documents',  # table name - positional argument
    vector_column='embedding',
    query_vector=query_vector,
    limit=5,
    distance_type='cosine'
)

# ⭐ CRITICAL: Monitor IVF index health for production
stats = client.vector_ops.get_ivf_stats('documents', 'embedding')
counts = stats['distribution']['centroid_count']
balance_ratio = max(counts) / min(counts) if min(counts) > 0 else float('inf')

print(f"Index Health: {len(counts)} centroids, balance ratio: {balance_ratio:.2f}")
if balance_ratio > 2.5:
    print("⚠️  Index needs rebuilding for optimal performance!")
```

### Fulltext Search

```python
# Enable and create fulltext index using client API
client.fulltext_index.enable_fulltext()
client.fulltext_index.create(
    'documents',  # table name - positional argument
    name='ftidx_content',
    columns=['title', 'content']
)

# Boolean search with encourage (like natural language search)
from matrixone.sqlalchemy_ext.fulltext_search import boolean_match

results = client.query(
    Document.title,
    Document.content,
    boolean_match('title', 'content').encourage('machine learning techniques')
).execute()

# Boolean search with must/should/must_not operators
results = client.query(
    Document.title,
    Document.content,
    boolean_match('title', 'content')
        .must('machine')
        .must('learning')
        .must_not('basic')
).execute()
```

### Metadata Analysis

```python
# Get table metadata with statistics
metadata_rows = client.metadata.scan(
    dbname='test',
    tablename='documents',
    columns='*'  # Get structured MetadataRow objects
)

for row in metadata_rows:
    print(f"Column: {row.col_name}")
    print(f"  Nulls: {row.null_cnt}")
    print(f"  Rows: {row.rows_cnt}")
    print(f"  Size: {row.origin_size}")

# Get table statistics
brief_stats = client.metadata.get_table_brief_stats(
    dbname='test',
    tablename='documents'
)
table_stats = brief_stats['documents']
print(f"Total rows: {table_stats['row_cnt']}")
print(f"Original size: {table_stats['original_size']} bytes")
print(f"Compressed size: {table_stats['compress_size']} bytes")
```

### Secondary Index Verification

```python
# Get all secondary index tables
index_tables = client.get_secondary_index_tables('my_table')
print(f"Found {len(index_tables)} secondary indexes")

# Get specific index by name
physical_table = client.get_secondary_index_table_by_name('my_table', 'idx_name')
print(f"Index 'idx_name' -> {physical_table}")

# Verify all indexes have same count as main table
try:
    count = client.verify_table_index_counts('my_table')
    print(f"✓ Verification passed! Row count: {count}")
except ValueError as e:
    print(f"✗ Index mismatch detected:")
    print(e)
    # Error includes all count details for debugging
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

## 🛠️ mo-diag - Interactive Diagnostic Tool

The `mo-diag` command-line tool provides an interactive shell for diagnosing and maintaining MatrixOne databases, with a special focus on vector indexes, secondary indexes, and table statistics.

### Installation

After installing the SDK, `mo-diag` is automatically available as a command:

```bash
pip install matrixone-python-sdk
mo-diag --help
```

### Quick Start

#### Interactive Mode

Launch the interactive shell to execute multiple diagnostic commands:

```bash
# Connect to default localhost
mo-diag --database test

# Connect to remote database
mo-diag --host 192.168.1.100 --port 6001 --user admin --password secret --database production
```

**Interactive Features**:
- 🔍 **Tab Completion**: Press `Tab` to auto-complete commands, table names, and database names
- ⬆️⬇️ **Command History**: Use arrow keys to browse command history
- 🔎 **History Search**: Press `Ctrl+R` to search command history
- 🎨 **Colored Output**: Clear visual feedback with syntax highlighting
- 💾 **Persistent History**: Command history saved to `~/.mo_diag_history`

#### Non-Interactive Mode

Execute single commands directly for scripting and automation:

```bash
# Check IVF index status
mo-diag -d test -c "show_ivf_status"

# Get detailed table statistics
mo-diag -d test -c "show_table_stats my_table -a -d"

# Execute SQL query
mo-diag -d test -c "sql SELECT COUNT(*) FROM my_table"

# Flush table and all indexes
mo-diag -d test -c "flush_table my_table"
```

### Available Commands

#### Index Management

**`show_indexes <table> [database]`**
- Display all indexes for a table including IVF, HNSW, Fulltext, and regular indexes
- Shows physical table names, index types, and statistics
- Includes object counts, row counts, and sizes for vector/fulltext indexes

```
MO-DIAG[test]> show_indexes ivf_health_demo_docs

📊 Secondary Indexes for 'test.ivf_health_demo_docs'

*************************** 1. row ***************************
      Index Name: idx_embedding_ivf_v2
       Algorithm: ivfflat
      Table Type: metadata
  Physical Table: __mo_index_secondary_0199e725-0a7a-77b8-b689-ccdd0a33f581
         Columns: embedding
      Statistics:
                   - Objects: 1
                   - Rows: 7
                   - Compressed Size: 940 B
                   - Original Size: 1.98 KB

*************************** 2. row ***************************
      Index Name: idx_embedding_ivf_v2
       Algorithm: ivfflat
      Table Type: centroids
  Physical Table: __mo_index_secondary_0199e725-0a7b-706e-8f0a-a50edc3621a1
         Columns: embedding
      Statistics:
                   - Objects: 1
                   - Rows: 17
                   - Compressed Size: 3.09 KB
                   - Original Size: 6.83 KB

*************************** 3. row ***************************
      Index Name: idx_embedding_ivf_v2
       Algorithm: ivfflat
      Table Type: entries
  Physical Table: __mo_index_secondary_0199e725-0a7c-77f4-8d0b-48fd8258098a
         Columns: embedding
      Statistics:
                   - Objects: 1
                   - Rows: 1,000
                   - Compressed Size: 156.34 KB
                   - Original Size: 176.07 KB

Total: 3 index tables (1 ivfflat with 3 physical tables)
```

**Note**: IVF indexes have 3 physical tables (metadata, centroids, entries)

**`show_all_indexes [database]`**
- Health report for all tables with secondary indexes in the database
- Row count consistency checks
- IVF/HNSW/Fulltext index status

```
MO-DIAG[test]> show_all_indexes

📊 Index Health Report for Database 'test':
========================================================================================================================

✓ HEALTHY (3 tables)
------------------------------------------------------------------------------------------------------------------------
Table Name                          | Indexes  | Row Count            | Notes
------------------------------------------------------------------------------------------------------------------------
cms_all_content_chunk_info          | 3        | ✓ 32,712 rows        | -
demo_mixed_indexes                  | 4        | ✓ 20 rows            | -
ivf_health_demo_docs                | 1        | 300 rows             | IVF: 17 centroids, 300 vectors

========================================================================================================================
Summary:
  ✓ 3 healthy tables
  Total: 3 tables with indexes

💡 Tip: Use 'verify_counts <table>' or 'show_ivf_status' for detailed diagnostics
```

**`verify_counts <table> [database]`**
- Verify row count consistency between main table and all its secondary indexes
- Highlights any mismatches

```
MO-DIAG[test]> verify_counts my_table

📊 Row Count Verification for 'test.my_table'
════════════════════════════════════════════════════════════════════════════
Main table: 10,000 rows
────────────────────────────────────────────────────────────────────────────
✓ __mo_index_secondary_xxx: 10,000 rows
✓ __mo_index_unique_yyy: 10,000 rows
════════════════════════════════════════════════════════════════════════════
✅ PASSED: All index tables match (10,000 rows)
```

#### Vector Index Monitoring

**`show_ivf_status [database] [-v] [-t table]`**
- Display IVF index building status and centroid distribution
- `-v`: Verbose mode with detailed centroid information
- `-t <table>`: Filter by specific table

```
MO-DIAG[test]> show_ivf_status

📊 IVF Index Status in 'test':
════════════════════════════════════════════════════════════════════════════
Table                          | Index                     | Column               | Centroids  | Vectors      | Balance    | Status
────────────────────────────────────────────────────────────────────────────
ivf_health_demo_docs           | idx_embedding_ivf_v2      | embedding            | 17         | 1,000        | 2.35       | ✓ active
```

#### Table Statistics

**`show_table_stats <table> [database] [-t] [-a] [-d]`**
- Display table metadata and statistics
- `-t`: Include tombstone statistics
- `-a`: Include all indexes (hierarchical view with -d)
- `-d`: Show detailed object lists

```
MO-DIAG[test]> show_table_stats ivf_health_demo_docs -a -d

📊 Detailed Table Statistics for 'test.ivf_health_demo_docs':
======================================================================================================================================================

Table: ivf_health_demo_docs
  Objects: 1 | Rows: 1,000 | Null: 0 | Original: 176.03 KB | Compressed: 156.24 KB
  
  Objects:
  Object Name                                        | Rows         | Null Cnt   | Original Size   | Compressed Size
  ----------------------------------------------------------------------------------------------------------------------------------------------------
  0199e729-642e-71e0-b338-67c4980ee294_00000         | 1000         | 0          | 176.03 KB       | 156.24 KB

Index: idx_embedding_ivf_v2
  └─ (metadata): __mo_index_secondary_0199e725-0a7a-77b8-b689-ccdd0a33f581:272851
     Objects: 1 | Rows: 7 | Null: 0 | Original: 1.98 KB | Compressed: 940 B
     
     Objects:
     Object Name                                        | Rows         | Null Cnt   | Original Size   | Compressed Size
     ----------------------------------------------------------------------------------------------------------------------------------------------------
     0199e729-642a-7d36-ac37-0ae17325f7ec_00000         | 7            | 0          | 1.98 KB         | 940 B
  
  └─ (centroids): __mo_index_secondary_0199e725-0a7b-706e-8f0a-a50edc3621a1:272852
     Objects: 1 | Rows: 17 | Null: 0 | Original: 6.83 KB | Compressed: 3.09 KB
     
     Objects:
     Object Name                                        | Rows         | Null Cnt   | Original Size   | Compressed Size
     ----------------------------------------------------------------------------------------------------------------------------------------------------
     0199e729-6431-794d-9c13-d10f2e4a59e7_00000         | 17           | 0          | 6.83 KB         | 3.09 KB
  
  └─ (entries): __mo_index_secondary_0199e725-0a7c-77f4-8d0b-48fd8258098a:272853
     Objects: 1 | Rows: 1,000 | Null: 0 | Original: 696.11 KB | Compressed: 626.37 KB
     
     Objects:
     Object Name                                        | Rows         | Null Cnt   | Original Size   | Compressed Size
     ----------------------------------------------------------------------------------------------------------------------------------------------------
     0199e729-6438-7db1-bcb6-7b74c59b0aee_00000         | 1000         | 0          | 696.11 KB       | 626.37 KB
```

**Note**: 
- Hierarchical structure: Table → Index → Physical Tables → Objects
- IVF index has 3 physical tables: metadata (7 rows), centroids (17 rows), entries (1,000 rows)
- Physical table name format: `table_name:table_id`
- Use `-t` flag to include tombstone statistics for deleted/updated data

#### Database Operations

**`flush_table <table> [database]`**
- Flush main table and all its secondary index physical tables
- Includes IVF metadata/centroids/entries, HNSW, Fulltext, and regular indexes
- Requires sys user privileges

```
MO-DIAG[test]> flush_table ivf_health_demo_docs

🔄 Flushing table: test.ivf_health_demo_docs
✓ Main table flushed: ivf_health_demo_docs
📋 Found 3 index physical tables

Index: idx_embedding_ivf_v2
  ✓ metadata: __mo_index_secondary_0199e725-0a7a-77b8-b689-ccdd0a33f581
  ✓ centroids: __mo_index_secondary_0199e725-0a7b-706e-8f0a-a50edc3621a1
  ✓ entries: __mo_index_secondary_0199e725-0a7c-77f4-8d0b-48fd8258098a

📊 Summary:
  Main table: ✓ flushed
  Index tables: 3/3 flushed successfully
```

**Note**: 
- Automatically discovers and flushes all index physical tables
- For IVF indexes: flushes metadata, centroids, and entries tables
- Requires sys user privileges
- Recommended before backups or after bulk operations

**`tables [database]`**
- List all tables in current or specified database

**`databases`**
- List all databases (highlights current database)

**`use <database>`**
- Switch to a different database

**`sql <SQL statement>`**
- Execute arbitrary SQL query

```
MO-DIAG[test]> sql SELECT COUNT(*) FROM my_table

col0
----
10000

1 row(s) returned
```

#### Utility Commands

**`history [n | -c]`**
- Show last n commands (default: 20)
- `-c`: Clear command history

**`help [command]`**
- Show help for all commands or specific command

**`exit` / `quit`**
- Exit the interactive shell

### Command-Line Options

```
usage: mo-diag [-h] [--host HOST] [--port PORT] [--user USER]
               [--password PASSWORD] [--database DATABASE]
               [--log-level {DEBUG,INFO,WARNING,ERROR,CRITICAL}]
               [--command COMMAND]

options:
  --host HOST           Database host (default: localhost)
  --port PORT           Database port (default: 6001)
  --user USER           Database user (default: root)
  --password PASSWORD   Database password (default: 111)
  --database DATABASE   Database name (optional)
  -d DATABASE           Short form of --database
  --log-level LEVEL     Logging level (default: ERROR)
  --command COMMAND     Execute single command and exit
  -c COMMAND            Short form of --command
```

### Use Cases

#### 1. Monitor IVF Index Health in Production

```bash
# Quick check on index status
mo-diag -d production -c "show_ivf_status -v"

# Detailed index inspection with physical table stats
mo-diag -d production -c "show_indexes my_vector_table"

# Verify centroid distribution balance
mo-diag -d production -c "show_ivf_status -t my_vector_table -v"
```

#### 2. Debug Index Count Mismatches

```bash
# Check row consistency
mo-diag -d test -c "verify_counts my_table"

# Get health report for all indexes
mo-diag -d test -c "show_all_indexes"

# Flush to sync if needed
mo-diag -d test -c "flush_table my_table"
```

#### 3. Analyze Table Storage

```bash
# Get table statistics with all indexes
mo-diag -d test -c "show_table_stats my_table -a"

# Detailed object-level analysis
mo-diag -d test -c "show_table_stats my_table -a -d"

# Include tombstone statistics
mo-diag -d test -c "show_table_stats my_table -a -t -d"
```

#### 4. Automated Maintenance Scripts

```bash
#!/bin/bash
# daily_index_check.sh

DATABASES=("prod_db1" "prod_db2" "prod_db3")

for db in "${DATABASES[@]}"; do
    echo "Checking $db..."
    mo-diag -d "$db" -c "show_all_indexes" >> /var/log/mo_diag_daily.log
    mo-diag -d "$db" -c "show_ivf_status" >> /var/log/mo_diag_daily.log
done
```

### Tips and Best Practices

1. **Regular Health Checks**: Run `show_all_indexes` daily to catch index issues early
2. **Monitor IVF Balance**: Use `show_ivf_status -v` to ensure even centroid distribution
3. **Before Major Operations**: Always `verify_counts` before bulk updates or migrations
4. **Production Debugging**: Use non-interactive mode for logging and monitoring
5. **Tab Completion**: Leverage Tab completion to avoid typos in table/database names
6. **Command History**: Use `Ctrl+R` to quickly find and re-execute previous diagnostic commands
7. **Flush Regularly**: If you notice index count mismatches, `flush_table` can help sync

### Troubleshooting

**Issue**: Tab completion not working
- **Solution**: Ensure `prompt_toolkit>=3.0.0` is installed: `pip install prompt_toolkit`

**Issue**: "Unknown database" error
- **Solution**: Create the database first: `mo-diag -d test -c "sql CREATE DATABASE IF NOT EXISTS test"`

**Issue**: Permission denied for flush operations
- **Solution**: Connect as sys user or a user with sufficient privileges

**Issue**: IVF index shows 0 centroids
- **Solution**: The index might be empty or still building. Check with `show_table_stats <table> -a`

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


## Advanced Features

### PITR (Point-in-Time Recovery)

```python
# Create PITR for cluster


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

## Examples

Check out the `examples/` directory for comprehensive usage examples:

**Basic Examples:**
- `example_01_basic_connection.py` - Basic database operations and connectivity
- `example_02_account_management.py` - User and role management
- `example_03_async_operations.py` - Async/await operations with AsyncClient
- `example_04_transaction_management.py` - Transaction handling and rollback
- `example_05_snapshot_restore.py` - Snapshot creation and restore operations
- `example_06_sqlalchemy_integration.py` - SQLAlchemy ORM integration
- `example_07_advanced_features.py` - Advanced SDK features
- `example_08_pubsub_operations.py` - Pub/Sub operations
- `example_09_logger_integration.py` - Logging configuration and integration
- `example_10_version_management.py` - Version compatibility management
- `example_11_matrixone_version_demo.py` - MatrixOne version detection

**Vector Search Examples:**
- `example_12_vector_basics.py` - Vector data types and basic distance functions
- `example_13_vector_indexes.py` - IVF and HNSW index management and performance
- `example_14_vector_search.py` - Vector similarity search operations
- `example_15_vector_advanced.py` - Advanced vector operations and optimization

**ORM and Advanced Examples:**
- `example_18_snapshot_orm.py` - Snapshot operations with ORM
- `example_19_sqlalchemy_style_orm.py` - SQLAlchemy-style ORM patterns
- `example_20_sqlalchemy_engine_integration.py` - SQLAlchemy engine integration
- `example_21_advanced_orm_features.py` - Advanced ORM features
- `example_22_unified_sql_builder.py` - Unified SQL query builder
- `example_24_query_update.py` - Query and update operations
- `example_25_metadata_operations.py` - Table metadata analysis and statistics

**Specialized Examples:**
- `example_connection_hooks.py` - Connection hooks for custom initialization
- `example_dynamic_logging.py` - Dynamic logging configuration
- `example_ivf_stats_complete.py` - IVF index statistics and analysis

### Running Examples

Run all examples:
```bash
make examples
```

Run specific examples:
```bash
make example-basic    # Basic connection and database operations
make example-async    # Async/await operations
make example-vector   # Vector search and similarity
```

Run individual example files:
```bash
# Basic examples
python examples/example_01_basic_connection.py
python examples/example_03_async_operations.py

# Vector examples
python examples/example_12_vector_basics.py
python examples/example_13_vector_indexes.py
python examples/example_14_vector_search.py

# Metadata and advanced examples
python examples/example_25_metadata_operations.py
python examples/example_22_unified_sql_builder.py
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

The MatrixOne Python SDK includes comprehensive testing with support for multiple Python and SQLAlchemy versions.

**📖 For detailed testing documentation, see [TESTING.md](TESTING.md)**

#### Quick Test Commands

```bash
# Check environment and dependencies
make check-env

# Install test dependencies
make install-sqlalchemy14  # For SQLAlchemy 1.4.x
make install-sqlalchemy20  # For SQLAlchemy 2.0.x

# Run tests
make test-offline          # Fast mock-based tests
make test-online           # Database integration tests (requires MatrixOne)
make test-sqlalchemy14     # Full test suite with SQLAlchemy 1.4.x
make test-sqlalchemy20     # Full test suite with SQLAlchemy 2.0.x
make test-matrix           # Test both SQLAlchemy versions
```

#### Test Types

- **Offline Tests**: Mock-based unit tests (450+ tests, ~3-5 seconds)
- **Online Tests**: Database integration tests (100+ tests, ~20-30 seconds)
- **Matrix Tests**: Cross-version compatibility testing (Python 3.10/3.11 + SQLAlchemy 1.4/2.0)

### Development Setup

```bash
# Check environment requirements
make check-env

# Setup development environment
make dev-setup

# Run all tests
make test

# Run examples
make examples
```

For detailed testing instructions, environment setup, and troubleshooting, see [TESTING.md](TESTING.md).


## 📚 Documentation

### 📖 Online Documentation

**[Full Documentation on ReadTheDocs](https://matrixone.readthedocs.io/)** ⭐

Complete documentation with examples, API reference, and production best practices.

### Local Documentation Guides

**Getting Started:**
- **[Quick Start](docs/quickstart.rst)**: Get started quickly with common use cases
- **[Installation Guide](docs/installation.rst)**: Installation and setup instructions
- **[Configuration Guide](docs/configuration_guide.rst)**: Connection and client configuration

**Core Features:**
- **[Vector Guide](docs/vector_guide.rst)**: Vector search, indexing, and ⭐ **IVF health monitoring**
- **[Fulltext Guide](docs/fulltext_guide.rst)**: Fulltext search and indexing
- **[ORM Guide](docs/orm_guide.rst)**: SQLAlchemy ORM integration and patterns
- **[Metadata Guide](docs/metadata_guide.rst)**: Table metadata analysis and statistics

**Advanced Features:**
- **[Snapshot & Restore](docs/snapshot_restore_guide.rst)**: Snapshot and PITR operations
- **[Clone Guide](docs/clone_guide.rst)**: Database and table cloning
- **[Account Guide](docs/account_guide.rst)**: User and role management
- **[Pub/Sub Guide](docs/pubsub_guide.rst)**: Publication and subscription operations
- **[MoCTL Guide](docs/moctl_guide.rst)**: MoCTL integration

**Production Guide:**
- **[Best Practices](docs/best_practices.rst)**: Production-ready patterns using SDK APIs
- **[Connection Hooks](docs/connection_hooks_guide.rst)**: Pre/post connection hooks

**Reference:**
- **[API Reference](docs/api/index.rst)**: Complete API reference
- **[Examples](docs/examples.rst)**: Comprehensive usage examples
- **[Testing Guide](TESTING.md)**: Testing instructions and environment setup

Generate HTML documentation:
```bash
make docs          # Generate HTML documentation
make docs-serv     # Start documentation server at http://localhost:8000
```

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