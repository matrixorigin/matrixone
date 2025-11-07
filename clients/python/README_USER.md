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
- 📤 **Data Export**: Pandas-style export with intuitive ``to_csv()`` and ``to_jsonl()`` methods
  - Export to local files or external stages (``stage://`` protocol)
  - Support for raw SQL, SQLAlchemy, and MatrixOne queries
  - Transaction-aware exports for consistency
- 🔁 **Change Data Capture (CDC)**: Create, monitor, and control CDC tasks directly from Python (`client.cdc`, `async_client.cdc`)
- 📸 **Snapshot Management**: Create and manage database snapshots at multiple levels
- ⏰ **Point-in-Time Recovery**: PITR functionality for precise data recovery
- 🔄 **Table Cloning**: Clone databases and tables efficiently
- 👥 **Account Management**: Comprehensive user and role management
- 📊 **Pub/Sub**: Real-time publication and subscription support
- 🔧 **Version Management**: Automatic backend version detection and compatibility
- 🛡️ **Type Safety**: Full type hints support with comprehensive documentation
- 📚 **SQLAlchemy Integration**: Seamless SQLAlchemy ORM integration with enhanced features
- 🛠️ **mo-diag CLI Tool**: Interactive diagnostic tool for MatrixOne database maintenance
  - Index health monitoring and verification
  - IVF/HNSW vector index status inspection
  - Table statistics and metadata analysis
  - Interactive shell with Tab completion and command history
  - Non-interactive mode for scripting and automation
  - Batch operations on tables and indexes

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

## ⚠️ Important: Column Naming Convention

**🚨 CRITICAL: Always use lowercase with underscores (snake_case) for column names!**

MatrixOne does not support SQL standard double-quoted identifiers in queries, which causes issues with camelCase column names when using SQLAlchemy ORM.

```python
# ❌ DON'T: CamelCase column names (will fail in SELECT queries)
class User(Base):
    userName = Column(String(50))      # CREATE succeeds, SELECT fails!
    userId = Column(Integer)           # Will cause SQL syntax errors

# ✅ DO: Use lowercase with underscores (snake_case)
class User(Base):
    user_name = Column(String(50))     # Works perfectly
    user_id = Column(Integer)          # All operations succeed
```

**Why this matters:**
- ✅ CREATE TABLE works with both styles (uses backticks)
- ✅ INSERT works with both styles  
- ❌ **SELECT fails with camelCase** (uses double quotes, not supported by MatrixOne)

**Example of the problem:**
```python
# CamelCase generates: SELECT "userName" FROM user  ❌ Fails!
# snake_case generates: SELECT user_name FROM user  ✅ Works!
```

---

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

### Transaction Management (Recommended)

Use `client.session()` for atomic transactions with automatic commit/rollback:

```python
from matrixone import Client
from sqlalchemy import select, insert, update, delete

client = Client()
client.connect(database='test')

# Transaction with automatic commit/rollback
with client.session() as session:
    # All operations are atomic
    session.execute(insert(User).values(name='Alice', age=30))
    session.execute(update(User).where(User.age < 18).values(status='minor'))
    
    # Query within transaction
    stmt = select(User).where(User.age > 25)
    result = session.execute(stmt)
    users = result.scalars().all()
    # Commits automatically on success, rolls back on error

client.disconnect()
```

**Why use sessions?**
- ✅ Atomic operations - all succeed or fail together
- ✅ Automatic rollback on errors
- ✅ Access to all MatrixOne managers (snapshots, clones, load_data, etc.)
- ✅ Full SQLAlchemy ORM support

### Data Loading (Pandas-Style)

Load bulk data from files with pandas-compatible API:

```python
from matrixone import Client
from matrixone.orm import declarative_base, Column, Integer, String

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String(100))

client = Client()
client.connect(database='test')
client.create_table(User)

# CSV loading (pandas-style)
client.load_data.read_csv('users.csv', table=User, skiprows=1)

# Custom separator (pandas-style)
client.load_data.read_csv('users.txt', table=User, sep='|')

# JSON Lines (pandas-style)
client.load_data.read_json('events.jsonl', table='events', lines=True)

# Parquet (pandas-style)
client.load_data.read_parquet('data.parquet', table=User)

# From stage
client.load_data.read_csv_stage('s3_stage', 'users.csv', table=User)

client.disconnect()
```

### Data Export (Pandas-Style)

Export query results with pandas-compatible API:

```python
from matrixone import Client
from sqlalchemy import select

client = Client()
client.connect(database='test')

# CSV export (pandas-style)
client.export.to_csv('/tmp/users.csv', "SELECT * FROM users")

# TSV export
client.export.to_csv('/tmp/users.tsv', "SELECT * FROM users", sep='\t')

# JSONL export
client.export.to_jsonl('/tmp/users.jsonl', "SELECT * FROM users")

# Export with SQLAlchemy
stmt = select(User).where(User.age > 25)
client.export.to_csv('/tmp/adults.csv', stmt)

# Export to external stage (using stage:// protocol)
client.export.to_csv('stage://s3_stage/backup.csv', stmt)

# Export to external stage (using convenience method)
client.export.to_csv_stage('s3_stage', 'backup2.csv', stmt)

client.disconnect()
```

### CDC Task Management

Create and control Change Data Capture (CDC) tasks directly from the Python SDK:

```python
from matrixone import Client, build_mysql_uri

client = Client()
client.connect(database='test')

source_uri = build_mysql_uri('127.0.0.1', 6001, user='admin', password='111', account='acct')
mysql_sink = build_mysql_uri('192.168.1.100', 3306, user='root', password='111')

# Database-level replication (automatically ensures Level='database')
client.cdc.create_database_task(
    task_name='replicate_sales',
    source_uri=source_uri,
    sink_type='mysql',
    sink_uri=mysql_sink,
    source_database='sales',
    sink_database='sales_archive',
    options={'Frequency': '30m'}
)

# Table-level replication helper (list of tuples/dicts/strings)
client.cdc.create_table_task(
    task_name='replicate_key_tables',
    source_uri=source_uri,
    sink_type='mysql',
    sink_uri=mysql_sink,
    table_mappings=[('sales', 'orders', 'sales_archive', 'orders_backup'), 'sales.customers:sales_archive.customers']
)

# Pause/resume lifecycle
client.cdc.pause('replicate_sales')
client.cdc.resume('replicate_sales')

# Inspect status and per-table watermarks
tasks = client.cdc.list()
watermarks = client.cdc.list_watermarks('replicate_sales')

# Drop task when no longer needed
client.cdc.drop('replicate_sales')
client.cdc.drop('replicate_key_tables')

client.disconnect()
```

> Async usage: call the same helpers via `await async_client.cdc.create_database_task(...)` / `create_table_task(...)` or inside `async with client.session()`.

### Wrapping Existing Sessions (For Legacy Code)

If you have existing SQLAlchemy code, wrap your sessions to add MatrixOne features:

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from matrixone import Client
from matrixone.session import Session as MatrixOneSession

# Your existing SQLAlchemy code
engine = create_engine('mysql+pymysql://root:111@localhost:6001/test')
SessionFactory = sessionmaker(bind=engine)
sqlalchemy_session = SessionFactory()

# Create MatrixOne client
mo_client = Client()
mo_client.connect(host='localhost', port=6001, user='root', password='111', database='test')

# Wrap existing session with MatrixOne features
mo_session = MatrixOneSession(
    client=mo_client,
    wrap_session=sqlalchemy_session
)

# Now use both SQLAlchemy and MatrixOne features
mo_session.execute("SELECT * FROM users")  # Standard SQLAlchemy
mo_session.stage.create_s3('backup', bucket='my-backups')  # MatrixOne feature
mo_session.snapshots.create('backup', level='database')  # MatrixOne feature
mo_session.commit()
mo_session.close()
```

**Perfect for:**
- 🔄 Migrating legacy projects incrementally
- 🏢 Adding MatrixOne to existing applications
- 📦 Testing new features without refactoring

### SQLAlchemy ORM Integration

The SDK provides seamless SQLAlchemy integration:

```python
from matrixone import Client
from matrixone.orm import Base, Column, Integer, String, select, insert, update

# Define ORM models
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(255))
    age = Column(Integer)

client = Client()
client.connect(database='test')

# Create table from model
client.create_table(User)

# ORM-style operations
stmt = insert(User).values(name='John', email='john@example.com', age=30)
client.execute(stmt)

stmt = select(User).where(User.age > 25)
result = client.execute(stmt)
for user in result.scalars():
    print(f"{user.name}: {user.email}")

client.disconnect()
```

**Recommended approach:**
- ✅ Use SQLAlchemy statements (`select`, `insert`, `update`, `delete`)
- ✅ Use `session()` for multi-statement transactions
- ✅ Use `client.execute()` for single-statement operations

### Async Usage

Full async/await support with async sessions:

```python
import asyncio
from matrixone import AsyncClient
from sqlalchemy import select, insert, update

async def main():
    client = AsyncClient()
    await client.connect(database='test')
    
    # Basic async query
    result = await client.execute(select(User).where(User.age > 25))
    print(result.fetchall())
    
    # Async transaction
    async with client.session() as session:
        # All operations are atomic
        await session.execute(insert(User).values(name='Alice', age=30))
        await session.execute(update(User).where(User.age < 18).values(status='minor'))
        
        # Concurrent queries
        user_result, order_result = await asyncio.gather(
            session.execute(select(User)),
            session.execute(select(Order))
        )
        # Commits automatically
    
    await client.disconnect()

asyncio.run(main())
```

**Async advantages:**
- ✅ Non-blocking operations
- ✅ Concurrent execution with `asyncio.gather()`
- ✅ Perfect for web frameworks (FastAPI, aiohttp)

### Snapshot Management

Create and manage database snapshots. Use `session()` for atomic operations:

```python
from matrixone import SnapshotLevel

# Single snapshot operation
snapshot = client.snapshots.create(
    name='my_snapshot',
    level=SnapshotLevel.DATABASE,
    database='production'
)

# List snapshots
snapshots = client.snapshots.list()
for snap in snapshots:
    print(f"Snapshot: {snap.name}, Created: {snap.created_at}")

# Atomic snapshot + clone in transaction
with client.session() as session:
    # Create snapshot
    snapshot = session.snapshots.create(
        name='daily_backup',
        level=SnapshotLevel.DATABASE,
        database='production'
    )
    
    # Clone from snapshot atomically
    session.clone.clone_database(
        target_db='production_copy',
        source_db='production',
        snapshot_name='daily_backup'
    )
    # Both operations commit together
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
