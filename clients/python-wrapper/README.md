# MatrixOne Python SDK

A high-level Python SDK for MatrixOne that provides SQLAlchemy-like interface for database operations, snapshot management, PITR (Point-in-Time Recovery), restore operations, table cloning, and mo-ctl integration.

## Features

- 🚀 **SQLAlchemy Integration**: Seamless integration with SQLAlchemy for ORM operations
- 📸 **Snapshot Management**: Create, list, and manage database snapshots
- 🔍 **Flexible Snapshot Reads**: Single SQL snapshot reads with `FOR SNAPSHOT` clause
- ⏰ **PITR Support**: Point-in-Time Recovery functionality with recovery window display
- 🔄 **Restore Operations**: Restore databases, tables, and accounts from snapshots
- 📋 **Table Cloning**: Clone tables and databases with snapshot support
- 🛠️ **mo-ctl Integration**: Access MatrixOne control operations
- 🏗️ **High-level API**: Intuitive Python interface for complex operations
- 🔒 **Transaction Support**: Full transaction management with rollback capabilities

## Requirements
- Python 3.10+
- MatrixOne 3.0.0+
- SQLAlchemy 2.0.0+

## Installation

```bash
pip install matrixone
```

## Quick Start

### Basic Connection

```python
import matrixone
from sqlalchemy import create_engine, text

# Create MatrixOne client
client = matrixone.Client()
client.connect(
    host="localhost", 
    port=6001, 
    user="root", 
    password="111",
    database="test"
)

# Get SQLAlchemy engine for ORM operations
engine = client.get_sqlalchemy_engine()

# Execute raw SQL
with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print(result.fetchone())
```

### SQLAlchemy ORM Integration

```python
from sqlalchemy import Column, Integer, String, DateTime, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    email = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)

# Create tables
Base.metadata.create_all(engine)

# Use with SQLAlchemy ORM
Session = sessionmaker(bind=engine)
session = Session()

# Create user
user = User(name="John Doe", email="john@example.com")
session.add(user)
session.commit()

# Query users
users = session.query(User).all()
for user in users:
    print(f"User: {user.name}, Email: {user.email}")
```

## Snapshot Management

### Create Snapshots

```python
# Create cluster-level snapshot
snapshot = client.snapshots.create(
    name="daily_backup",
    level="cluster",  # cluster, account, database, table
    description="Daily backup snapshot"
)

# Create database-level snapshot
db_snapshot = client.snapshots.create(
    name="db_backup_20241201",
    level="database",
    database="my_database",
    description="Database backup for December 1st"
)

# Create table-level snapshot
table_snapshot = client.snapshots.create(
    name="table_backup",
    level="table",
    database="my_database",
    table="users",
    description="Users table backup"
)
```

### List and Manage Snapshots

```python
# List all snapshots
snapshots = client.snapshots.list()
for snapshot in snapshots:
    print(f"Snapshot: {snapshot.name}, Level: {snapshot.level}, Created: {snapshot.created_at}")

# Get specific snapshot
snapshot = client.snapshots.get("daily_backup")
print(f"Snapshot details: {snapshot}")

# Delete snapshot
client.snapshots.delete("daily_backup")
```

### Use Snapshots in Queries

```python
# Method 1: High-level snapshot query API
result = client.snapshot_query("daily_backup", "SELECT * FROM users WHERE age > 25")
print(result.fetchall())

# Method 2: SQLAlchemy with snapshot wrapper
with engine.connect() as conn:
    result = client.snapshot_query("daily_backup", "SELECT COUNT(*) FROM users", conn=conn)
    print(f"User count at snapshot: {result.scalar()}")

# Method 3: Context manager for multiple snapshot queries
with client.snapshot("daily_backup") as snapshot_client:
    result = snapshot_client.execute("SELECT * FROM users")
    print(result.fetchall())

# Method 4: Get SQLAlchemy engine with snapshot
snapshot_engine = client.get_snapshot_engine("daily_backup")
with snapshot_engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM users"))
    print(f"User count at snapshot: {result.scalar()}")

# Method 5: Mixed queries with snapshot helper
with engine.connect() as conn:
    # Current data
    current_count = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
    
    # Snapshot data using helper
    snapshot_count = client.snapshot_query("daily_backup", "SELECT COUNT(*) FROM users", conn=conn).scalar()
    
    print(f"Current users: {current_count}, Snapshot users: {snapshot_count}")
```

### Advanced Snapshot Query Methods

```python
# Method 1: Simple snapshot query
result = client.snapshot_query("daily_backup", "SELECT * FROM users WHERE age > 25")
print(result.fetchall())

# Method 2: Complex snapshot query with JOINs
result = client.snapshot_query("daily_backup", """
    SELECT u.name, u.email, p.title 
    FROM users u 
    JOIN profiles p ON u.id = p.user_id 
    WHERE u.created_at > '2024-01-01'
""")
print(result.fetchall())

# Method 3: Snapshot query with parameters
result = client.snapshot_query("daily_backup", 
    "SELECT * FROM users WHERE age > ? AND city = ?", 
    params=[25, "New York"]
)
print(result.fetchall())

# Method 4: Batch snapshot queries for analytics
def analyze_user_trends():
    # Current data
    current_users = client.execute("SELECT COUNT(*) FROM users").scalar()
    
    # Data from different snapshots using helper
    week_ago = f"daily_backup_{(datetime.now() - timedelta(days=7)).strftime('%Y%m%d')}"
    month_ago = f"daily_backup_{(datetime.now() - timedelta(days=30)).strftime('%Y%m%d')}"
    
    week_ago_users = client.snapshot_query(week_ago, "SELECT COUNT(*) FROM users").scalar()
    month_ago_users = client.snapshot_query(month_ago, "SELECT COUNT(*) FROM users").scalar()
    
    # Calculate growth rates
    weekly_growth = current_users - week_ago_users
    monthly_growth = current_users - month_ago_users
    
    print(f"Weekly growth: {weekly_growth}, Monthly growth: {monthly_growth}")

# Method 5: Snapshot comparison helper
def audit_data_changes():
    # Get current data
    current_data = client.execute("SELECT id, name, email FROM users ORDER BY id").fetchall()
    
    # Get snapshot data using helper
    snapshot_data = client.snapshot_query("daily_backup", 
        "SELECT id, name, email FROM users ORDER BY id").fetchall()
    
    # Compare and find changes
    current_dict = {row[0]: (row[1], row[2]) for row in current_data}
    snapshot_dict = {row[0]: (row[1], row[2]) for row in snapshot_data}
    
    changes = []
    for user_id in current_dict:
        if user_id not in snapshot_dict:
            changes.append(f"New user: {user_id}")
        elif current_dict[user_id] != snapshot_dict[user_id]:
            changes.append(f"Updated user: {user_id}")
    
    for user_id in snapshot_dict:
        if user_id not in current_dict:
            changes.append(f"Deleted user: {user_id}")
    
    print(f"Changes detected: {len(changes)}")
    for change in changes:
        print(change)

# Method 6: Complex snapshot analysis with helper
def complex_snapshot_analysis():
    result = client.snapshot_query("daily_backup", """
        SELECT 
            u.name,
            u.email,
            COUNT(o.id) as order_count,
            SUM(o.amount) as total_spent
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.created_at BETWEEN '2024-01-01' AND '2024-12-31'
        GROUP BY u.id, u.name, u.email
        HAVING COUNT(o.id) > 5
        ORDER BY total_spent DESC
        LIMIT 10
    """)
    
    top_customers = result.fetchall()
    print("Top customers from snapshot:")
    for customer in top_customers:
        print(f"{customer.name}: {customer.order_count} orders, ${customer.total_spent}")

# Method 7: Snapshot query with SQLAlchemy connection
def snapshot_with_sqlalchemy():
    with engine.connect() as conn:
        # Use existing connection for snapshot query
        result = client.snapshot_query("daily_backup", 
            "SELECT COUNT(*) FROM users", 
            conn=conn
        )
        print(f"Snapshot user count: {result.scalar()}")

# Method 8: Snapshot query builder for complex scenarios
def snapshot_query_builder():
    # Build complex snapshot queries
    query_builder = client.snapshot_query_builder("daily_backup")
    
    # Chain methods for complex queries
    result = (query_builder
        .select("u.name", "u.email", "COUNT(o.id) as order_count")
        .from_table("users u")
        .left_join("orders o", "u.id = o.user_id")
        .where("u.created_at > ?", "2024-01-01")
        .group_by("u.id", "u.name", "u.email")
        .having("COUNT(o.id) > ?", 5)
        .order_by("order_count DESC")
        .limit(10)
        .execute()
    )
    
    print("Top customers from snapshot:")
    for customer in result.fetchall():
        print(f"{customer.name}: {customer.order_count} orders")

# Method 9: Snapshot data comparison utilities
def snapshot_comparison():
    # Compare data between current and snapshot
    comparison = client.compare_snapshots(
        current_query="SELECT id, name, email FROM users",
        snapshot_name="daily_backup",
        snapshot_query="SELECT id, name, email FROM users"
    )
    
    print(f"Added users: {len(comparison.added)}")
    print(f"Modified users: {len(comparison.modified)}")
    print(f"Deleted users: {len(comparison.deleted)}")
    
    # Get detailed changes
    for change in comparison.changes:
        print(f"Change: {change.type} - User {change.id}")

# Method 10: Snapshot analytics helpers
def snapshot_analytics():
    # Get snapshot statistics
    stats = client.snapshot_stats("daily_backup", "users")
    print(f"Snapshot stats: {stats}")
    
    # Compare metrics across snapshots
    metrics = client.compare_snapshot_metrics(
        snapshots=["daily_backup", "weekly_backup"],
        query="SELECT COUNT(*) as user_count FROM users"
    )
    
    for snapshot, metric in metrics.items():
        print(f"{snapshot}: {metric.user_count} users")
```

## Point-in-Time Recovery (PITR)

PITR (Point-in-Time Recovery) is a MatrixOne feature that enables continuous backup and recovery to any point in time within a specified recovery window. This feature provides:

- **Continuous Backup**: Automatic backup of transaction logs
- **Recovery Window**: Time range within which recovery is possible
- **Point-in-Time Recovery**: Restore to any specific timestamp
- **Granular Recovery**: Support for cluster, account, database, and table level recovery

### PITR Recovery Window

The recovery window defines the time range during which PITR can recover data. It includes:
- **Start Time**: Earliest recoverable timestamp
- **End Time**: Latest recoverable timestamp  
- **Duration**: Total time span available for recovery
- **Status**: Whether the recovery window is active and valid

### Create PITR

```python
# Create PITR for continuous backup
pitr = client.pitr.create(
    name="continuous_backup",
    level="account",  # cluster, account, database, table
    description="Continuous point-in-time recovery"
)

# Create PITR for specific database
db_pitr = client.pitr.create(
    name="db_pitr",
    level="database",
    database="my_database",
    description="Database PITR"
)
```

### Restore from PITR

```python
from datetime import datetime, timedelta

# Restore to specific timestamp
restore_time = datetime.now() - timedelta(hours=2)
client.pitr.restore(
    name="continuous_backup",
    timestamp=restore_time,
    level="account"
)

# Restore specific database to timestamp
client.pitr.restore(
    name="db_pitr",
    timestamp=restore_time,
    level="database",
    database="my_database"
)

# Restore specific table to timestamp
client.pitr.restore(
    name="db_pitr",
    timestamp=restore_time,
    level="table",
    database="my_database",
    table="users"
)
```

### PITR Recovery Window

```python
# List all PITR configurations
pitr_list = client.pitr.list()
for pitr in pitr_list:
    print(f"PITR: {pitr.name}, Level: {pitr.level}")

# Display recovery window for PITR
recovery_window = client.pitr.get_recovery_window("continuous_backup")
print(f"Recovery window: {recovery_window.start_time} to {recovery_window.end_time}")
print(f"Available recovery time: {recovery_window.duration}")

# Check if timestamp is within recovery window
restore_time = datetime.now() - timedelta(hours=1)
if recovery_window.contains(restore_time):
    print("Timestamp is within recovery window")
else:
    print("Timestamp is outside recovery window")

# Get recovery window details
window_details = client.pitr.get_recovery_window_details("continuous_backup")
print(f"Window details: {window_details}")

# Delete PITR
client.pitr.delete("continuous_backup")
```

### PITR Usage Examples

```python
# Example 1: Monitor PITR recovery window
def monitor_pitr_window():
    pitr_list = client.pitr.list()
    for pitr in pitr_list:
        window = client.pitr.get_recovery_window(pitr.name)
        print(f"PITR: {pitr.name}")
        print(f"  Recovery window: {window.start_time} to {window.end_time}")
        print(f"  Duration: {window.duration}")
        print(f"  Status: {'Active' if window.is_valid() else 'Inactive'}")
        
        # Check remaining recovery time
        remaining = window.get_remaining_time()
        print(f"  Remaining time: {remaining}")

# Example 2: Safe PITR restore with window validation
def safe_pitr_restore(pitr_name, restore_time):
    # Check if PITR exists
    if not client.pitr.exists(pitr_name):
        raise ValueError(f"PITR {pitr_name} does not exist")
    
    # Get recovery window
    window = client.pitr.get_recovery_window(pitr_name)
    
    # Validate restore time
    if not window.contains(restore_time):
        raise ValueError(f"Restore time {restore_time} is outside recovery window")
    
    # Perform restore
    client.pitr.restore(pitr_name, restore_time, level="database", database="my_database")
    print(f"Successfully restored to {restore_time}")

# Example 3: PITR status monitoring
def pitr_status_monitor():
    pitr_list = client.pitr.list()
    for pitr in pitr_list:
        window = client.pitr.get_recovery_window(pitr.name)
        details = client.pitr.get_recovery_window_details(pitr.name)
        
        print(f"PITR: {pitr.name}")
        print(f"  Level: {pitr.level}")
        print(f"  Status: {details.get('status', 'Unknown')}")
        print(f"  Last backup: {details.get('last_backup', 'Unknown')}")
        print(f"  Backup frequency: {details.get('backup_frequency', 'Unknown')}")
        print(f"  Storage used: {details.get('storage_used', 'Unknown')}")
```

## Restore Operations

### Restore from Snapshots

```python
# Restore entire cluster from snapshot
client.restore.from_snapshot(
    snapshot_name="daily_backup",
    level="cluster"
)

# Restore account from snapshot
client.restore.from_snapshot(
    snapshot_name="account_backup",
    level="account",
    account="my_account"
)

# Restore database from snapshot
client.restore.from_snapshot(
    snapshot_name="db_backup_20241201",
    level="database",
    database="my_database"
)

# Restore table from snapshot
client.restore.from_snapshot(
    snapshot_name="table_backup",
    level="table",
    database="my_database",
    table="users"
)
```

### Restore with Custom Options

```python
# Restore with specific target account
client.restore.from_snapshot(
    snapshot_name="account_backup",
    level="account",
    target_account="new_account"
)

# Restore with custom database name
client.restore.from_snapshot(
    snapshot_name="db_backup_20241201",
    level="database",
    database="my_database",
    target_database="restored_database"
)
```

## Table and Database Cloning

### Clone Tables

```python
# Clone table with current data
client.clone.table(
    source_database="source_db",
    source_table="users",
    target_database="target_db",
    target_table="users_clone"
)

# Clone table from specific snapshot
client.clone.table(
    source_database="source_db",
    source_table="users",
    target_database="target_db",
    target_table="users_snapshot",
    snapshot="daily_backup"
)

# Clone table with timestamp
from datetime import datetime
client.clone.table(
    source_database="source_db",
    source_table="users",
    target_database="target_db",
    target_table="users_timestamp",
    timestamp=datetime(2024, 12, 1, 10, 0, 0)
)
```

### Clone Databases

```python
# Clone entire database
client.clone.database(
    source_database="source_db",
    target_database="target_db_clone"
)

# Clone database from snapshot
client.clone.database(
    source_database="source_db",
    target_database="target_db_snapshot",
    snapshot="daily_backup"
)

# Clone database to different account
client.clone.database(
    source_database="source_db",
    target_database="target_db",
    target_account="new_account"
)
```

## mo-ctl Integration

### Control Operations

```python
# Ping MatrixOne service
result = client.ctl.ping()
print(f"Service status: {result}")

# Flush data to storage
client.ctl.flush()

# Force garbage collection
client.ctl.force_gc()

# Get snapshot timestamp
snapshot_ts = client.ctl.get_snapshot_ts()
print(f"Current snapshot timestamp: {snapshot_ts}")

# Use specific snapshot timestamp
client.ctl.use_snapshot_ts(snapshot_ts)

# Create checkpoint
client.ctl.checkpoint()

# Create global checkpoint
client.ctl.global_checkpoint()
```

### Advanced Control Operations

```python
# Inspect table metadata
inspection = client.ctl.inspect_table("my_database", "users")
print(f"Table inspection: {inspection}")

# Get table statistics
stats = client.ctl.get_table_stats("my_database", "users")
print(f"Table stats: {stats}")

# Merge objects for optimization
client.ctl.merge_objects("my_database", "users")

# Set label for node
client.ctl.set_label("region", "us-west-1")

# Set work state
client.ctl.set_work_state("draining")

# Add fault point for testing
client.ctl.add_fault_point("test_fault", "panic", "100%")

# Remove fault point
client.ctl.remove_fault_point("test_fault")
```

## Transaction Management

### Basic Transactions

```python
# Start transaction
with client.transaction() as tx:
    tx.execute("INSERT INTO users (name, email) VALUES ('John', 'john@example.com')")
    tx.execute("INSERT INTO users (name, email) VALUES ('Jane', 'jane@example.com')")
    # Transaction commits automatically on success

# Manual transaction control
tx = client.begin_transaction()
try:
    tx.execute("INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')")
    tx.commit()
except Exception as e:
    tx.rollback()
    raise e
```

### Transaction with Snapshot

```python
# Transaction with specific snapshot for all queries
with client.transaction(snapshot="daily_backup") as tx:
    result = tx.execute("SELECT * FROM users")
    users = result.fetchall()
    # All queries in this transaction use the snapshot

# Transaction with mixed snapshot and current data queries
with client.transaction() as tx:
    # Current data
    current_users = tx.execute("SELECT * FROM users").fetchall()
    
    # Snapshot data
    snapshot_users = tx.execute("SELECT * FROM users FOR SNAPSHOT 'daily_backup'").fetchall()
    
    # Compare data
    print(f"Current users: {len(current_users)}, Snapshot users: {len(snapshot_users)}")
```

## Error Handling

```python
from matrixone.exceptions import (
    MatrixOneError,
    ConnectionError,
    SnapshotError,
    PITRError,
    RestoreError,
    CloneError
)

try:
    client.snapshots.create(name="test_snapshot", level="cluster")
except SnapshotError as e:
    print(f"Snapshot error: {e}")
except MatrixOneError as e:
    print(f"MatrixOne error: {e}")
```

## Configuration

### Connection Configuration

```python
# Advanced connection configuration
client = matrixone.Client(
    connection_timeout=30,
    query_timeout=300,
    auto_commit=True,
    charset='utf8mb4'
)

client.connect(
    host="localhost",
    port=6001,
    user="root",
    password="111",
    database="test",
    ssl_mode="preferred",  # disabled, preferred, required
    ssl_ca="/path/to/ca.pem",
    ssl_cert="/path/to/client-cert.pem",
    ssl_key="/path/to/client-key.pem"
)
```

### SQLAlchemy Engine Configuration

```python
# Get SQLAlchemy engine with custom configuration
engine = client.get_sqlalchemy_engine(
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=3600,
    echo=True  # Enable SQL logging
)
```

## Best Practices

### 1. Connection Management

```python
# Use context managers for automatic cleanup
with matrixone.Client() as client:
    client.connect(host="localhost", port=6001, user="root", password="111")
    # Client automatically disconnects when exiting context
```

### 2. Snapshot Strategy

```python
# Create regular snapshots
import schedule
import time

def create_daily_snapshot():
    client.snapshots.create(
        name=f"daily_backup_{datetime.now().strftime('%Y%m%d')}",
        level="cluster",
        description="Daily automated backup"
    )

# Schedule daily snapshots
schedule.every().day.at("02:00").do(create_daily_snapshot)

while True:
    schedule.run_pending()
    time.sleep(60)

# Flexible snapshot usage in analytics
def compare_data_growth():
    # Get current data
    current_stats = client.execute("""
        SELECT COUNT(*) as user_count, 
               MAX(created_at) as latest_user
        FROM users
    """).fetchone()
    
    # Get snapshot data from yesterday using helper
    yesterday_snapshot = f"daily_backup_{(datetime.now() - timedelta(days=1)).strftime('%Y%m%d')}"
    snapshot_stats = client.snapshot_query(yesterday_snapshot, """
        SELECT COUNT(*) as user_count, 
               MAX(created_at) as latest_user
        FROM users
    """).fetchone()
    
    growth = current_stats.user_count - snapshot_stats.user_count
    print(f"User growth in 24h: {growth}")

# Advanced snapshot analytics with query builder
def advanced_snapshot_analytics():
    # Use query builder for complex snapshot analysis
    builder = client.snapshot_query_builder("daily_backup")
    
    # Build complex query
    result = (builder
        .select("DATE(created_at) as date", "COUNT(*) as daily_users")
        .from_table("users")
        .where("created_at >= ?", "2024-01-01")
        .group_by("DATE(created_at)")
        .order_by("date DESC")
        .limit(30)
        .execute()
    )
    
    print("Daily user growth from snapshot:")
    for row in result.fetchall():
        print(f"{row.date}: {row.daily_users} users")
```

### 3. Error Recovery

```python
def safe_restore(snapshot_name, max_retries=3):
    for attempt in range(max_retries):
        try:
            client.restore.from_snapshot(snapshot_name, level="database")
            break
        except RestoreError as e:
            if attempt == max_retries - 1:
                raise e
            time.sleep(2 ** attempt)  # Exponential backoff
```

## API Reference

### Client Class

```python
class Client:
    def connect(self, host, port, user, password, database, **kwargs)
    def disconnect(self)
    def execute(self, sql, params=None)
    def get_sqlalchemy_engine(self, **kwargs)
    def get_snapshot_engine(self, snapshot_name, **kwargs)
    def transaction(self, snapshot=None)
    def begin_transaction(self, snapshot=None)
    
    # Snapshot query methods
    def snapshot_query(self, snapshot_name, sql, params=None, conn=None)
    def snapshot_query_builder(self, snapshot_name) -> SnapshotQueryBuilder
    def compare_snapshots(self, current_query, snapshot_name, snapshot_query)
    def snapshot_stats(self, snapshot_name, table_name)
    def compare_snapshot_metrics(self, snapshots, query)
    
    # Snapshot context manager
    def snapshot(self, snapshot_name)
    
    # Properties
    @property
    def snapshots(self) -> SnapshotManager
    @property
    def pitr(self) -> PITRManager
    @property
    def restore(self) -> RestoreManager
    @property
    def clone(self) -> CloneManager
    @property
    def ctl(self) -> ControlManager
```

### Snapshot Manager

```python
class SnapshotManager:
    def create(self, name, level, database=None, table=None, description=None)
    def list(self)
    def get(self, name)
    def delete(self, name)
    def exists(self, name) -> bool
```

### Snapshot Query Builder

```python
class SnapshotQueryBuilder:
    def select(self, *columns) -> SnapshotQueryBuilder
    def from_table(self, table) -> SnapshotQueryBuilder
    def join(self, table, condition) -> SnapshotQueryBuilder
    def left_join(self, table, condition) -> SnapshotQueryBuilder
    def right_join(self, table, condition) -> SnapshotQueryBuilder
    def where(self, condition, *params) -> SnapshotQueryBuilder
    def group_by(self, *columns) -> SnapshotQueryBuilder
    def having(self, condition, *params) -> SnapshotQueryBuilder
    def order_by(self, *columns) -> SnapshotQueryBuilder
    def limit(self, count) -> SnapshotQueryBuilder
    def offset(self, count) -> SnapshotQueryBuilder
    def execute(self) -> ResultSet
```

### PITR Manager

```python
class PITRManager:
    def create(self, name, level, database=None, description=None)
    def list(self)
    def get(self, name)
    def delete(self, name)
    def exists(self, name) -> bool
    def restore(self, name, timestamp, level, database=None, table=None)
    def get_recovery_window(self, name) -> RecoveryWindow
    def get_recovery_window_details(self, name) -> dict
```

### Recovery Window

```python
class RecoveryWindow:
    def __init__(self, start_time, end_time, duration)
    def contains(self, timestamp) -> bool
    def is_valid(self) -> bool
    def get_remaining_time(self) -> timedelta
```

### Restore Manager

```python
class RestoreManager:
    def from_snapshot(self, snapshot_name, level, **kwargs)
    def from_pitr(self, pitr_name, timestamp, level, **kwargs)
```

### Clone Manager

```python
class CloneManager:
    def table(self, source_database, source_table, target_database, target_table, **kwargs)
    def database(self, source_database, target_database, **kwargs)
```

### Control Manager

```python
class ControlManager:
    def ping(self)
    def flush(self)
    def force_gc(self)
    def get_snapshot_ts(self)
    def use_snapshot_ts(self, timestamp)
    def checkpoint(self)
    def global_checkpoint(self)
    def inspect_table(self, database, table)
    def get_table_stats(self, database, table)
    def merge_objects(self, database, table)
    def set_label(self, key, value)
    def set_work_state(self, state)
    def add_fault_point(self, name, action, frequency)
    def remove_fault_point(self, name)
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

- 📖 [Documentation](https://docs.matrixorigin.cn/)
- 💬 [Community Forum](https://github.com/matrixorigin/matrixone/discussions)
- 🐛 [Issue Tracker](https://github.com/matrixorigin/matrixone/issues)
- 📧 [Email Support](mailto:support@matrixorigin.cn)
