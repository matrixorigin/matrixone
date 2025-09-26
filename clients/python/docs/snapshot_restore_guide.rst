Snapshot and Restore Guide
===========================

This guide covers snapshot creation, restoration, and point-in-time recovery (PITR) operations in the MatrixOne Python SDK, enabling data backup, disaster recovery, and version control.

Overview
--------

MatrixOne's snapshot and restore system provides:

* **Database Snapshots**: Create point-in-time snapshots of entire databases
* **Table Snapshots**: Create snapshots of specific tables
* **Incremental Snapshots**: Efficient backup of changes since last snapshot
* **Point-in-Time Recovery**: Restore data to any specific timestamp
* **Cross-Database Restore**: Restore snapshots to different databases
* **Snapshot Management**: List, query, and delete snapshots
* **Automated Backup**: Schedule regular snapshot creation

Getting Started
---------------

Basic Setup
~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params

   # Connect to MatrixOne
   connection_params = get_connection_params()
   client = Client(*connection_params)
   client.connect(*connection_params)

   # Get snapshot manager
   snapshot_manager = client.snapshot

Snapshot Creation
-----------------

Database Snapshots
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Create a full database snapshot
   snapshot = snapshot_manager.create(
       name="daily_backup_20240115",
       level=SnapshotLevel.DATABASE,
       database="production_db",
       description="Daily backup of production database"
   )
   print(f"Created snapshot: {snapshot.name}")

   # Create snapshot with compression
   snapshot = snapshot_manager.create(
       name="compressed_backup",
       level=SnapshotLevel.DATABASE,
       database="analytics_db",
       compression=True,
       description="Compressed backup for long-term storage"
   )

   # Create incremental snapshot
   snapshot = snapshot_manager.create(
       name="incremental_backup",
       level=SnapshotLevel.DATABASE,
       database="production_db",
       incremental=True,
       base_snapshot="daily_backup_20240114"
   )

Table Snapshots
~~~~~~~~~~~~~~~

.. code-block:: python

   # Create table snapshot
   snapshot = snapshot_manager.create(
       name="users_table_backup",
       level=SnapshotLevel.TABLE,
       database="production_db",
       table="users",
       description="Backup of users table"
   )

   # Create snapshot of multiple tables
   snapshot = snapshot_manager.create(
       name="multi_table_backup",
       level=SnapshotLevel.TABLE,
       database="production_db",
       tables=["users", "orders", "products"],
       description="Backup of critical tables"
   )

   # Create table snapshot with specific columns
   snapshot = snapshot_manager.create(
       name="users_data_backup",
       level=SnapshotLevel.TABLE,
       database="production_db",
       table="users",
       columns=["id", "username", "email", "created_at"],
       description="Backup of essential user data"
   )

Snapshot Management
-------------------

Listing Snapshots
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # List all snapshots
   snapshots = snapshot_manager.list()
   for snapshot in snapshots:
       print(f"Snapshot: {snapshot.name}")
       print(f"  Level: {snapshot.level}")
       print(f"  Database: {snapshot.database}")
       print(f"  Created: {snapshot.created_at}")
       print(f"  Size: {snapshot.size_mb} MB")

   # List snapshots for specific database
   db_snapshots = snapshot_manager.list(database="production_db")
   for snapshot in db_snapshots:
       print(f"Database snapshot: {snapshot.name}")

   # List table snapshots
   table_snapshots = snapshot_manager.list(level=SnapshotLevel.TABLE)
   for snapshot in table_snapshots:
       print(f"Table snapshot: {snapshot.name}")

Getting Snapshot Details
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Get specific snapshot
   snapshot = snapshot_manager.get("daily_backup_20240115")
   if snapshot:
       print(f"Snapshot details:")
       print(f"  Name: {snapshot.name}")
       print(f"  Level: {snapshot.level}")
       print(f"  Database: {snapshot.database}")
       print(f"  Created: {snapshot.created_at}")
       print(f"  Size: {snapshot.size_mb} MB")
       print(f"  Status: {snapshot.status}")

   # Get snapshot metadata
   metadata = snapshot_manager.get_metadata("daily_backup_20240115")
   print(f"Metadata: {metadata}")

Snapshot Restoration
--------------------

Full Database Restore
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Restore entire database from snapshot
   restore_result = snapshot_manager.restore(
       snapshot_name="daily_backup_20240115",
       target_database="restored_production_db",
       overwrite=True
   )
   print(f"Restore completed: {restore_result.success}")

   # Restore to existing database (replace data)
   restore_result = snapshot_manager.restore(
       snapshot_name="daily_backup_20240115",
       target_database="production_db",
       overwrite=True,
       backup_existing=True
   )

   # Restore with specific options
   restore_result = snapshot_manager.restore(
       snapshot_name="daily_backup_20240115",
       target_database="test_restore_db",
       overwrite=True,
       create_database=True,
       restore_permissions=True
   )

Table Restore
~~~~~~~~~~~~~

.. code-block:: python

   # Restore specific table
   restore_result = snapshot_manager.restore_table(
       snapshot_name="users_table_backup",
       target_database="restored_db",
       target_table="users",
       overwrite=True
   )

   # Restore table to different name
   restore_result = snapshot_manager.restore_table(
       snapshot_name="users_table_backup",
       target_database="restored_db",
       target_table="users_backup",
       overwrite=True
   )

   # Restore multiple tables
   restore_result = snapshot_manager.restore_tables(
       snapshot_name="multi_table_backup",
       target_database="restored_db",
       overwrite=True
   )

Point-in-Time Recovery
----------------------

PITR Operations
~~~~~~~~~~~~~~~

.. code-block:: python

   # Restore to specific timestamp
   restore_result = snapshot_manager.restore_to_timestamp(
       database="production_db",
       target_timestamp="2024-01-15 14:30:00",
       target_database="pitr_restore_db"
   )

   # Restore with transaction log replay
   restore_result = snapshot_manager.restore_to_timestamp(
       database="production_db",
       target_timestamp="2024-01-15 14:30:00",
       target_database="pitr_restore_db",
       replay_logs=True
   )

   # Get available recovery points
   recovery_points = snapshot_manager.get_recovery_points(
       database="production_db",
       start_date="2024-01-15",
       end_date="2024-01-16"
   )
   
   for point in recovery_points:
       print(f"Recovery point: {point.timestamp}")
       print(f"  Type: {point.type}")
       print(f"  Available: {point.available}")

Snapshot Queries
----------------

Querying Historical Data
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Query data from snapshot
   results = client.query_snapshot(
       snapshot_name="daily_backup_20240115",
       sql="SELECT COUNT(*) FROM users"
   )
   print(f"User count in snapshot: {results.rows[0][0]}")

   # Query specific table from snapshot
   results = client.query_snapshot(
       snapshot_name="users_table_backup",
       sql="SELECT * FROM users WHERE created_at > '2024-01-01'"
   )

   # Compare data between snapshots
   old_count = client.query_snapshot(
       snapshot_name="daily_backup_20240114",
       sql="SELECT COUNT(*) FROM users"
   ).rows[0][0]
   
   new_count = client.query_snapshot(
       snapshot_name="daily_backup_20240115",
       sql="SELECT COUNT(*) FROM users"
   ).rows[0][0]
   
   print(f"User growth: {new_count - old_count}")

Snapshot Cleanup
----------------

Deleting Snapshots
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Delete specific snapshot
   snapshot_manager.delete("old_backup_20240101")
   print("Snapshot deleted")

   # Delete multiple snapshots
   snapshots_to_delete = ["backup1", "backup2", "backup3"]
   for snapshot_name in snapshots_to_delete:
       snapshot_manager.delete(snapshot_name)

   # Delete snapshots older than specified date
   deleted_count = snapshot_manager.delete_older_than(
       cutoff_date="2024-01-01",
       database="production_db"
   )
   print(f"Deleted {deleted_count} old snapshots")

   # Cleanup with retention policy
   cleanup_result = snapshot_manager.cleanup_retention_policy(
       database="production_db",
       keep_daily=7,      # Keep 7 daily snapshots
       keep_weekly=4,     # Keep 4 weekly snapshots
       keep_monthly=12    # Keep 12 monthly snapshots
   )
   print(f"Cleanup completed: {cleanup_result.deleted_count} snapshots deleted")

Async Operations
----------------

Async Snapshot Operations
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient

   async def async_snapshot_operations():
       # Connect asynchronously
       connection_params = get_connection_params()
       async_client = AsyncClient(*connection_params)
       await async_client.connect(*connection_params)

       # Get async snapshot manager
       snapshot_manager = async_client.snapshot

       # Async snapshot creation
       snapshot = await snapshot_manager.create_async(
           name="async_backup",
           level=SnapshotLevel.DATABASE,
           database="production_db"
       )

       # Async restore
       restore_result = await snapshot_manager.restore_async(
           snapshot_name="async_backup",
           target_database="async_restored_db",
           overwrite=True
       )

       # Async snapshot listing
       snapshots = await snapshot_manager.list_async()
       print(f"Found {len(snapshots)} snapshots")

       await async_client.disconnect()

   # Run async operations
   asyncio.run(async_snapshot_operations())

Real-world Examples
-------------------

Automated Backup System
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import schedule
   import time
   from datetime import datetime, timedelta

   class AutomatedBackupSystem:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.snapshot_manager = self.client.snapshot

       def daily_backup(self):
           """Create daily backup of production database"""
           timestamp = datetime.now().strftime("%Y%m%d")
           snapshot_name = f"daily_backup_{timestamp}"
           
           try:
               snapshot = self.snapshot_manager.create(
                   name=snapshot_name,
                   level=SnapshotLevel.DATABASE,
                   database="production_db",
                   description=f"Daily backup for {timestamp}"
               )
               print(f"Daily backup created: {snapshot.name}")
               
               # Cleanup old backups (keep 30 days)
               self.cleanup_old_backups(days=30)
               
           except Exception as e:
               print(f"Daily backup failed: {e}")

       def weekly_backup(self):
           """Create weekly backup with compression"""
           timestamp = datetime.now().strftime("%Y%m%d")
           snapshot_name = f"weekly_backup_{timestamp}"
           
           try:
               snapshot = self.snapshot_manager.create(
                   name=snapshot_name,
                   level=SnapshotLevel.DATABASE,
                   database="production_db",
                   compression=True,
                   description=f"Weekly backup for {timestamp}"
               )
               print(f"Weekly backup created: {snapshot.name}")
               
           except Exception as e:
               print(f"Weekly backup failed: {e}")

       def cleanup_old_backups(self, days=30):
           """Clean up backups older than specified days"""
           cutoff_date = datetime.now() - timedelta(days=days)
           deleted_count = self.snapshot_manager.delete_older_than(
               cutoff_date=cutoff_date,
               database="production_db"
           )
           print(f"Cleaned up {deleted_count} old backups")

       def start_scheduler(self):
           """Start the backup scheduler"""
           # Schedule daily backup at 2 AM
           schedule.every().day.at("02:00").do(self.daily_backup)
           
           # Schedule weekly backup on Sunday at 3 AM
           schedule.every().sunday.at("03:00").do(self.weekly_backup)
           
           print("Backup scheduler started")
           
           while True:
               schedule.run_pending()
               time.sleep(60)  # Check every minute

   # Usage
   backup_system = AutomatedBackupSystem()
   # backup_system.start_scheduler()  # Uncomment to start scheduler

Disaster Recovery System
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   class DisasterRecoverySystem:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.snapshot_manager = self.client.snapshot

       def get_latest_snapshot(self, database):
           """Get the most recent snapshot for a database"""
           snapshots = self.snapshot_manager.list(database=database)
           if not snapshots:
               return None
           
           # Sort by creation time and return latest
           latest = max(snapshots, key=lambda s: s.created_at)
           return latest

       def emergency_restore(self, database, target_database=None):
           """Perform emergency restore from latest snapshot"""
           latest_snapshot = self.get_latest_snapshot(database)
           if not latest_snapshot:
               raise Exception(f"No snapshots found for database {database}")
           
           target_db = target_database or f"{database}_restored"
           
           print(f"Emergency restore from snapshot: {latest_snapshot.name}")
           print(f"Target database: {target_db}")
           
           restore_result = self.snapshot_manager.restore(
               snapshot_name=latest_snapshot.name,
               target_database=target_db,
               overwrite=True,
               create_database=True
           )
           
           if restore_result.success:
               print(f"Emergency restore completed successfully")
               return target_db
           else:
               raise Exception(f"Emergency restore failed: {restore_result.error}")

       def point_in_time_recovery(self, database, target_timestamp, target_database=None):
           """Perform point-in-time recovery"""
           target_db = target_database or f"{database}_pitr_restored"
           
           print(f"Point-in-time recovery to: {target_timestamp}")
           print(f"Target database: {target_db}")
           
           restore_result = self.snapshot_manager.restore_to_timestamp(
               database=database,
               target_timestamp=target_timestamp,
               target_database=target_db,
               replay_logs=True
           )
           
           if restore_result.success:
               print(f"Point-in-time recovery completed successfully")
               return target_db
           else:
               raise Exception(f"Point-in-time recovery failed: {restore_result.error}")

       def validate_restore(self, database):
           """Validate that restored database is working correctly"""
           try:
               # Test basic connectivity
               result = self.client.execute(f"SELECT 1 FROM {database}.information_schema.tables LIMIT 1")
               
               # Test data integrity
               table_count = self.client.execute(f"SELECT COUNT(*) FROM {database}.information_schema.tables")
               print(f"Restored database has {table_count.rows[0][0]} tables")
               
               return True
           except Exception as e:
               print(f"Restore validation failed: {e}")
               return False

   # Usage
   dr_system = DisasterRecoverySystem()
   
   # Emergency restore
   restored_db = dr_system.emergency_restore("production_db")
   
   # Validate restore
   if dr_system.validate_restore(restored_db):
       print("Restore validation successful")

Data Migration System
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   class DataMigrationSystem:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.snapshot_manager = self.client.snapshot

       def migrate_database(self, source_db, target_db, tables=None):
           """Migrate database using snapshots"""
           # Create snapshot of source database
           snapshot_name = f"migration_{source_db}_{int(time.time())}"
           
           snapshot = self.snapshot_manager.create(
               name=snapshot_name,
               level=SnapshotLevel.DATABASE,
               database=source_db,
               description=f"Migration snapshot from {source_db}"
           )
           
           # Restore to target database
           restore_result = self.snapshot_manager.restore(
               snapshot_name=snapshot_name,
               target_database=target_db,
               overwrite=True,
               create_database=True
           )
           
           if restore_result.success:
               print(f"Database migration completed: {source_db} -> {target_db}")
               
               # Cleanup migration snapshot
               self.snapshot_manager.delete(snapshot_name)
               
               return True
           else:
               print(f"Database migration failed: {restore_result.error}")
               return False

       def migrate_tables(self, source_db, target_db, tables):
           """Migrate specific tables"""
           for table in tables:
               snapshot_name = f"table_migration_{table}_{int(time.time())}"
               
               # Create table snapshot
               snapshot = self.snapshot_manager.create(
                   name=snapshot_name,
                   level=SnapshotLevel.TABLE,
                   database=source_db,
                   table=table
               )
               
               # Restore table
               restore_result = self.snapshot_manager.restore_table(
                   snapshot_name=snapshot_name,
                   target_database=target_db,
                   target_table=table,
                   overwrite=True
               )
               
               if restore_result.success:
                   print(f"Table migration completed: {table}")
               else:
                   print(f"Table migration failed: {table}")
                   
               # Cleanup
               self.snapshot_manager.delete(snapshot_name)

   # Usage
   migration_system = DataMigrationSystem()
   
   # Migrate entire database
   migration_system.migrate_database("old_production", "new_production")
   
   # Migrate specific tables
   migration_system.migrate_tables("old_production", "new_production", 
                                   ["users", "orders", "products"])

Error Handling
--------------

Robust error handling for production applications:

.. code-block:: python

   from matrixone.exceptions import SnapshotError, RestoreError

   try:
       # Snapshot operations
       snapshot = snapshot_manager.create(
           name="test_backup",
           level=SnapshotLevel.DATABASE,
           database="test_db"
       )
   except SnapshotError as e:
       print(f"Snapshot error: {e}")
   except RestoreError as e:
       print(f"Restore error: {e}")
   except Exception as e:
       print(f"Unexpected error: {e}")

   # Retry mechanism for snapshot operations
   def create_snapshot_with_retry(snapshot_manager, name, level, database, max_retries=3):
       for attempt in range(max_retries):
           try:
               return snapshot_manager.create(name=name, level=level, database=database)
           except Exception as e:
               print(f"Snapshot attempt {attempt + 1} failed: {e}")
               if attempt == max_retries - 1:
                   raise
               time.sleep(2 ** attempt)  # Exponential backoff

Performance Optimization
------------------------

Best practices for optimal performance:

.. code-block:: python

   # Batch snapshot operations
   def batch_create_snapshots(snapshot_manager, databases, snapshot_prefix):
       snapshots = []
       for database in databases:
           snapshot_name = f"{snapshot_prefix}_{database}_{int(time.time())}"
           try:
               snapshot = snapshot_manager.create(
                   name=snapshot_name,
                   level=SnapshotLevel.DATABASE,
                   database=database
               )
               snapshots.append(snapshot)
           except Exception as e:
               print(f"Failed to create snapshot for {database}: {e}")
       return snapshots

   # Efficient snapshot cleanup
   def smart_cleanup(snapshot_manager, database, retention_days=30):
       # Get all snapshots for database
       snapshots = snapshot_manager.list(database=database)
       
       # Sort by creation date
       snapshots.sort(key=lambda s: s.created_at)
       
       # Keep latest snapshot and apply retention policy
       cutoff_date = datetime.now() - timedelta(days=retention_days)
       snapshots_to_delete = [s for s in snapshots[:-1] if s.created_at < cutoff_date]
       
       for snapshot in snapshots_to_delete:
           snapshot_manager.delete(snapshot.name)
       
       return len(snapshots_to_delete)

Troubleshooting
---------------

Common issues and solutions:

**Snapshot creation failures**
   - Verify database exists and is accessible
   - Check available disk space
   - Ensure proper permissions

**Restore failures**
   - Verify snapshot exists and is valid
   - Check target database permissions
   - Ensure sufficient disk space for restore

**Performance issues**
   - Use incremental snapshots for large databases
   - Schedule snapshots during low-activity periods
   - Consider compression for long-term storage

**Point-in-time recovery issues**
   - Verify transaction logs are available
   - Check recovery point availability
   - Ensure proper timestamp format

For more information, see the :doc:`api/client` and :doc:`best_practices`.
