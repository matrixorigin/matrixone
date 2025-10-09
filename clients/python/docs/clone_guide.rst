Database and Table Cloning Guide
=================================

This guide covers database and table cloning operations in the MatrixOne Python SDK, enabling efficient data replication, testing environments, and backup strategies.

Overview
--------

MatrixOne's cloning system provides:

* **Database Cloning**: Create complete copies of databases with all tables and data
* **Table Cloning**: Clone individual tables with or without data
* **Schema-Only Cloning**: Clone database structure without data
* **Selective Cloning**: Clone specific tables or schemas
* **Cross-Database Cloning**: Clone between different databases
* **Incremental Cloning**: Clone only changes since last clone
* **Async Operations**: Full async/await support for large-scale operations

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

   # Get clone manager
   clone_manager = client.clone

Database Cloning
----------------

Full Database Cloning
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Clone entire database with all data
   clone_result = clone_manager.clone_database(
       source_database="production_db",
       target_database="test_db",
       include_data=True
   )
   print(f"Database cloned: {clone_result.success}")

   # Clone database with specific options
   clone_result = clone_manager.clone_database(
       source_database="production_db",
       target_database="staging_db",
       include_data=True,
       include_indexes=True,
       include_permissions=True,
       description="Staging environment clone"
   )

   # Clone database to different server
   clone_result = clone_manager.clone_database(
       source_database="production_db",
       target_database="backup_db",
       target_server="backup-server:6001",
       include_data=True
   )

Schema-Only Cloning
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Clone database structure without data
   clone_result = clone_manager.clone_database_schema(
       source_database="production_db",
       target_database="empty_test_db",
       include_indexes=True,
       include_constraints=True
   )

   # Clone schema with specific tables
   clone_result = clone_manager.clone_database_schema(
       source_database="production_db",
       target_database="partial_test_db",
       tables=["users", "orders", "products"],
       include_indexes=True
   )

Incremental Database Cloning
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Clone only changes since last clone
   clone_result = clone_manager.clone_database_incremental(
       source_database="production_db",
       target_database="incremental_backup",
       last_clone_timestamp="2024-01-15 10:00:00"
   )

   # Clone with change tracking
   clone_result = clone_manager.clone_database_incremental(
       source_database="production_db",
       target_database="tracked_backup",
       track_changes=True,
       change_window_hours=24
   )

Table Cloning
-------------

Single Table Cloning
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Clone table with all data
   clone_result = clone_manager.clone_table(
       source_database="production_db",
       source_table="users",
       target_database="test_db",
       target_table="users_copy",
       include_data=True
   )

   # Clone table with specific columns
   clone_result = clone_manager.clone_table(
       source_database="production_db",
       source_table="users",
       target_database="test_db",
       target_table="users_essential",
       columns=["id", "username", "email", "created_at"],
       include_data=True
   )

   # Clone table structure only
   clone_result = clone_manager.clone_table_schema(
       source_database="production_db",
       source_table="users",
       target_database="test_db",
       target_table="users_template",
       include_indexes=True,
       include_constraints=True
   )

Multiple Table Cloning
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Clone multiple tables
   tables_to_clone = ["users", "orders", "products", "categories"]
   clone_result = clone_manager.clone_tables(
       source_database="production_db",
       target_database="test_db",
       tables=tables_to_clone,
       include_data=True
   )

   # Clone tables with filtering
   clone_result = clone_manager.clone_tables(
       source_database="production_db",
       target_database="filtered_test_db",
       tables=["users", "orders"],
       where_conditions={
           "users": "created_at > '2024-01-01'",
           "orders": "status = 'completed'"
       },
       include_data=True
   )

   # Clone related tables (with foreign key dependencies)
   clone_result = clone_manager.clone_related_tables(
       source_database="production_db",
       target_database="related_test_db",
       root_table="users",
       include_data=True,
       max_depth=3  # Include tables up to 3 levels deep
   )

Advanced Cloning Operations
---------------------------

Conditional Cloning
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Clone with WHERE conditions
   clone_result = clone_manager.clone_table(
       source_database="production_db",
       source_table="orders",
       target_database="test_db",
       target_table="recent_orders",
       where_condition="created_at > '2024-01-01' AND status = 'completed'",
       include_data=True
   )

   # Clone with data transformation
   clone_result = clone_manager.clone_table(
       source_database="production_db",
       source_table="users",
       target_database="anonymized_db",
       target_table="anonymized_users",
       data_transformations={
           "email": "CONCAT('user_', id, '@example.com')",
           "phone": "NULL",
           "address": "NULL"
       },
       include_data=True
   )

Cross-Database Cloning
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Clone between different databases on same server
   clone_result = clone_manager.clone_cross_database(
       source_database="production_db",
       source_table="users",
       target_database="analytics_db",
       target_table="user_analytics",
       include_data=True
   )

   # Clone to different server
   clone_result = clone_manager.clone_cross_server(
       source_database="production_db",
       source_table="users",
       target_server="analytics-server:6001",
       target_database="analytics_db",
       target_table="users",
       include_data=True
   )

Async Operations
----------------

Async Database Cloning
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient

   async def async_database_cloning():
       # Connect asynchronously
       connection_params = get_connection_params()
       async_client = AsyncClient(*connection_params)
       await async_client.connect(*connection_params)

       # Get async clone manager
       clone_manager = async_client.clone

       # Async database clone
       clone_result = await clone_manager.clone_database_async(
           source_database="production_db",
           target_database="async_test_db",
           include_data=True
       )

       # Async table clone
       clone_result = await clone_manager.clone_table_async(
           source_database="production_db",
           source_table="users",
           target_database="async_test_db",
           target_table="users_async",
           include_data=True
       )

       await async_client.disconnect()

   # Run async operations
   asyncio.run(async_database_cloning())

Clone Management
----------------

Listing Clones
~~~~~~~~~~~~~~

.. code-block:: python

   # List all clones
   clones = clone_manager.list_clones()
   for clone in clones:
       print(f"Clone: {clone.name}")
       print(f"  Source: {clone.source_database}")
       print(f"  Target: {clone.target_database}")
       print(f"  Created: {clone.created_at}")
       print(f"  Status: {clone.status}")

   # List clones for specific database
   db_clones = clone_manager.list_clones(database="production_db")
   for clone in db_clones:
       print(f"Database clone: {clone.name}")

   # Get clone details
   clone = clone_manager.get_clone("test_db")
   if clone:
       print(f"Clone details: {clone.name}")
       print(f"  Size: {clone.size_mb} MB")
       print(f"  Tables: {clone.table_count}")
       print(f"  Last updated: {clone.last_updated}")

Clone Status and Monitoring
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Check clone status
   status = clone_manager.get_clone_status("test_db")
   print(f"Clone status: {status.state}")
   print(f"Progress: {status.progress}%")
   print(f"Tables cloned: {status.tables_completed}/{status.tables_total}")

   # Monitor clone progress
   def monitor_clone_progress(clone_name):
       while True:
           status = clone_manager.get_clone_status(clone_name)
           print(f"Progress: {status.progress}%")
           
           if status.state in ["completed", "failed"]:
               break
           
           time.sleep(5)  # Check every 5 seconds

   # Get clone statistics
   stats = clone_manager.get_clone_statistics("test_db")
   print(f"Clone statistics:")
   print(f"  Total size: {stats.total_size_mb} MB")
   print(f"  Table count: {stats.table_count}")
   print(f"  Row count: {stats.total_rows}")
   print(f"  Index count: {stats.index_count}")

Clone Cleanup
~~~~~~~~~~~~~

.. code-block:: python

   # Delete specific clone
   delete_result = clone_manager.delete_clone("test_db")
   if delete_result.success:
       print("Clone deleted successfully")

   # Delete multiple clones
   clones_to_delete = ["test_db", "staging_db", "backup_db"]
   for clone_name in clones_to_delete:
       clone_manager.delete_clone(clone_name)

   # Cleanup old clones
   cleanup_result = clone_manager.cleanup_old_clones(
       older_than_days=30,
       database="production_db"
   )
   print(f"Cleaned up {cleanup_result.deleted_count} old clones")

Real-world Examples
-------------------

Development Environment Setup
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   class DevelopmentEnvironmentManager:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.clone_manager = self.client.clone

       def setup_dev_environment(self, developer_name):
           """Set up development environment for a developer"""
           dev_db_name = f"dev_{developer_name}_db"
           
           # Clone production database for development
           clone_result = self.clone_manager.clone_database(
               source_database="production_db",
               target_database=dev_db_name,
               include_data=True,
               description=f"Development environment for {developer_name}"
           )
           
           if clone_result.success:
               print(f"Development environment created: {dev_db_name}")
               
               # Anonymize sensitive data
               self.anonymize_sensitive_data(dev_db_name)
               
               return dev_db_name
           else:
               print(f"Failed to create development environment: {clone_result.error}")
               return None

       def anonymize_sensitive_data(self, database_name):
           """Anonymize sensitive data in development database"""
           # Anonymize user emails
           self.client.execute(f"""
               UPDATE {database_name}.users 
               SET email = CONCAT('dev_user_', id, '@example.com')
           """)
           
           # Anonymize phone numbers
           self.client.execute(f"""
               UPDATE {database_name}.users 
               SET phone = NULL
           """)
           
           # Anonymize addresses
           self.client.execute(f"""
               UPDATE {database_name}.users 
               SET address = 'Anonymized Address'
           """)
           
           print(f"Sensitive data anonymized in {database_name}")

       def cleanup_dev_environment(self, developer_name):
           """Clean up development environment"""
           dev_db_name = f"dev_{developer_name}_db"
           delete_result = self.clone_manager.delete_clone(dev_db_name)
           
           if delete_result.success:
               print(f"Development environment cleaned up: {dev_db_name}")
           else:
               print(f"Failed to clean up development environment: {delete_result.error}")

   # Usage
   dev_manager = DevelopmentEnvironmentManager()
   dev_db = dev_manager.setup_dev_environment("john_doe")
   # dev_manager.cleanup_dev_environment("john_doe")  # When done

Testing Environment Management
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   class TestingEnvironmentManager:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.clone_manager = self.client.clone

       def create_test_environment(self, test_type="integration"):
           """Create test environment based on test type"""
           test_db_name = f"test_{test_type}_{int(time.time())}"
           
           if test_type == "unit":
               # Unit tests - schema only
               clone_result = self.clone_manager.clone_database_schema(
                   source_database="production_db",
                   target_database=test_db_name,
                   include_indexes=True
               )
           elif test_type == "integration":
               # Integration tests - limited data
               clone_result = self.clone_manager.clone_database(
                   source_database="production_db",
                   target_database=test_db_name,
                   include_data=True,
                   data_filters={
                       "users": "id <= 1000",
                       "orders": "created_at > '2024-01-01'"
                   }
               )
           elif test_type == "performance":
               # Performance tests - full data
               clone_result = self.clone_manager.clone_database(
                   source_database="production_db",
                   target_database=test_db_name,
                   include_data=True
               )
           
           if clone_result.success:
               print(f"Test environment created: {test_db_name}")
               return test_db_name
           else:
               print(f"Failed to create test environment: {clone_result.error}")
               return None

       def cleanup_test_environment(self, test_db_name):
           """Clean up test environment"""
           delete_result = self.clone_manager.delete_clone(test_db_name)
           
           if delete_result.success:
               print(f"Test environment cleaned up: {test_db_name}")
           else:
               print(f"Failed to clean up test environment: {delete_result.error}")

       def run_test_suite(self, test_db_name, test_scripts):
           """Run test suite against test environment"""
           print(f"Running tests against {test_db_name}")
           
           for script in test_scripts:
               try:
                   result = self.client.execute(script)
                   print(f"Test passed: {script[:50]}...")
               except Exception as e:
                   print(f"Test failed: {script[:50]}... Error: {e}")

   # Usage
   test_manager = TestingEnvironmentManager()
   test_db = test_manager.create_test_environment("integration")
   
   if test_db:
       # Run tests
       test_scripts = [
           "SELECT COUNT(*) FROM users",
           "SELECT COUNT(*) FROM orders",
           "SELECT * FROM users LIMIT 5"
       ]
       test_manager.run_test_suite(test_db, test_scripts)
       
       # Cleanup
       test_manager.cleanup_test_environment(test_db)

Data Migration System
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   class DataMigrationSystem:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.clone_manager = self.client.clone

       def migrate_database(self, source_db, target_db, migration_plan):
           """Migrate database according to migration plan"""
           print(f"Starting migration: {source_db} -> {target_db}")
           
           # Create target database
           self.client.execute(f"CREATE DATABASE IF NOT EXISTS {target_db}")
           
           # Clone tables according to plan
           for table_plan in migration_plan:
               if table_plan["action"] == "clone":
                   self.clone_table_with_plan(source_db, target_db, table_plan)
               elif table_plan["action"] == "transform":
                   self.transform_table_during_clone(source_db, target_db, table_plan)
               elif table_plan["action"] == "skip":
                   print(f"Skipping table: {table_plan['table']}")
           
           print(f"Migration completed: {source_db} -> {target_db}")

       def clone_table_with_plan(self, source_db, target_db, table_plan):
           """Clone table according to migration plan"""
           table_name = table_plan["table"]
           
           clone_result = self.clone_manager.clone_table(
               source_database=source_db,
               source_table=table_name,
               target_database=target_db,
               target_table=table_plan.get("target_table", table_name),
               columns=table_plan.get("columns"),
               where_condition=table_plan.get("where_condition"),
               include_data=table_plan.get("include_data", True)
           )
           
           if clone_result.success:
               print(f"Cloned table: {table_name}")
           else:
               print(f"Failed to clone table {table_name}: {clone_result.error}")

       def transform_table_during_clone(self, source_db, target_db, table_plan):
           """Transform table during clone process"""
           table_name = table_plan["table"]
           transformations = table_plan.get("transformations", {})
           
           # Clone table structure first
           self.clone_manager.clone_table_schema(
               source_database=source_db,
               source_table=table_name,
               target_database=target_db,
               target_table=table_name
           )
           
           # Apply transformations during data copy
           if transformations:
               self.apply_data_transformations(source_db, target_db, table_name, transformations)

       def apply_data_transformations(self, source_db, target_db, table_name, transformations):
           """Apply data transformations during clone"""
           # This would involve custom SQL to transform data during copy
           print(f"Applying transformations to {table_name}")

   # Usage
   migration_system = DataMigrationSystem()
   
   migration_plan = [
       {
           "action": "clone",
           "table": "users",
           "include_data": True
       },
       {
           "action": "transform",
           "table": "orders",
           "transformations": {
               "status": "CASE WHEN status = 'pending' THEN 'new' ELSE status END"
           }
       },
       {
           "action": "skip",
           "table": "temp_data"
       }
   ]
   
   migration_system.migrate_database("old_production", "new_production", migration_plan)

Backup and Recovery System
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   class BackupRecoverySystem:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.clone_manager = self.client.clone

       def create_backup_clone(self, database_name, backup_type="full"):
           """Create backup clone of database"""
           timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
           backup_name = f"{database_name}_backup_{backup_type}_{timestamp}"
           
           if backup_type == "full":
               clone_result = self.clone_manager.clone_database(
                   source_database=database_name,
                   target_database=backup_name,
                   include_data=True
               )
           elif backup_type == "incremental":
               # Get last backup timestamp
               last_backup = self.get_last_backup_timestamp(database_name)
               clone_result = self.clone_manager.clone_database_incremental(
                   source_database=database_name,
                   target_database=backup_name,
                   last_clone_timestamp=last_backup
               )
           
           if clone_result.success:
               print(f"Backup created: {backup_name}")
               return backup_name
           else:
               print(f"Backup failed: {clone_result.error}")
               return None

       def restore_from_backup(self, backup_name, target_database):
           """Restore database from backup clone"""
           # This would involve dropping target database and renaming backup
           print(f"Restoring {target_database} from backup {backup_name}")
           
           # Drop target database if exists
           self.client.execute(f"DROP DATABASE IF EXISTS {target_database}")
           
           # Rename backup to target
           self.client.execute(f"ALTER DATABASE {backup_name} RENAME TO {target_database}")
           
           print(f"Restore completed: {target_database}")

       def get_last_backup_timestamp(self, database_name):
           """Get timestamp of last backup for incremental backup"""
           # This would query backup metadata
           return "2024-01-15 10:00:00"  # Placeholder

       def cleanup_old_backups(self, database_name, keep_days=30):
           """Clean up old backup clones"""
           cutoff_date = datetime.now() - timedelta(days=keep_days)
           clones = self.clone_manager.list_clones(database=database_name)
           
           deleted_count = 0
           for clone in clones:
               if "backup" in clone.name and clone.created_at < cutoff_date:
                   delete_result = self.clone_manager.delete_clone(clone.name)
                   if delete_result.success:
                       deleted_count += 1
           
           print(f"Cleaned up {deleted_count} old backups")

   # Usage
   backup_system = BackupRecoverySystem()
   
   # Create backup
   backup_name = backup_system.create_backup_clone("production_db", "full")
   
   # Restore from backup
   # backup_system.restore_from_backup(backup_name, "restored_production_db")
   
   # Cleanup old backups
   backup_system.cleanup_old_backups("production_db", keep_days=30)

Error Handling
--------------

Robust error handling for production applications:

.. code-block:: python

   from matrixone.exceptions import CloneError, DatabaseError

   try:
       # Clone operations
       clone_result = clone_manager.clone_database(
           source_database="production_db",
           target_database="test_db",
           include_data=True
       )
   except CloneError as e:
       print(f"Clone error: {e}")
   except DatabaseError as e:
       print(f"Database error: {e}")
   except Exception as e:
       print(f"Unexpected error: {e}")

   # Retry mechanism for clone operations
   def clone_with_retry(clone_manager, source_db, target_db, max_retries=3):
       for attempt in range(max_retries):
           try:
               return clone_manager.clone_database(
                   source_database=source_db,
                   target_database=target_db,
                   include_data=True
               )
           except Exception as e:
               print(f"Clone attempt {attempt + 1} failed: {e}")
               if attempt == max_retries - 1:
                   raise
               time.sleep(2 ** attempt)  # Exponential backoff

Performance Optimization
------------------------

Best practices for optimal performance:

.. code-block:: python

   # Batch table cloning
   def batch_clone_tables(clone_manager, source_db, target_db, tables):
       results = []
       for table in tables:
           try:
               result = clone_manager.clone_table(
                   source_database=source_db,
                   source_table=table,
                   target_database=target_db,
                   include_data=True
               )
               results.append((table, result))
           except Exception as e:
               results.append((table, f"Error: {e}"))
       return results

   # Efficient large database cloning
   def efficient_large_db_clone(clone_manager, source_db, target_db):
       # Clone schema first
       clone_manager.clone_database_schema(
           source_database=source_db,
           target_database=target_db
       )
       
       # Clone tables in parallel (if supported)
       tables = clone_manager.get_database_tables(source_db)
       batch_clone_tables(clone_manager, source_db, target_db, tables)

   # Connection pooling for high-throughput applications
   class CloneService:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.clone_manager = self.client.clone
           self.lock = threading.Lock()

       def thread_safe_clone(self, source_db, target_db):
           with self.lock:
               return self.clone_manager.clone_database(
                   source_database=source_db,
                   target_database=target_db,
                   include_data=True
               )

Troubleshooting
---------------

Common issues and solutions:

**Clone failures**
   - Verify source database exists and is accessible
   - Check target database name conflicts
   - Ensure sufficient disk space

**Performance issues**
   - Use schema-only cloning for large databases
   - Clone tables in batches
   - Consider incremental cloning for frequent updates

**Data consistency issues**
   - Use transactions for multi-table clones
   - Verify foreign key constraints
   - Check data type compatibility

**Permission issues**
   - Verify database access permissions
   - Check table-level permissions
   - Ensure proper user roles

For more information, see the :doc:`api/client` and :doc:`best_practices`.
