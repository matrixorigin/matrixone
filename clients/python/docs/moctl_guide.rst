MOCTL Command Line Interface Guide
===================================

This guide covers the MOCTL command line interface in the MatrixOne Python SDK, providing programmatic access to MatrixOne control operations.

Overview
--------

MOCTL (MatrixOne Control) provides:

* **Table Flushing**: Force flush table data to disk
* **Incremental Checkpoint**: Generate incremental checkpoints and truncate WAL
* **Global Checkpoint**: Generate global checkpoints across all nodes

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

   # Get MOCTL manager
   moctl = client.moctl

Core Operations
---------------

Table Flushing
~~~~~~~~~~~~~~

Force flush table data to disk:

.. code-block:: python

   # Flush a specific table
   result = moctl.flush_table("production_db", "users")
   print(f"Flush result: {result}")

   # Flush multiple tables
   tables_to_flush = [
       ("production_db", "users"),
       ("production_db", "orders"),
       ("production_db", "products")
   ]
   
   for database, table in tables_to_flush:
       result = moctl.flush_table(database, table)
       print(f"Flushed {database}.{table}: {result}")

   # Flush with error handling
   try:
       result = moctl.flush_table("production_db", "large_table")
       if result.get("result", [{}])[0].get("returnStr") == "OK":
           print("Table flushed successfully")
       else:
           print(f"Flush failed: {result}")
   except Exception as e:
       print(f"Flush error: {e}")

Incremental Checkpoint
~~~~~~~~~~~~~~~~~~~~~~

Generate incremental checkpoint and truncate WAL:

.. code-block:: python

   # Force incremental checkpoint
   result = moctl.increment_checkpoint()
   print(f"Incremental checkpoint result: {result}")

   # Checkpoint with validation
   try:
       result = moctl.increment_checkpoint()
       if result.get("result", [{}])[0].get("returnStr") == "OK":
           print("Incremental checkpoint completed successfully")
       else:
           print(f"Checkpoint failed: {result}")
   except Exception as e:
       print(f"Checkpoint error: {e}")

   # Regular checkpoint scheduling
   import time
   from datetime import datetime

   def schedule_checkpoints(interval_minutes=30):
       """Schedule regular incremental checkpoints"""
       while True:
           try:
               result = moctl.increment_checkpoint()
               print(f"{datetime.now()}: Checkpoint completed")
           except Exception as e:
               print(f"{datetime.now()}: Checkpoint failed: {e}")
           
           time.sleep(interval_minutes * 60)

Global Checkpoint
~~~~~~~~~~~~~~~~~

Generate global checkpoint across all nodes:

.. code-block:: python

   # Force global checkpoint
   result = moctl.global_checkpoint()
   print(f"Global checkpoint result: {result}")

   # Global checkpoint with validation
   try:
       result = moctl.global_checkpoint()
       if result.get("result", [{}])[0].get("returnStr") == "OK":
           print("Global checkpoint completed successfully")
       else:
           print(f"Global checkpoint failed: {result}")
   except Exception as e:
       print(f"Global checkpoint error: {e}")

   # Coordinated checkpoint strategy
   def coordinated_checkpoint():
       """Perform coordinated checkpoint across cluster"""
       print("Starting coordinated checkpoint...")
       
       # First, flush critical tables
       critical_tables = [
           ("production_db", "users"),
           ("production_db", "orders"),
           ("production_db", "transactions")
       ]
       
       for database, table in critical_tables:
           try:
               moctl.flush_table(database, table)
               print(f"Flushed {database}.{table}")
           except Exception as e:
               print(f"Failed to flush {database}.{table}: {e}")
       
       # Then perform global checkpoint
       try:
           result = moctl.global_checkpoint()
           print("Coordinated checkpoint completed")
           return result
       except Exception as e:
           print(f"Coordinated checkpoint failed: {e}")
           return None

Async Operations
----------------

Async MOCTL Operations
~~~~~~~~~~~~~~~~~~~~~~

Full async/await support for high-performance applications:

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient

   async def async_moctl_operations():
       # Connect asynchronously
       connection_params = get_connection_params()
       async_client = AsyncClient(*connection_params)
       await async_client.connect(*connection_params)

       # Get async MOCTL manager
       moctl = async_client.moctl

       # Async table flush
       result = await moctl.flush_table("production_db", "users")
       print(f"Async flush result: {result}")

       # Async incremental checkpoint
       result = await moctl.increment_checkpoint()
       print(f"Async incremental checkpoint result: {result}")

       # Async global checkpoint
       result = await moctl.global_checkpoint()
       print(f"Async global checkpoint result: {result}")

       await async_client.disconnect()

   # Run async operations
   asyncio.run(async_moctl_operations())

Real-world Examples
-------------------

Database Maintenance System
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import time
   from datetime import datetime, timedelta

   class DatabaseMaintenanceSystem:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.moctl = self.client.moctl

       def perform_maintenance(self):
           """Perform routine database maintenance"""
           print("Starting database maintenance...")
           
           # 1. Flush critical tables
           self.flush_critical_tables()
           
           # 2. Perform incremental checkpoint
           self.perform_incremental_checkpoint()
           
           # 3. Perform global checkpoint
           self.perform_global_checkpoint()
           
           print("Database maintenance completed")

       def flush_critical_tables(self):
           """Flush critical tables to disk"""
           critical_tables = [
               ("production_db", "users"),
               ("production_db", "orders"),
               ("production_db", "products"),
               ("analytics_db", "metrics"),
               ("analytics_db", "events")
           ]
           
           for database, table in critical_tables:
               try:
                   result = self.moctl.flush_table(database, table)
                   print(f"Flushed {database}.{table}")
               except Exception as e:
                   print(f"Failed to flush {database}.{table}: {e}")

       def perform_incremental_checkpoint(self):
           """Perform incremental checkpoint"""
           try:
               result = self.moctl.increment_checkpoint()
               print("Incremental checkpoint completed")
           except Exception as e:
               print(f"Incremental checkpoint failed: {e}")

       def perform_global_checkpoint(self):
           """Perform global checkpoint"""
           try:
               result = self.moctl.global_checkpoint()
               print("Global checkpoint completed")
           except Exception as e:
               print(f"Global checkpoint failed: {e}")

       def schedule_maintenance(self, interval_hours=6):
           """Schedule regular maintenance"""
           while True:
               self.perform_maintenance()
               time.sleep(interval_hours * 3600)

   # Usage
   maintenance = DatabaseMaintenanceSystem()
   maintenance.perform_maintenance()

Backup Preparation System
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   class BackupPreparationSystem:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.moctl = self.client.moctl

       def prepare_for_backup(self):
           """Prepare database for backup by ensuring data consistency"""
           print("Preparing database for backup...")
           
           # 1. Flush all tables to ensure data is on disk
           self.flush_all_tables()
           
           # 2. Perform incremental checkpoint to truncate WAL
           self.checkpoint_before_backup()
           
           print("Database prepared for backup")

       def flush_all_tables(self):
           """Flush all tables in the database"""
           # Get list of all tables (this would be database-specific)
           all_tables = [
               ("production_db", "users"),
               ("production_db", "orders"),
               ("production_db", "products"),
               ("production_db", "categories"),
               ("production_db", "transactions")
           ]
           
           for database, table in all_tables:
               try:
                   self.moctl.flush_table(database, table)
                   print(f"Flushed {database}.{table}")
               except Exception as e:
                   print(f"Failed to flush {database}.{table}: {e}")

       def checkpoint_before_backup(self):
           """Perform checkpoint before backup"""
           try:
               # First incremental checkpoint
               self.moctl.increment_checkpoint()
               print("Incremental checkpoint completed")
               
               # Then global checkpoint
               self.moctl.global_checkpoint()
               print("Global checkpoint completed")
               
           except Exception as e:
               print(f"Checkpoint failed: {e}")

   # Usage
   backup_prep = BackupPreparationSystem()
   backup_prep.prepare_for_backup()

Performance Monitoring System
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   class PerformanceMonitoringSystem:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.moctl = self.client.moctl

       def monitor_and_optimize(self):
           """Monitor performance and perform optimizations"""
           print("Starting performance monitoring...")
           
           # Monitor table sizes and flush large tables
           self.flush_large_tables()
           
           # Perform regular checkpoints
           self.perform_regular_checkpoints()
           
           print("Performance monitoring completed")

       def flush_large_tables(self):
           """Flush tables that are likely to be large"""
           large_tables = [
               ("production_db", "user_activity_logs"),
               ("production_db", "system_logs"),
               ("analytics_db", "event_logs"),
               ("analytics_db", "metrics_data")
           ]
           
           for database, table in large_tables:
               try:
                   self.moctl.flush_table(database, table)
                   print(f"Flushed large table {database}.{table}")
               except Exception as e:
                   print(f"Failed to flush {database}.{table}: {e}")

       def perform_regular_checkpoints(self):
           """Perform regular checkpoints for performance"""
           try:
               # Incremental checkpoint for WAL truncation
               self.moctl.increment_checkpoint()
               print("Regular incremental checkpoint completed")
               
               # Global checkpoint for consistency
               self.moctl.global_checkpoint()
               print("Regular global checkpoint completed")
               
           except Exception as e:
               print(f"Regular checkpoint failed: {e}")

       def start_monitoring(self, interval_minutes=15):
           """Start continuous performance monitoring"""
           while True:
               self.monitor_and_optimize()
               time.sleep(interval_minutes * 60)

   # Usage
   monitor = PerformanceMonitoringSystem()
   monitor.monitor_and_optimize()

Error Handling
--------------

Robust error handling for production applications:

.. code-block:: python

   from matrixone.exceptions import MoCtlError

   try:
       # MOCTL operations
       result = moctl.flush_table("production_db", "users")
   except MoCtlError as e:
       print(f"MOCTL error: {e}")
   except Exception as e:
       print(f"Unexpected error: {e}")

   # Retry mechanism for MOCTL operations
   def moctl_operation_with_retry(moctl, operation, max_retries=3):
       for attempt in range(max_retries):
           try:
               return operation()
           except Exception as e:
               print(f"Operation attempt {attempt + 1} failed: {e}")
               if attempt == max_retries - 1:
                   raise
               time.sleep(2 ** attempt)  # Exponential backoff

   # Safe table flushing with retry
   def safe_flush_table(moctl, database, table, max_retries=3):
       def flush_operation():
           return moctl.flush_table(database, table)
       
       return moctl_operation_with_retry(moctl, flush_operation, max_retries)

Performance Optimization
------------------------

Best practices for optimal performance:

.. code-block:: python

   # Batch table flushing
   def batch_flush_tables(moctl, tables):
       results = []
       for database, table in tables:
           try:
               result = moctl.flush_table(database, table)
               results.append((database, table, result))
           except Exception as e:
               results.append((database, table, f"Error: {e}"))
       return results

   # Efficient checkpoint scheduling
   def efficient_checkpoint_schedule(moctl):
       # Flush critical tables first
       critical_tables = [
           ("production_db", "users"),
           ("production_db", "orders")
       ]
       
       for database, table in critical_tables:
           moctl.flush_table(database, table)
       
       # Then perform checkpoints
       moctl.increment_checkpoint()
       moctl.global_checkpoint()

   # Connection pooling for high-throughput applications
   class MoCtlService:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.moctl = self.client.moctl
           self.lock = threading.Lock()

       def thread_safe_flush(self, database, table):
           with self.lock:
               return self.moctl.flush_table(database, table)

Troubleshooting
---------------

Common issues and solutions:

**Table flush failures**
   - Verify table exists and is accessible
   - Check database permissions
   - Ensure table is not locked by other operations

**Checkpoint failures**
   - Verify cluster is in healthy state
   - Check for ongoing transactions
   - Ensure sufficient disk space

**Global checkpoint issues**
   - Verify all nodes are accessible
   - Check network connectivity
   - Ensure cluster consistency

**Performance issues**
   - Use incremental checkpoints for frequent operations
   - Flush tables during low-activity periods
   - Monitor WAL size and truncate regularly

For more information, see the :doc:`api/client` and :doc:`best_practices`.