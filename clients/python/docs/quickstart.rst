Quick Start
===========

This guide will help you get started with the MatrixOne Python SDK quickly.

Basic Usage
-----------

.. code-block:: python

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

Async Usage
-----------

.. code-block:: python

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
       print(await result.fetchall())
       
       await client.disconnect()

   asyncio.run(main())

Snapshot Management
-------------------

.. code-block:: python

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
   client.snapshots.clone_database(
       target_db='new_database',
       source_db='old_database',
       snapshot_name='my_snapshot'
   )

Version Management
------------------

.. code-block:: python

   # Check if feature is available
   if client.is_feature_available('snapshot_creation'):
       snapshot = client.snapshots.create('my_snapshot', 'cluster')
   else:
       hint = client.get_version_hint('snapshot_creation')
       print(f"Feature not available: {hint}")

   # Check version compatibility
   if client.check_version_compatibility('3.0.0', '>='):
       print("Backend supports 3.0.0+ features")

Configuration
-------------

.. code-block:: python

   client = Client(
       connection_timeout=30,
       query_timeout=300,
       auto_commit=True,
       charset='utf8mb4',
       enable_performance_logging=True,
       enable_sql_logging=True
   )

Error Handling
--------------

.. code-block:: python

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

Next Steps
----------

* Read the :doc:`api/index` for detailed API documentation
* Check out the :doc:`examples` for more usage examples
* Learn about :doc:`contributing` to contribute to the project
