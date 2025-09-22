Examples
========

This section provides comprehensive examples of using the MatrixOne Python SDK.

Basic Operations
----------------

Connection and Query Execution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

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

   # Execute a simple query
   result = client.execute("SELECT 1 as test_value, USER() as user_info")
   print(result.fetchall())

   # Execute with parameters
   result = client.execute(
       "SELECT * FROM users WHERE age > %s",
       (18,)
   )
   for row in result:
       print(f"User: {row[1]}, Age: {row[2]}")

   client.disconnect()

Async Operations
~~~~~~~~~~~~~~~~

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

       # Execute async query
       result = await client.execute("SELECT COUNT(*) FROM users")
       count = await result.fetchone()
       print(f"Total users: {count[0]}")

       await client.disconnect()

   asyncio.run(main())

Transaction Management
----------------------

Basic Transactions
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Using context manager (recommended)
   with client.transaction() as tx:
       tx.execute("INSERT INTO users (name, email) VALUES (%s, %s)", 
                  ("John Doe", "john@example.com"))
       tx.execute("INSERT INTO users (name, email) VALUES (%s, %s)", 
                  ("Jane Smith", "jane@example.com"))
       # Transaction commits automatically on success

   # Manual transaction control
   tx = client.begin_transaction()
   try:
       tx.execute("UPDATE users SET last_login = NOW() WHERE id = %s", (user_id,))
       tx.execute("INSERT INTO login_log (user_id, login_time) VALUES (%s, NOW())", 
                  (user_id,))
       tx.commit()
   except Exception as e:
       tx.rollback()
       raise e

Snapshot Management
-------------------

Creating and Managing Snapshots
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Create a snapshot
   snapshot = client.snapshots.create(
       name='backup_before_migration',
       level='cluster',
       description='Full cluster backup before data migration'
   )
   print(f"Created snapshot: {snapshot.name}")

   # List all snapshots
   snapshots = client.snapshots.list()
   for snap in snapshots:
       print(f"Snapshot: {snap.name}, Created: {snap.created_at}")

   # Get snapshot details
   details = client.snapshots.get('backup_before_migration')
   print(f"Snapshot size: {details.size}")

   # Clone database from snapshot
   client.snapshots.clone_database(
       target_db='restored_database',
       source_db='original_database',
       snapshot_name='backup_before_migration'
   )

Point-in-Time Recovery (PITR)
------------------------------

Creating PITR
~~~~~~~~~~~~~

.. code-block:: python

   from datetime import datetime, timedelta

   # Create PITR for cluster
   pitr = client.pitr.create_cluster_pitr(
       name='daily_backup',
       range_value=7,
       range_unit='d'
   )

   # Create PITR for specific database
   pitr = client.pitr.create_database_pitr(
       name='user_db_backup',
       database='user_database',
       range_value=24,
       range_unit='h'
   )

   # Restore to specific time
   restore_point = datetime.now() - timedelta(hours=2)
   client.pitr.restore_to_time(restore_point)

Account Management
------------------

User and Role Management
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Create a new user
   user = client.account.create_user(
       username='newuser',
       password='secure_password',
       description='New application user'
   )

   # Create a role
   role = client.account.create_role(
       role_name='data_analyst',
       description='Role for data analysis tasks'
   )

   # Grant privileges
   client.account.grant_privilege(
       user='newuser',
       role='data_analyst',
       privileges=['SELECT', 'INSERT', 'UPDATE']
   )

   # List all users
   users = client.account.list_users()
   for user in users:
       print(f"User: {user.name}, Created: {user.created_at}")

Pub/Sub Operations
------------------

Publication and Subscription
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Create a publication
   publication = client.pubsub.create_publication(
       name='user_changes',
       tables=['users', 'user_profiles'],
       description='Publication for user data changes'
   )

   # Create a subscription
   subscription = client.pubsub.create_subscription(
       name='user_sync',
       publication_name='user_changes',
       target_tables=['users_backup', 'user_profiles_backup']
   )

   # List publications
   publications = client.pubsub.list_publications()
   for pub in publications:
       print(f"Publication: {pub.name}, Tables: {pub.tables}")

Version Management
------------------

Feature Detection and Compatibility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Check if feature is available
   if client.is_feature_available('snapshot_creation'):
       snapshot = client.snapshots.create('test_snapshot', 'cluster')
   else:
       hint = client.get_version_hint('snapshot_creation')
       print(f"Feature not available: {hint}")

   # Check version compatibility
   if client.check_version_compatibility('3.0.0', '>='):
       print("Backend supports 3.0.0+ features")
   else:
       print("Backend version is too old")

   # Get backend version
   version = client.get_backend_version()
   print(f"MatrixOne version: {version}")

   # Check if running development version
   if client.is_development_version():
       print("Running development version - all features available")

Error Handling
--------------

Comprehensive Error Handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone.exceptions import (
       ConnectionError,
       QueryError,
       VersionError,
       SnapshotError,
       AccountError
   )

   try:
       # Attempt to create a snapshot
       snapshot = client.snapshots.create('test_snapshot', 'cluster')
   except VersionError as e:
       print(f"Version compatibility error: {e}")
       print(f"Required version: {e.required_version}")
   except SnapshotError as e:
       print(f"Snapshot operation failed: {e}")
       print(f"Error code: {e.error_code}")
   except ConnectionError as e:
       print(f"Connection failed: {e}")
   except Exception as e:
       print(f"Unexpected error: {e}")

Configuration and Logging
-------------------------

Custom Configuration
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client, MatrixOneLogger

   # Create custom logger
   logger = MatrixOneLogger(
       level=logging.INFO,
       enable_performance_logging=True,
       enable_slow_sql_logging=True,
       slow_sql_threshold=1.0
   )

   # Create client with custom configuration
   client = Client(
       connection_timeout=60,
       query_timeout=600,
       auto_commit=False,
       charset='utf8mb4',
       logger=logger,
       enable_performance_logging=True,
       enable_sql_logging=True
   )

SQLAlchemy Integration
----------------------

Using with SQLAlchemy
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import create_engine, text
   from matrixone import Client

   # Get SQLAlchemy engine from client
   engine = client.get_sqlalchemy_engine()

   # Use with SQLAlchemy
   with engine.connect() as conn:
       result = conn.execute(text("SELECT * FROM users"))
       for row in result:
           print(row)

   # Or use client's SQLAlchemy integration
   with client.sqlalchemy_session() as session:
       result = session.execute(text("SELECT COUNT(*) FROM users"))
       count = result.scalar()
       print(f"Total users: {count}")
