Stage Management Guide
======================

This guide covers external stage management in MatrixOne, including creating stages, loading data from stages, and using stages in transactions.

Overview
--------

Stages provide centralized configuration for external data sources, making it easy to load data from:

* **S3 and S3-compatible storage** (AWS S3, MinIO, etc.)
* **Local filesystem** paths
* **Cloud storage** services

Stages enable:

* ✅ **Centralized data source configuration** - define once, use everywhere
* ✅ **Simplified data loading** - load data with simple stage references
* ✅ **Transaction support** - atomic stage operations with ``session()``
* ✅ **Security** - store credentials separately from queries

Creating Stages with Simple Interfaces (Recommended)
-----------------------------------------------------

Use ``create_s3()`` and ``create_local()`` for simplified stage creation:

S3 Stage Creation
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Create S3 stage with simple interface (recommended)
   client.stage.create_s3(
       name='production_s3',
       bucket='my-bucket',
       path='data/',
       aws_key_id='AKIAIOSFODNN7EXAMPLE',
       aws_secret_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
       region='us-east-1',
       comment='Production data bucket'
   )
   
   # Create S3-compatible MinIO stage
   client.stage.create_s3(
       name='minio_stage',
       bucket='my-bucket',
       endpoint='http://localhost:9000',
       aws_key_id='minioadmin',
       aws_secret_key='minioadmin',
       comment='Local MinIO instance'
   )
   
   client.disconnect()

Local Filesystem Stage
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Create local filesystem stage
   client.stage.create_local(
       name='local_imports',
       path='/data/imports/',
       comment='Local data import directory'
   )
   
   client.disconnect()

Stage Management Operations
----------------------------

List, Get, Alter, and Drop Stages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # List all stages
   stages = client.stage.list()
   for stage in stages:
       print(f"Stage: {stage.name}")
       print(f"  URL: {stage.url}")
       print(f"  Enabled: {stage.enabled}")
       print(f"  Created: {stage.created_time}")
   
   # Get specific stage details
   stage = client.stage.get('production_s3')
   print(f"Stage URL: {stage.url}")
   print(f"Comment: {stage.comment}")
   
   # Alter stage (update credentials or URL)
   client.stage.alter(
       'production_s3',
       credentials={
           'AWS_KEY_ID': 'new_key_id',
           'AWS_SECRET_KEY': 'new_secret_key'
       }
   )
   
   # Disable stage temporarily
   client.stage.alter('production_s3', enable=False)
   
   # Re-enable stage
   client.stage.alter('production_s3', enable=True)
   
   # Drop stage when no longer needed
   client.stage.drop('old_stage')
   
   client.disconnect()

Loading Data from Stages with ORM Models (Recommended)
-------------------------------------------------------

Use ORM models for type-safe data loading from stages:

Load from S3 Stage
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import Base, Column, Integer, String, Float
   from sqlalchemy import select
   
   # Define ORM model
   Base = declarative_base()
   
   class User(Base):
       __tablename__ = 'users'
       id = Column(Integer, primary_key=True)
       name = Column(String(100))
       email = Column(String(255))
       age = Column(Integer)
   
   client = Client()
   client.connect(database='test')
   
   # Create table
   client.create_table(User)
   
   # Load data from S3 stage using ORM model (recommended)
   client.load_data.read_csv_stage('production_s3', 'users.csv', table=User)
   
   # Verify data loaded
   stmt = select(User)
   result = client.execute(stmt)
   for user in result.scalars():
       print(f"User: {user.name}, Age: {user.age}")
   
   client.disconnect()

Load from Local Stage
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Load from local filesystem stage
   client.load_data.read_csv_stage('local_imports', 'orders.csv', table=Order)
   
   # Load with custom options
   client.load_data.read_csv_stage(
       'local_imports',
       'products.csv',
       Product,
       ignore_lines=1,  # Skip header
       delimiter=','
   )
   
   client.disconnect()

Transactional Stage Operations (Recommended)
---------------------------------------------

Use ``session()`` for atomic stage operations:

Atomic Stage Creation and Data Loading
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import Base, Column, Integer, String
   from sqlalchemy import insert, select
   
   class User(Base):
       __tablename__ = 'users'
       id = Column(Integer, primary_key=True)
       name = Column(String(100))
       email = Column(String(255))
   
   class Order(Base):
       __tablename__ = 'orders'
       id = Column(Integer, primary_key=True)
       user_id = Column(Integer)
       amount = Column(Float)
   
   client = Client()
   client.connect(database='test')
   
   # Atomic multi-stage operations
   with client.session() as session:
       # Create stages
       session.stage.create_local('import_stage', '/data/imports/')
       session.stage.create_s3('export_stage', 'backup-bucket', 'exports/', 'key', 'secret')
       
       # Load data from stages atomically
       session.load_data.read_csv_stage('import_stage', 'users.csv', table=User)
       session.load_data.read_csv_stage('import_stage', 'orders.csv', table=Order)
       
       # Insert additional data in same transaction
       session.execute(insert(User).values(name='Admin', email='admin@example.com'))
       
       # All operations commit together or rollback on error
   
   client.disconnect()

Complex Transaction with Multiple Stages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client, SnapshotLevel
   from sqlalchemy import select, func
   
   client = Client()
   client.connect(database='test')
   
   # Complex atomic operation
   with client.session() as session:
       # Create import stage
       session.stage.create_local('daily_import', '/data/daily/')
       
       # Load data
       session.load_data.read_csv_stage('daily_import', 'users.csv', table=User)
       session.load_data.read_csv_stage('daily_import', 'orders.csv', table=Order)
       
       # Verify data loaded correctly
       stmt = select(func.count(User.id))
       user_count = session.execute(stmt).scalar()
       
       stmt = select(func.count(Order.id))
       order_count = session.execute(stmt).scalar()
       
       print(f"Loaded {user_count} users and {order_count} orders")
       
       # Create snapshot after successful load
       session.snapshots.create(
           name='post_import_snapshot',
           level=SnapshotLevel.DATABASE,
           database='test'
       )
       
       # All operations succeed or fail together
   
   client.disconnect()

Async Stage Operations
-----------------------

Full async/await support for non-blocking stage management:

Async Stage Creation and Loading
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   from sqlalchemy import select
   
   async def async_stage_example():
       client = AsyncClient()
       await client.connect(database='test')
       
       # Create S3 stage asynchronously
       await client.stage.create_s3(
           name='async_s3',
           bucket='my-bucket',
           path='data/',
           aws_key_id='key',
           aws_secret_key='secret'
       )
       
       # Load data asynchronously
       await client.load_data.read_csv_stage('async_s3', 'users.csv', table=User)
       
       # Query loaded data
       stmt = select(User).where(User.age > 25)
       result = await client.execute(stmt)
       users = result.scalars().all()
       
       await client.disconnect()
   
   asyncio.run(async_stage_example())

Concurrent Async Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   
   async def concurrent_stage_ops():
       client = AsyncClient()
       await client.connect(database='test')
       
       # Create multiple stages concurrently
       await asyncio.gather(
           client.stage.create_local('stage1', '/data1/'),
           client.stage.create_local('stage2', '/data2/'),
           client.stage.create_s3('stage3', 'bucket3', 'path/', 'key', 'secret')
       )
       
       # Load from multiple stages concurrently
       await asyncio.gather(
           client.load_data.read_csv_stage('stage1', 'users.csv', table=User),
           client.load_data.read_csv_stage('stage2', 'orders.csv', table=Order),
           client.load_data.read_csv_stage('stage3', 'products.csv', table=Product)
       )
       
       await client.disconnect()
   
   asyncio.run(concurrent_stage_ops())

Async Transaction with Stages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   from sqlalchemy import insert, select
   
   async def async_transaction_example():
       client = AsyncClient()
       await client.connect(database='test')
       
       # Async transaction
       async with client.session() as session:
           # Create stage
           await session.stage.create_local('import_stage', '/data/')
           
           # Load data
           await session.load_data.read_csv_stage('import_stage', 'users.csv', table=User)
           
           # Insert additional data
           await session.execute(insert(User).values(name='Admin', email='admin@example.com'))
           
           # Query within transaction
           stmt = select(User)
           result = await session.execute(stmt)
           users = result.scalars().all()
           
           # All operations commit atomically
       
       await client.disconnect()
   
   asyncio.run(async_transaction_example())

Best Practices
--------------

1. **Use Simple Interfaces**
   
   Prefer ``create_s3()`` and ``create_local()`` over generic ``create()``

2. **Use ORM Models**
   
   Use ORM models for type-safe data loading

3. **Use Sessions for Transactions**
   
   Use ``session()`` for atomic multi-stage operations

4. **Secure Credentials**
   
   Store credentials in environment variables or secrets management

5. **Test Stage Connectivity**
   
   Test stage access before production use

6. **Monitor Load Operations**
   
   Track load performance and error rates

Common Use Cases
----------------

**ETL Pipelines**

.. code-block:: python

   with client.session() as session:
       # Extract from S3
       session.stage.create_s3('source_stage', 'data-lake', 'raw/', 'key', 'secret')
       session.load_data.read_csv_stage('source_stage', 'data.csv', table=RawData)
       
       # Transform
       session.execute(
           insert(ProcessedData).from_select(
               ['id', 'value'],
               select(RawData.id, func.upper(RawData.value))
           )
       )
       
       # Load complete - atomic commit

**Backup and Restore**

.. code-block:: python

   with client.session() as session:
       # Create backup stage
       session.stage.create_s3('backup_stage', 'backups', 'daily/', 'key', 'secret')
       
       # Create snapshot
       session.snapshots.create(name='daily_backup', level=SnapshotLevel.DATABASE, database='prod')
       
       # Both operations atomic

**Multi-Source Data Integration**

.. code-block:: python

   with client.session() as session:
       # Multiple sources
       session.stage.create_s3('source_a', 'bucket-a', 'data/', 'key', 'secret')
       session.stage.create_local('source_b', '/local/data/')
       
       # Load from all sources atomically
       session.load_data.read_csv_stage('source_a', 'users.csv', table=User)
       session.load_data.read_csv_stage('source_b', 'orders.csv', table=Order)

See Also
--------

* :doc:`load_data_guide` - Data loading operations
* :doc:`snapshot_restore_guide` - Snapshot and restore operations
* :doc:`quickstart` - Quick start guide

