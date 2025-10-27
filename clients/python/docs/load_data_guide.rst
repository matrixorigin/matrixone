Data Loading Guide
==================

This guide covers comprehensive data loading operations in MatrixOne, including CSV, TSV, JSON Lines, Parquet, and stage-based loading with ORM models.

Overview
--------

The MatrixOne SDK provides flexible data loading capabilities:

* **Multiple formats**: CSV, TSV, JSON Lines, Parquet
* **ORM model support**: Type-safe loading with SQLAlchemy models
* **Stage integration**: Load from S3, local filesystem, and cloud storage
* **Transaction support**: Atomic multi-file loading with ``session()``
* **Parallel loading**: High-performance parallel data loading
* **Compression support**: gzip, bzip2 compression

Loading Data with ORM Models (Recommended)
-------------------------------------------

Use ORM models for type-safe, maintainable data loading:

CSV Loading with ORM
~~~~~~~~~~~~~~~~~~~~~

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
   
   # Create table from model
   client.create_table(User)
   
   # Load CSV using ORM model (recommended)
   client.load_data.from_csv('/path/to/users.csv', User)
   
   # Verify data loaded
   stmt = select(User).where(User.age > 25)
   result = client.execute(stmt)
   for user in result.scalars():
       print(f"User: {user.name}, Age: {user.age}")
   
   client.disconnect()

CSV with Header and Custom Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Load CSV with header row (skip first line)
   client.load_data.from_csv(
       '/path/to/users.csv',
       User,
       ignore_lines=1  # Skip header
   )
   
   # Custom delimiter and quote character
   client.load_data.from_csv(
       '/path/to/data.txt',
       User,
       delimiter='|',
       enclosed_by='"',
       optionally_enclosed=True
   )
   
   # Load compressed file
   client.load_data.from_csv(
       '/path/to/data.csv.gz',
       User,
       compression='gzip'
   )
   
   client.disconnect()

Parallel Loading for Large Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Enable parallel loading for high performance
   client.load_data.from_csv(
       '/path/to/large_data.csv',
       User,
       parallel=True  # Enables parallel loading
   )
   
   client.disconnect()

Different File Formats
----------------------

TSV (Tab-Separated Values)
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Load TSV file using ORM model
   client.load_data.from_tsv('/path/to/data.tsv', User)
   
   client.disconnect()

JSON Lines Format
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Load JSON Lines format using ORM model
   client.load_data.from_jsonlines('/path/to/data.jsonl', User)
   
   client.disconnect()

Column Mapping and Transformation
----------------------------------

Map CSV Columns to Table Columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import select
   
   client = Client()
   client.connect(database='test')
   
   # Load with column mapping and transformation
   client.load_data.from_csv(
       '/path/to/data.csv',
       User,
       columns=['name', 'email', 'age'],  # Map CSV columns
       set_clause={
           'created_at': 'NOW()',
           'status': "'active'"
       }
   )
   
   # Verify transformed data
   stmt = select(User).where(User.status == 'active')
   result = client.execute(stmt)
   for user in result.scalars():
       print(f"User: {user.name}, Created: {user.created_at}")
   
   client.disconnect()

Loading from External Stages
-----------------------------

Load from S3 Stage
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import Base, Column, Integer, String
   
   class User(Base):
       __tablename__ = 'users'
       id = Column(Integer, primary_key=True)
       name = Column(String(100))
       email = Column(String(255))
   
   client = Client()
   client.connect(database='test')
   
   # Create S3 stage
   client.stage.create_s3(
       name='data_stage',
       bucket='my-bucket',
       path='data/',
       aws_key_id='key',
       aws_secret_key='secret'
   )
   
   # Load from S3 stage using ORM model (recommended)
   client.load_data.from_stage_csv('data_stage', 'users.csv', User)
   
   client.disconnect()

Load from Local Stage
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Create local stage
   client.stage.create_local('local_stage', '/data/imports/')
   
   # Load from local stage using ORM model
   client.load_data.from_stage_csv('local_stage', 'users.csv', User)
   
   client.disconnect()

Transactional Data Loading (Recommended)
-----------------------------------------

Use ``session()`` for atomic multi-file loading:

Basic Transaction
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import Base, Column, Integer, String, Float
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
   
   # Atomic multi-file loading
   with client.session() as session:
       # Load multiple files - all succeed or fail together
       session.load_data.from_csv('/data/users.csv', User)
       session.load_data.from_csv('/data/orders.csv', Order)
       
       # Insert additional data in same transaction
       session.execute(insert(User).values(name='Admin', email='admin@example.com'))
       
       # All operations commit together
   
   client.disconnect()

Complex Transaction with Validation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import select, func, insert
   
   client = Client()
   client.connect(database='test')
   
   # Transaction with validation
   with client.session() as session:
       # Load data
       session.load_data.from_csv('/data/users.csv', User)
       session.load_data.from_csv('/data/orders.csv', Order)
       
       # Validate data loaded correctly
       stmt = select(func.count(User.id))
       user_count = session.execute(stmt).scalar()
       
       stmt = select(func.count(Order.id))
       order_count = session.execute(stmt).scalar()
       
       if user_count == 0 or order_count == 0:
           raise Exception("Data validation failed")
       
       print(f"Loaded {user_count} users and {order_count} orders")
       
       # Update statistics
       session.execute("ANALYZE TABLE users")
       session.execute("ANALYZE TABLE orders")
       
       # All operations succeed or fail together
   
   client.disconnect()

Transaction with Stage and Snapshot
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client, SnapshotLevel
   
   client = Client()
   client.connect(database='test')
   
   # Comprehensive atomic operation
   with client.session() as session:
       # Create stage
       session.stage.create_local('import_stage', '/data/daily/')
       
       # Load data from stage
       session.load_data.from_stage_csv('import_stage', 'users.csv', User)
       session.load_data.from_stage_csv('import_stage', 'orders.csv', Order)
       
       # Create snapshot after successful load
       session.snapshots.create(
           name='post_load_snapshot',
           level=SnapshotLevel.DATABASE,
           database='test'
       )
       
       # All operations commit together
   
   client.disconnect()

Async Data Loading
------------------

Full async/await support for non-blocking data loading:

Basic Async Loading
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   from sqlalchemy import select
   
   async def async_load_example():
       client = AsyncClient()
       await client.connect(database='test')
       
       # Async CSV loading using ORM model
       await client.load_data.from_csv('/data/users.csv', User)
       
       # Async query
       stmt = select(User).where(User.age > 25)
       result = await client.execute(stmt)
       users = result.scalars().all()
       
       await client.disconnect()
   
   asyncio.run(async_load_example())

Concurrent Async Loading
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   
   async def concurrent_load():
       client = AsyncClient()
       await client.connect(database='test')
       
       # Load multiple files concurrently
       await asyncio.gather(
           client.load_data.from_csv('/data/users.csv', User),
           client.load_data.from_csv('/data/orders.csv', Order),
           client.load_data.from_csv('/data/products.csv', Product)
       )
       
       await client.disconnect()
   
   asyncio.run(concurrent_load())

Async Transaction
~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   from sqlalchemy import insert, select, func
   
   async def async_transaction():
       client = AsyncClient()
       await client.connect(database='test')
       
       # Async transaction
       async with client.session() as session:
           # Load data atomically
           await session.load_data.from_csv('/data/users.csv', User)
           await session.load_data.from_csv('/data/orders.csv', Order)
           
           # Insert additional data
           await session.execute(insert(User).values(name='Admin', email='admin@example.com'))
           
           # Query within transaction
           stmt = select(func.count(User.id))
           count = (await session.execute(stmt)).scalar()
           print(f"Total users: {count}")
           
           # All operations commit atomically
       
       await client.disconnect()
   
   asyncio.run(async_transaction())

Performance Tips
----------------

1. **Use Parallel Loading**
   
   Enable ``parallel=True`` for large files (>100MB)

2. **Use Compression**
   
   Use gzip or bzip2 to reduce I/O and transfer time

3. **Batch Operations**
   
   Load multiple files in single transaction for atomicity

4. **Use Stages**
   
   Use stages for repeated loads from same source

5. **Monitor Performance**
   
   Track load times and optimize based on data size

6. **Use ORM Models**
   
   ORM models provide type safety and better error messages

Best Practices
--------------

1. **Use Sessions for Atomicity**
   
   Use ``session()`` for multi-file atomic loads

2. **Use ORM Models**
   
   Prefer ORM models over table names for type safety

3. **Validate After Loading**
   
   Verify row counts and data integrity after loads

4. **Handle Errors Gracefully**
   
   Wrap loads in try-except with proper error handling

5. **Use Stages for External Data**
   
   Use stages for S3 and cloud storage sources

6. **Test with Sample Data**
   
   Test load operations with small sample before full load

Common Use Cases
----------------

**ETL Pipeline**

.. code-block:: python

   with client.session() as session:
       # Extract - load from S3
       session.stage.create_s3('source', 'data-lake', 'raw/', 'key', 'secret')
       session.load_data.from_stage_csv('source', 'data.csv', RawData)
       
       # Transform - process data
       session.execute(
           insert(CleanData).from_select(
               ['id', 'value'],
               select(RawData.id, func.upper(RawData.value)).where(RawData.value.isnot(None))
           )
       )
       
       # Load complete - atomic commit

**Daily Data Import**

.. code-block:: python

   with client.session() as session:
       # Load daily files
       session.load_data.from_csv('/data/daily/users.csv', User, ignore_lines=1)
       session.load_data.from_csv('/data/daily/orders.csv', Order, ignore_lines=1)
       
       # Create snapshot after import
       session.snapshots.create(
           name=f'daily_import_{date.today()}',
           level=SnapshotLevel.DATABASE,
           database='production'
       )

**Multi-Source Integration**

.. code-block:: python

   with client.session() as session:
       # Load from multiple sources
       session.load_data.from_csv('/local/users.csv', User)
       session.load_data.from_stage_csv('s3_stage', 'orders.csv', Order)
       session.load_data.from_jsonlines('/local/events.jsonl', Event)
       
       # All loads atomic

Troubleshooting
---------------

**File Format Issues**

* Ensure correct delimiter and quote characters
* Check for BOM (Byte Order Mark) in UTF-8 files
* Verify line endings (LF vs CRLF)

**Performance Issues**

* Use ``parallel=True`` for large files
* Consider compression for network transfers
* Monitor memory usage for very large files

**Data Validation**

* Verify row counts after loading
* Check for null values in required columns
* Validate data types match model definitions

See Also
--------

* :doc:`stage_guide` - Stage management operations
* :doc:`snapshot_restore_guide` - Snapshot and restore
* :doc:`quickstart` - Quick start guide

