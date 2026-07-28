Data Loading Guide
==================

This guide covers comprehensive data loading operations in MatrixOne using the **pandas-style interface** for CSV, TSV, JSON Lines, and Parquet files.

Overview
--------

The MatrixOne SDK provides a pandas-compatible interface for data loading:

* **Pandas-style API**: Methods like ``read_csv()``, ``read_json()``, ``read_parquet()`` match pandas naming
* **Multiple formats**: CSV, TSV, JSON Lines, Parquet
* **ORM model support**: Type-safe loading with SQLAlchemy models
* **Stage integration**: Load from S3, local filesystem, and cloud storage
* **Transaction support**: Atomic multi-file loading
* **Parallel loading**: High-performance parallel data loading
* **Compression support**: gzip, bzip2, LZ4, and more

Basic CSV Loading
------------------

The simplest way to load CSV data (pandas-style):

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import declarative_base, Column, Integer, String
   
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
   
   # Basic CSV load (pandas-style)
   client.load_data.read_csv('users.csv', table=User)
   
   # CSV with header (pandas-style)
   client.load_data.read_csv('users.csv', table=User, skiprows=1)
   
   # Custom separator (pandas-style)
   client.load_data.read_csv('users.txt', table=User, sep='|')
   
   # Tab-separated (TSV) (pandas-style)
   client.load_data.read_csv('users.tsv', table=User, sep='\t')
   
   client.disconnect()

CSV with Advanced Options
--------------------------

Use pandas-compatible parameters for advanced CSV loading:

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # All pandas-style parameters
   client.load_data.read_csv(
       'data.csv',
       table='users',
       sep=',',              # pandas: sep
       quotechar='"',        # pandas: quotechar
       skiprows=1,           # pandas: skiprows
       names=['id', 'name'], # pandas: names
       encoding='utf-8',     # pandas: encoding
       compression='gzip',   # pandas: compression
       parallel=True         # MatrixOne: parallel loading
   )
   
   client.disconnect()

JSON Lines Loading
-------------------

Load JSON Lines (JSONL) files using pandas-compatible ``read_json()``:

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # JSON Lines with objects (pandas-style)
   client.load_data.read_json(
       'events.jsonl',
       table='events',
       lines=True,          # pandas: lines
       orient='records'     # pandas: orient
   )
   
   # JSON Lines with arrays (pandas-style)
   client.load_data.read_json(
       'data.jsonl',
       table='users',
       lines=True,
       orient='values'      # Array format
   )
   
   # Compressed JSON Lines
   client.load_data.read_json(
       'events.jsonl.gz',
       table='events',
       lines=True,
       compression='gzip'
   )
   
   client.disconnect()

Parquet Loading
----------------

Load Parquet files using pandas-compatible ``read_parquet()``:

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Basic Parquet load (pandas-style)
   client.load_data.read_parquet('data.parquet', table='users')
   
   # With ORM model
   client.load_data.read_parquet('data.parquet', table=User)
   
   client.disconnect()

**Parquet Support Notes:**

- ✅ Fully supports: SNAPPY, GZIP, LZ4, ZSTD, Brotli compression
- ✅ Fully supports: Parquet 1.0 and 2.0, statistics, nullable columns
- ⚠️ **Must disable dictionary encoding**: ``use_dictionary=False``
- ⚠️ **VARCHAR only**: Use ``VARCHAR`` in table schema, not ``TEXT``
- ⚠️ **UTC timestamps**: Use ``pa.timestamp('ms', tz='UTC')``

Loading from External Stages
------------------------------

Load data from external stages (S3, local filesystem):

Using stage:// Protocol
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Create S3 stage
   client.stage.create_s3(
       name='s3_stage',
       bucket='my-bucket',
       path='data/',
       aws_key_id='key',
       aws_secret_key='secret'
   )
   
   # Load using stage:// protocol (pandas-style)
   client.load_data.read_csv('stage://s3_stage/users.csv', table='users')
   client.load_data.read_json('stage://s3_stage/events.jsonl', table='events')
   client.load_data.read_parquet('stage://s3_stage/data.parquet', table='users')
   
   client.disconnect()

Using Convenience Methods
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Create stage
   client.stage.create_local('local_stage', '/data/')
   
   # Load using convenience methods (pandas-style)
   client.load_data.read_csv_stage('local_stage', 'users.csv', table='users')
   client.load_data.read_json_stage('local_stage', 'events.jsonl', table='events')
   client.load_data.read_parquet_stage('local_stage', 'data.parquet', table='users')
   
   # With options
   client.load_data.read_csv_stage(
       'local_stage',
       'data.csv',
       table='users',
       sep='\t',
       skiprows=1
   )
   
   client.disconnect()

Transaction-Based Loading
--------------------------

Load multiple files atomically within a transaction:

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import select, insert
   
   client = Client()
   client.connect(database='test')
   
   # Atomic multi-file loading (pandas-style)
   with client.session() as session:
       # Load multiple files atomically
       session.load_data.read_csv('users.csv', table=User)
       session.load_data.read_csv('orders.csv', table=Order)
       session.load_data.read_json('events.jsonl', table=Event, lines=True)
       
       # Mix with other operations
       session.execute(insert(User).values(name='Admin', email='admin@example.com'))
       
       # All operations commit together or rollback on error
   
   client.disconnect()

Inline Data Loading
--------------------

Load data from strings without creating files:

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # CSV inline (pandas-style)
   csv_data = "1,Alice,alice@example.com\\n2,Bob,bob@example.com\\n"
   client.load_data.read_csv(csv_data, table='users', inline=True)
   
   # Or use explicit inline method
   client.load_data.read_csv_inline(csv_data, table='users')
   
   # JSON Lines inline (pandas-style)
   json_data = '{"id":1,"name":"Alice"}\\n{"id":2,"name":"Bob"}\\n'
   client.load_data.read_json(json_data, table='users', inline=True)
   
   # Or use explicit inline method
   client.load_data.read_json_inline(json_data, table='users', orient='records')
   
   client.disconnect()

Async Data Loading
-------------------

All loading methods have async versions for non-blocking operations:

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   
   async def async_load_example():
       client = AsyncClient()
       await client.connect(database='test')
       
       # Async CSV load (pandas-style)
       await client.load_data.read_csv('users.csv', table='users', skiprows=1)
       
       # Async JSON load (pandas-style)
       await client.load_data.read_json('events.jsonl', table='events', lines=True)
       
       # Async Parquet load (pandas-style)
       await client.load_data.read_parquet('data.parquet', table='users')
       
       # From stage
       await client.load_data.read_csv_stage('s3_stage', 'data.csv', table='users')
       
       await client.disconnect()
   
   asyncio.run(async_load_example())

Concurrent Loading
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   
   async def concurrent_load():
       client = AsyncClient()
       await client.connect(database='test')
       
       # Load multiple files concurrently (pandas-style)
       await asyncio.gather(
           client.load_data.read_csv('users.csv', table='users'),
           client.load_data.read_csv('orders.csv', table='orders'),
           client.load_data.read_csv('products.csv', table='products')
       )
       
       await client.disconnect()
   
   asyncio.run(concurrent_load())

Parameter Reference
--------------------

Pandas-Compatible Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All parameters match pandas naming conventions:

.. list-table::
   :header-rows: 1
   :widths: 20 20 60

   * - Parameter
     - Pandas Equivalent
     - Description
   * - ``filepath_or_buffer``
     - ``filepath_or_buffer``
     - File path, stage path, or inline data
   * - ``table``
     - N/A
     - Table name (str) or SQLAlchemy model
   * - ``sep``
     - ``sep``
     - Field separator (default: ',')
   * - ``quotechar``
     - ``quotechar``
     - Quote character (e.g., '"')
   * - ``skiprows``
     - ``skiprows``
     - Number of rows to skip (default: 0)
   * - ``names``
     - ``names``
     - Column names to load
   * - ``encoding``
     - ``encoding``
     - Character encoding (e.g., 'utf-8')
   * - ``compression``
     - ``compression``
     - Compression format ('gzip', 'bzip2', etc.)
   * - ``lines``
     - ``lines``
     - Read JSON as lines (JSONL format)
   * - ``orient``
     - ``orient``
     - JSON structure ('records' or 'values')

Best Practices
--------------

1. **Use Pandas-Style API**
   
   The new ``read_csv()``, ``read_json()``, ``read_parquet()`` methods are more intuitive

2. **Use ORM Models**
   
   Pass SQLAlchemy models for type safety: ``read_csv('data.csv', table=User)``

3. **Use Transactions for Atomicity**
   
   Load multiple files atomically with ``session()``

4. **Use Stages for External Data**
   
   Load from S3 or cloud storage using ``read_csv_stage()`` or ``stage://`` protocol

5. **Use Parallel Loading for Large Files**
   
   Enable ``parallel=True`` for files >100MB

6. **Handle Headers with skiprows**
   
   Use ``skiprows=1`` to skip header rows (pandas convention)

Common Use Cases
----------------

Data Migration
~~~~~~~~~~~~~~

.. code-block:: python

   with client.session() as session:
       # Migrate multiple tables atomically (pandas-style)
       session.load_data.read_csv('users.csv', table=User, skiprows=1)
       session.load_data.read_csv('orders.csv', table=Order, skiprows=1)
       session.load_data.read_csv('products.csv', table=Product, skiprows=1)

ETL Pipeline
~~~~~~~~~~~~

.. code-block:: python

   # Extract from various sources, load into MatrixOne (pandas-style)
   client.load_data.read_csv('crm_export.csv', table='customers', sep='|')
   client.load_data.read_json('events.jsonl', table='events', lines=True)
   client.load_data.read_parquet('analytics.parquet', table='metrics')

Log File Import
~~~~~~~~~~~~~~~

.. code-block:: python

   # Load log files with custom parsing (pandas-style)
   client.load_data.read_csv(
       'application.log',
       table='logs',
       sep='\t',
       names=['timestamp', 'level', 'message', 'source'],
       skiprows=0
   )

See Also
--------

* :doc:`stage_guide` - External stage management
* :doc:`export_guide` - Data export operations
* :doc:`quickstart` - Quick start guide
* :doc:`api/load_data_manager` - LoadDataManager API reference
