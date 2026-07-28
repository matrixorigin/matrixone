Data Export Guide
==================

This guide covers data export operations in MatrixOne using the pandas-style interface for exporting query results to files and stages.

Overview
--------

MatrixOne's export functionality provides a **pandas-style interface** with these key features:

* **Pandas-Compatible API**: Methods like ``to_csv()`` and ``to_jsonl()`` match pandas naming
* **Multiple Formats**: CSV, TSV, pipe-delimited, and JSONL formats
* **Stage Integration**: Export directly to external stages using ``stage://`` protocol
* **Query Flexibility**: Support for raw SQL, SQLAlchemy select(), and MatrixOne queries
* **Transaction Support**: Export within atomic transactions
* **Custom Options**: Configurable separators, quotes, and line terminators

Basic CSV Export
-----------------

The simplest way to export data to CSV:

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Basic CSV export (pandas-style)
   client.export.to_csv('/tmp/users.csv', "SELECT * FROM users")
   
   # With custom separator
   client.export.to_csv('/tmp/users.tsv', "SELECT * FROM users", sep='\t')
   
   # Pipe-delimited
   client.export.to_csv('/tmp/users.txt', "SELECT * FROM users", sep='|')
   
   client.disconnect()

Export with SQLAlchemy
-----------------------

Use SQLAlchemy select() statements for type-safe exports:

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import declarative_base
   from sqlalchemy import Column, Integer, String, Decimal, select
   
   Base = declarative_base()
   
   class Product(Base):
       __tablename__ = 'products'
       id = Column(Integer, primary_key=True)
       name = Column(String(100))
       category = Column(String(50))
       price = Column(Decimal(10, 2))
   
   client = Client()
   client.connect(database='test')
   
   # Export with filter (pandas-style)
   stmt = select(Product).where(Product.category == 'Electronics')
   client.export.to_csv('/tmp/electronics.csv', stmt)
   
   # Export with aggregation
   from sqlalchemy import func
   stmt = select(
       Product.category,
       func.count(Product.id).label('count'),
       func.avg(Product.price).label('avg_price')
   ).group_by(Product.category)
   client.export.to_csv('/tmp/category_stats.csv', stmt, sep=',')
   
   client.disconnect()

Export with MatrixOne Query Builder
-------------------------------------

Use MatrixOne's ORM-style query builder:

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Build query then export (pandas-style)
   query = client.query(Product).filter(Product.price > 100)
   client.export.to_csv('/tmp/expensive.csv', query)
   
   # Chain filters
   query = (client.query(Product)
       .filter(Product.category == 'Electronics')
       .filter(Product.price < 500))
   client.export.to_csv('/tmp/affordable_electronics.csv', query, sep='|')
   
   client.disconnect()

JSONL Export
-------------

Export data in JSON Lines format (one JSON object per line):

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import select
   
   client = Client()
   client.connect(database='test')
   
   # JSONL export (pandas-style)
   client.export.to_jsonl('/tmp/products.jsonl', "SELECT * FROM products")
   
   # With SQLAlchemy
   stmt = select(Product).where(Product.stock > 0)
   client.export.to_jsonl('/tmp/in_stock.jsonl', stmt)
   
   client.disconnect()

Export to External Stages
---------------------------

Export directly to S3, local, or other external stages using ``stage://`` protocol:

Export to S3 Stage
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Create S3 stage
   client.stage.create_s3(
       name='backup_stage',
       bucket='my-backups',
       path='exports/',
       aws_key_id='AKIAIOSFODNN7EXAMPLE',
       aws_secret_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
   )
   
   # Export to stage using stage:// protocol (pandas-style)
   client.export.to_csv(
       'stage://backup_stage/daily_export.csv',
       "SELECT * FROM sales WHERE date = CURDATE()"
   )
   
   # Export to stage using convenience method (pandas-style)
   client.export.to_csv_stage(
       'backup_stage',
       'daily_export2.csv',
       "SELECT * FROM sales WHERE date = CURDATE()"
   )
   
   client.disconnect()

Export to Local Stage
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Create local filesystem stage
   client.stage.create_local('local_stage', '/exports/')
   
   # Export to local stage (pandas-style)
   client.export.to_csv(
       'stage://local_stage/backup.csv',
       "SELECT * FROM products"
   )
   
   # Export to local stage using convenience method
   client.export.to_csv_stage(
       'local_stage',
       'backup2.csv',
       "SELECT * FROM products"
   )
   
   # JSONL to stage
   client.export.to_jsonl(
       'stage://local_stage/backup.jsonl',
       "SELECT * FROM products"
   )
   
   # JSONL to stage using convenience method
   client.export.to_jsonl_stage(
       'local_stage',
       'backup2.jsonl',
       "SELECT * FROM products"
   )
   
   client.disconnect()

Transaction-Aware Exports
---------------------------

Export within transactions for data consistency:

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import select, insert
   
   client = Client()
   client.connect(database='test')
   
   # Export within transaction (pandas-style)
   with client.session() as session:
       # Insert new data
       session.execute(
           insert(Product).values(
               name='New Laptop',
               category='Electronics',
               price=1299.99
           )
       )
       
       # Export including new data
       stmt = select(Product).where(Product.category == 'Electronics')
       session.export.to_csv('/tmp/updated_electronics.csv', stmt)
       
       # Both operations commit together or rollback on error
   
   client.disconnect()

CSV Export Options
-------------------

Customize CSV export with pandas-compatible parameters:

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Custom separator (pandas: sep parameter)
   client.export.to_csv('/tmp/data.csv', query, sep=',')
   
   # Tab-separated (TSV)
   client.export.to_csv('/tmp/data.tsv', query, sep='\t')
   
   # Pipe-delimited
   client.export.to_csv('/tmp/data.txt', query, sep='|')
   
   # With quote character (pandas: quotechar parameter)
   client.export.to_csv('/tmp/data.csv', query, quotechar='"')
   
   # Custom line terminator (pandas: lineterminator parameter)
   client.export.to_csv('/tmp/data.csv', query, lineterminator='\r\n')
   
   # Note: header=True is not yet supported by MatrixOne
   # client.export.to_csv('/tmp/data.csv', query, header=True)  # Will warn
   
   client.disconnect()

Async Export Operations
-------------------------

Full async/await support for non-blocking exports:

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   from sqlalchemy import select
   
   async def async_export_example():
       client = AsyncClient()
       await client.connect(database='test')
       
       # Async CSV export (pandas-style)
       await client.export.to_csv('/tmp/async_export.csv', "SELECT * FROM sales")
       
       # Async JSONL export
       await client.export.to_jsonl('/tmp/async_export.jsonl', "SELECT * FROM sales")
       
       # With SQLAlchemy
       stmt = select(Product).where(Product.price > 100)
       await client.export.to_csv('/tmp/expensive.csv', stmt, sep='|')
       
       await client.disconnect()
   
   asyncio.run(async_export_example())

Concurrent Exports
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   
   async def concurrent_exports():
       client = AsyncClient()
       await client.connect(database='test')
       
       # Export multiple queries concurrently (pandas-style)
       await asyncio.gather(
           client.export.to_csv('/tmp/electronics.csv', 
               "SELECT * FROM products WHERE category='Electronics'"),
           client.export.to_csv('/tmp/clothing.csv',
               "SELECT * FROM products WHERE category='Clothing'"),
           client.export.to_csv('/tmp/food.csv',
               "SELECT * FROM products WHERE category='Food'")
       )
       
       await client.disconnect()
   
   asyncio.run(concurrent_exports())

Async Transaction Export
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   from sqlalchemy import select, insert
   
   async def async_transaction_export():
       client = AsyncClient()
       await client.connect(database='test')
       
       # Async transaction with export (pandas-style)
       async with client.session() as session:
           # Insert data
           await session.execute(
               insert(Product).values(
                   name='Tablet',
                   category='Electronics',
                   price=399.99
               )
           )
           
           # Export within transaction
           stmt = select(Product).where(Product.category == 'Electronics')
           await session.export.to_csv('/tmp/electronics.csv', stmt)
           
           # Both operations commit together
       
       await client.disconnect()
   
   asyncio.run(async_transaction_export())

Best Practices
--------------

1. **Use Pandas-Style API**
   
   The new ``to_csv()`` and ``to_jsonl()`` methods are more intuitive and align with pandas conventions

2. **Use Transactions for Consistency**
   
   Export within ``session()`` context to ensure data consistency

3. **Use Stages for Large Exports**
   
   Export to external stages (``stage://``) for better performance with large datasets

4. **Choose Appropriate Format**
   
   - CSV for structured data and Excel compatibility
   - JSONL for complex nested data or streaming
   - TSV for tab-delimited requirements

5. **Use SQLAlchemy for Type Safety**
   
   Prefer SQLAlchemy ``select()`` statements over raw SQL for better maintainability

6. **Handle Special Characters**
   
   Use ``quotechar`` parameter when data contains separators

Common Use Cases
----------------

Data Backup
~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Create backup stage
   client.stage.create_s3('backup_stage', 'backups', 'daily/', 'key', 'secret')
   
   # Export tables to stage (pandas-style)
   client.export.to_csv('stage://backup_stage/users.csv', "SELECT * FROM users")
   client.export.to_csv('stage://backup_stage/orders.csv', "SELECT * FROM orders")
   client.export.to_csv('stage://backup_stage/products.csv', "SELECT * FROM products")
   
   client.disconnect()

Analytics Export
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import select, func
   
   client = Client()
   client.connect(database='test')
   
   # Export aggregated analytics (pandas-style)
   stmt = select(
       func.date(Sale.created_at).label('date'),
       func.sum(Sale.amount).label('daily_revenue'),
       func.count(Sale.id).label('order_count')
   ).group_by(func.date(Sale.created_at))
   
   client.export.to_csv('/reports/daily_revenue.csv', stmt, sep=',')
   
   client.disconnect()

Data Sharing with Partners
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import select
   
   client = Client()
   client.connect(database='test')
   
   # Export filtered data for partner (pandas-style)
   stmt = select(Sale).where(Sale.partner_id == 'PARTNER123')
   
   # Create partner stage
   client.stage.create_s3('partner_stage', 'partner-data', '', 'key', 'secret')
   
   # Export to partner stage
   client.export.to_csv(
       'stage://partner_stage/partner_sales.csv',
       stmt,
       sep=','
   )
   
   client.disconnect()

MatrixOne Limitations
----------------------

Current limitations to be aware of:

* **No Simultaneous sep and quotechar**: MatrixOne doesn't support using both ``sep`` and ``quotechar`` at the same time. The SDK will use ``sep`` if both are provided.
* **No Header Option**: ``header=True`` is not yet supported by MatrixOne's ``INTO OUTFILE``. The SDK will warn if you use it.
* **Single-Character Separators**: MatrixOne may only support single-character separators. The SDK will warn if you use multi-character ``sep``.

See Also
--------

* :doc:`stage_guide` - External stage management
* :doc:`load_data_guide` - Data loading operations  
* :doc:`quickstart` - Quick start guide
* :doc:`api/export_manager` - ExportManager API reference
