Data Export Guide
==================

This guide covers data export operations in MatrixOne, including exporting query results to files, stages, and various formats.

Overview
--------

MatrixOne's export functionality provides:

* **Query Result Export**: Export SELECT query results to external files
* **Multiple Formats**: CSV, JSONLINE, TSV, and custom delimited formats
* **Stage Integration**: Export directly to external stages (S3, local filesystem)
* **ORM-Style Export**: Chainable export methods with query builder
* **Transaction Support**: Export operations within transactions
* **Custom Options**: Configurable delimiters, enclosures, and headers

Transaction-Aware Export Operations (Recommended)
--------------------------------------------------

Use ``client.session()`` for atomic export operations:

Export with Data Transformation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client, ExportFormat
   from matrixone.orm import Base, Column, Integer, String, Decimal
   from sqlalchemy import select, insert, func
   
   # Define ORM model
   Base = declarative_base()
   
   class Sale(Base):
       __tablename__ = 'sales'
       id = Column(Integer, primary_key=True)
       product = Column(String(100))
       category = Column(String(50))
       amount = Column(Decimal(10, 2))
       quantity = Column(Integer)
   
   client = Client()
   client.connect(database='test')
   
   # Atomic data processing and export
   with client.session() as session:
       # Insert data
       session.execute(
           insert(Sale).values([
               {'product': 'Laptop', 'category': 'Electronics', 'amount': 999.99, 'quantity': 5},
               {'product': 'Phone', 'category': 'Electronics', 'amount': 699.99, 'quantity': 10}
           ])
       )
       
       # Export query results to file
       stmt = select(Sale).where(Sale.category == 'Electronics')
       session.export.from_query(
           stmt,
           output_file='/tmp/electronics.csv',
           format=ExportFormat.CSV
       )
       
       # All operations commit together
   
   client.disconnect()

Export with Aggregation
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import select, func
   
   client = Client()
   client.connect(database='test')
   
   # Export aggregated results
   with client.session() as session:
       # Aggregate query
       stmt = select(
           Sale.category,
           func.sum(Sale.amount).label('total_amount'),
           func.count(Sale.id).label('order_count')
       ).group_by(Sale.category)
       
       # Export aggregated results
       session.export.from_query(
           stmt,
           output_file='/tmp/category_summary.csv',
           format=ExportFormat.CSV,
           header=True
       )
   
   client.disconnect()

Export to External Stages (Recommended)
----------------------------------------

Export directly to external stages for backup or data sharing:

Export to S3 Stage
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client, ExportFormat
   from sqlalchemy import select
   
   client = Client()
   client.connect(database='test')
   
   # Atomic stage creation and export
   with client.session() as session:
       # Create S3 stage
       session.stage.create_s3(
           name='export_stage',
           bucket='backups',
           path='daily/',
           aws_key_id='key',
           aws_secret_key='secret'
       )
       
       # Export to stage
       stmt = select(Sale)
       session.export.to_stage(
           stmt,
           stage_name='export_stage',
           filename='sales_backup.csv',
           format=ExportFormat.CSV
       )
       
       # Both operations atomic
   
   client.disconnect()

Export to Local Stage
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import select
   
   client = Client()
   client.connect(database='test')
   
   # Export to local filesystem stage
   with client.session() as session:
       # Create local stage
       session.stage.create_local('local_export', '/exports/')
       
       # Export data
       stmt = select(Sale).where(Sale.amount > 100)
       session.export.to_stage(
           stmt,
           stage_name='local_export',
           filename='high_value_sales.csv',
           format=ExportFormat.CSV,
           header=True
       )
   
   client.disconnect()

ORM-Style Export (Recommended)
-------------------------------

Use query builder with chainable export methods:

Basic ORM Export
~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Chainable query and export
   result = (
       client.query(Sale)
       .filter(Sale.category == 'Electronics')
       .export_to_file('/tmp/electronics.csv', format='csv')
   )
   
   print(f"Exported {result.row_count} rows")
   
   client.disconnect()

Export with Filters and Ordering
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(database='test')
   
   # Complex query with export
   result = (
       client.query(Sale)
       .filter(Sale.amount > 500)
       .filter(Sale.quantity > 5)
       .order_by(Sale.amount.desc())
       .export_to_stage(
           stage_name='export_stage',
           filename='premium_sales.csv'
       )
   )
   
   client.disconnect()

Export Formats and Options
---------------------------

Supported Export Formats
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client, ExportFormat
   from sqlalchemy import select
   
   client = Client()
   client.connect(database='test')
   
   stmt = select(Sale)
   
   # CSV format (default)
   client.export.from_query(
       stmt,
       output_file='/tmp/data.csv',
       format=ExportFormat.CSV,
       delimiter=',',
       header=True
   )
   
   # TSV format
   client.export.from_query(
       stmt,
       output_file='/tmp/data.tsv',
       format=ExportFormat.TSV,
       header=True
   )
   
   # JSONLINE format (one JSON object per line)
   client.export.from_query(
       stmt,
       output_file='/tmp/data.jsonl',
       format=ExportFormat.JSONLINE
   )
   
   # Custom delimiter
   client.export.from_query(
       stmt,
       output_file='/tmp/data.txt',
       format=ExportFormat.CSV,
       delimiter='|',
       enclosed_by='"',
       header=True
   )
   
   client.disconnect()

Export with Custom Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import select
   
   client = Client()
   client.connect(database='test')
   
   stmt = select(Sale)
   
   # Export with all options
   client.export.from_query(
       stmt,
       output_file='/tmp/custom_export.csv',
       format=ExportFormat.CSV,
       delimiter=',',
       enclosed_by='"',
       escaped_by='\\',
       header=True,
       max_file_size='100MB',  # Split into multiple files if needed
       compression='gzip'  # Compress output
   )
   
   client.disconnect()

Async Export Operations
------------------------

Full async/await support for non-blocking exports:

Async Export to File
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   from sqlalchemy import select
   
   async def async_export_example():
       client = AsyncClient()
       await client.connect(database='test')
       
       # Async export
       stmt = select(Sale).where(Sale.category == 'Electronics')
       await client.export.from_query(
           stmt,
           output_file='/tmp/async_export.csv',
           format=ExportFormat.CSV
       )
       
       await client.disconnect()
   
   asyncio.run(async_export_example())

Concurrent Export Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient, ExportFormat
   from sqlalchemy import select
   
   async def concurrent_exports():
       client = AsyncClient()
       await client.connect(database='test')
       
       # Export multiple categories concurrently
       await asyncio.gather(
           client.export.from_query(
               select(Sale).where(Sale.category == 'Electronics'),
               '/tmp/electronics.csv',
               ExportFormat.CSV
           ),
           client.export.from_query(
               select(Sale).where(Sale.category == 'Clothing'),
               '/tmp/clothing.csv',
               ExportFormat.CSV
           ),
           client.export.from_query(
               select(Sale).where(Sale.category == 'Food'),
               '/tmp/food.csv',
               ExportFormat.CSV
           )
       )
       
       await client.disconnect()
   
   asyncio.run(concurrent_exports())

Async Transaction Export
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   from sqlalchemy import select, insert
   
   async def async_transaction_export():
       client = AsyncClient()
       await client.connect(database='test')
       
       # Async transaction with export
       async with client.session() as session:
           # Insert data
           await session.execute(
               insert(Sale).values(
                   product='Tablet',
                   category='Electronics',
                   amount=399.99,
                   quantity=3
               )
           )
           
           # Export within transaction
           stmt = select(Sale)
           await session.export.from_query(
               stmt,
               output_file='/tmp/all_sales.csv',
               format=ExportFormat.CSV
           )
           
           # Both operations commit together
       
       await client.disconnect()
   
   asyncio.run(async_transaction_export())

Best Practices
--------------

1. **Use Transactions for Consistency**
   
   Export within transactions to ensure data consistency

2. **Use Stages for Large Exports**
   
   Export to external stages for better performance with large datasets

3. **Choose Appropriate Format**
   
   - CSV for structured data and Excel compatibility
   - JSONLINE for complex nested data
   - TSV for tab-delimited requirements

4. **Compress Large Exports**
   
   Use compression for large files to save storage and transfer time

5. **Monitor Export Performance**
   
   Track export times and file sizes for optimization

6. **Use ORM-Style Exports**
   
   Prefer chainable query builder for type safety

Common Use Cases
----------------

**Data Backup**

.. code-block:: python

   with client.session() as session:
       # Create backup stage
       session.stage.create_s3('backup_stage', 'backups', 'daily/', 'key', 'secret')
       
       # Export all tables
       for table in [Users, Orders, Products]:
           stmt = select(table)
           session.export.to_stage(
               stmt,
               stage_name='backup_stage',
               filename=f'{table.__tablename__}_backup.csv'
           )

**Data Sharing**

.. code-block:: python

   with client.session() as session:
       # Export filtered data for partner
       stmt = select(Sale).where(Sale.partner_id == 'ABC123')
       session.export.to_stage(
           stmt,
           stage_name='partner_stage',
           filename='partner_sales.csv',
           format=ExportFormat.CSV,
           header=True
       )

**Analytics Export**

.. code-block:: python

   with client.session() as session:
       # Export aggregated analytics
       stmt = select(
           Sale.date,
           func.sum(Sale.amount).label('daily_revenue')
       ).group_by(Sale.date)
       
       session.export.from_query(
           stmt,
           output_file='/reports/daily_revenue.csv',
           format=ExportFormat.CSV,
           header=True
       )

See Also
--------

* :doc:`stage_guide` - External stage management
* :doc:`load_data_guide` - Data loading operations
* :doc:`quickstart` - Quick start guide

