Metadata Operations Guide
==========================

MatrixOne provides powerful metadata scanning capabilities through the `metadata_scan` function, which allows you to analyze table statistics, column information, and data distribution. The MatrixOne Python client provides a convenient interface to access these capabilities.

Overview
--------

The metadata module provides the following key features:

- **Table Metadata Scanning**: Analyze table structure and statistics
- **Column Statistics**: Get detailed information about each column
- **Row Count Analysis**: Count rows with optional tombstone filtering
- **Size Information**: Analyze storage usage and data distribution
- **Index Metadata**: Scan metadata for specific indexes
- **Tombstone Analysis**: Analyze deleted data objects
- **Structured Schema**: Fixed schema with 13 predefined columns
- **Column Selection**: Choose specific columns to return
- **Type Safety**: Strongly typed data structures for metadata rows
- **Transaction Support**: Metadata operations within transactions

Transaction-Aware Metadata Operations (Recommended)
----------------------------------------------------

Use ``client.session()`` for metadata analysis within transactions:

Metadata Analysis in Transaction
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import Base, Column, Integer, String
   from sqlalchemy import select, insert, func
   
   client = Client()
   client.connect(database='test')
   
   # Analyze metadata within transaction
   with client.session() as session:
       # Insert test data
       session.execute(
           insert(User).values(name='Alice', email='alice@example.com')
       )
       session.execute(
           insert(User).values(name='Bob', email='bob@example.com')
       )
       
       # Scan metadata within same transaction
       metadata_rows = session.metadata.scan(
           dbname='test',
           tablename='users',
           columns='*'
       )
       
       for row in metadata_rows:
           print(f"Column: {row.col_name}")
           print(f"  Rows: {row.rows_cnt}")
           print(f"  Nulls: {row.null_cnt}")
           print(f"  Size: {row.origin_size}")
       
       # Get table statistics
       stats = session.metadata.get_table_brief_stats(
           dbname='test',
           tablename='users'
       )
       print(f"Total rows: {stats['users']['row_cnt']}")
       
       # All operations commit together
   
   client.disconnect()

Metadata-Driven Table Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import select, func
   
   client = Client()
   client.connect(database='test')
   
   # Use metadata to drive table operations
   with client.session() as session:
       # Scan table metadata
       metadata_rows = session.metadata.scan(
           dbname='test',
           tablename='large_table'
       )
       
       # Analyze null counts
       high_null_columns = [
           row.col_name for row in metadata_rows
           if row.null_cnt > row.rows_cnt * 0.5  # > 50% nulls
       ]
       
       if high_null_columns:
           print(f"Columns with high null rate: {high_null_columns}")
           
           # Optionally clean up or report
           for col in high_null_columns:
               # Could update, log, or alert
               pass
       
       # Get detailed statistics
       stats = session.metadata.get_table_brief_stats(
           dbname='test',
           tablename='large_table'
       )
       
       # All analysis within transaction
   
   client.disconnect()

Cross-Table Metadata Comparison
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import select
   
   client = Client()
   client.connect(database='test')
   
   # Compare metadata across tables
   with client.session() as session:
       # Scan multiple tables
       users_meta = session.metadata.scan(dbname='test', tablename='users')
       orders_meta = session.metadata.scan(dbname='test', tablename='orders')
       
       # Compare sizes
       users_size = sum(row.origin_size for row in users_meta)
       orders_size = sum(row.origin_size for row in orders_meta)
       
       print(f"Users table size: {users_size} bytes")
       print(f"Orders table size: {orders_size} bytes")
       
       # Get brief stats for both
       users_stats = session.metadata.get_table_brief_stats('test', 'users')
       orders_stats = session.metadata.get_table_brief_stats('test', 'orders')
       
       print(f"Users: {users_stats['users']['row_cnt']} rows")
       print(f"Orders: {orders_stats['orders']['row_cnt']} rows")
       
       # All operations atomic
   
   client.disconnect()

Basic Usage
-----------

Synchronous Operations
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import Client
    from matrixone.config import get_connection_kwargs

    # Connect to MatrixOne
    client = Client()
    client.connect(**get_connection_kwargs())

    # Basic table metadata scan
    result = client.metadata.scan("mo_catalog", "mo_columns")
    rows = result.fetchall()
    
    for row in rows:
        print(f"Column: {row._mapping['col_name']}")
        print(f"Rows: {row._mapping['rows_cnt']}")
        print(f"Nulls: {row._mapping['null_cnt']}")
        print(f"Size: {row._mapping['origin_size']}")

    client.disconnect()

Metadata Schema and Column Selection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The metadata scan returns a fixed schema with 13 predefined columns. You can choose to return all columns as structured data or select specific columns.

Available Columns
^^^^^^^^^^^^^^^^^

The metadata schema includes the following columns:

.. code-block:: python

    from matrixone.metadata import MetadataColumn
    
    # Available columns
    print(MetadataColumn.COL_NAME.value)        # 'col_name'
    print(MetadataColumn.OBJECT_NAME.value)     # 'object_name'
    print(MetadataColumn.IS_HIDDEN.value)       # 'is_hidden'
    print(MetadataColumn.OBJ_LOC.value)         # 'obj_loc'
    print(MetadataColumn.CREATE_TS.value)       # 'create_ts'
    print(MetadataColumn.DELETE_TS.value)       # 'delete_ts'
    print(MetadataColumn.ROWS_CNT.value)        # 'rows_cnt'
    print(MetadataColumn.NULL_CNT.value)        # 'null_cnt'
    print(MetadataColumn.COMPRESS_SIZE.value)   # 'compress_size'
    print(MetadataColumn.ORIGIN_SIZE.value)     # 'origin_size'
    print(MetadataColumn.MIN.value)             # 'min'
    print(MetadataColumn.MAX.value)             # 'max'
    print(MetadataColumn.SUM.value)             # 'sum'

Structured Data with All Columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To get all columns as structured data objects:

.. code-block:: python

    from matrixone.metadata import MetadataColumn
    
    # Get all columns as structured MetadataRow objects
    rows = client.metadata.scan("mo_catalog", "mo_columns", columns="*")
    
    for row in rows:
        print(f"Column: {row.col_name}")
        print(f"Rows: {row.rows_cnt}")
        print(f"Hidden: {row.is_hidden}")
        print(f"Size: {row.origin_size}")
        print(f"Min: {row.min}")
        print(f"Max: {row.max}")

Selecting Specific Columns
^^^^^^^^^^^^^^^^^^^^^^^^^^

To get only specific columns:

.. code-block:: python

    from matrixone.metadata import MetadataColumn
    
    # Get only column name and row count
    rows = client.metadata.scan("mo_catalog", "mo_columns", 
                               columns=[MetadataColumn.COL_NAME, MetadataColumn.ROWS_CNT])
    
    for row in rows:
        print(f"Column: {row['col_name']}")
        print(f"Rows: {row['rows_cnt']}")
    
    # Or using string column names
    rows = client.metadata.scan("mo_catalog", "mo_columns", 
                               columns=['col_name', 'origin_size'])
    
    for row in rows:
        print(f"Column: {row['col_name']}")
        print(f"Size: {row['origin_size']}")

Distinct Object Names
^^^^^^^^^^^^^^^^^^^^^

To get only distinct object names from metadata scan:

.. code-block:: python

    from matrixone.metadata import MetadataColumn
    
    # Get distinct object names only
    rows = client.metadata.scan("mo_catalog", "mo_columns", distinct_object_name=True)
    
    for row in rows:
        print(f"Object: {row._mapping['object_name']}")
    
    # Get distinct object names with structured data
    rows = client.metadata.scan("mo_catalog", "mo_columns", 
                               distinct_object_name=True, columns="*")
    
    for row in rows:
        print(f"Object: {row.object_name}")

Asynchronous Operations
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import asyncio
    from matrixone import AsyncClient
    from matrixone.config import get_connection_kwargs

    async def main():
        # Connect to MatrixOne
        client = AsyncClient()
        await client.connect(**get_connection_kwargs())

        # Basic async table metadata scan (raw SQLAlchemy Result)
        result = await client.metadata.scan("mo_catalog", "mo_columns")
        rows = result.fetchall()
        
        for row in rows:
            print(f"Column: {row._mapping['col_name']}")
            print(f"Rows: {row._mapping['rows_cnt']}")
            print(f"Nulls: {row._mapping['null_cnt']}")
            print(f"Size: {row._mapping['origin_size']}")
        
        # Get structured data with all columns
        rows = await client.metadata.scan("mo_catalog", "mo_columns", columns="*")
        
        for row in rows:
            print(f"Column: {row.col_name}")
            print(f"Rows: {row.rows_cnt}")
            print(f"Hidden: {row.is_hidden}")
            print(f"Size: {row.origin_size}")
        
        # Get only specific columns
        rows = await client.metadata.scan("mo_catalog", "mo_columns", 
                                         columns=['col_name', 'rows_cnt', 'origin_size'])
        
        for row in rows:
            print(f"Column: {row['col_name']}")
            print(f"Rows: {row['rows_cnt']}")
            print(f"Size: {row['origin_size']}")

        await client.disconnect()

    asyncio.run(main())

Metadata Scan Syntax
--------------------

The `metadata_scan` function supports different syntax patterns for various use cases:

Basic Table Scan
~~~~~~~~~~~~~~~~

.. code-block:: python

    # Scan all columns of a table
    result = client.metadata.scan("mo_catalog", "mo_columns")
    
    # Equivalent SQL: SELECT * FROM metadata_scan('mo_catalog.mo_columns', '*')

Index-Specific Scan
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Scan specific index
    result = client.metadata.scan("mo_catalog", "mo_columns", indexname="index_name")
    
    # Equivalent SQL: SELECT * FROM metadata_scan('mo_catalog.mo_columns.?index_name', '*')

Tombstone Scan
~~~~~~~~~~~~~~

.. code-block:: python

    # Scan tombstone objects
    result = client.metadata.scan("mo_catalog", "mo_columns", is_tombstone=True)
    
    # Equivalent SQL: SELECT * FROM metadata_scan('mo_catalog.mo_columns.#', '*')

Index Tombstone Scan
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Scan tombstone objects for specific index
    result = client.metadata.scan("mo_catalog", "mo_columns", indexname="index_name", is_tombstone=True)
    
    # Equivalent SQL: SELECT * FROM metadata_scan('mo_catalog.mo_columns.?index_name.#', '*')

High-Level Methods
------------------

Table Brief Statistics
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Get brief statistics for a table
    stats = client.metadata.get_table_brief_stats("mo_catalog", "mo_columns")
    
    for table_name, table_stats in stats.items():
        print(f"Table: {table_name}")
        print(f"  Total objects: {table_stats['total_objects']}")
        print(f"  Total rows: {table_stats['row_cnt']}")
        print(f"  Total nulls: {table_stats['null_cnt']}")
        print(f"  Original size: {table_stats['original_size']}")
        print(f"  Compressed size: {table_stats['compress_size']}")

Table Detailed Statistics
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Get detailed statistics for a table
    detail_stats = client.metadata.get_table_detail_stats("mo_catalog", "mo_columns")
    
    for table_name, table_details in detail_stats.items():
        print(f"Table: {table_name}")
        for detail in table_details:
            print(f"  Object: {detail['object_name']}")
            print(f"    Created: {detail['create_ts']}")
            print(f"    Rows: {detail['row_cnt']}, Nulls: {detail['null_cnt']}")
            print(f"    Size: {detail['original_size']} -> {detail['compress_size']}")

Transaction Operations
----------------------

Metadata operations can also be performed within transactions:

.. code-block:: python

    with client.transaction() as tx:
        # Get table brief stats within transaction
        stats = tx.metadata.get_table_brief_stats("mo_catalog", "mo_columns")
        
        # Get table detailed stats within transaction
        detail_stats = tx.metadata.get_table_detail_stats("mo_catalog", "mo_columns")
        
        # Scan metadata within transaction
        result = tx.metadata.scan("mo_catalog", "mo_columns")

Async Transaction Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    async with client.transaction() as tx:
        # Get table brief stats within async transaction
        stats = await tx.metadata.get_table_brief_stats("mo_catalog", "mo_columns")
        
        # Get table detailed stats within async transaction
        detail_stats = await tx.metadata.get_table_detail_stats("mo_catalog", "mo_columns")
        
        # Scan metadata within async transaction
        result = await tx.metadata.scan("mo_catalog", "mo_columns")

Metadata Fields
---------------

The metadata scan results contain the following fields:

- **col_name**: Column name
- **rows_cnt**: Number of rows in the column
- **null_cnt**: Number of null values
- **origin_size**: Original size in bytes
- **min**: Minimum value (if applicable)
- **max**: Maximum value (if applicable)
- **sum**: Sum of values (if applicable)
- **create_ts**: Creation timestamp
- **is_tombstone**: Whether this is a tombstone object

Example Use Cases
-----------------

Database Analysis
~~~~~~~~~~~~~~~~~

.. code-block:: python

    def analyze_database(client, database_name):
        """Analyze all tables in a database"""
        # Get list of tables
        result = client.execute(f"SHOW TABLES FROM {database_name}")
        tables = [row[0] for row in result.rows]
        
        total_size = 0
        total_rows = 0
        
        for table in tables:
            stats = client.metadata.get_table_brief_stats(database_name, table)
            table_stats = stats.get(table, {})
            total_size += table_stats.get('original_size', 0)
            total_rows += table_stats.get('row_cnt', 0)
            
            print(f"Table: {table}")
            print(f"  Rows: {table_stats.get('row_cnt', 0)}")
            print(f"  Size: {table_stats.get('original_size', 0)} bytes")
            print(f"  Objects: {table_stats.get('total_objects', 0)}")
        
        print(f"\nDatabase Summary:")
        print(f"Total tables: {len(tables)}")
        print(f"Total rows: {total_rows}")
        print(f"Total size: {total_size} bytes")

Storage Optimization
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    def find_large_tables(client, database_name, size_threshold=1024*1024):
        """Find tables larger than threshold"""
        result = client.execute(f"SHOW TABLES FROM {database_name}")
        tables = [row[0] for row in result.rows]
        
        large_tables = []
        
        for table in tables:
            stats = client.metadata.get_table_brief_stats(database_name, table)
            table_stats = stats.get(table, {})
            table_size = table_stats.get('original_size', 0)
            
            if table_size > size_threshold:
                large_tables.append({
                    'table': table,
                    'size': table_size,
                    'objects': table_stats.get('total_objects', 0)
                })
        
        # Sort by size
        large_tables.sort(key=lambda x: x['size'], reverse=True)
        
        print("Large tables:")
        for table_info in large_tables:
            print(f"  {table_info['table']}: {table_info['size']} bytes, {table_info['objects']} objects")

Data Quality Analysis
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    def analyze_data_quality(client, database_name, table_name):
        """Analyze data quality metrics"""
        result = client.metadata.scan(database_name, table_name)
        rows = result.fetchall()
        
        print(f"Data Quality Analysis for {table_name}:")
        
        for row in rows:
            column_name = row._mapping['col_name']
            total_rows = row._mapping['rows_cnt']
            null_count = row._mapping['null_cnt']
            null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
            
            print(f"  {column_name}:")
            print(f"    Total rows: {total_rows}")
            print(f"    Null values: {null_count} ({null_percentage:.2f}%)")
            print(f"    Data completeness: {100 - null_percentage:.2f}%")

Performance Monitoring
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    def monitor_table_growth(client, database_name, table_name):
        """Monitor table growth over time"""
        import time
        
        while True:
            stats = client.metadata.get_table_brief_stats(database_name, table_name)
            table_stats = stats.get(table_name, {})
            
            print(f"Table: {table_name}")
            print(f"  Rows: {table_stats.get('row_cnt', 0)}")
            print(f"  Size: {table_stats.get('original_size', 0)} bytes")
            print(f"  Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("-" * 40)
            
            time.sleep(60)  # Check every minute

Best Practices
--------------

1. **Use Appropriate Methods**: Choose the right method for your use case:
   - Use `scan()` for raw metadata access and column-specific analysis
   - Use `get_table_brief_stats()` for quick table overview
   - Use `get_table_detail_stats()` for comprehensive table analysis with object details

2. **Handle Tombstone Data**: Be aware of tombstone objects when analyzing data:
   - Use `is_tombstone=False` to exclude deleted data
   - Use `is_tombstone=True` to analyze deleted data patterns

3. **Index Analysis**: Use index-specific scans to analyze index performance:
   - Monitor index size and usage
   - Identify unused or oversized indexes

4. **Async Operations**: Use async methods for better performance in concurrent scenarios:
   - Async methods are non-blocking
   - Better resource utilization
   - Suitable for monitoring and analysis tools

5. **Transaction Context**: Use metadata operations within transactions when needed:
   - Ensures consistency with other operations
   - Useful for data migration and analysis

Error Handling
--------------

.. code-block:: python

    try:
        stats = client.metadata.get_table_brief_stats("mo_catalog", "mo_columns")
    except Exception as e:
        print(f"Error getting table stats: {e}")
        # Handle error appropriately

    try:
        result = client.metadata.scan("mo_catalog", "nonexistent_table")
    except Exception as e:
        print(f"Table does not exist: {e}")
        # Handle missing table

Troubleshooting
---------------

Common Issues
~~~~~~~~~~~~~

1. **Table Not Found**: Ensure the table exists and you have proper permissions
2. **Database Not Found**: Verify the database name and connection
3. **Permission Denied**: Check user permissions for metadata access
4. **Empty Results**: Some metadata operations may return empty results for new tables

Performance Considerations
~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **Large Tables**: Metadata scanning can be slow for very large tables
2. **Frequent Queries**: Consider caching results for frequently accessed metadata
3. **Index Usage**: Use index-specific scans when possible for better performance
4. **Async Operations**: Use async methods for better concurrency

For more examples and advanced usage, see the `examples/example_metadata.py` file.
