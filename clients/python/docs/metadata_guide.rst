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
    result = client.metadata.scan("database_name", "table_name")
    rows = result.fetchall()
    
    for row in rows:
        print(f"Column: {row._mapping['col_name']}")
        print(f"Rows: {row._mapping['rows_cnt']}")
        print(f"Nulls: {row._mapping['null_cnt']}")
        print(f"Size: {row._mapping['origin_size']}")

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
    rows = client.metadata.scan("database_name", "table_name", columns="*")
    
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
    rows = client.metadata.scan("database_name", "table_name", 
                               columns=[MetadataColumn.COL_NAME, MetadataColumn.ROWS_CNT])
    
    for row in rows:
        print(f"Column: {row['col_name']}")
        print(f"Rows: {row['rows_cnt']}")
    
    # Or using string column names
    rows = client.metadata.scan("database_name", "table_name", 
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
    rows = client.metadata.scan("database_name", "table_name", distinct_object_name=True)
    
    for row in rows:
        print(f"Object: {row._mapping['object_name']}")
    
    # Get distinct object names with structured data
    rows = client.metadata.scan("database_name", "table_name", 
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
        result = await client.metadata.scan("database_name", "table_name")
        rows = result.fetchall()
        
        for row in rows:
            print(f"Column: {row._mapping['col_name']}")
            print(f"Rows: {row._mapping['rows_cnt']}")
            print(f"Nulls: {row._mapping['null_cnt']}")
            print(f"Size: {row._mapping['origin_size']}")
        
        # Get structured data with all columns
        rows = await client.metadata.scan("database_name", "table_name", columns="*")
        
        for row in rows:
            print(f"Column: {row.col_name}")
            print(f"Rows: {row.rows_cnt}")
            print(f"Hidden: {row.is_hidden}")
            print(f"Size: {row.origin_size}")
        
        # Get only specific columns
        rows = await client.metadata.scan("database_name", "table_name", 
                                         columns=['col_name', 'rows_cnt', 'origin_size'])
        
        for row in rows:
            print(f"Column: {row['col_name']}")
            print(f"Rows: {row['rows_cnt']}")
            print(f"Size: {row['origin_size']}")

    asyncio.run(main())

Metadata Scan Syntax
--------------------

The `metadata_scan` function supports different syntax patterns for various use cases:

Basic Table Scan
~~~~~~~~~~~~~~~~

.. code-block:: python

    # Scan all columns of a table
    result = client.metadata.scan("db_name", "table_name")
    
    # Equivalent SQL: SELECT * FROM metadata_scan('db_name.table_name', '*')

Index-Specific Scan
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Scan specific index
    result = client.metadata.scan("db_name", "table_name", indexname="index_name")
    
    # Equivalent SQL: SELECT * FROM metadata_scan('db_name.table_name.?index_name', '*')

Tombstone Scan
~~~~~~~~~~~~~~

.. code-block:: python

    # Scan tombstone objects
    result = client.metadata.scan("db_name", "table_name", is_tombstone=True)
    
    # Equivalent SQL: SELECT * FROM metadata_scan('db_name.table_name.#', '*')

Index Tombstone Scan
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Scan tombstone objects for specific index
    result = client.metadata.scan("db_name", "table_name", indexname="index_name", is_tombstone=True)
    
    # Equivalent SQL: SELECT * FROM metadata_scan('db_name.table_name.?index_name.#', '*')

High-Level Methods
------------------

Column Statistics
~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Get statistics for all columns
    stats = client.metadata.get_column_stats("database_name", "table_name")
    
    for stat in stats:
        print(f"Column: {stat['name']}")
        print(f"Rows: {stat['rows_count']}")
        print(f"Nulls: {stat['null_count']}")
        print(f"Size: {stat['size']}")
        print(f"Min: {stat['min_value']}")
        print(f"Max: {stat['max_value']}")
        print(f"Sum: {stat['sum_value']}")

    # Get statistics for specific column
    stats = client.metadata.get_column_stats("database_name", "table_name", "column_name")

Table Information
~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Get comprehensive table information
    info = client.metadata.get_table_info("database_name", "table_name")
    
    print(f"Total rows: {info['total_rows']}")
    print(f"Total size: {info['total_size']} bytes")
    print(f"Columns: {len(info['columns'])}")
    
    for column in info['columns']:
        print(f"  - {column['name']}: {column['rows_count']} rows, {column['size']} bytes")

Row Count
~~~~~~~~~

.. code-block:: python

    # Get total row count
    count = client.metadata.get_row_count("database_name", "table_name")
    print(f"Total rows: {count}")
    
    # Get non-tombstone row count
    count = client.metadata.get_row_count("database_name", "table_name", is_tombstone=False)
    print(f"Active rows: {count}")

Size Information
~~~~~~~~~~~~~~~~

.. code-block:: python

    # Get size information
    size_info = client.metadata.get_size_info("database_name", "table_name")
    
    print(f"Total size: {size_info['total_size']} bytes")
    print(f"Column count: {size_info['column_count']}")

Transaction Operations
----------------------

Metadata operations can also be performed within transactions:

.. code-block:: python

    with client.transaction() as tx:
        # Get table info within transaction
        info = tx.metadata.get_table_info("database_name", "table_name")
        
        # Get column stats within transaction
        stats = tx.metadata.get_column_stats("database_name", "table_name")
        
        # Get row count within transaction
        count = tx.metadata.get_row_count("database_name", "table_name")

Async Transaction Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    async with client.transaction() as tx:
        # Get table info within async transaction
        info = await tx.metadata.get_table_info("database_name", "table_name")
        
        # Get column stats within async transaction
        stats = await tx.metadata.get_column_stats("database_name", "table_name")

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
            info = client.metadata.get_table_info(database_name, table)
            total_size += info['total_size']
            total_rows += info['total_rows']
            
            print(f"Table: {table}")
            print(f"  Rows: {info['total_rows']}")
            print(f"  Size: {info['total_size']} bytes")
            print(f"  Columns: {len(info['columns'])}")
        
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
            size_info = client.metadata.get_size_info(database_name, table)
            
            if size_info['total_size'] > size_threshold:
                large_tables.append({
                    'table': table,
                    'size': size_info['total_size'],
                    'columns': size_info['column_count']
                })
        
        # Sort by size
        large_tables.sort(key=lambda x: x['size'], reverse=True)
        
        print("Large tables:")
        for table_info in large_tables:
            print(f"  {table_info['table']}: {table_info['size']} bytes, {table_info['columns']} columns")

Data Quality Analysis
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    def analyze_data_quality(client, database_name, table_name):
        """Analyze data quality metrics"""
        stats = client.metadata.get_column_stats(database_name, table_name)
        
        print(f"Data Quality Analysis for {table_name}:")
        
        for stat in stats:
            column_name = stat['col_name']
            total_rows = stat['rows_cnt']
            null_count = stat['null_cnt']
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
            info = client.metadata.get_table_info(database_name, table_name)
            
            print(f"Table: {table_name}")
            print(f"  Rows: {info['total_rows']}")
            print(f"  Size: {info['total_size']} bytes")
            print(f"  Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("-" * 40)
            
            time.sleep(60)  # Check every minute

Best Practices
--------------

1. **Use Appropriate Methods**: Choose the right method for your use case:
   - Use `scan()` for raw metadata access
   - Use `get_table_info()` for comprehensive table analysis
   - Use `get_column_stats()` for column-specific analysis
   - Use `get_row_count()` for simple row counting

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
        info = client.metadata.get_table_info("database_name", "table_name")
    except Exception as e:
        print(f"Error getting table info: {e}")
        # Handle error appropriately

    try:
        result = client.metadata.scan("database_name", "nonexistent_table")
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
