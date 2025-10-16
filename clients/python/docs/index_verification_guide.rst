Secondary Index Verification Guide
====================================

This guide demonstrates how to verify the consistency of secondary indexes in MatrixOne using the Python SDK.

Overview
--------

MatrixOne uses secondary index tables to speed up queries. The SDK provides utilities to:

1. Get all secondary index table names for a table
2. Get a specific index table by its index name  
3. Verify that all index tables have the same row count as the main table

These operations are available in both synchronous (:class:`~matrixone.Client`) and asynchronous (:class:`~matrixone.AsyncClient`) clients.

Basic Usage
-----------

Getting Secondary Index Tables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import Client
    
    client = Client()
    client.connect(host='localhost', port=6001, user='root', password='111', database='test')
    
    # Get all secondary index tables
    index_tables = client.get_secondary_index_tables('my_table')
    print(f"Found {len(index_tables)} secondary indexes:")
    for table in index_tables:
        print(f"  - {table}")

Getting Index Table by Name
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Get physical table name for a specific index
    physical_table = client.get_secondary_index_table_by_name('my_table', 'idx_name')
    
    if physical_table:
        print(f"Index 'idx_name' maps to: {physical_table}")
    else:
        print("Index not found")

Getting Detailed Index Information
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For comprehensive index information including IVF, HNSW, Fulltext, and regular indexes:

.. code-block:: python

    # Get detailed information about all indexes for a table
    indexes = client.get_table_indexes_detail('my_table')
    
    for idx in indexes:
        print(f"Index: {idx['index_name']}")
        print(f"  Algorithm: {idx['algo'] or 'regular'}")
        print(f"  Table Type: {idx['algo_table_type'] or 'index'}")
        print(f"  Physical Table: {idx['physical_table_name']}")
        print(f"  Columns: {', '.join(idx['columns'])}")
        
    # Example output for IVF index:
    # Index: idx_embedding_ivf
    #   Algorithm: ivfflat
    #   Table Type: metadata
    #   Physical Table: __mo_index_secondary_018e1234_meta
    #   Columns: embedding
    # Index: idx_embedding_ivf
    #   Algorithm: ivfflat
    #   Table Type: centroids
    #   Physical Table: __mo_index_secondary_018e5678_centroids
    #   Columns: embedding
    # Index: idx_embedding_ivf
    #   Algorithm: ivfflat
    #   Table Type: entries
    #   Physical Table: __mo_index_secondary_018e9abc_entries
    #   Columns: embedding

Verifying Index Consistency
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Verify all index tables have the same count as main table
    try:
        count = client.verify_table_index_counts('my_table')
        print(f"✓ Verification passed! Row count: {count}")
    except ValueError as e:
        print(f"✗ Verification failed!")
        print(e)
        # Error message includes details about which indexes are mismatched

Complete Example
----------------

.. code-block:: python

    from matrixone import Client
    from matrixone.orm import declarative_base
    from sqlalchemy import Column, String, Integer, Index
    
    # Connect to database
    client = Client()
    client.connect(host='localhost', port=6001, user='root', password='111', database='test')
    
    # Define model with secondary indexes
    Base = declarative_base()
    
    class Product(Base):
        __tablename__ = 'products'
        
        id = Column(Integer, primary_key=True)
        name = Column(String(100))
        category = Column(String(50))
        price = Column(Integer)
        
        __table_args__ = (
            Index('idx_name', 'name'),
            Index('idx_category', 'category'),
            Index('idx_price', 'price'),
        )
    
    # Create table with indexes
    client.create_table(Product)
    
    # Insert data
    products = [
        {'id': i, 'name': f'Product {i}', 'category': f'Cat {i % 5}', 'price': i * 100}
        for i in range(1, 1001)
    ]
    client.batch_insert(Product, products)
    
    # Get all secondary indexes
    print("Secondary indexes:")
    index_tables = client.get_secondary_index_tables('products')
    for idx_table in index_tables:
        print(f"  {idx_table}")
    
    # Get specific index by name
    name_index = client.get_secondary_index_table_by_name('products', 'idx_name')
    print(f"\nName index table: {name_index}")
    
    # Get detailed index information
    print("\nDetailed index information:")
    indexes = client.get_table_indexes_detail('products')
    for idx in indexes:
        print(f"  {idx['index_name']} ({idx['algo'] or 'regular'}) - {idx['physical_table_name']}")
    
    # Verify consistency
    try:
        count = client.verify_table_index_counts('products')
        print(f"\n✓ All indexes verified! Row count: {count}")
    except ValueError as e:
        print(f"\n✗ Verification failed: {e}")
    
    client.disconnect()

Async Usage
-----------

The same functionality is available in async mode:

.. code-block:: python

    import asyncio
    from matrixone import AsyncClient
    
    async def verify_indexes():
        client = AsyncClient()
        await client.connect(
            host='localhost',
            port=6001,
            user='root',
            password='111',
            database='test'
        )
        
        # Get secondary index tables (async)
        index_tables = await client.get_secondary_index_tables('my_table')
        
        # Get specific index by name (async)
        idx_table = await client.get_secondary_index_table_by_name('my_table', 'idx_name')
        
        # Verify consistency (async)
        try:
            count = await client.verify_table_index_counts('my_table')
            print(f"✓ Verified! Count: {count}")
        except ValueError as e:
            print(f"✗ Verification failed: {e}")
        
        await client.disconnect()
    
    asyncio.run(verify_indexes())

Use Cases
---------

Data Integrity Checks
~~~~~~~~~~~~~~~~~~~~~~

Use these methods to verify data integrity after:

- Bulk data operations
- Data migration
- Index rebuilds
- Database recovery

.. code-block:: python

    # After bulk insert
    client.batch_insert(MyTable, large_dataset)
    
    # Verify indexes are consistent
    count = client.verify_table_index_counts('my_table')
    print(f"Verified {count} rows across all indexes")

Monitoring and Diagnostics
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Monitor index health in production:

.. code-block:: python

    import time
    
    while True:
        try:
            count = client.verify_table_index_counts('critical_table')
            print(f"{time.ctime()}: ✓ Indexes OK ({count} rows)")
        except ValueError as e:
            print(f"{time.ctime()}: ✗ INDEX MISMATCH DETECTED!")
            print(e)
            # Alert monitoring system
        
        time.sleep(60)  # Check every minute

API Reference
-------------

Client.get_secondary_index_tables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: matrixone.Client.get_secondary_index_tables
   :noindex:

Client.get_secondary_index_table_by_name
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: matrixone.Client.get_secondary_index_table_by_name
   :noindex:

Client.get_table_indexes_detail
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: matrixone.Client.get_table_indexes_detail
   :noindex:

   This method provides comprehensive information about all indexes including:
   
   - Regular secondary indexes (MULTIPLE, UNIQUE)
   - IVF vector indexes (with metadata, centroids, and entries tables)
   - HNSW vector indexes
   - Fulltext indexes
   
   Each index may have multiple physical tables (especially for IVF indexes which have
   metadata, centroids, and entries tables).

Client.verify_table_index_counts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: matrixone.Client.verify_table_index_counts
   :noindex:

AsyncClient Methods
~~~~~~~~~~~~~~~~~~~

All methods are also available in async form:

- :meth:`~matrixone.AsyncClient.get_secondary_index_tables`
- :meth:`~matrixone.AsyncClient.get_secondary_index_table_by_name`
- :meth:`~matrixone.AsyncClient.get_table_indexes_detail`
- :meth:`~matrixone.AsyncClient.verify_table_index_counts`

Error Handling
--------------

The ``verify_table_index_counts()`` method raises a ``ValueError`` with detailed information when verification fails:

.. code-block:: python

    try:
        count = client.verify_table_index_counts('my_table')
    except ValueError as e:
        # Example error message:
        # Index count verification failed!
        # Main table 'my_table': 20000 rows
        # ✗ MISMATCH Index '__mo_index_secondary_..._idx1': 19900 rows
        # ✓ Index '__mo_index_secondary_..._idx2': 20000 rows
        # ✓ Index '__mo_index_secondary_..._idx3': 20000 rows
        print(str(e))

Performance Notes
-----------------

- All count comparisons are done in a **single SQL query** for consistency
- The verification is atomic - all counts are from the same transaction
- Efficient for tables with multiple indexes (no N+1 query problem)

Example SQL generated:

.. code-block:: sql

    SELECT 
        (SELECT COUNT(*) FROM `main_table`) as main_count,
        (SELECT COUNT(*) FROM `__mo_index_secondary_..._idx1`) as idx1_count,
        (SELECT COUNT(*) FROM `__mo_index_secondary_..._idx2`) as idx2_count,
        (SELECT COUNT(*) FROM `__mo_index_secondary_..._idx3`) as idx3_count

See Also
--------

- :doc:`orm_guide` - ORM model definition with indexes
- :doc:`api/client` - Complete Client API reference
- :doc:`api/async_client` - Complete AsyncClient API reference

