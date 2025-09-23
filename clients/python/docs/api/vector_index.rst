Vector Index Extensions
========================

The MatrixOne Python SDK provides advanced vector indexing and similarity search capabilities through SQLAlchemy extensions.

VectorIndex
-----------

.. autoclass:: matrixone.sqlalchemy_ext.vector_index.VectorIndex
   :members:
   :undoc-members:
   :show-inheritance:

HNSWConfig
----------

.. autoclass:: matrixone.sqlalchemy_ext.hnsw_config.HNSWConfig
   :members:
   :undoc-members:
   :show-inheritance:

IVFConfig
---------

.. autoclass:: matrixone.sqlalchemy_ext.ivf_config.IVFConfig
   :members:
   :undoc-members:
   :show-inheritance:

Usage Examples
--------------

Basic Vector Index Creation
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import Client
    from matrixone.sqlalchemy_ext import VectorIndex, HNSWConfig

    client = Client()
    client.connect(host='localhost', port=6001, user='root', password='111', database='test')

    # Create a table with vector column
    client.execute("""
        CREATE TABLE embeddings (
            id INT PRIMARY KEY,
            text VARCHAR(200),
            embedding VECTOR(384)
        )
    """)

    # Create HNSW vector index
    config = HNSWConfig(m=16, ef_construction=200)
    client.vector_index.create(
        table_name='embeddings',
        name='idx_embedding',
        column='embedding',
        algorithm='hnsw',
        config=config
    )

    # Search for similar vectors
    result = client.execute("""
        SELECT id, text, embedding <-> '[0.1, 0.2, ...]' as distance
        FROM embeddings
        ORDER BY embedding <-> '[0.1, 0.2, ...]'
        LIMIT 10
    """)

Advanced Vector Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone.sqlalchemy_ext import VectorIndex, IVFConfig

    # Create IVF vector index
    config = IVFConfig(nlist=100, nprobe=10)
    client.vector_index.create(
        table_name='embeddings',
        name='idx_ivf',
        column='embedding',
        algorithm='ivf',
        config=config
    )

    # Batch insert vectors
    vectors = [
        (1, 'Document 1', [0.1, 0.2, 0.3, ...]),
        (2, 'Document 2', [0.4, 0.5, 0.6, ...]),
        (3, 'Document 3', [0.7, 0.8, 0.9, ...])
    ]
    
    for vector_data in vectors:
        client.execute(
            "INSERT INTO embeddings (id, text, embedding) VALUES (%s, %s, %s)",
            vector_data
        )

    # Search with different distance metrics
    result = client.execute("""
        SELECT id, text, 
               embedding <#> '[0.1, 0.2, ...]' as cosine_distance,
               embedding <-> '[0.1, 0.2, ...]' as l2_distance
        FROM embeddings
        ORDER BY embedding <-> '[0.1, 0.2, ...]'
        LIMIT 5
    """)

Async Vector Operations
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    import asyncio
    from matrixone import AsyncClient

    async def vector_async_example():
        async_client = AsyncClient()
        await async_client.connect(
            host='localhost', 
            port=6001, 
            user='root', 
            password='111', 
            database='test'
        )

        # Create vector index asynchronously
        config = HNSWConfig(m=16, ef_construction=200)
        await async_client.vector_index.create(
            table_name='embeddings',
            name='idx_async',
            column='embedding',
            algorithm='hnsw',
            config=config
        )

        # Search asynchronously
        result = await async_client.execute("""
            SELECT id, text FROM embeddings
            ORDER BY embedding <-> '[0.1, 0.2, ...]'
            LIMIT 10
        """)

        for row in result:
            print(f"Found: {row[1]}")

        await async_client.disconnect()

    asyncio.run(vector_async_example())

Vector Index Management
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # List all vector indexes
    indexes = client.vector_index.list()
    for idx in indexes:
        print(f"Index: {idx.name}, Table: {idx.table_name}")

    # Get index information
    info = client.vector_index.get('idx_embedding')
    print(f"Index algorithm: {info.algorithm}")
    print(f"Index config: {info.config}")

    # Drop vector index
    client.vector_index.drop('idx_embedding')

    # Rebuild index
    client.vector_index.rebuild('idx_embedding')
