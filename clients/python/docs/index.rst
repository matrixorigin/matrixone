MatrixOne Python SDK Documentation
==================================

Welcome to the MatrixOne Python SDK documentation!

The MatrixOne Python SDK provides a comprehensive, high-level interface for MatrixOne database operations,
including SQLAlchemy-like ORM interface, vector similarity search, fulltext search, snapshot management,
PITR (Point-in-Time Recovery), restore operations, table cloning, account management, pub/sub operations,
and mo-ctl integration. The SDK is designed for both synchronous and asynchronous operations with full
type safety and extensive documentation.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   orm_guide
   logical_in_guide
   vector_guide
   pinecone_guide
   fulltext_guide
   orm_vector_search_guide
   account_guide
   pubsub_guide
   snapshot_restore_guide
   clone_guide
   moctl_guide
   best_practices
   api/index
   examples
   contributing

Features
--------

* 🚀 **High Performance**: Optimized for MatrixOne database operations with connection pooling
* 🔄 **Async Support**: Full async/await support with AsyncClient for non-blocking operations
* 🧠 **Vector Search**: Advanced vector similarity search with HNSW and IVF indexing
* 🔍 **Fulltext Search**: Powerful fulltext search with BM25 and TF-IDF algorithms
* 📸 **Snapshot Management**: Create and manage database snapshots at multiple levels
* ⏰ **Point-in-Time Recovery**: PITR functionality for precise data recovery
* 🔄 **Table Cloning**: Clone databases and tables efficiently with data replication
* 👥 **Account Management**: Comprehensive user, role, and permission management
* 📊 **Pub/Sub**: Real-time publication and subscription support
* 🔧 **Version Management**: Automatic backend version detection and compatibility checking
* 🛡️ **Type Safety**: Full type hints support with comprehensive documentation
* 📚 **SQLAlchemy Integration**: Seamless SQLAlchemy integration with enhanced ORM features
* 🔗 **Enhanced Query Building**: Advanced query building with SQLAlchemy expressions
* 🎯 **Logical Operations**: Enhanced logical operations including logical_in functionality
* 📖 **Comprehensive Documentation**: Detailed API documentation with examples

Quick Start
-----------

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import func

   # Create and connect to MatrixOne
   client = Client()
   client.connect(
       host='localhost',
       port=6001,
       user='root',
       password='111',
       database='test'
   )

   # Execute queries
   result = client.execute("SELECT 1 as test")
   print(result.fetchall())

   # Create a vector table
   client.create_table("documents", {
       "id": "int",
       "content": "text",
       "embedding": "vector(384,f32)"
   }, primary_key="id")

   # Create vector index
   client.vector.create_hnsw(
       table_name="documents",
       name="idx_embedding",
       column="embedding",
       m=16,
       ef_construction=200
   )

   # Insert vector data
   client.insert("documents", {
       "id": 1,
       "content": "Sample document",
       "embedding": [0.1, 0.2, 0.3] * 128  # 384-dimensional vector
   })

   # Vector similarity search
   results = client.vector_query.similarity_search(
       table_name="documents",
       vector_column="embedding",
       query_vector=[0.1, 0.2, 0.3] * 128,
       limit=5,
       distance_function="cosine"
   )

   # Get backend version (auto-detected)
   version = client.version()
   print(f"MatrixOne version: {version}")

   client.disconnect()

Installation
------------

.. code-block:: bash

   pip install matrixone-python-sdk

For development installation, see the :doc:`installation` page.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
