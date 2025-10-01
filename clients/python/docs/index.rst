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
   configuration_guide
   connection_hooks_guide
   orm_guide
   vector_guide
   fulltext_guide
   metadata_guide
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
* 📊 **Metadata Analysis**: Comprehensive table and column metadata analysis with statistics
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

**Basic Database Operations:**

.. code-block:: python

   from matrixone import Client

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

   # Get backend version (auto-detected)
   version = client.get_backend_version()
   print(f"MatrixOne version: {version}")

   client.disconnect()

**Vector Search:**

.. code-block:: python

   from matrixone.sqlalchemy_ext import create_vector_column
   from sqlalchemy import Column, Integer, String, Text
   from matrixone.orm import declarative_base
   import numpy as np

   # Define vector table model
   Base = declarative_base()
   
   class Document(Base):
       __tablename__ = 'documents'
       id = Column(Integer, primary_key=True)
       title = Column(String(200))
       content = Column(Text)
       embedding = create_vector_column(384, 'f32')

   # Create table and HNSW index
   client.create_table(Document)
   client.vector_ops.enable_hnsw()
   client.vector_ops.create_hnsw(
       table_name='documents',
       name='idx_embedding',
       column='embedding',
       m=16,
       ef_construction=200
   )

   # Insert and search
   client.insert('documents', {
       'id': 1,
       'title': 'AI Research',
       'content': 'Machine learning paper...',
       'embedding': np.random.rand(384).tolist()
   })

   results = client.vector_ops.similarity_search(
       table_name='documents',
       vector_column='embedding',
       query_vector=np.random.rand(384).tolist(),
       limit=5,
       distance_type='cosine'
   )

**Fulltext Search:**

.. code-block:: python

   # Create fulltext index
   client.fulltext_index.create(
       'documents',
       'ftidx_content',
       ['title', 'content'],
       algorithm='BM25'
   )

   # Search with natural language
   from matrixone.sqlalchemy_ext.fulltext_search import natural_match
   
   results = client.query(Document).filter(
       natural_match('title', 'content', query='machine learning techniques')
   ).all()

**Metadata Analysis:**

.. code-block:: python

   # Analyze table metadata
   metadata = client.metadata.scan(
       dbname='test',
       tablename='documents'
   )

   for row in metadata:
       print(f"{row.column_name}: {row.data_type}")
       print(f"  Nulls: {row.null_count}, Distinct: {row.distinct_count}")

   # Get table statistics
   stats = client.metadata.get_table_brief_stats(
       dbname='test',
       tablename='documents'
   )
   print(f"Rows: {stats.row_count}, Size: {stats.size_bytes} bytes")

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
