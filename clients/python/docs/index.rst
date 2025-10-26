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
   :caption: Getting Started

   installation
   quickstart
   configuration_guide

.. toctree::
   :maxdepth: 2
   :caption: Core Features

   vector_guide
   fulltext_guide
   orm_guide
   metadata_guide
   index_verification_guide

.. toctree::
   :maxdepth: 2
   :caption: Data Management

   stage_guide
   load_data_guide
   export_guide
   snapshot_restore_guide
   clone_guide

.. toctree::
   :maxdepth: 2
   :caption: Advanced Features

   account_guide
   pubsub_guide
   moctl_guide
   mo_diag_guide

.. toctree::
   :maxdepth: 2
   :caption: Production Guide

   best_practices
   connection_hooks_guide

.. toctree::
   :maxdepth: 2
   :caption: Reference

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
* 🛠️ **MO-DIAG Tool**: Interactive diagnostic tool for index inspection and health monitoring
* 📖 **Comprehensive Documentation**: Detailed API documentation with examples

Quick Start
-----------

**Basic Connection:**

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

   # Get backend version (auto-detected)
   version = client.get_backend_version()
   print(f"MatrixOne version: {version}")

   client.disconnect()

**Transaction Management (Recommended):**

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import Base, Column, Integer, String
   from sqlalchemy import select, insert, update, delete

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
   
   # Create table
   client.create_table(User)

   # Use session for atomic transactions (recommended)
   with client.session() as session:
       # All operations are atomic - succeed or fail together
       session.execute(insert(User).values(name='Alice', email='alice@example.com', age=30))
       session.execute(update(User).where(User.age < 18).values(status='minor'))
       
       # Query using SQLAlchemy select
       stmt = select(User).where(User.age > 25)
       result = session.execute(stmt)
       for user in result.scalars():
           print(f"User: {user.name}, Age: {user.age}")
       
       # Commits automatically on success, rolls back on error

   client.disconnect()

.. note::
   **Why use ``session()``?**
   
   * ✅ **Atomic operations** - all succeed or fail together
   * ✅ **Automatic rollback** on errors
   * ✅ **Access to all managers** (snapshots, clones, load_data, etc.)
   * ✅ **Full SQLAlchemy ORM** support with type safety
   
   See :doc:`quickstart` and :doc:`orm_guide` for detailed examples.

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

   # Create table and insert initial data first (recommended)
   client.create_table(Document)
   
   # ⚠️ Best practice: Insert initial data BEFORE creating IVF index
   client.insert('documents', {
       'id': 1,
       'title': 'AI Research',
       'content': 'Machine learning paper...',
       'embedding': np.random.rand(384).tolist()
   })
   
   # Create IVF index after initial data (better clustering)
   client.vector_ops.enable_ivf()
   client.vector_ops.create_ivf(
       'documents',  # Table name as positional argument
       name='idx_embedding',
       column='embedding',
       lists=100  # Number of clusters
   )
   
   # IVF supports dynamic updates - can continue inserting
   client.insert('documents', {'id': 2, ...})  # ✅ Works fine

   results = client.vector_ops.similarity_search(
       'documents',  # Table name as positional argument
       vector_column='embedding',
       query_vector=np.random.rand(384).tolist(),
       limit=5,
       distance_type='cosine'
   )

**⭐ Monitor IVF Index Health (Critical for Production):**

.. code-block:: python

   # Get IVF index statistics - Essential for monitoring index quality
   stats = client.vector_ops.get_ivf_stats("documents", "embedding")
   
   # Check index balance
   counts = stats['distribution']['centroid_count']
   balance_ratio = max(counts) / min(counts) if min(counts) > 0 else float('inf')
   
   print(f"Total centroids: {len(counts)}")
   print(f"Total vectors: {sum(counts)}")
   print(f"Balance ratio: {balance_ratio:.2f}")
   
   # Rebuild if imbalanced (ratio > 2.5)
   if balance_ratio > 2.5:
       print("⚠️  Index needs rebuilding for optimal performance!")
       # See vector_guide for detailed monitoring and rebuild procedures

.. note::
   **HNSW Index Notes:**
   
   * Requires ``BigInteger`` primary key (not ``Integer``)
   * 🚧 Currently read-only after creation (*dynamic updates coming soon*)
   * Workaround: Drop index → Modify data → Recreate index
   
   See :doc:`vector_guide` for details on HNSW vs IVF selection.

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
