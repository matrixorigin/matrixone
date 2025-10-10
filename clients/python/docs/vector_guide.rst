Vector Search Guide
===================

This guide covers comprehensive vector search and indexing capabilities in the MatrixOne Python SDK, including modern vector operations for AI applications.

.. important::

   **⭐ Production Critical**: If you're using IVF indexes in production, jump directly to 
   :ref:`IVF Index Health Monitoring <ivf-index-health-monitoring>` to learn how to monitor 
   and maintain index quality with ``get_ivf_stats()``. This is **essential** for optimal 
   vector search performance.

Overview
--------

MatrixOne provides powerful vector operations designed for modern AI and machine learning applications:

* **Modern Vector API**: High-level vector operations with `vector_ops` and `vector_query` managers
* **Multiple Index Types**: HNSW and IVF indexes for different performance characteristics
* **⭐ Index Health Monitoring**: ``get_ivf_stats()`` for monitoring IVF index balance and performance
* **Flexible Data Types**: Support for vecf32 and vecf64 vector types with configurable dimensions
* **Distance Metrics**: L2, cosine, inner product, and negative inner product calculations
* **Client Integration**: Seamless integration with MatrixOne client interface
* **ORM Support**: Full SQLAlchemy integration with vector types
* **Performance Optimization**: Built-in query optimization and index management

The vector guide covers both basic vector operations and advanced indexing features.

Modern Vector Operations
------------------------

The MatrixOne Python SDK provides a modern, high-level API for vector operations that simplifies common AI workflows.

Creating Vector Tables with Table Models
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import Column, Integer, String, Text
   from matrixone.orm import declarative_base
   from matrixone.sqlalchemy_ext import create_vector_column

   # Create client and connect (see configuration_guide for connection options)
   client = Client()
   client.connect(host="127.0.0.1", port=6001, user="root", password="111", database="test")

   # Define table models
   Base = declarative_base()

   class Document(Base):
       __tablename__ = 'documents'
       id = Column(Integer, primary_key=True)
       title = Column(String(200))
       content = Column(Text)
       category = Column(String(50))
       # Use create_vector_column for ORM with distance methods (l2_distance, cosine_distance, etc.)
       embedding = create_vector_column(384, precision='f32')

   class Product(Base):
       __tablename__ = 'products'
       id = Column(Integer, primary_key=True)
       name = Column(String(200))
       description = Column(Text)
       category = Column(String(50))
       # Use create_vector_column for ORM with distance methods
       features = create_vector_column(512, precision='f64')

   # Create tables using models
   client.create_table(Document)
   client.create_table(Product)

   # Alternative: Create tables using create_table API with column definitions
   client.create_table("documents_alt", {
       "id": "int",
       "title": "varchar(200)",
       "content": "text",
       "category": "varchar(50)",
       "embedding": "vecf32(384)"  # 384-dimensional f32 vector
   }, primary_key="id")

Vector Index Management
~~~~~~~~~~~~~~~~~~~~~~~

MatrixOne provides powerful vector index management through the `vector_ops` API:

.. code-block:: python

   # Enable IVF indexing
   client.vector_ops.enable_ivf()

   # Create IVF index for similarity search
   client.vector_ops.create_ivf(
       "documents",
       name="idx_embedding_ivf",
       column="embedding",
       lists=50,                    # Number of clusters
       op_type="vector_l2_ops"      # Distance function
   )

   # Create another IVF index with different parameters
   client.vector_ops.create_ivf(
       "products",
       name="idx_features_ivf",
       column="features",
       lists=100,                   # More clusters for larger datasets
       op_type="vector_cosine_ops"  # Cosine distance
   )

   # Enable HNSW indexing
   client.vector_ops.enable_hnsw()

   # Create HNSW index
   client.vector_ops.create_hnsw(
       "documents",
       name="idx_embedding_hnsw",
       column="embedding",
       m=16,                        # Number of bi-directional links
       ef_construction=200,         # Size of dynamic candidate list
       ef_search=50                 # Size of dynamic candidate list for search
   )

   # Drop vector indexes using drop method
   client.vector_ops.drop("documents", "idx_embedding_ivf")
   client.vector_ops.drop("documents", "idx_embedding_hnsw")

Vector Data Insertion
~~~~~~~~~~~~~~~~~~~~~

Insert vector data using the modern insert API:

.. code-block:: python

   import numpy as np

   # Insert single document
   client.insert(Document, {
       "id": 1,
       "title": "AI Research Paper",
       "content": "Advanced artificial intelligence research",
       "category": "research",
       "embedding": np.random.rand(384).astype(np.float32).tolist()
   })

   # Batch insert multiple documents
   documents = [
       {
           "id": 2,
           "title": "Machine Learning Guide",
           "content": "Comprehensive machine learning tutorial",
           "category": "tutorial",
           "embedding": np.random.rand(384).astype(np.float32).tolist()
       },
       {
           "id": 3,
           "title": "Data Science Handbook",
           "content": "Complete data science reference",
           "category": "reference",
           "embedding": np.random.rand(384).astype(np.float32).tolist()
       }
   ]

   client.batch_insert(Document, documents)

Vector Similarity Search
~~~~~~~~~~~~~~~~~~~~~~~~

The `vector_query` API provides powerful similarity search capabilities:

.. code-block:: python

   # Perform vector similarity search
   query_vector = np.random.rand(384).astype(np.float32).tolist()
   
   # L2 distance search (returns list of tuples)
   results = client.vector_ops.similarity_search(
       "documents",
       vector_column="embedding",
       query_vector=query_vector,
       limit=5,
       distance_type="l2"
   )

   print("L2 Distance Search Results:")
   for result in results:
       print(f"  ID: {result[0]}, Title: {result[1]}, Distance: {result[-1]:.4f}")

   # Cosine distance search (returns list of tuples)
   cosine_results = client.vector_ops.similarity_search(
       "documents",
       vector_column="embedding",
       query_vector=query_vector,
       limit=5,
       distance_type="cosine"
   )

   print("Cosine Distance Search Results:")
   for result in cosine_results:
       print(f"  ID: {result[0]}, Title: {result[1]}, Distance: {result[-1]:.4f}")

Advanced Vector Search
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Search with offset for pagination
   results = client.vector_ops.similarity_search(
       "documents",
       vector_column="embedding",
       query_vector=query_vector,
       limit=10,
       offset=20,  # Skip first 20 results
       distance_type="l2"
   )

   # Search with custom select columns
   results = client.vector_ops.similarity_search(
       "documents",
       vector_column="embedding",
       query_vector=query_vector,
       limit=5,
       distance_type="l2",
       select_columns=["id", "title", "content"]  # Only return these columns
   )

   # Search with category filtering
   results = client.vector_ops.similarity_search(
       "documents",
       vector_column="embedding",
       query_vector=query_vector,
       limit=5,
       distance_type="l2",
       where_conditions=["category = ?"],
       where_params=["research"]
   )

Complex Vector Queries with Query Builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For complex vector queries, use the query builder with vector functions:

.. code-block:: python

   # Complex vector query with JOIN
  # ORM-style query with JOIN and vector filtering using client.query
  from sqlalchemy import and_
  
  results = client.query(Document).select(
      Document.id,
      Document.title,
      Document.content,
      Document.embedding.l2_distance(query_vector).label('distance')
  ).join(
      Category, Document.category_id == Category.id
  ).filter(
      and_(
          Document.embedding.l2_distance(query_vector) < 0.5,
          Category.name == 'AI'
      )
  ).order_by(
      Document.embedding.l2_distance(query_vector)
  ).limit(10).all()

  # ORM-style subquery for complex vector filtering
  from sqlalchemy import select
  
  # Create subquery for similar documents
  similar_docs = select(
      Document.id,
      Document.title,
      Document.embedding.l2_distance(query_vector).label('distance')
  ).where(
      Document.embedding.l2_distance(query_vector) < 0.8
  ).order_by('distance').limit(20).subquery('similar_docs')
  
  # Join subquery with full document data (use session.query for subquery joins)
  results = session.query(
      similar_docs.c.id,
      similar_docs.c.title,
      similar_docs.c.distance,
      Document.content
  ).join(
      Document, similar_docs.c.id == Document.id
  ).all()

  # ORM-style vector query with aggregation using client.query
  from sqlalchemy import func
  
  results = client.query(Document).select(
      Document.category,
      func.count(Document.id).label('doc_count'),
      func.avg(Document.embedding.l2_distance(query_vector)).label('avg_distance')
  ).filter(
      Document.embedding.l2_distance(query_vector) < 1.0
  ).group_by(
      Document.category
  ).having(
      func.count(Document.id) > 5
  ).all()

   # ORM-style IN subquery for top-k vector results using client.query
   from sqlalchemy import select
   
   # Create subquery to get top-k similar document IDs
   top_ids = select(Document.id).where(
       Document.embedding.l2_distance(query_vector) < 0.5
   ).order_by(
       Document.embedding.l2_distance(query_vector)
   ).limit(10).scalar_subquery()
   
   # Query full documents using IN clause
   results = client.query(Document).filter(
       Document.id.in_(top_ids)
   ).all()

Async Vector Operations
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient

   async def async_vector_operations():
       # Get connection parameters
       host, port, user, password, database = get_connection_params()
       
       client = AsyncClient()
       await client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create vector table using async create_table API
       await client.create_table("async_documents", {
           "id": "int",
           "title": "varchar(200)",
           "embedding": "vecf32(256)"
       }, primary_key="id")

       # Enable IVF indexing
       await client.vector_ops.enable_ivf()

       # Create vector index
       await client.vector_ops.create_ivf(
           "async_documents",
           name="idx_async_embedding",
           column="embedding",
           lists=25,
           op_type="vector_l2_ops"
       )

       # Insert data using async insert API
       await client.insert(AsyncDocument, {
           "id": 1,
           "title": "Async Document",
           "embedding": np.random.rand(256).astype(np.float32).tolist()
       })

       # Vector similarity search using async vector_query API
       query_vector = np.random.rand(256).astype(np.float32).tolist()
       results = await client.vector_ops.similarity_search(
           "async_documents",
           vector_column="embedding",
           query_vector=query_vector,
           limit=3,
           distance_type="l2"
       )

       print("Async Vector Search Results:")
       for result in results.rows:
           print(f"  {result[1]} (Distance: {result[-1]:.4f})")

       # Clean up
       await client.drop_table("async_documents")
       await client.disconnect()

   asyncio.run(async_vector_operations())

ORM with Vector Types
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Column, Integer, String, Text
   from matrixone.orm import declarative_base
   from matrixone.sqlalchemy_ext import create_vector_column

   # Define ORM models with vector columns
   Base = declarative_base()

   class Document(Base):
       __tablename__ = 'orm_documents'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(200), nullable=False)
       content = Column(Text)
       category = Column(String(50))
       embedding = create_vector_column(384, "f32")  # 384-dimensional f32 vector

   # Create table using ORM model
   client.create_table(Document)

   # Insert data using client API
   client.insert(Document, {
       "title": "ORM Document",
       "content": "This is a document created using ORM",
       "category": "tutorial",
       "embedding": np.random.rand(384).astype(np.float32).tolist()
   })

   # Query using client API with filtering
   documents = client.query(Document).filter(Document.category == "tutorial").all()
   for doc in documents:
       print(f"Document: {doc.title}, Category: {doc.category}")

   # Clean up
   client.drop_table(Document)

Vector Index Types and Performance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MatrixOne supports different vector index types for different use cases:

.. code-block:: python

   # IVF Index - Good for large datasets (first argument is positional)
   client.vector_ops.create_ivf(
       "large_dataset",  # table name - positional argument
       name="idx_ivf_large",
       column="embedding",
       lists=1000,  # More lists for larger datasets
       op_type="vector_l2_ops"
   )

   # IVF Index with cosine distance
   client.vector_ops.create_ivf(
       "recommendations",  # table name - positional argument
       name="idx_ivf_cosine",
       column="features",
       lists=100,
       op_type="vector_cosine_ops"
   )

   # IVF Index with inner product
   client.vector_ops.create_ivf(
       "similarity",  # table name - positional argument
       name="idx_ivf_inner",
       column="vectors",
       lists=50,
       op_type="vector_inner_product_ops"
   )

Vector Data Management
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import func
   
   # Update vector data using client.query
   doc = client.query(Document).filter(Document.id == 1).first()
   if doc:
       doc.embedding = new_embedding_vector
       # Note: Changes are automatically committed with client.query
   
   # Or use bulk update
   client.query(Document).filter(Document.id == 1).update(
       {Document.embedding: new_embedding_vector}
   )

   # Delete vector data using client.query
   client.query(Document).filter(Document.id == 1).delete()

   # Query vector data with conditions
   results = client.query(Document).filter(Document.id > 5).all()
   for doc in results:
       print(f"Document: {doc.title}")

   # Get vector statistics using client.query
   total_docs = client.query(Document).select(
       func.count(Document.id)
   ).scalar()
   print(f"Total documents: {total_docs}")

Performance Optimization
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Batch operations for better performance
   large_batch = []
   for i in range(1000):
       large_batch.append({
           "id": i,
           "title": f"Document {i}",
           "embedding": np.random.rand(384).astype(np.float32).tolist()
       })

   # Use batch_insert for large datasets
   client.batch_insert(Document, large_batch)

   # Optimize index parameters for your use case
   client.vector_ops.create_ivf(
       "documents",
       name="idx_optimized",
       column="embedding",
       lists=200,  # Adjust based on dataset size
       op_type="vector_l2_ops"
   )

   # Use appropriate distance functions
   # - L2: Good for general similarity
   # - Cosine: Good for normalized vectors
   # - Inner Product: Good for specific similarity measures

Error Handling
~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone.exceptions import QueryError, ConnectionError

   def robust_vector_operations():
       try:
           # Create vector table with error handling
           try:
               client.create_table("robust_docs", {
                   "id": "int",
                   "embedding": "vecf32(128)"
               }, primary_key="id")
           except QueryError as e:
               print(f"Table creation failed: {e}")

           # Create index with error handling
           try:
               client.vector_ops.create_ivf(
                   "robust_docs",
                   name="idx_robust",
                   column="embedding",
                   lists=10,
                   op_type="vector_l2_ops"
               )
           except QueryError as e:
               print(f"Index creation failed: {e}")

           # Insert data with error handling
           try:
               client.insert(RobustDocument, {
                   "id": 1,
                   "embedding": [0.1] * 128
               })
           except QueryError as e:
               print(f"Data insertion failed: {e}")

           # Vector search with error handling
           try:
               results = client.vector_ops.similarity_search(
                   "robust_docs",
                   vector_column="embedding",
                   query_vector=[0.1] * 128,
                   limit=5,
                   distance_type="l2"
               )
               print(f"Search successful: {len(results.rows)} results")
           except QueryError as e:
               print(f"Vector search failed: {e}")

       except ConnectionError as e:
           print(f"Connection failed: {e}")
       finally:
           # Clean up
           try:
               client.drop_table("robust_docs")
           except Exception as e:
               print(f"Cleanup warning: {e}")

   robust_vector_operations()

Best Practices
~~~~~~~~~~~~~~

1. **Choose the right vector type**:
   - Use `vecf32` for memory efficiency
   - Use `vecf64` for higher precision

2. **Optimize index parameters**:
   - More lists for larger datasets
   - Fewer lists for smaller datasets

3. **Use batch operations**:
   - Use `batch_insert` for large datasets
   - Use `batch_update` for bulk updates

4. **Choose appropriate distance functions**:
   - L2 for general similarity
   - Cosine for normalized vectors
   - Inner product for specific measures

5. **Monitor performance**:
   - Use performance logging
   - Monitor query execution times
   - Monitor IVF index health and distribution
   - Optimize based on usage patterns

.. _ivf-index-health-monitoring:

IVF Index Health Monitoring
~~~~~~~~~~~~~~~~~~~~~~~~~~~

⭐ **PRODUCTION CRITICAL** ⭐

Monitor IVF index health to ensure optimal performance. The ``get_ivf_stats`` method is **critical** for evaluating 
index quality and determining when to rebuild indexes.

**Why IVF Stats Matter:**

* Evaluate index balance - unbalanced indexes lead to poor query performance
* Monitor cluster distribution - identify hot spots and load imbalance
* Determine rebuild timing - know when index quality has degraded
* Capacity planning - understand how data distributes across centroids

Basic Usage
^^^^^^^^^^^

.. code-block:: python

   from matrixone import Client
   
   client = Client()
   client.connect(host="127.0.0.1", port=6001, user="root", password="111", database="test")
   
   # Get IVF index statistics for a table
   # Method 1: Specify column name explicitly
   stats = client.vector_ops.get_ivf_stats("documents", "embedding")
   
   # Method 2: Auto-infer column name (if only one vector column exists)
   stats = client.vector_ops.get_ivf_stats("documents")
   
   # Method 3: Use with ORM model
   from matrixone.orm import declarative_base
   from sqlalchemy import Column, Integer, String
   from matrixone.sqlalchemy_ext import create_vector_column
   
   Base = declarative_base()
   
   class Document(Base):
       __tablename__ = 'documents'
       id = Column(Integer, primary_key=True)
       title = Column(String(200))
       embedding = create_vector_column(384, 'f32')
   
   stats = client.vector_ops.get_ivf_stats(Document, "embedding")

Understanding the Return Value
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``get_ivf_stats`` method returns a comprehensive dictionary with the following structure:

**Example Return Value:**

.. code-block:: python

   {
       'database': 'test',
       'table_name': 'documents',
       'column_name': 'embedding',
       'index_tables': {
           'metadata': '__mo_index_ivf_0012e2e4-0153-7bd7-acf0-1f32def60681',
           'centroids': '__mo_index_ivf_0012e2e4-0153-7bd7-acf0-1f32def60682',
           'entries': '__mo_index_ivf_0012e2e4-0153-7bd7-acf0-1f32def60683'
       },
       'distribution': {
           'centroid_id': [0, 1, 2, 3, 4],
           'centroid_count': [8, 5, 7, 6, 4],
           'centroid_version': [0, 0, 0, 0, 0]
       }
   }

**Field Descriptions:**

* **database**: Name of the database containing the table
* **table_name**: Name of the table with the IVF index
* **column_name**: Name of the vector column being indexed
* **index_tables**: Internal IVF index table names (metadata, centroids, entries)
* **distribution**: The most important section for monitoring:
  
  * **centroid_id**: List of centroid cluster IDs
  * **centroid_count**: Number of vectors assigned to each centroid (critical for balance analysis)
  * **centroid_version**: Version number of each centroid (usually all 0 for stable indexes)

Analyzing Index Balance
^^^^^^^^^^^^^^^^^^^^^^^^

**Key Metrics to Evaluate:**

.. code-block:: python

   # Get statistics
   stats = client.vector_ops.get_ivf_stats("documents", "embedding")
   
   # Extract distribution data
   distribution = stats['distribution']
   centroid_ids = distribution['centroid_id']
   centroid_counts = distribution['centroid_count']
   centroid_versions = distribution['centroid_version']
   
   # Calculate balance metrics
   total_centroids = len(centroid_ids)
   total_vectors = sum(centroid_counts)
   min_count = min(centroid_counts) if centroid_counts else 0
   max_count = max(centroid_counts) if centroid_counts else 0
   avg_count = total_vectors / total_centroids if total_centroids > 0 else 0
   
   # Balance ratio: ideal is 1.0, higher values indicate imbalance
   balance_ratio = max_count / min_count if min_count > 0 else float('inf')
   
   # Standard deviation: measure of distribution spread
   import statistics
   std_dev = statistics.stdev(centroid_counts) if len(centroid_counts) > 1 else 0
   
   print("=" * 60)
   print("IVF INDEX HEALTH REPORT")
   print("=" * 60)
   print(f"Database: {stats['database']}")
   print(f"Table: {stats['table_name']}")
   print(f"Column: {stats['column_name']}")
   print(f"\nIndex Tables:")
   print(f"  - Metadata: {stats['index_tables']['metadata']}")
   print(f"  - Centroids: {stats['index_tables']['centroids']}")
   print(f"  - Entries: {stats['index_tables']['entries']}")
   print(f"\nDistribution Metrics:")
   print(f"  - Total Centroids: {total_centroids}")
   print(f"  - Total Vectors: {total_vectors}")
   print(f"  - Vectors per Centroid (avg): {avg_count:.2f}")
   print(f"  - Min Vectors in Centroid: {min_count}")
   print(f"  - Max Vectors in Centroid: {max_count}")
   print(f"  - Balance Ratio (max/min): {balance_ratio:.2f}")
   print(f"  - Standard Deviation: {std_dev:.2f}")
   print(f"\nCentroid Details:")
   for cid, count, version in zip(centroid_ids, centroid_counts, centroid_versions):
       percentage = (count / total_vectors * 100) if total_vectors > 0 else 0
       bar = "█" * int(count / max_count * 40) if max_count > 0 else ""
       print(f"  Centroid {cid:3d}: {count:5d} vectors ({percentage:5.1f}%) {bar}")
   print("=" * 60)

**Example Output:**

.. code-block:: text

   ============================================================
   IVF INDEX HEALTH REPORT
   ============================================================
   Database: test
   Table: documents
   Column: embedding
   
   Index Tables:
     - Metadata: __mo_index_ivf_0012e2e4-0153-7bd7-acf0-1f32def60681
     - Centroids: __mo_index_ivf_0012e2e4-0153-7bd7-acf0-1f32def60682
     - Entries: __mo_index_ivf_0012e2e4-0153-7bd7-acf0-1f32def60683
   
   Distribution Metrics:
     - Total Centroids: 5
     - Total Vectors: 30
     - Vectors per Centroid (avg): 6.00
     - Min Vectors in Centroid: 4
     - Max Vectors in Centroid: 8
     - Balance Ratio (max/min): 2.00
     - Standard Deviation: 1.58
   
   Centroid Details:
     Centroid   0:     8 vectors ( 26.7%) ████████████████████████████████████████
     Centroid   1:     5 vectors ( 16.7%) █████████████████████████
     Centroid   2:     7 vectors ( 23.3%) ███████████████████████████████████
     Centroid   3:     6 vectors ( 20.0%) ██████████████████████████████
     Centroid   4:     4 vectors ( 13.3%) ████████████████████
   ============================================================

Health Check and Decision Making
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Automated Health Checks:**

.. code-block:: python

   def check_ivf_index_health(client, table_name, column_name, expected_lists):
       """
       Comprehensive IVF index health check with recommendations.
       
       Args:
           client: MatrixOne client instance
           table_name: Name of the table
           column_name: Name of the vector column
           expected_lists: Expected number of centroids (from index creation)
       
       Returns:
           Dict with health status and recommendations
       """
       stats = client.vector_ops.get_ivf_stats(table_name, column_name)
       
       distribution = stats['distribution']
       centroid_counts = distribution['centroid_count']
       
       if not centroid_counts:
           return {
               'status': 'ERROR',
               'message': 'No centroids found - index may be corrupted',
               'action': 'REBUILD_REQUIRED'
           }
       
       total_centroids = len(centroid_counts)
       total_vectors = sum(centroid_counts)
       min_count = min(centroid_counts)
       max_count = max(centroid_counts)
       avg_count = total_vectors / total_centroids
       balance_ratio = max_count / min_count if min_count > 0 else float('inf')
       
       issues = []
       warnings = []
       
       # Check 1: Centroid count mismatch
       if total_centroids != expected_lists:
           issues.append(
               f"Centroid count mismatch: expected {expected_lists}, found {total_centroids}"
           )
       
       # Check 2: Severe imbalance (>3x ratio)
       if balance_ratio > 3.0:
           issues.append(
               f"Severe load imbalance: max/min ratio is {balance_ratio:.2f} (threshold: 3.0)"
           )
       # Check 3: Moderate imbalance (2-3x ratio)
       elif balance_ratio > 2.0:
           warnings.append(
               f"Moderate load imbalance: max/min ratio is {balance_ratio:.2f} (threshold: 2.0)"
           )
       
       # Check 4: Empty centroids
       empty_centroids = sum(1 for c in centroid_counts if c == 0)
       if empty_centroids > 0:
           warnings.append(f"{empty_centroids} empty centroids found")
       
       # Check 5: Sparse population (<5 vectors per centroid on average)
       if avg_count < 5:
           warnings.append(
               f"Sparse population: {avg_count:.1f} vectors/centroid (consider reducing lists parameter)"
           )
       
       # Determine overall status
       if issues:
           status = 'UNHEALTHY'
           action = 'REBUILD_REQUIRED'
       elif warnings:
           status = 'WARNING'
           action = 'MONITOR'
       else:
           status = 'HEALTHY'
           action = 'NONE'
       
       return {
           'status': status,
           'action': action,
           'metrics': {
               'total_centroids': total_centroids,
               'total_vectors': total_vectors,
               'balance_ratio': balance_ratio,
               'avg_vectors_per_centroid': avg_count,
               'min_count': min_count,
               'max_count': max_count
           },
           'issues': issues,
           'warnings': warnings
       }

   # Usage example
   health = check_ivf_index_health(
       client, 
       table_name="documents", 
       column_name="embedding",
       expected_lists=100
   )
   
   print(f"Status: {health['status']}")
   print(f"Action: {health['action']}")
   
   if health['issues']:
       print("\n❌ Critical Issues:")
       for issue in health['issues']:
           print(f"   - {issue}")
   
   if health['warnings']:
       print("\n⚠️  Warnings:")
       for warning in health['warnings']:
           print(f"   - {warning}")
   
   if health['status'] == 'HEALTHY':
       print("\n✅ Index is healthy and well-balanced!")

When to Rebuild the IVF Index
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Rebuild Triggers:**

1. **Centroid Count Mismatch**: Actual centroids don't match expected ``lists`` parameter
2. **Severe Imbalance**: Balance ratio > 3.0 (one centroid has 3x more vectors than another)
3. **Performance Degradation**: Query times increasing despite similar data volume
4. **Major Data Changes**: After bulk inserts (>20% of data), updates, or deletes
5. **Empty Centroids**: Multiple centroids with zero vectors assigned
6. **Very Sparse or Dense**: Average vectors per centroid < 5 or > 1000

**Rebuild Process:**

.. code-block:: python

   # Check health first
   stats = client.vector_ops.get_ivf_stats("documents", "embedding")
   balance_ratio = max(stats['distribution']['centroid_count']) / min(stats['distribution']['centroid_count'])
   
   if balance_ratio > 3.0:
       print("⚠️  Index needs rebuilding due to poor balance")
       
       # Step 1: Drop existing IVF index
       client.vector_ops.drop("documents", "idx_embedding_ivf")
       
       # Step 2: Recreate with optimal parameters
       # Recommended: lists = sqrt(total_vectors) to 4*sqrt(total_vectors)
       total_vectors = sum(stats['distribution']['centroid_count'])
       import math
       optimal_lists = int(math.sqrt(total_vectors) * 2)
       
       client.vector_ops.create_ivf(
           "documents",
           name="idx_embedding_ivf",
           column="embedding",
           lists=optimal_lists,
           op_type="vector_l2_ops"
       )
       
       # Step 3: Verify new index
       new_stats = client.vector_ops.get_ivf_stats("documents", "embedding")
       new_balance = max(new_stats['distribution']['centroid_count']) / min(new_stats['distribution']['centroid_count'])
       print(f"✅ Index rebuilt. New balance ratio: {new_balance:.2f}")

Best Practices for IVF Index Monitoring
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. **Regular Monitoring**: Check index health weekly or after major data operations
2. **Set Alerts**: Monitor balance ratio and trigger alerts when > 2.5
3. **Track Trends**: Log stats over time to identify degradation patterns
4. **Right-size Lists**: Use ``lists = sqrt(N)`` to ``4*sqrt(N)`` where N is total vectors
5. **Rebuild Proactively**: Don't wait for severe degradation - rebuild at balance ratio > 2.5
6. **Monitor After Changes**: Always check stats after bulk operations

**Production Monitoring Example:**

.. code-block:: python

   import time
   from datetime import datetime
   
   def monitor_ivf_health(client, table_name, column_name, check_interval=3600):
       """
       Continuous IVF index health monitoring.
       
       Args:
           client: MatrixOne client
           table_name: Table to monitor
           column_name: Vector column to monitor
           check_interval: Seconds between checks (default: 1 hour)
       """
       while True:
           try:
               stats = client.vector_ops.get_ivf_stats(table_name, column_name)
               counts = stats['distribution']['centroid_count']
               
               if counts:
                   balance_ratio = max(counts) / min(counts) if min(counts) > 0 else float('inf')
                   total_vectors = sum(counts)
                   
                   log_entry = {
                       'timestamp': datetime.now().isoformat(),
                       'total_centroids': len(counts),
                       'total_vectors': total_vectors,
                       'balance_ratio': balance_ratio,
                       'status': 'OK' if balance_ratio <= 2.5 else 'WARNING' if balance_ratio <= 3.5 else 'CRITICAL'
                   }
                   
                   print(f"[{log_entry['timestamp']}] {table_name}.{column_name}: "
                         f"Balance={balance_ratio:.2f}, Vectors={total_vectors}, "
                         f"Status={log_entry['status']}")
                   
                   # Alert on critical status
                   if log_entry['status'] == 'CRITICAL':
                       print(f"🚨 ALERT: Index on {table_name}.{column_name} needs immediate attention!")
                       # Send notification (email, Slack, etc.)
               
               time.sleep(check_interval)
               
           except Exception as e:
               print(f"Error monitoring index: {e}")
               time.sleep(check_interval)
   
   # Run in background thread or separate process
   # monitor_ivf_health(client, "documents", "embedding", check_interval=3600)

Usage in Transactions
^^^^^^^^^^^^^^^^^^^^^^

You can also use ``get_ivf_stats`` within transaction contexts:

.. code-block:: python

   # Within a transaction
   with client.transaction() as tx:
       stats = tx.vector_ops.get_ivf_stats("documents", "embedding")
       
       # Make decisions based on stats
       balance_ratio = max(stats['distribution']['centroid_count']) / min(stats['distribution']['centroid_count'])
       
       if balance_ratio > 3.0:
           # Perform rebuild within transaction
           tx.vector_ops.drop("documents", "idx_embedding_ivf")
           tx.vector_ops.create_ivf("documents", "idx_embedding_ivf", "embedding", lists=50)
   
   # Transaction commits automatically if successful

6. **Handle errors gracefully**:
   - Always use try-catch blocks
   - Provide meaningful error messages
   - Clean up resources properly

Pinecone-Compatible Interface
-----------------------------

MatrixOne provides a Pinecone-compatible interface for easy migration from Pinecone:

.. code-block:: python

   from matrixone import Client
   from matrixone.search_vector_index import PineconeCompatibleIndex

   client = Client()
   client.connect(host="127.0.0.1", port=6001, user="root", password="111", database="test")

   # Create Pinecone-compatible index
   index = PineconeCompatibleIndex(
       client=client,
       table_name="documents",
       vector_column="embedding",
       dimension=384
   )

   # Pinecone-style operations
   index.upsert([
       {"id": "1", "values": [0.1, 0.2, 0.3] * 128, "metadata": {"title": "Document 1"}},
       {"id": "2", "values": [0.4, 0.5, 0.6] * 128, "metadata": {"title": "Document 2"}}
   ])

   # Query with Pinecone-style interface
   results = index.query(
       vector=[0.1, 0.2, 0.3] * 128,
       top_k=5,
       include_metadata=True
   )

Next Steps
----------

* Read the :doc:`api/vector_manager` for detailed vector query API
* Check out the :doc:`api/vector_index` for vector index management
* Learn about :doc:`orm_guide` for ORM patterns with vectors
* Check out the :doc:`examples` for comprehensive usage examples