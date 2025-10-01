Vector Search Guide
===================

This guide covers comprehensive vector search and indexing capabilities in the MatrixOne Python SDK, including modern vector operations for AI applications.

Overview
--------

MatrixOne provides powerful vector operations designed for modern AI and machine learning applications:

* **Modern Vector API**: High-level vector operations with `vector_ops` and `vector_query` managers
* **Multiple Index Types**: HNSW and IVF indexes for different performance characteristics
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
   from sqlalchemy.orm import sessionmaker
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

   # Create session
   Session = sessionmaker(bind=client.get_sqlalchemy_engine())
   session = Session()

   # Insert data using ORM
   doc = Document(
       title="ORM Document",
       content="This is a document created using ORM",
       category="tutorial",
       embedding=np.random.rand(384).astype(np.float32).tolist()
   )
   session.add(doc)
   session.commit()

   # Query using ORM with filtering
   documents = session.query(Document).filter(Document.category == "tutorial").all()
   for doc in documents:
       print(f"Document: {doc.title}, Category: {doc.category}")

   # Clean up
   client.drop_table(Document)
   session.close()

Vector Index Types and Performance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MatrixOne supports different vector index types for different use cases:

.. code-block:: python

   # IVF Index - Good for large datasets
   client.vector_ops.create_ivf(
       "large_dataset",
       name="idx_ivf_large",
       column="embedding",
       lists=1000,  # More lists for larger datasets
       op_type="vector_l2_ops"
   )

   # IVF Index with cosine distance
   client.vector_ops.create_ivf(
       "recommendations",
       name="idx_ivf_cosine",
       column="features",
       lists=100,
       op_type="vector_cosine_ops"
   )

   # IVF Index with inner product
   client.vector_ops.create_ivf(
       "similarity",
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
   - Optimize based on usage patterns

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