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
   from sqlalchemy import Column, Integer, String, Text, JSON
   from sqlalchemy.ext.declarative import declarative_base
   from matrixone.sqlalchemy_ext import Vectorf32, Vectorf64

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
       embedding = Column(Vectorf32(384))  # 384-dimensional f32 vector
       metadata = Column(JSON)

   class Product(Base):
       __tablename__ = 'products'
       id = Column(Integer, primary_key=True)
       name = Column(String(200))
       description = Column(Text)
       features = Column(Vectorf64(512))  # 512-dimensional f64 vector
       category = Column(String(50))

   # Create tables using models
   client.create_table(Document)
   client.create_table(Product)

   # Alternative: Create tables using create_table API with column definitions
   client.create_table("documents_alt", {
       "id": "int",
       "title": "varchar(200)",
       "content": "text",
       "embedding": "vecf32(384)",  # 384-dimensional f32 vector
       "metadata": "json"
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
   client.insert("documents", {
           "id": 1,
           "title": "AI Research Paper",
           "content": "Advanced artificial intelligence research",
           "embedding": np.random.rand(384).astype(np.float32).tolist(),
       "metadata": '{"category": "research", "year": 2024}'
   })

   # Batch insert multiple documents
   documents = [
       {
           "id": 2,
           "title": "Machine Learning Guide",
           "content": "Comprehensive machine learning tutorial",
           "embedding": np.random.rand(384).astype(np.float32).tolist(),
           "metadata": '{"category": "tutorial", "level": "beginner"}'
       },
       {
           "id": 3,
           "title": "Data Science Handbook",
           "content": "Complete data science reference",
           "embedding": np.random.rand(384).astype(np.float32).tolist(),
           "metadata": '{"category": "reference", "pages": 500}'
       }
   ]

   client.batch_insert("documents", documents)

Vector Similarity Search
~~~~~~~~~~~~~~~~~~~~~~~~

The `vector_query` API provides powerful similarity search capabilities:

.. code-block:: python

   # Perform vector similarity search
   query_vector = np.random.rand(384).astype(np.float32).tolist()
   
   # L2 distance search
   results = client.vector_ops.similarity_search(
       "documents",
       vector_column="embedding",
       query_vector=query_vector,
       limit=5,
       distance_type="l2"
   )

   print("L2 Distance Search Results:")
   for result in results.rows:
       print(f"  {result[1]} (Distance: {result[-1]:.4f})")

   # Cosine distance search
   cosine_results = client.vector_ops.similarity_search(
       "documents",
       vector_column="embedding",
       query_vector=query_vector,
       limit=5,
       distance_type="cosine"
   )

   print("Cosine Distance Search Results:")
   for result in cosine_results.rows:
       print(f"  {result[1]} (Similarity: {1 - result[-1]:.4f})")

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

   # Search with metadata filtering
   results = client.vector_ops.similarity_search(
       "documents",
       vector_column="embedding",
       query_vector=query_vector,
       limit=5,
       distance_type="l2",
       where_conditions=["JSON_EXTRACT(metadata, '$.category') = ?"],
       where_params=["research"]
   )

Complex Vector Queries with Query Builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For complex vector queries, use the query builder with vector functions:

.. code-block:: python

   # Complex vector query with JOIN
   result = client.query("documents d").select(
       "d.id", "d.title", "d.content",
       "l2_distance(d.embedding, ?) as distance"
   ).join(
       "categories c", "d.category_id = c.id"
   ).where(
       "l2_distance(d.embedding, ?) < ?", 
       (query_vector, query_vector, 0.5)
   ).and_where(
       "c.name = ?", "AI"
   ).order_by(
       "l2_distance(d.embedding, ?)", query_vector
   ).limit(10).execute()

   # Vector query with CTE (Common Table Expression)
   result = client.query().select("*").from_(
       """
       WITH similar_docs AS (
           SELECT id, title, l2_distance(embedding, ?) as distance
           FROM documents
           WHERE l2_distance(embedding, ?) < ?
           ORDER BY distance
           LIMIT 20
       )
       SELECT sd.*, d.content
       FROM similar_docs sd
       JOIN documents d ON sd.id = d.id
       """, (query_vector, query_vector, 0.8)
   ).execute()

   # Vector query with aggregation
   result = client.query("documents").select(
       "category",
       "COUNT(*) as doc_count",
       "AVG(l2_distance(embedding, ?)) as avg_distance"
   ).where(
       "l2_distance(embedding, ?) < ?",
       (query_vector, query_vector, 1.0)
   ).group_by("category").having(
       "COUNT(*) > ?", 5
   ).execute()

   # Vector query with subquery
   result = client.query("documents").select("*").where(
       "id IN (SELECT id FROM documents WHERE l2_distance(embedding, ?) < ? ORDER BY l2_distance(embedding, ?) LIMIT 10)",
       (query_vector, 0.5, query_vector)
   ).execute()

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
       await client.insert("async_documents", {
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
   from sqlalchemy.ext.declarative import declarative_base
   from sqlalchemy.orm import sessionmaker
   from matrixone.sqlalchemy_ext import create_vector_column

   # Define ORM models with vector columns
   Base = declarative_base()

   class Document(Base):
       __tablename__ = 'orm_documents'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(200), nullable=False)
       content = Column(Text)
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
       embedding=np.random.rand(384).astype(np.float32).tolist()
   )
   session.add(doc)
   session.commit()

   # Query using ORM
   documents = session.query(Document).all()
   for doc in documents:
       print(f"Document: {doc.title}")

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

   # Update vector data
   client.query("documents").update({
       "embedding": new_embedding_vector
   }).where("id = ?", 1).execute()

   # Delete vector data
   client.query("documents").where("id = ?", 1).delete()

   # Query vector data with conditions
   result = client.query("documents").select("*").where("id > ?", 5).execute()
   for row in result.fetchall():
       print(f"Document: {row[1]}")

   # Get vector statistics
   result = client.query("documents").select("COUNT(*)").execute()
   total_docs = result.fetchall()[0][0]
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
   client.batch_insert("documents", large_batch)

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
               client.insert("robust_docs", {
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