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

For detailed information about the Pinecone-compatible index interface, see the :doc:`pinecone_guide`.

Modern Vector Operations
------------------------

The MatrixOne Python SDK provides a modern, high-level API for vector operations that simplifies common AI workflows.

Creating Vector Tables
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params

   # Get connection parameters
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Create vector table using create_table API
   client.create_table("documents", {
       "id": "int",
       "title": "varchar(200)",
       "content": "text",
       "embedding": "vecf32(384)",  # 384-dimensional f32 vector
       "metadata": "json"
   }, primary_key="id")

   # Create another vector table for products
   client.create_table("products", {
       "id": "int",
       "name": "varchar(200)",
       "description": "text",
       "features": "vecf64(512)",  # 512-dimensional f64 vector
       "category": "varchar(50)"
   }, primary_key="id")

Vector Index Management
~~~~~~~~~~~~~~~~~~~~~~~

MatrixOne provides powerful vector index management through the `vector_ops` API:

.. code-block:: python

   # Enable IVF indexing
   client.vector_ops.enable_ivf()

   # Create IVF index for similarity search
   client.vector_ops.create_ivf(
       table_name="documents",
       name="idx_embedding_ivf",
       column="embedding",
       lists=50,                    # Number of clusters
       op_type="vector_l2_ops"      # Distance function
   )

   # Create another IVF index with different parameters
   client.vector_ops.create_ivf(
       table_name="products",
       name="idx_features_ivf",
       column="features",
       lists=100,                   # More clusters for larger datasets
       op_type="vector_cosine_ops"  # Cosine distance
   )

   # List all vector indexes
   indexes = client.vector_ops.list_indexes("documents")
   print("Vector indexes:", indexes)

   # Drop an index
   client.vector_ops.drop_index("documents", "idx_embedding_ivf")

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
       table_name="documents",
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
       table_name="documents",
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
       table_name="documents",
       vector_column="embedding",
       query_vector=query_vector,
       limit=10,
       offset=20,  # Skip first 20 results
       distance_type="l2"
   )

   # Search with custom select columns
   results = client.vector_ops.similarity_search(
       table_name="documents",
       vector_column="embedding",
       query_vector=query_vector,
       limit=5,
       distance_type="l2",
       select_columns=["id", "title", "content"]  # Only return these columns
   )

   # Search with metadata filtering
   results = client.vector_ops.similarity_search(
       table_name="documents",
       vector_column="embedding",
       query_vector=query_vector,
       limit=5,
       distance_type="l2",
       where_clause="JSON_EXTRACT(metadata, '$.category') = 'research'"
   )

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
           table_name="async_documents",
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
           table_name="async_documents",
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
       table_name="large_dataset",
       name="idx_ivf_large",
       column="embedding",
       lists=1000,  # More lists for larger datasets
       op_type="vector_l2_ops"
   )

   # IVF Index with cosine distance
   client.vector_ops.create_ivf(
       table_name="recommendations",
       name="idx_ivf_cosine",
       column="features",
       lists=100,
       op_type="vector_cosine_ops"
   )

   # IVF Index with inner product
   client.vector_ops.create_ivf(
       table_name="similarity",
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
       table_name="documents",
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
                   table_name="robust_docs",
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
                   table_name="robust_docs",
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

Next Steps
----------

* Read the :doc:`api/vector_query_manager` for detailed vector query API
* Check out the :doc:`api/vector_manager` for vector index management
* Explore :doc:`pinecone_guide` for Pinecone-compatible interface
* Learn about :doc:`orm_guide` for ORM patterns with vectors
* Check out the :doc:`examples` for comprehensive usage examples