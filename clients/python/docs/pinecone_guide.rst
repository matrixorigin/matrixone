Pinecone-Compatible Index Guide
===============================

This guide covers the Pinecone-compatible index interface in the MatrixOne Python SDK, providing a familiar API for users migrating from Pinecone or building cross-platform vector applications.

Overview
--------

The Pinecone-compatible index interface provides:

* **Familiar API**: Drop-in replacement for Pinecone index operations
* **Vector Operations**: Query, upsert, delete, and fetch operations
* **Metadata Filtering**: JSON-based filtering with logical operators
* **Namespace Support**: Multi-tenant data organization
* **Async Operations**: Full async/await support for high-performance applications
* **Statistics**: Index statistics and health monitoring

Getting Started
---------------

Basic Setup
~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params

   # Connect to MatrixOne
   connection_params = get_connection_params()
   client = Client(*connection_params)
   client.connect(*connection_params)

   # Get Pinecone-compatible index
   index = client.get_pinecone_index(
       table_name="documents",
       vector_column="embedding",
       id_column="id"  # Optional, defaults to 'id'
   )

Creating Tables for Pinecone Index
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Create table with vector column
   client.execute("""
       CREATE TABLE documents (
           id VARCHAR(50) PRIMARY KEY,
           title VARCHAR(200),
           content TEXT,
           embedding vecf32(384),
           metadata JSON
       )
   """)

   # Create vector index
   client.vector_index.create_ivf(
       table_name="documents",
       name="idx_documents_embedding",
       column="embedding",
       lists=100
   )

Basic Operations
----------------

Upsert Operations
~~~~~~~~~~~~~~~~~

Insert or update vectors with metadata:

.. code-block:: python

   # Single vector upsert
   index.upsert(
       vectors=[{
           "id": "doc1",
           "values": [0.1, 0.2, 0.3, ...],  # 384-dimensional vector
           "metadata": {
               "title": "Introduction to AI",
               "category": "technology",
               "author": "John Doe"
           }
       }]
   )

   # Batch upsert
   vectors = [
       {
           "id": "doc2",
           "values": [0.4, 0.5, 0.6, ...],
           "metadata": {"title": "Machine Learning Basics", "category": "education"}
       },
       {
           "id": "doc3", 
           "values": [0.7, 0.8, 0.9, ...],
           "metadata": {"title": "Deep Learning", "category": "technology"}
       }
   ]
   index.upsert(vectors=vectors)

Query Operations
~~~~~~~~~~~~~~~~

Search for similar vectors:

.. code-block:: python

   # Basic similarity search
   results = index.query(
       vector=[0.1, 0.2, 0.3, ...],  # Query vector
       top_k=10,
       include_metadata=True,
       include_values=False
   )

   print(f"Found {len(results.matches)} matches")
   for match in results.matches:
       print(f"ID: {match.id}, Score: {match.score}")
       print(f"Metadata: {match.metadata}")

   # Query with metadata filtering
   results = index.query(
       vector=[0.1, 0.2, 0.3, ...],
       top_k=5,
       filter={
           "category": {"$eq": "technology"},
           "author": {"$ne": "Jane Smith"}
       },
       include_metadata=True
   )

Delete Operations
~~~~~~~~~~~~~~~~~

Remove vectors by ID or metadata:

.. code-block:: python

   # Delete by ID
   index.delete(ids=["doc1", "doc2"])

   # Delete by metadata filter
   index.delete(
       filter={
           "category": {"$eq": "outdated"},
           "created_at": {"$lt": "2023-01-01"}
       }
   )

   # Delete all vectors in namespace
   index.delete(delete_all=True)

Fetch Operations
~~~~~~~~~~~~~~~~

Retrieve vectors by ID:

.. code-block:: python

   # Fetch specific vectors
   results = index.fetch(ids=["doc1", "doc2", "doc3"])

   for vector_id, vector_data in results.vectors.items():
       print(f"ID: {vector_id}")
       print(f"Values: {vector_data.values[:5]}...")  # Show first 5 dimensions
       print(f"Metadata: {vector_data.metadata}")

Advanced Filtering
------------------

Metadata Filtering
~~~~~~~~~~~~~~~~~~

The Pinecone-compatible interface supports complex metadata filtering using JSON-based syntax:

.. code-block:: python

   # Equality and inequality
   filter1 = {"category": {"$eq": "technology"}}
   filter2 = {"price": {"$ne": 0}}

   # Comparison operators
   filter3 = {"rating": {"$gt": 4.0}}
   filter4 = {"age": {"$gte": 18}}
   filter5 = {"score": {"$lt": 100}}
   filter6 = {"count": {"$lte": 50}}

   # Array operations
   filter7 = {"tags": {"$in": ["ai", "ml", "python"]}}
   filter8 = {"status": {"$nin": ["deleted", "archived"]}}

   # Logical operators
   filter9 = {
       "$and": [
           {"category": {"$eq": "technology"}},
           {"rating": {"$gt": 4.0}},
           {"$or": [
               {"author": {"$eq": "John Doe"}},
               {"author": {"$eq": "Jane Smith"}}
           ]}
       ]
   }

   # Complex nested filtering
   filter10 = {
       "$and": [
           {"category": {"$in": ["technology", "science"]}},
           {"$or": [
               {"publication_year": {"$gte": 2020}},
               {"is_featured": {"$eq": True}}
           ]},
           {"tags": {"$nin": ["deprecated"]}}
       ]
   }

   # Use filters in queries
   results = index.query(
       vector=[0.1, 0.2, 0.3, ...],
       top_k=10,
       filter=filter9,
       include_metadata=True
   )

Namespace Support
~~~~~~~~~~~~~~~~~

Organize data using namespaces:

.. code-block:: python

   # Upsert to specific namespace
   index.upsert(
       vectors=[{
           "id": "doc1",
           "values": [0.1, 0.2, 0.3, ...],
           "metadata": {"title": "Document 1"}
       }],
       namespace="user_123"
   )

   # Query within namespace
   results = index.query(
       vector=[0.1, 0.2, 0.3, ...],
       top_k=5,
       namespace="user_123",
       include_metadata=True
   )

   # Delete from namespace
   index.delete(ids=["doc1"], namespace="user_123")

Async Operations
----------------

Full async/await support for high-performance applications:

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient

   async def async_vector_operations():
       # Connect asynchronously
       connection_params = get_connection_params()
       async_client = AsyncClient(*connection_params)
       await async_client.connect(*connection_params)

       # Get async Pinecone index
       index = async_client.get_pinecone_index(
           table_name="documents",
           vector_column="embedding"
       )

       # Async upsert
       await index.upsert_async(
           vectors=[{
               "id": "async_doc1",
               "values": [0.1, 0.2, 0.3, ...],
               "metadata": {"title": "Async Document"}
           }]
       )

       # Async query
       results = await index.query_async(
           vector=[0.1, 0.2, 0.3, ...],
           top_k=10,
           include_metadata=True
       )

       # Async delete
       await index.delete_async(ids=["async_doc1"])

       await async_client.disconnect()

   # Run async operations
   asyncio.run(async_vector_operations())

Index Statistics
----------------

Monitor index health and performance:

.. code-block:: python

   # Get index statistics
   stats = index.describe_index_stats()

   print(f"Total vectors: {stats.total_vector_count}")
   print(f"Dimension: {stats.dimension}")
   print(f"Index fullness: {stats.index_fullness}")
   print(f"Namespaces: {len(stats.namespaces)}")

   # Namespace-specific statistics
   for namespace, ns_stats in stats.namespaces.items():
       print(f"Namespace '{namespace}': {ns_stats.vector_count} vectors")

   # Async statistics
   async_stats = await index.describe_index_stats_async()
   print(f"Async stats: {async_stats.total_vector_count} vectors")

Error Handling
--------------

Robust error handling for production applications:

.. code-block:: python

   from matrixone.exceptions import QueryError, ConnectionError

   try:
       # Vector operations
       results = index.query(
           vector=[0.1, 0.2, 0.3, ...],
           top_k=10,
           filter={"invalid_field": {"$invalid_op": "value"}}
       )
   except QueryError as e:
       print(f"Query error: {e}")
   except ConnectionError as e:
       print(f"Connection error: {e}")
   except Exception as e:
       print(f"Unexpected error: {e}")

   # Validate vector dimensions
   def validate_vector(vector, expected_dim):
       if len(vector) != expected_dim:
           raise ValueError(f"Vector dimension {len(vector)} != expected {expected_dim}")
       return True

   # Safe upsert with validation
   def safe_upsert(index, vectors, expected_dim=384):
       for vector in vectors:
           validate_vector(vector["values"], expected_dim)
       
       return index.upsert(vectors=vectors)

Performance Optimization
------------------------

Best practices for optimal performance:

.. code-block:: python

   # Batch operations for better performance
   def batch_upsert(index, vectors, batch_size=100):
       for i in range(0, len(vectors), batch_size):
           batch = vectors[i:i + batch_size]
           index.upsert(vectors=batch)

   # Use appropriate top_k values
   # For search: top_k=10-100
   # For recommendations: top_k=5-20
   # For clustering: top_k=1-5

   # Optimize metadata filtering
   # Use indexed fields for filtering
   # Avoid complex nested filters when possible
   # Use $in instead of multiple $eq with $or

   # Connection pooling for high-throughput applications
   from matrixone import Client
   import threading

   class VectorService:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.index = self.client.get_pinecone_index(
               table_name="documents",
               vector_column="embedding"
           )
           self.lock = threading.Lock()

       def thread_safe_query(self, vector, top_k=10):
           with self.lock:
               return self.index.query(vector=vector, top_k=top_k)

Migration from Pinecone
-----------------------

Easy migration from Pinecone to MatrixOne:

.. code-block:: python

   # Original Pinecone code
   # import pinecone
   # pinecone.init(api_key="your-api-key", environment="your-environment")
   # index = pinecone.Index("your-index-name")

   # MatrixOne equivalent
   from matrixone import Client
   client = Client(*get_connection_params())
   client.connect(*get_connection_params())
   index = client.get_pinecone_index(
       table_name="your-table-name",
       vector_column="your-vector-column"
   )

   # Same API calls work identically
   index.upsert(vectors=vectors)
   results = index.query(vector=query_vector, top_k=10)
   index.delete(ids=ids_to_delete)

   # Additional MatrixOne benefits
   # - No API rate limits
   # - Full SQL access to your data
   # - Advanced filtering capabilities
   # - Cost-effective storage

Integration Examples
--------------------

Real-world integration patterns:

.. code-block:: python

   # Document search application
   class DocumentSearch:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.index = self.client.get_pinecone_index(
               table_name="documents",
               vector_column="embedding"
           )

       def add_document(self, doc_id, content, embedding, metadata=None):
           self.index.upsert(vectors=[{
               "id": doc_id,
               "values": embedding,
               "metadata": {
                   "content": content,
                   "timestamp": datetime.now().isoformat(),
                   **(metadata or {})
               }
           }])

       def search_documents(self, query_embedding, filters=None, top_k=10):
           results = self.index.query(
               vector=query_embedding,
               top_k=top_k,
               filter=filters,
               include_metadata=True
           )
           return [match.metadata for match in results.matches]

   # Recommendation system
   class RecommendationEngine:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.index = self.client.get_pinecone_index(
               table_name="products",
               vector_column="feature_vector"
           )

       def get_recommendations(self, user_id, user_preferences, top_k=20):
           # Get user's preference vector
           user_vector = self.get_user_preference_vector(user_preferences)
           
           # Find similar products
           results = self.index.query(
               vector=user_vector,
               top_k=top_k,
               filter={"status": {"$eq": "active"}},
               include_metadata=True
           )
           
           return [match.metadata for match in results.matches]

   # Multi-tenant application
   class MultiTenantVectorDB:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.index = self.client.get_pinecone_index(
               table_name="tenant_data",
               vector_column="embedding"
           )

       def tenant_upsert(self, tenant_id, vectors):
           self.index.upsert(vectors=vectors, namespace=tenant_id)

       def tenant_query(self, tenant_id, query_vector, top_k=10):
           return self.index.query(
               vector=query_vector,
               top_k=top_k,
               namespace=tenant_id,
               include_metadata=True
           )

Troubleshooting
---------------

Common issues and solutions:

**Vector dimension mismatch**
   - Ensure all vectors have the same dimension as defined in the table schema
   - Use validation functions to check vector dimensions before upsert

**Filter syntax errors**
   - Use proper JSON syntax for filters
   - Validate filter structure before querying
   - Check supported operators: $eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $and, $or

**Performance issues**
   - Use batch operations for large datasets
   - Optimize metadata filtering
   - Consider using appropriate vector index types (IVF vs HNSW)

**Connection issues**
   - Verify MatrixOne server is running
   - Check connection parameters
   - Ensure proper network connectivity

For more information, see the :doc:`vector_guide` and :doc:`api/vector_index`.
