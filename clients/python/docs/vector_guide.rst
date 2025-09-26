Vector Search Guide
===================

This guide covers vector search and indexing capabilities in the MatrixOne Python SDK, including IVF and HNSW vector indexes.

Overview
--------

MatrixOne supports advanced vector operations for AI and machine learning applications:

* **Vector Data Types**: Support for f32 and f64 vector types with configurable dimensions
* **IVF Indexing**: Inverted File (IVF) indexes for efficient approximate nearest neighbor search
* **HNSW Indexing**: Hierarchical Navigable Small Worlds for high-performance vector search
* **Distance Metrics**: L2, cosine, and inner product distance calculations
* **ORM Integration**: Seamless integration with SQLAlchemy models
* **Pinecone-Compatible Interface**: Familiar API for users migrating from Pinecone

For detailed information about the Pinecone-compatible index interface, see the :doc:`pinecone_guide`.

Vector Data Types
-----------------

Defining Vector Columns with ORM
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Column, Integer, String, Text
   from sqlalchemy.ext.declarative import declarative_base
   from matrixone import Client
   from matrixone.sqlalchemy_ext import create_vector_column

   Base = declarative_base()

   class Document(Base):
       __tablename__ = 'documents'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(200), nullable=False)
       content = Column(Text)
       # 384-dimensional f32 vector for embeddings
       embedding = create_vector_column(384, "f32")
       # 512-dimensional f64 vector for features
       features = create_vector_column(512, "f64")

   class Product(Base):
       __tablename__ = 'products'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       name = Column(String(200), nullable=False)
       description = Column(Text)
       # 128-dimensional vector for product features
       feature_vector = create_vector_column(128, "f32")

Working with Vector Data
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params
   import numpy as np

   # Get connection and create client
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Create tables
   client.create_all(Base)

   # Insert documents with vector embeddings using ORM
   from sqlalchemy.orm import sessionmaker
   
   Session = sessionmaker(bind=client.get_sqlalchemy_engine())
   session = Session()
   
   documents_data = [
       Document(
           title='AI Research Paper',
           content='This paper discusses artificial intelligence and machine learning',
           embedding=np.random.rand(384).tolist(),  # Convert numpy array to list
           features=np.random.rand(512).tolist()
       ),
       Document(
           title='Database Optimization',
           content='Techniques for optimizing database performance',
           embedding=np.random.rand(384).tolist(),
           features=np.random.rand(512).tolist()
       ),
       Document(
           title='Web Development Guide',
           content='Best practices for modern web development',
           embedding=np.random.rand(384).tolist(),
           features=np.random.rand(512).tolist()
       )
   ]

   session.add_all(documents_data)
   session.commit()
   session.close()

   print("✓ Inserted documents with vector embeddings using ORM")
   client.disconnect()

IVF Vector Indexing
-------------------

Creating IVF Indexes
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Create table if not exists
   client.create_all(Base)

   # Enable IVF indexing
   client.vector_index.enable_ivf()
   print("✓ IVF indexing enabled")

   # Create IVF index on embedding column
   client.vector_index.create_ivf(
       table_name='documents',
       name='idx_document_embedding',
       column='embedding',
       lists=100,  # Number of clusters (centroids)
       op_type='vector_l2_ops'  # Distance metric
   )
   print("✓ IVF index created on embedding column")

   # Create another IVF index on features column
   client.vector_index.create_ivf(
       table_name='documents',
       name='idx_document_features',
       column='features',
       lists=50,
       op_type='vector_cosine_ops'  # Use cosine distance
   )
   print("✓ IVF index created on features column")

   client.disconnect()

IVF Index Configuration
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Different IVF configurations for different use cases
   configurations = [
       {
           'name': 'fast_search',
           'lists': 50,      # Fewer clusters = faster search, less accuracy
           'probe_limit': 1  # Search fewer clusters
       },
       {
           'name': 'balanced',
           'lists': 100,     # Balanced clusters
           'probe_limit': 5  # Search more clusters for better accuracy
       },
       {
           'name': 'accurate_search',
           'lists': 200,     # More clusters = slower search, better accuracy
           'probe_limit': 10 # Search many clusters for best accuracy
       }
   ]

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   for config in configurations:
       table_name = f"documents_{config['name']}"
       
       # Create table for this configuration
       client.execute(f"""
           CREATE TABLE {table_name} (
               id INT PRIMARY KEY AUTO_INCREMENT,
               title VARCHAR(200),
               embedding VECF32(384)
           )
       """)
       
       # Enable IVF and set probe limit
       client.vector_index.enable_ivf(probe_limit=config['probe_limit'])
       
       # Create IVF index with specific configuration
       client.vector_index.create_ivf(
           table_name=table_name,
           name=f"idx_{config['name']}",
           column='embedding',
           lists=config['lists'],
           op_type='vector_l2_ops'
       )
       
       print(f"✓ Created {config['name']} configuration: lists={config['lists']}, probe_limit={config['probe_limit']}")

   client.disconnect()

HNSW Vector Indexing
--------------------

Creating HNSW Indexes
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Create table for HNSW demo
   client.execute("""
       CREATE TABLE hnsw_documents (
           id INT PRIMARY KEY AUTO_INCREMENT,
           title VARCHAR(200),
           content TEXT,
           embedding VECF32(128)
       )
   """)

   # Enable HNSW indexing
   client.vector_index.enable_hnsw()
   print("✓ HNSW indexing enabled")

   # Create HNSW index
   client.vector_index.create_hnsw(
       table_name='hnsw_documents',
       name='idx_hnsw_embedding',
       column='embedding',
       m=16,                    # Number of bi-directional links for each node
       ef_construction=200,     # Size of dynamic candidate list during construction
       ef_search=50,           # Size of dynamic candidate list during search
       op_type='vector_l2_ops'
   )
   print("✓ HNSW index created")

   # Insert sample data using ORM
   import numpy as np
   from sqlalchemy.orm import sessionmaker
   
   Session = sessionmaker(bind=client.get_sqlalchemy_engine())
   session = Session()
   
   hnsw_docs = [
       HNSWDocument(
           title='HNSW Research Paper',
           content='Research on HNSW algorithm',
           embedding=np.random.rand(128).tolist()
       ),
       HNSWDocument(
           title='Vector Database Guide',
           content='Guide to vector databases',
           embedding=np.random.rand(128).tolist()
       ),
       HNSWDocument(
           title='Machine Learning Basics',
           content='Introduction to ML',
           embedding=np.random.rand(128).tolist()
       ),
       HNSWDocument(
           title='Deep Learning Tutorial',
           content='Deep learning concepts',
           embedding=np.random.rand(128).tolist()
       ),
       HNSWDocument(
           title='AI Applications',
           content='Real-world AI applications',
           embedding=np.random.rand(128).tolist()
       )
   ]

   session.add_all(hnsw_docs)
   session.commit()
   session.close()

   print(f"✓ Inserted {len(hnsw_docs)} documents using ORM")
   client.disconnect()

HNSW Configuration Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Different HNSW configurations for different performance requirements
   hnsw_configs = [
       {
           'name': 'fast_hnsw',
           'm': 8,              # Fewer connections = faster search, less accuracy
           'ef_construction': 100,
           'ef_search': 32
       },
       {
           'name': 'balanced_hnsw',
           'm': 16,             # Balanced configuration
           'ef_construction': 200,
           'ef_search': 64
       },
       {
           'name': 'accurate_hnsw',
           'm': 32,             # More connections = slower search, better accuracy
           'ef_construction': 400,
           'ef_search': 128
       }
   ]

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   client.vector_index.enable_hnsw()

   for config in hnsw_configs:
       table_name = f"hnsw_{config['name']}"
       
       # Create table
       client.execute(f"""
           CREATE TABLE {table_name} (
               id INT PRIMARY KEY AUTO_INCREMENT,
               title VARCHAR(200),
               embedding VECF32(128)
           )
       """)
       
       # Create HNSW index with specific configuration
       client.vector_index.create_hnsw(
           table_name=table_name,
           name=f"idx_{config['name']}",
           column='embedding',
           m=config['m'],
           ef_construction=config['ef_construction'],
           ef_search=config['ef_search'],
           op_type='vector_l2_ops'
       )
       
       print(f"✓ Created {config['name']}: M={config['m']}, EF_CONSTRUCTION={config['ef_construction']}, EF_SEARCH={config['ef_search']}")

   client.disconnect()

Vector Search Operations
------------------------

Query Vector Parameter Formats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `query_vector` parameter in vector search functions supports multiple formats:

**1. List Format (Recommended):**
.. code-block:: python

   import numpy as np
   
   # Generate query vector as list
   query_vector_list = np.random.rand(384).tolist()  # [0.1, 0.2, 0.3, ...]
   
   # Use in vector search
   results = client.vector_query.similarity_search(
   table_name='documents',
   vector_column='embedding',
   query_vector=query_vector_list,  # List format
   limit=5,
   distance_type='l2'
   )

**2. String Format:**

.. code-block:: python

   # Convert list to string format
   query_vector_str = str(query_vector_list)  # '[0.1, 0.2, 0.3, ...]'
   
   # Use in vector search
   results = client.vector_query.similarity_search(
       table_name='documents',
       vector_column='embedding',
       query_vector=query_vector_str,  # String format
       limit=5,
       distance_type='l2'
   )

**3. In ORM Queries:**
.. code-block:: python

   from sqlalchemy.orm import sessionmaker
   from matrixone.sqlalchemy_ext import create_vector_column
   
   # Both formats work in ORM queries
   results_list = session.query(
   Document.id,
   Document.title,
   Document.embedding.l2_distance(query_vector_list).label('distance')
   ).filter(
   Document.embedding.within_distance(query_vector_list, 1.0)
   ).order_by(
   Document.embedding.l2_distance(query_vector_list)
   ).all()  # List format
   
   results_str = session.query(
       Document.id,
       Document.title,
       Document.embedding.l2_distance(query_vector_str).label('distance')
   ).filter(
       Document.embedding.within_distance(query_vector_str, 1.0)
   ).order_by(
       Document.embedding.l2_distance(query_vector_str)
   ).all()  # String format

**4. With VectorColumn Methods:**

.. code-block:: python

   from matrixone.sqlalchemy_ext import VectorColumn
   
   # Both formats work with VectorColumn methods
   session.query(Document).filter(
       Document.embedding.within_distance(query_vector_list, 1.0)  # List format
   ).all()
   
   session.query(Document).filter(
       Document.embedding.within_distance(query_vector_str, 1.0)   # String format
   ).all()

Similarity Search with Client Interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   import numpy as np

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Query vector (in practice, this would be an embedding from your ML model)
   # Can be either a list of floats or a string representation
   query_vector_list = np.random.rand(384).tolist()  # List format: [0.1, 0.2, 0.3, ...]
   query_vector_str = str(query_vector_list)         # String format: '[0.1, 0.2, 0.3, ...]'

   # Perform similarity search using L2 distance with list format
   l2_results = client.vector_query.similarity_search(
       table_name='documents',
       vector_column='embedding',
       query_vector=query_vector_list,  # Using list format
       limit=5,
       distance_type='l2',
       select_columns=['id', 'title', 'content']
   )

   print("L2 Distance Search Results:")
   for result in l2_results:
       doc_id, title, content, distance = result[0], result[1], result[2], result[-1]
       print(f"  Document {doc_id}: {title}")
       print(f"    Content: {content[:50]}...")
       print(f"    L2 Distance: {distance:.4f}")

   # Perform similarity search using cosine distance with string format
   cosine_results = client.vector_query.similarity_search(
       table_name='documents',
       vector_column='embedding',
       query_vector=query_vector_str,  # Using string format
       limit=5,
       distance_type='cosine',
       select_columns=['id', 'title']
   )

   print("\nCosine Distance Search Results:")
   for result in cosine_results:
       doc_id, title, distance = result[0], result[1], result[-1]
       print(f"  Document {doc_id}: {title}")
       print(f"    Cosine Distance: {distance:.4f}")

   # Perform similarity search using inner product
   inner_results = client.vector_query.similarity_search(
       table_name='documents',
       vector_column='embedding',
       query_vector=query_vector,
       limit=5,
       distance_type='inner_product'
   )

   print("\nInner Product Search Results:")
   for result in inner_results:
       doc_id, title, distance = result[0], result[1], result[-1]
       print(f"  Document {doc_id}: {title}")
       print(f"    Inner Product: {distance:.4f}")

   client.disconnect()

Advanced Vector Queries with ORM
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy.orm import sessionmaker
   from sqlalchemy import text
   from matrixone import Client
   import numpy as np

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Get SQLAlchemy engine and create session
   engine = client.get_sqlalchemy_engine()
   Session = sessionmaker(bind=engine)
   session = Session()

   # Query vector in both formats
   query_vector_list = np.random.rand(384).tolist()  # List format
   query_vector_str = str(query_vector_list)         # String format

   try:
       # Complex vector query with filters using ORM (list format)
       result = session.execute(text("""
           SELECT id, title, content,
                  l2_distance(embedding, :query_vector) as distance
           FROM documents
           WHERE l2_distance(embedding, :query_vector) < :max_distance
             AND title LIKE :title_pattern
           ORDER BY distance ASC
           LIMIT :limit_count
       """), {
           'query_vector': query_vector_list,  # Using list format
           'max_distance': 1.0,
           'title_pattern': '%AI%',
           'limit_count': 10
       })

       print("Filtered Vector Search Results:")
       for row in result:
           print(f"  Document {row.id}: {row.title}")
           print(f"    Distance: {row.distance:.4f}")
           print(f"    Content: {row.content[:50]}...")

       # Vector search with aggregation (string format)
       aggregation_result = session.execute(text("""
           SELECT 
               CASE 
                   WHEN l2_distance(embedding, :query_vector) < 0.5 THEN 'Very Similar'
                   WHEN l2_distance(embedding, :query_vector) < 1.0 THEN 'Similar'
                   ELSE 'Different'
               END as similarity_category,
               COUNT(*) as document_count,
               AVG(l2_distance(embedding, :query_vector)) as avg_distance
           FROM documents
           GROUP BY 
               CASE 
                   WHEN l2_distance(embedding, :query_vector) < 0.5 THEN 'Very Similar'
                   WHEN l2_distance(embedding, :query_vector) < 1.0 THEN 'Similar'
                   ELSE 'Different'
               END
           ORDER BY avg_distance ASC
       """), {'query_vector': query_vector_str})  # Using string format

       print("\nSimilarity Distribution:")
       for row in aggregation_result:
           print(f"  {row.similarity_category}: {row.document_count} documents (avg distance: {row.avg_distance:.4f})")

   finally:
       session.close()
       client.disconnect()

Performance Comparison
----------------------

IVF vs HNSW Performance Testing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import time
   import numpy as np
   from matrixone import Client

   def performance_comparison():
       client = Client()
       client.connect(host='localhost', port=6001, user='root', password='111', database='test')

       # Prepare test data
       query_vector = np.random.rand(128).tolist()
       num_iterations = 100

       print("Vector Index Performance Comparison")
       print("=" * 50)

       # Test IVF performance
       print("Testing IVF Index Performance...")
       client.vector_index.enable_ivf(probe_limit=5)
       
       start_time = time.time()
       for _ in range(num_iterations):
           results = client.vector_query.similarity_search(
               table_name='documents',  # Assuming 128-dim vectors
               vector_column='embedding',
               query_vector=query_vector,
               limit=10,
               distance_type='l2'
           )
       ivf_time = time.time() - start_time
       
       print(f"✓ IVF Index: {num_iterations} searches in {ivf_time:.3f}s")
       print(f"  Average per search: {(ivf_time/num_iterations)*1000:.2f}ms")

       # Test HNSW performance
       print("Testing HNSW Index Performance...")
       client.vector_index.enable_hnsw()
       
       start_time = time.time()
       for _ in range(num_iterations):
           results = client.vector_query.similarity_search(
               table_name='hnsw_documents',  # HNSW table
               vector_column='embedding',
               query_vector=query_vector,
               limit=10,
               distance_type='l2'
           )
       hnsw_time = time.time() - start_time
       
       print(f"✓ HNSW Index: {num_iterations} searches in {hnsw_time:.3f}s")
       print(f"  Average per search: {(hnsw_time/num_iterations)*1000:.2f}ms")

       # Performance comparison
       if hnsw_time < ivf_time:
           improvement = ((ivf_time - hnsw_time) / ivf_time * 100)
           print(f"\n✓ HNSW is {improvement:.1f}% faster than IVF")
       else:
           improvement = ((hnsw_time - ivf_time) / hnsw_time * 100)
           print(f"\n✓ IVF is {improvement:.1f}% faster than HNSW")

       client.disconnect()

   performance_comparison()

Vector Index Management
-----------------------

Index Information and Maintenance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client

   def manage_vector_indexes():
       client = Client()
       client.connect(host='localhost', port=6001, user='root', password='111', database='test')

       # List all vector indexes
       print("Current Vector Indexes:")
       print("-" * 30)
       
       # Show indexes for a specific table
       result = client.execute("SHOW INDEX FROM documents")
       indexes = result.fetchall()
       
       for idx in indexes:
           if 'vector' in str(idx).lower() or 'ivf' in str(idx).lower() or 'hnsw' in str(idx).lower():
               print(f"  Index: {idx[2]}")
               print(f"  Column: {idx[4]}")
               print(f"  Type: {idx[10] if len(idx) > 10 else 'N/A'}")

       # Drop an index if needed
       try:
           client.vector_index.drop(
               table_name='documents',
               name='idx_document_embedding'
           )
           print("\n✓ Dropped vector index: idx_document_embedding")
       except Exception as e:
           print(f"\n⚠️  Could not drop index: {e}")

       # Recreate index with different parameters
       try:
           client.vector_index.enable_ivf()
           client.vector_index.create_ivf(
               table_name='documents',
               name='idx_document_embedding_v2',
               column='embedding',
               lists=150,  # Different configuration
               op_type='vector_l2_ops'
           )
           print("✓ Created new vector index with updated configuration")
       except Exception as e:
           print(f"❌ Failed to create new index: {e}")

       # Check index statistics
       try:
           result = client.execute("""
               SELECT table_name, index_name, cardinality 
               FROM information_schema.statistics 
               WHERE table_name = 'documents' 
               AND index_name LIKE '%vector%'
           """)
           
           stats = result.fetchall()
           if stats:
               print("\nIndex Statistics:")
               for stat in stats:
                   print(f"  Table: {stat[0]}, Index: {stat[1]}, Cardinality: {stat[2]}")
           else:
               print("\nNo vector index statistics available")
               
       except Exception as e:
           print(f"⚠️  Could not retrieve index statistics: {e}")

       client.disconnect()

   manage_vector_indexes()

Async Vector Operations
-----------------------

Async Vector Search
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   import numpy as np
   from sqlalchemy.ext.declarative import declarative_base
   from sqlalchemy import Column, Integer, String, Text
   from matrixone import AsyncClient
   from matrixone.sqlalchemy_ext import create_vector_column

   AsyncBase = declarative_base()

   class AsyncDocument(AsyncBase):
       __tablename__ = 'async_documents'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(200), nullable=False)
       content = Column(Text)
       embedding = create_vector_column(256, "f32")

   async def async_vector_operations():
       client = AsyncClient()
       await client.connect(
           host='localhost',
           port=6001,
           user='root',
           password='111',
           database='test'
       )

       # Create table
       await client.create_all(AsyncBase)

       # Enable vector indexing
       await client.vector_index.enable_ivf()

       # Create vector index
       await client.vector_index.create_ivf(
           table_name='async_documents',
           name='idx_async_embedding',
           column='embedding',
           lists=50,
           op_type='vector_l2_ops'
       )

       # Insert sample data using ORM with transaction
       from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
       
       AsyncSessionLocal = async_sessionmaker(
           bind=client.get_sqlalchemy_engine(),
           class_=AsyncSession
       )
       
       documents = [
           AsyncDocument(
               title='Async AI Research',
               content='Research on async AI systems',
               embedding=np.random.rand(256).tolist()
           ),
           AsyncDocument(
               title='Async Database Guide',
               content='Guide to async database operations',
               embedding=np.random.rand(256).tolist()
           ),
           AsyncDocument(
               title='Async Web Development',
               content='Building async web applications',
               embedding=np.random.rand(256).tolist()
           )
       ]

       async with AsyncSessionLocal() as session:
           session.add_all(documents)
           await session.commit()

       print("✓ Inserted async documents with vector embeddings using ORM")

       # Perform async vector search
       query_vector = np.random.rand(256).tolist()
       
       results = await client.vector_query.similarity_search(
           table_name='async_documents',
           vector_column='embedding',
           query_vector=query_vector,
           limit=5,
           distance_type='l2'
       )

       print("Async Vector Search Results:")
       for result in results:
           print(f"  Document: {result[1]} (Distance: {result[-1]:.4f})")

       # Clean up
       await client.drop_all(AsyncBase)
       await client.disconnect()

   # Run async example
   asyncio.run(async_vector_operations())

Best Practices
--------------

Vector Index Selection Guidelines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Use IVF when:**

* You have large datasets (millions of vectors)
* You need good balance between speed and accuracy
* You can tolerate slightly lower recall for better performance
* Your vectors are relatively low-dimensional (< 1000 dimensions)

**Use HNSW when:**

* You need the highest search accuracy
* You have high-dimensional vectors (> 1000 dimensions)
* Query latency is more important than index build time
* You have sufficient memory for the graph structure

Performance Optimization Tips
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **Choose Appropriate Dimensions**: Use the minimum necessary dimensions for your embeddings.

2. **Index Configuration**: 
   - For IVF: Start with sqrt(N) lists where N is the number of vectors
   - For HNSW: Start with M=16, ef_construction=200 for most use cases

3. **Distance Metrics**: 
   - Use L2 for general similarity
   - Use cosine for normalized vectors
   - Use inner product for recommendation systems

4. **Batch Operations**: Insert vectors in batches for better performance.

5. **Memory Management**: Monitor memory usage, especially with HNSW indexes.

Error Handling
~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.exceptions import QueryError

   def robust_vector_operations():
       client = None
       try:
           client = Client()
           client.connect(host='localhost', port=6001, user='root', password='111', database='test')

           # Check if vector indexing is available
           try:
               client.vector_index.enable_ivf()
               print("✓ Vector indexing is available")
           except QueryError as e:
               if "not supported" in str(e).lower():
                   print("❌ Vector indexing not supported in this MatrixOne version")
                   return
               else:
                   raise

           # Create index with error handling
           try:
               client.vector_index.create_ivf(
                   table_name='documents',
                   name='idx_safe_embedding',
                   column='embedding',
                   lists=100,
                   op_type='vector_l2_ops'
               )
               print("✓ Vector index created successfully")
           except QueryError as e:
               if "already exists" in str(e).lower():
                   print("⚠️  Vector index already exists")
               else:
                   print(f"❌ Failed to create vector index: {e}")

           # Perform search with error handling
           try:
               query_vector = [0.1] * 384  # Make sure dimensions match
               results = client.vector_query.similarity_search(
                   table_name='documents',
                   vector_column='embedding',
                   query_vector=query_vector,
                   limit=5,
                   distance_type='l2'
               )
               print(f"✓ Vector search completed, found {len(results)} results")
           except QueryError as e:
               print(f"❌ Vector search failed: {e}")

       except Exception as e:
           print(f"❌ Unexpected error: {e}")
       finally:
           if client:
               client.disconnect()

   robust_vector_operations()

Next Steps
----------

* Explore :doc:`fulltext_guide` for fulltext search capabilities
* Check :doc:`orm_guide` for advanced ORM patterns with vectors
* Review :doc:`examples` for comprehensive vector search examples
* See :doc:`api/vector_index` for detailed API documentation
