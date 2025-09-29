Quick Start
===========

This guide will help you get started with the MatrixOne Python SDK quickly using modern ORM patterns, vector search, fulltext search, and enhanced query building capabilities.

Basic Usage with Configuration Helper
--------------------------------------

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params, print_config

   # Print connection configuration (reads from environment variables)
   print_config()

   # Get connection parameters automatically
   host, port, user, password, database = get_connection_params()

   # Create and connect to MatrixOne
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Execute queries
   result = client.execute("SELECT 1 as test, USER() as user_info")
   print(result.fetchall())

   # Get backend version (auto-detected)
   version = client.version()
   print(f"MatrixOne version: {version}")

   client.disconnect()

Async Usage with ORM
---------------------

.. code-block:: python

   import asyncio
   from sqlalchemy import Column, Integer, String
   from sqlalchemy.ext.declarative import declarative_base
   from matrixone import AsyncClient
   from matrixone.config import get_connection_params

   # Define ORM model
   Base = declarative_base()

   class QuickUser(Base):
       __tablename__ = 'quick_users'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       name = Column(String(100), nullable=False)
       email = Column(String(200), unique=True, nullable=False)

   async def async_orm_quickstart():
       # Get connection parameters
       host, port, user, password, database = get_connection_params()
       
       client = AsyncClient()
       await client.connect(host=host, port=port, user=user, password=password, database=database)
       
       # Create table using ORM
       await client.create_all(Base)
       
       # Insert data using async transaction
       async with client.transaction() as tx:
           await tx.execute(
               "INSERT INTO quick_users (name, email) VALUES (%s, %s)",
               ('Quick User', 'quick@example.com')
           )
       
       # Query data
       result = await client.execute("SELECT * FROM quick_users")
       rows = await result.fetchall()
       for row in rows:
           print(f"User: {row[1]}, Email: {row[2]}")
       
       # Clean up
       await client.drop_all(Base)
       await client.disconnect()

   asyncio.run(async_orm_quickstart())

ORM with Transaction Management
--------------------------------

.. code-block:: python

   from sqlalchemy import Column, Integer, String, DECIMAL
   from sqlalchemy.ext.declarative import declarative_base
   from matrixone import Client
   from matrixone.config import get_connection_params

   # Define ORM models
   Base = declarative_base()

   class Account(Base):
       __tablename__ = 'quick_accounts'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       name = Column(String(100), nullable=False)
       balance = Column(DECIMAL(10, 2), nullable=False)

   # Get connection and create client
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Create table using ORM
   client.create_all(Base)

   # Insert initial data
   with client.transaction() as tx:
       tx.execute("INSERT INTO quick_accounts (name, balance) VALUES (%s, %s)", ('Alice', 1000.00))
       tx.execute("INSERT INTO quick_accounts (name, balance) VALUES (%s, %s)", ('Bob', 500.00))

   # Transfer money using transaction
   with client.transaction() as tx:
       tx.execute("UPDATE quick_accounts SET balance = balance - %s WHERE name = %s", (100.00, 'Alice'))
       tx.execute("UPDATE quick_accounts SET balance = balance + %s WHERE name = %s", (100.00, 'Bob'))

   # Verify the transfer
   result = client.execute("SELECT name, balance FROM quick_accounts ORDER BY name")
   for row in result.fetchall():
       print(f"{row[0]}: ${row[1]}")

   # Clean up
   client.drop_all(Base)
   client.disconnect()

Vector Search Quick Start
--------------------------

.. code-block:: python

   from sqlalchemy import Column, Integer, String, Text
   from sqlalchemy.ext.declarative import declarative_base
   from matrixone import Client
   from matrixone.config import get_connection_params
   from matrixone.sqlalchemy_ext import create_vector_column

   # Define vector model
   VectorBase = declarative_base()

   class QuickDocument(VectorBase):
       __tablename__ = 'quick_documents'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(200), nullable=False)
       content = Column(Text)
       embedding = create_vector_column(128, "f32")  # 128-dimensional vector

   # Connect and setup
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Create table and index
   client.create_all(VectorBase)
   client.vector_index.enable_ivf()
   client.vector_index.create_ivf(
       table_name='quick_documents',
       name='idx_quick_embedding',
       column='embedding',
       lists=50,
       op_type='vector_l2_ops'
   )

   # Insert sample documents
   # Insert documents using ORM
   from sqlalchemy.orm import sessionmaker
   
   Session = sessionmaker(bind=client.get_sqlalchemy_engine())
   session = Session()
   
   docs = [
       QuickDocument(
           title='AI Research',
           content='Artificial intelligence research paper',
           embedding=[0.1] * 128
       ),
       QuickDocument(
           title='ML Guide',
           content='Machine learning tutorial',
           embedding=[0.2] * 128
       )
   ]
   
   session.add_all(docs)
   session.commit()
   session.close()

   # Vector similarity search
   query_vector = [0.15] * 128
   results = client.vector_query.similarity_search(
       table_name='quick_documents',
       vector_column='embedding',
       query_vector=query_vector,
       limit=5,
       distance_type='l2'
   )

   print("Vector Search Results:")
   for result in results:
       print(f"Document: {result[1]} (Distance: {result[-1]:.4f})")

   # Clean up
   client.drop_all(VectorBase)
   client.disconnect()

Configuration Best Practices
-----------------------------

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params, print_config

   # Use environment variables for configuration
   # Set these in your environment:
   # export MATRIXONE_HOST=127.0.0.1
   # export MATRIXONE_PORT=6001
   # export MATRIXONE_USER=root
   # export MATRIXONE_PASSWORD=111
   # export MATRIXONE_DATABASE=test

   # Print current configuration
   print_config()

   # Get connection parameters from environment
   host, port, user, password, database = get_connection_params()

   # Create client with optimized settings
   client = Client(
       connection_timeout=30,        # Connection timeout in seconds
       query_timeout=300,           # Query timeout in seconds
       auto_commit=True,            # Enable auto-commit for better performance
       charset='utf8mb4',           # Support for international characters
       enable_performance_logging=True,  # Monitor query performance
       enable_sql_logging=False     # Disable SQL logging in production
   )

   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Check backend capabilities
   version = client.get_backend_version()
   print(f"✓ Connected to MatrixOne {version}")

   if client.is_feature_available('vector_search'):
       print("✓ Vector search is available")
   
   if client.is_feature_available('fulltext_search'):
       print("✓ Fulltext search is available")

   client.disconnect()

Error Handling
--------------

.. code-block:: python

   from matrixone import Client
   from matrixone.exceptions import ConnectionError, QueryError
   from matrixone.config import get_connection_params

   def robust_connection_example():
       client = None
       try:
           host, port, user, password, database = get_connection_params()
           
           # Create client with error handling
           client = Client()
           client.connect(host=host, port=port, user=user, password=password, database=database)
           
           # Execute query with error handling
           try:
               result = client.execute("SELECT 1 as test")
               print(f"✓ Query successful: {result.fetchall()}")
           except QueryError as e:
               print(f"❌ Query failed: {e}")
               
       except ConnectionError as e:
           print(f"❌ Connection failed: {e}")
       except Exception as e:
           print(f"❌ Unexpected error: {e}")
       finally:
           # Always clean up
           if client:
               client.disconnect()
               print("✓ Connection closed")

   robust_connection_example()

Enhanced Query Building with logical_in
---------------------------------------

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import logical_in
   from matrixone.sqlalchemy_ext import boolean_match
   from sqlalchemy import func
   from matrixone.config import get_connection_params

   def enhanced_query_example():
       host, port, user, password, database = get_connection_params()
       client = Client()
       client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create a sample table
       client.create_table("products", {
           "id": "int",
           "name": "varchar(100)",
           "category": "varchar(50)",
           "price": "decimal(10,2)",
           "description": "text"
       }, primary_key="id")

       # Insert sample data
       products = [
           {"id": 1, "name": "Laptop", "category": "Electronics", "price": 999.99, "description": "High-performance laptop"},
           {"id": 2, "name": "Phone", "category": "Electronics", "price": 699.99, "description": "Smartphone with AI features"},
           {"id": 3, "name": "Book", "category": "Education", "price": 29.99, "description": "Programming guide"},
           {"id": 4, "name": "Tablet", "category": "Electronics", "price": 499.99, "description": "Portable tablet device"}
       ]
       client.batch_insert("products", products)

       # Enhanced query building with logical_in
       query = client.query("products")
       
       # Filter by multiple categories
       results = query.filter(logical_in("category", ["Electronics", "Education"])).all()
       print("Products in Electronics or Education:")
       for row in results:
           print(f"  {row[1]} - {row[2]} - ${row[3]}")

       # Filter by price range using logical_in with subquery
       price_range_query = client.query("products").select(func.min("price"), func.max("price"))
       results = query.filter(logical_in("price", price_range_query)).all()
       print("Products in price range:")
       for row in results:
           print(f"  {row[1]} - ${row[3]}")

       # Create fulltext index for advanced search
       client.fulltext_index.create("products", "idx_description", "description", algorithm="BM25")

       # Use logical_in with fulltext search
       fulltext_filter = boolean_match("description").must("laptop OR phone")
       results = query.filter(logical_in("id", fulltext_filter)).all()
       print("Products matching fulltext search:")
       for row in results:
           print(f"  {row[1]} - {row[4]}")

       client.disconnect()

   enhanced_query_example()

Vector Search with Enhanced Features
------------------------------------

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params
   import numpy as np

   def vector_search_example():
       host, port, user, password, database = get_connection_params()
       client = Client()
       client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create vector table
       client.create_table("documents", {
           "id": "int",
           "title": "varchar(200)",
           "content": "text",
           "embedding": "vector(384,f32)"
       }, primary_key="id")

       # Create HNSW index for fast vector search
       client.vector.create_hnsw(
           table_name="documents",
           name="idx_embedding_hnsw",
           column="embedding",
           m=16,
           ef_construction=200
       )

       # Insert documents with embeddings
       documents = [
           {
               "id": 1,
               "title": "AI Research Paper",
               "content": "Advanced artificial intelligence research",
               "embedding": np.random.rand(384).astype(np.float32).tolist()
           },
           {
               "id": 2,
               "title": "Machine Learning Guide",
               "content": "Comprehensive machine learning tutorial",
               "embedding": np.random.rand(384).astype(np.float32).tolist()
           },
           {
               "id": 3,
               "title": "Data Science Handbook",
               "content": "Complete data science reference",
               "embedding": np.random.rand(384).astype(np.float32).tolist()
           }
       ]
       client.batch_insert("documents", documents)

       # Perform vector similarity search
       query_vector = np.random.rand(384).astype(np.float32).tolist()
       results = client.vector_query.similarity_search(
           table_name="documents",
           vector_column="embedding",
           query_vector=query_vector,
           limit=3,
           distance_function="cosine"
       )

       print("Vector Search Results:")
       for result in results:
           print(f"  {result[1]} (Similarity: {1 - result[-1]:.4f})")

       client.disconnect()

   vector_search_example()

Next Steps
----------

* Read the :doc:`api/index` for detailed API documentation
* Check out the :doc:`logical_in_guide` for advanced query building
* Explore :doc:`vector_guide` for comprehensive vector operations
* Learn about :doc:`fulltext_guide` for text search capabilities
* Check out the :doc:`examples` for comprehensive usage examples
* Learn about :doc:`contributing` to contribute to the project
* Run ``make examples`` to test all examples with your MatrixOne setup
* Use ``make test`` to run the test suite and verify your setup
