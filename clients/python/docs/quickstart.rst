Quick Start
===========

This guide will help you get started with the MatrixOne Python SDK quickly. For detailed configuration options, see :doc:`configuration_guide`.

Basic Usage
-----------

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import Column, Integer, String, DateTime
   from matrixone.orm import declarative_base

   # Create and connect to MatrixOne
   client = Client()
   client.connect(
       host="127.0.0.1",
       port=6001,
       user="root",
       password="111",
       database="test"
   )

   # Define table model
   Base = declarative_base()
   
   class User(Base):
       __tablename__ = 'users'
       id = Column(Integer, primary_key=True)
       name = Column(String(100))
       email = Column(String(200))
       created_at = Column(DateTime)

   # Create table using model
   client.create_table(User)

   # Insert data using insert API
   client.insert("users", {
       "id": 1,
       "name": "John Doe",
       "email": "john@example.com",
       "created_at": "2024-01-01 10:00:00"
   })

   # Simple query using execute API
   result = client.execute("SELECT * FROM users WHERE id = ?", (1,))
   print(result.fetchall())

   # Complex query using query builder
   result = client.query(User).select("*").filter(User.id == 1).execute()
   print(result.fetchall())

   # Update data using query API
   client.query(User).update({"name": "Jane Doe"}).filter(User.id == 1).execute()

   # Delete data using query API
   client.query(User).filter(User.id == 1).delete()

   # Drop table using drop_table API
   client.drop_table(User)

   client.disconnect()

Async Usage with Table Models and Modern API
----------------------------------------------

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   from matrixone.config import get_connection_params
   from sqlalchemy import Column, Integer, String, DECIMAL
   from matrixone.orm import declarative_base

   async def async_quickstart():
       # Get connection parameters
       host, port, user, password, database = get_connection_params()
       
       client = AsyncClient()
       await client.connect(host=host, port=port, user=user, password=password, database=database)
       
       # Define table model
       Base = declarative_base()
       
       class Product(Base):
           __tablename__ = 'products'
           id = Column(Integer, primary_key=True)
           name = Column(String(200))
           price = Column(DECIMAL(10, 2))
           category = Column(String(50))
       
       # Create table using model
       await client.create_table(Product)
       
       # Insert data using async insert API
       await client.insert("products", {
           "id": 1,
           "name": "Laptop",
           "price": 999.99,
           "category": "Electronics"
       })
       
       # Simple query using async execute API
       result = await client.execute("SELECT * FROM products WHERE category = ?", ("Electronics",))
       rows = result.fetchall()
       for row in rows:
           print(f"Product: {row[1]}, Price: ${row[2]}")
       
       # Complex query using async query builder
       result = await client.query(Product).select("*").filter(Product.category == "Electronics").execute()
       rows = result.fetchall()
       for row in rows:
           print(f"Product: {row[1]}, Price: ${row[2]}")
       
       # Batch insert using async batch_insert API
       products = [
           {"id": 2, "name": "Phone", "price": 699.99, "category": "Electronics"},
           {"id": 3, "name": "Book", "price": 29.99, "category": "Education"}
       ]
       await client.batch_insert(Product, products)
       
       # Clean up using async drop_table API
       await client.drop_table(Product)
       await client.disconnect()

   asyncio.run(async_quickstart())

Vector Operations with Table Models
------------------------------------

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params
   from sqlalchemy import Column, Integer, String, Text
   from matrixone.orm import declarative_base
   from matrixone.sqlalchemy_ext import Vectorf32

   # Get connection parameters
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Define vector table model
   Base = declarative_base()
   
   class Document(Base):
       __tablename__ = 'documents'
       id = Column(Integer, primary_key=True)
       title = Column(String(200))
       content = Column(Text)
       embedding = Column(Vectorf32(384))  # 384-dimensional vector

   # Create table using model
   client.create_table(Document)

   # Insert vector data
   client.insert("documents", {
       "id": 1,
       "title": "Introduction to AI",
       "content": "Artificial Intelligence is a field of computer science...",
       "embedding": [0.1] * 384  # Example 384-dimensional vector
   })

   # Enable IVF indexing
   client.vector_ops.enable_ivf()

   # Create IVF index (first argument is positional)
   client.vector_ops.create_ivf(Document, name="idx_embedding", column="embedding", lists=100)

   # Vector similarity search using simple interface (first argument is positional)
   query_vector = [0.1] * 384
   results = client.vector_ops.similarity_search(
       Document,
       vector_column="embedding",
       query_vector=query_vector,
       limit=5,
       distance_type="l2"
   )
   print("Similarity search results:", results)

   # Complex vector query using query builder
   result = client.query("documents").select("*").where(
       "l2_distance(embedding, ?) < ?", 
       (query_vector, 0.5)
   ).order_by("l2_distance(embedding, ?)", query_vector).limit(10).execute()
   
   for row in result.fetchall():
       print(f"Document: {row[1]}, Distance: {row[3]}")

   # Drop vector index
   client.vector_ops.drop(Document, "idx_embedding")

   # Clean up
   client.drop_table(Document)
   client.disconnect()

HNSW Vector Indexing
--------------------

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params
   from sqlalchemy import Column, BigInteger, String
   from matrixone.orm import declarative_base
   from matrixone.sqlalchemy_ext import Vectorf32

   # Get connection parameters
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Define vector table model
   Base = declarative_base()
   
   class Product(Base):
       __tablename__ = 'products'
       # IMPORTANT: HNSW index requires BigInteger primary key
       id = Column(BigInteger, primary_key=True, autoincrement=True)
       name = Column(String(200))
       features = Column(Vectorf32(128))  # 128-dimensional feature vector

   # Create table using model
   client.create_table(Product)

   # Insert vector data using client API
   client.insert(Product, {
       "name": "Smartphone",
       "features": [0.2] * 128  # Example 128-dimensional vector
   })

   # Enable HNSW indexing
   client.vector_ops.enable_hnsw()

   # Create HNSW index (first argument is positional)
   client.vector_ops.create_hnsw(Product, name="idx_features", column="features", m=16, ef_construction=200)

   # Vector similarity search (first argument is positional)
   query_vector = [0.2] * 128
   results = client.vector_ops.similarity_search(
       Product,
       vector_column="features",
       query_vector=query_vector,
       limit=5,
       distance_type="cosine"
   )
   print("HNSW similarity search results:", results)

   # Drop vector index
   client.vector_ops.drop(Product, "idx_features")

   # Clean up
   client.drop_table(Product)
   client.disconnect()

ORM with Modern Patterns
------------------------

.. code-block:: python

   from sqlalchemy import Column, Integer, String, DECIMAL, DateTime
   from matrixone.orm import declarative_base
   from matrixone import Client
   from matrixone.config import get_connection_params

   # Define ORM models
   Base = declarative_base()

   class Account(Base):
       __tablename__ = 'accounts'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       name = Column(String(100), nullable=False)
       balance = Column(DECIMAL(10, 2), nullable=False)
       created_at = Column(DateTime, nullable=False)

   # Get connection and create client
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Create table using ORM model
   client.create_table(Account)

   # Insert data using client API
   accounts_data = [
       {"name": "Alice", "balance": 1000.00, "created_at": "2024-01-01 10:00:00"},
       {"name": "Bob", "balance": 500.00, "created_at": "2024-01-01 10:00:00"}
   ]
   client.batch_insert(Account, accounts_data)

   # Query using client API
   accounts = client.query(Account).filter(Account.balance > 600).all()
   for account in accounts:
       print(f"{account.name}: ${account.balance}")

   # Update using ORM
   session.query(Account).filter(Account.name == "Alice").update({"balance": 1200.00})
   session.commit()

   # Clean up using ORM
   client.drop_table(Account)
   session.close()
   client.disconnect()

Vector Search with Modern API
-----------------------------

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params
   from matrixone.sqlalchemy_ext import create_vector_column
   import numpy as np

   # Get connection parameters
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Create vector table using create_table API
   client.create_table("documents", {
       "id": "int",
       "title": "varchar(200)",
       "content": "text",
       "embedding": "vecf32(384)"  # 384-dimensional f32 vector
   }, primary_key="id")

   # Create vector index using vector_ops API
   client.vector_ops.enable_ivf()
   client.vector_ops.create_ivf(
       table_name_or_model="documents",
       name="idx_embedding",
       column="embedding",
       lists=50,
       op_type="vector_l2_ops"
   )

   # Insert documents with embeddings using insert API
   documents = [
       {
           "id": 1,
           "title": "AI Research",
           "content": "Artificial intelligence research paper",
           "embedding": np.random.rand(384).astype(np.float32).tolist()
       },
       {
           "id": 2,
           "title": "ML Guide",
           "content": "Machine learning tutorial",
           "embedding": np.random.rand(384).astype(np.float32).tolist()
       }
   ]

   for doc in documents:
       client.insert("documents", doc)

   # Vector similarity search using vector_query API
   query_vector = np.random.rand(384).astype(np.float32).tolist()
   results = client.vector_ops.similarity_search(
       table_name_or_model="documents",
       vector_column="embedding",
       query_vector=query_vector,
       limit=5,
       distance_type="l2"
   )

   print("Vector Search Results:")
   for result in results.rows:
       print(f"Document: {result[1]} (Distance: {result[-1]:.4f})")

   # Clean up using drop_table API
   client.drop_table("documents")
   client.disconnect()

Async Vector Operations
-----------------------

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   from matrixone.config import get_connection_params
   import numpy as np

   async def async_vector_example():
       # Get connection parameters
       host, port, user, password, database = get_connection_params()
       
       client = AsyncClient()
       await client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create vector table using async create_table API
       await client.create_table("products", {
           "id": "int",
           "name": "varchar(200)",
           "description": "text",
           "features": "vecf64(512)"  # 512-dimensional f64 vector
       }, primary_key="id")

       # Create vector index using async vector_ops API
       await client.vector_ops.enable_ivf()
       await client.vector_ops.create_ivf(
           table_name_or_model="products",
           name="idx_features",
           column="features",
           lists=100,
           op_type="vector_cosine_ops"
       )

       # Insert products with feature vectors using async insert API
       products = [
           {
               "id": 1,
               "name": "Smartphone",
               "description": "Latest smartphone with AI features",
               "features": np.random.rand(512).astype(np.float64).tolist()
           },
           {
               "id": 2,
               "name": "Laptop",
               "description": "High-performance laptop for professionals",
               "features": np.random.rand(512).astype(np.float64).tolist()
           }
       ]

       for product in products:
           await client.insert("products", product)

       # Vector similarity search using async vector_query API
       query_vector = np.random.rand(512).astype(np.float64).tolist()
       results = await client.vector_ops.similarity_search(
           table_name_or_model="products",
           vector_column="features",
           query_vector=query_vector,
           limit=3,
           distance_type="cosine"
       )

       print("Async Vector Search Results:")
       for result in results.rows:
           print(f"Product: {result[1]} (Similarity: {1 - result[-1]:.4f})")

       # Clean up using async drop_table API
       await client.drop_table("products")
       await client.disconnect()

   asyncio.run(async_vector_example())

Transaction Management
----------------------

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params

   def transaction_example():
       host, port, user, password, database = get_connection_params()
       client = Client()
       client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create table using create_table API
       client.create_table("orders", {
           "id": "int",
           "customer_id": "int",
           "amount": "decimal(10,2)",
           "status": "varchar(20)"
       }, primary_key="id")

       # Use transaction for atomic operations
       with client.transaction() as tx:
           # Insert order
           tx.insert("orders", {
               "id": 1,
               "customer_id": 100,
               "amount": 99.99,
               "status": "pending"
           })
           
           # Update order status
           tx.query("orders").update({"status": "confirmed"}).where("id = ?", 1).execute()
           
           # If any operation fails, the entire transaction is rolled back

       # Verify the transaction
       result = client.query("orders").select("*").where("id = ?", 1).execute()
       print("Order after transaction:", result.fetchall())

       # Clean up
       client.drop_table("orders")
       client.disconnect()

   transaction_example()

Error Handling with Modern API
------------------------------

.. code-block:: python

   from matrixone import Client
   from matrixone.exceptions import ConnectionError, QueryError
   from matrixone.config import get_connection_params

   def robust_example():
       client = None
       try:
           host, port, user, password, database = get_connection_params()
           
           # Create client with error handling
           client = Client()
           client.connect(host=host, port=port, user=user, password=password, database=database)
           
           # Create table with error handling
           try:
               client.create_table("test_table", {
                   "id": "int",
                   "name": "varchar(100)"
               }, primary_key="id")
               print("✓ Table created successfully")
           except QueryError as e:
               print(f"❌ Table creation failed: {e}")
               
           # Insert data with error handling
           try:
               client.insert("test_table", {"id": 1, "name": "Test"})
               print("✓ Data inserted successfully")
           except QueryError as e:
               print(f"❌ Data insertion failed: {e}")
               
           # Query data with error handling
           try:
               result = client.query("test_table").select("*").execute()
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
               try:
                   client.drop_table("test_table")
                   client.disconnect()
                   print("✓ Cleanup completed")
               except Exception as e:
                   print(f"⚠️ Cleanup warning: {e}")

   robust_example()

Configuration Best Practices
----------------------------

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params, print_config

   def configuration_example():
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
           sql_log_mode='simple',       # Simple SQL logging for production
           slow_query_threshold=1.0     # Alert on queries > 1s
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

   configuration_example()

Next Steps
----------

* Read the :doc:`api/index` for detailed API documentation
* Check out the :doc:`vector_guide` for comprehensive vector operations
* Explore :doc:`fulltext_guide` for text search capabilities
* Learn about :doc:`orm_guide` for ORM patterns
* Check out the :doc:`examples` for comprehensive usage examples
* Learn about :doc:`contributing` to contribute to the project
* Run ``make examples`` to test all examples with your MatrixOne setup
* Use ``make test`` to run the test suite and verify your setup