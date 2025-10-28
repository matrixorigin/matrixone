Quick Start
===========

This guide will help you get started with the MatrixOne Python SDK quickly. For detailed configuration options, see :doc:`configuration_guide`.

Basic Usage
-----------

.. note::
   **Connection API Requirements**
   
   The ``connect()`` method requires **keyword arguments** (not positional):
   
   - ``database`` - **Required**, no default value
   - ``host`` - Default: ``'localhost'``
   - ``port`` - Default: ``6001``
   - ``user`` - Default: ``'root'``
   - ``password`` - Default: ``'111'``
   
   **Minimal connection** (uses all defaults):
   
   .. code-block:: python
   
      client.connect(database='test')
   
   By default, all features (IVF, HNSW, fulltext) are automatically enabled via ``on_connect=[ConnectionAction.ENABLE_ALL]``.

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

Transaction Management (Recommended)
------------------------------------

Use ``client.session()`` for atomic transactions. All operations within a session succeed or fail together with automatic commit/rollback.

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import select, insert, update, delete
   from sqlalchemy import Column, Integer, String
   from matrixone.orm import declarative_base

   client = Client()
   client.connect(database='test')

   # Define model
   Base = declarative_base()
   
   class User(Base):
       __tablename__ = 'users'
       id = Column(Integer, primary_key=True)
       name = Column(String(100))
       email = Column(String(255))
       age = Column(Integer)
       status = Column(String(20))

   # Create table
   client.create_table(User)

   # Basic transaction with automatic commit/rollback
   with client.session() as session:
       # All operations are atomic
       session.execute(insert(User).values(name='Alice', email='alice@example.com', age=30))
       session.execute(update(User).where(User.age < 18).values(status='minor'))
       
       # Query within transaction
       stmt = select(User).where(User.age > 25)
       result = session.execute(stmt)
       users = result.scalars().all()
       # Commits automatically on success, rolls back on error

   # Error handling with automatic rollback
   try:
       with client.session() as session:
           session.execute(insert(User).values(name='Bob', age=25))
           # This will fail and trigger automatic rollback
           session.execute(insert(InvalidTable).values(data='test'))
   except Exception as e:
       print(f"Transaction failed and rolled back: {e}")

   client.disconnect()

**Key Benefits:**

- ✅ **Atomic operations** - all succeed or fail together
- ✅ **Automatic rollback** on errors
- ✅ **Access to all managers** (snapshots, clones, load_data, etc.)
- ✅ **Full SQLAlchemy ORM** support

Wrapping Existing SQLAlchemy Sessions
--------------------------------------

If you have existing SQLAlchemy code, you can wrap your sessions to add MatrixOne features without refactoring:

.. code-block:: python

   from sqlalchemy import create_engine
   from sqlalchemy.orm import sessionmaker
   from matrixone import Client
   from matrixone.session import Session as MatrixOneSession

   # Your existing SQLAlchemy setup
   engine = create_engine('mysql+pymysql://root:111@localhost:6001/test')
   SessionFactory = sessionmaker(bind=engine)
   sqlalchemy_session = SessionFactory()

   # Create MatrixOne client
   mo_client = Client()
   mo_client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Wrap your existing session with MatrixOne features
   mo_session = MatrixOneSession(
       client=mo_client,
       wrap_session=sqlalchemy_session
   )

   try:
       # Your existing SQLAlchemy operations still work
       result = mo_session.execute("SELECT * FROM users")
       
       # Now you can also use MatrixOne-specific features
       mo_session.stage.create_s3('backup_stage', bucket='my-backups', path='')
       mo_session.snapshots.create('daily_backup', level='database')
       mo_session.load_data.read_csv('/data/users.csv', table='users')
       
       mo_session.commit()
   finally:
       mo_session.close()

**Perfect For:**

- 🔄 Gradual migration from pure SQLAlchemy to MatrixOne
- 🏢 Adding MatrixOne features to existing enterprise applications
- 📦 Legacy code modernization without complete refactoring
- 🔧 Testing MatrixOne features alongside existing code

**Async Version:**

.. code-block:: python

   from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
   from matrixone import AsyncClient
   from matrixone.session import AsyncSession as MatrixOneAsyncSession

   # Existing async SQLAlchemy setup
   async_engine = create_async_engine('mysql+aiomysql://root:111@localhost:6001/test')
   AsyncSessionFactory = async_sessionmaker(bind=async_engine)
   sqlalchemy_async_session = AsyncSessionFactory()

   # Create async MatrixOne client
   mo_async_client = AsyncClient()
   await mo_async_client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Wrap existing async session
   mo_async_session = MatrixOneAsyncSession(
       client=mo_async_client,
       wrap_session=sqlalchemy_async_session
   )

   try:
       # Standard async SQLAlchemy operations
       result = await mo_async_session.execute("SELECT * FROM users")
       
       # MatrixOne async features now available
       await mo_async_session.stage.create_local('export_stage', '/exports/')
       await mo_async_session.snapshots.create('async_backup', level='database')
       
       await mo_async_session.commit()
   finally:
       await mo_async_session.close()

SQLAlchemy ORM Style (Recommended)
-----------------------------------

The SDK provides seamless SQLAlchemy integration with ORM-style operations:

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import Base, Column, Integer, String
   from sqlalchemy import select, insert, update, delete, and_, or_

   # Define ORM models
   class User(Base):
       __tablename__ = 'users'
       id = Column(Integer, primary_key=True)
       name = Column(String(100))
       email = Column(String(255))
       age = Column(Integer)

   client = Client()
   client.connect(database='test')
   client.create_table(User)

   # ORM-style INSERT
   stmt = insert(User).values(name='John', email='john@example.com', age=30)
   client.execute(stmt)

   # ORM-style SELECT with WHERE
   stmt = select(User).where(User.age > 25)
   result = client.execute(stmt)
   for user in result.scalars():
       print(f"User: {user.name}, Age: {user.age}")

   # ORM-style SELECT with complex WHERE
   stmt = select(User).where(
       and_(
           User.age > 18,
           or_(User.status == 'active', User.status == 'pending')
       )
   )
   result = client.execute(stmt)

   # ORM-style UPDATE
   stmt = update(User).where(User.id == 1).values(email='newemail@example.com')
   result = client.execute(stmt)
   print(f"Updated {result.affected_rows} rows")

   # ORM-style DELETE
   stmt = delete(User).where(User.age < 18)
   result = client.execute(stmt)

   client.disconnect()

**Recommended Practices:**

- ✅ Use SQLAlchemy statements (``select``, ``insert``, ``update``, ``delete``)
- ✅ Use ``session()`` for multi-statement transactions
- ✅ Use ``client.execute()`` for single-statement operations
- ✅ Prefer SQLAlchemy statements over raw SQL strings

Async Usage with Sessions and ORM
----------------------------------

Full async/await support with async sessions for non-blocking operations:

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   from matrixone.orm import Base, Column, Integer, String, DECIMAL
   from sqlalchemy import select, insert, update

   async def async_quickstart():
       client = AsyncClient()
       await client.connect(database='test')
       
       # Define table model
       Base = declarative_base()
       
       class Product(Base):
           __tablename__ = 'products'
           id = Column(Integer, primary_key=True)
           name = Column(String(200))
           price = Column(DECIMAL(10, 2))
           category = Column(String(50))
       
       # Create table
       await client.create_table(Product)
       
       # ORM-style async INSERT
       stmt = insert(Product).values(
           id=1, name='Laptop', price=999.99, category='Electronics'
       )
       await client.execute(stmt)
       
       # ORM-style async SELECT
       stmt = select(Product).where(Product.category == 'Electronics')
       result = await client.execute(stmt)
       for product in result.scalars():
           print(f"Product: {product.name}, Price: ${product.price}")
       
       # Async transaction with session
       async with client.session() as session:
           # All operations are atomic
           await session.execute(
               insert(Product).values(id=2, name='Phone', price=699.99, category='Electronics')
           )
           await session.execute(
               update(Product).where(Product.id == 1).values(price=899.99)
           )
           
           # Concurrent queries with asyncio.gather
           product_result, count_result = await asyncio.gather(
               session.execute(select(Product).where(Product.category == 'Electronics')),
               session.execute(select(func.count(Product.id)))
           )
           # Commits automatically
       
       # Clean up
       await client.drop_table(Product)
       await client.disconnect()

   asyncio.run(async_quickstart())

**Async Advantages:**

- ✅ **Non-blocking operations** - don't block the event loop
- ✅ **Concurrent execution** with ``asyncio.gather()``
- ✅ **Perfect for web frameworks** (FastAPI, aiohttp)
- ✅ **Same transaction guarantees** as sync version

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

   # ⚠️ Insert initial data BEFORE creating IVF index (recommended)
   client.insert("documents", {
       "id": 1,
       "title": "Introduction to AI",
       "content": "Artificial Intelligence is a field of computer science...",
       "embedding": [0.1] * 384  # Example 384-dimensional vector
   })

   # Enable IVF indexing
   client.vector_ops.enable_ivf()

   # Create IVF index after initial data (better clustering)
   client.vector_ops.create_ivf(Document, name="idx_embedding", column="embedding", lists=100)
   
   # IVF supports dynamic updates (can continue inserting)
   client.insert("documents", {"id": 2, ...})  # ✅ Works fine

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

   # ⭐ IMPORTANT: Monitor IVF index health for production systems
   stats = client.vector_ops.get_ivf_stats(Document, "embedding")
   counts = stats['distribution']['centroid_count']
   balance_ratio = max(counts) / min(counts) if min(counts) > 0 else float('inf')
   print(f"Index health - Centroids: {len(counts)}, Balance ratio: {balance_ratio:.2f}")
   if balance_ratio > 2.5:
       print("⚠️  Warning: Index needs rebuilding for optimal performance")
   
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

   # ⭐ CRITICAL: Check IVF index health - Essential for production monitoring
   stats = client.vector_ops.get_ivf_stats("documents", "embedding")
   
   # Analyze index balance
   distribution = stats['distribution']
   counts = distribution['centroid_count']
   total_centroids = len(counts)
   total_vectors = sum(counts)
   balance_ratio = max(counts) / min(counts) if min(counts) > 0 else float('inf')
   
   print(f"\nIVF Index Health:")
   print(f"  - Total centroids: {total_centroids}")
   print(f"  - Total vectors: {total_vectors}")
   print(f"  - Balance ratio: {balance_ratio:.2f} {'✓' if balance_ratio <= 2.5 else '⚠️'}")
   
   if balance_ratio > 2.5:
       print(f"  - ⚠️  Index imbalanced - consider rebuilding")
       print(f"  - See vector_guide for detailed monitoring procedures")

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

       # ⭐ Monitor IVF index health asynchronously
       stats = await client.vector_ops.get_ivf_stats("products", "features")
       counts = stats['distribution']['centroid_count']
       balance_ratio = max(counts) / min(counts) if min(counts) > 0 else float('inf')
       
       print(f"\nAsync IVF Index Health:")
       print(f"  - Centroids: {len(counts)}, Balance: {balance_ratio:.2f}")

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