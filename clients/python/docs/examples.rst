Examples
========

This section provides comprehensive examples of using the MatrixOne Python SDK with modern API patterns, showcasing the latest features and best practices.

Modern API Examples
-------------------

Connection and Basic Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params, print_config

   # Print connection configuration
   print_config()

   # Get connection parameters from environment or defaults
   host, port, user, password, database = get_connection_params()

   # Create client and connect
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Execute a simple query
   result = client.execute("SELECT 1 as test_value, USER() as user_info")
   print(result.fetchall())

   # Get backend version information
   version = client.version()
   print(f"MatrixOne version: {version}")

   client.disconnect()

Table Management with Modern API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params

   # Get connection parameters
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Create table using modern API
   client.create_table("products", {
       "id": "int",
       "name": "varchar(200)",
       "description": "text",
       "price": "decimal(10,2)",
       "category": "varchar(50)",
       "in_stock": "boolean"
   }, primary_key="id")

   # Insert data efficiently
   products_data = [
       {"id": 1, "name": "Laptop", "description": "High-performance laptop", "price": 999.99, "category": "Electronics", "in_stock": True},
       {"id": 2, "name": "Phone", "description": "Smartphone with AI features", "price": 699.99, "category": "Electronics", "in_stock": True},
       {"id": 3, "name": "Book", "description": "Programming guide", "price": 29.99, "category": "Education", "in_stock": False}
   ]
   client.batch_insert("products", products_data)

   # Create indexes for better performance
   client.create_index("products", "idx_category", "category")
   client.create_index("products", "idx_price", "price")

   print("Table created and data inserted successfully")

Modern Query Building
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import logical_in
   from matrixone.sqlalchemy_ext import boolean_match
   from sqlalchemy import func
   from matrixone.config import get_connection_params

   # Get connection parameters
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Modern query building with enhanced features
   query = client.query("products")
   
   # Basic filtering
   results = query.filter("price > 500").all()
   print("Expensive products:", results)

   # Using logical_in for flexible IN conditions
   results = query.filter(logical_in("category", ["Electronics", "Education"])).all()
   print("Products in Electronics or Education:", results)

   # Complex queries with expressions
   results = (query
              .select("category", func.count("id").label("product_count"))
              .group_by("category")
              .having(func.count("id") > 1)
              .all())
   print("Categories with multiple products:", results)

   # Order by with expressions
   results = (query
              .select("name", "price")
              .order_by(func.upper("name"))
              .all())
   print("Products ordered by name:", results)

   # Explain queries for optimization
   explain_result = query.filter("price > 500").explain(verbose=True)
   print("Query execution plan:", explain_result)

   # Generate SQL without executing
   sql = query.filter(logical_in("category", ["Electronics"])).to_sql()
   print("Generated SQL:", sql)

Vector Operations Example
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import numpy as np
   from matrixone import Client
   from matrixone.config import get_connection_params

   # Get connection parameters
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

   # Create HNSW index for similarity search
   client.vector.create_hnsw(
       table_name="documents",
       name="idx_embedding",
       column="embedding",
       m=16,
       ef_construction=200
   )

   # Insert documents with embeddings
   documents_data = [
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
       }
   ]
   client.batch_insert("documents", documents_data)

   # Vector similarity search
   query_vector = np.random.rand(384).astype(np.float32).tolist()
   results = client.vector_query.similarity_search(
       table_name="documents",
       vector_column="embedding",
       query_vector=query_vector,
       limit=5,
       distance_function="cosine"
   )

   print("Similar documents:")
   for result in results:
       print(f"  ID: {result[0]}, Title: {result[1]}, Distance: {result[-1]:.4f}")

Fulltext Search Example
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.sqlalchemy_ext import boolean_match
   from matrixone.orm import logical_in
   from matrixone.config import get_connection_params

   # Get connection parameters
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Create fulltext index
   client.fulltext_index.create("products", "idx_description", "description", algorithm="BM25")

   # Simple fulltext search
   results = client.fulltext_index.simple_query(
       table_name="products",
       columns=["description"],
       query="laptop OR phone",
       limit=10
   )
   print("Fulltext search results:", results)

   # Advanced fulltext search with boolean expressions
   fulltext_filter = boolean_match("description").must("laptop").should("phone").must_not("broken")
   results = client.query("products").filter(logical_in("id", fulltext_filter)).all()
   print("Advanced fulltext search results:", results)

SQLAlchemy ORM Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Column, Integer, String, Text
   from sqlalchemy.ext.declarative import declarative_base
   from sqlalchemy.orm import sessionmaker
   from matrixone import Client

   # Create declarative base
   Base = declarative_base()

   class User(Base):
       __tablename__ = 'users'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       name = Column(String(100), nullable=False)
       email = Column(String(200), unique=True, nullable=False)
       bio = Column(Text)
       
       def to_dict(self):
           return {c.name: getattr(self, c.name) for c in self.__table__.columns}

   # Connect to MatrixOne
   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Create tables using ORM interface
   client.create_all(Base)

   # Use SQLAlchemy session
   engine = client.get_sqlalchemy_engine()
   Session = sessionmaker(bind=engine)
   session = Session()

   # Create and insert users
   user1 = User(name='John Doe', email='john@example.com', bio='Software developer')
   user2 = User(name='Jane Smith', email='jane@example.com', bio='Data scientist')
   
   session.add_all([user1, user2])
   session.commit()

   # Query users with ORM
   users = session.query(User).filter(User.name.like('%John%')).all()
   for user in users:
       print(f"User: {user.name}, Email: {user.email}")

   session.close()
   client.disconnect()

Transaction Management
----------------------

Using Client Transaction Interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Column, Integer, String, DECIMAL
   from sqlalchemy.ext.declarative import declarative_base
   from matrixone import Client

   Base = declarative_base()

   class Account(Base):
       __tablename__ = 'accounts'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       name = Column(String(100), nullable=False)
       balance = Column(DECIMAL(10, 2), nullable=False)

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Create table using ORM interface
   client.create_all(Base)

   # Insert initial data
   accounts_data = [
       {'name': 'Alice', 'balance': 1000.00},
       {'name': 'Bob', 'balance': 500.00}
   ]
   
   for account in accounts_data:
       client.execute(
           "INSERT INTO accounts (name, balance) VALUES (%s, %s)",
           (account['name'], account['balance'])
       )

   # Transfer money using client transaction interface
   with client.transaction() as tx:
       # Debit from Alice
       tx.execute(
           "UPDATE accounts SET balance = balance - %s WHERE name = %s",
           (100.00, 'Alice')
       )
       # Credit to Bob
       tx.execute(
           "UPDATE accounts SET balance = balance + %s WHERE name = %s",
           (100.00, 'Bob')
       )
       # Transaction commits automatically on success

   # Verify the transfer
   result = client.execute("SELECT name, balance FROM accounts ORDER BY name")
   for row in result.fetchall():
       print(f"{row[0]}: ${row[1]}")

   client.disconnect()

Vector Search and Indexing
---------------------------

Vector Index with ORM and Client Interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
       embedding = create_vector_column(384, "f32")  # 384-dimensional f32 vector

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Create table using ORM interface
   client.create_all(Base)

   # Enable and create IVF vector index using client interface
   client.vector_index.enable_ivf()
   client.vector_index.create_ivf(
       table_name='documents',
       name='idx_document_embedding',
       column='embedding',
       lists=100,
       op_type='vector_l2_ops'
   )

   # Insert sample documents
   documents_data = [
       {
           'title': 'AI Research Paper',
           'content': 'This paper discusses artificial intelligence',
           'embedding': [0.1, 0.2, 0.3] + [0.0] * 381
       },
       {
           'title': 'Machine Learning Guide',
           'content': 'Learn machine learning concepts',
           'embedding': [0.4, 0.5, 0.6] + [0.0] * 381
       }
   ]

   # Insert documents using ORM
   from sqlalchemy.orm import sessionmaker
   
   Session = sessionmaker(bind=client.get_sqlalchemy_engine())
   session = Session()
   
   documents = [
       Document(
           title=doc['title'],
           content=doc['content'],
           embedding=doc['embedding']
       ) for doc in documents_data
   ]
   
   session.add_all(documents)
   session.commit()
   session.close()

   # Vector similarity search using client interface
   query_vector = [0.1, 0.2, 0.3] + [0.0] * 381
   
   results = client.vector_query.similarity_search(
       table_name='documents',
       vector_column='embedding',
       query_vector=query_vector,
       limit=5,
       distance_type='l2',
       select_columns=['id', 'title', 'content']
   )

   print("Vector Search Results:")
   for result in results:
       print(f"Document {result[0]}: {result[1]}")
       print(f"  L2 Distance: {result[-1]:.4f}")

   client.disconnect()

HNSW Vector Index
~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Column, Integer, String
   from sqlalchemy.ext.declarative import declarative_base
   from matrixone import Client
   from matrixone.sqlalchemy_ext import create_vector_column

   HNSWBase = declarative_base()

   class HNSWDocument(HNSWBase):
       __tablename__ = 'hnsw_documents'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(200), nullable=False)
       embedding = create_vector_column(128, "f32")

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Create table using ORM interface
   client.create_all(HNSWBase)

   # Enable and create HNSW vector index using client interface
   client.vector_index.enable_hnsw()
   client.vector_index.create_hnsw(
       table_name='hnsw_documents',
       name='idx_hnsw_embedding',
       column='embedding',
       m=16,
       ef_construction=200,
       ef_search=50,
       op_type='vector_l2_ops'
   )

   # Insert and search data
   hnsw_docs = [
       {'title': f'HNSW Document {i}', 'embedding': [i * 0.1] * 128}
       for i in range(1, 6)
   ]

   # Insert HNSW documents using ORM
   Session = sessionmaker(bind=client.get_sqlalchemy_engine())
   session = Session()
   
   hnsw_documents = [
       HNSWDocument(
           title=doc['title'],
           embedding=doc['embedding']
       ) for doc in hnsw_docs
   ]
   
   session.add_all(hnsw_documents)
   session.commit()
   session.close()

   # Search using HNSW index
   query_vector = [0.2] * 128
   results = client.vector_query.similarity_search(
       table_name='hnsw_documents',
       vector_column='embedding',
       query_vector=query_vector,
       limit=3,
       distance_type='l2'
   )

   print("HNSW Search Results:")
   for result in results:
       print(f"Document {result[0]}: {result[1]} (Distance: {result[-1]:.4f})")

   client.disconnect()

Fulltext Search
---------------

Fulltext Index with Client Interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Column, Integer, String, Text
   from sqlalchemy.ext.declarative import declarative_base
   from matrixone import Client, FulltextAlgorithmType, FulltextModeType

   FulltextBase = declarative_base()

   class Article(FulltextBase):
       __tablename__ = 'articles'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(200), nullable=False)
       content = Column(Text, nullable=False)
       author = Column(String(100))
       tags = Column(String(200))

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Create table using ORM interface
   client.create_all(FulltextBase)

   # Enable and create fulltext index using client interface
   client.fulltext_index.enable_fulltext()
   client.fulltext_index.create(
       table_name='articles',
       name='ftidx_article_content',
       columns=['title', 'content', 'tags'],
       algorithm=FulltextAlgorithmType.BM25
   )

   # Insert sample articles
   articles_data = [
       {
           'title': 'Database Management Systems',
           'content': 'Learn about database management systems and their applications',
           'author': 'John Doe',
           'tags': 'database, systems, software'
       },
       {
           'title': 'Python Programming Guide',
           'content': 'Introduction to Python programming language and best practices',
           'author': 'Jane Smith',
           'tags': 'python, programming, development'
       }
   ]

   for article in articles_data:
       client.execute(
           "INSERT INTO articles (title, content, author, tags) VALUES (%s, %s, %s, %s)",
           (article['title'], article['content'], article['author'], article['tags'])
       )

   # Fulltext search using client interface
   search_results = client.fulltext_index.fulltext_search(
       table_name='articles',
       columns=['title', 'content', 'tags'],
       search_term='Python programming',
       mode=FulltextModeType.NATURAL_LANGUAGE,
       with_score=True,
       limit=5
   )

   print("Fulltext Search Results:")
   for result in search_results:
       print(f"Article: {result.get('title', 'N/A')}")
       print(f"  Score: {result.get('score', 'N/A')}")

   client.disconnect()

Async Operations
----------------

Async ORM Operations
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from sqlalchemy import Column, Integer, String
   from sqlalchemy.ext.declarative import declarative_base
   from matrixone import AsyncClient

   AsyncBase = declarative_base()

   class AsyncUser(AsyncBase):
       __tablename__ = 'async_users'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       name = Column(String(100), nullable=False)
       email = Column(String(200), unique=True, nullable=False)
       
       def to_dict(self):
           return {c.name: getattr(self, c.name) for c in self.__table__.columns}

   async def async_orm_example():
       client = AsyncClient()
       await client.connect(
           host='localhost',
           port=6001,
           user='root',
           password='111',
           database='test'
       )

       # Create tables using async client ORM interface
       await client.create_all(AsyncBase)

       # Insert data using async transaction
       async with client.transaction() as tx:
           await tx.execute(
               "INSERT INTO async_users (name, email) VALUES (%s, %s)",
               ('Async User', 'async@example.com')
           )

       # Query data
       result = await client.execute("SELECT * FROM async_users WHERE name = %s", ('Async User',))
       rows = await result.fetchall()
       for row in rows:
           print(f"Async User: {row[1]}, Email: {row[2]}")

       # Clean up
       await client.drop_all(AsyncBase)
       await client.disconnect()

   asyncio.run(async_orm_example())

Error Handling Best Practices
------------------------------

Robust Database Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.exceptions import ConnectionError, QueryError
   from sqlalchemy import Column, Integer, String
   from sqlalchemy.ext.declarative import declarative_base

   ErrorBase = declarative_base()

   class TestTable(ErrorBase):
       __tablename__ = 'test_error_handling'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       name = Column(String(100), nullable=False)

   def robust_database_operations():
       client = None
       try:
           # Connection with error handling
           client = Client()
           client.connect(
               host='localhost',
               port=6001,
               user='root',
               password='111',
               database='test'
           )
           
           # Table creation with error handling
           try:
               client.create_all(ErrorBase)
               print("✓ Table created successfully")
           except QueryError as e:
               if "already exists" in str(e):
                   print("⚠️  Table already exists, continuing...")
               else:
                   raise

           # Data operations with transaction
           try:
               with client.transaction() as tx:
                   tx.execute(
                       "INSERT INTO test_error_handling (name) VALUES (%s)",
                       ('Test Name',)
                   )
                   print("✓ Data inserted successfully")
           except QueryError as e:
               print(f"❌ Failed to insert data: {e}")

       except ConnectionError as e:
           print(f"❌ Connection failed: {e}")
       except Exception as e:
           print(f"❌ Unexpected error: {e}")
       finally:
           # Always clean up resources
           if client:
               try:
                   client.drop_all(ErrorBase)
                   client.disconnect()
                   print("✓ Cleanup completed")
               except Exception as e:
                   print(f"⚠️  Cleanup warning: {e}")

   robust_database_operations()

Configuration Best Practices
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params, print_config

   def best_practices_example():
       # Use configuration helpers
       print_config()
       host, port, user, password, database = get_connection_params()

       # Create client with optimized settings
       client = Client(
           connection_timeout=30,        # 30 second connection timeout
           query_timeout=300,           # 5 minute query timeout
           auto_commit=True,            # Enable auto-commit for performance
           charset='utf8mb4',           # Use UTF-8 for international characters
           enable_performance_logging=True,  # Enable performance monitoring
           enable_sql_logging=False     # Disable SQL logging in production
       )

       try:
           client.connect(
               host=host,
               port=port,
               user=user,
               password=password,
               database=database
           )
           
           # Verify connection and version
           version = client.get_backend_version()
           print(f"✓ Connected to MatrixOne {version}")
           
           # Check feature availability before using
           if client.is_feature_available('vector_search'):
               print("✓ Vector search is available")
           else:
               hint = client.get_version_hint('vector_search')
               print(f"⚠️  Vector search not available: {hint}")

       except Exception as e:
           print(f"❌ Connection failed: {e}")
       finally:
           client.disconnect()

   best_practices_example()

Next Steps
----------

* Explore the :doc:`api/index` for detailed API documentation
* Check out the :doc:`quickstart` for quick setup instructions
* Learn about :doc:`contributing` to contribute to the project
* Review the ``examples/`` directory for more comprehensive examples
* Run ``make examples`` to test all examples with your MatrixOne setup
