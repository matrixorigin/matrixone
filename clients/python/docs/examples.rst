Examples
========

This section provides comprehensive examples of using the MatrixOne Python SDK.

Basic Operations
----------------

Connection and Query Execution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client

   # Create client and connect
   client = Client()
   client.connect(
       host='localhost',
       port=6001,
       user='root',
       password='111',
       database='test'
   )

   # Execute a simple query
   result = client.execute("SELECT 1 as test_value, USER() as user_info")
   print(result.fetchall())

   # Execute with parameters
   result = client.execute(
       "SELECT * FROM users WHERE age > %s",
       (18,)
   )
   for row in result:
       print(f"User: {row[1]}, Age: {row[2]}")

   client.disconnect()

Async Operations
~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient

   async def main():
       client = AsyncClient()
       await client.connect(
           host='localhost',
           port=6001,
           user='root',
           password='111',
           database='test'
       )

       # Execute async query
       result = await client.execute("SELECT COUNT(*) FROM users")
       count = await result.fetchone()
       print(f"Total users: {count[0]}")

       await client.disconnect()

   asyncio.run(main())

Transaction Management
----------------------

Basic Transactions
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Using context manager (recommended)
   with client.transaction() as tx:
       tx.execute("INSERT INTO users (name, email) VALUES (%s, %s)", 
                  ("John Doe", "john@example.com"))
       tx.execute("INSERT INTO users (name, email) VALUES (%s, %s)", 
                  ("Jane Smith", "jane@example.com"))
       # Transaction commits automatically on success

   # Manual transaction control
   tx = client.begin_transaction()
   try:
       tx.execute("UPDATE users SET last_login = NOW() WHERE id = %s", (user_id,))
       tx.execute("INSERT INTO login_log (user_id, login_time) VALUES (%s, NOW())", 
                  (user_id,))
       tx.commit()
   except Exception as e:
       tx.rollback()
       raise e

Snapshot Management
-------------------

Creating and Managing Snapshots
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Create a snapshot
   snapshot = client.snapshots.create(
       name='backup_before_migration',
       level='cluster',
       description='Full cluster backup before data migration'
   )
   print(f"Created snapshot: {snapshot.name}")

   # List all snapshots
   snapshots = client.snapshots.list()
   for snap in snapshots:
       print(f"Snapshot: {snap.name}, Created: {snap.created_at}")

   # Get snapshot details
   details = client.snapshots.get('backup_before_migration')
   print(f"Snapshot size: {details.size}")

   # Clone database from snapshot
   client.snapshots.clone_database(
       target_db='restored_database',
       source_db='original_database',
       snapshot_name='backup_before_migration'
   )

Point-in-Time Recovery (PITR)
------------------------------

Creating PITR
~~~~~~~~~~~~~

.. code-block:: python

   from datetime import datetime, timedelta

   # Create PITR for cluster
   pitr = client.pitr.create_cluster_pitr(
       name='daily_backup',
       range_value=7,
       range_unit='d'
   )

   # Create PITR for specific database
   pitr = client.pitr.create_database_pitr(
       name='user_db_backup',
       database='user_database',
       range_value=24,
       range_unit='h'
   )

   # Restore to specific time
   restore_point = datetime.now() - timedelta(hours=2)
   client.pitr.restore_to_time(restore_point)

Account Management
------------------

User and Role Management
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Create a new user
   user = client.account.create_user(
       username='newuser',
       password='secure_password',
       description='New application user'
   )

   # Create a role
   role = client.account.create_role(
       role_name='data_analyst',
       description='Role for data analysis tasks'
   )

   # Grant privileges
   client.account.grant_privilege(
       user='newuser',
       role='data_analyst',
       privileges=['SELECT', 'INSERT', 'UPDATE']
   )

   # List all users
   users = client.account.list_users()
   for user in users:
       print(f"User: {user.name}, Created: {user.created_at}")

Pub/Sub Operations
------------------

Publication and Subscription
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Create a publication
   publication = client.pubsub.create_publication(
       name='user_changes',
       tables=['users', 'user_profiles'],
       description='Publication for user data changes'
   )

   # Create a subscription
   subscription = client.pubsub.create_subscription(
       name='user_sync',
       publication_name='user_changes',
       target_tables=['users_backup', 'user_profiles_backup']
   )

   # List publications
   publications = client.pubsub.list_publications()
   for pub in publications:
       print(f"Publication: {pub.name}, Tables: {pub.tables}")

Version Management
------------------

Feature Detection and Compatibility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Check if feature is available
   if client.is_feature_available('snapshot_creation'):
       snapshot = client.snapshots.create('test_snapshot', 'cluster')
   else:
       hint = client.get_version_hint('snapshot_creation')
       print(f"Feature not available: {hint}")

   # Check version compatibility
   if client.check_version_compatibility('3.0.0', '>='):
       print("Backend supports 3.0.0+ features")
   else:
       print("Backend version is too old")

   # Get backend version
   version = client.get_backend_version()
   print(f"MatrixOne version: {version}")

   # Check if running development version
   if client.is_development_version():
       print("Running development version - all features available")

Error Handling
--------------

Comprehensive Error Handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone.exceptions import (
       ConnectionError,
       QueryError,
       VersionError,
       SnapshotError,
       AccountError
   )

   try:
       # Attempt to create a snapshot
       snapshot = client.snapshots.create('test_snapshot', 'cluster')
   except VersionError as e:
       print(f"Version compatibility error: {e}")
       print(f"Required version: {e.required_version}")
   except SnapshotError as e:
       print(f"Snapshot operation failed: {e}")
       print(f"Error code: {e.error_code}")
   except ConnectionError as e:
       print(f"Connection failed: {e}")
   except Exception as e:
       print(f"Unexpected error: {e}")

Configuration and Logging
-------------------------

Custom Configuration
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client, MatrixOneLogger

   # Create custom logger
   logger = MatrixOneLogger(
       level=logging.INFO,
       enable_performance_logging=True,
       enable_slow_sql_logging=True,
       slow_sql_threshold=1.0
   )

   # Create client with custom configuration
   client = Client(
       connection_timeout=60,
       query_timeout=600,
       auto_commit=False,
       charset='utf8mb4',
       logger=logger,
       enable_performance_logging=True,
       enable_sql_logging=True
   )

SQLAlchemy Integration
----------------------

Using with SQLAlchemy
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import create_engine, text
   from matrixone import Client

   # Get SQLAlchemy engine from client
   engine = client.get_sqlalchemy_engine()

   # Use with SQLAlchemy
   with engine.connect() as conn:
       result = conn.execute(text("SELECT * FROM users"))
       for row in result:
           print(row)

   # Or use client's SQLAlchemy integration
   with client.sqlalchemy_session() as session:
       result = session.execute(text("SELECT COUNT(*) FROM users"))
       count = result.scalar()
       print(f"Total users: {count}")

Fulltext Search
---------------

Basic Fulltext Index Creation with Client Interfaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client

   # Create client and connect
   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Enable fulltext indexing
   client.execute("SET experimental_fulltext_index = 1")

   # Create table using client interface
   client.create_table(
       table_name='articles',
       columns={
           'id': 'INT PRIMARY KEY',
           'title': 'VARCHAR(200)',
           'content': 'TEXT',
           'author': 'VARCHAR(100)'
       }
   )

   # Create fulltext index using client interface
   client.fulltext_index.create(
       table_name='articles',
       name='ftidx_article_content',
       columns=['title', 'content'],
       algorithm='TF-IDF'
   )

   # Insert article data using client interface
   articles_data = [
       {'id': 1, 'title': 'Database Management Systems', 'content': 'Learn about database management systems and their applications', 'author': 'John Doe'},
       {'id': 2, 'title': 'Python Programming Guide', 'content': 'Introduction to Python programming language and best practices', 'author': 'Jane Smith'},
       {'id': 3, 'title': 'Web Development with Python', 'content': 'Building modern web applications with Python frameworks', 'author': 'Bob Johnson'}
   ]
   
   for article in articles_data:
       client.insert('articles', article)

   # Search using natural language mode with client interface
   search_sql = client.search(
       table_name='articles',
       columns=['title', 'content'],
       search_term='database programming',
       mode='natural language mode'
   ).build_sql()

   # Execute the search
   search_result = client.execute(search_sql)
   for row in search_result.rows:
       print(f"Article {row[0]}: {row[1]} by {row[3]} (Score: {row[4] if len(row) > 4 else 'N/A'})")

   client.disconnect()

Advanced Search with Builder and Client Interfaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone.sqlalchemy_ext import FulltextSearchBuilder

   # Create a search builder
   builder = FulltextSearchBuilder('articles', ['title', 'content'])
   
   # Build a complex search query
   sql = (builder
          .search('python programming')
          .set_mode('natural language')
          .set_with_score(True)
          .where('id > 0')
          .set_order_by('score', 'DESC')
          .limit(5)
          .build_sql())
   
   print("Generated SQL:", sql)
   
   # Execute using client interface
   search_sql = client.search(
       table_name='articles',
       columns=['title', 'content'],
       search_term='python programming',
       mode='natural language mode'
   ).where('id > 0').order_by('score', 'DESC').limit(5).build_sql()
   
   # Execute the search
   search_result = client.execute(search_sql)
   for row in search_result.rows:
       print(f"Found: {row[1]} (Score: {row[3] if len(row) > 3 else 'N/A'})")

Boolean Mode Search with Client Interfaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Boolean mode search with operators using client interface
   
   # Search for articles containing 'python' but not 'database'
   search_sql = client.search(
       table_name='articles',
       columns=['title', 'content'],
       search_term='+python -database',
       mode='boolean mode'
   ).limit(10).build_sql()
   
   search_result = client.execute(search_sql)
   for row in search_result.rows:
       print(f"Boolean Search: {row[1]}")
   
   # Search for exact phrase
   search_sql = client.search(
       table_name='articles',
       columns=['title', 'content'],
       search_term='"web development"',
       mode='boolean mode'
   ).limit(10).build_sql()
   
   search_result = client.execute(search_sql)
   for row in search_result.rows:
       print(f"Phrase Search: {row[1]}")
   
   # Complex boolean search with client interface
   search_sql = client.search(
       table_name='articles',
       columns=['title', 'content'],
       search_term='+(python OR programming) -database +web',
       mode='boolean mode'
   ).order_by('score', 'DESC').limit(10).build_sql()
   
   search_result = client.execute(search_sql)
   for row in search_result.rows:
       print(f"Complex Boolean: {row[1]} (Score: {row[3] if len(row) > 3 else 'N/A'})")

Async Fulltext Operations with Client Interfaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient

   async def fulltext_async_example():
       async_client = AsyncClient()
       await async_client.connect(
           host='localhost', 
           port=6001, 
           user='root', 
           password='111', 
           database='test'
       )

       # Enable fulltext indexing
       await async_client.execute("SET experimental_fulltext_index = 1")

       # Create table using async client interface
       await async_client.create_table(
           table_name='async_articles',
           columns={
               'id': 'INT PRIMARY KEY',
               'title': 'VARCHAR(200)',
               'content': 'TEXT',
               'author': 'VARCHAR(100)'
           }
       )

       # Create fulltext index asynchronously using client interface
       await async_client.fulltext_index.create(
           table_name='async_articles',
           name='ftidx_async',
           columns=['title', 'content']
       )

       # Insert data asynchronously using client interface
       articles_data = [
           {'id': 1, 'title': 'Async Python Guide', 'content': 'Learn async Python programming', 'author': 'Async Author'},
           {'id': 2, 'title': 'Async Database Operations', 'content': 'Async database programming patterns', 'author': 'DB Expert'}
       ]
       
       for article in articles_data:
           await async_client.insert('async_articles', article)

       # Search asynchronously using client interface
       search_sql = await async_client.search(
           table_name='async_articles',
           columns=['title', 'content'],
           search_term='python programming',
           mode='natural language mode'
       ).limit(10).build_sql()

       # Execute the search
       search_result = await async_client.execute(search_sql)
       for row in search_result.rows:
           print(f"Found: {row[1]} by {row[3]} (Score: {row[4] if len(row) > 4 else 'N/A'})")

       await async_client.disconnect()

   asyncio.run(fulltext_async_example())

JSON and Chinese Text Search with Client Interfaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Create additional tables for JSON and Chinese text search using client interface
   
   # Create products table with JSON column
   client.create_table(
       table_name='products',
       columns={
           'id': 'INT PRIMARY KEY',
           'name': 'VARCHAR(100)',
           'details': 'JSON'
       }
   )

   # Create fulltext index with JSON parser using client interface
   client.fulltext_index.create(
       table_name='products',
       name='ftidx_json',
       columns=['details'],
       algorithm='TF-IDF'
   )

   # Insert JSON data using client interface
   products_data = [
       {'id': 1, 'name': 'Laptop', 'details': {'brand': 'Dell', 'specs': 'i7, 16GB RAM', 'price': 1200}},
       {'id': 2, 'name': 'Phone', 'details': {'brand': 'Apple', 'model': 'iPhone 12', 'price': 800}},
       {'id': 3, 'name': 'Tablet', 'details': {'brand': 'Samsung', 'model': 'Galaxy Tab', 'price': 600}}
   ]
   
   for product in products_data:
       client.insert('products', product)

   # Search JSON content using client interface
   search_sql = client.search(
       table_name='products',
       columns=['details'],
       search_term='Dell',
       mode='natural language mode'
   ).limit(10).build_sql()

   search_result = client.execute(search_sql)
   for row in search_result.rows:
       print(f"Product: {row[1]}, Details: {row[2]}")

   # Create Chinese articles table
   client.create_table(
       table_name='chinese_articles',
       columns={
           'id': 'INT PRIMARY KEY',
           'title': 'VARCHAR(200)',
           'content': 'TEXT'
       }
   )

   # Create fulltext index with ngram parser for Chinese using client interface
   client.fulltext_index.create(
       table_name='chinese_articles',
       name='ftidx_chinese',
       columns=['title', 'content'],
       algorithm='TF-IDF'
   )

   # Insert Chinese text data using client interface
   chinese_articles_data = [
       {'id': 1, 'title': '数据库管理', 'content': '学习数据库管理系统的基本概念和最佳实践'},
       {'id': 2, 'title': 'Python编程', 'content': 'Python编程语言入门教程和高级技巧'},
       {'id': 3, 'title': '机器学习', 'content': '机器学习算法原理和应用实例'}
   ]
   
   for article in chinese_articles_data:
       client.insert('chinese_articles', article)

   # Search Chinese text using client interface
   search_sql = client.search(
       table_name='chinese_articles',
       columns=['title', 'content'],
       search_term='数据库',
       mode='natural language mode'
   ).limit(10).build_sql()

   search_result = client.execute(search_sql)
   for row in search_result.rows:
       print(f"Chinese Article: {row[1]} - {row[2]}")

Vector Search
-------------

Basic Vector Index Creation with Client Interfaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.sqlalchemy_ext import HNSWConfig

   # Create client and connect
   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Create table using client interface
   client.create_table(
       table_name='documents',
       columns={
           'id': 'INT PRIMARY KEY',
           'title': 'VARCHAR(200)',
           'content': 'TEXT',
           'embedding': 'VECTOR(384)'
       }
   )

   # Create IVF vector index using client interface
   client.vector_index.create_ivf(
       table_name='documents',
       name='idx_document_embedding',
       vector_column='embedding'
   )

   # Insert vector data using client interface
   documents_data = [
       {'id': 1, 'title': 'AI Research Paper', 'content': 'This paper discusses AI', 
        'embedding': [0.1, 0.2, 0.3] + [0.0] * 381},
       {'id': 2, 'title': 'Machine Learning Guide', 'content': 'Learn ML concepts', 
        'embedding': [0.4, 0.5, 0.6] + [0.0] * 381},
       {'id': 3, 'title': 'Deep Learning Tutorial', 'content': 'Deep learning basics', 
        'embedding': [0.7, 0.8, 0.9] + [0.0] * 381}
   ]
   
   for doc in documents_data:
       client.vector_data.insert('documents', doc)

   # Search for similar vectors using client interface
   query_vector = [0.1, 0.2, 0.3] + [0.0] * 381
   results = client.vector_query.similarity_search(
       table_name='documents',
       vector_column='embedding',
       query_vector=query_vector,
       limit=5,
       distance_type='l2'
   )

   for result in results:
       print(f"Document {result[0]}: {result[1]} (Distance: {result[-1]})")

   client.disconnect()

Advanced Vector Operations with Client Interfaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone.sqlalchemy_ext import IVFConfig

   # Create a new table for IVF index demonstration using client interface
   client.create_table(
       table_name='products',
       columns={
           'id': 'INT PRIMARY KEY',
           'name': 'VARCHAR(200)',
           'description': 'TEXT',
           'features': 'VECTOR(512)'
       }
   )

   # Create IVF vector index using client interface
   client.vector_index.create_ivf(
       table_name='products',
       name='idx_product_features',
       vector_column='features'
   )

   # Insert product data using client interface
   products_data = [
       {'id': 1, 'name': 'Laptop', 'description': 'High-performance laptop', 
        'features': [0.1, 0.2, 0.3] + [0.0] * 509},
       {'id': 2, 'name': 'Smartphone', 'description': 'Latest smartphone', 
        'features': [0.4, 0.5, 0.6] + [0.0] * 509},
       {'id': 3, 'name': 'Tablet', 'description': 'Portable tablet device', 
        'features': [0.7, 0.8, 0.9] + [0.0] * 509}
   ]
   
   for product in products_data:
       client.vector_data.insert('products', product)

   # Search using different distance metrics with client interface
   query_features = [0.1, 0.2, 0.3] + [0.0] * 509
   
   # L2 distance search
   l2_results = client.vector_query.similarity_search(
       table_name='products',
       vector_column='features',
       query_vector=query_features,
       limit=5,
       distance_type='l2'
   )

   for result in l2_results:
       print(f"Product {result[0]}: {result[1]}")
       print(f"  L2 distance: {result[-1]}")
   
   # Cosine distance search
   cosine_results = client.vector_query.similarity_search(
       table_name='products',
       vector_column='features',
       query_vector=query_features,
       limit=5,
       distance_type='cosine'
   )

   for result in cosine_results:
       print(f"Product {result[0]}: {result[1]}")
       print(f"  Cosine distance: {result[-1]}")
   
   # Inner product search
   inner_results = client.vector_query.similarity_search(
       table_name='products',
       vector_column='features',
       query_vector=query_features,
       limit=5,
       distance_type='inner_product'
   )

   for result in inner_results:
       print(f"Product {result[0]}: {result[1]}")
       print(f"  Inner product: {result[-1]}")

Vector Index Management with Client Interfaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # List all vector indexes using client interface
   indexes = client.vector_index.list()
   for idx in indexes:
       print(f"Index: {idx.name}, Table: {idx.table_name}")

   # Get index information using client interface
   info = client.vector_index.get('idx_document_embedding')
   print(f"Index algorithm: {info.algorithm}")
   print(f"Index config: {info.config}")

   # Rebuild index using client interface
   client.vector_index.rebuild('idx_document_embedding')

   # Drop vector index using client interface
   client.vector_index.drop('idx_document_embedding')

   # Create a new index with different configuration using client interface
   client.vector_index.create_ivf(
       table_name='documents',
       name='idx_document_embedding_v2',
       vector_column='embedding'
   )
   
   # Update index configuration using client interface
   client.vector_index.update_config(
       index_name='idx_document_embedding_v2',
       config={'nlist': 200, 'nprobe': 20}
   )

Async Vector Operations with Client Interfaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   from matrixone.sqlalchemy_ext import HNSWConfig

   async def vector_async_example():
       async_client = AsyncClient()
       await async_client.connect(
           host='localhost', 
           port=6001, 
           user='root', 
           password='111', 
           database='test'
       )

       # Create table using async client interface
       await async_client.create_table(
           table_name='async_documents',
           columns={
               'id': 'INT PRIMARY KEY',
               'title': 'VARCHAR(200)',
               'content': 'TEXT',
               'embedding': 'VECTOR(384)'
           }
       )

       # Create vector index asynchronously using client interface
       await async_client.vector_index.create_ivf(
           table_name='async_documents',
           name='idx_async',
           vector_column='embedding'
       )

       # Insert data asynchronously using client interface
       documents_data = [
           {'id': 1, 'title': 'Async AI Research', 'content': 'This is async AI research', 
            'embedding': [0.1, 0.2, 0.3] + [0.0] * 381},
           {'id': 2, 'title': 'Async ML Guide', 'content': 'Async ML concepts', 
            'embedding': [0.4, 0.5, 0.6] + [0.0] * 381}
       ]
       
       for doc in documents_data:
           await async_client.vector_data.insert('async_documents', doc)

       # Search asynchronously using client interface
       query_vector = [0.1, 0.2, 0.3] + [0.0] * 381
       results = await async_client.vector_query.similarity_search(
           table_name='async_documents',
           vector_column='embedding',
           query_vector=query_vector,
           limit=10,
           distance_type='l2'
       )

       for result in results:
           print(f"Found: {result[1]} (Distance: {result[-1]})")

       await async_client.disconnect()

   asyncio.run(vector_async_example())

Hybrid Vector and Fulltext Search with ORM
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import text, select, and_, or_, func, case
   from sqlalchemy.orm import sessionmaker, declarative_base
   from matrixone import Client
   from matrixone.sqlalchemy_ext import VectorType, FulltextIndex, VectorIndex

   # Create declarative base
   Base = declarative_base()

   class HybridDocument(Base):
       __tablename__ = 'hybrid_documents'
       
       id = Column(Integer, primary_key=True)
       title = Column(String(200))
       content = Column(String(1000))
       embedding = Column(VectorType(384))
       category = Column(String(50))
       tags = Column(String(200))
       
       # Define vector index in the model
       vector_index = VectorIndex(
           name='idx_hybrid_embedding',
           column='embedding',
           algorithm='ivf'
       )
       
       # Define fulltext index in the model
       fulltext_index = FulltextIndex(
           name='ftidx_hybrid_content',
           columns=['title', 'content', 'tags'],
           algorithm='TF-IDF'
       )

   # Create client and connect
   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Enable fulltext indexing
   client.execute("SET experimental_fulltext_index = 1")

   # Get SQLAlchemy engine
   engine = client.get_sqlalchemy_engine()

   # Create table and indexes using ORM
   Base.metadata.create_all(engine)

   # Insert data using client interface
   documents_data = [
       {'id': 1, 'title': 'AI Research Paper', 'content': 'This paper discusses AI and machine learning', 
        'embedding': [0.1, 0.2, 0.3] + [0.0] * 381, 'category': 'Research', 'tags': 'AI, ML, research'},
       {'id': 2, 'title': 'Database Optimization', 'content': 'Learn database optimization techniques', 
        'embedding': [0.4, 0.5, 0.6] + [0.0] * 381, 'category': 'Database', 'tags': 'database, optimization, SQL'},
       {'id': 3, 'title': 'Python Web Development', 'content': 'Building web apps with Python frameworks', 
        'embedding': [0.7, 0.8, 0.9] + [0.0] * 381, 'category': 'Web', 'tags': 'python, web, framework'},
       {'id': 4, 'title': 'Machine Learning Tutorial', 'content': 'Complete guide to machine learning algorithms', 
        'embedding': [0.2, 0.3, 0.4] + [0.0] * 381, 'category': 'Tutorial', 'tags': 'ML, tutorial, algorithms'},
       {'id': 5, 'title': 'Vector Database Design', 'content': 'Designing vector databases for AI applications', 
        'embedding': [0.5, 0.6, 0.7] + [0.0] * 381, 'category': 'Database', 'tags': 'vector, database, AI'}
   ]
   
   for doc in documents_data:
       client.vector_data.insert('hybrid_documents', doc)

   # Use SQLAlchemy session for hybrid queries
   Session = sessionmaker(bind=engine)
   session = Session()

   # Example 1: Vector similarity search using client interface
   query_vector = [0.1, 0.2, 0.3] + [0.0] * 381
   
   vector_results = client.vector_query.similarity_search(
       table_name='hybrid_documents',
       vector_column='embedding',
       query_vector=query_vector,
       limit=5,
       distance_type='l2',
       select_columns=['id', 'title', 'content', 'category', 'tags']
   )

   print("=== Vector Similarity Search ===")
   for result in vector_results:
       print(f"Document {result[0]}: {result[1]}")
       print(f"  Category: {result[3]}")
       print(f"  Vector Distance: {result[-1]}")
       print(f"  Content: {result[2][:50]}...")

   # Example 2: Fulltext search using client interface
   fulltext_sql = client.search(
       table_name='hybrid_documents',
       columns=['title', 'content', 'tags'],
       search_term='AI machine learning',
       mode='natural language mode'
   ).limit(5).build_sql()

   fulltext_result = client.execute(fulltext_sql)
   print("\n=== Fulltext Search ===")
   for row in fulltext_result.rows:
       print(f"Document {row[0]}: {row[1]}")
       print(f"  Text Score: {row[5] if len(row) > 5 else 'N/A'}")
       print(f"  Tags: {row[4]}")

   # Example 3: Complex hybrid query using ORM with text() for complex SQL
   # For complex queries with vector operations and fulltext, we use text() with ORM parameters
   complex_result = session.execute(text("""
       SELECT id, title, content, category, tags,
              embedding <-> :query_vector as vector_distance,
              MATCH(title, content, tags) AGAINST(:search_term IN NATURAL LANGUAGE MODE) as text_score
       FROM hybrid_documents
       WHERE (category IN (:cat1, :cat2) OR tags LIKE :tag_pattern)
         AND MATCH(title, content, tags) AGAINST(:search_term IN NATURAL LANGUAGE MODE)
       ORDER BY 
         CASE 
           WHEN embedding <-> :query_vector < 0.5 THEN 1
           WHEN MATCH(title, content, tags) AGAINST(:search_term IN NATURAL LANGUAGE MODE) > 0.5 THEN 2
           ELSE 3
         END,
         embedding <-> :query_vector
       LIMIT 5
   """), {
       'query_vector': query_vector, 
       'search_term': 'database AI',
       'cat1': 'Database',
       'cat2': 'Research',
       'tag_pattern': '%AI%'
   })

   print("\n=== Complex Hybrid Query with ORM ===")
   for row in complex_result:
       print(f"Document {row.id}: {row.title}")
       print(f"  Category: {row.category}")
       print(f"  Text Score: {row.text_score}")
       print(f"  Vector Distance: {row.vector_distance}")
       print(f"  Combined Relevance: {'High' if row.vector_distance < 0.5 or row.text_score > 0.5 else 'Medium'}")

   # Example 4: Aggregation with hybrid search using ORM
   aggregation_result = session.execute(text("""
       SELECT category,
              COUNT(*) as doc_count,
              AVG(embedding <-> :query_vector) as avg_vector_distance,
              AVG(MATCH(title, content, tags) AGAINST(:search_term IN NATURAL LANGUAGE MODE)) as avg_text_score
       FROM hybrid_documents
       WHERE MATCH(title, content, tags) AGAINST(:search_term IN NATURAL LANGUAGE MODE)
       GROUP BY category
       ORDER BY avg_text_score DESC, avg_vector_distance ASC
   """), {'query_vector': query_vector, 'search_term': 'AI database'})

   print("\n=== Aggregation with Hybrid Search using ORM ===")
   for row in aggregation_result:
       print(f"Category: {row.category}")
       print(f"  Document Count: {row.doc_count}")
       print(f"  Average Vector Distance: {row.avg_vector_distance:.4f}")
       print(f"  Average Text Score: {row.avg_text_score:.4f}")

   # Example 5: Simple ORM queries for basic operations
   print("\n=== Simple ORM Queries ===")
   
   # Basic filtering with ORM
   research_docs = session.query(HybridDocument).filter(
       HybridDocument.category == 'Research'
   ).all()
   
   print(f"Found {len(research_docs)} research documents:")
   for doc in research_docs:
       print(f"  - {doc.title} (ID: {doc.id})")
   
   # Text filtering with ORM
   ai_docs = session.query(HybridDocument).filter(
       HybridDocument.tags.like('%AI%')
   ).all()
   
   print(f"\nFound {len(ai_docs)} AI-related documents:")
   for doc in ai_docs:
       print(f"  - {doc.title} (Category: {doc.category})")
   
   # Update with ORM
   tutorial_doc = session.query(HybridDocument).filter(
       HybridDocument.id == 4
   ).first()
   
   if tutorial_doc:
       tutorial_doc.tags = tutorial_doc.tags + ', updated'
       session.commit()
       print(f"\nUpdated document {tutorial_doc.id}: {tutorial_doc.tags}")

   session.close()
   client.disconnect()

Client Transaction and Interface Examples
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.sqlalchemy_ext import VectorType, VectorIndex, HNSWConfig, FulltextIndex

   # Create client and connect
   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Example 1: Using client interfaces for vector operations
   # Create table using client interface
   client.create_table(
       table_name='articles',
       columns={
           'id': 'INT PRIMARY KEY',
           'title': 'VARCHAR(200)',
           'content': 'TEXT',
           'embedding': 'VECTOR(384)'
       }
   )
   
   # Create vector index using client interface
   client.vector_index.create_ivf(
       table_name='articles',
       name='idx_article_embedding',
       vector_column='embedding'
   )
   
   # Insert data using client interface
   articles_data = [
       {'id': 1, 'title': 'AI Research', 'content': 'This article discusses AI', 
        'embedding': [0.1, 0.2, 0.3] + [0.0] * 381},
       {'id': 2, 'title': 'Machine Learning', 'content': 'ML concepts explained', 
        'embedding': [0.4, 0.5, 0.6] + [0.0] * 381},
       {'id': 3, 'title': 'Deep Learning', 'content': 'Deep learning basics', 
        'embedding': [0.7, 0.8, 0.9] + [0.0] * 381}
   ]
   
   for article in articles_data:
       client.vector_data.insert('articles', article)

   # Example 2: Using client similarity search interface
   query_vector = [0.1, 0.2, 0.3] + [0.0] * 381
   
   # Search using client similarity search interface
   results = client.vector_query.similarity_search(
       table_name='articles',
       vector_column='embedding',
       query_vector=query_vector,
       limit=5,
       distance_type='l2'
   )

   for result in results:
       print(f"Article {result[0]}: {result[1]}")
       print(f"  L2 Distance: {result[-1]}")
       print(f"  Content: {result[2]}")
   
   # Search with cosine distance
   cosine_results = client.vector_query.similarity_search(
       table_name='articles',
       vector_column='embedding',
       query_vector=query_vector,
       limit=5,
       distance_type='cosine'
   )

   for result in cosine_results:
       print(f"Article {result[0]}: {result[1]}")
       print(f"  Cosine Distance: {result[-1]}")

   # Example 3: Using client interfaces for fulltext operations
   # Enable fulltext indexing
   client.execute("SET experimental_fulltext_index = 1")
   
   # Create fulltext index using client interface
   client.fulltext_index.create(
       table_name='articles',
       name='ftidx_article_content',
       columns=['title', 'content'],
       algorithm='TF-IDF'
   )

   # Search using client fulltext search interface
   fulltext_results = client.search(
       table_name='articles',
       columns=['title', 'content'],
       search_term='AI machine learning',
       mode='natural language mode'
   ).build_sql()

   # Execute the search
   search_result = client.execute(fulltext_results)
   for row in search_result.rows:
       print(f"Fulltext Match: {row[1]} (Score: {row[3] if len(row) > 3 else 'N/A'})")
       print(f"  Content: {row[2]}")

   # Example 4: Combined vector and fulltext search using ORM
   from sqlalchemy import text
   from sqlalchemy.orm import sessionmaker
   
   # Get SQLAlchemy engine and create session
   engine = client.get_sqlalchemy_engine()
   Session = sessionmaker(bind=engine)
   session = Session()
   
   # Combined search using ORM with raw SQL
   combined_result = session.execute(text("""
       SELECT id, title, content,
              embedding <-> :query_vector as vector_distance,
              MATCH(title, content) AGAINST(:search_term IN NATURAL LANGUAGE MODE) as text_score
       FROM articles
       WHERE MATCH(title, content) AGAINST(:search_term IN NATURAL LANGUAGE MODE)
       ORDER BY (embedding <-> :query_vector) + (1 - MATCH(title, content) AGAINST(:search_term IN NATURAL LANGUAGE MODE)) ASC
       LIMIT 3
   """), {'query_vector': query_vector, 'search_term': 'AI research'})

   for row in combined_result:
       print(f"Combined Search: {row.title}")
       print(f"  Vector Distance: {row.vector_distance}")
       print(f"  Text Score: {row.text_score}")
   
   session.close()
   client.disconnect()
