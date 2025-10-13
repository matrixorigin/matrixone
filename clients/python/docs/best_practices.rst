Best Practices Guide
====================

This guide provides best practices for using the MatrixOne Python SDK effectively in production environments,
with emphasis on using the SDK's high-level APIs and avoiding raw SQL.

SDK-First Development Philosophy
---------------------------------

The MatrixOne Python SDK provides rich, high-level APIs designed to replace raw SQL and complex SQLAlchemy code.
Always prefer SDK APIs for better maintainability, type safety, and feature integration.

Table Operations Best Practices
--------------------------------

Creating Tables with SDK API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params
   
   # Connect using environment configuration
   client = Client()
   host, port, user, password, database = get_connection_params()
   client.connect(host=host, port=port, user=user, password=password, database=database)
   
   # ✅ GOOD: Use create_table API with dictionary
   client.create_table("users", {
       "id": "int",
       "username": "varchar(100)",
       "email": "varchar(255)",
       "created_at": "timestamp",
       "age": "int"
   }, primary_key="id")
   
   # ✅ GOOD: Use create_table with ORM model
   from sqlalchemy import Column, Integer, String, DateTime
   from matrixone.orm import declarative_base
   
   Base = declarative_base()
   
   class Product(Base):
       __tablename__ = 'products'
       id = Column(Integer, primary_key=True)
       name = Column(String(200))
       price = Column(Integer)
       category = Column(String(50))
       created_at = Column(DateTime)
   
   client.create_table(Product)
   
   # ❌ AVOID: Raw SQL for table creation
   # client.execute("CREATE TABLE users (id INT, username VARCHAR(100)...)")

Data Insertion Best Practices
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # ✅ GOOD: Single insert using SDK API
   client.insert("users", {
       "id": 1,
       "username": "alice",
       "email": "alice@example.com",
       "age": 25
   })
   
   # ✅ GOOD: Batch insert for multiple rows - MUCH FASTER
   users_data = [
       {"id": 2, "username": "bob", "email": "bob@example.com", "age": 30},
       {"id": 3, "username": "charlie", "email": "charlie@example.com", "age": 28},
       {"id": 4, "username": "diana", "email": "diana@example.com", "age": 32}
   ]
   client.batch_insert("users", users_data)
   
   # ✅ GOOD: Insert with model class
   client.insert(Product, {
       "id": 101,
       "name": "Laptop",
       "price": 999,
       "category": "Electronics"
   })
   
   # ❌ AVOID: Manual SQL INSERT statements
   # client.execute("INSERT INTO users VALUES (1, 'alice', 'alice@example.com', 25)")
   
   # ❌ AVOID: Loop with individual inserts (use batch_insert instead)
   # for user in users_data:
   #     client.execute(f"INSERT INTO users ...")  # Slow!

Query Building Best Practices
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone.orm import logical_in
   from sqlalchemy import func
   
   # ✅ GOOD: Use query builder for simple queries
   results = client.query("users").filter("age > 25").all()
   
   # ✅ GOOD: Use where with parameters (prevents SQL injection)
   results = client.query("users").where("email = ?", "alice@example.com").all()
   
   # ✅ GOOD: Use logical_in for flexible IN queries
   results = client.query("users").filter(logical_in("age", [25, 28, 30])).all()
   
   # ✅ GOOD: Chain multiple filters
   results = (client.query("users")
              .filter("age > 20")
              .filter(logical_in("username", ["alice", "bob"]))
              .order_by("created_at DESC")
              .limit(10)
              .all())
   
   # ✅ GOOD: Use aggregation functions
   result = (client.query("users")
             .select("age", func.count("id").label("count"))
             .group_by("age")
             .having(func.count("id") > 1)
             .all())
   
   # ✅ GOOD: Use explain for query analysis
   explain_result = client.query("users").filter("age > 25").explain(verbose=True)
   print(explain_result)
   
   # ❌ AVOID: Complex raw SQL that query builder can handle
   # client.execute("SELECT age, COUNT(id) as count FROM users 
   #                 WHERE age > 20 GROUP BY age HAVING COUNT(id) > 1")

Update and Delete Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # ✅ GOOD: Update using query builder
   client.query("users").update({"age": 26}).filter("id = 1").execute()
   
   # ✅ GOOD: Update multiple columns
   client.query("users").update({
       "username": "alice_updated",
       "age": 27
   }).filter("id = 1").execute()
   
   # ✅ GOOD: Delete using query builder
   client.query("users").filter("id = 999").delete()
   
   # ✅ GOOD: Bulk delete with conditions
   client.query("users").filter("age < 18").delete()
   
   # ❌ AVOID: Raw UPDATE/DELETE SQL
   # client.execute("UPDATE users SET age = 26 WHERE id = 1")
   # client.execute("DELETE FROM users WHERE id = 999")

Vector Operations Best Practices
---------------------------------

Creating Vector Tables
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone.sqlalchemy_ext import create_vector_column
   from matrixone.orm import declarative_base
   from sqlalchemy import Column, Integer, String, Text
   
   Base = declarative_base()
   
   # ✅ GOOD: Define vector table with ORM
   class Document(Base):
       __tablename__ = 'documents'
       id = Column(Integer, primary_key=True)
       title = Column(String(200))
       content = Column(Text)
       category = Column(String(50))
       # Vector column for embeddings
       embedding = create_vector_column(384, 'f32')  # 384-dim f32 vectors
   
   client.create_table(Document)
   
   # ✅ GOOD: Create vector table using dictionary API
   client.create_table("articles", {
       "id": "int",
       "title": "varchar(200)",
       "content": "text",
       "embedding": "vecf32(768)"  # 768-dimensional vectors
   }, primary_key="id")

IVF Index Management
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import numpy as np
   
   # ✅ GOOD: Enable IVF indexing
   client.vector_ops.enable_ivf()
   
   # ✅ GOOD: Create IVF index with optimal parameters
   client.vector_ops.create_ivf(
       "documents",  # Table name as positional argument
       name="idx_embedding_ivf",
       column="embedding",
       lists=100,  # Use sqrt(N) to 4*sqrt(N) where N is total vectors
       op_type="vector_l2_ops"
   )
   
   # ✅ GOOD: Insert vectors using SDK API
   vectors_data = []
   for i in range(100):
       vectors_data.append({
           "id": i,
           "title": f"Document {i}",
           "content": f"Content for document {i}",
           "embedding": np.random.rand(384).tolist()
       })
   client.batch_insert("documents", vectors_data)
   
   # ✅ CRITICAL: Monitor IVF index health regularly
   stats = client.vector_ops.get_ivf_stats("documents", "embedding")
   
   counts = stats['distribution']['centroid_count']
   balance_ratio = max(counts) / min(counts) if min(counts) > 0 else float('inf')
   
   print(f"Total centroids: {len(counts)}")
   print(f"Total vectors: {sum(counts)}")
   print(f"Balance ratio: {balance_ratio:.2f}")
   
   # Rebuild if imbalanced
   if balance_ratio > 2.5:
       print("⚠️  Index needs rebuilding")
       client.vector_ops.drop("documents", "idx_embedding_ivf")
       client.vector_ops.create_ivf("documents", "idx_embedding_ivf", "embedding", lists=100)
   
   # ❌ AVOID: Raw SQL for vector indexing
   # client.execute("CREATE INDEX idx_embedding ON documents USING ivf (embedding vector_l2_ops) LISTS = 100")

Vector Similarity Search
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # ✅ GOOD: Use vector_ops.similarity_search API
   query_vector = np.random.rand(384).tolist()
   
   results = client.vector_ops.similarity_search(
       "documents",  # Table name as positional argument
       vector_column="embedding",
       query_vector=query_vector,
       limit=10,
       distance_type="l2"
   )
   
   print(f"Found {len(results.rows)} similar documents")
   for row in results.rows:
       print(f"ID: {row[0]}, Title: {row[1]}, Distance: {row[-1]:.4f}")
   
   # ✅ GOOD: Similarity search with filters
   results = client.vector_ops.similarity_search(
       "documents",  # Table name as positional argument
       vector_column="embedding",
       query_vector=query_vector,
       limit=10,
       distance_type="cosine",
       filter_conditions="category = 'technology'"
   )
   
   # ✅ GOOD: Range search for distance threshold
   results = client.vector_ops.range_search(
       "documents",  # Table name as positional argument
       vector_column="embedding",
       query_vector=query_vector,
       max_distance=0.5,
       distance_type="l2"
   )
   
   # ❌ AVOID: Raw SQL for vector search
   # client.execute("SELECT * FROM documents ORDER BY l2_distance(embedding, ?) LIMIT 10")

HNSW Index for High Accuracy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # ✅ GOOD: Enable and create HNSW index
   client.vector_ops.enable_hnsw()
   
   client.vector_ops.create_hnsw(
       "documents",  # Table name as positional argument
       name="idx_embedding_hnsw",
       column="embedding",
       m=16,                  # Connections per node (higher = better recall, slower build)
       ef_construction=200,   # Build-time search depth (higher = better quality)
       op_type="vector_l2_ops"
   )
   
   # Search works the same way - SDK automatically uses the best index
   results = client.vector_ops.similarity_search(
       "documents",  # Table name as positional argument
       vector_column="embedding",
       query_vector=query_vector,
       limit=10
   )

Fulltext Search Best Practices
-------------------------------

Fulltext Index Creation
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # ✅ GOOD: Enable fulltext indexing first
   client.fulltext_index.enable_fulltext()
   
   # ✅ GOOD: Create fulltext index using SDK API
   client.fulltext_index.create(
       "documents",  # Table name as positional argument
       name="ftidx_content",
       columns=["title", "content"]
   )
   
   # ✅ GOOD: Create index with specific algorithm
   from matrixone import FulltextAlgorithmType
   
   client.fulltext_index.create(
       "documents",  # Table name as positional argument
       name="ftidx_advanced",
       columns=["content"],
       algorithm=FulltextAlgorithmType.BM25
   )
   
   # ❌ AVOID: Raw SQL for fulltext index creation
   # client.execute("CREATE FULLTEXT INDEX ftidx_content ON documents(title, content) 
   #                 WITH PARSER ngram ALGORITHM = BM25")

Fulltext Search Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone.sqlalchemy_ext.fulltext_search import boolean_match
   from matrixone.orm import logical_in
   
   # ✅ GOOD: Boolean search with encourage (like search)
   results = client.query(
       "documents.title",
       "documents.content",
       boolean_match("title", "content").encourage("machine learning")
   ).execute()
   
   print(f"Found {len(results.rows)} results")
   for row in results.rows:
       print(f"Title: {row[0]}, Content: {row[1]}")
   
   # ✅ GOOD: Boolean search with must/should operators
   ft_filter = (boolean_match("title", "content")
                .must("python")
                .should("tensorflow", "pytorch")
                .must_not("deprecated"))
   
   results = client.query(
       "documents.title",
       "documents.content",
       ft_filter
   ).execute()
   
   # ✅ GOOD: Combine fulltext with regular filters
   results = (client.query(
                  "documents.title",
                  "documents.content",
                  boolean_match("title", "content").encourage("deep learning")
              )
              .filter("created_at > '2024-01-01'")
              .limit(10)
              .execute())

Metadata Analysis Best Practices
---------------------------------

Table Metadata Scanning
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # ✅ GOOD: Use metadata.scan for comprehensive table analysis
   metadata = client.metadata.scan(
       dbname="test",
       tablename="users"
   )
   
   print("Column Analysis:")
   for row in metadata:
       print(f"\n{row.column_name} ({row.data_type}):")
       print(f"  - Nullable: {row.is_nullable}")
       print(f"  - Null count: {row.null_count}")
       print(f"  - Distinct values: {row.distinct_count}")
       print(f"  - Min: {row.min_value}, Max: {row.max_value}")
       print(f"  - Avg length: {row.avg_length}")
   
   # ✅ GOOD: Get table-level statistics
   stats = client.metadata.get_table_brief_stats(
       dbname="test",
       tablename="users"
   )
   
   print(f"\nTable Statistics:")
   print(f"  - Total rows: {stats.row_count}")
   print(f"  - Size: {stats.size_bytes / 1024 / 1024:.2f} MB")
   print(f"  - Columns: {stats.column_count}")
   
   # ✅ GOOD: Scan all tables in a database
   all_metadata = client.metadata.scan(dbname="test")
   
   for row in all_metadata:
       print(f"{row.table_name}.{row.column_name}: {row.data_type}")

Transaction Management
----------------------

Basic Transaction Usage
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # ✅ GOOD: Use transaction context manager
   with client.transaction() as tx:
       # Insert user
       tx.insert("users", {
           "id": 100,
           "username": "transaction_user",
           "email": "tx@example.com",
           "age": 30
       })
       
       # Insert related order
       tx.insert("orders", {
           "id": 1,
           "user_id": 100,
           "amount": 99.99,
           "status": "pending"
       })
       
       # Transaction commits automatically if no exception
   
   # On exception, transaction rolls back automatically
   try:
       with client.transaction() as tx:
           tx.insert("users", {"id": 101, "username": "test"})
           raise Exception("Something went wrong")
           tx.insert("orders", {"id": 2, "user_id": 101})  # Never executed
   except Exception as e:
       print(f"Transaction rolled back: {e}")

Advanced Transaction Patterns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # ✅ GOOD: Transaction with query operations
   with client.transaction() as tx:
       # Check balance
       result = tx.query("accounts").filter("id = 1").one()
       balance = result[2]  # Assuming balance is 3rd column
       
       if balance >= 100:
           # Deduct from account
           tx.query("accounts").update({
               "balance": balance - 100
           }).filter("id = 1").execute()
           
           # Record transaction
           tx.insert("transactions", {
               "account_id": 1,
               "amount": -100,
               "type": "withdrawal"
           })
   
   # ✅ GOOD: Batch operations in transaction
   with client.transaction() as tx:
       # Batch insert multiple records
       users = [
           {"id": i, "username": f"user{i}", "email": f"user{i}@example.com"}
           for i in range(200, 300)
       ]
       tx.batch_insert("users", users)
       
       # Update related statistics
       tx.execute("UPDATE user_stats SET total_users = total_users + 100")

Snapshot and PITR Operations
-----------------------------

Snapshot Management
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone.snapshot import SnapshotLevel
   
   # ✅ GOOD: Create cluster-level snapshot
   snapshot = client.snapshots.create(
       name="cluster_backup_20240101",
       level=SnapshotLevel.CLUSTER
   )
   print(f"Created snapshot: {snapshot.name}")
   
   # ✅ GOOD: Create account-level snapshot
   snapshot = client.snapshots.create(
       name="account_backup",
       level=SnapshotLevel.ACCOUNT
   )
   
   # ✅ GOOD: Create database-level snapshot
   snapshot = client.snapshots.create(
       name="db_backup_test",
       level=SnapshotLevel.DATABASE,
       database_name="test"
   )
   
   # ✅ GOOD: List all snapshots
   snapshots = client.snapshots.list()
   for snap in snapshots:
       print(f"{snap.name}: {snap.level}")
   
   # ✅ GOOD: Drop snapshot when no longer needed
   client.snapshots.drop("old_backup_20231201")

Table Cloning
~~~~~~~~~~~~~

.. code-block:: python

   # ✅ GOOD: Clone database
   client.clone.clone_database(
       target_database="test_copy",
       source_database="test",
       snapshot_name="db_backup_test"
   )
   
   # ✅ GOOD: Clone table
   client.clone.clone_table(
       target_table="users_backup",
       source_table="users",
       snapshot_name="users_backup",
       if_not_exists=True
   )
   
   # ✅ GOOD: Clone table within transaction
   with client.transaction() as tx:
       tx.clone.clone_table(
           target_table="users_temp",
           source_table="users",
           snapshot_name="users_backup"
       )

Point-in-Time Recovery (PITR)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # ✅ GOOD: Create PITR for cluster
   pitr_name = "pitr_backup"
   pitr = client.pitr.create_cluster_pitr(
       name=pitr_name,
       range_value=1,
       range_unit="d"  # days
   )
   
   # ✅ GOOD: List PITR snapshots
   pitr_list = client.pitr.list()
   for pitr_item in pitr_list:
       print(f"PITR: {pitr_item}")
   
   # ✅ GOOD: Drop PITR when done
   try:
       client.pitr.drop_cluster_pitr(pitr_name)
   except Exception as e:
       print(f"PITR cleanup: {e}")

Account and User Management
----------------------------

Account Operations
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone.account import AccountManager
   
   # ✅ GOOD: Initialize AccountManager
   account_manager = AccountManager(client)
   
   # ✅ GOOD: Create account (name, admin_name, admin_password)
   account = account_manager.create_account(
       "test_account",
       "admin_user",
       "secure_password_123"
   )
   print(f"Created account: {account.name}")
   
   # ✅ GOOD: List accounts
   accounts = account_manager.list_accounts()
   for acc in accounts:
       print(f"Account: {acc.name}, Status: {acc.status}")
   
   # ✅ GOOD: Drop account
   account_manager.drop_account("test_account")

User and Role Management
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone.account import AccountManager
   
   account_manager = AccountManager(client)
   
   # ✅ GOOD: Create user (user_name, password)
   user = account_manager.create_user("developer", "dev_password_123")
   print(f"Created user: {user.name}")
   
   # ✅ GOOD: Create role (role_name)
   role = account_manager.create_role("data_analyst")
   print(f"Created role: {role.name}")
   
   # ✅ GOOD: Grant privileges on specific table (optional)
   # Note: table must exist, use database.table format
   account_manager.grant_privilege(
       "SELECT",           # privilege
       "TABLE",            # object_type
       "users",       # object_name (database.table)
       to_role="data_analyst"
   )
   
   # ✅ GOOD: Grant role to user (role_name, to_user)
   account_manager.grant_role("data_analyst", "developer")
   print(f"Granted role to user")
   
   # ✅ GOOD: List users
   users = account_manager.list_users()
   for user in users:
       print(f"User: {user.name}")
   
   # ✅ GOOD: Clean up
   account_manager.drop_user("developer")
   account_manager.drop_role("data_analyst")
   
   # ✅ GOOD: List roles
   roles = account_manager.list_roles()
   for role in roles:
       print(f"Role: {role.name}")

Pub/Sub Operations
------------------

Publication and Subscription Management
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # ✅ GOOD: List publications
   publications = client.pubsub.list_publications()
   for pub in publications:
       print(f"Publication: {pub}")
   
   # ✅ GOOD: List subscriptions
   subscriptions = client.pubsub.list_subscriptions()
   for sub in subscriptions:
       print(f"Subscription: {sub}")
   
   # ✅ GOOD: Drop subscription (if exists)
   try:
       client.pubsub.drop_subscription("test_subscription")
   except Exception as e:
       print(f"Drop subscription: {e}")
   
   # ✅ GOOD: Drop publication (if exists)
   try:
       client.pubsub.drop_publication("test_publication")
   except Exception as e:
       print(f"Drop publication: {e}")

Async Operations Best Practices
--------------------------------

Basic Async Usage
~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   from matrixone.config import get_connection_params
   
   async def async_operations():
       # ✅ GOOD: Use AsyncClient for async operations
       client = AsyncClient()
       host, port, user, password, database = get_connection_params()
       await client.connect(host=host, port=port, user=user, password=password, database=database)
       
       try:
           # Create table
           await client.create_table("async_users", {
               "id": "int",
               "username": "varchar(100)",
               "email": "varchar(255)"
           }, primary_key="id")
           
           # Batch insert
           users = [
               {"id": i, "username": f"user{i}", "email": f"user{i}@example.com"}
               for i in range(100)
           ]
           await client.batch_insert("async_users", users)
           
           # Query
           results = await client.query("async_users").filter("id < 10").all()
           print(f"Found {len(results.rows)} users")
           
       finally:
           await client.disconnect()
   
   asyncio.run(async_operations())

Async Vector Operations
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import numpy as np
   
   async def async_vector_operations():
       client = AsyncClient()
       await client.connect(...)
       
       try:
           # Create vector table
           await client.create_table("async_documents", {
               "id": "int",
               "title": "varchar(200)",
               "embedding": "vecf32(384)"
           }, primary_key="id")
           
           # Enable and create IVF index
           await client.vector_ops.enable_ivf()
           await client.vector_ops.create_ivf(
               "async_documents",
               "idx_embedding",
               "embedding",
               lists=50
           )
           
           # Batch insert vectors
           docs = []
           for i in range(100):
               docs.append({
                   "id": i,
                   "title": f"Document {i}",
                   "embedding": np.random.rand(384).tolist()
               })
           await client.batch_insert("async_documents", docs)
           
           # Similarity search
           query_vector = np.random.rand(384).tolist()
           results = await client.vector_ops.similarity_search(
               "async_documents",
               "embedding",
               query_vector,
               limit=10
           )
           
           # Monitor index health
           stats = await client.vector_ops.get_ivf_stats("async_documents", "embedding")
           counts = stats['distribution']['centroid_count']
           print(f"Index health: {len(counts)} centroids, {sum(counts)} vectors")
           
       finally:
           await client.disconnect()

Async Transactions
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   async def async_transaction():
       client = AsyncClient()
       await client.connect(...)
       
       try:
           # ✅ GOOD: Async transaction
           async with client.transaction() as tx:
               await tx.insert("users", {
                   "id": 500,
                   "username": "async_user",
                   "email": "async@example.com"
               })
               
               await tx.insert("orders", {
                   "id": 1000,
                   "user_id": 500,
                   "amount": 199.99
               })
               # Auto-commit on success
               
       finally:
           await client.disconnect()

Performance Optimization
------------------------

Batch Operations
~~~~~~~~~~~~~~~~

.. code-block:: python

   # ✅ GOOD: Always use batch_insert for multiple rows
   # This is 10-100x faster than individual inserts
   large_dataset = []
   for i in range(10000):
       large_dataset.append({
           "id": i,
           "data": f"row_{i}",
           "value": i * 2
       })
   
   client.batch_insert("large_table", large_dataset)
   
   # ❌ AVOID: Loop with individual inserts
   # for row in large_dataset:
   #     client.insert("large_table", row)  # Very slow!

Connection Pooling
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   
   # ✅ GOOD: Configure connection pooling for production
   client = Client(
       pool_size=20,           # Number of connections in pool
       max_overflow=40,        # Additional connections when pool is full
       pool_timeout=30,        # Wait time for connection
       pool_recycle=3600,      # Recycle connections after 1 hour
       connection_timeout=30,  # Connection establishment timeout
       query_timeout=300       # Query execution timeout
   )

Query Optimization
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # ✅ GOOD: Use EXPLAIN to analyze queries
   explain_result = client.query("users").filter("age > 25").explain(verbose=True)
   print(explain_result)
   
   # ✅ GOOD: Add indexes for frequently queried columns
   client.execute("CREATE INDEX idx_users_age ON users(age)")
   client.execute("CREATE INDEX idx_users_email ON users(email)")
   
   # ✅ GOOD: Use limit for large result sets
   results = client.query("users").order_by("created_at DESC").limit(100).all()
   
   # ✅ GOOD: Filter before ordering/grouping
   results = (client.query("users")
              .filter("created_at > '2024-01-01'")  # Filter first
              .order_by("username")                  # Then order
              .limit(50)                             # Then limit
              .all())

Monitoring and Logging
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone.logging import MatrixOneLogger, LogLevel
   
   # ✅ GOOD: Configure logging for production
   logger = MatrixOneLogger(
       level=LogLevel.INFO,        # INFO level for production
       log_file="matrixone.log",   # Log to file
       max_bytes=10485760,         # 10MB max file size
       backup_count=5              # Keep 5 backup files
   )
   
   client = Client(
       logger=logger,
       sql_log_mode='simple',        # Simple SQL logging
       slow_query_threshold=1.0      # Log queries > 1 second
   )

Error Handling Best Practices
------------------------------

Comprehensive Exception Handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone.exceptions import (
       ConnectionError,
       QueryError,
       TransactionError,
       SnapshotError
   )
   
   # ✅ GOOD: Handle specific exceptions
   try:
       client.connect(...)
   except ConnectionError as e:
       print(f"Failed to connect: {e}")
       # Implement retry logic or fallback
   
   # ✅ GOOD: Handle transaction errors
   try:
       with client.transaction() as tx:
           tx.insert("users", {...})
           tx.insert("orders", {...})
   except TransactionError as e:
       print(f"Transaction failed: {e}")
       # Transaction auto-rolled back
   
   # ✅ GOOD: Handle query errors with retry
   max_retries = 3
   for attempt in range(max_retries):
       try:
           results = client.query("users").all()
           break
       except QueryError as e:
           if attempt == max_retries - 1:
               raise
           print(f"Query failed, retrying ({attempt + 1}/{max_retries})...")
           time.sleep(1)

Resource Cleanup
~~~~~~~~~~~~~~~~

.. code-block:: python

   from contextlib import contextmanager
   
   # ✅ GOOD: Use context managers for automatic cleanup
   @contextmanager
   def get_client():
       client = Client()
       client.connect(...)
       try:
           yield client
       finally:
           client.disconnect()
   
   # Usage
   with get_client() as client:
       results = client.query("users").all()
       # Client automatically disconnected

Testing Best Practices
-----------------------

Unit Testing with SDK
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import pytest
   from matrixone import Client
   from matrixone.config import get_connection_params
   
   @pytest.fixture
   def test_client():
       """Fixture for test client"""
       client = Client()
       host, port, user, password, database = get_connection_params()
       client.connect(host=host, port=port, user=user, password=password, database=database)
       yield client
       client.disconnect()
   
   @pytest.fixture
   def test_table(test_client):
       """Fixture for test table"""
       table_name = "test_users"
       test_client.create_table(table_name, {
           "id": "int",
           "username": "varchar(100)",
           "email": "varchar(255)"
       }, primary_key="id")
       yield table_name
       test_client.drop_table(table_name)
   
   def test_insert_and_query(test_client, test_table):
       """Test insert and query operations"""
       # Insert data
       test_client.insert(test_table, {
           "id": 1,
           "username": "testuser",
           "email": "test@example.com"
       })
       
       # Query data
       results = test_client.query(test_table).filter("id = 1").all()
       assert len(results.rows) == 1
       assert results.rows[0][1] == "testuser"

Summary of SDK Features
-----------------------

Essential SDK APIs to Use:

**Table Operations:**
- ``client.create_table()`` - Create tables
- ``client.drop_table()`` - Drop tables
- ``client.insert()`` - Insert single row
- ``client.batch_insert()`` - Batch insert (fastest)
- ``client.query()`` - Query builder

**Vector Operations:**
- ``client.vector_ops.create_ivf()`` - Create IVF index
- ``client.vector_ops.create_hnsw()`` - Create HNSW index
- ``client.vector_ops.similarity_search()`` - Vector search
- ``client.vector_ops.get_ivf_stats()`` - Monitor index health (CRITICAL)
- ``client.vector_ops.drop()`` - Drop index

**Fulltext Operations:**
- ``client.fulltext_index.enable_fulltext()`` - Enable fulltext
- ``client.fulltext_index.create()`` - Create fulltext index
- ``client.fulltext_index.drop()`` - Drop fulltext index
- Use ``boolean_match()`` in queries with ``encourage()``, ``must()``, ``should()``

**Metadata Operations:**
- ``client.metadata.scan()`` - Scan table metadata
- ``client.metadata.get_table_brief_stats()`` - Get table statistics

**Snapshot Operations:**
- ``client.snapshots.create()`` - Create snapshot
- ``client.snapshots.list()`` - List snapshots
- ``client.snapshots.drop()`` - Drop snapshot
- ``client.clone.clone_database()`` - Clone database
- ``client.clone.clone_table()`` - Clone table

**Transaction Operations:**
- ``with client.transaction() as tx:`` - Transaction context

**Account Operations:**
- ``AccountManager(client)`` - Initialize account manager
- ``account_manager.create_account()`` - Create account
- ``account_manager.create_user()`` - Create user
- ``account_manager.create_role()`` - Create role
- ``account_manager.grant_role()`` - Grant role to user
- ``account_manager.drop_user()`` / ``drop_role()`` - Clean up

**Pub/Sub Operations:**
- ``client.pubsub.list_publications()`` - List publications
- ``client.pubsub.list_subscriptions()`` - List subscriptions
- ``client.pubsub.drop_publication()`` - Drop publication
- ``client.pubsub.drop_subscription()`` - Drop subscription

Remember: Always prefer SDK APIs over raw SQL for better maintainability, type safety, and feature integration!
