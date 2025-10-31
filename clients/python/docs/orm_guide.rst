ORM Usage Guide
===============

This guide provides comprehensive information on using the MatrixOne Python SDK with modern ORM patterns and advanced query building capabilities.

.. danger::
   **🚨 CRITICAL: Column Naming Convention**
   
   **Always use lowercase with underscores (snake_case) for column names!**
   
   MatrixOne does not support SQL standard double-quoted identifiers in queries, which causes 
   issues with camelCase column names when using SQLAlchemy ORM.
   
   .. code-block:: python
   
      # ❌ DON'T: CamelCase column names (will fail in SELECT queries)
      class User(Base):
          userName = Column(String(50))      # CREATE succeeds, SELECT fails!
          userId = Column(Integer)           # Will cause SQL syntax errors
      
      # ✅ DO: Use lowercase with underscores (snake_case)
      class User(Base):
          user_name = Column(String(50))     # Works perfectly
          user_id = Column(Integer)          # All operations succeed
   
   **Problem demonstration:**
   
   .. code-block:: sql
   
      -- CamelCase generates:
      SELECT "userName" FROM user  -- ❌ Fails with SQL syntax error!
      
      -- snake_case generates:
      SELECT user_name FROM user   -- ✅ Works perfectly!
   
   **Why this happens:**
   
   - CREATE TABLE uses backticks: ``CREATE TABLE user (`userName` VARCHAR(50))`` ✅ Works
   - SELECT uses double quotes: ``SELECT "userName" FROM user`` ❌ MatrixOne doesn't support
   - Solution: Use snake_case to avoid any quoting: ``SELECT user_name FROM user`` ✅ Works

Overview
--------

The MatrixOne Python SDK provides powerful ORM capabilities that integrate seamlessly with SQLAlchemy, offering:

* **Modern Query Builder**: Enhanced query building with `MatrixOneQuery` and `BaseMatrixOneQuery`
* **SQLAlchemy Integration**: Full SQLAlchemy declarative models and session management
* **Advanced Query Features**: Support for `logical_in`, `having`, `group_by`, `order_by` with expressions
* **Vector and Fulltext Support**: Built-in support for vector similarity search and fulltext indexing
* **Transaction Management**: Comprehensive transaction support with ORM
* **Async Support**: Full async/await support with `AsyncClient`
* **Type Safety**: Complete type hints and validation

Transaction-Aware ORM Operations (Recommended)
------------------------------------------------

Use ``client.session()`` for atomic ORM operations with automatic commit/rollback. This is the recommended approach for multi-statement operations.

Basic Transaction with ORM
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import Base, Column, Integer, String
   from sqlalchemy import select, insert, update, delete

   # Define ORM model
   Base = declarative_base()
   
   class User(Base):
       __tablename__ = 'users'
       id = Column(Integer, primary_key=True)
       name = Column(String(100))
       email = Column(String(255))
       age = Column(Integer)
       status = Column(String(20))

   client = Client()
   client.connect(database='test')
   client.create_table(User)

   # Transaction with automatic commit/rollback
   with client.session() as session:
       # All operations are atomic
       session.execute(insert(User).values(name='Alice', email='alice@example.com', age=30))
       session.execute(update(User).where(User.age < 18).values(status='minor'))
       
       # Query within transaction
       stmt = select(User).where(User.age > 25)
       result = session.execute(stmt)
       users = result.scalars().all()
       for user in users:
           print(f"User: {user.name}, Age: {user.age}")
       # Commits automatically on success

   client.disconnect()

**Key Features:**

- ✅ All operations succeed or fail together
- ✅ Automatic rollback on errors
- ✅ Access to all MatrixOne managers within session
- ✅ Full SQLAlchemy ORM support

Complex Transactions with Multiple Tables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import select, insert, update, and_, func

   # Define models
   class Order(Base):
       __tablename__ = 'orders'
       id = Column(Integer, primary_key=True)
       user_id = Column(Integer)
       amount = Column(Decimal(10, 2))
       status = Column(String(20))

   client = Client()
   client.connect(database='test')

   # Complex transaction with multiple operations
   with client.session() as session:
       # Insert order
       session.execute(
           insert(Order).values(user_id=1, amount=100.00, status='pending')
       )
       
       # Update user status
       session.execute(
           update(User).where(User.id == 1).values(status='has_orders')
       )
       
       # Query with JOIN (if needed)
       stmt = select(User, Order).join(Order, User.id == Order.user_id)
       result = session.execute(stmt)
       
       # Calculate totals
       stmt = select(func.sum(Order.amount)).where(Order.user_id == 1)
       total = session.execute(stmt).scalar()
       
       print(f"Total orders: ${total}")
       # All operations commit together

   client.disconnect()

Transaction Error Handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from sqlalchemy import insert

   client = Client()
   client.connect(database='test')

   # Automatic rollback on error
   try:
       with client.session() as session:
           session.execute(insert(User).values(name='Bob', age=25))
           
           # This will fail and trigger automatic rollback
           session.execute(insert(InvalidTable).values(data='test'))
           
           # Bob will NOT be inserted due to rollback
   except Exception as e:
       print(f"Transaction failed and rolled back: {e}")

   # Verify rollback worked
   stmt = select(func.count(User.id)).where(User.name == 'Bob')
   count = client.execute(stmt).scalar()
   print(f"Bob count: {count}")  # Should be 0

   client.disconnect()

Modern Query Builder Usage
---------------------------

The MatrixOne Python SDK provides a powerful query builder that supports both traditional SQLAlchemy patterns and enhanced MatrixOne-specific features.

Basic Query Building
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import logical_in
   from sqlalchemy import func
   from matrixone.config import get_connection_params

   # Get connection parameters
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Create a table using the client API
   client.create_table("users", {
       "id": "int",
       "username": "varchar(50)",
       "email": "varchar(100)",
       "age": "int",
       "department_id": "int",
       "salary": "decimal(10,2)"
   }, primary_key="id")

   # Insert data using the client API
   users_data = [
       {"id": 1, "username": "alice", "email": "alice@example.com", "age": 25, "department_id": 1, "salary": 50000.00},
       {"id": 2, "username": "bob", "email": "bob@example.com", "age": 30, "department_id": 1, "salary": 60000.00},
       {"id": 3, "username": "charlie", "email": "charlie@example.com", "age": 35, "department_id": 2, "salary": 70000.00},
       {"id": 4, "username": "diana", "email": "diana@example.com", "age": 28, "department_id": 2, "salary": 55000.00}
   ]
   client.batch_insert("users", users_data)

   # Basic query using query API
   result = client.query("users").select("*").where("age > ?", 25).execute()
   print("Users over 25:")
   for row in result.fetchall():
       print(f"  {row[1]} - {row[2]} - Age: {row[3]}")

   # Query with multiple conditions
   result = client.query("users").select("username", "salary").where("department_id = ? AND salary > ?", 1, 55000).execute()
   print("High earners in department 1:")
   for row in result.fetchall():
       print(f"  {row[0]} - ${row[1]}")

   # Clean up
   client.drop_table("users")
   client.disconnect()

Advanced Query Building with ORM Models
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Column, Integer, String, DECIMAL, ForeignKey
   from matrixone.orm import declarative_base
   from sqlalchemy.orm import sessionmaker, relationship
   from matrixone import Client
   from matrixone.config import get_connection_params

   # Define ORM models
   Base = declarative_base()

   class Department(Base):
       __tablename__ = 'departments'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       name = Column(String(50), nullable=False)
       budget = Column(DECIMAL(12, 2), nullable=False)
       
       # Relationship
       users = relationship("User", back_populates="department")

   class User(Base):
       __tablename__ = 'users'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       username = Column(String(50), nullable=False, unique=True)
       email = Column(String(100), nullable=False, unique=True)
       age = Column(Integer, nullable=False)
       department_id = Column(Integer, ForeignKey('departments.id'), nullable=False)
       salary = Column(DECIMAL(10, 2), nullable=False)
       
       # Relationship
       department = relationship("Department", back_populates="users")

   # Get connection and create client
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Create tables using ORM models
   client.create_table(Department)
   client.create_table(User)

   # Insert data using client API
   departments = [
       {"name": "Engineering", "budget": 1000000.00},
       {"name": "Marketing", "budget": 500000.00}
   ]
   client.batch_insert(Department, departments)

   users = [
       {"username": "alice", "email": "alice@example.com", "age": 25, "department_id": 1, "salary": 50000.00},
       {"username": "bob", "email": "bob@example.com", "age": 30, "department_id": 1, "salary": 60000.00},
       {"username": "charlie", "email": "charlie@example.com", "age": 35, "department_id": 2, "salary": 70000.00}
   ]
   client.batch_insert(User, users)

   # Query using client API
   users = client.query(User).filter(User.age > 25).all()
   print("Users over 25:")
   for user in users:
       print(f"  {user.username} - {user.email} - Age: {user.age}")

   # Query with joins using client API
   results = client.query(User, Department).join(Department).filter(Department.name == "Engineering").all()
   print("Engineering users:")
   for user, dept in results:
       print(f"  {user.username} - {dept.name} - ${user.salary}")

   # Update using client API
   client.query(User).filter(User.username == "alice").update({"salary": 55000.00})

   # Delete using client API
   client.query(User).filter(User.username == "charlie").delete()

   # Clean up
   client.drop_table(User)
   client.drop_table(Department)
   client.disconnect()

Enhanced Query Building with logical_in
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

       # Clean up
       client.drop_table("products")
       client.disconnect()

   enhanced_query_example()

Vector Operations with ORM
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Column, Integer, String, Text
   from matrixone.orm import declarative_base
   from sqlalchemy.orm import sessionmaker
   from matrixone import Client
   from matrixone.config import get_connection_params
   from matrixone.sqlalchemy_ext import create_vector_column
   import numpy as np

   # Define vector ORM model
   VectorBase = declarative_base()

   class Document(VectorBase):
       __tablename__ = 'documents'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(200), nullable=False)
       content = Column(Text)
       embedding = create_vector_column(384, "f32")  # 384-dimensional vector

   # Connect and setup
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Create table using ORM model
   client.create_table(Document)

   # Create vector index
   client.vector_ops.enable_ivf()
   client.vector_ops.create_ivf(
       'documents',  # Table name as positional argument
       name='idx_embedding',
       column='embedding',
       lists=50,
       op_type='vector_l2_ops'
   )

   # Create session
   Session = sessionmaker(bind=client.get_sqlalchemy_engine())
   session = Session()

   # Insert documents using ORM
   docs = [
       Document(
           title='AI Research',
           content='Artificial intelligence research paper',
           embedding=np.random.rand(384).astype(np.float32).tolist()
       ),
       Document(
           title='ML Guide',
           content='Machine learning tutorial',
           embedding=np.random.rand(384).astype(np.float32).tolist()
       )
   ]
   
   client.batch_insert(Document, [
       {"title": doc.title, "content": doc.content, "embedding": doc.embedding}
       for doc in docs
   ])

   # Vector similarity search using vector_query API (first argument is positional)
   query_vector = np.random.rand(384).astype(np.float32).tolist()
   results = client.vector_ops.similarity_search(
       'documents',  # table name - positional argument
       vector_column='embedding',
       query_vector=query_vector,
       limit=5,
       distance_type='l2'
   )

   print("Vector Search Results:")
   for result in results.rows:
       print(f"Document: {result[1]} (Distance: {result[-1]:.4f})")

   # Clean up
   client.drop_table(Document)
   session.close()
   client.disconnect()

Async ORM Operations
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from sqlalchemy import Column, Integer, String, DECIMAL
   from matrixone.orm import declarative_base
   from sqlalchemy.orm import sessionmaker
   from matrixone import AsyncClient
   from matrixone.config import get_connection_params

   # Define async ORM model
   AsyncBase = declarative_base()

   class AsyncUser(AsyncBase):
       __tablename__ = 'async_users'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       username = Column(String(50), nullable=False, unique=True)
       email = Column(String(100), nullable=False, unique=True)
       balance = Column(DECIMAL(10, 2), nullable=False, default=0.00)

   async def async_orm_example():
       # Get connection parameters
       host, port, user, password, database = get_connection_params()
       
       client = AsyncClient()
       await client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create table using async create_table API
       await client.create_table(AsyncUser)

       # Insert data using client API
       users = [
           {"username": "async_alice", "email": "alice@async.com", "balance": 1000.00},
           {"username": "async_bob", "email": "bob@async.com", "balance": 500.00}
       ]
       client.batch_insert(AsyncUser, users)

       # Query using client API
       users = client.query(AsyncUser).filter(AsyncUser.balance > 600).all()
       print("Users with balance > 600:")
       for user in users:
           print(f"  {user.username} - ${user.balance}")

       # Update using ORM
       session.query(AsyncUser).filter(AsyncUser.username == "async_alice").update({"balance": 1200.00})
       session.commit()

       # Delete using ORM
       session.query(AsyncUser).filter(AsyncUser.username == "async_bob").delete()
       session.commit()

       # Clean up
       await client.drop_table(AsyncUser)
       session.close()
       await client.disconnect()

   asyncio.run(async_orm_example())

Transaction Management with ORM
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Column, Integer, String, DECIMAL
   from matrixone.orm import declarative_base
   from sqlalchemy.orm import sessionmaker
   from matrixone import Client
   from matrixone.config import get_connection_params

   # Define transaction ORM models
   TransactionBase = declarative_base()

   class Account(TransactionBase):
       __tablename__ = 'accounts'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       name = Column(String(100), nullable=False)
       balance = Column(DECIMAL(10, 2), nullable=False)

   class Transaction(TransactionBase):
       __tablename__ = 'transactions'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       from_account_id = Column(Integer, nullable=False)
       to_account_id = Column(Integer, nullable=False)
       amount = Column(DECIMAL(10, 2), nullable=False)
       timestamp = Column(String(50), nullable=False)

   def transaction_example():
       host, port, user, password, database = get_connection_params()
       client = Client()
       client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create tables using ORM models
       client.create_table(Account)
       client.create_table(Transaction)

       # Insert initial data using client API
       accounts = [
           {"name": "Alice", "balance": 1000.00},
           {"name": "Bob", "balance": 500.00}
       ]
       client.batch_insert(Account, accounts)

       # Transfer money using transaction
       try:
           # Use client transaction API
           with client.transaction() as tx:
               # Update balances
               tx.query(Account).filter(Account.name == "Alice").update({"balance": 900.00})
               tx.query(Account).filter(Account.name == "Bob").update({"balance": 600.00})
               
               # Record transaction
               tx.insert(Transaction, {
                   "from_account_id": 1,
                   "to_account_id": 2,
                   "amount": 100.00,
                   "timestamp": "2024-01-01 10:00:00"
               })
               
               print("✓ Transaction completed successfully")
           
       except Exception as e:
           # Transaction is automatically rolled back on error
           print(f"❌ Transaction failed: {e}")

       # Verify the transfer
       accounts = session.query(Account).all()
       for account in accounts:
           print(f"{account.name}: ${account.balance}")

       # Clean up
       client.drop_table(Transaction)
       client.drop_table(Account)
       session.close()
       client.disconnect()

   transaction_example()

Advanced Query Features
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import logical_in
   from sqlalchemy import func, text
   from matrixone.config import get_connection_params

   def advanced_query_features():
       host, port, user, password, database = get_connection_params()
       client = Client()
       client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create sample table
       client.create_table("sales", {
           "id": "int",
           "product_id": "int",
           "customer_id": "int",
           "amount": "decimal(10,2)",
           "sale_date": "date",
           "region": "varchar(50)"
       }, primary_key="id")

       # Insert sample data
       sales_data = [
           {"id": 1, "product_id": 101, "customer_id": 201, "amount": 100.00, "sale_date": "2024-01-01", "region": "North"},
           {"id": 2, "product_id": 102, "customer_id": 202, "amount": 200.00, "sale_date": "2024-01-02", "region": "South"},
           {"id": 3, "product_id": 101, "customer_id": 203, "amount": 150.00, "sale_date": "2024-01-03", "region": "North"},
           {"id": 4, "product_id": 103, "customer_id": 201, "amount": 300.00, "sale_date": "2024-01-04", "region": "East"}
       ]
       client.batch_insert("sales", sales_data)

       # Group by with having clause
       result = client.query("sales").select(
           "region", 
           func.sum("amount").label("total_sales"),
           func.count("*").label("sale_count")
       ).group_by("region").having(func.sum("amount") > 200).execute()

       print("Regions with sales > 200:")
       for row in result.fetchall():
           print(f"  {row[0]}: ${row[1]} ({row[2]} sales)")

       # Order by with expressions
       result = client.query("sales").select("*").order_by("amount DESC").limit(2).execute()
       print("Top 2 sales by amount:")
       for row in result.fetchall():
           print(f"  Sale {row[0]}: ${row[3]}")

       # Complex where conditions with logical_in
       result = client.query("sales").select("*").filter(
           logical_in("product_id", [101, 102]) & 
           logical_in("region", ["North", "South"])
       ).execute()

       print("Sales for products 101,102 in North/South:")
       for row in result.fetchall():
           print(f"  Sale {row[0]}: Product {row[1]}, Region {row[5]}, Amount ${row[3]}")

       # Clean up
       client.drop_table("sales")
       client.disconnect()

   advanced_query_features()

Error Handling with ORM
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.exceptions import QueryError, ConnectionError
   from matrixone.config import get_connection_params
   from sqlalchemy.exc import SQLAlchemyError

   def robust_orm_example():
       client = None
       session = None
       
       try:
           host, port, user, password, database = get_connection_params()
           
           # Create client with error handling
           client = Client()
           client.connect(host=host, port=port, user=user, password=password, database=database)

           # Create table with error handling
           try:
               client.create_table("robust_users", {
                   "id": "int",
                   "username": "varchar(50)",
                   "email": "varchar(100)"
               }, primary_key="id")
               print("✓ Table created successfully")
           except QueryError as e:
               print(f"❌ Table creation failed: {e}")

           # Create session with error handling
           try:
               from sqlalchemy.orm import sessionmaker
               Session = sessionmaker(bind=client.get_sqlalchemy_engine())
               session = Session()
               print("✓ Session created successfully")
           except SQLAlchemyError as e:
               print(f"❌ Session creation failed: {e}")

           # Insert data with error handling
           try:
               client.insert("robust_users", {"id": 1, "username": "test", "email": "test@example.com"})
               print("✓ Data inserted successfully")
           except QueryError as e:
               print(f"❌ Data insertion failed: {e}")

           # Query data with error handling
           try:
               result = client.query("robust_users").select("*").execute()
               print(f"✓ Query successful: {result.fetchall()}")
           except QueryError as e:
               print(f"❌ Query failed: {e}")

       except ConnectionError as e:
           print(f"❌ Connection failed: {e}")
       except Exception as e:
           print(f"❌ Unexpected error: {e}")
       finally:
           # Always clean up
           if session:
               try:
                   session.close()
                   print("✓ Session closed")
               except Exception as e:
                   print(f"⚠️ Session cleanup warning: {e}")
           
           if client:
               try:
                   client.drop_table("robust_users")
                   client.disconnect()
                   print("✓ Cleanup completed")
               except Exception as e:
                   print(f"⚠️ Cleanup warning: {e}")

   robust_orm_example()

Best Practices
~~~~~~~~~~~~~~

1. **Use ORM models for complex schemas**:
   - Define clear relationships between tables
   - Use proper foreign keys and constraints
   - Leverage SQLAlchemy's declarative base

2. **Combine ORM with query API**:
   - Use ORM for data modeling and relationships
   - Use query API for complex queries and performance-critical operations
   - Mix both approaches as needed

3. **Handle transactions properly**:
   - Always use try-catch blocks for transactions
   - Rollback on errors
   - Commit only when all operations succeed

4. **Use async operations for I/O-bound tasks**:
   - Use AsyncClient for concurrent operations
   - Use async/await patterns consistently
   - Handle async errors properly

5. **Optimize queries**:
   - Use appropriate indexes
   - Avoid N+1 query problems
   - Use batch operations for bulk data

6. **Error handling**:
   - Always use try-catch blocks
   - Provide meaningful error messages
   - Clean up resources properly

Next Steps
----------

* Read the :doc:`api/query_builders` for detailed query builder API
* Check out the :doc:`api/orm_classes` for ORM class documentation
* Explore :doc:`vector_guide` for vector operations with ORM
* Learn about :doc:`fulltext_guide` for fulltext search with ORM
* Check out the :doc:`examples` for comprehensive usage examples