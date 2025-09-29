ORM Usage Guide
===============

This guide provides comprehensive information on using the MatrixOne Python SDK with modern ORM patterns and advanced query building capabilities.

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
       {"id": 3, "username": "charlie", "email": "charlie@example.com", "age": 35, "department_id": 2, "salary": 70000.00}
   ]
   client.batch_insert("users", users_data)

   # Modern query building with enhanced features
   query = client.query("users")
   
   # Basic filtering
   results = query.filter("age > 25").all()
   print("Users over 25:", results)

   # Using logical_in for flexible IN conditions
   results = query.filter(logical_in("department_id", [1, 2])).all()
   print("Users in departments 1 or 2:", results)

   # Complex queries with expressions
   results = query.filter(logical_in("id", [1, 2, 3])).filter("salary > 55000").all()
   print("High-earning users:", results)

SQLAlchemy Integration
~~~~~~~~~~~~~~~~~~~~~~

For more complex scenarios, you can use full SQLAlchemy integration:

.. code-block:: python

   from sqlalchemy import Column, Integer, String, DECIMAL, TIMESTAMP, func
   from sqlalchemy.ext.declarative import declarative_base
   from matrixone import Client

   # Create declarative base
   Base = declarative_base()

   class User(Base):
       __tablename__ = 'users'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       username = Column(String(50), unique=True, nullable=False)
       email = Column(String(100), unique=True, nullable=False)
       full_name = Column(String(200))
       bio = Column(Text)
       balance = Column(DECIMAL(10, 2), default=0.00)
       created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
       updated_at = Column(TIMESTAMP, server_default=func.current_timestamp(), 
                          onupdate=func.current_timestamp())

       def to_dict(self):
           """Convert model instance to dictionary"""
           return {c.name: getattr(self, c.name) for c in self.__table__.columns}

       @classmethod
       def from_dict(cls, data):
           """Create model instance from dictionary"""
           return cls(**{k: v for k, v in data.items() if hasattr(cls, k)})

       def __repr__(self):
           return f"<User(id={self.id}, username='{self.username}', email='{self.email}')>"

Advanced Query Features
~~~~~~~~~~~~~~~~~~~~~~~

The MatrixOne query builder supports advanced features that go beyond basic SQLAlchemy:

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import logical_in
   from sqlalchemy import func
   from matrixone.sqlalchemy_ext import boolean_match

   # Advanced filtering with logical_in
   query = client.query("users")
   
   # Filter by multiple values
   results = query.filter(logical_in("department_id", [1, 2, 3])).all()
   
   # Filter with fulltext search integration
   fulltext_filter = boolean_match("bio").must("python developer")
   results = query.filter(logical_in("id", fulltext_filter)).all()
   
   # Complex expressions with group_by and having
   results = (query
              .select("department_id", func.count("id").label("user_count"))
              .group_by("department_id")
              .having(func.count("id") > 1)
              .all())
   
   # Order by with expressions
   results = (query
              .select("username", "salary")
              .order_by(func.upper("username"))
              .all())

   # Explain queries for optimization
   explain_result = query.filter("age > 25").explain(verbose=True)
   print("Query execution plan:", explain_result)

   # Generate SQL without executing
   sql = query.filter(logical_in("id", [1, 2, 3])).to_sql()
   print("Generated SQL:", sql)

Table Management with Client Interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The MatrixOne client provides powerful table management capabilities:

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params

   # Get connection parameters
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Create tables with various column types
   client.create_table("products", {
       "id": "int",
       "name": "varchar(200)",
       "description": "text",
       "price": "decimal(10,2)",
       "category": "varchar(50)",
       "in_stock": "boolean",
       "created_at": "timestamp"
   }, primary_key="id")

   # Create vector table for AI applications
   client.create_table("documents", {
       "id": "int",
       "title": "varchar(200)",
       "content": "text",
       "embedding": "vector(384,f32)"
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

   # Create vector index for similarity search
   client.vector.create_hnsw(
       table_name="documents",
       name="idx_embedding",
       column="embedding",
       m=16,
       ef_construction=200
   )

   # Create fulltext index for text search
   client.fulltext_index.create("products", "idx_description", "description", algorithm="BM25")

Vector Operations Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MatrixOne provides powerful vector operations for AI applications:

.. code-block:: python

   import numpy as np

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

   print("Similar documents:", results)

Fulltext Search Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~

MatrixOne provides powerful fulltext search capabilities:

.. code-block:: python

   from matrixone.sqlalchemy_ext import boolean_match

   # Create fulltext index
   client.fulltext_index.create("products", "idx_description", "description", algorithm="BM25")

   # Simple fulltext search
   results = client.fulltext_index.simple_query(
       table_name="products",
       columns=["description"],
       query="laptop OR phone",
       limit=10
   )

   # Advanced fulltext search with boolean expressions
   fulltext_filter = boolean_match("description").must("laptop").should("phone").must_not("broken")
   results = client.query("products").filter(logical_in("id", fulltext_filter)).all()

   print("Fulltext search results:", results)

Transaction Management
~~~~~~~~~~~~~~~~~~~~~~

MatrixOne provides comprehensive transaction support:

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params

   # Get connection parameters
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Transaction with automatic rollback on error
   try:
       with client.transaction() as tx:
           # Insert multiple records
           tx.insert("users", {"username": "alice", "email": "alice@example.com"})
           tx.insert("users", {"username": "bob", "email": "bob@example.com"})
           
           # Update records
           tx.execute("UPDATE users SET email = %s WHERE username = %s", 
                     ("alice.new@example.com", "alice"))
           
           # Transaction will be committed automatically
           print("Transaction completed successfully")
   except Exception as e:
       print(f"Transaction failed and was rolled back: {e}")

   # Manual transaction control
   tx = client.transaction()
   try:
       tx.insert("users", {"username": "charlie", "email": "charlie@example.com"})
       tx.execute("UPDATE users SET email = %s WHERE username = %s", 
                 ("charlie.new@example.com", "charlie"))
       tx.commit()
       print("Manual transaction committed")
   except Exception as e:
       tx.rollback()
       print(f"Manual transaction rolled back: {e}")

Async Operations
~~~~~~~~~~~~~~~~

MatrixOne provides full async support for modern Python applications:

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   from matrixone.config import get_connection_params

   async def async_operations():
       # Get connection parameters
       host, port, user, password, database = get_connection_params()
       client = AsyncClient()
       await client.connect(host=host, port=port, user=user, password=password, database=database)

       try:
           # Async query execution
           result = await client.execute("SELECT COUNT(*) as user_count FROM users")
           print(f"User count: {result.rows[0][0]}")

           # Async transaction
           async with client.transaction() as tx:
               await tx.insert("users", {"username": "async_user", "email": "async@example.com"})
               await tx.execute("UPDATE users SET email = %s WHERE username = %s", 
                               ("async.new@example.com", "async_user"))

           # Async query building
           query = client.query("users")
           results = await query.filter("username LIKE %s", "async%").all()
           print("Async query results:", results)

       finally:
           await client.disconnect()

   # Run async operations
   asyncio.run(async_operations())

Working with SQLAlchemy Sessions
---------------------------------

Session Management
~~~~~~~~~~~~~~~~~~

MatrixOne provides seamless SQLAlchemy session integration:

.. code-block:: python

   from sqlalchemy.orm import sessionmaker
   from matrixone import Client
   from matrixone.config import get_connection_params

   # Get connection parameters
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Create tables using ORM
   client.create_all(Base)

   # Get SQLAlchemy engine from client
   engine = client.get_sqlalchemy_engine()

   # Create session factory
   Session = sessionmaker(bind=engine)

   # Use session for ORM operations
   session = Session()

   try:
       # Create new users
       user1 = User(
           username='alice',
           email='alice@example.com',
           full_name='Alice Johnson',
           bio='Software Engineer',
           balance=1500.00
       )
       
       user2 = User(
           username='bob',
           email='bob@example.com',
           full_name='Bob Smith',
           bio='Data Scientist',
           balance=2000.00
       )

       # Add users to session
       session.add_all([user1, user2])
       session.commit()
       print("✓ Users created successfully")

       # Query users
       users = session.query(User).filter(User.balance > 1000).all()
       print(f"✓ Found {len(users)} users with balance > $1000:")
       for user in users:
           print(f"  - {user.username}: ${user.balance}")

       # Update user
       alice = session.query(User).filter(User.username == 'alice').first()
       if alice:
           alice.bio = 'Senior Software Engineer'
           session.commit()
           print(f"✓ Updated {alice.username}'s bio")

       # Advanced queries
       high_balance_users = session.query(User).filter(
           User.balance > 1500
       ).order_by(User.created_at.desc()).all()
       
       print(f"✓ Found {len(high_balance_users)} users with balance > $1500:")
       for user in high_balance_users:
           print(f"  - {user.full_name} ({user.username}): ${user.balance}")

   finally:
       session.close()
       client.disconnect()

Transaction Management with ORM
--------------------------------

Client Transaction Interface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Column, Integer, String, DECIMAL
   from sqlalchemy.ext.declarative import declarative_base
   from matrixone import Client

   Base = declarative_base()

   class Account(Base):
       __tablename__ = 'accounts'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       account_number = Column(String(20), unique=True, nullable=False)
       owner_name = Column(String(100), nullable=False)
       balance = Column(DECIMAL(15, 2), nullable=False, default=0.00)

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Create table
   client.create_all(Base)

   # Insert initial accounts using client transaction
   accounts_data = [
       ('ACC-001', 'Alice Johnson', 5000.00),
       ('ACC-002', 'Bob Smith', 3000.00),
       ('ACC-003', 'Charlie Brown', 2000.00)
   ]

   with client.transaction() as tx:
       for acc_num, owner, balance in accounts_data:
           tx.execute(
               "INSERT INTO accounts (account_number, owner_name, balance) VALUES (%s, %s, %s)",
               (acc_num, owner, balance)
           )
   print("✓ Initial accounts created")

   # Transfer money between accounts using transaction
   def transfer_money(from_account, to_account, amount):
       with client.transaction() as tx:
           # Check source account balance
           result = tx.execute(
               "SELECT balance FROM accounts WHERE account_number = %s",
               (from_account,)
           )
           source_balance = result.fetchone()
           
           if not source_balance or source_balance[0] < amount:
               raise ValueError(f"Insufficient funds in account {from_account}")
           
           # Debit from source account
           tx.execute(
               "UPDATE accounts SET balance = balance - %s WHERE account_number = %s",
               (amount, from_account)
           )
           
           # Credit to destination account
           tx.execute(
               "UPDATE accounts SET balance = balance + %s WHERE account_number = %s",
               (amount, to_account)
           )
           
           print(f"✓ Transferred ${amount} from {from_account} to {to_account}")

   # Perform transfers
   try:
       transfer_money('ACC-001', 'ACC-002', 500.00)
       transfer_money('ACC-002', 'ACC-003', 200.00)
   except ValueError as e:
       print(f"❌ Transfer failed: {e}")

   # Verify final balances
   result = client.execute("SELECT account_number, owner_name, balance FROM accounts ORDER BY account_number")
   print("\nFinal Account Balances:")
   for row in result.fetchall():
       print(f"  {row[0]} ({row[1]}): ${row[2]}")

   client.disconnect()

Mixed ORM and SQL Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy.orm import sessionmaker
   from sqlalchemy import text
   from matrixone import Client

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Create tables and get session
   client.create_all(Base)
   engine = client.get_sqlalchemy_engine()
   Session = sessionmaker(bind=engine)

   # Combine ORM operations with raw SQL in transaction
   session = Session()
   
   try:
       # Begin transaction
       session.begin()
       
       # ORM operations
       new_user = User(
           username='charlie',
           email='charlie@example.com',
           full_name='Charlie Wilson',
           balance=1000.00
       )
       session.add(new_user)
       session.flush()  # Get the ID without committing
       
       user_id = new_user.id
       print(f"✓ Created user with ID: {user_id}")
       
       # Raw SQL operations within the same transaction
       session.execute(text("""
           INSERT INTO accounts (account_number, owner_name, balance) 
           VALUES (:acc_num, :owner, :balance)
       """), {
           'acc_num': f'ACC-{user_id:03d}',
           'owner': new_user.full_name,
           'balance': new_user.balance
       })
       
       # Complex query using raw SQL
       result = session.execute(text("""
           SELECT u.username, u.full_name, a.account_number, a.balance
           FROM users u
           JOIN accounts a ON a.owner_name = u.full_name
           WHERE u.id = :user_id
       """), {'user_id': user_id})
       
       user_account = result.fetchone()
       if user_account:
           print(f"✓ Created account {user_account[2]} for user {user_account[0]}")
       
       # Commit transaction
       session.commit()
       print("✓ Transaction completed successfully")
       
   except Exception as e:
       session.rollback()
       print(f"❌ Transaction failed, rolled back: {e}")
   finally:
       session.close()

   client.disconnect()

Advanced ORM Patterns
----------------------

Model Relationships and Joins
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Column, Integer, String, ForeignKey, Text
   from sqlalchemy.ext.declarative import declarative_base
   from sqlalchemy.orm import relationship, sessionmaker
   from matrixone import Client

   Base = declarative_base()

   class Department(Base):
       __tablename__ = 'departments'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       name = Column(String(100), nullable=False, unique=True)
       description = Column(Text)
       
       # Relationship (note: foreign keys work but relationships need manual handling)
       # employees = relationship("Employee", back_populates="department")

   class Employee(Base):
       __tablename__ = 'employees'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       name = Column(String(100), nullable=False)
       email = Column(String(200), unique=True, nullable=False)
       department_id = Column(Integer, ForeignKey('departments.id'))
       position = Column(String(100))
       
       # department = relationship("Department", back_populates="employees")

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Create tables
   client.create_all(Base)

   engine = client.get_sqlalchemy_engine()
   Session = sessionmaker(bind=engine)
   session = Session()

   try:
       # Create departments
       eng_dept = Department(name='Engineering', description='Software development team')
       hr_dept = Department(name='Human Resources', description='People operations')
       
       session.add_all([eng_dept, hr_dept])
       session.commit()

       # Create employees
       employees = [
           Employee(name='Alice Johnson', email='alice@company.com', 
                   department_id=eng_dept.id, position='Senior Developer'),
           Employee(name='Bob Smith', email='bob@company.com', 
                   department_id=eng_dept.id, position='DevOps Engineer'),
           Employee(name='Carol Wilson', email='carol@company.com', 
                   department_id=hr_dept.id, position='HR Manager')
       ]
       
       session.add_all(employees)
       session.commit()

       # Query with joins using raw SQL through ORM
       from sqlalchemy import text
       
       result = session.execute(text("""
           SELECT e.name, e.position, d.name as department_name
           FROM employees e
           JOIN departments d ON e.department_id = d.id
           ORDER BY d.name, e.name
       """))
       
       print("Employee Directory:")
       for row in result:
           print(f"  {row[0]} - {row[1]} ({row[2]})")

       # Aggregation queries
       dept_counts = session.execute(text("""
           SELECT d.name, COUNT(e.id) as employee_count
           FROM departments d
           LEFT JOIN employees e ON d.id = e.department_id
           GROUP BY d.id, d.name
           ORDER BY employee_count DESC
       """))
       
       print("\nDepartment Employee Counts:")
       for row in dept_counts:
           print(f"  {row[0]}: {row[1]} employees")

   finally:
       session.close()
       client.disconnect()

Bulk Operations
~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy.orm import sessionmaker
   from sqlalchemy import text
   from matrixone import Client
   import time

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Create table for bulk operations demo
   client.create_all(Base)

   engine = client.get_sqlalchemy_engine()
   Session = sessionmaker(bind=engine)

   # Bulk insert using client transaction (recommended for large datasets)
   def bulk_insert_with_client():
       print("Bulk insert using client transaction...")
       start_time = time.time()
       
       # Generate test data
       users_data = [
           (f'user_{i}', f'user{i}@example.com', f'User {i}', f'Bio for user {i}', 1000 + i)
           for i in range(1000)
       ]
       
       # Use client transaction for bulk insert
       with client.transaction() as tx:
           for username, email, full_name, bio, balance in users_data:
               tx.execute(
                   "INSERT INTO users (username, email, full_name, bio, balance) VALUES (%s, %s, %s, %s, %s)",
                   (username, email, full_name, bio, balance)
               )
       
       elapsed = time.time() - start_time
       print(f"✓ Inserted 1000 users in {elapsed:.2f} seconds using client transaction")

   # Bulk update using SQLAlchemy
   def bulk_update_with_orm():
       print("Bulk update using ORM...")
       session = Session()
       
       try:
           start_time = time.time()
           
           # Bulk update using raw SQL through ORM
           result = session.execute(text("""
               UPDATE users 
               SET balance = balance * 1.1 
               WHERE balance > :min_balance
           """), {'min_balance': 1500})
           
           session.commit()
           
           elapsed = time.time() - start_time
           print(f"✓ Updated {result.rowcount} users in {elapsed:.4f} seconds using ORM")
           
       finally:
           session.close()

   # Bulk query with pagination
   def paginated_query():
       print("Paginated query example...")
       session = Session()
       
       try:
           page_size = 100
           offset = 0
           
           while True:
               users = session.query(User).offset(offset).limit(page_size).all()
               
               if not users:
                   break
                   
               print(f"✓ Page {offset // page_size + 1}: {len(users)} users")
               # Process users here
               
               offset += page_size
               
               if len(users) < page_size:
                   break
                   
       finally:
           session.close()

   # Run bulk operations
   try:
       bulk_insert_with_client()
       bulk_update_with_orm()
       paginated_query()
       
       # Verify final state
       result = client.execute("SELECT COUNT(*) FROM users")
       count = result.fetchone()[0]
       print(f"✓ Total users in database: {count}")
       
   finally:
       client.disconnect()

Error Handling in ORM Operations
---------------------------------

Comprehensive Error Handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.exceptions import ConnectionError, QueryError
   from sqlalchemy.exc import IntegrityError, SQLAlchemyError
   from sqlalchemy.orm import sessionmaker

   def robust_orm_operations():
       client = None
       session = None
       
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
           print("✓ Connected to database")

           # Table creation with error handling
           try:
               client.create_all(Base)
               print("✓ Tables created/verified")
           except QueryError as e:
               if "already exists" in str(e).lower():
                   print("⚠️  Tables already exist, continuing...")
               else:
                   raise

           # Session operations with error handling
           engine = client.get_sqlalchemy_engine()
           Session = sessionmaker(bind=engine)
           session = Session()

           # Insert with duplicate key handling
           try:
               duplicate_user = User(
                   username='alice',  # This might already exist
                   email='alice@example.com',
                   full_name='Alice Johnson'
               )
               session.add(duplicate_user)
               session.commit()
               print("✓ User created successfully")
               
           except IntegrityError as e:
               session.rollback()
               if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                   print("⚠️  User already exists, skipping...")
               else:
                   print(f"❌ Integrity constraint violation: {e}")
                   
           except SQLAlchemyError as e:
               session.rollback()
               print(f"❌ Database error: {e}")

           # Query with error handling
           try:
               users = session.query(User).filter(User.balance > 1000).all()
               print(f"✓ Found {len(users)} users with high balance")
               
               for user in users:
                   print(f"  - {user.username}: ${user.balance}")
                   
           except SQLAlchemyError as e:
               print(f"❌ Query failed: {e}")

           # Transaction with rollback on error
           try:
               session.begin()
               
               # Simulated business logic that might fail
               high_balance_user = session.query(User).filter(User.balance > 2000).first()
               if high_balance_user:
                   high_balance_user.balance -= 500
                   
                   # Simulate potential error
                   if high_balance_user.balance < 0:
                       raise ValueError("Balance cannot be negative")
                   
                   session.commit()
                   print(f"✓ Updated {high_balance_user.username}'s balance")
               else:
                   print("⚠️  No high balance users found")
                   
           except ValueError as e:
               session.rollback()
               print(f"❌ Business logic error: {e}")
           except SQLAlchemyError as e:
               session.rollback()
               print(f"❌ Transaction failed: {e}")

       except ConnectionError as e:
           print(f"❌ Connection failed: {e}")
       except Exception as e:
           print(f"❌ Unexpected error: {e}")
       finally:
           # Clean up resources
           if session:
               session.close()
               print("✓ Session closed")
           if client:
               try:
                   client.disconnect()
                   print("✓ Database connection closed")
               except Exception as e:
                   print(f"⚠️  Warning during cleanup: {e}")

   robust_orm_operations()

Best Practices
--------------

Performance Tips
~~~~~~~~~~~~~~~~

1. **Use Client Transactions for Bulk Operations**: For large batch operations, use ``client.transaction()`` instead of ORM sessions.

2. **Connection Pooling**: The client automatically provides connection pooling through SQLAlchemy.

3. **Session Management**: Always close sessions and use try/finally blocks.

4. **Bulk Operations**: For inserting/updating many records, use raw SQL through transactions.

5. **Query Optimization**: Use EXPLAIN to analyze query performance.

Security Best Practices
~~~~~~~~~~~~~~~~~~~~~~~~

1. **Parameterized Queries**: Always use parameterized queries to prevent SQL injection.

2. **Input Validation**: Validate data before inserting into the database.

3. **Connection Security**: Use environment variables for connection credentials.

4. **Error Handling**: Don't expose sensitive database information in error messages.

Model Design Guidelines
~~~~~~~~~~~~~~~~~~~~~~~

1. **Clear Table Names**: Use descriptive table names and follow naming conventions.

2. **Proper Data Types**: Choose appropriate data types for your columns.

3. **Constraints**: Define proper constraints (unique, nullable, etc.).

4. **Serialization**: Add ``to_dict()`` and ``from_dict()`` methods for JSON serialization.

5. **Documentation**: Document complex models and relationships.

Next Steps
----------

* Explore :doc:`vector_guide` for vector search with ORM
* Check :doc:`fulltext_guide` for fulltext search integration  
* Review :doc:`examples` for more comprehensive examples
* See :doc:`api/index` for detailed API documentation
