Best Practices Guide
====================

This guide provides best practices for using the MatrixOne Python SDK effectively in production environments.

Connection Management
---------------------

Environment Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~

**Use Environment Variables for Configuration:**

.. code-block:: python

   import os
   from matrixone import Client
   from matrixone.config import get_connection_params

   # Recommended: Use environment variables
   # Set these in your environment or .env file:
   # MATRIXONE_HOST=your-host
   # MATRIXONE_PORT=6001
   # MATRIXONE_USER=your-user
   # MATRIXONE_PASSWORD=your-password
   # MATRIXONE_DATABASE=your-database

   def create_client():
       host, port, user, password, database = get_connection_params()
       
       client = Client(
           connection_timeout=30,        # 30 second timeout
           query_timeout=300,           # 5 minute query timeout
           auto_commit=True,            # Enable auto-commit
           charset='utf8mb4',           # Support international characters
           enable_performance_logging=True,  # Monitor performance
           enable_sql_logging=False     # Disable in production
       )
       
       client.connect(host=host, port=port, user=user, password=password, database=database)
       return client

**Connection Pooling Best Practices:**

.. code-block:: python

   from matrixone import Client
   from contextlib import contextmanager

   class DatabaseManager:
       def __init__(self):
           self._client = None
           
       def get_client(self):
           if self._client is None:
               self._client = create_client()
           return self._client
           
       @contextmanager
       def get_connection(self):
           client = self.get_client()
           try:
               yield client
           except Exception as e:
               # Log error and potentially reconnect
               print(f"Database error: {e}")
               raise
           finally:
               # Don't disconnect here - keep connection alive
               pass
               
       def close(self):
           if self._client:
               self._client.disconnect()
               self._client = None

   # Usage
   db_manager = DatabaseManager()

   with db_manager.get_connection() as client:
       result = client.execute("SELECT 1")
       print(result.fetchall())

   # Clean up when application shuts down
   db_manager.close()

Error Handling and Resilience
-----------------------------

Comprehensive Error Handling
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.exceptions import ConnectionError, QueryError, VersionError
   import logging
   import time
   from functools import wraps

   # Configure logging
   logging.basicConfig(level=logging.INFO)
   logger = logging.getLogger(__name__)

   def retry_on_failure(max_retries=3, delay=1):
       """Decorator for retrying operations on failure"""
       def decorator(func):
           @wraps(func)
           def wrapper(*args, **kwargs):
               for attempt in range(max_retries):
                   try:
                       return func(*args, **kwargs)
                   except (ConnectionError, QueryError) as e:
                       if attempt == max_retries - 1:
                           logger.error(f"Operation failed after {max_retries} attempts: {e}")
                           raise
                       
                       logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                       time.sleep(delay)
                       
               return None
           return wrapper
       return decorator

   class RobustDatabaseClient:
       def __init__(self):
           self.client = None
           self.max_retries = 3
           self.retry_delay = 1
           
       def connect(self):
           """Connect with retry logic"""
           for attempt in range(self.max_retries):
               try:
                   self.client = Client()
                   self.client.connect(
                       host='localhost',
                       port=6001,
                       user='root',
                       password='111',
                       database='test'
                   )
                   logger.info("Successfully connected to database")
                   return
                   
               except ConnectionError as e:
                   if attempt == self.max_retries - 1:
                       logger.error(f"Failed to connect after {self.max_retries} attempts: {e}")
                       raise
                   
                   logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                   time.sleep(self.retry_delay)
                   
       @retry_on_failure(max_retries=3, delay=1)
       def execute_query(self, query, params=None):
           """Execute query with retry logic"""
           try:
               result = self.client.execute(query, params)
               return result.fetchall()
               
           except QueryError as e:
               logger.error(f"Query failed: {e}")
               raise
               
           except Exception as e:
               logger.error(f"Unexpected error: {e}")
               raise
               
       def check_connection(self):
           """Check if connection is still alive"""
           try:
               self.client.execute("SELECT 1")
               return True
           except Exception:
               return False
               
       def reconnect_if_needed(self):
           """Reconnect if connection is lost"""
           if not self.check_connection():
               logger.info("Connection lost, attempting to reconnect...")
               self.connect()

   # Usage
   db_client = RobustDatabaseClient()
   db_client.connect()

   try:
       results = db_client.execute_query("SELECT * FROM users WHERE active = %s", (True,))
       print(f"Found {len(results)} active users")
   except Exception as e:
       logger.error(f"Database operation failed: {e}")

ORM Best Practices
------------------

Model Design Guidelines
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, func, Index
   from sqlalchemy.ext.declarative import declarative_base
   from matrixone import Client

   Base = declarative_base()

   class User(Base):
       __tablename__ = 'users'
       
       # Primary key with auto-increment
       id = Column(Integer, primary_key=True, autoincrement=True)
       
       # Required fields with proper constraints
       username = Column(String(50), unique=True, nullable=False, index=True)
       email = Column(String(100), unique=True, nullable=False, index=True)
       
       # Optional fields with defaults
       full_name = Column(String(200))
       bio = Column(Text)
       is_active = Column(Integer, default=1, nullable=False)
       
       # Timestamps with automatic updates
       created_at = Column(TIMESTAMP, server_default=func.current_timestamp(), nullable=False)
       updated_at = Column(TIMESTAMP, server_default=func.current_timestamp(), 
                          onupdate=func.current_timestamp(), nullable=False)
       
       # Composite indexes for common queries
       __table_args__ = (
           Index('idx_user_active_created', 'is_active', 'created_at'),
           Index('idx_user_email_active', 'email', 'is_active'),
       )
       
       def to_dict(self):
           """Convert to dictionary for JSON serialization"""
           return {
               'id': self.id,
               'username': self.username,
               'email': self.email,
               'full_name': self.full_name,
               'bio': self.bio,
               'is_active': bool(self.is_active),
               'created_at': self.created_at.isoformat() if self.created_at else None,
               'updated_at': self.updated_at.isoformat() if self.updated_at else None
           }
           
       @classmethod
       def from_dict(cls, data):
           """Create instance from dictionary"""
           # Filter out None values and non-existent attributes
           filtered_data = {k: v for k, v in data.items() 
                          if hasattr(cls, k) and v is not None}
           return cls(**filtered_data)
           
       def __repr__(self):
           return f"<User(id={self.id}, username='{self.username}', email='{self.email}')>"

Session Management
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy.orm import sessionmaker
   from contextlib import contextmanager
   from matrixone import Client

   class ORMManager:
       def __init__(self, client):
           self.client = client
           self.engine = client.get_sqlalchemy_engine()
           self.Session = sessionmaker(bind=self.engine)
           
       @contextmanager
       def get_session(self):
           """Get database session with automatic cleanup"""
           session = self.Session()
           try:
               yield session
               session.commit()
           except Exception as e:
               session.rollback()
               raise
           finally:
               session.close()
               
       def create_user(self, user_data):
           """Create user with proper error handling"""
           with self.get_session() as session:
               try:
                   user = User.from_dict(user_data)
                   session.add(user)
                   session.flush()  # Get the ID without committing
                   
                   # Additional validation or business logic
                   if not user.username or len(user.username) < 3:
                       raise ValueError("Username must be at least 3 characters")
                       
                   return user.to_dict()
                   
               except Exception as e:
                   logger.error(f"Failed to create user: {e}")
                   raise
                   
       def get_active_users(self, limit=100, offset=0):
           """Get active users with pagination"""
           with self.get_session() as session:
               users = session.query(User).filter(
                   User.is_active == 1
               ).offset(offset).limit(limit).all()
               
               return [user.to_dict() for user in users]

   # Usage
   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')
   
   orm_manager = ORMManager(client)
   
   # Create user
   user_data = {
       'username': 'johndoe',
       'email': 'john@example.com',
       'full_name': 'John Doe',
       'bio': 'Software developer'
   }
   
   try:
       created_user = orm_manager.create_user(user_data)
       print(f"Created user: {created_user}")
   except Exception as e:
       print(f"Failed to create user: {e}")

Transaction Management
----------------------

Best Practices for Transactions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from sqlalchemy.orm import sessionmaker
   from contextlib import contextmanager

   class TransactionManager:
       def __init__(self, client):
           self.client = client
           self.engine = client.get_sqlalchemy_engine()
           self.Session = sessionmaker(bind=self.engine)
           
       @contextmanager
       def transaction(self):
           """Client-level transaction with automatic rollback on error"""
           with self.client.transaction() as tx:
               try:
                   yield tx
               except Exception as e:
                   logger.error(f"Transaction failed: {e}")
                   raise
                   
       @contextmanager
       def orm_transaction(self):
           """ORM-level transaction with automatic rollback"""
           session = self.Session()
           try:
               yield session
               session.commit()
           except Exception as e:
               session.rollback()
               logger.error(f"ORM transaction failed: {e}")
               raise
           finally:
               session.close()
               
       def transfer_money(self, from_account_id, to_account_id, amount):
           """Example of complex transaction with validation"""
           with self.orm_transaction() as session:
               # Get accounts
               from_account = session.query(Account).filter(
                   Account.id == from_account_id
               ).with_for_update().first()  # Lock for update
               
               to_account = session.query(Account).filter(
                   Account.id == to_account_id
               ).with_for_update().first()
               
               if not from_account or not to_account:
                   raise ValueError("One or both accounts not found")
                   
               if from_account.balance < amount:
                   raise ValueError("Insufficient funds")
                   
               # Perform transfer
               from_account.balance -= amount
               to_account.balance += amount
               
               # Log transaction
               transaction_log = TransactionLog(
                   from_account_id=from_account_id,
                   to_account_id=to_account_id,
                   amount=amount,
                   status='completed'
               )
               session.add(transaction_log)
               
               logger.info(f"Transferred ${amount} from account {from_account_id} to {to_account_id}")

Performance Optimization
------------------------

Query Optimization
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy.orm import sessionmaker
   from sqlalchemy import text, func
   from matrixone import Client

   class PerformanceOptimizedClient:
       def __init__(self, client):
           self.client = client
           self.engine = client.get_sqlalchemy_engine()
           self.Session = sessionmaker(bind=self.engine)
           
       def bulk_insert_users(self, users_data):
           """Bulk insert for better performance"""
           with self.client.transaction() as tx:
               # Use raw SQL for bulk operations
               placeholders = ', '.join(['%s'] * len(users_data[0]))
               columns = ['username', 'email', 'full_name', 'is_active']
               
               sql = f"""
                   INSERT INTO users ({', '.join(columns)}) 
                   VALUES ({placeholders})
               """
               
               # Prepare data
               values = [
                   (user['username'], user['email'], user['full_name'], user.get('is_active', 1))
                   for user in users_data
               ]
               
               # Execute bulk insert
               for value_set in values:
                   tx.execute(sql, value_set)
                   
       def get_user_statistics(self):
           """Optimized query with aggregation"""
           with self.get_session() as session:
               # Use raw SQL for complex aggregations
               result = session.execute(text("""
                   SELECT 
                       COUNT(*) as total_users,
                       COUNT(CASE WHEN is_active = 1 THEN 1 END) as active_users,
                       COUNT(CASE WHEN created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY) THEN 1 END) as new_users,
                       AVG(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) as active_ratio
                   FROM users
               """))
               
               stats = result.fetchone()
               return {
                   'total_users': stats[0],
                   'active_users': stats[1],
                   'new_users': stats[2],
                   'active_ratio': float(stats[3]) if stats[3] else 0
               }
               
       def search_users_optimized(self, search_term, limit=50):
           """Optimized search with proper indexing"""
           with self.get_session() as session:
               # Use fulltext search if available
               try:
                   results = self.client.fulltext_index.fulltext_search(
                       table_name='users',
                       columns=['username', 'email', 'full_name'],
                       search_term=search_term,
                       limit=limit
                   )
                   return results
               except Exception:
                   # Fallback to LIKE search
                   users = session.query(User).filter(
                       (User.username.like(f'%{search_term}%')) |
                       (User.email.like(f'%{search_term}%')) |
                       (User.full_name.like(f'%{search_term}%'))
                   ).limit(limit).all()
                   
                   return [user.to_dict() for user in users]

Connection Pooling and Caching
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from functools import lru_cache
   import threading
   from matrixone import Client

   class CachedDatabaseClient:
       def __init__(self):
           self._client = None
           self._lock = threading.Lock()
           
       def get_client(self):
           """Thread-safe client creation"""
           if self._client is None:
               with self._lock:
                   if self._client is None:
                       self._client = Client()
                       self._client.connect(
                           host='localhost',
                           port=6001,
                           user='root',
                           password='111',
                           database='test'
                       )
           return self._client
           
       @lru_cache(maxsize=128)
       def get_user_by_id(self, user_id):
           """Cached user lookup"""
           client = self.get_client()
           result = client.execute(
               "SELECT * FROM users WHERE id = %s",
               (user_id,)
           )
           row = result.fetchone()
           return dict(zip([col[0] for col in result.description], row)) if row else None
           
       def invalidate_user_cache(self, user_id):
           """Invalidate cache when user is updated"""
           self.get_user_by_id.cache_clear()

Vector Search Best Practices
----------------------------

Query Vector Parameter Formats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `query_vector` parameter in vector search functions supports multiple formats:

**List Format (Recommended):**
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

**String Format:**
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

**In ORM Queries:**
.. code-block:: python

   from sqlalchemy import text
   
   # Both formats work in raw SQL queries
   session.execute(text("""
       SELECT id, title, l2_distance(embedding, :query_vector) as distance
       FROM documents
       WHERE l2_distance(embedding, :query_vector) < 1.0
       ORDER BY distance ASC
   """), {'query_vector': query_vector_list})  # List format
   
   session.execute(text("""
       SELECT id, title, l2_distance(embedding, :query_vector) as distance
       FROM documents
       WHERE l2_distance(embedding, :query_vector) < 1.0
       ORDER BY distance ASC
   """), {'query_vector': query_vector_str})   # String format

**With VectorColumn Methods:**
.. code-block:: python

   from matrixone.sqlalchemy_ext import VectorColumn
   
   # Both formats work with VectorColumn methods
   session.query(Document).filter(
       Document.embedding.within_distance(query_vector_list, 1.0)  # List format
   ).all()
   
   session.query(Document).filter(
       Document.embedding.within_distance(query_vector_str, 1.0)   # String format
   ).all()

Index Configuration
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.sqlalchemy_ext import create_vector_column
   from sqlalchemy import Column, Integer, String, Text
   from sqlalchemy.ext.declarative import declarative_base

   Base = declarative_base()

   class Document(Base):
       __tablename__ = 'documents'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(200), nullable=False, index=True)
       content = Column(Text)
       # Use appropriate vector dimensions (384 for sentence-transformers, 1536 for OpenAI)
       embedding = create_vector_column(384, "f32")
       created_at = Column(TIMESTAMP, server_default=func.current_timestamp())

   class VectorSearchManager:
       def __init__(self, client):
           self.client = client
           
       def setup_vector_indexes(self):
           """Setup optimized vector indexes"""
           # Enable vector indexing
           self.client.vector_index.enable_ivf()
           
           # Create IVF index for large datasets
           self.client.vector_index.create_ivf(
               table_name='documents',
               name='idx_documents_embedding_ivf',
               column='embedding',
               lists=100,  # sqrt(number_of_documents) is a good starting point
               op_type='vector_l2_ops'
           )
           
           # Enable HNSW for high-accuracy searches
           self.client.vector_index.enable_hnsw()
           
           # Create HNSW index for high-accuracy searches
           self.client.vector_index.create_hnsw(
               table_name='documents',
               name='idx_documents_embedding_hnsw',
               column='embedding',
               m=16,                    # Good balance for most use cases
               ef_construction=200,    # Higher for better quality
               ef_search=50,           # Adjust based on accuracy needs
               op_type='vector_l2_ops'
           )
           
       def search_documents(self, query_vector, search_type='ivf', limit=10):
           """Search documents with different index types"""
           if search_type == 'ivf':
               # Fast search with IVF
               results = self.client.vector_query.similarity_search(
                   table_name='documents',
                   vector_column='embedding',
                   query_vector=query_vector,
                   limit=limit,
                   distance_type='l2'
               )
           elif search_type == 'hnsw':
               # High-accuracy search with HNSW
               results = self.client.vector_query.similarity_search(
                   table_name='documents',
                   vector_column='embedding',
                   query_vector=query_vector,
                   limit=limit,
                   distance_type='l2'
               )
           else:
               raise ValueError("search_type must be 'ivf' or 'hnsw'")
               
           return results

Fulltext Search Best Practices
------------------------------

Index Design and Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client, FulltextAlgorithmType, FulltextModeType

   class FulltextSearchManager:
       def __init__(self, client):
           self.client = client
           
       def setup_fulltext_indexes(self):
           """Setup optimized fulltext indexes"""
           # Enable fulltext indexing
           self.client.fulltext_index.enable_fulltext()
           
           # Create BM25 index for better relevance scoring
           self.client.fulltext_index.create(
               table_name='documents',
               name='ftidx_documents_content',
               columns=['title', 'content', 'tags'],
               algorithm=FulltextAlgorithmType.BM25
           )
           
           # Create separate index for metadata
           self.client.fulltext_index.create(
               table_name='documents',
               name='ftidx_documents_metadata',
               columns=['title', 'tags'],
               algorithm=FulltextAlgorithmType.TF_IDF
           )
           
       def search_documents(self, search_term, mode='natural', limit=20):
           """Search documents with different modes"""
           if mode == 'natural':
               # Natural language search
               results = self.client.fulltext_index.fulltext_search(
                   table_name='documents',
                   columns=['title', 'content', 'tags'],
                   search_term=search_term,
                   mode=FulltextModeType.NATURAL_LANGUAGE,
                   with_score=True,
                   limit=limit
               )
           elif mode == 'boolean':
               # Boolean search with operators
               results = self.client.fulltext_index.fulltext_search(
                   table_name='documents',
                   columns=['title', 'content', 'tags'],
                   search_term=search_term,
                   mode=FulltextModeType.BOOLEAN,
                   with_score=True,
                   limit=limit
               )
           else:
               raise ValueError("mode must be 'natural' or 'boolean'")
               
           return results

Security Best Practices
-----------------------

Input Validation and Sanitization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import re
   from typing import Optional, List
   from matrixone import Client

   class SecureDatabaseClient:
       def __init__(self, client):
           self.client = client
           
       def validate_username(self, username: str) -> bool:
           """Validate username format"""
           if not username or len(username) < 3 or len(username) > 50:
               return False
           # Only allow alphanumeric and underscore
           return bool(re.match(r'^[a-zA-Z0-9_]+$', username))
           
       def validate_email(self, email: str) -> bool:
           """Validate email format"""
           pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
           return bool(re.match(pattern, email))
           
       def sanitize_search_term(self, search_term: str) -> str:
           """Sanitize search term for fulltext search"""
           # Remove potentially dangerous characters
           sanitized = re.sub(r'[^\w\s-]', '', search_term)
           # Limit length
           return sanitized[:100]
           
       def create_user_safe(self, username: str, email: str, full_name: str) -> Optional[dict]:
           """Create user with validation"""
           # Validate inputs
           if not self.validate_username(username):
               raise ValueError("Invalid username format")
               
           if not self.validate_email(email):
               raise ValueError("Invalid email format")
               
           if not full_name or len(full_name) > 200:
               raise ValueError("Invalid full name")
               
           # Use parameterized queries
           try:
               result = self.client.execute(
                   "INSERT INTO users (username, email, full_name) VALUES (%s, %s, %s)",
                   (username, email, full_name)
               )
               return {'id': result.lastrowid, 'username': username, 'email': email}
           except Exception as e:
               logger.error(f"Failed to create user: {e}")
               raise

Access Control and Permissions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from functools import wraps
   from matrixone import Client

   class AccessControlManager:
       def __init__(self, client):
           self.client = client
           
       def check_user_permissions(self, user_id: int, required_permission: str) -> bool:
           """Check if user has required permission"""
           result = self.client.execute(
               """
               SELECT COUNT(*) FROM user_permissions up
               JOIN permissions p ON up.permission_id = p.id
               WHERE up.user_id = %s AND p.name = %s
               """,
               (user_id, required_permission)
           )
           return result.fetchone()[0] > 0
           
       def require_permission(self, permission: str):
           """Decorator to require specific permission"""
           def decorator(func):
               @wraps(func)
               def wrapper(self, user_id: int, *args, **kwargs):
                   if not self.check_user_permissions(user_id, permission):
                       raise PermissionError(f"User {user_id} lacks permission: {permission}")
                   return func(self, user_id, *args, **kwargs)
               return wrapper
           return decorator

   # Usage
   access_manager = AccessControlManager(client)

   class UserService:
       def __init__(self, access_manager):
           self.access_manager = access_manager
           
       @access_manager.require_permission('user.read')
       def get_user(self, user_id: int, target_user_id: int):
           """Get user information (requires read permission)"""
           result = self.client.execute(
               "SELECT id, username, email FROM users WHERE id = %s",
               (target_user_id,)
           )
           return result.fetchone()
           
       @access_manager.require_permission('user.write')
       def update_user(self, user_id: int, target_user_id: int, data: dict):
           """Update user information (requires write permission)"""
           # Implementation here
           pass

Monitoring and Logging
----------------------

Performance Monitoring
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import time
   import logging
   from functools import wraps
   from matrixone import Client

   # Configure logging
   logging.basicConfig(
       level=logging.INFO,
       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
       handlers=[
           logging.FileHandler('database.log'),
           logging.StreamHandler()
       ]
   )
   logger = logging.getLogger(__name__)

   def monitor_performance(func):
       """Decorator to monitor function performance"""
       @wraps(func)
       def wrapper(*args, **kwargs):
           start_time = time.time()
           try:
               result = func(*args, **kwargs)
               execution_time = time.time() - start_time
               
               logger.info(f"{func.__name__} executed successfully in {execution_time:.3f}s")
               
               # Log slow queries
               if execution_time > 1.0:
                   logger.warning(f"Slow query detected: {func.__name__} took {execution_time:.3f}s")
                   
               return result
               
           except Exception as e:
               execution_time = time.time() - start_time
               logger.error(f"{func.__name__} failed after {execution_time:.3f}s: {e}")
               raise
               
       return wrapper

   class MonitoredDatabaseClient:
       def __init__(self, client):
           self.client = client
           
       @monitor_performance
       def execute_query(self, query: str, params=None):
           """Execute query with performance monitoring"""
           result = self.client.execute(query, params)
           return result.fetchall()
           
       @monitor_performance
       def bulk_insert(self, table: str, data: List[dict]):
           """Bulk insert with performance monitoring"""
           with self.client.transaction() as tx:
               for record in data:
                   tx.execute(
                       f"INSERT INTO {table} VALUES (%s, %s, %s)",
                       (record['col1'], record['col2'], record['col3'])
                   )

Health Checks and Monitoring
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import time
   import threading
   from matrixone import Client

   class DatabaseHealthMonitor:
       def __init__(self, client):
           self.client = client
           self.is_healthy = True
           self.last_check = None
           self.check_interval = 30  # seconds
           self._monitoring = False
           self._monitor_thread = None
           
       def check_health(self) -> bool:
           """Check database health"""
           try:
               start_time = time.time()
               result = self.client.execute("SELECT 1")
               response_time = time.time() - start_time
               
               # Check response time
               if response_time > 5.0:
                   logger.warning(f"Slow database response: {response_time:.3f}s")
                   
               # Check connection
               if not result.fetchone():
                   return False
                   
               self.is_healthy = True
               self.last_check = time.time()
               return True
               
           except Exception as e:
               logger.error(f"Database health check failed: {e}")
               self.is_healthy = False
               return False
               
       def start_monitoring(self):
           """Start continuous health monitoring"""
           if self._monitoring:
               return
               
           self._monitoring = True
           self._monitor_thread = threading.Thread(target=self._monitor_loop)
           self._monitor_thread.daemon = True
           self._monitor_thread.start()
           
       def stop_monitoring(self):
           """Stop health monitoring"""
           self._monitoring = False
           if self._monitor_thread:
               self._monitor_thread.join()
               
       def _monitor_loop(self):
           """Monitor loop running in background"""
           while self._monitoring:
               self.check_health()
               time.sleep(self.check_interval)

Deployment Best Practices
-------------------------

Configuration Management
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import os
   import json
   from typing import Dict, Any
   from matrixone import Client

   class ConfigurationManager:
       def __init__(self, config_file: str = None):
           self.config = self._load_config(config_file)
           
       def _load_config(self, config_file: str) -> Dict[str, Any]:
           """Load configuration from file or environment"""
           config = {
               'database': {
                   'host': os.getenv('MATRIXONE_HOST', 'localhost'),
                   'port': int(os.getenv('MATRIXONE_PORT', '6001')),
                   'user': os.getenv('MATRIXONE_USER', 'root'),
                   'password': os.getenv('MATRIXONE_PASSWORD', '111'),
                   'database': os.getenv('MATRIXONE_DATABASE', 'test')
               },
               'client': {
                   'connection_timeout': int(os.getenv('CONNECTION_TIMEOUT', '30')),
                   'query_timeout': int(os.getenv('QUERY_TIMEOUT', '300')),
                   'auto_commit': os.getenv('AUTO_COMMIT', 'true').lower() == 'true',
                   'enable_performance_logging': os.getenv('ENABLE_PERFORMANCE_LOGGING', 'true').lower() == 'true',
                   'enable_sql_logging': os.getenv('ENABLE_SQL_LOGGING', 'false').lower() == 'true'
               }
           }
           
           # Override with config file if provided
           if config_file and os.path.exists(config_file):
               with open(config_file, 'r') as f:
                   file_config = json.load(f)
                   config.update(file_config)
                   
           return config
           
       def get_database_config(self) -> Dict[str, Any]:
           """Get database configuration"""
           return self.config['database']
           
       def get_client_config(self) -> Dict[str, Any]:
           """Get client configuration"""
           return self.config['client']

   # Usage
   config_manager = ConfigurationManager('config.json')
   
   client = Client(**config_manager.get_client_config())
   client.connect(**config_manager.get_database_config())

Graceful Shutdown
~~~~~~~~~~~~~~~~~

.. code-block:: python

   import signal
   import sys
   from contextlib import contextmanager
   from matrixone import Client

   class ApplicationManager:
       def __init__(self):
           self.client = None
           self.shutdown_requested = False
           self._setup_signal_handlers()
           
       def _setup_signal_handlers(self):
           """Setup signal handlers for graceful shutdown"""
           signal.signal(signal.SIGINT, self._signal_handler)
           signal.signal(signal.SIGTERM, self._signal_handler)
           
       def _signal_handler(self, signum, frame):
           """Handle shutdown signals"""
           logger.info(f"Received signal {signum}, initiating graceful shutdown...")
           self.shutdown_requested = True
           
       @contextmanager
       def get_client(self):
           """Get client with automatic cleanup"""
           if self.client is None:
               self.client = Client()
               self.client.connect(
                   host='localhost',
                   port=6001,
                   user='root',
                   password='111',
                   database='test'
               )
               
           try:
               yield self.client
           finally:
               if self.shutdown_requested:
                   self.cleanup()
                   
       def cleanup(self):
           """Cleanup resources"""
           if self.client:
               logger.info("Closing database connection...")
               self.client.disconnect()
               self.client = None
               
       def run(self):
           """Main application loop"""
           try:
               while not self.shutdown_requested:
                   with self.get_client() as client:
                       # Your application logic here
                       result = client.execute("SELECT 1")
                       print(f"Database check: {result.fetchone()}")
                       
                   time.sleep(1)
                   
           except KeyboardInterrupt:
               logger.info("Application interrupted by user")
           finally:
               self.cleanup()

   # Usage
   app_manager = ApplicationManager()
   app_manager.run()

Testing Best Practices
----------------------

Test Database Setup
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import pytest
   import tempfile
   import os
   from matrixone import Client
   from sqlalchemy import create_engine
   from sqlalchemy.orm import sessionmaker

   @pytest.fixture(scope="session")
   def test_client():
       """Create test client for the entire test session"""
       client = Client()
       client.connect(
           host='localhost',
           port=6001,
           user='root',
           password='111',
           database='test'
       )
       yield client
       client.disconnect()

   @pytest.fixture(scope="function")
   def clean_database(test_client):
       """Clean database before each test"""
       # Clean up test data
       test_client.execute("DELETE FROM users WHERE username LIKE 'test_%'")
       test_client.execute("DELETE FROM documents WHERE title LIKE 'Test %'")
       yield
       # Clean up after test
       test_client.execute("DELETE FROM users WHERE username LIKE 'test_%'")
       test_client.execute("DELETE FROM documents WHERE title LIKE 'Test %'")

   @pytest.fixture
   def test_user_data():
       """Provide test user data"""
       return {
           'username': 'test_user',
           'email': 'test@example.com',
           'full_name': 'Test User',
           'bio': 'Test user for unit tests'
       }

   def test_create_user(test_client, clean_database, test_user_data):
       """Test user creation"""
       # Test user creation
       result = test_client.execute(
           "INSERT INTO users (username, email, full_name, bio) VALUES (%s, %s, %s, %s)",
           (test_user_data['username'], test_user_data['email'], 
            test_user_data['full_name'], test_user_data['bio'])
       )
       
       user_id = result.lastrowid
       assert user_id > 0
       
       # Verify user was created
       result = test_client.execute(
           "SELECT * FROM users WHERE id = %s",
           (user_id,)
       )
       user = result.fetchone()
       assert user is not None
       assert user[1] == test_user_data['username']

   def test_vector_search(test_client, clean_database):
       """Test vector search functionality"""
       # Insert test document
       test_client.execute(
           "INSERT INTO documents (title, content, embedding) VALUES (%s, %s, %s)",
           ('Test Document', 'This is a test document', [0.1, 0.2, 0.3] + [0.0] * 381)
       )
       
       # Test vector search
       results = test_client.vector_query.similarity_search(
           table_name='documents',
           vector_column='embedding',
           query_vector=[0.1, 0.2, 0.3] + [0.0] * 381,
           limit=5,
           distance_type='l2'
       )
       
       assert len(results) > 0
       assert results[0][1] == 'Test Document'

Next Steps
----------

* Review :doc:`orm_guide` for detailed ORM patterns
* Check :doc:`vector_guide` for vector search optimization
* See :doc:`fulltext_guide` for fulltext search best practices
* Explore :doc:`examples` for comprehensive usage examples
* Run ``make test`` to verify your implementation
* Use ``make docs`` to generate updated documentation
