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

Table Management with Table Models and Modern API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import declarative_base
   from matrixone.config import get_connection_params
   from sqlalchemy import Column, Integer, String, DECIMAL, Text, DateTime

   # Get connection parameters
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Define table model
   Base = declarative_base()

   class Product(Base):
       __tablename__ = 'products'
       id = Column(Integer, primary_key=True)
       name = Column(String(200))
       category = Column(String(50))
       price = Column(DECIMAL(10, 2))
       description = Column(Text)
       created_at = Column(DateTime)

   # Create table using model
   client.create_table(Product)

   # Alternative: Create table using create_table API with column definitions
   client.create_table("products_alt", {
       "id": "int",
       "name": "varchar(200)",
       "category": "varchar(50)",
       "price": "decimal(10,2)",
       "description": "text",
       "created_at": "datetime"
   }, primary_key="id")

   # Insert data using insert API
   client.insert(Product, {
       "id": 1,
       "name": "Laptop",
       "category": "Electronics",
       "price": 999.99,
       "description": "High-performance laptop",
       "created_at": "2024-01-01 10:00:00"
   })

   # Batch insert using batch_insert API
   products = [
       {"id": 2, "name": "Phone", "category": "Electronics", "price": 699.99, "description": "Smartphone", "created_at": "2024-01-01 10:00:00"},
       {"id": 3, "name": "Book", "category": "Education", "price": 29.99, "description": "Programming guide", "created_at": "2024-01-01 10:00:00"}
   ]
   client.batch_insert(Product, products)

   # Simple query using execute API - direct SQL execution for simple cases
   result = client.execute("SELECT * FROM products WHERE category = ?", ("Electronics",))
   print("Electronics products (simple query):")
   for row in result.fetchall():
       print(f"  {row[1]} - ${row[3]}")

   # ORM-style query using query builder - type-safe, modern syntax
   # Filter by category using SQLAlchemy column expressions
   result = client.query(Product).select("*").filter(Product.category == "Electronics").execute()
   print("Electronics products (ORM query builder):")
   for row in result.fetchall():
       print(f"  {row[1]} - ${row[3]}")

   # Update data using ORM-style query API - type-safe updates
   # Updates the price for the product with id=1
   client.query(Product).update({"price": 1099.99}).filter(Product.id == 1).execute()

   # Delete data using ORM-style query API - type-safe deletions
   # Deletes the product with id=3
   client.query(Product).filter(Product.id == 3).delete()

   # Drop table using drop_table API
   client.drop_table(Product)

   client.disconnect()

Async Operations with Modern API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   from matrixone.config import get_connection_params

   async def async_example():
       # Get connection parameters
       host, port, user, password, database = get_connection_params()
       
       client = AsyncClient()
       await client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create table using async create_table API
       await client.create_table("async_orders", {
           "id": "int",
           "customer_id": "int",
           "product_id": "int",
           "quantity": "int",
           "total": "decimal(10,2)",
           "order_date": "datetime"
       }, primary_key="id")

       # Insert data using async insert API
       await client.insert("async_orders", {
           "id": 1,
           "customer_id": 100,
           "product_id": 1,
           "quantity": 2,
           "total": 1999.98,
           "order_date": "2024-01-01 10:00:00"
       })

       # Batch insert using async batch_insert API
       orders = [
           {"id": 2, "customer_id": 101, "product_id": 2, "quantity": 1, "total": 699.99, "order_date": "2024-01-01 10:00:00"},
           {"id": 3, "customer_id": 102, "product_id": 1, "quantity": 1, "total": 999.99, "order_date": "2024-01-01 10:00:00"}
       ]
       await client.batch_insert("async_orders", orders)

       # Query data using async query API - string-based queries for async operations
       result = await client.query("async_orders").select("*").where("customer_id = ?", 100).execute()
       print("Orders for customer 100:")
       for row in result.fetchall():
           print(f"  Order {row[0]}: {row[2]} x {row[3]} = ${row[4]}")

       # Update data using async query API - batch updates with multiple fields
       # Updates both quantity and total for the order with id=1
       await client.query("async_orders").update({"quantity": 3, "total": 2999.97}).where("id = ?", 1).execute()

       # Delete data using async query API - removes specific order
       # Deletes the order with id=3
       await client.query("async_orders").where("id = ?", 3).delete()

       # Drop table using async drop_table API
       await client.drop_table("async_orders")
       await client.disconnect()

   asyncio.run(async_example())

ORM Examples with Modern API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Column, Integer, String, DECIMAL, DateTime, ForeignKey
   from sqlalchemy.orm import sessionmaker, relationship
   from matrixone import Client
   from matrixone.config import get_connection_params
   from matrixone.orm import declarative_base

   # Define ORM models
   Base = declarative_base()

   class Customer(Base):
       __tablename__ = 'customers'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       name = Column(String(100), nullable=False)
       email = Column(String(200), unique=True, nullable=False)
       created_at = Column(DateTime, nullable=False)
       
       # Relationship
       orders = relationship("Order", back_populates="customer")

   class Order(Base):
       __tablename__ = 'orders'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       customer_id = Column(Integer, ForeignKey('customers.id'), nullable=False)
       total = Column(DECIMAL(10, 2), nullable=False)
       status = Column(String(20), nullable=False, default='pending')
       created_at = Column(DateTime, nullable=False)
       
       # Relationship
       customer = relationship("Customer", back_populates="orders")

   def orm_example():
       # Get connection parameters
       host, port, user, password, database = get_connection_params()
       client = Client()
       client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create tables using ORM models
       client.create_table(Customer)
       client.create_table(Order)

       # Create session
       Session = sessionmaker(bind=client.get_sqlalchemy_engine())
       session = Session()

       # Insert data using ORM
       customer1 = Customer(name="Alice Johnson", email="alice@example.com", created_at="2024-01-01 10:00:00")
       customer2 = Customer(name="Bob Smith", email="bob@example.com", created_at="2024-01-01 10:00:00")
       session.add_all([customer1, customer2])
       session.commit()

       order1 = Order(customer_id=1, total=199.99, status="completed", created_at="2024-01-01 11:00:00")
       order2 = Order(customer_id=2, total=299.99, status="pending", created_at="2024-01-01 12:00:00")
       session.add_all([order1, order2])
       session.commit()

       # Query using ORM with relationships
       customers_with_orders = session.query(Customer).join(Order).all()
       print("Customers with orders:")
       for customer in customers_with_orders:
           print(f"  {customer.name} - {customer.email}")
           for order in customer.orders:
               print(f"    Order {order.id}: ${order.total} ({order.status})")

       # Update using ORM
       session.query(Order).filter(Order.status == "pending").update({"status": "processing"})
       session.commit()

       # Delete using ORM
       session.query(Order).filter(Order.status == "completed").delete()
       session.commit()

       # Clean up
       client.drop_table(Order)
       client.drop_table(Customer)
       session.close()
       client.disconnect()

   orm_example()

Complex Query Examples with Query Builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params
   from sqlalchemy import Column, Integer, String, DECIMAL, DateTime, ForeignKey, func, exists
   from matrixone.orm import declarative_base

   def complex_query_examples():
       # Get connection parameters
       host, port, user, password, database = get_connection_params()
       client = Client()
       client.connect(host=host, port=port, user=user, password=password, database=database)

       # Define table models
       Base = declarative_base()

       class Customer(Base):
           __tablename__ = 'customers'
           id = Column(Integer, primary_key=True)
           name = Column(String(100))
           email = Column(String(200))
           city = Column(String(50))

       class Order(Base):
           __tablename__ = 'orders'
           id = Column(Integer, primary_key=True)
           customer_id = Column(Integer, ForeignKey('customers.id'))
           total = Column(DECIMAL(10, 2))
           status = Column(String(20))
           created_at = Column(DateTime)

       class Product(Base):
           __tablename__ = 'products'
           id = Column(Integer, primary_key=True)
           name = Column(String(200))
           price = Column(DECIMAL(10, 2))
           category = Column(String(50))

       # Create tables
       client.create_table(Customer)
       client.create_table(Order)
       client.create_table(Product)

       # Insert sample data
       client.batch_insert("customers", [
           {"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "city": "New York"},
           {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "city": "Los Angeles"},
           {"id": 3, "name": "Charlie Brown", "email": "charlie@example.com", "city": "Chicago"}
       ])

       client.batch_insert("orders", [
           {"id": 1, "customer_id": 1, "total": 199.99, "status": "completed", "created_at": "2024-01-01 10:00:00"},
           {"id": 2, "customer_id": 2, "total": 299.99, "status": "pending", "created_at": "2024-01-02 11:00:00"},
           {"id": 3, "customer_id": 1, "total": 149.99, "status": "completed", "created_at": "2024-01-03 12:00:00"}
       ])

       client.batch_insert("products", [
           {"id": 1, "name": "Laptop", "price": 999.99, "category": "Electronics"},
           {"id": 2, "name": "Phone", "price": 699.99, "category": "Electronics"},
           {"id": 3, "name": "Book", "price": 29.99, "category": "Education"}
       ])

       # 1. JOIN query with aggregation - combining tables and calculating statistics
       # Joins customers with their orders, groups by customer, and calculates totals
       result = client.query(Customer).select(
           Customer.name,
           Customer.city,
           func.count(Order.id).label("order_count"),
           func.sum(Order.total).label("total_spent")
       ).join(Order)
       .where(Order.status == "completed")  # Only completed orders
       .group_by(Customer.id, Customer.name, Customer.city)  # Group by customer
       .having(func.count(Order.id) > 0)  # Only customers with orders
       .order_by(func.sum(Order.total).desc())  # Sort by total spent
       .execute()

       print("Customer order summary:")
       for row in result.fetchall():
           print(f"  {row[0]} ({row[1]}): {row[2]} orders, ${row[3]}")

       # 2. CTE (Common Table Expression) query - reusable subqueries
       # Creates a temporary named result set for complex queries
       cte = client.query(Order).select(
           Order.id,
           Order.customer_id,
           Order.total,
           Order.status
       ).cte("order_stats")

       result = client.query(Customer).select(
           Customer.name,
           Customer.city,
           func.count(Order.id).label("order_count"),
           func.sum(Order.total).label("total_spent")
       ).with_cte(cte)  # Use the CTE
       .join(cte)  # Join with the CTE
       .where(cte.status == "completed")
       .group_by(Customer.id, Customer.name, Customer.city)
       .having(func.count(Order.id) > 0)
       .order_by(func.sum(Order.total).desc())
       .execute()

       print("\nCustomer analysis (using CTE):")
       for row in result.fetchall():
           print(f"  {row[0]}: {row[1]} orders, avg ${row[2]:.2f} ({row[3]})")

       # 3. Subquery with EXISTS - finding customers with specific conditions
       # Uses EXISTS to check if customer has any completed orders
       result = client.query(Customer).select(
           Customer.name, Customer.email
       ).where(
           exists().where(
               (Order.customer_id == Customer.id) & (Order.status == "completed")
           )
       ).execute()

       print("\nCustomers with completed orders:")
       for row in result.fetchall():
           print(f"  {row[0]} - {row[1]}")

       # 4. Complex UPDATE with JOIN - updating based on related table conditions
       # Updates order status based on customer location and current status
       client.query(Order).update(
           {"status": "processing"}
       ).join(Customer).where(
           (Customer.city == "New York") & (Order.status == "pending")
       ).execute()

       # 5. Complex DELETE with subquery - deleting based on complex conditions
       # Creates subquery to find orders from customers in Chicago, then deletes them
       subquery = client.query(Order.id).join(Customer).where(
           Customer.city == "Chicago"
       ).subquery()
       
       client.query(Order).where(
           Order.id.in_(subquery)  # Use subquery in IN clause
       ).delete()

       # 6. Window functions - advanced ranking and analytics
       # Ranks orders by customer based on order total (highest first)
       result = client.query(Order).select(
           Order.id, Order.customer_id, Order.total,
           func.row_number().over(
               partition_by=Order.customer_id,  # Reset rank for each customer
               order_by=Order.total.desc()  # Order by total descending
           ).label("rank")
       ).execute()

       print("\nOrder ranking by customer:")
       for row in result.fetchall():
           print(f"  Order {row[0]} (Customer {row[1]}): ${row[2]} (Rank: {row[3]})")

       # Clean up
       client.drop_table(Product)
       client.drop_table(Order)
       client.drop_table(Customer)
       client.disconnect()

   complex_query_examples()

Vector Search Examples
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params
   from matrixone.sqlalchemy_ext import create_vector_column
   import numpy as np

   def vector_search_example():
       # Get connection parameters
       host, port, user, password, database = get_connection_params()
       client = Client()
       client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create vector table using create_table API
       client.create_table("documents", {
           "id": "int",
           "title": "varchar(200)",
           "content": "text",
           "embedding": "vecf32(384)",  # 384-dimensional f32 vector
           "category": "varchar(50)"
       }, primary_key="id")

       # Enable IVF indexing
       client.vector_ops.enable_ivf()

       # Create vector index using vector_ops API
       client.vector_ops.create_ivf(
           "documents",  # Table name as positional argument
           name="idx_embedding",
           column="embedding",
           lists=50,
           op_type="vector_l2_ops"
       )

       # Insert documents with embeddings using insert API
       documents = [
           {
               "id": 1,
               "title": "AI Research Paper",
               "content": "Advanced artificial intelligence research and applications",
               "embedding": np.random.rand(384).astype(np.float32).tolist(),
               "category": "research"
           },
           {
               "id": 2,
               "title": "Machine Learning Guide",
               "content": "Comprehensive machine learning tutorial and best practices",
               "embedding": np.random.rand(384).astype(np.float32).tolist(),
               "category": "tutorial"
           },
           {
               "id": 3,
               "title": "Data Science Handbook",
               "content": "Complete data science reference and methodology",
               "embedding": np.random.rand(384).astype(np.float32).tolist(),
               "category": "reference"
           }
       ]

       for doc in documents:
           client.insert("documents", doc)

       # Vector similarity search using vector_ops API - modern vector search interface
       query_vector = np.random.rand(384).astype(np.float32).tolist()
       
       # L2 distance search - Euclidean distance for geometric similarity
       # Lower distances indicate more similar vectors
       results = client.vector_ops.similarity_search(
           "documents",  # Table name as positional argument
           vector_column="embedding",
           query_vector=query_vector,
           limit=3,
           distance_type="l2"  # Use l2 for geometric distance
       )

       print("L2 Distance Search Results:")
       for result in results.rows:
           print(f"  {result[1]} (Distance: {result[-1]:.4f})")

       # Cosine distance search - angular similarity for semantic similarity
       # Lower cosine distance = higher semantic similarity
       cosine_results = client.vector_ops.similarity_search(
           "documents",  # Table name as positional argument
           vector_column="embedding",
           query_vector=query_vector,
           limit=3,
           distance_type="cosine"  # Use cosine for semantic similarity
       )

       print("Cosine Distance Search Results:")
       for result in cosine_results.rows:
           print(f"  {result[1]} (Similarity: {1 - result[-1]:.4f})")

       # Search with metadata filtering - combining vector search with SQL filters
       # Filters results by category before performing vector similarity
       filtered_results = client.vector_ops.similarity_search(
           "documents",  # Table name as positional argument
           vector_column="embedding",
           query_vector=query_vector,
           limit=2,
           distance_type="l2",
           where_clause="category = 'research'"  # SQL filter applied before vector search
       )

       print("Filtered Search Results (research category):")
       for result in filtered_results.rows:
           print(f"  {result[1]} (Distance: {result[-1]:.4f})")

       # Clean up
       client.drop_table("documents")
       client.disconnect()

   vector_search_example()

Async Vector Operations
~~~~~~~~~~~~~~~~~~~~~~~

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
       await client.create_table("async_products", {
           "id": "int",
           "name": "varchar(200)",
           "description": "text",
           "features": "vecf64(512)",  # 512-dimensional f64 vector
           "category": "varchar(50)"
       }, primary_key="id")

       # Enable IVF indexing
       await client.vector_ops.enable_ivf()

       # Create vector index using async vector_ops API
       await client.vector_ops.create_ivf(
           "async_products",  # Table name as positional argument
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
               "description": "Latest smartphone with AI features and advanced camera",
               "features": np.random.rand(512).astype(np.float64).tolist(),
               "category": "electronics"
           },
           {
               "id": 2,
               "name": "Laptop",
               "description": "High-performance laptop for professionals and developers",
               "features": np.random.rand(512).astype(np.float64).tolist(),
               "category": "electronics"
           },
           {
               "id": 3,
               "name": "Headphones",
               "description": "Premium wireless headphones with noise cancellation",
               "features": np.random.rand(512).astype(np.float64).tolist(),
               "category": "audio"
           }
       ]

       for product in products:
           await client.insert("async_products", product)

       # Vector similarity search using async vector_ops API - non-blocking vector search
       query_vector = np.random.rand(512).astype(np.float64).tolist()
       
       # Async similarity search with cosine distance for semantic similarity
       results = await client.vector_ops.similarity_search(
           "async_products",  # Table name as positional argument
           vector_column="features",
           query_vector=query_vector,
           limit=3,
           distance_type="cosine"  # Cosine similarity for semantic matching
       )

       print("Async Vector Search Results:")
       for result in results.rows:
           print(f"  {result[1]} (Similarity: {1 - result[-1]:.4f})")

       # Search with pagination - handling large result sets efficiently
       # Page 1: Get first 2 results
       results_page1 = await client.vector_ops.similarity_search(
           "async_products",  # Table name as positional argument
           vector_column="features",
           query_vector=query_vector,
           limit=2,
           offset=0,  # Start from beginning
           distance_type="cosine"
       )

       # Page 2: Get next 2 results (skip first 2)
       results_page2 = await client.vector_ops.similarity_search(
           "async_products",  # Table name as positional argument
           vector_column="features",
           query_vector=query_vector,
           limit=2,
           offset=2,  # Skip first 2 results
           distance_type="cosine"
       )

       print("Page 1 Results:")
       for result in results_page1.rows:
           print(f"  {result[1]}")

       print("Page 2 Results:")
       for result in results_page2.rows:
           print(f"  {result[1]}")

       # Clean up
       await client.drop_table("async_products")
       await client.disconnect()

   asyncio.run(async_vector_example())

Transaction Management Examples
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params

   def transaction_example():
       host, port, user, password, database = get_connection_params()
       client = Client()
       client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create tables using create_table API
       client.create_table("accounts", {
           "id": "int",
           "name": "varchar(100)",
           "balance": "decimal(10,2)"
       }, primary_key="id")

       client.create_table("transactions", {
           "id": "int",
           "from_account_id": "int",
           "to_account_id": "int",
           "amount": "decimal(10,2)",
           "timestamp": "datetime"
       }, primary_key="id")

       # Insert initial data
       client.insert("accounts", {"id": 1, "name": "Alice", "balance": 1000.00})
       client.insert("accounts", {"id": 2, "name": "Bob", "balance": 500.00})

       # Transfer money using transaction - ensuring data consistency
       # All operations must succeed or all are rolled back
       try:
           with client.transaction() as tx:
               # Update sender balance - deduct $100 from Alice's account
               tx.query("accounts").update({"balance": 900.00}).where("id = ?", 1).execute()
               
               # Update receiver balance - add $100 to Bob's account
               tx.query("accounts").update({"balance": 600.00}).where("id = ?", 2).execute()
               
               # Record transaction - create audit trail
               tx.insert("transactions", {
                   "id": 1,
                   "from_account_id": 1,
                   "to_account_id": 2,
                   "amount": 100.00,
                   "timestamp": "2024-01-01 10:00:00"
               })
               
               # If any operation fails, the entire transaction is automatically rolled back
               # This ensures data consistency and prevents partial updates
               
           print("✓ Transaction completed successfully")
           
       except Exception as e:
           print(f"❌ Transaction failed: {e}")
           # Transaction automatically rolled back on exception

       # Verify the transfer
       result = client.query("accounts").select("*").execute()
       print("Account balances after transfer:")
       for row in result.fetchall():
           print(f"  {row[1]}: ${row[2]}")

       # Check transaction record
       result = client.query("transactions").select("*").execute()
       print("Transaction records:")
       for row in result.fetchall():
           print(f"  {row[1]} -> {row[2]}: ${row[3]}")

       # Clean up
       client.drop_table("transactions")
       client.drop_table("accounts")
       client.disconnect()

   transaction_example()

Fulltext Search Examples
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params

   def fulltext_search_example():
       host, port, user, password, database = get_connection_params()
       client = Client()
       client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create table with text content using create_table API
       client.create_table("articles", {
           "id": "int",
           "title": "varchar(200)",
           "content": "text",
           "author": "varchar(100)",
           "published_date": "date"
       }, primary_key="id")

       # Insert articles using insert API
       articles = [
           {
               "id": 1,
               "title": "Introduction to Machine Learning",
               "content": "Machine learning is a subset of artificial intelligence that focuses on algorithms and statistical models.",
               "author": "John Doe",
               "published_date": "2024-01-01"
           },
           {
               "id": 2,
               "title": "Deep Learning Fundamentals",
               "content": "Deep learning uses neural networks with multiple layers to model and understand complex patterns.",
               "author": "Jane Smith",
               "published_date": "2024-01-02"
           },
           {
               "id": 3,
               "title": "Natural Language Processing",
               "content": "NLP combines computational linguistics with machine learning to process human language.",
               "author": "Bob Johnson",
               "published_date": "2024-01-03"
           }
       ]

       for article in articles:
           client.insert("articles", article)

       # Create fulltext index using fulltext_index API
       client.fulltext_index.create("articles", "idx_content", "content", algorithm="BM25")

       # Natural language fulltext search - user-friendly search with automatic processing
       # Handles synonyms, stemming, and stopword removal automatically
       result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE)", "machine learning").execute()
       print("Fulltext search results for 'machine learning':")
       for row in result.fetchall():
           print(f"  {row[1]} by {row[3]}")

       # Boolean fulltext search - precise control with operators (OR, AND, NOT, etc.)
       # Use boolean operators for exact term matching and complex queries
       result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN BOOLEAN MODE)", "deep learning OR neural networks").execute()
       print("Boolean fulltext search results:")
       for row in result.fetchall():
           print(f"  {row[1]} by {row[3]}")

       # Fulltext search with relevance scoring - ranked results by relevance
       # Higher relevance scores indicate better matches; useful for result ranking
       result = client.query("articles").select("*, MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE) as relevance", "artificial intelligence").order_by("relevance DESC").execute()
       print("Fulltext search with relevance scoring:")
       for row in result.fetchall():
           print(f"  {row[1]} (Relevance: {row[-1]:.4f})")

       # Clean up
       client.drop_table("articles")
       client.disconnect()

   fulltext_search_example()

ORM-Style Fulltext Search Examples
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Modern ORM-style fulltext search with boolean_match and natural_match:

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params
   from matrixone.orm import declarative_base
   from sqlalchemy import Column, Integer, String, Text
   from matrixone.sqlalchemy_ext.fulltext_search import boolean_match, natural_match, group

   # Define ORM models for fulltext search
   Base = declarative_base()

   class Article(Base):
       __tablename__ = 'orm_articles'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(200), nullable=False)
       content = Column(Text, nullable=False)
       tags = Column(String(500))
       category = Column(String(50))

   def orm_fulltext_search_example():
       host, port, user, password, database = get_connection_params()
       client = Client()
       client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create table using ORM model
       client.create_table(Article)

       # Create fulltext index on content and tags columns
       client.fulltext_index.create("orm_articles", "idx_content_tags", "content,tags", algorithm="BM25")

       # Insert articles with batch_insert for efficiency
       articles = [
           {"title": "Python Programming Guide", "content": "Learn Python programming from basics to advanced concepts.", "tags": "python,programming,tutorial", "category": "Programming"},
           {"title": "Machine Learning with Python", "content": "Introduction to machine learning using Python and scikit-learn.", "tags": "python,machine-learning,AI", "category": "AI"},
           {"title": "Web Development Tutorial", "content": "Build modern web applications with Python and Django framework.", "tags": "python,web,django", "category": "Web"}
       ]
       client.batch_insert(Article, articles)

       # 1. Natural language search - user-friendly, handles variations automatically
       result = client.query(Article).filter(natural_match(Article.content, "python programming")).execute()
       print("Natural language search results:")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 2. Boolean search with must conditions - exact term matching
       result = client.query(Article).filter(boolean_match(Article.content).must("python")).execute()
       print("\nBoolean search - must contain 'python':")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 3. Boolean search with exclusion - filter out unwanted results
       result = client.query(Article).filter(
           boolean_match(Article.content).must("python").must_not("django")
       ).execute()
       print("\nBoolean search - must have 'python', must not have 'django':")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 4. Boolean search with preference - boost relevance without filtering
       result = client.query(Article).filter(
           boolean_match(Article.content).must("python").encourage("tutorial")
       ).execute()
       print("\nBoolean search - must have 'python', encourage 'tutorial':")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 5. Group search - logical OR within required conditions
       result = client.query(Article).filter(
           boolean_match(Article.content).must(group().medium("programming", "machine"))
       ).execute()
       print("\nGroup search - must contain either 'programming' or 'machine':")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 6. Combined fulltext and SQL filters - mix search with metadata
       result = client.query(Article).filter(
           boolean_match(Article.content).must("python")  # Fulltext search
       ).filter(
           Article.category == "Programming"  # SQL filter
       ).execute()
       print("\nCombined fulltext and SQL filters:")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 7. Complex boolean search with multiple conditions
       result = client.query(Article).filter(
           boolean_match(Article.content)
           .must("python")                                    # Required
           .must(group().medium("programming", "machine"))    # Required group
           .encourage("tutorial")                             # Preferred
           .discourage("legacy")                              # Discouraged
       ).execute()
       print("\nComplex boolean search:")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # Clean up
       client.drop_table(Article)
       client.disconnect()

   orm_fulltext_search_example()

Error Handling Examples
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.exceptions import ConnectionError, QueryError
   from matrixone.config import get_connection_params

   def error_handling_example():
       client = None
       
       try:
           host, port, user, password, database = get_connection_params()
           
           # Create client with error handling
           client = Client()
           client.connect(host=host, port=port, user=user, password=password, database=database)

           # Create table with error handling - robust table creation
           try:
               client.create_table("error_test", {
                   "id": "int",
                   "name": "varchar(100)"
               }, primary_key="id")
               print("✓ Table created successfully")
           except QueryError as e:
               print(f"❌ Table creation failed: {e}")

           # Insert data with error handling - safe data insertion
           try:
               client.insert("error_test", {"id": 1, "name": "Test"})
               print("✓ Data inserted successfully")
           except QueryError as e:
               print(f"❌ Data insertion failed: {e}")

           # Query data with error handling - safe data retrieval
           try:
               result = client.query("error_test").select("*").execute()
               print(f"✓ Query successful: {result.fetchall()}")
           except QueryError as e:
               print(f"❌ Query failed: {e}")

           # Update data with error handling - safe data modification
           try:
               client.query("error_test").update({"name": "Updated"}).where("id = ?", 1).execute()
               print("✓ Data updated successfully")
           except QueryError as e:
               print(f"❌ Data update failed: {e}")

           # Delete data with error handling - safe data removal
           try:
               client.query("error_test").where("id = ?", 1).delete()
               print("✓ Data deleted successfully")
           except QueryError as e:
               print(f"❌ Data deletion failed: {e}")

       except ConnectionError as e:
           print(f"❌ Connection failed: {e}")
       except Exception as e:
           print(f"❌ Unexpected error: {e}")
       finally:
           # Always clean up
           if client:
               try:
                   client.drop_table("error_test")
                   client.disconnect()
                   print("✓ Cleanup completed")
               except Exception as e:
                   print(f"⚠️ Cleanup warning: {e}")

   error_handling_example()

Performance Optimization Examples
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params
   import time

   def performance_example():
       host, port, user, password, database = get_connection_params()
       client = Client()
       client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create table for performance testing
       client.create_table("performance_test", {
           "id": "int",
           "name": "varchar(100)",
           "value": "decimal(10,2)",
           "category": "varchar(50)"
       }, primary_key="id")

       # Batch insert for better performance - inserting 1000 records efficiently
       # Batch operations are significantly faster than individual inserts
       start_time = time.time()
       
       large_dataset = []
       for i in range(1000):
           large_dataset.append({
               "id": i,
               "name": f"Item {i}",
               "value": i * 10.5,
               "category": "category_" + str(i % 10)  # 10 different categories
           })

       client.batch_insert("performance_test", large_dataset)
       
       insert_time = time.time() - start_time
       print(f"✓ Batch insert of 1000 records completed in {insert_time:.2f} seconds")

       # Query with index optimization - efficient data retrieval
       # Indexes on frequently queried columns improve query performance
       start_time = time.time()
       result = client.query("performance_test").select("*").where("category = ?", "category_1").execute()
       query_time = time.time() - start_time
       print(f"✓ Query completed in {query_time:.2f} seconds, returned {len(result.fetchall())} records")

       # Batch update for better performance - updating multiple records efficiently
       # Bulk updates are faster than individual record updates
       start_time = time.time()
       client.query("performance_test").update({"value": 999.99}).where("category = ?", "category_1").execute()
       update_time = time.time() - start_time
       print(f"✓ Batch update completed in {update_time:.2f} seconds")

       # Batch delete for better performance - removing multiple records efficiently
       # Bulk deletes are faster than individual record deletions
       start_time = time.time()
       client.query("performance_test").where("category = ?", "category_1").delete()
       delete_time = time.time() - start_time
       print(f"✓ Batch delete completed in {delete_time:.2f} seconds")

       # Clean up
       client.drop_table("performance_test")
       client.disconnect()

   performance_example()

Next Steps
----------

* Read the :doc:`quickstart` for a quick introduction
* Check out the :doc:`api/index` for detailed API documentation
* Explore :doc:`vector_guide` for comprehensive vector operations
* Learn about :doc:`fulltext_guide` for text search capabilities
* Read the :doc:`orm_guide` for ORM patterns and best practices
* Run ``make examples`` to test all examples with your MatrixOne setup
* Use ``make test`` to run the test suite and verify your setup