Fulltext Search Guide
=====================

This guide covers fulltext search capabilities in the MatrixOne Python SDK, including fulltext indexing, search modes, and advanced features.

Overview
--------

MatrixOne provides powerful fulltext search capabilities for text analysis and information retrieval:

* **Multiple Algorithms**: Support for TF-IDF and BM25 algorithms
* **Search Modes**: Natural language and boolean search modes
* **Client Interface**: Easy-to-use client methods for fulltext operations
* **ORM Integration**: Seamless integration with SQLAlchemy models
* **Multi-language Support**: Support for various languages including Chinese
* **JSON Search**: Fulltext search within JSON documents

Fulltext Index Setup
--------------------

Creating Tables with Text Content
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params

   # Get connection parameters
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Create table with text content using create_table API
   client.create_table("articles", {
       "id": "int",
       "title": "varchar(200)",
       "content": "text",
       "summary": "text",
       "author": "varchar(100)",
       "category": "varchar(50)",
       "tags": "varchar(200)",
       "metadata": "json"
   }, primary_key="id")

   # Create another table for documents
   client.create_table("documents", {
       "id": "int",
       "filename": "varchar(255)",
       "file_content": "text",
       "description": "text",
       "file_type": "varchar(50)"
   }, primary_key="id")

Creating Fulltext Indexes
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Create fulltext index using fulltext_index API
   client.fulltext_index.create("articles", "idx_content", "content", algorithm="BM25")
   client.fulltext_index.create("articles", "idx_title", "title", algorithm="TF-IDF")
   client.fulltext_index.create("documents", "idx_file_content", "file_content", algorithm="BM25")

   # List all fulltext indexes
   indexes = client.fulltext_index.list_indexes("articles")
   print("Fulltext indexes:", indexes)

   # Drop a fulltext index
   client.fulltext_index.drop_index("articles", "idx_title")

Inserting Text Data
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Insert articles using insert API
   articles = [
       {
           "id": 1,
           "title": "Introduction to Machine Learning",
           "content": "Machine learning is a subset of artificial intelligence that focuses on algorithms and statistical models. It enables computers to learn and make decisions from data without being explicitly programmed.",
           "summary": "An overview of machine learning concepts and applications",
           "author": "John Doe",
           "category": "Technology",
           "tags": "AI, ML, algorithms",
           "metadata": '{"language": "English", "difficulty": "beginner"}'
       },
       {
           "id": 2,
           "title": "Deep Learning Fundamentals",
           "content": "Deep learning uses neural networks with multiple layers to model and understand complex patterns in data. It has revolutionized fields like computer vision, natural language processing, and speech recognition.",
           "summary": "Understanding deep learning and neural networks",
           "author": "Jane Smith",
           "category": "Technology",
           "tags": "deep learning, neural networks, AI",
           "metadata": '{"language": "English", "difficulty": "intermediate"}'
       }
   ]

   for article in articles:
       client.insert("articles", article)

   # Insert documents using batch_insert API
   documents = [
       {
           "id": 1,
           "filename": "research_paper.pdf",
           "file_content": "This research paper discusses advanced machine learning techniques and their applications in real-world scenarios.",
           "description": "Academic research paper on ML",
           "file_type": "PDF"
       }
   ]

   client.batch_insert("documents", documents)

Basic Fulltext Search
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Natural language search using query API
   result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE)", "machine learning").execute()
   print("Natural language search results:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[4]}")

   # Boolean search using query API
   result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN BOOLEAN MODE)", "deep learning OR neural networks").execute()
   print("Boolean search results:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[4]}")

   # Search with relevance scoring
   result = client.query("articles").select("*, MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE) as relevance", "artificial intelligence").order_by("relevance DESC").execute()
   print("Search with relevance scoring:")
   for row in result.fetchall():
       print(f"  {row[1]} (Relevance: {row[-1]:.4f})")

Advanced Fulltext Search
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Search with multiple columns
   result = client.query("articles").select("*").where("MATCH(title, content) AGAINST(? IN NATURAL LANGUAGE MODE)", "machine learning").execute()
   print("Multi-column search results:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[4]}")

   # Search with filters
   result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE) AND category = ?", "AI", "Technology").execute()
   print("Filtered search results:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[4]}")

   # Search with limit and offset
   result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE)", "learning").limit(2).offset(1).execute()
   print("Paginated search results:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[4]}")

Boolean Search Operators
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # AND operator
   result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN BOOLEAN MODE)", "machine AND learning").execute()
   print("AND search results:")
   for row in result.fetchall():
       print(f"  {row[1]}")

   # OR operator
   result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN BOOLEAN MODE)", "deep OR neural").execute()
   print("OR search results:")
   for row in result.fetchall():
       print(f"  {row[1]}")

   # NOT operator
   result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN BOOLEAN MODE)", "machine -learning").execute()
   print("NOT search results:")
   for row in result.fetchall():
       print(f"  {row[1]}")

   # Phrase search
   result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN BOOLEAN MODE)", '"artificial intelligence"').execute()
   print("Phrase search results:")
   for row in result.fetchall():
       print(f"  {row[1]}")

Async Fulltext Operations
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from matrixone import AsyncClient
   from matrixone.config import get_connection_params

   async def async_fulltext_example():
       # Get connection parameters
       host, port, user, password, database = get_connection_params()
       
       client = AsyncClient()
       await client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create table using async create_table API
       await client.create_table("async_articles", {
           "id": "int",
           "title": "varchar(200)",
           "content": "text",
           "author": "varchar(100)"
       }, primary_key="id")

       # Create fulltext index using async fulltext_index API
       await client.fulltext_index.create("async_articles", "idx_content", "content", algorithm="BM25")

       # Insert data using async insert API
       await client.insert("async_articles", {
           "id": 1,
           "title": "Async Article",
           "content": "This is an article created using async operations for fulltext search testing.",
           "author": "Async Author"
       })

       # Fulltext search using async query API
       result = await client.query("async_articles").select("*").where("MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE)", "async operations").execute()
       print("Async fulltext search results:")
       for row in result.fetchall():
           print(f"  {row[1]} by {row[3]}")

       # Clean up
       await client.drop_table("async_articles")
       await client.disconnect()

   asyncio.run(async_fulltext_example())

ORM with Fulltext Search
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Column, Integer, String, Text
   from sqlalchemy.ext.declarative import declarative_base
   from sqlalchemy.orm import sessionmaker
   from matrixone import Client
   from matrixone.config import get_connection_params

   # Define ORM models
   Base = declarative_base()

   class Article(Base):
       __tablename__ = 'orm_articles'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(200), nullable=False)
       content = Column(Text, nullable=False)
       author = Column(String(100))
       category = Column(String(50))

   def orm_fulltext_example():
       # Get connection parameters
       host, port, user, password, database = get_connection_params()
       client = Client()
       client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create table using ORM model
       client.create_table(Article)

       # Create fulltext index
       client.fulltext_index.create("orm_articles", "idx_content", "content", algorithm="BM25")

       # Create session
       Session = sessionmaker(bind=client.get_sqlalchemy_engine())
       session = Session()

       # Insert data using ORM
       article1 = Article(
           title="ORM Article 1",
           content="This article demonstrates fulltext search with ORM models in MatrixOne.",
           author="ORM Author",
           category="Technology"
       )
       
       session.add(article1)
       session.commit()

       # Fulltext search using query API
       result = client.query("orm_articles").select("*").where("MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE)", "fulltext search").execute()
       print("ORM fulltext search results:")
       for row in result.fetchall():
           print(f"  {row[1]} by {row[3]}")

       # Clean up
       client.drop_table(Article)
       session.close()
       client.disconnect()

   orm_fulltext_example()

Error Handling
~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.exceptions import QueryError, ConnectionError
   from matrixone.config import get_connection_params

   def error_handling_example():
       client = None
       
       try:
           host, port, user, password, database = get_connection_params()
           
           # Create client with error handling
           client = Client()
           client.connect(host=host, port=port, user=user, password=password, database=database)

           # Create table with error handling
           try:
               client.create_table("error_articles", {
                   "id": "int",
                   "content": "text"
               }, primary_key="id")
               print("✓ Table created successfully")
           except QueryError as e:
               print(f"❌ Table creation failed: {e}")

           # Create fulltext index with error handling
           try:
               client.fulltext_index.create("error_articles", "idx_content", "content", algorithm="BM25")
               print("✓ Fulltext index created successfully")
           except QueryError as e:
               print(f"❌ Fulltext index creation failed: {e}")

           # Insert data with error handling
           try:
               client.insert("error_articles", {"id": 1, "content": "Test content for fulltext search"})
               print("✓ Data inserted successfully")
           except QueryError as e:
               print(f"❌ Data insertion failed: {e}")

           # Fulltext search with error handling
           try:
               result = client.query("error_articles").select("*").where("MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE)", "test content").execute()
               print(f"✓ Fulltext search successful: {len(result.fetchall())} results")
           except QueryError as e:
               print(f"❌ Fulltext search failed: {e}")

       except ConnectionError as e:
           print(f"❌ Connection failed: {e}")
       except Exception as e:
           print(f"❌ Unexpected error: {e}")
       finally:
           # Always clean up
           if client:
               try:
                   client.drop_table("error_articles")
                   client.disconnect()
                   print("✓ Cleanup completed")
               except Exception as e:
                   print(f"⚠️ Cleanup warning: {e}")

   error_handling_example()

Best Practices
~~~~~~~~~~~~~~

1. **Choose the right algorithm**:
   - Use BM25 for general fulltext search
   - Use TF-IDF for specific use cases

2. **Optimize index creation**:
   - Create indexes after data insertion
   - Use appropriate column types for text content

3. **Use appropriate search modes**:
   - Natural language mode for general search
   - Boolean mode for complex queries

4. **Handle errors gracefully**:
   - Always use try-catch blocks
   - Provide meaningful error messages
   - Clean up resources properly

5. **Optimize performance**:
   - Use batch operations for large datasets
   - Create indexes on frequently searched columns
   - Use appropriate search operators

Next Steps
----------

* Read the :doc:`api/fulltext_index` for detailed fulltext index API
* Check out the :doc:`api/fulltext_search` for fulltext search API
* Explore :doc:`vector_guide` for vector search capabilities
* Learn about :doc:`orm_guide` for ORM patterns with fulltext search
* Check out the :doc:`examples` for comprehensive usage examples