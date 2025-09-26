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

Defining Models with Text Columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Column, Integer, String, Text, JSON
   from sqlalchemy.ext.declarative import declarative_base
   from matrixone import Client, FulltextAlgorithmType

   Base = declarative_base()

   class Article(Base):
       __tablename__ = 'articles'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(200), nullable=False)
       content = Column(Text, nullable=False)
       summary = Column(Text)
       author = Column(String(100))
       category = Column(String(50))
       tags = Column(String(200))
       metadata = Column(JSON)

   class Document(Base):
       __tablename__ = 'documents'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       filename = Column(String(255), nullable=False)
       file_content = Column(Text, nullable=False)
       description = Column(Text)
       keywords = Column(String(500))

   class ChineseArticle(Base):
       __tablename__ = 'chinese_articles'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(200), nullable=False)
       content = Column(Text, nullable=False)
       author = Column(String(100))

Creating Fulltext Indexes
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client, FulltextAlgorithmType
   from matrixone.config import get_connection_params

   # Get connection and create client
   host, port, user, password, database = get_connection_params()
   client = Client()
   client.connect(host=host, port=port, user=user, password=password, database=database)

   # Create tables using ORM
   client.create_all(Base)

   # Enable fulltext indexing
   client.fulltext_index.enable_fulltext()
   print("✓ Fulltext indexing enabled")

   # Create fulltext index on articles table
   client.fulltext_index.create(
       table_name='articles',
       name='ftidx_articles_content',
       columns=['title', 'content', 'summary', 'tags'],
       algorithm=FulltextAlgorithmType.BM25
   )
   print("✓ Fulltext index created on articles")

   # Create fulltext index on documents table
   client.fulltext_index.create(
       table_name='documents',
       name='ftidx_documents_content',
       columns=['filename', 'file_content', 'description', 'keywords'],
       algorithm=FulltextAlgorithmType.TF_IDF
   )
   print("✓ Fulltext index created on documents")

   # Create fulltext index for Chinese content
   client.fulltext_index.create(
       table_name='chinese_articles',
       name='ftidx_chinese_content',
       columns=['title', 'content'],
       algorithm=FulltextAlgorithmType.BM25
   )
   print("✓ Fulltext index created for Chinese content")

   client.disconnect()

Basic Fulltext Search
----------------------

Natural Language Mode Search
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client, FulltextModeType
   from matrixone.config import get_connection_params

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Insert sample articles
   articles_data = [
       {
           'title': 'Introduction to Machine Learning',
           'content': 'Machine learning is a subset of artificial intelligence that focuses on algorithms and statistical models.',
           'summary': 'Overview of ML concepts and applications',
           'author': 'Dr. Jane Smith',
           'category': 'Technology',
           'tags': 'machine learning, AI, algorithms, data science'
       },
       {
           'title': 'Database Optimization Techniques',
           'content': 'Database optimization involves improving query performance, indexing strategies, and schema design.',
           'summary': 'Best practices for database performance',
           'author': 'John Doe',
           'category': 'Database',
           'tags': 'database, optimization, performance, indexing'
       },
       {
           'title': 'Python Web Development',
           'content': 'Python offers excellent frameworks like Django and Flask for building modern web applications.',
           'summary': 'Guide to Python web frameworks',
           'author': 'Alice Johnson',
           'category': 'Programming',
           'tags': 'python, web development, django, flask'
       },
       {
           'title': 'Artificial Intelligence in Healthcare',
           'content': 'AI applications in healthcare include medical imaging, drug discovery, and patient diagnosis.',
           'summary': 'AI transforming healthcare industry',
           'author': 'Dr. Bob Wilson',
           'category': 'Healthcare',
           'tags': 'AI, healthcare, medical imaging, diagnosis'
       }
   ]

   for article in articles_data:
       client.execute(
           "INSERT INTO articles (title, content, summary, author, category, tags) VALUES (%s, %s, %s, %s, %s, %s)",
           (article['title'], article['content'], article['summary'], 
            article['author'], article['category'], article['tags'])
       )

   print(f"✓ Inserted {len(articles_data)} articles")

   # Natural language search using client interface
   search_results = client.fulltext_index.fulltext_search(
       table_name='articles',
       columns=['title', 'content', 'tags'],
       search_term='machine learning artificial intelligence',
       mode=FulltextModeType.NATURAL_LANGUAGE,
       with_score=True,
       limit=5
   )

   print("Natural Language Search Results:")
   for result in search_results:
       print(f"  Article: {result.get('title', 'N/A')}")
       print(f"    Author: {result.get('author', 'N/A')}")
       print(f"    Category: {result.get('category', 'N/A')}")
       print(f"    Score: {result.get('score', 'N/A')}")
       print(f"    Content: {result.get('content', '')[:100]}...")
       print()

   client.disconnect()

Boolean Mode Search
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client, FulltextModeType

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Boolean search with operators
   print("Boolean Search Examples:")
   print("=" * 50)

   # Search for articles containing 'python' but not 'machine'
   boolean_results1 = client.fulltext_index.fulltext_search(
       table_name='articles',
       columns=['title', 'content', 'tags'],
       search_term='+python -machine',
       mode=FulltextModeType.BOOLEAN,
       with_score=True,
       limit=10
   )

   print("Search: '+python -machine'")
   for result in boolean_results1:
       print(f"  - {result.get('title', 'N/A')} (Score: {result.get('score', 'N/A')})")

   # Search for exact phrase
   boolean_results2 = client.fulltext_index.fulltext_search(
       table_name='articles',
       columns=['title', 'content', 'tags'],
       search_term='"web development"',
       mode=FulltextModeType.BOOLEAN,
       with_score=True,
       limit=10
   )

   print("\nSearch: '\"web development\"' (exact phrase)")
   for result in boolean_results2:
       print(f"  - {result.get('title', 'N/A')} (Score: {result.get('score', 'N/A')})")

   # Complex boolean search
   boolean_results3 = client.fulltext_index.fulltext_search(
       table_name='articles',
       columns=['title', 'content', 'tags'],
       search_term='+(AI OR artificial) +intelligence -healthcare',
       mode=FulltextModeType.BOOLEAN,
       with_score=True,
       limit=10
   )

   print("\nSearch: '+(AI OR artificial) +intelligence -healthcare'")
   for result in boolean_results3:
       print(f"  - {result.get('title', 'N/A')} (Score: {result.get('score', 'N/A')})")

   client.disconnect()

Advanced Fulltext Operations
-----------------------------

Fulltext Search with Filters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy.orm import sessionmaker
   from sqlalchemy import text
   from matrixone import Client

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Get SQLAlchemy engine for complex queries
   engine = client.get_sqlalchemy_engine()
   Session = sessionmaker(bind=engine)
   session = Session()

   try:
       # Fulltext search with category filter
       filtered_results = session.execute(text("""
           SELECT id, title, author, category, tags,
                  MATCH(title, content, tags) AGAINST(:search_term IN NATURAL LANGUAGE MODE) as relevance_score
           FROM articles
           WHERE MATCH(title, content, tags) AGAINST(:search_term IN NATURAL LANGUAGE MODE)
             AND category = :category
           ORDER BY relevance_score DESC
           LIMIT :limit_count
       """), {
           'search_term': 'optimization performance',
           'category': 'Database',
           'limit_count': 5
       })

       print("Filtered Fulltext Search (Database category):")
       for row in filtered_results:
           print(f"  Article: {row.title}")
           print(f"    Author: {row.author}")
           print(f"    Relevance Score: {row.relevance_score:.4f}")
           print(f"    Tags: {row.tags}")

       # Fulltext search with date range and multiple conditions
       complex_results = session.execute(text("""
           SELECT id, title, author, category,
                  MATCH(title, content, tags) AGAINST(:search_term IN NATURAL LANGUAGE MODE) as score
           FROM articles
           WHERE MATCH(title, content, tags) AGAINST(:search_term IN NATURAL LANGUAGE MODE) > :min_score
             AND (category IN (:cat1, :cat2) OR author LIKE :author_pattern)
           ORDER BY score DESC, title ASC
           LIMIT :limit_count
       """), {
           'search_term': 'AI artificial intelligence',
           'min_score': 0.1,
           'cat1': 'Technology',
           'cat2': 'Healthcare',
           'author_pattern': '%Dr.%',
           'limit_count': 10
       })

       print("\nComplex Filtered Search:")
       for row in complex_results:
           print(f"  Article: {row.title}")
           print(f"    Author: {row.author}")
           print(f"    Category: {row.category}")
           print(f"    Score: {row.score:.4f}")

   finally:
       session.close()
       client.disconnect()

Fulltext Search with Aggregation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import text
   from sqlalchemy.orm import sessionmaker
   from matrixone import Client

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   engine = client.get_sqlalchemy_engine()
   Session = sessionmaker(bind=engine)
   session = Session()

   try:
       # Analyze search results by category
       category_analysis = session.execute(text("""
           SELECT category,
                  COUNT(*) as article_count,
                  AVG(MATCH(title, content, tags) AGAINST(:search_term IN NATURAL LANGUAGE MODE)) as avg_relevance,
                  MAX(MATCH(title, content, tags) AGAINST(:search_term IN NATURAL LANGUAGE MODE)) as max_relevance
           FROM articles
           WHERE MATCH(title, content, tags) AGAINST(:search_term IN NATURAL LANGUAGE MODE) > 0
           GROUP BY category
           ORDER BY avg_relevance DESC
       """), {'search_term': 'technology AI machine learning'})

       print("Fulltext Search Analysis by Category:")
       print("-" * 60)
       for row in category_analysis:
           print(f"Category: {row.category}")
           print(f"  Articles: {row.article_count}")
           print(f"  Avg Relevance: {row.avg_relevance:.4f}")
           print(f"  Max Relevance: {row.max_relevance:.4f}")
           print()

       # Top authors by relevance
       author_analysis = session.execute(text("""
           SELECT author,
                  COUNT(*) as relevant_articles,
                  AVG(MATCH(title, content, tags) AGAINST(:search_term IN NATURAL LANGUAGE MODE)) as avg_score
           FROM articles
           WHERE MATCH(title, content, tags) AGAINST(:search_term IN NATURAL LANGUAGE MODE) > :min_score
           GROUP BY author
           HAVING relevant_articles > 0
           ORDER BY avg_score DESC
           LIMIT :limit_count
       """), {
           'search_term': 'artificial intelligence AI',
           'min_score': 0.1,
           'limit_count': 5
       })

       print("Top Authors by Relevance:")
       print("-" * 30)
       for row in author_analysis:
           print(f"{row.author}: {row.relevant_articles} articles (avg score: {row.avg_score:.4f})")

   finally:
       session.close()
       client.disconnect()

JSON Document Search
--------------------

Fulltext Search in JSON Fields
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client, FulltextModeType
   import json

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Create table for JSON documents
   client.execute("""
       CREATE TABLE IF NOT EXISTS json_documents (
           id INT PRIMARY KEY AUTO_INCREMENT,
           title VARCHAR(200),
           json_content JSON,
           description TEXT
       )
   """)

   # Create fulltext index on JSON content and description
   try:
       client.fulltext_index.create(
           table_name='json_documents',
           name='ftidx_json_content',
           columns=['json_content', 'description'],
           algorithm=FulltextAlgorithmType.BM25
       )
   except Exception as e:
       print(f"Index might already exist: {e}")

   # Insert JSON documents
   json_docs = [
       {
           'title': 'Product Catalog',
           'json_content': json.dumps({
               'product': 'Laptop',
               'brand': 'TechCorp',
               'specs': {'processor': 'Intel i7', 'memory': '16GB RAM', 'storage': '512GB SSD'},
               'description': 'High-performance laptop for professionals'
           }),
           'description': 'Latest laptop with advanced specifications'
       },
       {
           'title': 'User Profile',
           'json_content': json.dumps({
               'user': 'john_doe',
               'profile': {'name': 'John Doe', 'skills': ['Python', 'Machine Learning', 'Data Science'], 
                          'experience': '5 years'},
               'bio': 'Data scientist specializing in machine learning algorithms'
           }),
           'description': 'Professional profile of a data scientist'
       },
       {
           'title': 'Research Paper',
           'json_content': json.dumps({
               'title': 'Deep Learning in Natural Language Processing',
               'authors': ['Dr. Smith', 'Prof. Johnson'],
               'abstract': 'This paper explores deep learning techniques for NLP tasks',
               'keywords': ['deep learning', 'NLP', 'neural networks', 'transformers']
           }),
           'description': 'Academic research on deep learning applications'
       }
   ]

   for doc in json_docs:
       client.execute(
           "INSERT INTO json_documents (title, json_content, description) VALUES (%s, %s, %s)",
           (doc['title'], doc['json_content'], doc['description'])
       )

   print(f"✓ Inserted {len(json_docs)} JSON documents")

   # Search in JSON content
   json_search_results = client.fulltext_index.fulltext_search(
       table_name='json_documents',
       columns=['json_content', 'description'],
       search_term='machine learning data science',
       mode=FulltextModeType.NATURAL_LANGUAGE,
       with_score=True,
       limit=5
   )

   print("JSON Fulltext Search Results:")
   for result in json_search_results:
       print(f"  Document: {result.get('title', 'N/A')}")
       print(f"    Score: {result.get('score', 'N/A')}")
       print(f"    Description: {result.get('description', 'N/A')}")
       
       # Parse and display relevant JSON content
       json_content = result.get('json_content')
       if json_content:
           try:
               parsed_json = json.loads(json_content)
               print(f"    JSON Summary: {str(parsed_json)[:100]}...")
           except json.JSONDecodeError:
               print(f"    JSON Content: {str(json_content)[:100]}...")
       print()

   client.disconnect()

Multi-language Search
---------------------

Chinese Text Search
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client, FulltextModeType, FulltextAlgorithmType

   client = Client()
   client.connect(host='localhost', port=6001, user='root', password='111', database='test')

   # Insert Chinese articles
   chinese_articles_data = [
       {
           'title': '人工智能技术发展',
           'content': '人工智能是计算机科学的一个分支，它试图让机器模拟人类的智能行为。机器学习是人工智能的核心技术之一。',
           'author': '张教授'
       },
       {
           'title': '数据库优化技术',
           'content': '数据库优化包括索引优化、查询优化和存储优化等多个方面。良好的数据库设计可以显著提高系统性能。',
           'author': '李工程师'
       },
       {
           'title': 'Python编程语言',
           'content': 'Python是一种高级编程语言，广泛应用于数据科学、机器学习和Web开发等领域。它语法简洁易懂。',
           'author': '王开发者'
       },
       {
           'title': '深度学习研究',
           'content': '深度学习是机器学习的一个子领域，使用多层神经网络来学习数据的复杂模式。在图像识别和自然语言处理方面有重要应用。',
           'author': '陈博士'
       }
   ]

   for article in chinese_articles_data:
       client.execute(
           "INSERT INTO chinese_articles (title, content, author) VALUES (%s, %s, %s)",
           (article['title'], article['content'], article['author'])
       )

   print(f"✓ Inserted {len(chinese_articles_data)} Chinese articles")

   # Search Chinese content
   chinese_search_results = client.fulltext_index.fulltext_search(
       table_name='chinese_articles',
       columns=['title', 'content'],
       search_term='人工智能 机器学习',
       mode=FulltextModeType.NATURAL_LANGUAGE,
       with_score=True,
       limit=5
   )

   print("Chinese Fulltext Search Results:")
   for result in chinese_search_results:
       print(f"  文章标题: {result.get('title', 'N/A')}")
       print(f"    作者: {result.get('author', 'N/A')}")
       print(f"    相关度: {result.get('score', 'N/A')}")
       print(f"    内容: {result.get('content', '')[:50]}...")
       print()

   # Boolean search in Chinese
   chinese_boolean_results = client.fulltext_index.fulltext_search(
       table_name='chinese_articles',
       columns=['title', 'content'],
       search_term='+Python -数据库',
       mode=FulltextModeType.BOOLEAN,
       with_score=True,
       limit=5
   )

   print("Chinese Boolean Search Results (+Python -数据库):")
   for result in chinese_boolean_results:
       print(f"  文章: {result.get('title', 'N/A')} (得分: {result.get('score', 'N/A')})")

   client.disconnect()

Performance and Optimization
-----------------------------

Fulltext Search Performance Testing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import time
   from matrixone import Client, FulltextModeType

   def fulltext_performance_test():
       client = Client()
       client.connect(host='localhost', port=6001, user='root', password='111', database='test')

       # Test different search modes
       search_terms = [
           'artificial intelligence machine learning',
           'database optimization performance',
           'python web development',
           'AI healthcare medical'
       ]

       print("Fulltext Search Performance Test")
       print("=" * 50)

       for search_term in search_terms:
           print(f"\nTesting search: '{search_term}'")
           
           # Natural language mode performance
           start_time = time.time()
           nl_results = client.fulltext_index.fulltext_search(
               table_name='articles',
               columns=['title', 'content', 'tags'],
               search_term=search_term,
               mode=FulltextModeType.NATURAL_LANGUAGE,
               with_score=True,
               limit=10
           )
           nl_time = time.time() - start_time
           
           print(f"  Natural Language: {len(nl_results)} results in {nl_time*1000:.2f}ms")
           
           # Boolean mode performance
           boolean_search = f"+{search_term.split()[0]} +{search_term.split()[1] if len(search_term.split()) > 1 else search_term.split()[0]}"
           start_time = time.time()
           boolean_results = client.fulltext_index.fulltext_search(
               table_name='articles',
               columns=['title', 'content', 'tags'],
               search_term=boolean_search,
               mode=FulltextModeType.BOOLEAN,
               with_score=True,
               limit=10
           )
           boolean_time = time.time() - start_time
           
           print(f"  Boolean Mode: {len(boolean_results)} results in {boolean_time*1000:.2f}ms")

       client.disconnect()

   fulltext_performance_test()

Index Management and Maintenance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client

   def manage_fulltext_indexes():
       client = Client()
       client.connect(host='localhost', port=6001, user='root', password='111', database='test')

       print("Fulltext Index Management")
       print("=" * 30)

       # List existing fulltext indexes
       try:
           result = client.execute("SHOW INDEX FROM articles")
           indexes = result.fetchall()
           
           print("Existing indexes on 'articles' table:")
           for idx in indexes:
               if 'fulltext' in str(idx).lower() or 'ftidx' in str(idx[2]):
                   print(f"  - {idx[2]} on column {idx[4]}")
                   
       except Exception as e:
           print(f"Could not list indexes: {e}")

       # Drop and recreate index with different algorithm
       try:
           client.fulltext_index.drop(
               table_name='articles',
               name='ftidx_articles_content'
           )
           print("✓ Dropped existing fulltext index")
           
           # Recreate with different algorithm
           client.fulltext_index.create(
               table_name='articles',
               name='ftidx_articles_content_v2',
               columns=['title', 'content', 'tags'],
               algorithm=FulltextAlgorithmType.TF_IDF  # Different algorithm
           )
           print("✓ Created new fulltext index with TF-IDF algorithm")
           
       except Exception as e:
           print(f"Index management operation failed: {e}")

       # Test the new index
       try:
           test_results = client.fulltext_index.fulltext_search(
               table_name='articles',
               columns=['title', 'content', 'tags'],
               search_term='machine learning',
               mode=FulltextModeType.NATURAL_LANGUAGE,
               with_score=True,
               limit=3
           )
           print(f"✓ New index working correctly, found {len(test_results)} results")
           
       except Exception as e:
           print(f"New index test failed: {e}")

       client.disconnect()

   manage_fulltext_indexes()

Async Fulltext Operations
--------------------------

Async Fulltext Search
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import asyncio
   from sqlalchemy.ext.declarative import declarative_base
   from sqlalchemy import Column, Integer, String, Text
   from matrixone import AsyncClient, FulltextModeType, FulltextAlgorithmType

   AsyncBase = declarative_base()

   class AsyncArticle(AsyncBase):
       __tablename__ = 'async_articles'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(200), nullable=False)
       content = Column(Text, nullable=False)
       author = Column(String(100))
       tags = Column(String(200))

   async def async_fulltext_operations():
       client = AsyncClient()
       await client.connect(
           host='localhost',
           port=6001,
           user='root',
           password='111',
           database='test'
       )

       # Create table
       await client.create_all(AsyncBase)

       # Enable fulltext indexing
       await client.fulltext_index.enable_fulltext()

       # Create fulltext index
       await client.fulltext_index.create(
           table_name='async_articles',
           name='ftidx_async_content',
           columns=['title', 'content', 'tags'],
           algorithm=FulltextAlgorithmType.BM25
       )

       # Insert sample data with transaction
       articles = [
           ('Async Programming Guide', 'Learn about asynchronous programming patterns', 'Jane Doe', 'async, programming, python'),
           ('Database Concurrency', 'Understanding concurrent database operations', 'John Smith', 'database, concurrency, async'),
           ('Web API Design', 'Best practices for designing RESTful APIs', 'Alice Johnson', 'web, API, REST, design')
       ]

       async with client.transaction() as tx:
           for title, content, author, tags in articles:
               await tx.execute(
                   "INSERT INTO async_articles (title, content, author, tags) VALUES (%s, %s, %s, %s)",
                   (title, content, author, tags)
               )

       print("✓ Inserted async articles")

       # Perform async fulltext search
       search_results = await client.fulltext_index.fulltext_search(
           table_name='async_articles',
           columns=['title', 'content', 'tags'],
           search_term='async programming database',
           mode=FulltextModeType.NATURAL_LANGUAGE,
           with_score=True,
           limit=5
       )

       print("Async Fulltext Search Results:")
       for result in search_results:
           print(f"  Article: {result.get('title', 'N/A')}")
           print(f"    Author: {result.get('author', 'N/A')}")
           print(f"    Score: {result.get('score', 'N/A')}")

       # Clean up
       await client.drop_all(AsyncBase)
       await client.disconnect()

   # Run async example
   asyncio.run(async_fulltext_operations())

Best Practices
--------------

Fulltext Index Design Guidelines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Column Selection:**

* Include all text columns that users will search
* Consider including metadata columns (tags, categories)
* Avoid including columns with primarily numeric content

**Algorithm Selection:**

* **TF-IDF**: Good for general text search, especially with varied document lengths
* **BM25**: Better for short documents and when term frequency saturation is important

**Performance Optimization:**

1. **Index Maintenance**: Regularly update statistics and consider rebuilding indexes for large datasets
2. **Query Optimization**: Use specific search terms rather than very broad queries
3. **Result Limiting**: Always use appropriate LIMIT clauses in production
4. **Caching**: Consider caching frequently used search results

Error Handling and Troubleshooting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.exceptions import QueryError

   def robust_fulltext_operations():
       client = None
       try:
           client = Client()
           client.connect(host='localhost', port=6001, user='root', password='111', database='test')

           # Check if fulltext indexing is available
           try:
               client.fulltext_index.enable_fulltext()
               print("✓ Fulltext indexing is available")
           except QueryError as e:
               if "not supported" in str(e).lower():
                   print("❌ Fulltext indexing not supported in this MatrixOne version")
                   return
               else:
                   raise

           # Create index with error handling
           try:
               client.fulltext_index.create(
                   table_name='articles',
                   name='ftidx_safe_search',
                   columns=['title', 'content'],
                   algorithm=FulltextAlgorithmType.BM25
               )
               print("✓ Fulltext index created successfully")
           except QueryError as e:
               if "already exists" in str(e).lower():
                   print("⚠️  Fulltext index already exists")
               else:
                   print(f"❌ Failed to create fulltext index: {e}")

           # Perform search with error handling
           try:
               results = client.fulltext_index.fulltext_search(
                   table_name='articles',
                   columns=['title', 'content'],
                   search_term='test search query',
                   mode=FulltextModeType.NATURAL_LANGUAGE,
                   with_score=True,
                   limit=5
               )
               print(f"✓ Fulltext search completed, found {len(results)} results")
           except QueryError as e:
               print(f"❌ Fulltext search failed: {e}")

           # Handle empty search results
           if not results:
               print("⚠️  No results found, consider:")
               print("    - Broadening search terms")
               print("    - Checking spelling")
               print("    - Using boolean mode with OR operators")

       except Exception as e:
           print(f"❌ Unexpected error: {e}")
       finally:
           if client:
               client.disconnect()

   robust_fulltext_operations()

Search Quality Tips
~~~~~~~~~~~~~~~~~~~

1. **Query Expansion**: Consider synonyms and related terms
2. **Stemming**: Be aware that some algorithms may handle word variations
3. **Stop Words**: Common words (the, and, or) are typically ignored
4. **Score Interpretation**: Higher scores indicate better relevance
5. **Result Ranking**: Combine fulltext scores with other ranking factors

Next Steps
----------

* Explore :doc:`vector_guide` for combining fulltext with vector search
* Check :doc:`orm_guide` for advanced ORM patterns with fulltext
* Review :doc:`examples` for comprehensive fulltext search examples
* See :doc:`api/fulltext_index` for detailed API documentation
