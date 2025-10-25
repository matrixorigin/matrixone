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

   # Create client and connect (see configuration_guide for connection options)
   client = Client()
   client.connect(host="127.0.0.1", port=6001, user="root", password="111", database="test")

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

Basic Fulltext Indexes
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Create fulltext index using fulltext_index API
   client.fulltext_index.create("articles", name="idx_content", columns="content", algorithm="BM25")
   client.fulltext_index.create("articles", name="idx_title", columns="title", algorithm="TF-IDF")
   client.fulltext_index.create("documents", name="idx_file_content", columns="file_content", algorithm="BM25")

   # Create fulltext index on multiple columns
   client.fulltext_index.create("articles", name="idx_title_content", columns=["title", "content"], algorithm="BM25")

   # Drop a fulltext index
   client.fulltext_index.drop("articles", "idx_title")

Fulltext Indexes with Parsers
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

MatrixOne supports specialized parsers for different content types:

**JSON Parser** - For searching within JSON documents:

.. note::
   **JSON Parser Key Points:**
   
   * The JSON parser indexes **VALUES** in JSON documents, not keys
   * Perfect for searching product specifications, user preferences, configuration data
   * Supports nested JSON structures
   * Works with both JSON columns and TEXT columns containing JSON
   * Can be combined with regular SQL filters for powerful queries

.. code-block:: python

   from matrixone import Client, FulltextParserType
   from matrixone.orm import declarative_base
   from sqlalchemy import Column, BigInteger, Text

   client = Client()
   client.connect(host="127.0.0.1", port=6001, user="root", password="111", database="test")
   
   # Enable fulltext functionality
   client.fulltext_index.enable_fulltext()
   
   # Method 1: Using ORM with JSON parser
   Base = declarative_base()
   
   class Product(Base):
       __tablename__ = "products"
       id = Column(BigInteger, primary_key=True)
       details = Column(Text)  # Stores JSON as text
       
       # Define fulltext index with JSON parser in table definition
       __table_args__ = (
           FulltextIndex("ftidx_details", "details", parser=FulltextParserType.JSON),
       )
   
   # Create table with ORM (index is created automatically)
   client.create_table(Product)
   
   # Insert JSON data
   products = [
       {"id": 1, "details": '{"name": "Laptop", "brand": "Dell", "price": 1200}'},
       {"id": 2, "details": '{"name": "Phone", "brand": "Apple", "price": 800}'},
       {"id": 3, "details": '{"name": "Tablet", "brand": "Samsung", "price": 600}'},
   ]
   client.batch_insert(Product, products)
   
   # Search within JSON content
   result = client.query(Product).filter(
       boolean_match(Product.details).must("Dell")
   ).execute()
   
   for row in result.fetchall():
       print(f"Found: {row.details}")
   
   # Method 2: Create JSON index on existing table
   client.execute("CREATE TABLE json_docs (id INT PRIMARY KEY, data TEXT)")
   client.execute(
       "CREATE FULLTEXT INDEX ftidx_json ON json_docs (data) WITH PARSER json"
   )
   
   # Insert and search JSON data
   client.execute(
       "INSERT INTO json_docs VALUES "
       "(1, '{\"title\": \"Python Tutorial\", \"tags\": [\"python\", \"programming\"]}'), "
       "(2, '{\"title\": \"Java Guide\", \"tags\": [\"java\", \"programming\"]}'))"
   )
   
   result = client.execute(
       "SELECT * FROM json_docs WHERE MATCH(data) AGAINST('python' IN BOOLEAN MODE)"
   )
   for row in result.fetchall():
       print(f"ID: {row[0]}, Data: {row[1]}")

**NGRAM Parser** - For Chinese and other Asian languages:

.. code-block:: python

   from matrixone import Client, FulltextParserType
   from matrixone.orm import declarative_base
   from matrixone.sqlalchemy_ext import FulltextIndex, natural_match, boolean_match
   from sqlalchemy import Column, Integer, String, Text

   client = Client()
   client.connect(host="127.0.0.1", port=6001, user="root", password="111", database="test")
   
   # Enable fulltext functionality
   client.fulltext_index.enable_fulltext()
   
   # Method 1: Using ORM with NGRAM parser for Chinese content
   Base = declarative_base()
   
   class ChineseArticle(Base):
       __tablename__ = "chinese_articles"
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(200))
       body = Column(Text)
       
       # Define fulltext index with NGRAM parser for Chinese tokenization
       __table_args__ = (
           FulltextIndex("ftidx_chinese", ["title", "body"], parser=FulltextParserType.NGRAM),
       )
   
   # Create table with ORM (index is created automatically)
   client.create_table(ChineseArticle)
   
   # Insert Chinese content
   articles = [
       {"id": 1, "title": "神雕侠侣 第一回", "body": "越女采莲秋水畔，窄袖轻罗，暗露双金钏"},
       {"id": 2, "title": "神雕侠侣 第二回", "body": "正自发痴，忽听左首屋中传出一人喝道"},
       {"id": 3, "title": "神雕侠侣 第三回", "body": "郭靖在舟中潜运神功，数日间伤势便已痊愈了大半"},
   ]
   client.batch_insert(ChineseArticle, articles)
   
   # Search Chinese content with natural language mode
   result = client.query(ChineseArticle).filter(
       natural_match(ChineseArticle.title, ChineseArticle.body, query="神雕侠侣")
   ).execute()
   
   print(f"Found {len(result.fetchall())} articles about 神雕侠侣")
   
   # Search with boolean mode
   result = client.query(ChineseArticle).filter(
       boolean_match(ChineseArticle.title, ChineseArticle.body).must("郭靖")
   ).execute()
   
   for row in result.fetchall():
       print(f"Title: {row.title}, Body: {row.body[:20]}...")
   
   # Method 2: Create NGRAM index on existing table
   client.execute(
       "CREATE TABLE chinese_docs ("
       "id INT PRIMARY KEY, "
       "title VARCHAR(200), "
       "content TEXT"
       ")"
   )
   
   client.execute(
       "CREATE FULLTEXT INDEX ftidx_ngram ON chinese_docs (title, content) "
       "WITH PARSER ngram"
   )
   
   # Insert and search Chinese content
   client.execute(
       "INSERT INTO chinese_docs VALUES "
       "(1, 'MO全文索引示例', '这是一个关于MO全文索引的例子'), "
       "(2, 'ngram解析器', 'ngram解析器允许MO对中文进行分词')"
   )
   
   result = client.execute(
       "SELECT * FROM chinese_docs "
       "WHERE MATCH(title, content) AGAINST('全文索引' IN NATURAL LANGUAGE MODE)"
   )
   
   for row in result.fetchall():
       print(f"Title: {row[1]}, Content: {row[2]}")

**Mixed Content (English + Chinese)**:

.. code-block:: python

   from matrixone import Client, FulltextParserType
   from matrixone.orm import declarative_base
   from matrixone.sqlalchemy_ext import FulltextIndex, natural_match
   from sqlalchemy import Column, Integer, String, Text

   client = Client()
   client.connect(host="127.0.0.1", port=6001, user="root", password="111", database="test")
   client.fulltext_index.enable_fulltext()
   
   Base = declarative_base()
   
   class MixedContent(Base):
       __tablename__ = "mixed_articles"
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(255))
       content = Column(Text)
       
       # NGRAM parser works well for mixed English/Chinese content
       __table_args__ = (
           FulltextIndex("ftidx_mixed", ["title", "content"], parser=FulltextParserType.NGRAM),
       )
   
   client.create_table(MixedContent)
   
   # Insert mixed content
   articles = [
       {"id": 1, "title": "MO全文索引示例", "content": "这是关于MO fulltext index的例子"},
       {"id": 2, "title": "Python教程", "content": "Learn Python programming with 中文教程"},
   ]
   client.batch_insert(MixedContent, articles)
   
   # Search for Chinese terms
   result = client.query(MixedContent).filter(
       natural_match(MixedContent.title, MixedContent.content, query="全文索引")
   ).execute()
   
   # Search for English terms
   result = client.query(MixedContent).filter(
       natural_match(MixedContent.title, MixedContent.content, query="Python")
   ).execute()

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

   # Natural language search - automatically handles stopwords, stemming, and relevance scoring
   # This mode is ideal for user queries and general search applications
   result = client.query(
       "articles.id",
       "articles.title", 
       "articles.content",
       "articles.author"
   ).filter(natural_match("content", query="machine learning")).execute()
   print("Natural language search results:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[3]}")

   # Boolean search with phrase matching - provides precise control over search terms
   # Use phrase() for exact phrase matching, encourage() for boosting relevance
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content", 
       "articles.author"
   ).filter(
       boolean_match("content").phrase("deep learning").encourage("networks")
   ).execute()
   print("Boolean search results:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[3]}")

   # Search with relevance scoring - returns a relevance score for ranking results
   # Higher scores indicate better matches; useful for search result ranking
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content",
       "articles.author",
       natural_match("content", query="artificial intelligence").label("relevance")
   ).execute()
   print("Search with relevance scoring:")
   for row in result.fetchall():
       print(f"  {row[1]} (Relevance: {row[4]:.4f})")

   # Simple search without ordering - just get matching results
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content"
   ).filter(boolean_match("content").must("artificial intelligence")).execute()
   print("Simple search results:")
   for row in result.fetchall():
       print(f"  {row[1]}")

   # Using ORM models for fulltext search
   from sqlalchemy import Column, Integer, String, Text
   from matrixone.orm import declarative_base
   
   Base = declarative_base()
   
   class Article(Base):
       __tablename__ = 'articles'
       id = Column(Integer, primary_key=True)
       title = Column(String(200))
       content = Column(Text)
       author = Column(String(100))
       category = Column(String(50))
   
   # Natural language search with model
   result = client.query(Article).filter(
       natural_match(Article.content, query="machine learning")
   ).execute()
   print("Natural language search with model:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[3]}")
   
   # Boolean search with model
   result = client.query(Article).filter(
       boolean_match(Article.content).phrase("deep learning").encourage("networks")
   ).execute()
   print("Boolean search with model:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[3]}")
   
   # Search with scoring using model
   result = client.query(
       Article.id,
       Article.title,
       Article.content,
       Article.author,
       natural_match(Article.content, query="artificial intelligence").label("relevance")
   ).execute()
   print("Search with scoring using model:")
   for row in result.fetchall():
       print(f"  {row[1]} (Relevance: {row[4]:.4f})")

Fulltext Search Modes and Operators
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MatrixOne supports two fulltext search modes with different operators and use cases:

Natural Language Mode
^^^^^^^^^^^^^^^^^^^^^^

**Best for**: User-facing search, Google-style queries, general search applications

**Features**:

* Automatic stopword removal (e.g., "the", "a", "is", "to")
* Automatic word stemming (e.g., "running" → "run")
* Natural relevance scoring (BM25 or TF-IDF)
* No special operators needed
* Perfect for search boxes and user queries

**Usage:**

.. code-block:: python

   from matrixone.sqlalchemy_ext import natural_match
   
   # Simple keyword search
   result = client.query(Article).filter(
       natural_match(Article.content, query="machine learning")
   ).execute()
   
   # Multi-word natural query
   result = client.query(Article).filter(
       natural_match(Article.title, Article.content, 
                    query="how to learn python programming")
   ).execute()
   
   # Question-like queries (stopwords handled automatically)
   result = client.query(Article).filter(
       natural_match(Article.content, query="what is deep learning")
   ).execute()

**Best for**: User queries, search boxes, general search

Boolean Mode
^^^^^^^^^^^^^

**Best for**: Precise control, advanced filters, complex logic

**Boolean Mode Operators:**

1. **MUST** (+ operator, required terms):
   
   .. code-block:: python
   
      from matrixone.sqlalchemy_ext import boolean_match
      
      # Must contain "machine" AND "learning"
      result = client.query(Article).filter(
          boolean_match(Article.content).must("machine", "learning")
      ).execute()
      
      # Must contain "Python"
      result = client.query(Article).filter(
          boolean_match(Article.content).must("Python")
      ).execute()

2. **MUST_NOT** (- operator, excluded terms):
   
   .. code-block:: python
   
      # Contains "programming" but NOT "legacy"
      result = client.query(Article).filter(
          boolean_match(Article.content)
          .must("programming")
          .must_not("legacy")
      ).execute()
      
      # Contains "learning" but NOT "deep"
      result = client.query(Article).filter(
          boolean_match(Article.content)
          .must("learning")
          .must_not("deep")
      ).execute()

3. **ENCOURAGE** (boost relevance, optional terms):
   
   .. code-block:: python
   
      # Must have "Python", boost if has "data" or "science"
      result = client.query(Article).filter(
          boolean_match(Article.content)
          .must("Python")
          .encourage("data", "science")
      ).execute()
      
      # Articles with encouraged terms rank higher

4. **DISCOURAGE** (~ operator, reduce relevance):
   
   .. code-block:: python
   
      # Must have "Python", discourage "legacy" (still matches but ranks lower)
      result = client.query(Article).filter(
          boolean_match(Article.content)
          .must("Python")
          .encourage("modern")
          .discourage("legacy")
      ).execute()

5. **PHRASE** ("" operator, exact phrase matching):
   
   .. code-block:: python
   
      # Exact phrase "neural networks"
      result = client.query(Article).filter(
          boolean_match(Article.content).phrase("neural networks")
      ).execute()
      
      # Exact phrase "best practices"
      result = client.query(Article).filter(
          boolean_match(Article.content).phrase("best practices")
      ).execute()

6. **GROUP** (combine terms with OR logic):
   
   .. code-block:: python
   
      from matrixone.sqlalchemy_ext.fulltext_search import group
      
      # Must contain either "programming" OR "development"
      result = client.query(Article).filter(
          boolean_match(Article.content)
          .must(group().medium("programming", "development"))
      ).execute()

**Complex Boolean Queries:**

.. code-block:: python

   # Complex example: All operators combined
   result = client.query(Article).filter(
       boolean_match(Article.content)
       .must("learning")                              # Required term
       .must(group().medium("machine", "deep"))       # Required: either "machine" OR "deep"
       .encourage("tutorial")                          # Optional: boosts relevance
       .discourage("advanced")                         # Optional: reduces relevance
       .must_not("legacy")                            # Excluded term
   ).execute()
   
   # Practical example: Python articles
   result = client.query(Article).filter(
       boolean_match(Article.title, Article.content)
       .must("Python")                                # Must contain Python
       .encourage("beginner", "tutorial", "guide")    # Prefer beginner content
       .must_not("deprecated", "outdated")            # Exclude old content
   ).execute()

**Quick Comparison:**

==================  =======================  =======================
Feature             Natural Mode             Boolean Mode
==================  =======================  =======================
Query Style         "how to learn python"    must("python")
Processing          Auto (stopwords/stem)    Exact terms
Best For            Search boxes             Advanced filters
==================  =======================  =======================

Advanced Fulltext Search
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Multi-column search - searches across multiple text columns simultaneously
   # The columns must match exactly what's defined in your fulltext index
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content",
       "articles.author"
   ).filter(natural_match("title", "content", query="machine learning")).execute()
   print("Multi-column search results:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[3]}")

   # Combined search with SQL filters - combines fulltext search with regular SQL conditions
   # This allows you to filter by metadata while searching text content
   
   # Method 1: Multiple conditions in single filter()
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content",
       "articles.author"
   ).filter(
       natural_match("content", query="AI"),
       "articles.category = 'Technology'"
   ).execute()
   print("Filtered search results (single filter):")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[3]}")

   # Method 2: Chained filter() calls
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content",
       "articles.author"
   ).filter(boolean_match("content").must("AI")).filter("articles.category = 'Technology'").execute()
   print("Filtered search results (chained filters):")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[3]}")

   # Method 3: Complex filtering with multiple conditions
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content",
       "articles.author",
       "articles.category"
   ).filter(
       boolean_match("content").encourage("programming"),
       "articles.category = 'Programming'",
       "articles.id > 1"
   ).execute()
   print("Complex filtered search results:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[3]} - {row[4]}")

   # Paginated search results - useful for large result sets
   # LIMIT controls how many results to return, OFFSET skips the first N results
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content",
       "articles.author",
       natural_match("content", query="learning").label("relevance")
   ).limit(2).offset(1).execute()
   print("Paginated search results:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[3]} (Score: {row[4]:.4f})")

   # Simple pagination without ordering - just get next N results
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content"
   ).filter(boolean_match("content").must("learning")).limit(2).offset(1).execute()
   print("Simple paginated results:")
   for row in result.fetchall():
       print(f"  {row[1]}")

Combining Fulltext Search with Other Filters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can combine fulltext search with regular SQL filters in several ways:

.. code-block:: python

   # Method 1: Multiple conditions in single filter() call
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content",
       "articles.author",
       "articles.category"
   ).filter(
       boolean_match("content").must("python"),           # Fulltext condition
       "articles.category = 'Programming'",               # SQL condition 1
       "articles.id > 1",                                 # SQL condition 2
       "articles.author LIKE '%Smith%'"                   # SQL condition 3
   ).execute()

   # Method 2: Chained filter() calls (more readable for complex queries)
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content",
       "articles.author",
       "articles.category"
   ).filter(boolean_match("content").encourage("programming"))  # Fulltext condition
    .filter("articles.category = 'Programming'")                # SQL condition 1
    .filter("articles.id > 1")                                  # SQL condition 2
    .filter("articles.author LIKE '%Smith%'")                   # SQL condition 3
    .execute()

   # Method 3: Using ORM model attributes (when available)
   from sqlalchemy import Column, Integer, String, Text
   from matrixone.orm import declarative_base
   
   # Define ORM model
   Base = declarative_base()
   
   class Article(Base):
       __tablename__ = 'articles'
       id = Column(Integer, primary_key=True)
       title = Column(String(200))
       content = Column(Text)
       author = Column(String(100))
       category = Column(String(50))
   
   # Using model class in queries
   result = client.query(Article).filter(
       boolean_match(Article.content).must("python"),
       Article.category == "Programming",
       Article.id > 1,
       Article.author.like("%Smith%")
   ).execute()
   
   # Using model with natural_match
   result = client.query(Article).filter(
       natural_match(Article.title, Article.content, query="machine learning")
   ).execute()
   
   # Using model with scoring
   result = client.query(
       Article.id,
       Article.title,
       Article.content,
       boolean_match(Article.content).encourage("python").label("score")
   ).execute()

   # Method 4: Complex filtering with IN, BETWEEN, and other operators
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content",
       "articles.category",
       "articles.tags"
   ).filter(
       natural_match("title", "content", query="machine learning"),
       "articles.category IN ('AI', 'Technology', 'Programming')",
       "articles.id BETWEEN 1 AND 10",
       "articles.tags LIKE '%tutorial%'",
       "articles.author IS NOT NULL"
   ).execute()

   # Method 5: Combining with scoring
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content",
       "articles.category",
       boolean_match("title", "content").encourage("python").label("score")
   ).filter(
       "articles.category = 'Programming'",
       "articles.id > 1"
   ).limit(5).execute()

   # Method 6: Using SQLAlchemy and_/or_ for complex conditions
   from sqlalchemy import and_, or_
   
   # Logical AND: Combine fulltext search with category filter
   fulltext_condition = boolean_match("title", "content").must("python")
   category_condition = "articles.category = 'Programming'"
   
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content",
       "articles.category"
   ).filter(and_(fulltext_condition, category_condition)).execute()
   
   # Logical OR: Combine different category conditions
   programming_condition = "articles.category = 'Programming'"
   ai_condition = "articles.category = 'AI'"
   
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content",
       "articles.category"
   ).filter(or_(programming_condition, ai_condition)).execute()
   
   # Filter by multiple values using SQL IN
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content",
       "articles.category",
       "articles.author"
   ).filter("articles.category IN ('Programming', 'AI', 'Technology')").execute()
   
   # SQL IN with fulltext search
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content",
       "articles.author"
   ).filter(
       boolean_match("title", "content").encourage("python"),
       "articles.author IN ('John Doe', 'Jane Smith', 'Bob Wilson')"
   ).execute()
   
   # Complex nested logical conditions
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content",
       "articles.category",
       "articles.author"
   ).filter(
       and_(
           boolean_match("title", "content").encourage("programming"),
           or_(
               "articles.category IN ('Programming', 'AI')",
               "articles.category = 'Technology'"
           ),
           "articles.author IN ('John Doe', 'Jane Smith')",
           "articles.id > 1"
       )
   ).execute()

Boolean Search Operators
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # AND operator - both terms must be present in the document
   # Use must() for required terms (AND logic)
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content"
   ).filter(boolean_match("content").must("machine", "learning")).execute()
   print("AND search results:")
   for row in result.fetchall():
       print(f"  {row[1]}")

   # OR operator - at least one of the terms must be present
   # Use group().medium() for OR logic within required conditions
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content"
   ).filter(boolean_match("content").must(group().medium("deep", "neural"))).execute()
   print("OR search results:")
   for row in result.fetchall():
       print(f"  {row[1]}")

   # NOT operator (exclusion) - documents containing the excluded term are filtered out
   # Use must_not() to exclude documents with specific terms
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content"
   ).filter(boolean_match("content").must("machine").must_not("learning")).execute()
   print("NOT search results:")
   for row in result.fetchall():
       print(f"  {row[1]}")

   # Phrase search - exact phrase matching
   # Use phrase() for exact phrase matching
   result = client.query(
       "articles.id",
       "articles.title",
       "articles.content"
   ).filter(boolean_match("content").phrase("artificial intelligence")).execute()
   print("Phrase search results:")
   for row in result.fetchall():
       print(f"  {row[1]}")

   # Using ORM models with boolean search operators
   from sqlalchemy import Column, Integer, String, Text
   from matrixone.orm import declarative_base
   
   Base = declarative_base()
   
   class Article(Base):
       __tablename__ = 'articles'
       id = Column(Integer, primary_key=True)
       title = Column(String(200))
       content = Column(Text)
       author = Column(String(100))
       category = Column(String(50))
   
   # AND operator with model
   result = client.query(Article).filter(
       boolean_match(Article.content).must("machine", "learning")
   ).execute()
   print("AND search with model:")
   for row in result.fetchall():
       print(f"  {row[1]}")
   
   # OR operator with model
   result = client.query(Article).filter(
       boolean_match(Article.content).must(group().medium("deep", "neural"))
   ).execute()
   print("OR search with model:")
   for row in result.fetchall():
       print(f"  {row[1]}")
   
   # NOT operator with model
   result = client.query(Article).filter(
       boolean_match(Article.content).must("machine").must_not("learning")
   ).execute()
   print("NOT search with model:")
   for row in result.fetchall():
       print(f"  {row[1]}")
   
   # Phrase search with model
   result = client.query(Article).filter(
       boolean_match(Article.content).phrase("artificial intelligence")
   ).execute()
   print("Phrase search with model:")
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
       await client.fulltext_index.create("async_articles", name="idx_content", columns="content", algorithm="BM25")

       # Insert data using async insert API
       await client.insert("async_articles", {
           "id": 1,
           "title": "Async Article",
           "content": "This is an article created using async operations for fulltext search testing.",
           "author": "Async Author"
       })

       # Fulltext search using async query API
   result = await client.query(
       "async_articles.id",
       "async_articles.title",
       "async_articles.content",
       "async_articles.author"
   ).filter(natural_match("content", query="async operations")).execute()
       print("Async fulltext search results:")
       for row in result.fetchall():
           print(f"  {row[1]} by {row[3]}")

   # Using ORM models with async fulltext search
   from sqlalchemy import Column, Integer, String, Text
   from matrixone.orm import declarative_base
   
   Base = declarative_base()
   
   class AsyncArticle(Base):
       __tablename__ = 'async_articles'
       id = Column(Integer, primary_key=True)
       title = Column(String(200))
       content = Column(Text)
       author = Column(String(100))
       category = Column(String(50))
   
   # Async search with model using boolean_match
   result = await client.query(AsyncArticle).filter(
       boolean_match(AsyncArticle.content).must("async")
   ).execute()
   print("Async search with model (boolean_match):")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[3]}")
   
   # Async search with model using natural_match
   result = await client.query(AsyncArticle).filter(
       natural_match(AsyncArticle.title, AsyncArticle.content, query="async operations")
   ).execute()
   print("Async search with model (natural_match):")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[3]}")
   
   # Async search with model and scoring
   result = await client.query(
       AsyncArticle.id,
       AsyncArticle.title,
       AsyncArticle.content,
       boolean_match(AsyncArticle.content).encourage("async").label("score")
   ).execute()
   print("Async search with model and scoring:")
   for row in result.fetchall():
       print(f"  {row[1]} (Score: {row[3]:.4f})")

       # Clean up
       await client.drop_table("async_articles")
       await client.disconnect()

   asyncio.run(async_fulltext_example())

ORM with Fulltext Search
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Column, Integer, String, Text
   from matrixone.orm import declarative_base
   from sqlalchemy.orm import sessionmaker
   from matrixone import Client
   from matrixone.config import get_connection_params
   from matrixone.sqlalchemy_ext.fulltext_search import boolean_match, natural_match

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
       client.fulltext_index.create("orm_articles", name="idx_content", columns="content", algorithm="BM25")

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

       # Natural language search - automatically processes query for optimal results
       # Handles synonyms, stemming, and stopword removal automatically
       result = client.query(Article).filter(natural_match(Article.content, "fulltext search")).execute()
       print("Natural language search results:")
       for row in result.fetchall():
           print(f"  {row[1]} by {row[3]}")
       
       # Boolean search with must conditions - both terms are required
       # Chain multiple must() calls for AND logic; all terms must be present
       result = client.query(Article).filter(boolean_match(Article.content).must("fulltext").must("search")).execute()
       print("Boolean search results:")
       for row in result.fetchall():
           print(f"  {row[1]} by {row[3]}")
       
       # Complex boolean search with multiple operators
       # must() = required, encourage() = preferred but optional, must_not() = excluded
       result = client.query(Article).filter(
           boolean_match(Article.content)
           .must("fulltext")           # Required: must contain "fulltext"
           .encourage("search")        # Preferred: boost relevance if present
           .must_not("legacy")         # Excluded: filter out documents with "legacy"
       ).execute()
       print("Complex boolean search results:")
       for row in result.fetchall():
           print(f"  {row[1]} by {row[3]}")

       # Clean up
       client.drop_table(Article)
       session.close()
       client.disconnect()

   orm_fulltext_example()

Advanced ORM-Style Fulltext Queries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Modern ORM-style fulltext queries with boolean_match and natural_match:

.. code-block:: python

   from sqlalchemy import Column, Integer, String, Text
   from matrixone.orm import declarative_base
   from matrixone import Client
   from matrixone.config import get_connection_params
   from matrixone.sqlalchemy_ext.fulltext_search import boolean_match, natural_match, group

   # Define ORM models
   Base = declarative_base()

   class Article(Base):
       __tablename__ = 'advanced_articles'
       
       id = Column(Integer, primary_key=True, autoincrement=True)
       title = Column(String(200), nullable=False)
       content = Column(Text, nullable=False)
       tags = Column(String(500))
       category = Column(String(50))

   def advanced_orm_fulltext_example():
       host, port, user, password, database = get_connection_params()
       client = Client()
       client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create table using ORM model
       client.create_table(Article)

       # Create fulltext index
       client.fulltext_index.create("advanced_articles", name="idx_content_tags", columns=["content", "tags"], algorithm="BM25")

       # Insert test data
       articles = [
           {"title": "Python Programming Guide", "content": "Learn Python programming from basics to advanced concepts.", "tags": "python,programming,tutorial", "category": "Programming"},
           {"title": "Machine Learning with Python", "content": "Introduction to machine learning using Python and scikit-learn.", "tags": "python,machine-learning,AI", "category": "AI"},
           {"title": "Web Development Tutorial", "content": "Build modern web applications with Python and Django framework.", "tags": "python,web,django", "category": "Web"}
       ]
       client.batch_insert(Article, articles)

       # 1. Natural language search - user-friendly, handles variations automatically
       # Best for end-user search interfaces; processes "python programming" intelligently
       result = client.query(Article).filter(natural_match(Article.content, "python programming")).execute()
       print("Natural language search results:")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 2. Basic boolean search - exact term matching with required conditions
       # Must contain "python" - strict matching without stemming or variations
       result = client.query(Article).filter(boolean_match(Article.content).must("python")).execute()
       print("\nBoolean search - must contain 'python':")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 3. Boolean search with exclusion - filter out unwanted results
       # Required: "python", Excluded: "django" - finds Python articles without Django
       result = client.query(Article).filter(
           boolean_match(Article.content).must("python").must_not("django")
       ).execute()
       print("\nBoolean search - must have 'python', must not have 'django':")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 4. Boolean search with preference - boost relevance without filtering
       # Required: "python", Preferred: "tutorial" - boosts tutorial results in ranking
       result = client.query(Article).filter(
           boolean_match(Article.content).must("python").encourage("tutorial")
       ).execute()
       print("\nBoolean search - must have 'python', encourage 'tutorial':")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 5. Boolean search with discouragement - lower ranking for certain terms
       # Required: "python", Discouraged: "legacy" - lowers ranking of legacy content
       result = client.query(Article).filter(
           boolean_match(Article.content).must("python").discourage("legacy")
       ).execute()
       print("\nBoolean search - must have 'python', discourage 'legacy':")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 6. Group search - logical OR within required conditions
       # Must contain either "programming" OR "machine" - flexible matching
       result = client.query(Article).filter(
           boolean_match(Article.content).must(group().medium("programming", "machine"))
       ).execute()
       print("\nGroup search - must contain either 'programming' or 'machine':")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 7. Phrase search - exact phrase matching
       # Finds documents containing the exact phrase "machine learning"
       result = client.query(Article).filter(
           boolean_match(Article.content).phrase("machine learning")
       ).execute()
       print("\nPhrase search - exact phrase 'machine learning':")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 8. Prefix search - wildcard matching for word beginnings
       # Finds words starting with "python" (e.g., "pythonic", "pythonista")
       result = client.query(Article).filter(
           boolean_match(Article.content).prefix("python")
       ).execute()
       print("\nPrefix search - words starting with 'python':")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 9. Complex boolean search - combining multiple operators for sophisticated queries
       # Required: "python" AND (either "programming" OR "machine")
       # Preferred: "tutorial", Discouraged: "legacy" - advanced ranking control
       result = client.query(Article).filter(
           boolean_match(Article.content)
           .must("python")                                    # Must contain "python"
           .must(group().medium("programming", "machine"))    # Must contain either term
           .encourage("tutorial")                             # Boost tutorial content
           .discourage("legacy")                              # Lower legacy content ranking
       ).execute()
       print("\nComplex boolean search:")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 10. Combined fulltext and SQL filters - mix fulltext search with metadata filtering
       # Fulltext search for content + SQL filter for category metadata
       result = client.query(Article).filter(
           boolean_match(Article.content).must("python")      # Fulltext search
       ).filter(
           Article.category == "Programming"                  # SQL filter
       ).execute()
       print("\nCombined with regular filters:")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 11. Limited results - control result presentation
       # Return only top 2 results
       result = client.query(Article).filter(
           boolean_match(Article.content).must("python")
       ).limit(2).execute()
       print("\nLimited results:")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # Clean up
       client.drop_table(Article)
       client.disconnect()

   advanced_orm_fulltext_example()

Complete ORM-Style Fulltext Search Examples
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here are comprehensive examples showing all available operators in action:

.. code-block:: python

   from matrixone import Client
   from matrixone.sqlalchemy_ext.fulltext_search import boolean_match, natural_match, group
   from matrixone.config import get_connection_params

   def complete_fulltext_examples():
       host, port, user, password, database = get_connection_params()
       client = Client()
       client.connect(host=host, port=port, user=user, password=password, database=database)

       # Create table and index
       client.create_table("complete_articles", {
           "id": "int",
           "title": "varchar(200)",
           "content": "text",
           "tags": "varchar(500)",
           "category": "varchar(50)"
       }, primary_key="id")
       
       client.fulltext_index.create("complete_articles", name="idx_complete", columns=["title", "content", "tags"], algorithm="BM25")

       # Insert test data
       articles = [
           {"id": 1, "title": "Python Programming Guide", "content": "Learn Python programming from basics to advanced concepts.", "tags": "python,programming,tutorial", "category": "Programming"},
           {"id": 2, "title": "Machine Learning with Python", "content": "Introduction to machine learning using Python and scikit-learn.", "tags": "python,machine-learning,AI", "category": "AI"},
           {"id": 3, "title": "Web Development Tutorial", "content": "Build modern web applications with Python and Django framework.", "tags": "python,web,django", "category": "Web"},
           {"id": 4, "title": "Legacy Python Code", "content": "This is deprecated Python code that should be avoided.", "tags": "python,legacy,deprecated", "category": "Legacy"}
       ]
       client.batch_insert("complete_articles", articles)

       # 1. Natural language search with relevance scoring
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content",
           natural_match("title", "content", query="python programming").label("relevance")
       ).execute()
       print("Natural language search with scoring:")
       for row in result.fetchall():
           print(f"  {row[1]} (Score: {row[3]:.4f})")

       # 1b. Natural language search without ordering (simpler)
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content"
       ).filter(natural_match("title", "content", query="python programming")).execute()
       print("Natural language search (simple):")
       for row in result.fetchall():
           print(f"  {row[1]}")

       # 2. Boolean search with must conditions (AND logic)
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content"
       ).filter(boolean_match("title", "content").must("python", "programming")).execute()
       print("\nBoolean search - must contain 'python' AND 'programming':")
       for row in result.fetchall():
           print(f"  {row[1]}")

       # 3. Boolean search with exclusion (NOT logic)
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content"
       ).filter(boolean_match("title", "content").must("python").must_not("legacy")).execute()
       print("\nBoolean search - must have 'python', must not have 'legacy':")
       for row in result.fetchall():
           print(f"  {row[1]}")

       # 4. Boolean search with preference (encourage)
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content",
           boolean_match("title", "content").must("python").encourage("tutorial").label("score")
       ).execute()
       print("\nBoolean search - must have 'python', encourage 'tutorial':")
       for row in result.fetchall():
           print(f"  {row[1]} (Score: {row[3]:.4f})")

       # 5. Boolean search with discouragement
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content",
           boolean_match("title", "content").must("python").discourage("legacy").label("score")
       ).execute()
       print("\nBoolean search - must have 'python', discourage 'legacy':")
       for row in result.fetchall():
           print(f"  {row[1]} (Score: {row[3]:.4f})")

       # 6. Group search with OR logic
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content"
       ).filter(boolean_match("title", "content").must(group().medium("programming", "machine"))).execute()
       print("\nGroup search - must contain either 'programming' OR 'machine':")
       for row in result.fetchall():
           print(f"  {row[1]}")

       # 7. Weighted group search
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content",
           boolean_match("title", "content").encourage(group().high("tutorial").low("basic")).label("score")
       ).execute()
       print("\nWeighted group search - prefer 'tutorial' over 'basic':")
       for row in result.fetchall():
           print(f"  {row[1]} (Score: {row[3]:.4f})")

       # 8. Phrase search
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content"
       ).filter(boolean_match("title", "content").phrase("machine learning")).execute()
       print("\nPhrase search - exact phrase 'machine learning':")
       for row in result.fetchall():
           print(f"  {row[1]}")

       # 9. Prefix search
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content"
       ).filter(boolean_match("title", "content").prefix("python")).execute()
       print("\nPrefix search - words starting with 'python':")
       for row in result.fetchall():
           print(f"  {row[1]}")

       # 10. Complex boolean search combining multiple operators
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content",
           boolean_match("title", "content")
           .must("python")                                    # Must contain "python"
           .must(group().medium("programming", "machine"))    # Must contain either term
           .encourage("tutorial")                             # Boost tutorial content
           .discourage("legacy")                              # Lower legacy content ranking
           .label("complex_score")
       ).execute()
       print("\nComplex boolean search:")
       for row in result.fetchall():
           print(f"  {row[1]} (Score: {row[3]:.4f})")

       # 11. Combined fulltext and SQL filters (single filter with multiple conditions)
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content",
           "complete_articles.category"
       ).filter(
           boolean_match("title", "content").must("python"),  # Fulltext search
           "complete_articles.category = 'Programming'"        # SQL filter
       ).execute()
       print("\nCombined with regular filters (single filter):")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[3]}")

       # 11b. Chained filter calls
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content",
           "complete_articles.category"
       ).filter(boolean_match("title", "content").encourage("programming")).filter("complete_articles.category = 'Programming'").execute()
       print("\nCombined with regular filters (chained):")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[3]}")

       # 11c. Complex filtering with multiple SQL conditions
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content",
           "complete_articles.category",
           "complete_articles.tags"
       ).filter(
           boolean_match("title", "content").must("python"),
           "complete_articles.category = 'Programming'",
           "complete_articles.id > 1",
           "complete_articles.tags LIKE '%tutorial%'"
       ).execute()
       print("\nComplex filtering with multiple conditions:")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[3]} - {row[4]}")

       # 11d. Filtering with IN conditions
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content",
           "complete_articles.category"
       ).filter(
           boolean_match("title", "content").encourage("python"),
           "complete_articles.category IN ('Programming', 'AI')"
       ).execute()
       print("\nFiltering with IN conditions:")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[3]}")

       # 11e. Filtering with range conditions
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content"
       ).filter(
           boolean_match("title", "content").must("python"),
           "complete_articles.id BETWEEN 1 AND 3"
       ).execute()
       print("\nFiltering with range conditions:")
       for row in result.fetchall():
           print(f"  {row[1]}")

       # 11f. Using SQL IN for multiple value filtering
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content",
           "complete_articles.category"
       ).filter(
           boolean_match("title", "content").encourage("python"),
           "complete_articles.category IN ('Programming', 'AI', 'Technology')"
       ).execute()
       print("\nFiltering with SQL IN:")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[3]}")

       # 11g. Complex logical conditions with and_/or_
       from sqlalchemy import and_, or_
       
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content",
           "complete_articles.category",
           "complete_articles.tags"
       ).filter(
           and_(
               boolean_match("title", "content").must("python"),
               or_(
                   "complete_articles.category IN ('Programming', 'AI')",
                   "complete_articles.category = 'Technology'"
               ),
               "complete_articles.id > 1"
           )
       ).execute()
       print("\nComplex logical conditions:")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[3]}")

       # 11h. Using ORM models with fulltext search
       from sqlalchemy import Column, Integer, String, Text
       from matrixone.orm import declarative_base
       
       # Define ORM model
       Base = declarative_base()
       
       class ArticleModel(Base):
           __tablename__ = 'complete_articles'
           id = Column(Integer, primary_key=True)
           title = Column(String(200))
           content = Column(Text)
           tags = Column(String(500))
           category = Column(String(50))
       
       # Using model with boolean_match
       result = client.query(ArticleModel).filter(
           boolean_match(ArticleModel.content).must("python")
       ).execute()
       print("\nUsing ORM model with boolean_match:")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")
       
       # Using model with natural_match
       result = client.query(ArticleModel).filter(
           natural_match(ArticleModel.title, ArticleModel.content, query="machine learning")
       ).execute()
       print("\nUsing ORM model with natural_match:")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")
       
       # Using model with scoring and ordering
       result = client.query(
           ArticleModel.id,
           ArticleModel.title,
           ArticleModel.content,
           boolean_match(ArticleModel.content).encourage("python").label("score")
       ).execute()
       print("\nUsing ORM model with scoring:")
       for row in result.fetchall():
           print(f"  {row[1]} (Score: {row[3]:.4f})")
       
       # Using model with SQLAlchemy and_
       from sqlalchemy import and_
       
       result = client.query(ArticleModel).filter(
           and_(
               boolean_match(ArticleModel.content).must("python"),
               ArticleModel.category.in_(["Programming", "AI"]),
               ArticleModel.id > 1
           )
       ).execute()
       print("\nUsing ORM model with and_:")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # 12. Limited results
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content"
       ).filter(boolean_match("title", "content").must("python")).limit(2).execute()
       print("\nLimited results:")
       for row in result.fetchall():
           print(f"  {row[1]}")

       # 12b. Simple limited results without ordering
       result = client.query(
           "complete_articles.id",
           "complete_articles.title",
           "complete_articles.content"
       ).filter(boolean_match("title", "content").must("python")).limit(2).execute()
       print("\nSimple limited results:")
       for row in result.fetchall():
           print(f"  {row[1]}")

       # Clean up
       client.drop_table("complete_articles")
       client.disconnect()

   complete_fulltext_examples()

Boolean Match Operators Reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `boolean_match` function provides powerful operators for precise fulltext search control:

**Core Operators:**

* **`.must(term)`** - Required term (AND logic)
  - Document must contain this term
  - Chain multiple `.must()` calls for AND conditions
  - Example: `.must("python").must("programming")` = "python AND programming"

* **`.must_not(term)`** - Excluded term (NOT logic)
  - Document must NOT contain this term
  - Filters out unwanted results
  - Example: `.must("python").must_not("legacy")` = "python NOT legacy"

* **`.encourage(term)`** - Preferred term (positive weight)
  - Boosts relevance score if term is present
  - Does not filter results if term is absent
  - Example: `.must("python").encourage("tutorial")` = "python, prefer tutorial"

* **`.discourage(term)`** - Discouraged term (negative weight)
  - Lowers relevance score if term is present
  - Does not filter results if term is absent
  - Example: `.must("python").discourage("legacy")` = "python, avoid legacy"

**Group Operators:**

* **`.must(group().medium(term1, term2))`** - Required group (OR logic)
  - Document must contain at least one term from the group
  - Example: `.must(group().medium("python", "java"))` = "python OR java"

* **`.encourage(group().high(term1).low(term2))`** - Weighted group
  - `.high()` gives higher weight, `.low()` gives lower weight
  - Example: `.encourage(group().high("tutorial").low("basic"))` = "prefer tutorial over basic"

**Special Operators:**

* **`.phrase("exact phrase")`** - Exact phrase matching
  - Finds documents containing the exact phrase
  - Example: `.phrase("machine learning")` = exact phrase match

* **`.prefix("prefix")`** - Prefix/wildcard matching
  - Finds words starting with the prefix
  - Example: `.prefix("python")` = matches "python", "pythonic", "pythonista"

**Usage Patterns:**

.. code-block:: python

   # Basic required search
   boolean_match(Article.content).must("python")
   
   # Multiple requirements (AND)
   boolean_match(Article.content).must("python").must("programming")
   
   # Required with exclusion (AND NOT)
   boolean_match(Article.content).must("python").must_not("legacy")
   
   # Required with preference (AND, prefer X)
   boolean_match(Article.content).must("python").encourage("tutorial")
   
   # Required with discouragement (AND, avoid X)
   boolean_match(Article.content).must("python").discourage("deprecated")
   
   # Group requirements (AND (OR))
   boolean_match(Article.content).must(group().medium("python", "java"))
   
   # Complex combination
   boolean_match(Article.content)
       .must("programming")
       .must(group().medium("python", "java"))
       .encourage("tutorial")
       .discourage("legacy")
       .phrase("best practices")

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
               client.fulltext_index.create("error_articles", name="idx_content", columns="content", algorithm="BM25")
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
               result = client.query(
                   "error_articles.id",
                   "error_articles.content"
               ).filter(natural_match("content", query="test content")).execute()
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
   - **BM25**: Best for general fulltext search, handles modern document collections well
   - **TF-IDF**: Good for specific use cases, traditional approach with proven reliability
   - **Recommendation**: Start with BM25 for new applications

2. **Optimize index creation**:
   - **Create indexes after data insertion**: Avoid rebuilding indexes during data loading
   - **Use appropriate column types**: TEXT for large content, VARCHAR for shorter text
   - **Match index columns exactly**: Columns in MATCH() must exactly match fulltext index definition
   - **Consider multi-column indexes**: Index related text columns together for better performance

3. **Use appropriate search modes**:
   - **Natural language mode**: Best for user-facing search interfaces, handles variations automatically
   - **Boolean mode**: Best for programmatic queries, provides precise control over search terms
   - **ORM boolean_match**: Use for type-safe, chainable queries with modern syntax

4. **Pagination**:
   - **With scoring**: Use `.label("score")` to get relevance scores for ranking results
   - **Without scoring**: Skip scoring for simple searches where ranking isn't important
   - **Pagination**: Use `.limit()` and `.offset()` for pagination (ordering is optional)
   - **Performance**: Scoring may be slower than simple searches, but provides better relevance

5. **Optimize search queries**:
   - **Use encourage() over must()**: When terms are preferred but not required
   - **Use discourage() for ranking**: Lower unwanted content without filtering it out
   - **Combine with SQL filters**: Mix fulltext search with metadata filtering for better results
   - **Use phrases for exact matches**: Wrap exact phrases in quotes or use .phrase()

6. **Filter combination strategies**:
   - **Single filter() with multiple conditions**: More efficient for simple combinations
   - **Chained filter() calls**: Better readability for complex queries
   - **Use appropriate operators**: IN, BETWEEN, LIKE, IS NULL for different filtering needs
   - **Combine with scoring**: Use .label() for ranked results
   - **Performance consideration**: More filters = more precise results but potentially slower queries

7. **SQLAlchemy logical operators for complex conditions**:
   - **and_()**: Combine multiple conditions with AND logic (from sqlalchemy)
   - **or_()**: Combine multiple conditions with OR logic (from sqlalchemy)
   - **not_()**: Negate conditions (from sqlalchemy)
   - **SQL IN**: Filter by multiple values using SQL IN clause
   - **Nested combinations**: Use SQLAlchemy operators for complex nested conditions
   - **Fulltext + logical operators**: Combine fulltext search with SQLAlchemy expressions
   - **Performance**: Logical operators provide more control but may impact query performance

8. **Handle errors gracefully**:
   - **Always use try-catch blocks**: Fulltext operations can fail due to index issues
   - **Provide meaningful error messages**: Help users understand what went wrong
   - **Clean up resources properly**: Always disconnect clients and close sessions
   - **Validate query syntax**: Check boolean operators before executing complex queries

9. **Performance optimization**:
   - **Use batch operations**: Insert large datasets with batch_insert() instead of individual inserts
   - **Create indexes strategically**: Only index columns that will be searched
   - **Limit result sets**: Use LIMIT and OFFSET for pagination with large result sets
   - **Monitor index usage**: Regularly check which indexes are being used effectively

Next Steps
----------

* Read the :doc:`api/fulltext_index` for detailed fulltext index API
* Check out the :doc:`api/fulltext_search` for fulltext search API
* Explore :doc:`vector_guide` for vector search capabilities
* Learn about :doc:`orm_guide` for ORM patterns with fulltext search
* Check out the :doc:`examples` for comprehensive usage examples