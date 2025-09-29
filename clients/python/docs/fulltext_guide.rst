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

   # Natural language search - automatically handles stopwords, stemming, and relevance scoring
   # This mode is ideal for user queries and general search applications
   result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE)", "machine learning").execute()
   print("Natural language search results:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[4]}")

   # Boolean search - provides precise control over search terms using operators
   # Use OR, AND, NOT, +, -, quotes for phrases, and * for wildcards
   result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN BOOLEAN MODE)", "deep learning OR neural networks").execute()
   print("Boolean search results:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[4]}")

   # Search with relevance scoring - returns a relevance score for ranking results
   # Higher scores indicate better matches; useful for search result ranking
   result = client.query("articles").select("*, MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE) as relevance", "artificial intelligence").order_by("relevance DESC").execute()
   print("Search with relevance scoring:")
   for row in result.fetchall():
       print(f"  {row[1]} (Relevance: {row[-1]:.4f})")

Advanced Fulltext Search
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Multi-column search - searches across multiple text columns simultaneously
   # The columns must match exactly what's defined in your fulltext index
   result = client.query("articles").select("*").where("MATCH(title, content) AGAINST(? IN NATURAL LANGUAGE MODE)", "machine learning").execute()
   print("Multi-column search results:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[4]}")

   # Combined search with SQL filters - combines fulltext search with regular SQL conditions
   # This allows you to filter by metadata while searching text content
   result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE) AND category = ?", "AI", "Technology").execute()
   print("Filtered search results:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[4]}")

   # Paginated search results - useful for large result sets
   # LIMIT controls how many results to return, OFFSET skips the first N results
   result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE)", "learning").limit(2).offset(1).execute()
   print("Paginated search results:")
   for row in result.fetchall():
       print(f"  {row[1]} by {row[4]}")

Boolean Search Operators
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # AND operator - both terms must be present in the document
   # Alternative syntax: use + prefix (e.g., "+machine +learning")
   result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN BOOLEAN MODE)", "machine AND learning").execute()
   print("AND search results:")
   for row in result.fetchall():
       print(f"  {row[1]}")

   # OR operator - at least one of the terms must be present
   # Useful for finding documents about related topics
   result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN BOOLEAN MODE)", "deep OR neural").execute()
   print("OR search results:")
   for row in result.fetchall():
       print(f"  {row[1]}")

   # NOT operator (exclusion) - documents containing the excluded term are filtered out
   # Use minus sign (-) or NOT keyword; excludes documents with "learning"
   result = client.query("articles").select("*").where("MATCH(content) AGAINST(? IN BOOLEAN MODE)", "machine -learning").execute()
   print("NOT search results:")
   for row in result.fetchall():
       print(f"  {row[1]}")

   # Phrase search - exact phrase matching using double quotes
   # Finds documents containing the exact phrase "artificial intelligence"
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
       client.fulltext_index.create("advanced_articles", "idx_content_tags", "content,tags", algorithm="BM25")

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

       # 11. Ordered and limited results - control result presentation
       # Sort by title alphabetically and return only top 2 results
       result = client.query(Article).filter(
           boolean_match(Article.content).must("python")
       ).order_by(Article.title).limit(2).execute()
       print("\nOrdered and limited results:")
       for row in result.fetchall():
           print(f"  {row[1]} - {row[4]}")

       # Clean up
       client.drop_table(Article)
       client.disconnect()

   advanced_orm_fulltext_example()

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

4. **Optimize search queries**:
   - **Use encourage() over must()**: When terms are preferred but not required
   - **Use discourage() for ranking**: Lower unwanted content without filtering it out
   - **Combine with SQL filters**: Mix fulltext search with metadata filtering for better results
   - **Use phrases for exact matches**: Wrap exact phrases in quotes or use .phrase()

5. **Handle errors gracefully**:
   - **Always use try-catch blocks**: Fulltext operations can fail due to index issues
   - **Provide meaningful error messages**: Help users understand what went wrong
   - **Clean up resources properly**: Always disconnect clients and close sessions
   - **Validate query syntax**: Check boolean operators before executing complex queries

6. **Performance optimization**:
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