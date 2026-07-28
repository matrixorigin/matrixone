Fulltext Index Extensions
=========================

The MatrixOne Python SDK provides advanced fulltext indexing and search capabilities through SQLAlchemy extensions.

FulltextIndex
-------------

.. autoclass:: matrixone.sqlalchemy_ext.fulltext_index.FulltextIndex
   :members:
   :undoc-members:
   :show-inheritance:

FulltextSearchBuilder
---------------------

.. autoclass:: matrixone.sqlalchemy_ext.fulltext_index.FulltextSearchBuilder
   :members:
   :undoc-members:
   :show-inheritance:

FulltextAlgorithmType
---------------------

.. autoclass:: matrixone.sqlalchemy_ext.fulltext_index.FulltextAlgorithmType
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

FulltextModeType
----------------

.. autoclass:: matrixone.sqlalchemy_ext.fulltext_index.FulltextModeType
   :members:
   :undoc-members:
   :show-inheritance:

FulltextParserType
------------------

.. autoclass:: matrixone.sqlalchemy_ext.fulltext_index.FulltextParserType
   :members:
   :undoc-members:
   :show-inheritance:
   :no-index:

Usage Examples
--------------

Basic Fulltext Index Creation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import Client
    from matrixone.sqlalchemy_ext import FulltextIndex

    client = Client()
    client.connect(host='localhost', port=6001, user='root', password='111', database='test')

    # Create a fulltext index
    client.fulltext_index.create(
        table_name_or_model='articles',
        name='ftidx_content',
        columns=['title', 'content'],
        algorithm='TF-IDF'
    )

Advanced Search with Builder
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone.sqlalchemy_ext import FulltextSearchBuilder

    # Create a search builder
    builder = FulltextSearchBuilder('articles', ['title', 'content'])
    
    # Build a complex search query
    sql = (builder
           .search('database management')
           .set_mode('natural language')
           .set_with_score(True)
           .where('id > 0')
           .set_order_by('score', 'DESC')
           .limit(10)
           .build_sql())
    
    result = client.execute(sql)

Boolean Mode Search
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Boolean mode search with operators
    builder = FulltextSearchBuilder('articles', ['title', 'content'])
    sql = (builder
           .search('+database +management -mysql')
           .set_mode('boolean')
           .build_sql())
    
    result = client.execute(sql)

Async Operations
~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import AsyncClient

    async_client = AsyncClient()
    await async_client.connect(host='localhost', port=6001, user='root', password='111', database='test')

    # Create fulltext index asynchronously
    await async_client.fulltext_index.create(
        table_name_or_model='articles',
        name='ftidx_async',
        columns=['title', 'content']
    )

    # Search asynchronously
    result = await async_client.execute(sql)

Fulltext Index with JSON Parser
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import Client, FulltextParserType
    from matrixone.orm import declarative_base
    from matrixone.sqlalchemy_ext import FulltextIndex, boolean_match
    from sqlalchemy import Column, BigInteger, Text

    client = Client()
    client.connect(host='localhost', port=6001, user='root', password='111', database='test')
    client.fulltext_index.enable_fulltext()
    
    # Define ORM model with JSON parser
    Base = declarative_base()
    
    class Product(Base):
        __tablename__ = "products"
        id = Column(BigInteger, primary_key=True)
        details = Column(Text)  # Stores JSON as text
        
        # Define fulltext index with JSON parser
        __table_args__ = (
            FulltextIndex("ftidx_details", "details", parser=FulltextParserType.JSON),
        )
    
    # Create table (index is created automatically)
    client.create_table(Product)
    
    # Insert JSON data
    products = [
        {"id": 1, "details": '{"name": "Laptop", "brand": "Dell", "price": 1200}'},
        {"id": 2, "details": '{"name": "Phone", "brand": "Apple", "price": 800}'},
    ]
    client.batch_insert(Product, products)
    
    # Search within JSON content
    result = client.query(Product).filter(
        boolean_match(Product.details).must("Dell")
    ).execute()
    
    for row in result.fetchall():
        print(row.details)

Fulltext Index with NGRAM Parser
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import Client, FulltextParserType
    from matrixone.orm import declarative_base
    from matrixone.sqlalchemy_ext import FulltextIndex, natural_match, boolean_match
    from sqlalchemy import Column, Integer, String, Text

    client = Client()
    client.connect(host='localhost', port=6001, user='root', password='111', database='test')
    client.fulltext_index.enable_fulltext()
    
    # Define ORM model with NGRAM parser for Chinese content
    Base = declarative_base()
    
    class ChineseArticle(Base):
        __tablename__ = "chinese_articles"
        id = Column(Integer, primary_key=True, autoincrement=True)
        title = Column(String(200))
        body = Column(Text)
        
        # Define fulltext index with NGRAM parser for Chinese tokenization
        __table_args__ = (
            FulltextIndex("ftidx_chinese", ["title", "body"], 
                         parser=FulltextParserType.NGRAM),
        )
    
    # Create table (index is created automatically)
    client.create_table(ChineseArticle)
    
    # Insert Chinese content
    articles = [
        {"id": 1, "title": "神雕侠侣 第一回", "body": "越女采莲秋水畔，窄袖轻罗，暗露双金钏"},
        {"id": 2, "title": "神雕侠侣 第二回", "body": "正自发痴，忽听左首屋中传出一人喝道"},
    ]
    client.batch_insert(ChineseArticle, articles)
    
    # Search Chinese content with natural language mode
    result = client.query(ChineseArticle).filter(
        natural_match(ChineseArticle.title, ChineseArticle.body, query="神雕侠侣")
    ).execute()
    
    rows = result.fetchall()
    print(f"Found {len(rows)} articles")
    
    # Boolean mode search
    result = client.query(ChineseArticle).filter(
        boolean_match(ChineseArticle.title, ChineseArticle.body).must("杨过")
    ).execute()

BM25 Algorithm Usage
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import Client, FulltextAlgorithmType
    from matrixone.orm import declarative_base
    from matrixone.sqlalchemy_ext import FulltextIndex, boolean_match
    from sqlalchemy import Column, Integer, String, Text

    client = Client()
    client.connect(host='localhost', port=6001, user='root', password='111', database='test')
    client.fulltext_index.enable_fulltext()
    
    # Set algorithm to BM25 (affects all subsequent fulltext searches)
    client.execute('SET ft_relevancy_algorithm = "BM25"')
    
    # Define ORM model
    Base = declarative_base()
    
    class Article(Base):
        __tablename__ = "articles"
        id = Column(Integer, primary_key=True)
        title = Column(String(200))
        content = Column(Text)
        
        # Create index (algorithm is a runtime setting, not in DDL)
        __table_args__ = (
            FulltextIndex("ftidx_content", ["title", "content"], 
                         algorithm=FulltextAlgorithmType.BM25),
        )
    
    client.create_table(Article)
    
    # Insert data
    articles = [
        {"id": 1, "title": "Python Guide", "content": "Learn Python programming"},
        {"id": 2, "title": "Java Tutorial", "content": "Java programming basics"},
    ]
    client.batch_insert(Article, articles)
    
    # Search using BM25 scoring
    result = client.query(Article).filter(
        boolean_match(Article.title, Article.content).must("programming")
    ).execute()
    
    # Reset to TF-IDF if needed
    client.execute('SET ft_relevancy_algorithm = "TF-IDF"')
