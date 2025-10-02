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

FulltextModeType
----------------

.. autoclass:: matrixone.sqlalchemy_ext.fulltext_index.FulltextModeType
   :members:
   :undoc-members:
   :show-inheritance:

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
