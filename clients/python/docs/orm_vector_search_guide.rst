ORM Vector Search Guide
========================

This guide demonstrates how to convert raw SQL vector queries to elegant ORM-style queries in MatrixOne.

Original SQL Query
------------------

The following SQL query performs vector similarity search with filtering:

.. code-block:: sql

    SELECT id, title, content,
           l2_distance(embedding, :query_vector) as distance
    FROM documents
    WHERE l2_distance(embedding, :query_vector) < :max_distance
      AND title LIKE :title_pattern
    ORDER BY distance ASC
    LIMIT :limit_count

Raw SQL Implementation
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from sqlalchemy import text
    
    result = session.execute(text("""
        SELECT id, title, content,
               l2_distance(embedding, :query_vector) as distance
        FROM documents
        WHERE l2_distance(embedding, :query_vector) < :max_distance
          AND title LIKE :title_pattern
        ORDER BY distance ASC
        LIMIT :limit_count
    """), {
        'query_vector': query_vector,
        'max_distance': 1.0,
        'title_pattern': '%AI%',
        'limit_count': 10
    })

ORM Implementation
------------------

Basic ORM Approach
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from sqlalchemy.orm import sessionmaker
    from matrixone.sqlalchemy_ext import create_vector_column
    from sqlalchemy import Column, Integer, String, Text
    from sqlalchemy.ext.declarative import declarative_base
    
    Base = declarative_base()
    
    class Document(Base):
        __tablename__ = 'documents'
        
        id = Column(Integer, primary_key=True, autoincrement=True)
        title = Column(String(200), nullable=False)
        content = Column(Text)
        embedding = create_vector_column(384, "f32")
    
    # Basic ORM query
    results = session.query(
        Document.id,
        Document.title,
        Document.content,
        Document.embedding.l2_distance(query_vector).label('distance')
    ).filter(
        Document.embedding.l2_distance(query_vector) < 1.0
    ).filter(
        Document.title.like('%AI%')
    ).order_by(
        Document.embedding.l2_distance(query_vector)
    ).limit(10).all()

Enhanced ORM Approach
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Using enhanced VectorColumn methods
    results = session.query(
        Document.id,
        Document.title,
        Document.content,
        Document.embedding.l2_distance(query_vector).label('distance')
    ).filter(
        Document.embedding.within_distance(query_vector, max_distance=1.0)
    ).filter(
        Document.title.like('%AI%')
    ).order_by(
        Document.embedding.most_similar(query_vector)
    ).limit(10).all()

Functional API Approach
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone.sqlalchemy_ext import l2_distance, within_distance, most_similar
    
    # Using functional API
    results = session.query(
        Document.id,
        Document.title,
        Document.content,
        l2_distance(Document.embedding, query_vector).label('distance')
    ).filter(
        within_distance(Document.embedding, query_vector, 1.0, "l2")
    ).filter(
        Document.title.like('%AI%')
    ).order_by(
        most_similar(Document.embedding, query_vector, "l2")
    ).limit(10).all()

Ultra-Elegant ORM Approach
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # One-liner ORM query
    results = (session.query(Document)
              .filter(Document.embedding.within_distance(query_vector, 1.0))
              .filter(Document.title.like('%AI%'))
              .order_by(Document.embedding.most_similar(query_vector))
              .limit(10).all())

Custom Query Builder
~~~~~~~~~~~~~~~~~~~~

For maximum elegance and reusability:

.. code-block:: python

    class VectorSearchQuery:
        """Ultra-elegant vector search query builder."""
        
        def __init__(self, session, model_class):
            self.session = session
            self.model_class = model_class
            self._query = session.query(model_class)
            self._select_columns = []
            self._filters = []
            self._order_by = None
            self._limit_count = None
        
        def vector_search(self, vector_column, query_vector, distance_type="l2"):
            """Add vector similarity search."""
            if distance_type == "l2":
                distance_expr = vector_column.l2_distance(query_vector)
            elif distance_type == "cosine":
                distance_expr = vector_column.cosine_distance(query_vector)
            elif distance_type == "inner_product":
                distance_expr = vector_column.inner_product(query_vector)
            
            self._select_columns.append(distance_expr.label('distance'))
            self._order_by = distance_expr
            return self
        
        def within_distance(self, vector_column, query_vector, max_distance, distance_type="l2"):
            """Add distance threshold filter."""
            if distance_type == "l2":
                self._filters.append(vector_column.l2_distance(query_vector) < max_distance)
            elif distance_type == "cosine":
                self._filters.append(vector_column.cosine_distance(query_vector) < max_distance)
            elif distance_type == "inner_product":
                self._filters.append(vector_column.inner_product(query_vector) > max_distance)
            return self
        
        def filter_by(self, **kwargs):
            """Add additional filters."""
            for key, value in kwargs.items():
                if hasattr(self.model_class, key):
                    column = getattr(self.model_class, key)
                    if isinstance(value, str) and '%' in value:
                        self._filters.append(column.like(value))
                    else:
                        self._filters.append(column == value)
            return self
        
        def limit(self, count):
            """Set result limit."""
            self._limit_count = count
            return self
        
        def execute(self):
            """Execute the query and return results."""
            query = self.session.query(*self._select_columns) if self._select_columns else self._query
            
            for filter_condition in self._filters:
                query = query.filter(filter_condition)
            
            if self._order_by:
                query = query.order_by(self._order_by)
            
            if self._limit_count:
                query = query.limit(self._limit_count)
            
            return query.all()
    
    # Usage
    results = (VectorSearchQuery(session, Document)
              .vector_search(Document.embedding, query_vector, "l2")
              .within_distance(Document.embedding, query_vector, 1.0, "l2")
              .filter_by(title='%AI%')
              .limit(10)
              .execute())

Functional API
--------------

MatrixOne provides independent distance functions that can be used with any column,
offering more flexibility than the VectorColumn methods:

``l2_distance(column, other)``
    Calculate L2 (Euclidean) distance between vectors.

``cosine_distance(column, other)``
    Calculate cosine distance between vectors.

``inner_product(column, other)``
    Calculate inner product (dot product) between vectors.

``within_distance(column, query_vector, max_distance, distance_type="l2")``
    Create a distance threshold filter expression.

``most_similar(column, query_vector, distance_type="l2")``
    Create an expression for finding most similar vectors.

``vector_similarity_search(column, query_vector, distance_type="l2", max_distance=None)``
    Create a similarity search expression with optional distance filtering.

Examples:

.. code-block:: python

    from matrixone.sqlalchemy_ext import l2_distance, within_distance, most_similar
    
    # Basic distance calculation
    result = session.query(Document).filter(
        l2_distance(Document.embedding, [1, 2, 3]) < 0.5
    )
    
    # Distance threshold filtering
    result = session.query(Document).filter(
        within_distance(Document.embedding, [1, 2, 3], max_distance=1.0)
    )
    
    # Most similar vectors
    result = session.query(Document).order_by(
        most_similar(Document.embedding, [1, 2, 3])
    ).limit(10)
    
    # With string vectors
    result = session.query(Document).filter(
        l2_distance(Document.embedding, "[1,2,3]") < 0.5
    )
    
    # With another column
    result = session.query(Document).filter(
        l2_distance(Document.embedding, Document.query_vector) < 0.5
    )

Enhanced VectorColumn Methods
-----------------------------

The enhanced VectorColumn class provides these convenient methods:

``within_distance(other, max_distance, distance_type="l2")``
    Create a distance threshold filter expression.

``most_similar(other, distance_type="l2", limit=10)``
    Create an expression for finding most similar vectors.

``similarity_search(other, distance_type="l2", max_distance=None)``
    Create a similarity search expression with optional distance filtering.

Examples:

.. code-block:: python

    # Distance threshold filtering
    query = session.query(Document).filter(
        Document.embedding.within_distance([1, 2, 3], max_distance=1.0)
    )
    
    # Most similar vectors
    query = session.query(Document).order_by(
        Document.embedding.most_similar([1, 2, 3])
    ).limit(10)
    
    # Similarity search with distance calculation
    query = session.query(Document).order_by(
        Document.embedding.similarity_search([1, 2, 3])
    )

Multiple Distance Metrics
-------------------------

Support for different distance calculations:

.. code-block:: python

    # L2 distance (Euclidean)
    results = session.query(Document).order_by(
        Document.embedding.l2_distance(query_vector)
    ).limit(10).all()
    
    # Cosine distance
    results = session.query(Document).order_by(
        Document.embedding.cosine_distance(query_vector)
    ).limit(10).all()
    
    # Inner product (dot product)
    results = session.query(Document).order_by(
        Document.embedding.inner_product(query_vector).desc()
    ).limit(10).all()

Advanced ORM Patterns
---------------------

Complex queries with multiple distance metrics:

.. code-block:: python

    # Compare different distance metrics
    results = session.query(
        Document.id,
        Document.title,
        Document.embedding.l2_distance(query_vector).label('l2_distance'),
        Document.embedding.cosine_distance(query_vector).label('cosine_distance'),
        Document.embedding.inner_product(query_vector).label('inner_product')
    ).filter(
        Document.title.like('%AI%')
    ).order_by(
        Document.embedding.l2_distance(query_vector)
    ).limit(5).all()

Aggregation queries:

.. code-block:: python

    # Count documents by distance ranges
    results = session.query(
        func.case(
            (Document.embedding.l2_distance(query_vector) < 0.5, 'Very Close'),
            (Document.embedding.l2_distance(query_vector) < 1.0, 'Close'),
            (Document.embedding.l2_distance(query_vector) < 2.0, 'Moderate'),
            else_='Far'
        ).label('distance_range'),
        func.count(Document.id).label('count')
    ).filter(
        Document.title.like('%AI%')
    ).group_by(
        func.case(
            (Document.embedding.l2_distance(query_vector) < 0.5, 'Very Close'),
            (Document.embedding.l2_distance(query_vector) < 1.0, 'Close'),
            (Document.embedding.l2_distance(query_vector) < 2.0, 'Moderate'),
            else_='Far'
        )
    ).all()

Benefits of ORM Approach
------------------------

1. **Type Safety**: Column references are type-safe and checked at development time.

2. **IDE Support**: Full autocomplete and IntelliSense support in modern IDEs.

3. **Maintainability**: Easier to modify and extend queries.

4. **Reusability**: Query patterns can be easily reused and composed.

5. **Testing**: ORM queries are easier to unit test.

6. **Consistency**: Follows SQLAlchemy patterns and conventions.

7. **Performance**: SQLAlchemy optimizes query generation and execution.

8. **Security**: Built-in protection against SQL injection.

Migration Guide
---------------

To migrate from raw SQL to ORM:

1. **Define Models**: Create SQLAlchemy models with VectorColumn.

2. **Replace Raw SQL**: Convert raw SQL queries to ORM queries.

3. **Use Enhanced Methods**: Leverage the new VectorColumn methods for cleaner code.

4. **Create Query Builders**: For complex applications, create custom query builders.

5. **Test Thoroughly**: Ensure ORM queries produce the same results as raw SQL.

Best Practices
--------------

1. **Use VectorColumn**: Always use VectorColumn for vector operations.

2. **Leverage Enhanced Methods**: Use within_distance() and most_similar() for cleaner code.

3. **Create Query Builders**: For complex applications, create reusable query builders.

4. **Index Vectors**: Create vector indexes for better performance.

5. **Batch Operations**: Use bulk operations for large datasets.

6. **Connection Management**: Always use proper session management with try/finally blocks.

7. **Error Handling**: Implement proper error handling for database operations.

API Comparison
--------------

MatrixOne now provides three different approaches for vector operations:

1. **Raw SQL**: Direct SQL with parameters
2. **VectorColumn Methods**: Object-oriented approach with method chaining
3. **Functional API**: Independent functions for maximum flexibility

Comparison:

.. code-block:: python

    # Raw SQL
    result = session.execute(text("""
        SELECT id, title, l2_distance(embedding, :query_vector) as distance
        FROM documents
        WHERE l2_distance(embedding, :query_vector) < :max_distance
        ORDER BY distance ASC
    """), {'query_vector': query_vector, 'max_distance': 1.0})
    
    # VectorColumn Methods
    result = session.query(Document).filter(
        Document.embedding.within_distance(query_vector, 1.0)
    ).order_by(
        Document.embedding.most_similar(query_vector)
    ).all()
    
    # Functional API
    from matrixone.sqlalchemy_ext import l2_distance, within_distance, most_similar
    result = session.query(Document).filter(
        within_distance(Document.embedding, query_vector, 1.0, "l2")
    ).order_by(
        most_similar(Document.embedding, query_vector, "l2")
    ).all()

Benefits of Each Approach:

**Raw SQL**:
- Direct control over SQL generation
- Familiar SQL syntax
- No learning curve for SQL developers

**VectorColumn Methods**:
- Object-oriented style
- Method chaining
- Type safety with VectorColumn
- Encapsulation of vector operations

**Functional API**:
- Maximum flexibility
- Works with any column type
- Functional programming style
- Easy to compose and reuse
- Clear separation of concerns

Examples
--------

See the following example files for complete implementations:

- ``examples/example_orm_vector_search.py`` - Basic ORM vector search
- ``examples/example_enhanced_orm_vector_search.py`` - Enhanced ORM with custom query builder
- ``examples/example_functional_vector_search.py`` - Functional API demonstration

These examples demonstrate the complete evolution from raw SQL to ultra-elegant ORM queries with multiple API options.
