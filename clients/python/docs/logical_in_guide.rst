Logical IN Operations Guide
============================

The MatrixOne Python SDK provides enhanced logical IN operations through the ``logical_in`` function, which offers more flexibility and power than standard SQLAlchemy IN operations.

Overview
--------

The ``logical_in`` function allows you to create IN conditions that can handle various types of values and expressions, making it more versatile than traditional IN operations. It automatically generates appropriate SQL based on the input type and provides better integration with MatrixOne's query builder.

Key Features
------------

* **Multiple Value Types**: Support for lists, single values, SQLAlchemy expressions, and FulltextFilter objects
* **Automatic SQL Generation**: Generates appropriate SQL based on input type
* **SQLAlchemy Integration**: Seamless integration with SQLAlchemy expressions
* **FulltextFilter Support**: Direct integration with fulltext search conditions
* **Subquery Support**: Support for subqueries and complex expressions
* **Parameter Binding**: Automatic parameter binding and SQL injection prevention

Basic Usage
-----------

Import the function:

.. code-block:: python

   from matrixone.orm import logical_in

List of Values
--------------

The most common use case is checking if a column value is in a list of values:

.. code-block:: python

   # Basic list of values
   query = client.query(User).filter(
       logical_in("city", ["北京", "上海", "广州"])
   )
   # Generates: WHERE city IN ('北京', '上海', '广州')

   # Numeric values
   query = client.query(User).filter(
       logical_in(User.id, [1, 2, 3, 4, 5])
   )
   # Generates: WHERE id IN (1, 2, 3, 4, 5)

   # Mixed types
   query = client.query(User).filter(
       logical_in("status", ["active", "pending", 1, 2])
   )
   # Generates: WHERE status IN ('active', 'pending', 1, 2)

Single Values
-------------

You can also use single values:

.. code-block:: python

   # Single string value
   query = client.query(User).filter(
       logical_in("status", "active")
   )
   # Generates: WHERE status IN ('active')

   # Single numeric value
   query = client.query(User).filter(
       logical_in(User.id, 42)
   )
   # Generates: WHERE id IN (42)

   # NULL values
   query = client.query(User).filter(
       logical_in("description", None)
   )
   # Generates: WHERE description IN (NULL)

SQLAlchemy Expressions
----------------------

Support for SQLAlchemy expressions allows for complex conditions:

.. code-block:: python

   from sqlalchemy import func

   # Using aggregate functions
   query = client.query(User).filter(
       logical_in("id", func.count(User.id))
   )
   # Generates: WHERE id IN (SELECT COUNT(*) FROM users)

   # Using column expressions
   query = client.query(User).filter(
       logical_in(func.upper(User.name), ["JOHN", "JANE"])
   )
   # Generates: WHERE UPPER(name) IN ('JOHN', 'JANE')

FulltextFilter Integration
--------------------------

Direct integration with fulltext search conditions:

.. code-block:: python

   from matrixone.sqlalchemy_ext import boolean_match

   # Using FulltextFilter objects
   query = client.query(Article).filter(
       logical_in(Article.id, boolean_match("title", "content").must("python"))
   )
   # Generates: WHERE id IN (SELECT id FROM table WHERE MATCH(title, content) AGAINST('+python' IN BOOLEAN MODE))

   # Complex fulltext conditions
   query = client.query(Article).filter(
       logical_in(Article.id, boolean_match("title", "content").must("python").should("programming"))
   )

Subquery Support
----------------

Support for subqueries and complex expressions:

.. code-block:: python

   # Using subqueries
   active_user_ids = client.query(User).select(User.id).filter(User.active == True)
   query = client.query(Order).filter(
       logical_in("user_id", active_user_ids)
   )
   # Generates: WHERE user_id IN (SELECT id FROM users WHERE active = 1)

   # Complex subquery with joins
   admin_user_ids = (client.query(User)
                    .select(User.id)
                    .join(Role, User.role_id == Role.id)
                    .filter(Role.name == "admin"))
   
   query = client.query(Order).filter(
       logical_in("user_id", admin_user_ids)
   )

Combined Conditions
-------------------

You can combine logical_in with other conditions:

.. code-block:: python

   # Multiple conditions
   query = (client.query(User)
           .filter(logical_in("city", ["北京", "上海"]))
           .filter(logical_in("department", ["Engineering", "Sales"]))
           .filter(User.age > 25))
   
   # Generates: WHERE city IN ('北京', '上海') AND department IN ('Engineering', 'Sales') AND age > 25

   # With other filter conditions
   query = (client.query(User)
           .filter(User.active == True)
           .filter(logical_in("role_id", [1, 2, 3]))
           .filter(User.created_at > "2024-01-01"))

Advanced Examples
-----------------

Complex real-world scenarios:

.. code-block:: python

   # E-commerce product filtering
   def get_products_by_categories_and_price_range(categories, min_price, max_price):
       return (client.query(Product)
               .filter(logical_in("category", categories))
               .filter(Product.price >= min_price)
               .filter(Product.price <= max_price)
               .filter(Product.in_stock == True))

   # User analytics with multiple conditions
   def get_active_users_in_cities(cities, min_orders):
       return (client.query(User)
               .filter(logical_in("city", cities))
               .filter(User.active == True)
               .filter(logical_in("id", 
                   client.query(Order)
                   .select(func.count(Order.user_id))
                   .group_by(Order.user_id)
                   .having(func.count(Order.user_id) >= min_orders)
               )))

   # Content search with fulltext and categories
   def search_content_with_categories(query_text, categories):
       return (client.query(Article)
               .filter(logical_in(Article.id, 
                   boolean_match("title", "content").must(query_text)))
               .filter(logical_in("category", categories))
               .filter(Article.published == True))

Performance Considerations
--------------------------

* **Index Usage**: Ensure appropriate indexes exist on columns used in IN conditions
* **List Size**: Large lists may impact performance; consider pagination or chunking
* **Subqueries**: Complex subqueries may be slower; consider optimizing the subquery
* **FulltextFilter**: Fulltext operations require appropriate fulltext indexes

Best Practices
--------------

1. **Use Appropriate Data Types**: Ensure list elements match the column data type
2. **Index Optimization**: Create indexes on frequently queried columns
3. **Parameter Binding**: The function automatically handles parameter binding
4. **Error Handling**: Handle cases where lists might be empty
5. **Testing**: Test with various data types and edge cases

Error Handling
--------------

.. code-block:: python

   try:
       # Handle empty lists gracefully
       if user_ids:
           query = client.query(User).filter(logical_in("id", user_ids))
       else:
           query = client.query(User).filter(User.id == -1)  # No results
   except Exception as e:
       print(f"Error in logical_in operation: {e}")

Migration from Standard IN
--------------------------

If you're migrating from standard SQLAlchemy IN operations:

.. code-block:: python

   # Old way
   query = client.query(User).filter(User.id.in_([1, 2, 3]))

   # New way (more flexible)
   query = client.query(User).filter(logical_in(User.id, [1, 2, 3]))

   # Additional benefits with new way
   query = client.query(User).filter(
       logical_in(User.id, boolean_match("name", "email").must("john"))
   )

See Also
--------

* :doc:`orm_guide` - Complete ORM guide
* :doc:`fulltext_guide` - Fulltext search guide
* :ref:`api/orm_classes` - API reference for ORM classes
