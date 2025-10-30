JSON Type Guide
===============

MatrixOne Python SDK provides comprehensive support for JSON data type with SQLAlchemy-standard syntax.

.. contents:: Table of Contents
   :depth: 3
   :local:

Overview
--------

The SDK's JSON type provides:

* **SQLAlchemy Standard Syntax**: Use familiar ``column['key']`` dictionary-style access
* **Automatic Type Conversion**: ``.astext`` for strings, ``.cast()`` for numbers
* **Boolean Handling**: Native Python ``True``/``False`` support
* **Custom JSON Functions**: Direct access to MatrixOne JSON functions
* **Full ORM Integration**: Works seamlessly with SQLAlchemy queries

Quick Start
-----------

Basic Usage
~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import declarative_base
   from matrixone.sqlalchemy_ext import JSON
   from sqlalchemy import Column, Integer, String

   Base = declarative_base()

   class Product(Base):
       __tablename__ = 'products'
       id = Column(Integer, primary_key=True)
       name = Column(String(200))
       specifications = Column(JSON)  # MatrixOne JSON type

   client = Client()
   client.connect(database='test')
   client.create_table(Product)

Inserting JSON Data
~~~~~~~~~~~~~~~~~~~

You can insert Python dictionaries directly - they're automatically serialized:

.. code-block:: python

   # Using client.insert()
   client.insert(Product, {
       'id': 1,
       'name': 'Laptop',
       'specifications': {
           'brand': 'Dell',
           'ram': 16,
           'storage': '512GB SSD',
           'active': True
       }
   })

   # Using client.batch_insert()
   products = [
       {
           'id': 2,
           'name': 'Mouse',
           'specifications': {
               'brand': 'Logitech',
               'wireless': True,
               'price': 99.99
           }
       },
       # ... more products
   ]
   client.batch_insert(Product, products)

SQLAlchemy Standard Syntax
---------------------------

Dictionary-Style Access
~~~~~~~~~~~~~~~~~~~~~~~

Use ``column['key']`` to access JSON fields:

.. code-block:: python

   # Query with JSON field access
   results = client.query(Product).filter(
       Product.specifications['brand'] == 'Dell'
   ).all()

   # Nested access
   results = client.query(Product).filter(
       Product.specifications['hardware']['processor'] == 'Intel i7'
   ).all()

.. note::
   String values are automatically quoted in comparisons. Use ``.astext`` for explicit string extraction.

Text Extraction (.astext)
~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``.astext`` property removes JSON quotes automatically:

.. code-block:: python

   from sqlalchemy import select

   # Extract brand as plain text (no quotes)
   stmt = select(
       Product.name,
       Product.specifications['brand'].astext.label('brand')
   )
   
   results = client.execute(stmt).fetchall()
   for row in results:
       print(f"{row.name}: {row.brand}")  # No quotes around brand

   # Use in WHERE clause
   results = client.query(Product).filter(
       Product.specifications['category'].astext == 'Electronics'
   ).all()

Type Casting (.cast())
~~~~~~~~~~~~~~~~~~~~~~

Cast JSON values to SQL types for numeric operations:

.. code-block:: python

   from sqlalchemy import Numeric

   # Cast price to numeric for comparison
   expensive_products = client.query(Product).filter(
       Product.specifications['price'].cast(Numeric) > 500
   ).all()

   # Use in SELECT
   stmt = select(
       Product.name,
       Product.specifications['price'].cast(Numeric).label('price')
   ).order_by(
       Product.specifications['price'].cast(Numeric).desc()
   )

Boolean Handling
~~~~~~~~~~~~~~~~

Work with JSON boolean values naturally:

.. code-block:: python

   # Use Python True/False directly (recommended)
   active_products = client.query(Product).filter(
       Product.specifications['active'] == True
   ).all()

   inactive_products = client.query(Product).filter(
       Product.specifications['active'] == False
   ).all()

   # Or use string literals
   verified = client.query(Product).filter(
       Product.specifications['verified'] == 'true'
   ).all()

   # Use .asbool for explicit boolean conversion
   stmt = select(
       Product.name,
       Product.specifications['active'].asbool.label('is_active')
   )

Complete Example
~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import Numeric, select

   # Complex query with multiple JSON operations
   stmt = select(
       Product.name,
       Product.specifications['brand'].astext.label('brand'),
       Product.specifications['category'].astext.label('category'),
       Product.specifications['price'].cast(Numeric).label('price'),
       Product.specifications['active'].label('is_active')
   ).where(
       Product.specifications['active'] == True
   ).where(
       Product.specifications['price'].cast(Numeric) > 100
   ).order_by(
       Product.specifications['price'].cast(Numeric).desc()
   )

   results = client.execute(stmt).fetchall()
   for row in results:
       print(f"{row.name} ({row.brand}): ${row.price}")

Custom JSON Functions
---------------------

For advanced use cases, the SDK provides direct access to MatrixOne JSON functions.

json_extract
~~~~~~~~~~~~

Extract values from JSON columns:

.. code-block:: python

   from matrixone.sqlalchemy_ext import json_extract

   # Extract single field
   stmt = select(
       Product.name,
       json_extract(Product.specifications, '$.brand').label('brand')
   )

   # Extract nested field
   stmt = select(
       json_extract(Product.specifications, '$.hardware.processor')
   )

   # Extract with multiple paths
   stmt = select(
       json_extract(Product.specifications, '$.brand', '$.model')
   )

json_extract_string
~~~~~~~~~~~~~~~~~~~

Extract text without JSON quotes:

.. code-block:: python

   from matrixone.sqlalchemy_ext import json_extract_string

   stmt = select(
       Product.name,
       json_extract_string(Product.specifications, '$.brand').label('brand')
   )
   # Returns: 'Dell' (not '"Dell"')

json_extract_float64
~~~~~~~~~~~~~~~~~~~~

Extract numeric values:

.. code-block:: python

   from matrixone.sqlalchemy_ext import json_extract_float64

   stmt = select(
       Product.name,
       json_extract_float64(Product.specifications, '$.price').label('price')
   )

Modifying JSON Data
~~~~~~~~~~~~~~~~~~~~

Use ``json_set``, ``json_insert``, and ``json_replace`` for updates:

.. code-block:: python

   from matrixone.sqlalchemy_ext import json_set, json_insert, json_replace
   from sqlalchemy import update

   # json_set: Update or insert
   stmt = update(Product).values(
       specifications=json_set(
           Product.specifications,
           '$.ram', 32,
           '$.warranty', '3 years'
       )
   ).where(Product.id == 1)
   client.execute(stmt)

   # json_insert: Only insert new fields
   stmt = update(Product).values(
       specifications=json_insert(
           Product.specifications,
           '$.warranty', '2 years'  # Only added if doesn't exist
       )
   )

   # json_replace: Only update existing fields
   stmt = update(Product).values(
       specifications=json_replace(
           Product.specifications,
           '$.price', 999.99  # Only updated if exists
       )
   )

JSON Path Expressions
---------------------

MatrixOne supports standard JSON path syntax:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Path Expression
     - Description
   * - ``'$.key'``
     - Access object key at root level
   * - ``'$.key1.key2'``
     - Access nested object keys
   * - ``'$[0]'``
     - Access array element by index
   * - ``'$.items[0]'``
     - Access array element in object
   * - ``'$[*]'``
     - All array elements (wildcard)
   * - ``'$.*'``
     - All object values (wildcard)
   * - ``'$**.key'``
     - Recursive search for key

Examples:

.. code-block:: python

   from matrixone.sqlalchemy_ext import json_extract

   # Array access
   first_feature = json_extract(Product.specifications, '$.features[0]')

   # Nested objects
   cpu = json_extract(Product.specifications, '$.hardware.processor')

   # Wildcards
   all_values = json_extract(Product.specifications, '$.*')

Best Practices
--------------

1. **Use Standard Syntax**
   
   Prefer ``column['key']`` over ``json_extract()``:

   .. code-block:: python

      # ✅ Recommended: Standard syntax
      Product.specifications['brand'].astext
      
      # ⚠️ Less convenient: Direct function
      json_extract_string(Product.specifications, '$.brand')

2. **Type Conversions**
   
   Always cast for numeric operations:

   .. code-block:: python

      # ✅ Correct: Cast to Numeric
      Product.specifications['price'].cast(Numeric) > 100
      
      # ❌ Wrong: String comparison
      Product.specifications['price'] > 100

3. **Boolean Comparisons**
   
   Use Python booleans directly:

   .. code-block:: python

      # ✅ Recommended: Python boolean
      Product.specifications['active'] == True
      
      # ✅ Also works: String
      Product.specifications['active'] == 'true'

4. **Batch Inserts**
   
   Use ``batch_insert()`` for multiple records:

   .. code-block:: python

      # ✅ Efficient: Single batch
      client.batch_insert(Product, products_list)
      
      # ❌ Inefficient: Multiple inserts
      for product in products_list:
          client.insert(Product, product)

5. **Index JSON Extracts**
   
   For frequently queried JSON fields, consider extracting to regular columns:

   .. code-block:: python

      # Add regular columns for frequently queried fields
      class Product(Base):
          __tablename__ = 'products'
          id = Column(Integer, primary_key=True)
          name = Column(String(200))
          brand = Column(String(100))  # Extracted from JSON
          specifications = Column(JSON)  # Full data

Common Patterns
---------------

Filtering by Multiple JSON Conditions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import and_

   results = client.query(Product).filter(
       and_(
           Product.specifications['active'] == True,
           Product.specifications['category'].astext == 'Electronics',
           Product.specifications['price'].cast(Numeric) > 500
       )
   ).all()

Ordering by JSON Fields
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import desc

   # Order by price (descending)
   results = client.query(Product).order_by(
       Product.specifications['price'].cast(Numeric).desc()
   ).all()

Aggregating JSON Data
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import func

   # Count by brand
   stmt = select(
       Product.specifications['brand'].astext.label('brand'),
       func.count().label('count')
   ).group_by(
       Product.specifications['brand']
   )

   results = client.execute(stmt).fetchall()

Checking for NULL or Missing Keys
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from sqlalchemy import and_, or_

   # Find products where warranty is NULL or missing
   results = client.query(Product).filter(
       or_(
           Product.specifications['warranty'].is_(None),
           json_extract(Product.specifications, '$.warranty').is_(None)
       )
   ).all()

Troubleshooting
---------------

Common Issues
~~~~~~~~~~~~~

**Issue**: String comparisons not working

.. code-block:: python

   # ❌ May not work as expected
   Product.specifications['brand'] == 'Dell'

**Solution**: JSON strings include quotes. Use ``.astext`` or let the SDK handle it:

.. code-block:: python

   # ✅ Automatic quote handling
   Product.specifications['brand'] == 'Dell'  # SDK handles quotes
   
   # ✅ Or explicit with astext
   Product.specifications['brand'].astext == 'Dell'

**Issue**: Numeric comparisons not working

.. code-block:: python

   # ❌ String comparison
   Product.specifications['price'] > 100

**Solution**: Cast to numeric type:

.. code-block:: python

   # ✅ Numeric comparison
   Product.specifications['price'].cast(Numeric) > 100

**Issue**: Boolean values not matching

.. code-block:: python

   # May not work consistently
   Product.specifications['active'] == 1

**Solution**: Use Python boolean or explicit string:

.. code-block:: python

   # ✅ Use Python boolean
   Product.specifications['active'] == True
   
   # ✅ Or explicit string
   Product.specifications['active'] == 'true'

Performance Considerations
~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **JSON extraction is slower than regular columns**
   
   For frequently queried fields, extract to regular columns.

2. **Use specific extraction functions**
   
   ``json_extract_string`` and ``json_extract_float64`` are more efficient than ``json_extract`` with casting.

3. **Batch operations**
   
   Always use ``batch_insert()`` for multiple records.

4. **Index considerations**
   
   MatrixOne doesn't support direct indexing on JSON paths. Extract important fields to regular columns and index those.

API Reference
-------------

For detailed API documentation, see:

* :doc:`api/index` - Complete API reference
* ``matrixone.sqlalchemy_ext.JSON`` - JSON type class
* ``matrixone.sqlalchemy_ext.json_extract`` - JSON extraction functions
* ``matrixone.sqlalchemy_ext.json_set`` - JSON modification functions

Examples
--------

For complete working examples, see:

* ``examples/example_24_query_update.py`` - Query and update with JSON
* ``sdk_demo/test_json_standard_syntax.py`` - Standard syntax demonstrations
* ``sdk_demo/test_json_bool_handling.py`` - Boolean handling examples

See Also
--------

* :doc:`orm_guide` - ORM usage guide
* :doc:`quickstart` - Getting started guide
* :doc:`best_practices` - Best practices and patterns

