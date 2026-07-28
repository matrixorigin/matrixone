Column Naming Conventions
=========================

.. danger::
   **🚨 CRITICAL: This is the #1 cause of errors when using MatrixOne with SQLAlchemy!**
   
   **Always use lowercase with underscores (snake_case) for column names!**

The Problem
-----------

MatrixOne does not support SQL standard double-quoted identifiers in queries. This creates compatibility 
issues with SQLAlchemy ORM when using camelCase or PascalCase column names.

What Happens with CamelCase
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you define columns with mixed case names:

.. code-block:: python

   from matrixone.orm import declarative_base
   from sqlalchemy import Column, Integer, String
   
   Base = declarative_base()
   
   class User(Base):
       __tablename__ = 'users'
       userId = Column(Integer, primary_key=True)      # ❌ CamelCase
       userName = Column(String(50))                   # ❌ CamelCase
       emailAddress = Column(String(100))              # ❌ CamelCase

**Step 1: CREATE TABLE** ✅ **Works**

SQLAlchemy generates (using MySQL backticks):

.. code-block:: sql

   CREATE TABLE users (
       `userId` INTEGER NOT NULL AUTO_INCREMENT,
       `userName` VARCHAR(50),
       `emailAddress` VARCHAR(100),
       PRIMARY KEY (`userId`)
   )

✅ This works! MatrixOne supports backticks.

**Step 2: INSERT** ✅ **Works**

.. code-block:: sql

   INSERT INTO users (userId, userName, emailAddress) 
   VALUES (1, 'Alice', 'alice@example.com')

✅ This works! No quotes needed for INSERT.

**Step 3: SELECT** ❌ **FAILS!**

SQLAlchemy generates (using SQL standard double quotes):

.. code-block:: sql

   SELECT users."userId" AS userId, 
          users."userName" AS userName, 
          users."emailAddress" AS emailAddress 
   FROM users

❌ **SQL Syntax Error!** MatrixOne doesn't support double quotes for identifiers.

Error message::

   (pymysql.err.ProgrammingError) (1064, 'SQL parser error: You have an error in your SQL 
   syntax; check the manual that corresponds to your MatrixOne server version for the right 
   syntax to use. syntax error at line 1 column XX near ""userId"...')

The Solution: snake_case
-------------------------

Use lowercase with underscores for all column names:

.. code-block:: python

   class User(Base):
       __tablename__ = 'users'
       user_id = Column(Integer, primary_key=True)       # ✅ snake_case
       user_name = Column(String(50))                    # ✅ snake_case
       email_address = Column(String(100))               # ✅ snake_case
       created_at = Column(DateTime)                     # ✅ snake_case
       is_active = Column(Boolean)                       # ✅ snake_case

**All operations work perfectly:**

.. code-block:: sql

   -- CREATE TABLE (no quotes needed)
   CREATE TABLE users (
       user_id INTEGER NOT NULL AUTO_INCREMENT,
       user_name VARCHAR(50),
       email_address VARCHAR(100),
       created_at DATETIME,
       is_active BOOLEAN,
       PRIMARY KEY (user_id)
   )
   
   -- INSERT (no quotes needed)
   INSERT INTO users (user_id, user_name, email_address) 
   VALUES (1, 'Alice', 'alice@example.com')
   
   -- SELECT (no quotes needed)
   SELECT users.user_id AS user_id, 
          users.user_name AS user_name, 
          users.email_address AS email_address 
   FROM users

✅ All queries succeed!

Complete Example
----------------

Here's a complete working example following best practices:

.. code-block:: python

   from matrixone import Client
   from matrixone.orm import declarative_base
   from sqlalchemy import Column, Integer, String, DECIMAL, DateTime, Boolean
   from sqlalchemy.dialects.mysql import JSON
   from datetime import datetime
   
   Base = declarative_base()
   
   # ✅ Perfect: All column names use snake_case
   class Product(Base):
       __tablename__ = 'products'
       
       # Primary key
       product_id = Column(Integer, primary_key=True, autoincrement=True)
       
       # Basic info
       product_name = Column(String(200), nullable=False)
       product_code = Column(String(50), unique=True)
       
       # Pricing
       unit_price = Column(DECIMAL(10, 2))
       sale_price = Column(DECIMAL(10, 2))
       
       # Categorization
       category_name = Column(String(100))
       subcategory_name = Column(String(100))
       
       # Status
       is_active = Column(Boolean, default=True)
       is_featured = Column(Boolean, default=False)
       
       # Metadata (JSON field)
       product_metadata = Column(JSON)
       
       # Timestamps
       created_at = Column(DateTime, default=datetime.utcnow)
       updated_at = Column(DateTime, onupdate=datetime.utcnow)
   
   # Connect and create table
   client = Client()
   client.connect(database='demo')
   client.create_table(Product)
   
   # Insert data - direct dict with JSON auto-serialization
   client.insert(Product, {
       'product_name': 'Laptop',
       'product_code': 'LAP-001',
       'unit_price': 999.99,
       'category_name': 'Electronics',
       'is_active': True,
       'product_metadata': {'brand': 'Dell', 'warranty': '2 years'}  # ✅ Dict auto-serializes
   })
   
   # Query data - all operations work perfectly
   results = client.query(
       Product.product_id,
       Product.product_name,
       Product.unit_price,
       Product.product_metadata
   ).filter(Product.is_active == True).execute()
   
   for row in results.rows:
       print(f"{row[0]}: {row[1]} - ${row[2]}")

Naming Rules Summary
--------------------

.. list-table:: Column Naming Rules
   :header-rows: 1
   :widths: 30 20 50

   * - Style
     - Status
     - Examples
   * - **snake_case** (lowercase + underscores)
     - ✅ **Use this!**
     - ``user_name``, ``created_at``, ``is_active``
   * - camelCase
     - ❌ Don't use
     - ``userName``, ``createdAt``, ``isActive``
   * - PascalCase
     - ❌ Don't use
     - ``UserName``, ``CreatedAt``, ``IsActive``
   * - lowercase (no separator)
     - ⚠️ OK but not recommended
     - ``username``, ``createdat``, ``isactive``
   * - UPPERCASE
     - ❌ Don't use
     - ``USERNAME``, ``CREATED_AT``, ``IS_ACTIVE``

Additional Naming Tips
----------------------

1. **Table Names**: Also use snake_case for consistency
   
   .. code-block:: python
   
      __tablename__ = 'user_accounts'      # ✅ Good
      __tablename__ = 'UserAccounts'       # ❌ Avoid
      __tablename__ = 'userAccounts'       # ❌ Avoid

2. **Avoid SQL Reserved Words**
   
   Even with snake_case, avoid these problematic names:
   
   - ``select``, ``from``, ``where``, ``order``, ``group``
   - ``user``, ``table``, ``column``, ``database``
   - ``key``, ``index``, ``create``, ``drop``
   
   If you must use them, add a prefix/suffix:
   
   .. code-block:: python
   
      # ❌ Problematic
      order = Column(Integer)
      user = Column(String(50))
      
      # ✅ Better
      order_id = Column(Integer)
      user_name = Column(String(50))

3. **Boolean Columns**: Use ``is_`` prefix
   
   .. code-block:: python
   
      is_active = Column(Boolean)          # ✅ Clear intent
      is_deleted = Column(Boolean)         # ✅ Clear intent
      active = Column(Boolean)             # ⚠️ Less clear

4. **Timestamp Columns**: Use ``_at`` or ``_date`` suffix
   
   .. code-block:: python
   
      created_at = Column(DateTime)        # ✅ Standard convention
      updated_at = Column(DateTime)        # ✅ Standard convention
      deleted_at = Column(DateTime)        # ✅ Soft delete pattern
      birth_date = Column(Date)            # ✅ Clear for dates

5. **Foreign Key Columns**: Use ``_id`` suffix
   
   .. code-block:: python
   
      user_id = Column(Integer, ForeignKey('users.id'))          # ✅ Clear FK
      category_id = Column(Integer, ForeignKey('categories.id')) # ✅ Clear FK
      parent_id = Column(Integer, ForeignKey('products.id'))     # ✅ Self-reference

Technical Background
--------------------

**Why does MatrixOne have this limitation?**

MatrixOne is based on MySQL, which traditionally uses:

- **Backticks** ```identifier``` for quoted identifiers (MySQL-specific)
- **Double quotes** ``"identifier"`` only in ANSI_QUOTES mode (disabled by default)

MatrixOne currently:

- ✅ Supports backticks for DDL (CREATE TABLE)
- ❌ Does not support double quotes for DML/queries (SELECT, UPDATE, DELETE)

**SQLAlchemy behavior:**

SQLAlchemy's MySQL dialect uses:

- **Backticks** for CREATE TABLE (compatible with MatrixOne)
- **Double quotes** for SELECT/DML when preserving case (incompatible with MatrixOne)

**Solution:** Use snake_case to avoid any quoting entirely!

Comparison with Other Databases
--------------------------------

.. list-table:: Identifier Quoting Support
   :header-rows: 1
   :widths: 20 25 25 30

   * - Database
     - Backticks `
     - Double Quotes "
     - Recommended Style
   * - PostgreSQL
     - ❌ Not supported
     - ✅ Standard (SQL-92)
     - snake_case
   * - MySQL
     - ✅ Standard
     - ⚠️ Needs ANSI_QUOTES
     - snake_case
   * - MatrixOne
     - ✅ DDL only
     - ❌ Not supported
     - **snake_case only!**
   * - SQLite
     - ✅ Supported
     - ✅ Supported
     - snake_case (best practice)

See Also
--------

- :doc:`orm_guide` - Complete ORM usage guide
- :doc:`quickstart` - Quick start with correct naming
- :doc:`examples` - All examples use snake_case

