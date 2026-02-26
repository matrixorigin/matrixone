.. _branch_guide:

Branch Management Guide
=======================

Overview
--------

MatrixOne's branch management feature allows you to create isolated branches of your database for development, testing, and experimentation without affecting the main database. Branches are lightweight and efficient, making them ideal for:

* Development and testing
* Feature branches for different teams
* Safe experimentation with schema changes
* Parallel development workflows

**Version Requirement:** MatrixOne 3.0.5 or higher

Creating and Managing Branches
------------------------------

Basic Branch Operations
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import Client

    client = Client()
    client.connect(database='test')

    # Create a table branch
    client.branch.create_table_branch(
        target_table='users_branch',
        source_table='users'
    )

    # Create a database branch
    client.branch.create_database_branch(
        target_database='dev_db',
        source_database='main_db'
    )

    # Delete a table branch
    client.branch.delete_table_branch('users_branch')

    # Delete a database branch
    client.branch.delete_database_branch('dev_db')

Simplified API
~~~~~~~~~~~~~~

For convenience, you can use the simplified API that auto-detects table vs database operations:

.. code-block:: python

    from matrixone import Client

    client = Client()
    client.connect(database='test')

    # Create table branch (simplified)
    client.branch.create('users_branch', 'users')

    # Create database branch (simplified)
    client.branch.create('dev_db', 'main_db', database=True)

    # Delete table branch (simplified)
    client.branch.delete('users_branch')

    # Delete database branch (simplified)
    client.branch.delete('dev_db', database=True)

Branch Workflow Example
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from matrixone import Client

    client = Client()
    client.connect(database='production')

    # Create a development branch from production database
    client.branch.create_database_branch(
        target_database='feature_branch',
        source_database='production'
    )

    # Make changes in the branch database
    client.execute("USE feature_branch")
    client.execute("""
        ALTER TABLE users ADD COLUMN new_field VARCHAR(100)
    """)

    # Test the changes
    result = client.execute("SELECT * FROM users LIMIT 1")
    print(result.fetchall())

    # Compare with original
    diffs = client.branch.diff_table(
        table='feature_branch.users',
        against_table='production.users'
    )
    print(f"Differences: {diffs}")

    # If satisfied, merge back to main
    client.branch.merge_table(
        source_table='feature_branch.users',
        target_table='production.users',
        conflict_strategy='accept'
    )

    # Clean up
    client.branch.delete_database_branch('feature_branch')

Use Cases
---------

Development and Testing
~~~~~~~~~~~~~~~~~~~~~~~

Create isolated branches for each feature or bug fix:

.. code-block:: python

    from matrixone import Client

    client = Client()
    client.connect(database='main')

    # Create a branch for a new feature
    client.branch.create_table_branch(
        target_table='feature_users',
        source_table='users'
    )

    # Work on the feature in isolation
    client.execute("USE main")
    client.execute("""
        INSERT INTO feature_users (name, email) VALUES ('test', 'test@example.com')
    """)

    # Run tests and development
    result = client.execute("SELECT * FROM feature_users")
    print(result.fetchall())

Schema Experimentation
~~~~~~~~~~~~~~~~~~~~~~

Test schema changes safely without affecting production:

.. code-block:: python

    from matrixone import Client

    client = Client()
    client.connect(database='production')

    # Create a test branch
    client.branch.create_database_branch(
        target_database='schema_test',
        source_database='production'
    )

    # Switch to test database and experiment with schema changes
    client.execute("USE schema_test")
    client.execute("ALTER TABLE products ADD COLUMN category_id INT")
    client.execute("CREATE INDEX idx_category ON products(category_id)")

    # Verify changes work correctly
    result = client.execute("SELECT * FROM products LIMIT 1")
    print(result.fetchall())

    # If successful, apply to production
    # If not, simply delete the branch
    client.branch.delete_database_branch('schema_test')

Parallel Development
~~~~~~~~~~~~~~~~~~~~

Multiple teams can work on different branches simultaneously:

.. code-block:: python

    from matrixone import Client

    client = Client()
    client.connect(database='main')

    # Team A creates their branch
    client.branch.create_database_branch(
        target_database='team_a_feature',
        source_database='main'
    )

    # Team B creates their branch
    client.branch.create_database_branch(
        target_database='team_b_feature',
        source_database='main'
    )

    # Both teams work independently
    # Changes in one branch don't affect the other
    client.execute("USE team_a_feature")
    client.execute("INSERT INTO users (name) VALUES ('Team A User')")

    client.execute("USE team_b_feature")
    client.execute("INSERT INTO users (name) VALUES ('Team B User')")

Comparing and Merging Branches
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Compare differences between branches and merge them:

.. code-block:: python

    from matrixone import Client, DiffOutput, MergeConflictStrategy

    client = Client()
    client.connect(database='test')

    # Create a branch
    client.branch.create_table_branch(
        target_table='users_branch',
        source_table='users'
    )

    # Make changes in the branch
    client.execute("INSERT INTO users_branch (name) VALUES ('New User')")

    # Get count of differences (performance optimized)
    diff_count = client.branch.diff_table(
        table='users_branch',
        against_table='users',
        output=DiffOutput.COUNT
    )
    count_value = diff_count[0][0] if diff_count else 0
    print(f"Differences found: {count_value}")

    # Get detailed differences
    diffs = client.branch.diff_table(
        table='users_branch',
        against_table='users',
        output=DiffOutput.ROWS  # or just omit, it's the default
    )
    print(f"Detailed differences: {len(diffs)}")

    # Merge branch back to original (skip conflicts)
    client.branch.merge_table(
        source_table='users_branch',
        target_table='users',
        on_conflict=MergeConflictStrategy.SKIP
    )

    # Or accept source values on conflict
    client.branch.merge_table(
        source_table='users_branch',
        target_table='users',
        on_conflict=MergeConflictStrategy.ACCEPT
    )

    # Clean up
    client.branch.delete_table_branch('users_branch')

Point-in-Time Branching
~~~~~~~~~~~~~~~~~~~~~~~

Create branches from specific snapshots:

.. code-block:: python

    from matrixone import Client

    client = Client()
    client.connect(database='production')

    # Create a snapshot first
    client.snapshot.create_snapshot(
        snapshot_name='daily_backup_2024_01_01',
        database='production'
    )

    # Create a branch from the snapshot
    client.branch.create_table_branch(
        target_table='users_historical',
        source_table='users',
        snapshot_name='daily_backup_2024_01_01'
    )

    # Now you have a table as it was at that point in time
    result = client.execute("SELECT * FROM users_historical")
    print(result.fetchall())

Best Practices
--------------

1. **Use Descriptive Names**: Name branches clearly to indicate their purpose

   .. code-block:: python

       # Good
       client.branch.create_database_branch('feature_payment_integration', 'main')
       client.branch.create_database_branch('bugfix_user_login_issue', 'main')

       # Avoid
       client.branch.create_database_branch('test', 'main')
       client.branch.create_database_branch('temp', 'main')

2. **Clean Up**: Delete branches when they're no longer needed

   .. code-block:: python

       # Always clean up after finishing
       client.branch.delete_database_branch('feature_completed_feature')

3. **Test Before Merging**: Always test changes in a branch before applying to main

   .. code-block:: python

       # Create branch
       client.branch.create_table_branch('test_branch', 'main_table')

       # Make and test changes
       client.execute("INSERT INTO test_branch VALUES (...)")
       result = client.execute("SELECT * FROM test_branch")

       # Verify results before merging
       if result.fetchall():
           client.branch.merge_table('test_branch', 'main_table')

4. **Document Changes**: Keep track of what changes were made in each branch

5. **Use Consistent Naming**: Follow a naming convention for your branches

   - ``feature/`` for new features
   - ``bugfix/`` for bug fixes
   - ``hotfix/`` for urgent fixes
   - ``experiment/`` for experimental work

Performance Considerations
--------------------------

* Branches are lightweight and don't duplicate data
* Creating a branch is fast and efficient
* Multiple branches can coexist without significant overhead
* Diff and merge operations are optimized for performance

Limitations
-----------

* Branches are isolated - changes in one branch don't affect others until merged
* Merging branches requires explicit operations
* Some operations may have branch-specific constraints

API Reference
-------------

Enums
~~~~~

DiffOutput Enum
^^^^^^^^^^^^^^^

.. code-block:: python

    from matrixone import DiffOutput

    class DiffOutput(str, Enum):
        ROWS = 'rows'    # Return detailed differences (default)
        COUNT = 'count'  # Return only the count of differences

MergeConflictStrategy Enum
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

    from matrixone import MergeConflictStrategy

    class MergeConflictStrategy(str, Enum):
        SKIP = 'skip'      # Skip conflicting rows (default)
        ACCEPT = 'accept'  # Accept source values on conflict

Table Branch Operations
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Create table branch
    client.branch.create_table_branch(
        target_table: str,
        source_table: str,
        snapshot_name: Optional[str] = None
    ) -> None

    # Delete table branch
    client.branch.delete_table_branch(table: str) -> None

    # Compare tables
    client.branch.diff_table(
        table: str,
        against_table: str,
        snapshot_name: Optional[str] = None,
        output: DiffOutput = DiffOutput.ROWS
    ) -> List[Dict[str, Any]]

    # Merge tables
    client.branch.merge_table(
        source_table: str,
        target_table: str,
        on_conflict: Union[str, MergeConflictStrategy] = "skip"
    ) -> None

Database Branch Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Create database branch
    client.branch.create_database_branch(
        target_database: str,
        source_database: str
    ) -> None

    # Delete database branch
    client.branch.delete_database_branch(database: str) -> None

Simplified API
~~~~~~~~~~~~~~

.. code-block:: python

    # Create branch (auto-detects table vs database)
    client.branch.create(
        target: Union[str, Type],
        source: Union[str, Type],
        snapshot: Optional[str] = None,
        database: bool = False
    ) -> None

    # Delete branch (auto-detects table vs database)
    client.branch.delete(
        name: Union[str, Type],
        database: bool = False
    ) -> None

    # Compare branches
    client.branch.diff(
        table: Union[str, Type],
        against: Union[str, Type],
        snapshot: Optional[str] = None,
        output: DiffOutput = DiffOutput.ROWS
    ) -> List[Dict[str, Any]]

    # Merge branches
    client.branch.merge(
        source: Union[str, Type],
        target: Union[str, Type],
        on_conflict: Union[str, MergeConflictStrategy] = "skip"
    ) -> None

See Also
--------

* :doc:`snapshot_restore_guide` - For point-in-time recovery
* :doc:`clone_guide` - For database cloning
* :doc:`examples` - For more examples
