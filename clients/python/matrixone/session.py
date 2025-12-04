# Copyright 2021 - 2022 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
MatrixOne Session and AsyncSession classes.

This module provides Session and AsyncSession classes that extend SQLAlchemy's
Session and AsyncSession with MatrixOne-specific features like snapshots, clones,
vector operations, and fulltext search.
"""

from typing import Any, Optional, Tuple, TYPE_CHECKING

try:
    from sqlalchemy.orm import Session as SQLAlchemySession
except ImportError:
    SQLAlchemySession = None

try:
    from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession
except ImportError:
    SQLAlchemyAsyncSession = None

from .exceptions import QueryError

if TYPE_CHECKING:
    from .client import ResultSet  # noqa: F401
    from .async_client import AsyncResultSet  # noqa: F401


class Session(SQLAlchemySession):
    """
    MatrixOne Session - Transaction-aware session extending SQLAlchemy Session.

    This class provides a comprehensive transaction context for executing multiple database
    operations atomically. It inherits all SQLAlchemy Session capabilities while adding
    MatrixOne-specific features like snapshots, clones, vector operations, and fulltext search.

    Key Features:

    - **Full SQLAlchemy API**: All standard SQLAlchemy Session methods (add, delete, query, etc.)
    - **Atomic transactions**: All operations succeed or fail together
    - **Automatic rollback**: Errors trigger automatic transaction rollback
    - **MatrixOne managers**: Access to all MatrixOne-specific operations within transactions
    - **Hybrid SQL support**: Execute both SQLAlchemy statements and string SQL
    - **Query logging**: Integrated query logging with performance tracking
    - **Context manager**: Automatic transaction lifecycle management

    Transaction Behavior:

    - **Auto-commit on success**: Transaction commits when context manager exits normally
    - **Auto-rollback on error**: Transaction rolls back if any exception occurs
    - **Explicit control**: Manual commit() and rollback() also supported
    - **Isolation**: All operations within session are isolated from other transactions
    - **ACID compliance**: Full ACID guarantees for all operations

    Available Managers (all transaction-aware):

    - **snapshots**: SnapshotManager for creating/managing database snapshots
    - **clone**: CloneManager for cloning databases and tables
    - **restore**: RestoreManager for restoring from snapshots
    - **pitr**: PitrManager for point-in-time recovery operations
    - **pubsub**: PubSubManager for publish-subscribe operations
    - **account**: AccountManager for account and user management
    - **vector_ops**: VectorManager for vector operations and indexing
    - **fulltext_index**: FulltextIndexManager for fulltext search operations
    - **metadata**: MetadataManager for table metadata analysis
    - **load_data**: LoadDataManager for bulk data loading
    - **stage**: StageManager for external stage management
    - **cdc**: CDCManager for change data capture task operations

    Creating Session:

    There are two ways to create a MatrixOne Session:

    1. New Session (Recommended): Create directly from client::

        with client.session() as session:
            # Your operations here
            pass

    2. Wrap Existing Session (For Legacy Projects): Wrap existing SQLAlchemy session::

        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from matrixone.session import Session as MatrixOneSession

        # Your existing SQLAlchemy code
        engine = create_engine('mysql+pymysql://...')
        SessionFactory = sessionmaker(bind=engine)
        sqlalchemy_session = SessionFactory()

        # Wrap with MatrixOne features
        mo_session = MatrixOneSession(
            client=mo_client,
            wrap_session=sqlalchemy_session
        )
        # Now you can use both SQLAlchemy and MatrixOne features

    Usage Examples::

        from matrixone import Client
        from sqlalchemy import select, insert, update, delete

        client = Client(host='localhost', port=6001, user='root', password='111', database='test')

        # ============================================================
        # Example 0: Wrapping Existing SQLAlchemy Session (Legacy Projects)
        # ============================================================
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from matrixone.session import Session as MatrixOneSession

        # Your existing SQLAlchemy setup
        engine = create_engine('mysql+pymysql://root:111@127.0.0.1:6001/test')
        SessionFactory = sessionmaker(bind=engine)
        existing_session = SessionFactory()

        # Wrap it with MatrixOne features
        mo_session = MatrixOneSession(
            client=client,
            wrap_session=existing_session
        )

        try:
            # Standard SQLAlchemy operations work
            result = mo_session.execute("SELECT * FROM users")

            # MatrixOne features now available
            mo_session.stage.create_s3('backup', bucket='my-backup')
            mo_session.snapshots.create('snapshot1', level='database')
            mo_session.load_data.from_csv('/data/file.csv', 'users')

            mo_session.commit()
        finally:
            mo_session.close()

        # ============================================================
        # Example 1: Basic Transaction with ORM-style Operations
        # ============================================================
        with client.session() as session:
            # All operations are atomic - succeed or fail together
            from sqlalchemy import insert, update

            # Insert using SQLAlchemy insert()
            session.execute(insert(User).values(name='John', email='john@example.com'))
            session.execute(insert(Order).values(user_id=1, amount=100.0))

            # Update using SQLAlchemy update()
            session.execute(
                update(Account).where(Account.user_id == 1).values(balance=Account.balance - 100)
            )
            # Transaction commits automatically on successful completion

        # ============================================================
        # Example 2: SQLAlchemy ORM Operations
        # ============================================================
        with client.session() as session:
            # Create new objects
            user = User(name="Alice", email="alice@example.com")
            order = Order(user_id=1, amount=50.0)

            # Add to session
                session.add(user)
            session.add(order)

            # Query using ORM
            stmt = select(User).where(User.name == "Alice")
            result = session.execute(stmt)
            users = result.scalars().all()

            # Update using ORM
            user = session.get(User, 1)
            user.email = "newemail@example.com"

            # Commit explicitly (or let context manager do it)
                session.commit()

        # ============================================================
        # Example 3: MatrixOne Snapshot Operations in Transaction
        # ============================================================
            with client.session() as session:
            # Create snapshot within transaction
            snapshot = session.snapshots.create(
                name='daily_backup',
                level=SnapshotLevel.DATABASE,
                database='production'
            )

            # Clone database within same transaction
            session.clone.clone_database(
                target_db='production_copy',
                source_db='production',
                snapshot_name='daily_backup'
            )
            # Both operations commit atomically

        # ============================================================
        # Example 4: Bulk Data Loading in Transaction
        # ============================================================
        with client.session() as session:
            # Load data files atomically
            session.load_data.from_csv('/data/users.csv', 'users')
            session.load_data.from_csv('/data/orders.csv', 'orders')

            # Update statistics after loading
            session.execute("ANALYZE TABLE users")
            session.execute("ANALYZE TABLE orders")
            # All loads and updates commit together

        # ============================================================
        # Example 5: Error Handling with Automatic Rollback
        # ============================================================
        try:
            with client.session() as session:
                session.execute("INSERT INTO users (name) VALUES ('Bob')")
                session.execute("INSERT INTO invalid_table (data) VALUES ('test')")  # This fails
                # Transaction automatically rolls back - Bob is NOT inserted
        except Exception as e:
            print(f"Transaction failed and rolled back: {e}")

        # ============================================================
        # Example 6: Manual Transaction Control
        # ============================================================
        with client.session() as session:
            try:
                session.execute("INSERT INTO users (name) VALUES ('Charlie')")
                session.execute("UPDATE accounts SET balance = balance - 50")

                # Verify conditions before committing
                result = session.execute("SELECT balance FROM accounts WHERE id = 1")
                balance = result.scalar()

                if balance >= 0:
                    session.commit()  # Explicit commit
                else:
                    session.rollback()  # Explicit rollback
                    print("Transaction rolled back due to insufficient balance")
            except Exception as e:
                session.rollback()
                raise

        # ============================================================
        # Example 7: Complex Multi-Manager Transaction
        # ============================================================
        with client.session() as session:
            # Create publication
            pub = session.pubsub.create_database_publication(
                name='analytics_data',
                database='analytics',
                account='subscriber_account'
            )

            # Create stage using simple interface
            session.stage.create_local('export_stage', '/exports/')

            # Load fresh data
            session.load_data.from_csv('/data/latest.csv', Analytics)  # Use ORM model

            # Create snapshot after load
            session.snapshots.create(
                name='post_load_snapshot',
                level=SnapshotLevel.DATABASE,
                database='analytics'
            )
            # All operations are atomic

        # ============================================================
        # Example 8: Vector Operations in Transaction
        # ============================================================
        with client.session() as session:
            # Create vector table
            session.create_table('documents', {
                'id': 'int primary key',
                'content': 'text',
                'embedding': 'vecf32(384)'
            })

            # Create vector index
            session.vector_ops.create_ivf(
                'documents',
                name='doc_idx',
                column='embedding',
                lists=100
            )

            # Insert vector data
            session.insert('documents', {
                'id': 1,
                'content': 'sample document',
                'embedding': [0.1] * 384
            })

    Best Practices:

    1. **Always use context manager**: Use `with client.session()` for automatic cleanup
    2. **Keep transactions short**: Long transactions can block other operations
    3. **Handle exceptions**: Wrap session code in try-except for proper error handling
    4. **Avoid nested transactions**: SQLAlchemy doesn't support true nested transactions
    5. **Use explicit commits**: When you need fine-grained control over transaction boundaries
    6. **Test rollback behavior**: Ensure your application handles rollbacks correctly

    Important Notes:

    - Session is created by `Client.session()` context manager
    - Don't instantiate Session directly
    - All manager operations within session are transaction-aware
    - Session automatically manages transaction lifecycle
    - Errors trigger automatic rollback
    - Normal exit triggers automatic commit

    See Also:

        - Client.session(): Creates and manages Session instances
        - AsyncSession: Async version for async/await workflows
        - SQLAlchemy Session: Parent class documentation
    """

    def __init__(self, bind=None, client=None, wrap_session=None, **kwargs):
        """
        Initialize MatrixOne Session.

        Args:
            bind: SQLAlchemy Engine or Connection to bind to
            client: MatrixOne Client instance
            wrap_session: Existing SQLAlchemy Session to wrap with MatrixOne features
            **kwargs: Additional arguments passed to SQLAlchemy Session

        Examples:
            # Create new session from engine
            session = Session(bind=engine, client=client)

            # Wrap existing SQLAlchemy session (for legacy projects)
            existing_session = sessionmaker(bind=engine)()
            mo_session = Session(client=client, wrap_session=existing_session)
        """
        if wrap_session is not None:
            # Wrap existing SQLAlchemy session with MatrixOne features
            # Copy the session's state to this instance
            self.__dict__.update(wrap_session.__dict__)
        else:
            # Initialize parent SQLAlchemy Session with engine/connection
            super().__init__(bind=bind, expire_on_commit=False, **kwargs)

        # Store MatrixOne client reference
        self.client = client

        # Import manager classes dynamically to avoid circular imports
        # These are defined in their respective modules
        from .snapshot import SnapshotManager
        from .clone import CloneManager

        # These are defined in client.py after the Session class
        import sys

        client_module = sys.modules.get('matrixone.client')
        if client_module:
            VectorManager = getattr(client_module, 'VectorManager')
            FulltextIndexManager = getattr(client_module, 'FulltextIndexManager')

        # These are defined in their respective modules
        from .metadata import MetadataManager
        from .load_data import LoadDataManager
        from .stage import StageManager
        from .cdc import CDCManager
        from .pitr import PitrManager
        from .pubsub import PubSubManager
        from .restore import RestoreManager
        from .account import AccountManager
        from .export import ExportManager

        # Create managers that use this session as executor
        # The executor pattern allows managers to work in both client and session contexts
        self.snapshots = SnapshotManager(client, executor=self)
        self.clone = CloneManager(client, executor=self)
        self.restore = RestoreManager(client, executor=self)
        self.pitr = PitrManager(client, executor=self)
        self.pubsub = PubSubManager(client, executor=self)
        self.account = AccountManager(client, executor=self)
        self.vector_ops = VectorManager(client, executor=self)
        self.fulltext_index = FulltextIndexManager(client, executor=self)
        self.metadata = MetadataManager(client, executor=self)
        self.load_data = LoadDataManager(client, executor=self)
        self.stage = StageManager(client, executor=self)
        self.cdc = CDCManager(client, executor=self)
        self.export = ExportManager(self)

    def execute(self, sql_or_stmt, params: Optional[Tuple] = None, **kwargs):
        """
        Execute SQL or SQLAlchemy statement within the current session transaction.

        This method extends SQLAlchemy's Session.execute() with MatrixOne-specific features:

        - Parameter substitution for string SQL using '?' placeholders
        - Integrated query logging with performance tracking
        - Support for both SQLAlchemy statements and raw SQL strings

        All queries executed within the session participate in the current transaction.
        Changes are committed when the session context exits normally, or rolled back on error.

        Args:
            sql_or_stmt (str | SQLAlchemy statement): The SQL query to execute. Can be:
                - String SQL with '?' placeholders for parameters
                - SQLAlchemy select() statement
                - SQLAlchemy insert() statement
                - SQLAlchemy update() statement
                - SQLAlchemy delete() statement
                - SQLAlchemy text() statement

            params (Optional[Tuple]): Query parameters for string SQL only. Values are
                substituted for '?' placeholders in order. Ignored for SQLAlchemy statements.

            **kwargs: Additional keyword arguments:

                - _log_mode (str): Override SQL logging mode for this query only.
                  Options: 'off', 'simple', 'full'. If not specified, uses client's
                  global sql_log_mode setting.
                - Other kwargs are passed to SQLAlchemy's execute()

        Returns:
            sqlalchemy.engine.Result: SQLAlchemy Result object with methods:
                - fetchall(): Get all rows as list of tuples
                - fetchone(): Get next row as tuple or None
                - fetchmany(size): Get next N rows
                - scalars(): Get first column of each row
                - mappings(): Get rows as dictionaries
                - rowcount: Number of rows affected (for INSERT/UPDATE/DELETE)

        Raises:
            QueryError: If query execution fails or SQL syntax is invalid
            ConnectionError: If session is not connected to database

        Examples::

            from matrixone import Client
            from sqlalchemy import select, insert, update, delete, and_, or_

            client = Client(host='localhost', port=6001, user='root', password='111', database='test')

            # ========================================
            # String SQL with Parameters
            # ========================================
            with client.session() as session:
                # Simple INSERT with parameters
                result = session.execute(
                    "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                    ("John Doe", "john@example.com", 30)
                )
                print(f"Inserted {result.rowcount} rows")

                # SELECT with parameters
                result = session.execute(
                    "SELECT * FROM users WHERE age > ? AND email LIKE ?",
                    (25, "%@example.com")
                )
                for row in result:
                    print(f"User: {row.name}, Age: {row.age}")

                # UPDATE with parameters
                result = session.execute(
                    "UPDATE users SET status = ? WHERE age < ?",
                    ("active", 18)
                )
                print(f"Updated {result.rowcount} rows")

                # DELETE with parameters
                result = session.execute(
                    "DELETE FROM users WHERE status = ?",
                    ("inactive",)
                )
                print(f"Deleted {result.rowcount} rows")

            # ========================================
            # SQLAlchemy SELECT Statements
            # ========================================
            with client.session() as session:
                # Basic SELECT
                stmt = select(User).where(User.age > 25)
                result = session.execute(stmt)
                users = result.scalars().all()  # Returns list of User objects

                # SELECT specific columns
                stmt = select(User.name, User.email).where(User.status == 'active')
                result = session.execute(stmt)
                for name, email in result:
                    print(f"{name}: {email}")

                # Complex WHERE with AND/OR
                stmt = select(User).where(
                    and_(
                        User.age > 18,
                        or_(
                            User.status == 'active',
                            User.status == 'pending'
                        )
                    )
                )
                result = session.execute(stmt)

                # SELECT with JOINs
                stmt = select(User, Order).join(Order, User.id == Order.user_id)
                result = session.execute(stmt)
                for user, order in result:
                    print(f"{user.name} ordered {order.amount}")

                # SELECT with aggregation
                from sqlalchemy import func
                stmt = select(func.count(User.id), func.avg(User.age))
                result = session.execute(stmt)
                count, avg_age = result.one()

            # ========================================
            # SQLAlchemy INSERT Statements
            # ========================================
            with client.session() as session:
                # Single INSERT
                stmt = insert(User).values(name='Alice', email='alice@example.com', age=28)
                result = session.execute(stmt)
                print(f"Inserted row with ID: {result.lastrowid}")

                # Bulk INSERT
                stmt = insert(User).values([
                    {'name': 'Bob', 'email': 'bob@example.com', 'age': 35},
                    {'name': 'Carol', 'email': 'carol@example.com', 'age': 42}
                ])
                result = session.execute(stmt)
                print(f"Inserted {result.rowcount} rows")

            # ========================================
            # SQLAlchemy UPDATE Statements
            # ========================================
            with client.session() as session:
                # Simple UPDATE
                stmt = update(User).where(User.id == 1).values(email='newemail@example.com')
                result = session.execute(stmt)
                print(f"Updated {result.rowcount} rows")

                # Conditional UPDATE
                stmt = update(User).where(User.age < 18).values(status='minor')
                result = session.execute(stmt)

                # UPDATE with expressions
                stmt = update(Order).values(total=Order.quantity * Order.price)
                result = session.execute(stmt)

            # ========================================
            # SQLAlchemy DELETE Statements
            # ========================================
            with client.session() as session:
                # Simple DELETE
                stmt = delete(User).where(User.id == 1)
                result = session.execute(stmt)

                # Conditional DELETE
                stmt = delete(User).where(User.status == 'deleted')
                result = session.execute(stmt)
                print(f"Deleted {result.rowcount} rows")

            # ========================================
            # Result Processing
            # ========================================
            with client.session() as session:
                # fetchall() - get all rows
                result = session.execute("SELECT * FROM users")
                rows = result.fetchall()
                for row in rows:
                    print(f"User: {row.name}")

                # fetchone() - get one row at a time
                result = session.execute("SELECT * FROM users")
                while row := result.fetchone():
                    print(f"User: {row.name}")

                # fetchmany() - get N rows
                result = session.execute("SELECT * FROM users")
                rows = result.fetchmany(10)  # Get 10 rows

                # scalars() - get first column
                stmt = select(User.name)
                result = session.execute(stmt)
                names = result.scalars().all()  # List of names

                # mappings() - get rows as dicts
                result = session.execute("SELECT name, email FROM users")
                for row in result.mappings():
                    print(f"{row['name']}: {row['email']}")

                # scalar() - get single value
                stmt = select(func.count(User.id))
                result = session.execute(stmt)
                count = result.scalar()  # Single integer value

            # ========================================
            # Query Logging Control
            # ========================================
            with client.session() as session:
                # Disable logging for this query only
                result = session.execute(
                    "SELECT * FROM large_table",
                    _log_mode='off'
                )

                # Force full SQL logging for debugging
                result = session.execute(
                    "SELECT * FROM users WHERE complex_condition",
                    _log_mode='full'
                )

                # Simple logging (show operation type only)
                result = session.execute(
                    "UPDATE massive_table SET field = 'value'",
                    _log_mode='simple'
                )

        Best Practices:
            - Use parameters (?-placeholders) to prevent SQL injection
            - Use SQLAlchemy statements for complex queries
            - Use string SQL for simple, dynamic queries
            - Always consume or close result sets
            - Use _log_mode='off' for frequently executed queries in production

        See Also:
            - AsyncSession.execute(): Async version
            - Client.execute(): Client-level execute without transaction
            - SQLAlchemy Result: Result object documentation
        """
        import time

        start_time = time.time()

        # Extract _log_mode from kwargs (don't pass it to SQLAlchemy)
        _log_mode = kwargs.pop('_log_mode', None)

        try:
            # Check if this is a string SQL
            if isinstance(sql_or_stmt, str):
                # String SQL - apply MatrixOne parameter substitution
                final_sql = self.client._substitute_parameters(sql_or_stmt, params)
                original_sql = sql_or_stmt

                from sqlalchemy import text

                # Call parent's execute() with text()
                result = super().execute(text(final_sql), **kwargs)
            else:
                # SQLAlchemy statement - call parent's execute() directly
                result = super().execute(sql_or_stmt, params, **kwargs)
                original_sql = f"<SQLAlchemy {type(sql_or_stmt).__name__}>"

            execution_time = time.time() - start_time

            # Log query
            if hasattr(result, 'returns_rows') and result.returns_rows:
                self.client.logger.log_query(
                    original_sql, execution_time, None, success=True, override_sql_log_mode=_log_mode
                )
            else:
                self.client.logger.log_query(
                    original_sql,
                    execution_time,
                    getattr(result, 'rowcount', 0),
                    success=True,
                    override_sql_log_mode=_log_mode,
                )

            return result

        except Exception as e:
            execution_time = time.time() - start_time
            self.client.logger.log_query(
                original_sql if 'original_sql' in locals() else str(sql_or_stmt),
                execution_time,
                success=False,
                override_sql_log_mode=_log_mode,
            )
            self.client.logger.log_error(e, context="Session query execution")
            raise QueryError(f"Session query execution failed: {e}")

    def insert(self, table_name: str, data: dict[str, Any]) -> "ResultSet":
        """
        Insert data into a table within session.

        Args::

            table_name: Name of the table
            data: Data to insert (dict with column names as keys)

        Returns::

            ResultSet object
        """
        sql = self.client._build_insert_sql(table_name, data)
        return self.execute(sql)

    def batch_insert(self, table_name: str, data_list: list[dict[str, Any]]) -> "ResultSet":
        """
        Batch insert data into a table within session.

        Args::

            table_name: Name of the table
            data_list: List of data dictionaries to insert

        Returns::

            ResultSet object
        """
        if not data_list:
            from .client import ResultSet

            return ResultSet([], [], affected_rows=0)

        sql = self.client._build_batch_insert_sql(table_name, data_list)
        return self.execute(sql)

    def query(self, *columns, snapshot: str = None):
        """Get MatrixOne query builder within session - SQLAlchemy style

        Args::

            *columns: Can be:
                - Single model class: query(Article) - returns all columns from model
                - Multiple columns: query(Article.id, Article.title) - returns specific columns
                - Mixed: query(Article, Article.id, some_expression.label('alias')) - model + additional columns
            snapshot: Optional snapshot name for snapshot queries

        Returns::

            MatrixOneQuery instance configured for the specified columns within session
        """
        from .orm import MatrixOneQuery

        if len(columns) == 1:
            # Traditional single model class usage
            column = columns[0]
            if isinstance(column, str):
                # String table name
                return MatrixOneQuery(column, self.client, transaction_wrapper=self, snapshot=snapshot)
            elif hasattr(column, '__tablename__'):
                # This is a model class
                return MatrixOneQuery(column, self.client, transaction_wrapper=self, snapshot=snapshot)
            elif hasattr(column, 'name') and hasattr(column, 'as_sql'):
                # This is a CTE object
                from .orm import CTE

                if isinstance(column, CTE):
                    query = MatrixOneQuery(None, self.client, transaction_wrapper=self, snapshot=snapshot)
                    query._table_name = column.name
                    query._select_columns = ["*"]  # Default to select all from CTE
                    query._ctes = [column]  # Add the CTE to the query
                    return query
            else:
                # This is a single column/expression - need to handle specially
                query = MatrixOneQuery(None, self.client, transaction_wrapper=self, snapshot=snapshot)
                query._select_columns = [column]
                # Try to infer table name from column
                if hasattr(column, 'table') and hasattr(column.table, 'name'):
                    query._table_name = column.table.name
                return query
        else:
            # Multiple columns/expressions
            model_class = None
            select_columns = []

            for column in columns:
                if hasattr(column, '__tablename__'):
                    # This is a model class - use its table
                    model_class = column
                else:
                    # This is a column or expression
                    select_columns.append(column)

            if model_class:
                query = MatrixOneQuery(model_class, self.client, transaction_wrapper=self, snapshot=snapshot)
                if select_columns:
                    # Add additional columns to the model's default columns
                    query._select_columns = select_columns
                return query
            else:
                # No model class provided, need to infer table from columns
                query = MatrixOneQuery(None, self.client, transaction_wrapper=self, snapshot=snapshot)
                query._select_columns = select_columns

                # Try to infer table name from first column that has table info
                for col in select_columns:
                    if hasattr(col, 'table') and hasattr(col.table, 'name'):
                        query._table_name = col.table.name
                        break
                    elif isinstance(col, str) and '.' in col:
                        # String column like "table.column" - extract table name
                        parts = col.split('.')
                        if len(parts) >= 2:
                            # For "db.table.column" format, use "db.table"
                            # For "table.column" format, use "table"
                            table_name = '.'.join(parts[:-1])
                            query._table_name = table_name
                            break

                return query

    def create_table(self, table_name: str, columns: dict, **kwargs) -> "Session":
        """
        Create a table within MatrixOne session.

        Args::

            table_name: Name of the table
            columns: Dictionary mapping column names to their types (same format as client.create_table)
            **kwargs: Additional table parameters

        Returns::

            Session: Self for chaining
        """
        from sqlalchemy.schema import CreateTable
        from .sqlalchemy_ext import VectorTableBuilder

        # Parse primary key from kwargs
        primary_key = kwargs.pop('primary_key', None)

        # Use VectorTableBuilder to create the table
        builder = VectorTableBuilder(table_name)

        # Add columns
        for col_name, col_type in columns.items():
            is_primary = primary_key == col_name if primary_key else False
            if is_primary:
                builder.add_column(col_name, col_type, primary_key=True)
            else:
                builder.add_column(col_name, col_type)

        # Build the table
        table = builder.build_table()

        # Generate CREATE TABLE SQL
        create_sql = CreateTable(table)
        sql = str(create_sql.compile(dialect=self.client.get_sqlalchemy_engine().dialect))
        self.execute(sql)

        return self

    def drop_table(self, table_name: str) -> "Session":
        """
        Drop a table within MatrixOne session.

        Args::

            table_name: Name of the table to drop

        Returns::

            Session: Self for chaining
        """
        sql = f"DROP TABLE IF EXISTS {table_name}"
        self.execute(sql)
        return self

    def create_table_with_index(self, table_name: str, columns: dict, indexes: list = None, **kwargs) -> "Session":
        """
        Create a table with vector indexes within MatrixOne session.

        Args::

            table_name: Name of the table
            columns: Dictionary mapping column names to their types (same format as client.create_table)
            indexes: List of index definitions (same format as client.create_table_with_index)
            **kwargs: Additional table parameters

        Returns::

            Session: Self for chaining
        """
        from sqlalchemy.schema import CreateTable
        from .sqlalchemy_ext import VectorTableBuilder

        # First create the table (same as create_table)
        primary_key = kwargs.pop('primary_key', None)
        builder = VectorTableBuilder(table_name)

        for col_name, col_type in columns.items():
            is_primary = primary_key == col_name if primary_key else False
            if is_primary:
                builder.add_column(col_name, col_type, primary_key=True)
            else:
                builder.add_column(col_name, col_type)

        table = builder.build_table()
        create_sql = CreateTable(table)
        sql = str(create_sql.compile(dialect=self.client.get_sqlalchemy_engine().dialect))
        self.execute(sql)

        # Then create the indexes if provided
        if indexes:
            for index_def in indexes:
                index_name = index_def.get('name')
                column_name = index_def.get('column')
                index_type = index_def.get('type', 'ivfflat')
                params = index_def.get('params', {})

                if index_type == 'ivfflat':
                    lists = params.get('lists', 100)
                    distance = params.get('distance', 'l2_distance')
                    self.vector_ops.create_ivf(
                        table_name, name=index_name, column=column_name, lists=lists, distance=distance
                    )
                elif index_type == 'hnsw':
                    m = params.get('M', 16)
                    ef_construction = params.get('ef_construction', 200)
                    distance = params.get('distance', 'l2_distance')
                    self.vector_ops.create_hnsw(
                        table_name,
                        name=index_name,
                        column=column_name,
                        m=m,
                        ef_construction=ef_construction,
                        distance=distance,
                    )
                else:
                    raise ValueError(f"Unsupported index type: {index_type}")

        return self

    def create_table_orm(self, table_name: str, *columns, **kwargs) -> "Session":
        """
        Create a table using SQLAlchemy ORM-style definitions within MatrixOne session.

        Args::

            table_name: Name of the table
            *columns: SQLAlchemy Column objects and Index objects (including VectorIndex)
            **kwargs: Additional parameters (like enable_hnsw, enable_ivf)

        Returns::

            Session: Self for chaining
        """
        from sqlalchemy import MetaData, Table
        from sqlalchemy.schema import CreateTable

        # Create metadata and table
        metadata = MetaData()

        # Build table with columns
        table = Table(table_name, metadata, *columns)

        # Generate CREATE TABLE SQL
        create_sql = CreateTable(table)
        sql = str(create_sql.compile(dialect=self.client.get_sqlalchemy_engine().dialect))
        self.execute(sql)

        return self


class AsyncSession(SQLAlchemyAsyncSession):
    """
    MatrixOne Async Session - Asynchronous transaction-aware session extending SQLAlchemy AsyncSession.

    This class provides async/await support for all Session functionality with full
    SQLAlchemy AsyncSession API compatibility, while adding MatrixOne-specific async
    managers for snapshots, clones, vector operations, and more.

    Key Features:

    - **Full async SQLAlchemy API**: All standard async Session methods (add, delete, execute, etc.)
    - **Non-blocking transactions**: Async transaction management with async/await
    - **Async MatrixOne managers**: All MatrixOne operations available asynchronously
    - **Concurrent operations**: Execute multiple operations concurrently with asyncio.gather
    - **Automatic lifecycle**: Context manager handles async transaction lifecycle
    - **Query logging**: Integrated async query logging

    Transaction Behavior:

    - **Auto-commit on success**: Transaction commits when async context manager exits normally
    - **Auto-rollback on error**: Transaction rolls back if any exception occurs
    - **Non-blocking**: All operations use async/await and don't block the event loop
    - **Isolation**: All operations within session are isolated from other transactions
    - **ACID compliance**: Full ACID guarantees for all async operations

    Available Managers (all async/transaction-aware):

    - **snapshots**: AsyncSnapshotManager for async snapshot operations
    - **clone**: AsyncCloneManager for async clone operations
    - **restore**: AsyncRestoreManager for async restore operations
    - **pitr**: AsyncPitrManager for async point-in-time recovery
    - **pubsub**: AsyncPubSubManager for async publish-subscribe
    - **account**: AsyncAccountManager for async account management
    - **vector_ops**: AsyncVectorManager for async vector operations
    - **fulltext_index**: AsyncFulltextIndexManager for async fulltext search
    - **metadata**: AsyncMetadataManager for async metadata analysis
    - **load_data**: AsyncLoadDataManager for async bulk loading
    - **stage**: AsyncStageManager for async stage management
    - **cdc**: AsyncCDCManager for async change data capture task operations

    Creating AsyncSession:

    There are two ways to create a MatrixOne AsyncSession:

    1. **New AsyncSession (Recommended)**: Create directly from async client::

        async with client.session() as session:
            # Your async operations here
            pass

    2. **Wrap Existing AsyncSession (For Legacy Projects)**: Wrap existing SQLAlchemy async session::

        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
        from matrixone.session import AsyncSession as MatrixOneAsyncSession

        # Your existing async SQLAlchemy code
        async_engine = create_async_engine('mysql+aiomysql://...')
        AsyncSessionFactory = async_sessionmaker(bind=async_engine)
        sqlalchemy_async_session = AsyncSessionFactory()

        # Wrap with MatrixOne features
        mo_async_session = MatrixOneAsyncSession(
            client=mo_async_client,
            wrap_session=sqlalchemy_async_session
        )
        # Now you can use both async SQLAlchemy and MatrixOne features

    Usage Examples::

        from matrixone import AsyncClient
        from sqlalchemy import select, insert, update, delete
        import asyncio

        async def main():
            client = AsyncClient()
            await client.connect(host='localhost', port=6001, user='root', password='111', database='test')

            # ========================================
            # Example 0: Wrapping Existing Async Session (Legacy Projects)
            # ========================================
            from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
            from matrixone.session import AsyncSession as MatrixOneAsyncSession

            # Your existing async SQLAlchemy setup
            async_engine = create_async_engine('mysql+aiomysql://root:111@127.0.0.1:6001/test')
            AsyncSessionFactory = async_sessionmaker(bind=async_engine)
            existing_async_session = AsyncSessionFactory()

            # Wrap it with MatrixOne features
            mo_async_session = MatrixOneAsyncSession(
                client=client,
                wrap_session=existing_async_session
            )

            try:
                # Standard async SQLAlchemy operations work
                result = await mo_async_session.execute("SELECT * FROM users")

                # MatrixOne async features now available
                await mo_async_session.stage.create_s3('backup', bucket='my-backup')
                await mo_async_session.snapshots.create('snapshot1', level='database')
                await mo_async_session.load_data.from_csv('/data/file.csv', 'users')

                await mo_async_session.commit()
            finally:
                await mo_async_session.close()

            # ========================================
            # Example 1: Basic Async Transaction
            # ========================================
            async with client.session() as session:
                # All operations are atomic
                await session.execute("INSERT INTO users (name) VALUES ('John')")
                await session.execute("INSERT INTO orders (user_id, amount) VALUES (1, 100)")
                # Transaction commits automatically

            # ========================================
            # Example 2: Async ORM Operations
            # ========================================
            async with client.session() as session:
                # Add objects
                user = User(name="Alice")
                session.add(user)

                # Query
                stmt = select(User).where(User.name == "Alice")
                result = await session.execute(stmt)
                users = result.scalars().all()

                # Commit
                await session.commit()

            # ========================================
            # Example 3: Concurrent Operations
            # ========================================
            async with client.session() as session:
                # Execute multiple queries concurrently
                results = await asyncio.gather(
                    session.execute("SELECT * FROM users"),
                    session.execute("SELECT * FROM orders"),
                    session.execute("SELECT * FROM products")
                )
                users, orders, products = results

            # ========================================
            # Example 4: Async MatrixOne Managers
            # ========================================
            async with client.session() as session:
                # Create snapshot
                snapshot = await session.snapshots.create(
                    'backup',
                    SnapshotLevel.DATABASE,
                    database='production'
                )

                # Clone database
                await session.clone.clone_database('prod_copy', 'production')

                # Load data
                await session.load_data.from_csv('/data/users.csv', 'users')

            # ========================================
            # Example 5: Error Handling
            # ========================================
            try:
                async with client.session() as session:
                    await session.execute("INSERT INTO users (name) VALUES ('Bob')")
                    await session.execute("INSERT INTO invalid_table (x) VALUES (1)")
                    # Automatically rolls back on error
            except Exception as e:
                print(f"Transaction rolled back: {e}")

            await client.disconnect()

        asyncio.run(main())

    Important Notes:

    - Use `async with client.session()` for async sessions
    - All execute operations must be awaited
    - All manager operations must be awaited
    - Perfect for FastAPI, aiohttp, and other async frameworks
    - Enables high-concurrency database operations

    See Also:

        - AsyncClient.session(): Creates AsyncSession instances
        - Session: Synchronous version
        - SQLAlchemy AsyncSession: Parent class documentation
    """

    def __init__(self, bind=None, client=None, wrap_session=None, **kwargs):
        """
        Initialize MatrixOne AsyncSession.

        Args:
            bind: SQLAlchemy AsyncEngine or AsyncConnection to bind to
            client: MatrixOne AsyncClient instance
            wrap_session: Existing SQLAlchemy AsyncSession to wrap with MatrixOne features
            **kwargs: Additional arguments passed to SQLAlchemy AsyncSession

        Examples:
            # Create new async session from engine
            session = AsyncSession(bind=async_engine, client=async_client)

            # Wrap existing SQLAlchemy async session (for legacy projects)
            existing_session = async_sessionmaker(bind=async_engine)()
            mo_session = AsyncSession(client=async_client, wrap_session=existing_session)
        """
        # Store references before calling super().__init__
        self.client = client

        if wrap_session is not None:
            # Wrap existing SQLAlchemy async session with MatrixOne features
            # Copy the session's state to this instance
            self.__dict__.update(wrap_session.__dict__)
        else:
            # Initialize parent SQLAlchemy AsyncSession with engine/connection
            super().__init__(bind=bind, expire_on_commit=False, **kwargs)

        # Import manager classes dynamically to avoid circular imports
        # Some are defined in async_client.py after the AsyncSession class

        # Import async vector and fulltext managers
        from .vector_manager import AsyncVectorManager
        from .fulltext_manager import AsyncFulltextIndexManager

        # These are defined in their respective modules
        from .snapshot import AsyncSnapshotManager
        from .clone import AsyncCloneManager
        from .restore import AsyncRestoreManager
        from .pitr import AsyncPitrManager
        from .pubsub import AsyncPubSubManager
        from .account import AsyncAccountManager
        from .load_data import AsyncLoadDataManager
        from .stage import AsyncStageManager
        from .cdc import AsyncCDCManager
        from .metadata import AsyncMetadataManager
        from .export import AsyncExportManager

        # Create managers that use this session as executor
        # The executor pattern allows managers to work in both client and session contexts
        self.snapshots = AsyncSnapshotManager(client, executor=self)
        self.clone = AsyncCloneManager(client, executor=self)
        self.restore = AsyncRestoreManager(client, executor=self)
        self.pitr = AsyncPitrManager(client, executor=self)
        self.pubsub = AsyncPubSubManager(client, executor=self)
        self.account = AsyncAccountManager(client, executor=self)
        self.vector_ops = AsyncVectorManager(client, executor=self)
        self.fulltext_index = AsyncFulltextIndexManager(client, executor=self)
        self.metadata = AsyncMetadataManager(client, executor=self)
        self.load_data = AsyncLoadDataManager(client, executor=self)
        self.stage = AsyncStageManager(client, executor=self)
        self.cdc = AsyncCDCManager(client, executor=self)
        self.export = AsyncExportManager(self)

    async def execute(self, sql_or_stmt, params: Optional[Tuple] = None, **kwargs):
        """
        Execute SQL or SQLAlchemy statement within async session.

        Supports:
        - String SQL with MatrixOne parameter substitution
        - SQLAlchemy statements (select, update, delete, insert, text)
        - Query logging

        Args:
            sql_or_stmt: SQL string or SQLAlchemy statement
            params: Query parameters (only used for string SQL with '?' placeholders)
            **kwargs: Additional execution options (including _log_mode for logging control)

        Returns:
            SQLAlchemy async result object
        """
        import time

        start_time = time.time()

        # Extract _log_mode from kwargs (don't pass it to SQLAlchemy)
        _log_mode = kwargs.pop('_log_mode', None)

        try:
            # Check if this is a string SQL
            if isinstance(sql_or_stmt, str):
                # String SQL - apply MatrixOne parameter substitution
                final_sql = self.client._substitute_parameters(sql_or_stmt, params)
                original_sql = sql_or_stmt

                from sqlalchemy import text

                # Call parent's execute() with text()
                result = await super().execute(text(final_sql), **kwargs)
            else:
                # SQLAlchemy statement - call parent's execute() directly
                result = await super().execute(sql_or_stmt, params, **kwargs)
                original_sql = f"<SQLAlchemy {type(sql_or_stmt).__name__}>"

            execution_time = time.time() - start_time

            # Log query
            if hasattr(result, 'returns_rows') and result.returns_rows:
                self.client.logger.log_query(
                    original_sql, execution_time, None, success=True, override_sql_log_mode=_log_mode
                )
            else:
                self.client.logger.log_query(
                    original_sql,
                    execution_time,
                    getattr(result, 'rowcount', 0),
                    success=True,
                    override_sql_log_mode=_log_mode,
                )

            return result

        except Exception as e:
            execution_time = time.time() - start_time
            self.client.logger.log_query(
                original_sql if 'original_sql' in locals() else str(sql_or_stmt),
                execution_time,
                success=False,
                override_sql_log_mode=_log_mode,
            )
            self.client.logger.log_error(e, context="Async session query execution")
            raise QueryError(f"Async session query execution failed: {e}")

    async def insert(self, table_name_or_model, data: dict):
        """
        Insert data into a table within session asynchronously.

        Args::

            table_name_or_model: Either a table name (str) or a SQLAlchemy model class
            data: Data to insert (dict with column names as keys)

        Returns::

            SQLAlchemy async result object
        """
        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            # It's a model class
            table_name = table_name_or_model.__tablename__
        else:
            # It's a table name string
            table_name = table_name_or_model

        sql = self.client._build_insert_sql(table_name, data)
        return await self.execute(sql)

    async def batch_insert(self, table_name_or_model, data_list: list):
        """
        Batch insert data into a table within session asynchronously.

        Args::

            table_name_or_model: Either a table name (str) or a SQLAlchemy model class
            data_list: List of data dictionaries to insert

        Returns::

            SQLAlchemy async result object
        """
        if not data_list:
            # Return empty AsyncResultSet for consistency with client behavior
            from .async_client import AsyncResultSet  # noqa: F811

            return AsyncResultSet([], [], affected_rows=0)

        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            # It's a model class
            table_name = table_name_or_model.__tablename__
        else:
            # It's a table name string
            table_name = table_name_or_model

        sql = self.client._build_batch_insert_sql(table_name, data_list)
        return await self.execute(sql)

    def query(self, *columns, snapshot: str = None):
        """Get async MatrixOne query builder within session - SQLAlchemy style

        Args::

            *columns: Can be:
                - Single model class: query(Article) - returns all columns from model
                - Multiple columns: query(Article.id, Article.title) - returns specific columns
                - Mixed: query(Article, Article.id, some_expression.label('alias')) - model + additional columns
            snapshot: Optional snapshot name for snapshot queries

        Returns::

            AsyncMatrixOneQuery instance configured for the specified columns within session
        """
        from .async_orm import AsyncMatrixOneQuery

        if len(columns) == 1:
            # Traditional single model class usage
            column = columns[0]
            if isinstance(column, str):
                # String table name
                return AsyncMatrixOneQuery(column, self.client, None, transaction_wrapper=self, snapshot=snapshot)
            elif hasattr(column, '__tablename__'):
                # This is a model class
                return AsyncMatrixOneQuery(column, self.client, None, transaction_wrapper=self, snapshot=snapshot)
            elif hasattr(column, 'name') and hasattr(column, 'as_sql'):
                # This is a CTE object
                from .orm import CTE

                if isinstance(column, CTE):
                    query = AsyncMatrixOneQuery(None, self.client, None, transaction_wrapper=self, snapshot=snapshot)
                    query._table_name = column.name
                    query._select_columns = ["*"]  # Default to select all from CTE
                    query._ctes = [column]  # Add the CTE to the query
                    return query
            else:
                # This is a single column/expression
                query = AsyncMatrixOneQuery(None, self.client, None, transaction_wrapper=self, snapshot=snapshot)
                query._select_columns = [column]
                # Try to infer table name from column
                if hasattr(column, 'table') and hasattr(column.table, 'name'):
                    query._table_name = column.table.name
                return query
        else:
            # Multiple columns/expressions
            model_class = None
            select_columns = []

            for column in columns:
                if hasattr(column, '__tablename__'):
                    # This is a model class - use its table
                    model_class = column
                else:
                    # This is a column or expression
                    select_columns.append(column)

            if model_class:
                query = AsyncMatrixOneQuery(model_class, self.client, None, transaction_wrapper=self, snapshot=snapshot)
                if select_columns:
                    # Add additional columns to the model's default columns
                    query._select_columns = select_columns
                return query
            else:
                # No model class provided, need to infer table from columns
                query = AsyncMatrixOneQuery(None, self.client, None, transaction_wrapper=self, snapshot=snapshot)
                query._select_columns = select_columns

                # Try to infer table name from first column that has table info
                for col in select_columns:
                    if hasattr(col, 'table') and hasattr(col.table, 'name'):
                        query._table_name = col.table.name
                        break
                    elif isinstance(col, str) and '.' in col:
                        # String column like "table.column" - extract table name
                        parts = col.split('.')
                        if len(parts) >= 2:
                            table_name = '.'.join(parts[:-1])
                            query._table_name = table_name
                            break

                return query

    async def create_table(self, table_name: str, columns: dict, **kwargs):
        """
        Create a table within MatrixOne async session.

        Args::

            table_name: Name of the table
            columns: Dictionary mapping column names to their types
            **kwargs: Additional table parameters

        Returns::

            Self for chaining
        """
        from sqlalchemy.schema import CreateTable
        from .sqlalchemy_ext import VectorTableBuilder

        primary_key = kwargs.pop('primary_key', None)
        builder = VectorTableBuilder(table_name)

        for col_name, col_type in columns.items():
            is_primary = primary_key == col_name if primary_key else False
            if is_primary:
                builder.add_column(col_name, col_type, primary_key=True)
            else:
                builder.add_column(col_name, col_type)

        table = builder.build_table()
        create_sql = CreateTable(table)
        sql = str(create_sql.compile(dialect=self.client.get_sqlalchemy_engine().dialect))
        await self.execute(sql)

        return self

    async def drop_table(self, table_name: str):
        """
        Drop a table within MatrixOne async session.

        Args::

            table_name: Name of the table to drop

        Returns::

            Self for chaining
        """
        sql = f"DROP TABLE IF EXISTS {table_name}"
        await self.execute(sql)
        return self
