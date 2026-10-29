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

from __future__ import annotations

from ._telemetry import observe, pool_wait
from ._utils import result_returns_rows

from typing import TYPE_CHECKING, Any, Optional, Tuple

try:
    from sqlalchemy.orm import Session as SQLAlchemySession
except ImportError:
    SQLAlchemySession = None

try:
    from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession
except ImportError:
    SQLAlchemyAsyncSession = None

if TYPE_CHECKING:
    from .async_client import AsyncResultSet  # noqa: F401
    from .client import ResultSet  # noqa: F401


class Session(SQLAlchemySession):
    """Synchronous SQLAlchemy session with MatrixOne managers.

    Use ``client.session()`` to begin a server transaction before the first
    operation. The context commits on success, rolls back on errors (including
    cancellation), and closes the session. Explicit commits end that transaction;
    further SQL follows SQLAlchemy's transaction rules.

    A session belongs to one task. Await operations sequentially within an
    AsyncSession. Parallel transactions require separate sessions, each owned
    by its own task; they do not commit atomically together.

    Example::

        with client.session() as session:
            result = session.execute("SELECT * FROM users")
            session.execute("UPDATE users SET active = 1 WHERE id = 1")

    Managers execute through the session's connection. Server transaction
    restrictions still apply: manager availability does not make every DDL or
    administrative command transactional. Create snapshots through
    ``client.snapshots`` outside the transaction. CLONE and DML rollback are
    covered by the supported-server contract suite.

    Directly constructed or wrapped sessions retain their caller's transaction
    ownership; use the client context for explicit server BEGIN and cleanup.
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
        # These are defined in client.py after the Session class
        import sys

        from .branch import BranchManager
        from .clone import CloneManager
        from .snapshot import SnapshotManager

        client_module = sys.modules.get('matrixone.client')
        if client_module:
            VectorManager = getattr(client_module, 'VectorManager')
            FulltextIndexManager = getattr(client_module, 'FulltextIndexManager')

        # These are defined in their respective modules
        from .account import AccountManager
        from .cdc import CDCManager
        from .export import ExportManager
        from .load_data import LoadDataManager
        from .metadata import MetadataManager
        from .pitr import PitrManager
        from .pubsub import PubSubManager
        from .restore import RestoreManager
        from .stage import StageManager

        # Create managers that use this session as executor
        # The executor pattern allows managers to work in both client and session contexts
        self.snapshots = SnapshotManager(client, executor=self)
        self.clone = CloneManager(client, executor=self)
        self.branch = BranchManager(client, executor=self)
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

    @observe('session')
    def execute_driver(self, sql, params=None):
        """Driver-bound SQL on this session; the result belongs to the session."""
        if not isinstance(sql, str):
            raise TypeError('execute_driver requires SQL text')
        try:
            return self.connection().exec_driver_sql(sql, params, execution_options={'no_parameters': params is None})
        except Exception as error:
            from .client import _classify_db_error

            classified = _classify_db_error(error, sql)
            if classified is error:
                raise
            raise classified from error

    @observe('session')
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

                - log_mode (str): Override SQL logging mode for this query only.
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
                    log_mode='off'
                )

                # Force full SQL logging for debugging
                result = session.execute(
                    "SELECT * FROM users WHERE complex_condition",
                    log_mode='full'
                )

                # Simple logging (show operation type only)
                result = session.execute(
                    "UPDATE massive_table SET field = 'value'",
                    log_mode='simple'
                )

        Best Practices:
            - Use parameters (?-placeholders) to prevent SQL injection
            - Use SQLAlchemy statements for complex queries
            - Use string SQL for simple, dynamic queries
            - Always consume or close result sets
            - Use log_mode='off' for frequently executed queries in production

        See Also:
            - AsyncSession.execute(): Async version
            - Client.execute(): Client-level execute without transaction
            - SQLAlchemy Result: Result object documentation
        """
        import time

        start_time = time.time()

        # Extract log_mode from kwargs (don't pass it to SQLAlchemy)
        log_mode = kwargs.pop('log_mode', None)

        try:
            # Check if this is a string SQL
            if isinstance(sql_or_stmt, str):
                # String SQL - apply MatrixOne parameter substitution
                original_sql = sql_or_stmt

                from ._session_execution import execute_session_sql

                result = execute_session_sql(super().execute, sql_or_stmt, client=self.client, params=params, **kwargs)
            else:
                # SQLAlchemy statement - call parent's execute() directly
                result = super().execute(sql_or_stmt, params, **kwargs)
                original_sql = f"<SQLAlchemy {type(sql_or_stmt).__name__}>"

            execution_time = time.time() - start_time

            # Log query
            if result_returns_rows(result):
                self.client.logger.log_query(original_sql, execution_time, None, success=True, log_mode=log_mode)
            else:
                self.client.logger.log_query(
                    original_sql,
                    execution_time,
                    getattr(result, 'rowcount', 0),
                    success=True,
                    log_mode=log_mode,
                )

            return result

        except Exception as e:
            execution_time = time.time() - start_time
            self.client.logger.log_query(
                original_sql if 'original_sql' in locals() else str(sql_or_stmt),
                execution_time,
                success=False,
                log_mode=log_mode,
            )
            self.client.logger.log_error(e, context="Session query execution")
            from .client import _classify_db_error

            classified = _classify_db_error(e, sql_or_stmt)
            if classified is e:
                raise
            raise classified from e

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
        from .orm import create_query
        from .orm import MatrixOneQuery

        return create_query(MatrixOneQuery, self.client, columns, snapshot=snapshot, transaction_wrapper=self)

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
        from .sqlalchemy_ext.table_builder import simplified_table
        from sqlalchemy.schema import CreateTable

        table = simplified_table(table_name, columns, **kwargs)
        self.execute(CreateTable(table))
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
        self.create_table(table_name, columns, **kwargs)

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
    """Asynchronous SQLAlchemy session with MatrixOne managers.

    Use ``client.session()`` to begin a server transaction before the first
    operation. The context commits on success, rolls back on errors (including
    cancellation), and closes the session. Explicit commits end that transaction;
    further SQL follows SQLAlchemy's transaction rules.

    A session belongs to one task. Await operations sequentially within an
    AsyncSession. Parallel transactions require separate sessions, each owned
    by its own task; they do not commit atomically together.

    Example::

        async with client.session() as session:
            result = await session.execute("SELECT * FROM users")
            await session.execute("UPDATE users SET active = 1 WHERE id = 1")

    Managers execute through the session's connection. Server transaction
    restrictions still apply: manager availability does not make every DDL or
    administrative command transactional. Create snapshots through
    ``client.snapshots`` outside the transaction. CLONE and DML rollback are
    covered by the supported-server contract suite.

    Directly constructed or wrapped sessions retain their caller's transaction
    ownership; use the client context for explicit server BEGIN and cleanup.
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
        from .account import AsyncAccountManager
        from .branch import AsyncBranchManager
        from .cdc import AsyncCDCManager
        from .clone import AsyncCloneManager
        from .export import AsyncExportManager
        from .fulltext_manager import AsyncFulltextIndexManager
        from .load_data import AsyncLoadDataManager
        from .metadata import AsyncMetadataManager
        from .pitr import AsyncPitrManager
        from .pubsub import AsyncPubSubManager
        from .restore import AsyncRestoreManager

        # These are defined in their respective modules
        from .snapshot import AsyncSnapshotManager
        from .stage import AsyncStageManager
        from .vector_manager import AsyncVectorManager

        # Create managers that use this session as executor
        # The executor pattern allows managers to work in both client and session contexts
        self.snapshots = AsyncSnapshotManager(client, executor=self)
        self.clone = AsyncCloneManager(client, executor=self)
        self.branch = AsyncBranchManager(client, executor=self)
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

    @observe('session')
    async def execute_driver(self, sql, params=None):
        """Driver-bound SQL on this session; the result belongs to the session."""
        if not isinstance(sql, str):
            raise TypeError('execute_driver requires SQL text')
        try:
            connection = await self.connection()
            return await connection.exec_driver_sql(sql, params, execution_options={'no_parameters': params is None})
        except Exception as error:
            from .client import _classify_db_error

            classified = _classify_db_error(error, sql)
            if classified is error:
                raise
            raise classified from error

    @observe('session')
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
            **kwargs: Additional execution options (including log_mode for logging control)

        Returns:
            SQLAlchemy async result object
        """
        import time

        start_time = time.time()

        # Extract log_mode from kwargs (don't pass it to SQLAlchemy)
        log_mode = kwargs.pop('log_mode', None)

        try:
            # Check if this is a string SQL
            if isinstance(sql_or_stmt, str):
                # String SQL - apply MatrixOne parameter substitution
                original_sql = sql_or_stmt

                from ._session_execution import execute_session_sql

                result = await execute_session_sql(
                    super().execute, sql_or_stmt, client=self.client, params=params, **kwargs
                )
            else:
                # SQLAlchemy statement - call parent's execute() directly
                result = await super().execute(sql_or_stmt, params, **kwargs)
                original_sql = f"<SQLAlchemy {type(sql_or_stmt).__name__}>"

            execution_time = time.time() - start_time

            # Log query
            if result_returns_rows(result):
                self.client.logger.log_query(original_sql, execution_time, None, success=True, log_mode=log_mode)
            else:
                self.client.logger.log_query(
                    original_sql,
                    execution_time,
                    getattr(result, 'rowcount', 0),
                    success=True,
                    log_mode=log_mode,
                )

            return result

        except Exception as e:
            execution_time = time.time() - start_time
            self.client.logger.log_query(
                original_sql if 'original_sql' in locals() else str(sql_or_stmt),
                execution_time,
                success=False,
                log_mode=log_mode,
            )
            self.client.logger.log_error(e, context="Async session query execution")
            from .client import _classify_db_error

            classified = _classify_db_error(e, sql_or_stmt)
            if classified is e:
                raise
            raise classified from e

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
        from .orm import create_query
        from .async_orm import AsyncMatrixOneQuery

        return create_query(AsyncMatrixOneQuery, self.client, columns, snapshot=snapshot, transaction_wrapper=self)

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
        from .sqlalchemy_ext.table_builder import simplified_table
        from sqlalchemy.schema import CreateTable

        table = simplified_table(table_name, columns, **kwargs)
        await self.execute(CreateTable(table))
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
