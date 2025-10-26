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
    MatrixOne Session - extends SQLAlchemy Session with MatrixOne features.

    This class inherits from SQLAlchemy Session and provides a transaction context
    for executing multiple database operations atomically, while adding MatrixOne-specific
    capabilities like snapshots, clones, vector operations, and fulltext search.

    Key Features:

    - **Full SQLAlchemy Session API** - All inherited methods work as expected
    - Atomic transaction execution with automatic rollback on errors
    - Access to all MatrixOne managers within session context
    - Support for both SQLAlchemy statements and string SQL
    - Automatic commit/rollback handling

    Available Managers:
    - snapshots: SessionSnapshotManager for snapshot operations
    - clone: SessionCloneManager for clone operations
    - restore: SessionRestoreManager for restore operations
    - pitr: SessionPitrManager for point-in-time recovery
    - pubsub: SessionPubSubManager for pub/sub operations
    - account: SessionAccountManager for account operations
    - vector_ops: SessionVectorIndexManager for vector operations
    - fulltext_index: SessionFulltextIndexManager for fulltext operations

    Usage Examples

    .. code-block:: python

            # Standard SQLAlchemy usage
            from sqlalchemy import select
            with client.session() as session:
                # Execute SQLAlchemy statements
                stmt = select(User).where(User.age > 25)
                result = session.execute(stmt)
                users = result.scalars().all()  # Returns ORM objects

                # ORM operations
                user = User(name="John", age=30)
                session.add(user)
                session.commit()

            # MatrixOne features
            with client.session() as session:
                # Snapshot operations
                session.snapshots.create("backup", SnapshotLevel.DATABASE, database="mydb")

                # Clone operations
                session.clone.clone_database("new_db", "source_db")

    Note: This class is automatically created by the Client's session()
    context manager and should not be instantiated directly.
    """

    def __init__(self, connection, client):
        # Initialize parent SQLAlchemy Session
        super().__init__(bind=connection, expire_on_commit=False)

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
        from .pitr import PitrManager
        from .pubsub import PubSubManager
        from .restore import RestoreManager
        from .account import AccountManager

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

    def execute(self, sql_or_stmt, params: Optional[Tuple] = None, **kwargs):
        """
        Execute SQL or SQLAlchemy statement within session.

        Overrides SQLAlchemy Session.execute() to add:
        - Support for string SQL with MatrixOne parameter substitution
        - Query logging

        Args:
            sql_or_stmt: SQL string or SQLAlchemy statement (select, update, delete, insert, text)
            params: Query parameters (only used for string SQL with '?' placeholders)
            **kwargs: Additional arguments passed to parent execute()

        Returns:
            sqlalchemy.engine.Result: SQLAlchemy Result object

        Examples::

            # SQLAlchemy statements
            from sqlalchemy import select, update
            with client.session() as session:
                stmt = select(User).where(User.age > 25)
                result = session.execute(stmt)
                users = result.scalars().all()

            # String SQL (MatrixOne extension)
            with client.session() as session:
                result = session.execute("INSERT INTO users (name) VALUES (?)", ("John",))
                print(f"Inserted {result.rowcount} rows")
        """
        import time

        start_time = time.time()

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
                self.client.logger.log_query(original_sql, execution_time, None, success=True)
            else:
                self.client.logger.log_query(original_sql, execution_time, getattr(result, 'rowcount', 0), success=True)

            return result

        except Exception as e:
            execution_time = time.time() - start_time
            self.client.logger.log_query(
                original_sql if 'original_sql' in locals() else str(sql_or_stmt), execution_time, success=False
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
    MatrixOne Async Session - extends SQLAlchemy AsyncSession with MatrixOne features.

    This class inherits from SQLAlchemy AsyncSession and provides:
    - Full SQLAlchemy AsyncSession API
    - MatrixOne-specific managers (snapshots, clone, vector_ops, etc.)
    - Enhanced execute() with MatrixOne parameter substitution
    """

    def __init__(self, connection, client):
        # Store references before calling super().__init__
        self.client = client
        self._connection = connection

        # Initialize parent SQLAlchemy AsyncSession
        super().__init__(bind=connection, expire_on_commit=False)

        # Import manager classes dynamically to avoid circular imports
        # Some are defined in async_client.py after the AsyncSession class

        # Import async vector and fulltext managers
        from .vector_manager import AsyncVectorManager
        from .async_client import AsyncTransactionFulltextIndexManager

        # These are defined in their respective modules
        from .snapshot import AsyncSnapshotManager
        from .clone import AsyncCloneManager
        from .restore import AsyncRestoreManager
        from .pitr import AsyncPitrManager
        from .pubsub import AsyncPubSubManager
        from .account import AsyncAccountManager
        from .load_data import AsyncLoadDataManager
        from .stage import AsyncStageManager
        from .metadata import AsyncMetadataManager

        # Create managers that use this session as executor
        # The executor pattern allows managers to work in both client and session contexts
        self.snapshots = AsyncSnapshotManager(client, executor=self)
        self.clone = AsyncCloneManager(client, executor=self)
        self.restore = AsyncRestoreManager(client, executor=self)
        self.pitr = AsyncPitrManager(client, executor=self)
        self.pubsub = AsyncPubSubManager(client, executor=self)
        self.account = AsyncAccountManager(client, executor=self)
        self.vector_ops = AsyncVectorManager(client, executor=self)
        self.fulltext_index = AsyncTransactionFulltextIndexManager(client, self)
        self.metadata = AsyncMetadataManager(client, executor=self)
        self.load_data = AsyncLoadDataManager(client, executor=self)
        self.stage = AsyncStageManager(client, executor=self)

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
            **kwargs: Additional execution options

        Returns:
            SQLAlchemy async result object
        """
        import time

        start_time = time.time()

        try:
            # Check if this is a string SQL
            if isinstance(sql_or_stmt, str):
                # String SQL - apply MatrixOne parameter substitution
                final_sql = self.client._substitute_parameters(sql_or_stmt, params)
                original_sql = sql_or_stmt

                from sqlalchemy import text

                # Execute using the stored _connection to avoid greenlet issues
                result = await self._connection.execute(text(final_sql), **kwargs)
            else:
                # SQLAlchemy statement - call parent's execute() directly
                result = await super().execute(sql_or_stmt, params, **kwargs)
                original_sql = f"<SQLAlchemy {type(sql_or_stmt).__name__}>"

            execution_time = time.time() - start_time

            # Log query
            if hasattr(result, 'returns_rows') and result.returns_rows:
                self.client.logger.log_query(original_sql, execution_time, None, success=True)
            else:
                self.client.logger.log_query(original_sql, execution_time, getattr(result, 'rowcount', 0), success=True)

            return result

        except Exception as e:
            execution_time = time.time() - start_time
            self.client.logger.log_query(
                original_sql if 'original_sql' in locals() else str(sql_or_stmt), execution_time, success=False
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
