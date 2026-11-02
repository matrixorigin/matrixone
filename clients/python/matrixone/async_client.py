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
MatrixOne Async Client - Asynchronous implementation
"""

from __future__ import annotations

import operator

try:
    import aiomysql
except ImportError:
    aiomysql = None

try:
    from sqlalchemy import text
    from sqlalchemy.engine import URL
    from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
except ImportError:
    create_async_engine = None
    AsyncEngine = None
    text = None
    URL = None

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from ._telemetry import observe, pool_wait
from ._utils import discard_unrestored_connection, is_non_transactional_statement, result_returns_rows
from .restore import AsyncRestoreManager
from .account import AsyncAccountManager
from .base_client import BaseMatrixOneClient, BaseMatrixOneExecutor
from .branch import AsyncBranchManager
from .cdc import AsyncCDCManager
from .clone import AsyncCloneManager
from .connection_hooks import ConnectionAction, ConnectionHook, create_connection_hook
from .exceptions import (
    ConnectionError,
    MoCtlError,
    QueryError,
    RestoreError,
    SnapshotError,
)
from .fulltext_manager import AsyncFulltextIndexManager
from .load_data import AsyncLoadDataManager
from .logger import MatrixOneLogger, create_default_logger
from .metadata import AsyncMetadataManager
from .pitr import AsyncPitrManager
from .pubsub import AsyncPubSubManager
from .session import AsyncSession
from .snapshot import Snapshot, SnapshotLevel, AsyncSnapshotManager
from .stage import AsyncStageManager


class AsyncResultSet:
    """Async result set wrapper for query results"""

    def __init__(self, columns: List[str], rows: List[Tuple], affected_rows: int = 0):
        self.columns = columns
        self.rows = rows
        self.affected_rows = affected_rows
        self._cursor = 0  # Track current position in result set

    def fetchall(self) -> List[Tuple]:
        """Fetch all remaining rows"""
        remaining_rows = self.rows[self._cursor :]
        self._cursor = len(self.rows)
        return remaining_rows

    def fetchone(self) -> Optional[Tuple]:
        """Fetch one row"""
        if self._cursor < len(self.rows):
            row = self.rows[self._cursor]
            self._cursor += 1
            return row
        return None

    def fetchmany(self, size: int = 1) -> List[Tuple]:
        """Fetch many rows"""
        size = operator.index(size)
        if size < 0:
            raise ValueError('size must be non-negative')
        start = self._cursor
        end = min(start + size, len(self.rows))
        rows = self.rows[start:end]
        self._cursor = end
        return rows

    def scalar(self) -> Any:
        """Get scalar value (first column of first row)"""
        if self.rows and self.columns:
            return self.rows[0][0]
        return None

    def keys(self):
        """Get column names"""
        return iter(self.columns)

    def __iter__(self):
        return iter(self.rows)

    def __len__(self):
        return len(self.rows)


class AsyncMoCtlManager:
    """Async mo_ctl manager"""

    def __init__(self, client):
        self.client = client

    async def _execute_moctl(self, method: str, target: str, params: str = "") -> Dict[str, Any]:
        """Execute mo_ctl command asynchronously"""
        try:
            from .moctl import _build_moctl_sql, _decode_moctl_result

            sql = _build_moctl_sql(method, target, params)

            # Execute the command
            result = await self.client.execute(sql)

            if not result.rows:
                raise MoCtlError(f"mo_ctl command returned no results: {sql}")

            # Parse the JSON result
            result_str = result.rows[0][0]
            parsed_result = _decode_moctl_result(result_str, MoCtlError)

            # Check for errors in the result
            if "result" in parsed_result and parsed_result["result"]:
                first_result = parsed_result["result"][0]
                if "returnStr" in first_result and first_result["returnStr"] != "OK":
                    raise MoCtlError(f"mo_ctl operation failed: {first_result['returnStr']}")

            return parsed_result

        except MoCtlError:
            raise
        except Exception as e:
            raise MoCtlError(f"mo_ctl operation failed: {e}") from e

    async def flush_table(self, database: str, table: str) -> Dict[str, Any]:
        """Force flush table asynchronously"""
        table_ref = f"{database}.{table}"
        return await self._execute_moctl("dn", "flush", table_ref)

    async def increment_checkpoint(self) -> Dict[str, Any]:
        """Force incremental checkpoint asynchronously"""
        return await self._execute_moctl("dn", "checkpoint", "")

    async def global_checkpoint(self) -> Dict[str, Any]:
        """Force global checkpoint asynchronously"""
        return await self._execute_moctl("dn", "globalcheckpoint", "")


class AsyncClientExecutor(BaseMatrixOneExecutor):
    """Async client executor that uses AsyncClient's execute method"""

    def __init__(self, client):
        super().__init__(client)
        self.client = client

    async def _execute(self, sql: str):
        return await self.client.execute(sql)

    def _get_empty_result(self):
        return AsyncResultSet([], [], affected_rows=0)

    async def insert(self, table_name: str, data: dict):
        """Async insert method"""
        sql = self.base_client._build_insert_sql(table_name, data)
        return await self._execute(sql)

    async def batch_insert(self, table_name: str, data_list: list):
        """Async batch insert method"""
        if not data_list:
            return self._get_empty_result()

        sql = self.base_client._build_batch_insert_sql(table_name, data_list)
        return await self._execute(sql)


class AsyncClient(BaseMatrixOneClient):
    """
    MatrixOne Async Client - Asynchronous interface for MatrixOne database operations.

    This class provides a comprehensive asynchronous interface for connecting to and
    interacting with MatrixOne databases. It supports modern async/await patterns
    including table creation, data insertion, querying, vector operations, and
    transaction management.

    Key Features:

    - Asynchronous connection management with connection pooling
    - High-level table operations (create_table, drop_table, insert, batch_insert)
    - Query builder interface for complex async queries
    - Vector operations (similarity search, range search, indexing)
    - Async transaction management with context managers
    - Snapshot and restore operations
    - Account and user management
    - Fulltext search capabilities
    - Non-blocking I/O operations

    Supported Operations:

    - Async connection and disconnection
    - Async query execution (SELECT, INSERT, UPDATE, DELETE)
    - Async batch operations
    - Async transaction management
    - Async table creation and management
    - Async vector and fulltext operations
    - Async snapshot and restore operations

    Usage Examples::

        Basic async usage::

            async def main():
                client = AsyncClient()
                await client.connect('localhost', 6001, 'root', '111', 'test')

                # Create table using high-level API
                await client.create_table("users", {
                    "id": "int primary key",
                    "name": "varchar(100)",
                    "email": "varchar(255)"
                })

                # Insert data
                await client.insert("users", {"id": 1, "name": "John", "email": "john@example.com"})

                # Query data
                result = await client.query("users").where("id = ?", 1).all()
                print(result.rows)

                await client.disconnect()

        Vector operations::

            async def vector_example():
                client = AsyncClient()
                await client.connect('localhost', 6001, 'root', '111', 'test')

                # Create vector table
                await client.create_table("documents", {
                    "id": "int primary key",
                    "content": "text",
                    "embedding": "vecf32(384)"
                })

                # Vector similarity search
                results = await client.vector_ops.similarity_search(
                    "documents",
                    vector_column="embedding",
                    query_vector=[0.1, 0.2, 0.3, ...],  # 384-dimensional vector
                    limit=10,
                    distance_type="l2"
                )

                await client.disconnect()

        Async transaction usage::

            async def transaction_example():
                client = AsyncClient()
                await client.connect('localhost', 6001, 'root', '111', 'test')

                async with client.transaction() as tx:
                    await tx.execute("INSERT INTO users (name) VALUES (?)", ("John",))
                await tx.execute("INSERT INTO orders (user_id, amount) VALUES (?, ?)", (1, 100.0))
                # Transaction commits automatically on success

    Note: This class requires asyncio and async database drivers. Use the synchronous Client class
    for blocking operations or when async support is not needed.
    """

    def __init__(
        self,
        connection_timeout: int = 30,
        query_timeout: int = 300,
        auto_commit: bool = True,
        charset: str = "utf8mb4",
        logger: Optional[MatrixOneLogger] = None,
        sql_log_mode: str = "off",
        slow_query_threshold: float = 1.0,
        max_sql_display_length: int = 500,
        pool_size: int = 5,
        max_overflow: int = 10,
        pool_timeout: float = 30,
        pool_recycle: int = 3600,
    ):
        """
        Initialize MatrixOne async client

        Args::

            connection_timeout: Connection timeout in seconds
            query_timeout: Query timeout in seconds
            auto_commit: Enable auto-commit mode
            charset: Character set for connection
            logger: Custom logger instance. If None, creates a default logger
            sql_log_mode: SQL logging mode ('off', 'auto', 'simple', 'full')
                - 'off': No SQL logging (default)
                - 'auto': Smart logging - short SQL shown fully, long SQL summarized (opt-in)
                - 'simple': Show operation summary only
                - 'full': Show complete SQL regardless of length
            slow_query_threshold: Threshold in seconds for slow query warnings (default: 1.0)
            max_sql_display_length: Maximum SQL length in auto mode before summarizing (default: 500)
        """
        self._reset_compatibility()
        from ._connection_options import positive_timeout

        for name, value, minimum in [
            ('pool_size', pool_size, 0),
            ('max_overflow', max_overflow, -1),
            ('pool_recycle', pool_recycle, -1),
        ]:
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError('%s must be an integer >= %s' % (name, minimum))
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = positive_timeout(pool_timeout, 'pool_timeout')
        self.pool_recycle = pool_recycle
        self.connection_timeout = connection_timeout
        self.query_timeout = query_timeout
        self.auto_commit = auto_commit
        self.charset = charset

        # Initialize logger
        if logger is not None:
            self.logger = logger
        else:
            self.logger = create_default_logger(
                sql_log_mode=sql_log_mode,
                slow_query_threshold=slow_query_threshold,
                max_sql_display_length=max_sql_display_length,
            )

        # Connection management - using SQLAlchemy async engine instead of direct aiomysql connection
        self._engine = None
        self._connection = None  # Keep for backward compatibility, but will be managed by engine
        self._connection_params = {}
        self._login_info = None

        # Initialize managers
        self._snapshots = AsyncSnapshotManager(self)
        self._clone = AsyncCloneManager(self)
        self._branch = AsyncBranchManager(self)
        self._moctl = AsyncMoCtlManager(self)
        self._restore = AsyncRestoreManager(self)
        self._pitr = AsyncPitrManager(self)
        self._pubsub = AsyncPubSubManager(self)
        self._account = AsyncAccountManager(self)
        self._fulltext_index = None
        self._metadata = None
        self._load_data = None
        self._export = None
        self._stage = None
        self._cdc = None
        self._analytics = None

    @property
    def analytics(self):
        """Async file relations and bounded native Arrow streaming/upload."""
        if self._analytics is None:
            from .async_analytics import AsyncAnalyticsManager

            self._analytics = AsyncAnalyticsManager(self)
        return self._analytics

    async def get_capabilities(self, refresh=False):
        """Observe this server/session without enabling features or mutating data."""
        from .connection_hooks import assert_not_in_hook

        assert_not_in_hook(self)
        from .capabilities import get_capabilities_async

        import asyncio
        from ._connection_options import positive_timeout

        timeout = positive_timeout(self.query_timeout, 'query_timeout')
        return await asyncio.wait_for(get_capabilities_async(self, refresh), timeout)

    async def connect(
        self,
        *,
        host: str = "localhost",
        port: int = 6001,
        user: str = "root",
        password: str = "111",
        database: str,
        account: Optional[str] = None,
        role: Optional[str] = None,
        charset: str = "utf8mb4",
        connection_timeout: Optional[int] = None,
        auto_commit: bool = True,
        on_connect: Optional[Union[ConnectionHook, List[Union[ConnectionAction, str]], Callable]] = None,
        capability_overrides: Optional[dict] = None,
        local_infile: Optional[bool] = None,
    ):
        """
        Connect to MatrixOne database asynchronously

        Args::

            host: Database host
            port: Database port
            user: Username or login info in format "user", "account#user", "account#user#role",
                  "account:user", or "account:user:role" (both '#' and ':' separators are supported)
            password: Password
            database: Database name
            account: Optional account name (will be combined with user if user doesn't contain '#' or ':')
            role: Optional role name (will be combined with user if user doesn't contain '#' or ':')
            charset: Character set for the connection (default: utf8mb4)
            connection_timeout: Override constructor connection timeout; None inherits it
            auto_commit: Enable autocommit (default: True)
            on_connect: Connection hook to execute after successful connection.
                       Can be:
                       - ConnectionHook instance
                       - List of ConnectionAction or string action names
                       - Custom callback function (async or sync)

        Examples::

            # Enable all features after connection
            await client.connect(host, port, user, password, database,
                               on_connect=[ConnectionAction.ENABLE_ALL])

            # Enable only vector operations with custom charset
            await client.connect(host, port, user, password, database,
                               charset="utf8mb4",
                               on_connect=[ConnectionAction.ENABLE_VECTOR])

            # Custom async callback
            async def my_callback(client):
                print(f"Connected to {client._connection_params['host']}")

            await client.connect(host, port, user, password, database,
                               on_connect=my_callback)
        """
        if local_infile is not None and not isinstance(local_infile, bool):
            raise TypeError('local_infile must be None or a boolean')
        from .connection_hooks import assert_not_in_hook

        assert_not_in_hook(self)
        from .capabilities import validate_overrides

        overrides = validate_overrides(capability_overrides)
        from ._connection_options import positive_timeout

        positive_timeout(self.query_timeout, 'query_timeout')
        timeout = self.connection_timeout if connection_timeout is None else connection_timeout
        positive_timeout(timeout, 'connection_timeout')
        if self._engine is not None:
            await self.disconnect()
        if self._analytics is not None:
            self._analytics.reconnect()
        self._reset_compatibility(overrides)
        self.connection_timeout = timeout
        try:
            # Build final login info based on user parameter and optional account/role
            final_user, parsed_info = self._build_login_info(user, account, role)

            # Store parsed info for later use
            self._login_info = parsed_info

            # Store connection parameters for engine creation
            self._connection_params = {
                "host": host,
                "port": port,
                "user": final_user,
                "password": password,
                "database": database,
                "charset": charset,
                "autocommit": auto_commit,
                "local_infile": local_infile,
                "connect_timeout": timeout,
            }

            # Create SQLAlchemy async engine instead of direct aiomysql connection
            self._engine = self._create_async_engine()
            if on_connect is None:
                on_connect = [ConnectionAction.ENABLE_ALL]
            if on_connect:
                self._setup_connection_hook(on_connect)

            # Test the connection by executing a simple query
            import asyncio

            async def probe():
                async with self._engine.begin() as conn:
                    await conn.execute(text("SELECT 1"))

            await asyncio.wait_for(probe(), timeout=timeout)

            # Initialize vector managers after successful connection
            self._initialize_vector_managers()

            # Initialize metadata manager after successful connection
            self._metadata = AsyncMetadataManager(self)

            # Report success only after initialization returns. Unhandled
            # initialization failures are fatal, but ordinary callback errors
            # retain ConnectionHook's existing logged, best-effort policy.
            self.logger.log_connection(host, port, final_user, database or "default", success=True)

        except BaseException as e:
            if self._analytics is not None:
                self._analytics._workers.set_open(False)
            engine, self._engine = self._engine, None
            if engine is not None:
                try:
                    await engine.dispose()
                except Exception:
                    pass
            if not isinstance(e, Exception):
                raise
            from ._connection_options import redact_error

            error_msg = redact_error(e, password)
            self.logger.log_connection(host, port, final_user, database or "default", success=False)
            self.logger.log_error(ConnectionError(error_msg), context="Async connection")

            # Provide user-friendly error messages for common issues
            if 'Unknown database' in error_msg or '1049' in error_msg:
                db_name = database or "default"
                raise ConnectionError(
                    f"Database '{db_name}' does not exist. Please create it first:\n"
                    f"  mysql -h{host} -P{port} -u{user.split('#')[0] if '#' in user else user} -p "
                    f"-e \"CREATE DATABASE {db_name}\""
                ) from None
            else:
                raise ConnectionError(f"Failed to connect to MatrixOne: {error_msg}") from None

    def _setup_connection_hook(
        self, on_connect: Union[ConnectionHook, List[Union[ConnectionAction, str]], Callable]
    ) -> None:
        """Setup connection hook to be executed on each new connection"""
        try:
            if isinstance(on_connect, ConnectionHook):
                # Direct ConnectionHook instance
                hook = on_connect
            elif isinstance(on_connect, list):
                # List of actions - create a hook
                hook = create_connection_hook(actions=on_connect)
            elif callable(on_connect):
                # Custom callback function
                hook = create_connection_hook(custom_hook=on_connect)
            else:
                self.logger.warning(f"Invalid on_connect parameter type: {type(on_connect)}")
                return

            # Set the client reference and attach to engine
            hook.set_client(self)
            hook.attach_to_engine(self._engine)

        except Exception as e:
            self.logger.warning(f"Connection hook setup failed: {e}")

    async def _execute_connection_hook_immediately(
        self, on_connect: Union[ConnectionHook, List[Union[ConnectionAction, str]], Callable]
    ) -> None:
        """Execute connection hook immediately for the initial connection"""
        try:
            if isinstance(on_connect, ConnectionHook):
                # Direct ConnectionHook instance
                hook = on_connect
            elif isinstance(on_connect, list):
                # List of actions - create a hook
                hook = create_connection_hook(actions=on_connect)
            elif callable(on_connect):
                # Custom callback function
                hook = create_connection_hook(custom_hook=on_connect)
            else:
                self.logger.warning(f"Invalid on_connect parameter type: {type(on_connect)}")
                return

            # Execute the hook immediately
            await hook.execute_async(self)

        except Exception as e:
            self.logger.warning(f"Immediate connection hook execution failed: {e}")

    @classmethod
    def from_engine(cls, engine: AsyncEngine, **kwargs) -> "AsyncClient":
        """
            Create AsyncClient instance from existing SQLAlchemy AsyncEngine

            Args::

                engine: SQLAlchemy AsyncEngine instance (must use MySQL driver)
                **kwargs: Additional client configuration options

            Returns::

                AsyncClient: Configured async client instance

            Raises::

                ConnectionError: If engine doesn't use MySQL driver

            Examples

        Basic usage::

                    from sqlalchemy.ext.asyncio import create_async_engine
                    from matrixone import AsyncClient

                    engine = create_async_engine("mysql+aiomysql://user:pass@host:port/db")
                    client = AsyncClient.from_engine(engine)

                With custom configuration::

                    engine = create_async_engine("mysql+aiomysql://user:pass@host:port/db")
                    client = AsyncClient.from_engine(
                        engine,
                        sql_log_mode='auto',
                        slow_query_threshold=0.5
                    )
        """
        # Check if engine uses MySQL driver
        if not cls._is_mysql_async_engine(engine):
            raise ConnectionError(
                "MatrixOne AsyncClient only supports MySQL drivers. "
                "Please use mysql+aiomysql:// connection strings. "
                f"Current engine uses: {engine.dialect.name}"
            )

        # Create client instance with default parameters
        client = cls(**kwargs)

        # Set the provided engine
        from .sqlalchemy_ext.dialect import adapt_engine_dialect

        adapt_engine_dialect(engine.sync_engine)
        client._engine = engine

        # Initialize vector managers after engine is set
        client._initialize_vector_managers()

        return client

    @staticmethod
    def _is_mysql_async_engine(engine: AsyncEngine) -> bool:
        """
        Check if the async engine uses a MySQL driver

        Args::

            engine: SQLAlchemy AsyncEngine instance

        Returns::

            bool: True if engine uses MySQL driver, False otherwise
        """
        # Check dialect name
        dialect_name = engine.dialect.name.lower()

        # Check if it's a MySQL dialect
        if dialect_name == "mysql":
            return True

        # Check connection string for MySQL async drivers
        url = str(engine.url)
        mysql_async_drivers = [
            "mysql+aiomysql",
            "mysql+asyncmy",
            "mysql+aiopg",  # Note: aiopg is PostgreSQL, but included for completeness
        ]

        return any(driver in url.lower() for driver in mysql_async_drivers)

    def _create_async_engine(self) -> AsyncEngine:
        """Create SQLAlchemy async engine with connection pooling"""
        if not create_async_engine:
            raise ConnectionError("SQLAlchemy async engine not available. Please install sqlalchemy[asyncio]")

        # Build connection URL using SQLAlchemy's URL.create() for proper escaping
        # of special characters in user/password (e.g., @, #, %, etc.)
        url = URL.create(
            drivername="mysql+aiomysql",
            username=self._connection_params["user"],
            password=self._connection_params["password"],
            host=self._connection_params["host"],
            port=self._connection_params["port"],
            database=self._connection_params["database"] or None,
            query={"charset": self._connection_params["charset"]},
        )

        from ._local_infile import connection_options, configure_engine

        # Create async engine with connection pooling
        engine = create_async_engine(
            url,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_timeout=self.pool_timeout,
            pool_recycle=self.pool_recycle,
            pool_pre_ping=True,  # Verify connections before use
            pool_reset_on_return="rollback",  # Never commit an abandoned borrower
            connect_args={
                'connect_timeout': self._connection_params['connect_timeout'],
                **connection_options(self._connection_params.get('local_infile')),
            },
            echo=False,  # Set to True for SQL logging
        )

        configure_engine(engine, self._connection_params.get('local_infile'))

        from .sqlalchemy_ext.dialect import adapt_engine_dialect

        adapt_engine_dialect(engine.sync_engine)

        return engine

    def _initialize_vector_managers(self) -> None:
        """Initialize vector managers after successful connection"""
        try:
            from .vector_manager import AsyncVectorManager

            self._vector = AsyncVectorManager(self)
            self._fulltext_index = AsyncFulltextIndexManager(self)
        except ImportError:
            # Vector managers not available
            self._vector = None
            self._fulltext_index = None

    async def disconnect(self):
        """Disconnect from MatrixOne database asynchronously"""
        from .connection_hooks import assert_not_in_hook

        assert_not_in_hook(self)
        self._reset_compatibility()
        if self._analytics is not None:
            await self._analytics.disconnect()
        if self._engine:
            try:
                # First, try to close any active connections in the pool
                if hasattr(self._engine, "_pool") and self._engine._pool is not None:
                    # Close all connections in the pool
                    await self._engine._pool.close()
                    # Wait for all connections to be properly closed
                    await self._engine._pool.wait_closed()

                # Then dispose of the engine - this closes all connections in the pool
                await self._engine.dispose()

                # Force garbage collection to ensure connections are cleaned up
                import gc

                gc.collect()

                self.logger.log_disconnection(success=True)
            except Exception as e:
                self.logger.log_disconnection(success=False)
                self.logger.log_error(e, context="Async disconnection")
            finally:
                # Ensure all references are cleared
                self._engine = None
                self._connection = None
                # Clear any cached managers
                self._fulltext_index = None

    def disconnect_sync(self):
        """Synchronous disconnect for cleanup when event loop is closed"""
        if self._analytics is not None:
            self._analytics._workers.set_open(False)
        self._reset_compatibility()
        if self._engine:
            try:
                # Try to close the connection pool synchronously
                # SQLAlchemy AsyncEngine has a sync_engine property that can be disposed
                if hasattr(self._engine, "sync_engine") and self._engine.sync_engine is not None:
                    # Close all connections in the pool synchronously
                    self._engine.sync_engine.dispose()
                elif hasattr(self._engine, "_pool") and self._engine._pool is not None:
                    # Direct access to connection pool for cleanup
                    try:
                        self._engine._pool.close()
                    except Exception:
                        pass

                self.logger.log_disconnection(success=True)
            except Exception as e:
                self.logger.log_disconnection(success=False)
                self.logger.log_error(e, context="Sync disconnection")
            finally:
                # Ensure all references are cleared regardless of success/failure
                self._engine = None
                self._connection = None
                # Clear any cached managers
                self._fulltext_index = None

    def __del__(self):
        """Cleanup when object is garbage collected"""
        # Don't try to cleanup in __del__ as it can cause issues with event loops
        # The fixture should handle proper cleanup

    def get_sqlalchemy_engine(self) -> AsyncEngine:
        """
        Get SQLAlchemy async engine

        Returns::

            SQLAlchemy AsyncEngine
        """
        from .connection_hooks import assert_not_in_hook

        assert_not_in_hook(self)
        if not self._engine:
            raise ConnectionError("Not connected to database")
        return self._engine

    async def create_all(self, base_class=None):
        """
        Create all tables defined in the given base class or default Base.

        Args::

            base_class: SQLAlchemy declarative base class. If None, uses the default Base.
        """
        if base_class is None:
            from matrixone.orm import declarative_base

            base_class = declarative_base()

        async with self._engine.begin() as conn:
            await conn.run_sync(base_class.metadata.create_all)
        return self

    async def drop_all(self, base_class=None):
        """
        Drop all tables defined in the given base class or default Base.

        Args::

            base_class: SQLAlchemy declarative base class. If None, uses the default Base.
        """
        if base_class is None:
            from matrixone.orm import declarative_base

            base_class = declarative_base()

        # Get all table names from the metadata
        table_names = list(base_class.metadata.tables.keys())

        # Drop each table individually using direct SQL for better compatibility
        for table_name in table_names:
            try:
                await self.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception as e:
                # Log the error but continue with other tables
                print(f"Warning: Failed to drop table {table_name}: {e}")

        return self

    async def _execute_with_logging(
        self, connection, sql: str, context: str = "Async SQL execution", log_mode: str = None
    ):
        """
        Execute SQL asynchronously with proper logging through the client's logger.

        This is an internal helper method used by all SDK components to ensure
        consistent SQL logging across async vector operations, transactions, and other features.

        Args::

            connection: SQLAlchemy async connection object
            sql: SQL query string
            context: Context description for error logging (default: "Async SQL execution")
            log_mode: Temporarily override sql_log_mode for this query only

        Returns::

            SQLAlchemy result object

        Note:

            This method is used internally by AsyncVectorManager, AsyncSession,
            and other SDK components. External users should use execute() instead.
        """
        import time

        from sqlalchemy import text

        start_time = time.time()
        try:
            result = await connection.execute(text(sql))
            execution_time = time.time() - start_time

            # Try to get row count if available
            try:
                if result.returns_rows:
                    # For SELECT queries, we can't consume the result to count rows
                    # So we just log without row count
                    self.logger.log_query(sql, execution_time, None, success=True, log_mode=log_mode)
                else:
                    # For DML queries (INSERT/UPDATE/DELETE), we can get rowcount
                    self.logger.log_query(sql, execution_time, result.rowcount, success=True, log_mode=log_mode)
            except Exception:
                # Fallback: just log the query without row count
                self.logger.log_query(sql, execution_time, None, success=True, log_mode=log_mode)

            return result
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.log_query(sql, execution_time, success=False, log_mode=log_mode)
            self.logger.log_error(e, context=context)
            from .client import _classify_db_error

            classified = _classify_db_error(e, sql)
            if classified is e:
                raise
            raise classified from e

    async def execute_driver(self, sql, params=None, log_mode=None):
        """Execute unchanged SQL using aiomysql's %s / %(name)s parameters."""
        if not isinstance(sql, str):
            raise TypeError('execute_driver requires SQL text')
        return await self.execute(sql, params, log_mode, _driver_bound=True)

    @observe('query')
    async def execute(
        self, sql_or_stmt, params: Optional[Tuple] = None, log_mode: str = None, *, _driver_bound=False
    ) -> AsyncResultSet:
        """Execute with a deadline covering pool wait, execution, fetch and commit.

        A timeout cancels this call. A timed-out write's commit outcome must be
        reconciled before retrying; cancellation does not prove rollback.
        """
        from .connection_hooks import assert_not_in_hook

        assert_not_in_hook(self)
        import asyncio

        from ._connection_options import positive_timeout

        timeout = positive_timeout(self.query_timeout, 'query_timeout')
        operation = (
            self._execute(sql_or_stmt, params, log_mode, _driver_bound=True)
            if _driver_bound
            else self._execute(sql_or_stmt, params, log_mode)
        )
        return await asyncio.wait_for(operation, timeout=timeout)

    async def _execute(
        self, sql_or_stmt, params: Optional[Tuple] = None, log_mode: str = None, *, _driver_bound=False
    ) -> AsyncResultSet:
        """
        Execute SQL query or SQLAlchemy statement asynchronously without transaction isolation.

        This method executes queries asynchronously using the connection pool, without wrapping
        them in a transaction. Each statement executes independently with auto-commit enabled.
        For atomic multi-statement operations, use `async with client.session()` instead.

        The method supports both SQLAlchemy ORM-style statements (recommended) and string SQL
        with parameter binding. It's ideal for single-statement async operations like SELECT
        queries, simple INSERT/UPDATE/DELETE, or DDL statements.

        Key Features:

        - **Async/await support**: Non-blocking execution using async/await patterns
        - **ORM-style statements**: Full support for SQLAlchemy select(), insert(), update(), delete()
        - **Parameter binding**: Automatic escaping of parameters to prevent SQL injection
        - **Query logging**: Integrated async logging with performance tracking
        - **Auto-commit**: Each statement commits immediately (no transaction isolation)
        - **Connection pooling**: Efficient async connection reuse from pool

        Args:
            sql_or_stmt (str | SQLAlchemy statement): The SQL query to execute. Can be:
                - SQLAlchemy select() statement (recommended)
                - SQLAlchemy insert() statement (recommended)
                - SQLAlchemy update() statement (recommended)
                - SQLAlchemy delete() statement (recommended)
                - String SQL with '?' placeholders for parameters
                - SQLAlchemy text() statement

            params (Optional[Tuple]): Query parameters for string SQL only. Values are
                substituted for '?' placeholders in order. Automatically escaped to prevent
                SQL injection. Ignored for SQLAlchemy statements.

            log_mode (Optional[str]): Override SQL logging mode for this query only.
                Options: 'off', 'simple', 'full'. If None, uses client's global sql_log_mode
                setting. Useful for debugging or disabling logs for frequently-executed queries.

        Returns:
            AsyncResultSet: Async query result object with:
                - columns: List[str] - Column names
                - rows: List[Tuple] - Row data as tuples
                - affected_rows: int - Number of rows affected by DML operations
                - fetchall() -> List[Row] - Get all rows as list
                - fetchone() -> Optional[Row] - Get next row or None
                - fetchmany(size) -> List[Row] - Get next N rows

        Raises:
            ConnectionError: If not connected to database
            QueryError: If query execution fails or SQL syntax is invalid

        Usage Examples::

            from matrixone import AsyncClient
            from sqlalchemy import select, insert, update, delete, and_, or_, func
            from sqlalchemy.orm import declarative_base
            import asyncio

            Base = declarative_base()

            class User(Base):
                __tablename__ = 'users'
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                email = Column(String(255))
                age = Column(Integer)
                status = Column(String(20))

            class Order(Base):
                __tablename__ = 'orders'
                id = Column(Integer, primary_key=True)
                user_id = Column(Integer)
                amount = Column(Float)

            async def main():
                client = AsyncClient()
                await client.connect(host='localhost', port=6001, user='root', password='111', database='test')

                # ========================================
                # SQLAlchemy SELECT Statements (Recommended)
                # ========================================

                # Basic SELECT with WHERE clause
                stmt = select(User).where(User.age > 25)
                result = await client.execute(stmt)
                for user in result.fetchall():
                    print(f"User: {user.name}, Age: {user.age}")

                # SELECT specific columns
                stmt = select(User.name, User.email).where(User.status == 'active')
                result = await client.execute(stmt)
                for name, email in result.fetchall():
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
                result = await client.execute(stmt)

                # SELECT with JOIN
                stmt = select(User, Order).join(Order, User.id == Order.user_id)
                result = await client.execute(stmt)
                for user, order in result.fetchall():
                    print(f"{user.name} ordered ${order.amount}")

                # SELECT with aggregation
                stmt = select(func.count(User.id), func.avg(User.age)).where(User.status == 'active')
                result = await client.execute(stmt)
                count, avg_age = result.fetchone()
                print(f"Active users: {count}, Average age: {avg_age}")

                # ========================================
                # SQLAlchemy INSERT Statements (Recommended)
                # ========================================

                # Single INSERT
                stmt = insert(User).values(name='John', email='john@example.com', age=30)
                result = await client.execute(stmt)
                print(f"Inserted {result.affected_rows} rows")

                # Bulk INSERT
                stmt = insert(User).values([
                    {'name': 'Alice', 'email': 'alice@example.com', 'age': 28},
                    {'name': 'Bob', 'email': 'bob@example.com', 'age': 35},
                    {'name': 'Carol', 'email': 'carol@example.com', 'age': 42}
                ])
                result = await client.execute(stmt)
                print(f"Inserted {result.affected_rows} rows")

                # ========================================
                # SQLAlchemy UPDATE Statements (Recommended)
                # ========================================

                # Simple UPDATE
                stmt = update(User).where(User.id == 1).values(email='newemail@example.com')
                result = await client.execute(stmt)
                print(f"Updated {result.affected_rows} rows")

                # Conditional UPDATE
                stmt = update(User).where(User.age < 18).values(status='minor')
                result = await client.execute(stmt)

                # UPDATE with expressions
                stmt = update(Order).values(total=Order.quantity * Order.price)
                result = await client.execute(stmt)

                # ========================================
                # SQLAlchemy DELETE Statements (Recommended)
                # ========================================

                # Simple DELETE
                stmt = delete(User).where(User.id == 1)
                result = await client.execute(stmt)
                print(f"Deleted {result.affected_rows} rows")

                # Conditional DELETE
                stmt = delete(User).where(User.status == 'deleted')
                result = await client.execute(stmt)

                # DELETE with complex condition
                stmt = delete(User).where(
                    and_(
                        User.age < 18,
                        User.status == 'inactive'
                    )
                )
                result = await client.execute(stmt)

                # ========================================
                # Concurrent Execution with asyncio.gather
                # ========================================

                # Execute multiple independent queries concurrently
                user_stmt = select(User).where(User.age > 25)
                order_stmt = select(Order).where(Order.amount > 100)

                user_result, order_result = await asyncio.gather(
                    client.execute(user_stmt),
                    client.execute(order_stmt)
                )

                print(f"Users: {len(user_result.fetchall())}")
                print(f"Orders: {len(order_result.fetchall())}")

                # ========================================
                # String SQL with Parameters (Alternative)
                # ========================================

                # SELECT with parameters
                result = await client.execute(
                    "SELECT * FROM users WHERE age > ? AND status = ?",
                    (25, 'active')
                )

                # INSERT with parameters
                result = await client.execute(
                    "INSERT INTO users (name, email, age) VALUES (?, ?, ?)",
                    ('David', 'david@example.com', 28)
                )

                # UPDATE with parameters
                result = await client.execute(
                    "UPDATE users SET status = ? WHERE age < ?",
                    ('minor', 18)
                )

                # ========================================
                # Query Logging Control
                # ========================================

                # Disable logging for frequently executed query
                result = await client.execute(
                    select(User).where(User.id == 1),
                    log_mode='off'
                )

                # Force full SQL logging for debugging
                result = await client.execute(
                    select(User).where(User.name.like('%test%')),
                    log_mode='full'
                )

                await client.disconnect()

            asyncio.run(main())

        Important Notes:

        - **No transaction isolation**: Each execute() call commits immediately
        - **Use session() for transactions**: For atomic multi-statement operations
        - **ORM-style preferred**: Use SQLAlchemy statements for better type safety
        - **Auto-commit behavior**: Changes are permanent immediately after execute()
        - **Non-blocking**: Uses async/await and doesn't block event loop
        - **Concurrent execution**: Use asyncio.gather() for parallel queries

        Best Practices:

        1. **Prefer ORM-style statements**: Use select(), insert(), update(), delete()
        2. **Use parameters**: Always use parameter binding to prevent SQL injection
        3. **Session for transactions**: Use client.session() for atomic operations
        4. **Use asyncio.gather()**: For concurrent independent queries
        5. **Disable logging in production**: Use log_mode='off' for hot paths
        6. **Handle exceptions**: Wrap execute() in try-except for error handling

        See Also:

            - AsyncClient.session(): For transaction-aware async operations
            - AsyncSession.execute(): Execute within async transaction context
            - Client.execute(): Synchronous version
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        import time

        start_time = time.time()

        try:
            non_transactional = is_non_transactional_statement(sql_or_stmt)
            typed_statement = not isinstance(sql_or_stmt, str)
            # Keep typed statements on their SQLAlchemy execution path.
            if typed_statement and not non_transactional:
                # SQLAlchemy statement - delegate to session for consistent behavior
                async with self.session() as session:
                    # Suppress session-level logging; AsyncClient logs below
                    result = await session.execute(sql_or_stmt, params, log_mode='off')

                    # Convert SQLAlchemy result to AsyncResultSet
                    if result_returns_rows(result):
                        # SELECT query
                        from ._query_compilation import buffer_result

                        async_result = buffer_result(AsyncResultSet, result)
                        rows = async_result.rows
                        execution_time = time.time() - start_time
                        self.logger.log_query(
                            f"<SQLAlchemy {type(sql_or_stmt).__name__}>",
                            execution_time,
                            len(rows),
                            success=True,
                            log_mode=log_mode,
                        )
                        return async_result
                    else:
                        # INSERT/UPDATE/DELETE query
                        async_result = AsyncResultSet([], [], affected_rows=result.rowcount)
                        execution_time = time.time() - start_time
                        self.logger.log_query(
                            f"<SQLAlchemy {type(sql_or_stmt).__name__}>",
                            execution_time,
                            result.rowcount,
                            success=True,
                            log_mode=log_mode,
                        )
                        return async_result

            # Route standalone DDL on the same checkout used for execution.
            mode_sensitive = not typed_statement and not _driver_bound and params is not None and '\\' in sql_or_stmt
            final_sql = (
                sql_or_stmt
                if _driver_bound or typed_statement
                else None if mode_sensitive else self._substitute_parameters(sql_or_stmt, params)
            )

            # Standalone DDL and MERGE/PICK require autocommit. Change
            # only this checkout, restoring its baseline before pool reuse.
            # Ordinary SQL retains the engine.begin() transaction path.
            pool_wait()
            async with self._engine.connect() if non_transactional else self._engine.begin() as conn:
                restore_autocommit = False
                prev_autocommit = None
                if non_transactional:
                    prev_autocommit = (await conn.exec_driver_sql("SELECT @@autocommit")).scalar()
                    if isinstance(prev_autocommit, bytes):
                        prev_autocommit = prev_autocommit.decode()
                execution_failed = False
                try:
                    if mode_sensitive:
                        from ._sql_literals import connection_sql_mode

                        mode = await conn.run_sync(connection_sql_mode)
                        final_sql = self._substitute_parameters(sql_or_stmt, params, sql_mode=mode)
                    if non_transactional and str(prev_autocommit).strip().lower() not in ("1", "true", "on"):
                        # Record restoration responsibility BEFORE flipping, so a
                        # SET whose result read dies after the server applied it
                        # is still restored (or the connection invalidated).
                        restore_autocommit = True
                        await conn.exec_driver_sql("SET autocommit = 1")
                    # Use exec_driver_sql() to bypass SQLAlchemy's bind parameter parsing
                    # This prevents JSON strings like {"a":1} from being parsed as :1 bind params
                    if typed_statement:
                        result = await conn.execute(sql_or_stmt, params or {})
                    elif hasattr(conn, 'exec_driver_sql'):
                        # Escape % to %% for pymysql's format string handling
                        escaped_sql = final_sql if _driver_bound else final_sql.replace('%', '%%')
                        if _driver_bound:
                            result = await conn.exec_driver_sql(
                                escaped_sql, params, execution_options={'no_parameters': params is None}
                            )
                        else:
                            result = await conn.exec_driver_sql(escaped_sql)
                    else:
                        # Fallback for testing or older SQLAlchemy versions
                        from sqlalchemy import text

                        result = await conn.execute(text(final_sql.replace(':', r'\:')))
                except BaseException:
                    execution_failed = True
                    raise
                finally:
                    if restore_autocommit:
                        try:
                            await conn.exec_driver_sql("SET autocommit = 0")
                        except BaseException as restore_error:
                            try:
                                discard_unrestored_connection(conn.sync_connection)
                            except BaseException as discard_error:
                                # Preserve an in-flight SQL error/cancellation.
                                # A new interrupt after successful SQL still propagates.
                                if (
                                    not execution_failed
                                    and isinstance(restore_error, Exception)
                                    and not isinstance(discard_error, Exception)
                                ):
                                    raise
                            if not execution_failed and not isinstance(restore_error, Exception):
                                raise

                execution_time = time.time() - start_time
                sql_display = sql_or_stmt if not typed_statement else f"<SQLAlchemy {type(sql_or_stmt).__name__}>"

                if result.returns_rows:
                    from ._query_compilation import buffer_result

                    async_result = buffer_result(AsyncResultSet, result)
                    rows = async_result.rows
                    self.logger.log_query(sql_display, execution_time, len(rows), success=True, log_mode=log_mode)
                    return async_result
                else:
                    async_result = AsyncResultSet([], [], affected_rows=result.rowcount)
                    self.logger.log_query(sql_display, execution_time, result.rowcount, success=True, log_mode=log_mode)
                    return async_result

        except Exception as e:
            execution_time = time.time() - start_time

            # Log error FIRST, before any error processing
            # Wrap in try-except to ensure logging failure doesn't hide the real error
            try:
                sql_display = (
                    sql_or_stmt if isinstance(sql_or_stmt, str) else f"<SQLAlchemy {type(sql_or_stmt).__name__}>"
                )
                self.logger.log_query(sql_display, execution_time, success=False)
                self.logger.log_error(e, context="Async query execution")
            except Exception as log_err:
                # If logging fails, print to stderr as fallback but continue with error handling
                import sys

                print(f"Warning: Error logging failed: {log_err}", file=sys.stderr)

            from .client import _classify_db_error

            classified = _classify_db_error(e, sql_or_stmt)
            if classified is e:
                raise
            raise classified from e

    def _substitute_parameters(self, sql: str, params=None, *, sql_mode='') -> str:
        """Render legacy parameters outside SQL literals, identifiers and comments."""
        from ._sql_literals import substitute_parameters

        return substitute_parameters(sql, params, sql_mode=sql_mode)

    def _build_login_info(
        self, user: str, account: Optional[str] = None, role: Optional[str] = None
    ) -> tuple[str, dict]:
        """
        Build final login info based on user parameter and optional account/role

        Args::

            user: Username or login info in format "user", "account#user", "account#user#role",
                  "account:user", or "account:user:role"
            account: Optional account name
            role: Optional role name

        Returns::

            tuple: (final_user_string, parsed_info_dict)

        Rules:
        1. If user contains '#' or ':', it's already in format "account#user" or "account#user#role"
           (or with ':' separator, which will be normalized to '#')
           - If account or role is also provided, raise error (conflict)
        2. If user doesn't contain '#' or ':', combine with optional account/role:
           - No account/role: use user as-is
           - Only role: use "sys#user#role"
           - Only account: use "account#user"
           - Both: use "account#user#role"
        """
        # Check if user already contains login format (support both '#' and ':' as separators)
        delimiter = None
        if "#" in user:
            delimiter = "#"
        elif ":" in user:
            delimiter = ":"

        if delimiter is not None:
            # User is already in format "account#user" or "account#user#role" (or with ':')
            if account is not None or role is not None:
                raise ValueError(
                    f"Conflict: user parameter '{user}' already contains account/role info, "
                    f"but account='{account}' and role='{role}' are also provided. "
                    f"Use either user format or separate account/role parameters, not both."
                )

            # Parse the existing format
            parts = user.split(delimiter)
            if len(parts) == 2:
                # "account#user" or "account:user" format
                final_account, final_user, final_role = parts[0], parts[1], None
            elif len(parts) == 3:
                # "account#user#role" or "account:user:role" format
                final_account, final_user, final_role = parts[0], parts[1], parts[2]
            else:
                raise ValueError(
                    f"Invalid user format: '{user}'. Expected 'user', 'account#user', "
                    f"'account#user#role', 'account:user', or 'account:user:role'"
                )

            # Normalize to use '#' separator for consistency
            if delimiter == ":":
                if final_role:
                    final_user_string = f"{final_account}#{final_user}#{final_role}"
                else:
                    final_user_string = f"{final_account}#{final_user}"
            else:
                final_user_string = user

        else:
            # User is just a username, combine with optional account/role
            if account is None and role is None:
                # No account/role provided, use user as-is
                final_account, final_user, final_role = "sys", user, None
                final_user_string = user
            elif account is None and role is not None:
                # Only role provided, use sys account
                final_account, final_user, final_role = "sys", user, role
                final_user_string = f"sys#{user}#{role}"
            elif account is not None and role is None:
                # Only account provided, no role
                final_account, final_user, final_role = account, user, None
                final_user_string = f"{account}#{user}"
            else:
                # Both account and role provided
                final_account, final_user, final_role = account, user, role
                final_user_string = f"{account}#{user}#{role}"

        parsed_info = {"account": final_account, "user": final_user, "role": final_role}

        return final_user_string, parsed_info

    def get_login_info(self) -> Optional[dict]:
        """Get parsed login information"""
        return self._login_info

    def _escape_identifier(self, identifier: str) -> str:
        """Escapes an identifier to prevent SQL injection."""
        from ._sql_literals import sql_identifier

        return sql_identifier(identifier, force=True)

    def _escape_string(self, value: str) -> str:
        """Escapes a string value for SQL queries."""
        from ._sql_literals import sql_string_token

        return sql_string_token(value, context='Administrative SQL')

    def query(self, *columns, snapshot: str = None):
        """Get async MatrixOne query builder - SQLAlchemy style

            Args::

                *columns: Can be:
                    - Single model class: query(Article) - returns all columns from model
                    - Multiple columns: query(Article.id, Article.title) - returns specific columns
                    - Mixed: query(Article, Article.id, some_expression.label('alias')) - model + additional columns
                snapshot: Optional snapshot name for snapshot queries

            Examples

        # Traditional model query (all columns)
                await client.query(Article).filter(...).all()

                # Column-specific query
                await client.query(Article.id, Article.title).filter(...).all()

                # With fulltext score
                await client.query(Article.id, boolean_match("title", "content").must("python").label("score"))

                # Snapshot query
                await client.query(Article, snapshot="my_snapshot").filter(...).all()

            Returns::

                AsyncMatrixOneQuery instance configured for the specified columns
        """
        from .orm import create_query
        from .async_orm import AsyncMatrixOneQuery

        return create_query(AsyncMatrixOneQuery, self, columns, snapshot=snapshot)

    @asynccontextmanager
    async def snapshot(self, snapshot_name: str):
        """
        Snapshot context manager

        Usage

            async with client.snapshot("daily_backup") as snapshot_client:
            result = await snapshot_client.execute("SELECT * FROM users")
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        # Create a snapshot client wrapper
        from .client import SnapshotClient

        snapshot_client = SnapshotClient(self, snapshot_name)
        yield snapshot_client

    async def insert(self, table_name_or_model, data: dict) -> "AsyncResultSet":
        """
        Insert data into a table asynchronously.

        Args::

            table_name_or_model: Either a table name (str) or a SQLAlchemy model class
            data: Data to insert (dict with column names as keys)

        Returns::

            AsyncResultSet object
        """
        # Handle model class input
        table_name = table_name_or_model

        executor = AsyncClientExecutor(self)
        return await executor.insert(table_name, data)

    async def batch_insert(self, table_name_or_model, data_list: list) -> "AsyncResultSet":
        """
        Batch insert data into a table asynchronously.

        Args::

            table_name_or_model: Either a table name (str) or a SQLAlchemy model class
            data_list: List of data dictionaries to insert

        Returns::

            AsyncResultSet object
        """
        # Handle model class input
        table_name = table_name_or_model

        executor = AsyncClientExecutor(self)
        return await executor.batch_insert(table_name, data_list)

    @asynccontextmanager
    async def session(self):
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
        from .connection_hooks import assert_not_in_hook

        assert_not_in_hook(self)
        if not self._engine:
            raise ConnectionError("Not connected to database")

        tx_wrapper = None
        failed = False
        try:
            tx_wrapper = AsyncSession(bind=self._engine, client=self)
            await tx_wrapper.begin()
            pool_wait()
            connection = await tx_wrapper.connection()
            await connection.exec_driver_sql('BEGIN')
            yield tx_wrapper
            await tx_wrapper.commit()
        except BaseException:
            failed = True
            if tx_wrapper is not None:
                try:
                    await tx_wrapper.rollback()
                except BaseException:
                    self.logger.warning('Async Session rollback failed while preserving the original error')
            raise
        finally:
            if tx_wrapper is not None:
                try:
                    await tx_wrapper.close()
                except BaseException:
                    if not failed:
                        raise
                    self.logger.warning('Async Session close failed while preserving the original error')

    @property
    def snapshots(self) -> AsyncSnapshotManager:
        """Get async snapshot manager"""
        return self._snapshots

    @property
    def clone(self) -> AsyncCloneManager:
        """Get async clone manager"""
        return self._clone

    @property
    def branch(self) -> AsyncBranchManager:
        """
        Get async branch manager for Git-style version control operations.

        Provides asynchronous table and database branching, diffing, and merging capabilities.
        Requires MatrixOne 3.0.5 or higher.

        Returns:
            AsyncBranchManager instance for async branch operations

        Example::

            import asyncio
            from matrixone import AsyncClient

            async def main():
                client = AsyncClient()
                await client.connect(database='test')

                # Create table branch
                await client.branch.create_table_branch('users_branch', 'users')

                # Create database branch
                await client.branch.create_database_branch('dev_db', 'production')

                # Compare branches
                diffs = await client.branch.diff_table('users_branch', 'users')

                # Merge branches
                await client.branch.merge_table('users_branch', 'users')

                # Delete branches
                await client.branch.delete_table_branch('users_branch')
                await client.branch.delete_database_branch('dev_db')

                await client.disconnect()

            asyncio.run(main())

        See Also:
            - :doc:`branch_guide` - Complete branch management guide
        """
        return self._branch

    @property
    def moctl(self) -> AsyncMoCtlManager:
        """Get async mo_ctl manager"""
        return self._moctl

    @property
    def restore(self) -> AsyncRestoreManager:
        """Get async restore manager"""
        return self._restore

    @property
    def pitr(self) -> AsyncPitrManager:
        """Get async PITR manager"""
        return self._pitr

    @property
    def pubsub(self) -> AsyncPubSubManager:
        """Get async publish-subscribe manager"""
        return self._pubsub

    @property
    def account(self) -> AsyncAccountManager:
        """Get async account manager"""
        return self._account

    def connected(self) -> bool:
        """Check if client is connected to database"""
        return self._engine is not None

    @property
    def vector_ops(self):
        """Get unified vector operations manager for vector operations (index and data)"""
        return self._vector

    def get_pinecone_index(self, table_name_or_model, vector_column: str):
        """
        Get a PineconeCompatibleIndex object for vector search operations.

        This method creates a Pinecone-compatible vector search interface
        that automatically parses the table schema and vector index configuration.
        The primary key column is automatically detected, and all other columns
        except the vector column will be included as metadata.

        Args::

            table_name_or_model: Either a table name (str) or a SQLAlchemy model class
            vector_column: Name of the vector column

        Returns::

            PineconeCompatibleIndex object with Pinecone-compatible API

        Example::

            index = await client.get_pinecone_index("documents", "embedding")
            results = await index.query_async([0.1, 0.2, 0.3], top_k=5)
            for match in results.matches:
                print(f"ID: {match.id}, Score: {match.score}")
        """
        from .search_vector_index import PineconeCompatibleIndex

        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            table_name = table_name_or_model.__tablename__
        else:
            table_name = table_name_or_model

        return PineconeCompatibleIndex(
            client=self,
            table_name=table_name,
            vector_column=vector_column,
        )

    @property
    def fulltext_index(self):
        """Get fulltext index manager for fulltext index operations"""
        return self._fulltext_index

    async def version(self) -> str:
        """
            Get MatrixOne server version asynchronously

            Returns::

                str: MatrixOne server version string

            Raises::

                ConnectionError: If not connected to MatrixOne
                QueryError: If version query fails

            Example

        >>> client = AsyncClient()
                >>> await client.connect('localhost', 6001, 'root', '111', 'test')
                >>> version = await client.version()
                >>> print(f"MatrixOne version: {version}")
        """
        if not self.connected():
            raise ConnectionError("Not connected to MatrixOne")

        try:
            result = await self.execute("SELECT VERSION()")
            if result.rows:
                return result.rows[0][0]
            else:
                raise QueryError("Failed to get version information")
        except Exception as e:
            raise QueryError(f"Failed to get version: {e}")

    async def git_version(self) -> str:
        """
            Get MatrixOne git version information asynchronously

            Returns::

                str: MatrixOne git version string

            Raises::

                ConnectionError: If not connected to MatrixOne
                QueryError: If git version query fails

            Example

        >>> client = AsyncClient()
                >>> await client.connect('localhost', 6001, 'root', '111', 'test')
                >>> git_version = await client.git_version()
                >>> print(f"MatrixOne git version: {git_version}")
        """
        if not self.connected():
            raise ConnectionError("Not connected to MatrixOne")

        try:
            # Use MatrixOne's built-in git_version() function
            result = await self.execute("SELECT git_version()")
            if result.rows:
                return result.rows[0][0]
            else:
                raise QueryError("Failed to get git version information")
        except Exception as e:
            raise QueryError(f"Failed to get git version: {e}")

    async def get_secondary_index_tables(self, table_name: str, database_name: str = None) -> List[str]:
        """
        Get all secondary index table names for a given table (async version).

        This includes both regular secondary indexes (MULTIPLE type) and UNIQUE indexes.

        Args:
            table_name: Name of the table to get secondary indexes for
            database_name: Name of the database (optional). If None, uses the current database.

        Returns:
            List of secondary index table names (includes both __mo_index_secondary_... and __mo_index_unique_... tables)

        Examples::

            >>> async with AsyncClient() as client:
            ...     await client.connect(host='localhost', port=6001, user='root', password='111', database='test')
            ...     # Use current database
            ...     index_tables = await client.get_secondary_index_tables('cms_all_content_chunk_info')
            ...     # Or specify database explicitly
            ...     index_tables = await client.get_secondary_index_tables('cms_all_content_chunk_info', 'test')
            ...     print(index_tables)
        """
        from .index_utils import read_index_metadata

        async with self.get_sqlalchemy_engine().begin() as connection:
            _, _, rows = await connection.run_sync(read_index_metadata, table_name, database_name)
        return list(dict.fromkeys(row[2] for row in rows if row[2]))

    async def get_secondary_index_table_by_name(
        self, table_name: str, index_name: str, database_name: str = None
    ) -> Optional[str]:
        """
        Get the physical table name of a secondary index by its index name (async version).

        Args:
            table_name: Name of the table
            index_name: Name of the secondary index
            database_name: Name of the database (optional). If None, uses the current database.

        Returns:
            Physical table name of the secondary index, or None if not found

        Examples::

            >>> async with AsyncClient() as client:
            ...     await client.connect(host='localhost', port=6001, user='root', password='111', database='test')
            ...     # Use current database
            ...     index_table = await client.get_secondary_index_table_by_name('cms_all_content_chunk_info', 'cms_id')
            ...     # Or specify database explicitly
            ...     index_table = await client.get_secondary_index_table_by_name(
            ...         'cms_all_content_chunk_info', 'cms_id', 'test'
            ...     )
            ...     print(index_table)
        """
        from .index_utils import read_index_metadata

        async with self.get_sqlalchemy_engine().begin() as connection:
            _, _, rows = await connection.run_sync(read_index_metadata, table_name, database_name)
        return next((row[2] for row in rows if row[1] == index_name and row[2]), None)

    async def verify_table_index_counts(self, table_name: str) -> int:
        """
        Verify that the main table and all its secondary index tables have the same row count (async version).

        This method compares the COUNT(*) of the main table with all its secondary index tables
        in a single SQL query for consistency. If counts don't match, raises an exception.

        Args:
            table_name: Name of the table to verify

        Returns:
            Row count (int) if verification succeeds

        Raises:
            ValueError: If any secondary index table has a different count than the main table,
                       with details about all counts in the error message

        Examples::

            >>> async with AsyncClient() as client:
            ...     await client.connect(host='localhost', port=6001, user='root', password='111', database='test')
            ...     count = await client.verify_table_index_counts('cms_all_content_chunk_info')
            ...     print(f"✓ Verification passed, row count: {count}")

            >>> # If verification fails:
            >>> try:
            ...     count = await client.verify_table_index_counts('some_table')
            ... except ValueError as e:
            ...     print(f"Verification failed: {e}")
        """
        from .index_utils import index_count_report, verify_count_report

        async with self.get_sqlalchemy_engine().begin() as connection:
            report = await connection.run_sync(index_count_report, table_name)
        return verify_count_report(report)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Only disconnect if we're actually connected
        if self.connected():
            await self.disconnect()

    async def create_table(self, table_name_or_model, columns: dict = None, **kwargs) -> "AsyncClient":
        """
            Create a table asynchronously

            Args::

                table_name_or_model: Either a table name (str) or a SQLAlchemy model class
                columns: Dictionary mapping column names to their definitions (required if table_name_or_model is str)
                **kwargs: Additional table creation options

            Returns::

                AsyncClient: Self for chaining

            Example

        >>> await client.create_table("users", {
                ...     "id": "int primary key",
                ...     "name": "varchar(100)",
                ...     "email": "varchar(255)"
                ... })
        """
        from .sqlalchemy_ext.table_builder import simplified_table
        from sqlalchemy.schema import CreateTable

        if not self._engine:
            raise ConnectionError('Not connected to database')
        if hasattr(table_name_or_model, '__table__'):
            async with self._engine.begin() as connection:
                await connection.run_sync(table_name_or_model.__table__.create)
        else:
            table = simplified_table(table_name_or_model, columns, **kwargs)
            async with self._engine.begin() as connection:
                await connection.execute(CreateTable(table))
        return self

    async def drop_table(self, table_name_or_model) -> "AsyncClient":
        """
            Drop a table asynchronously

            Args::

                table_name_or_model: Either a table name (str) or a SQLAlchemy model class

            Returns::

                AsyncClient: Self for chaining

            Example

        >>> await client.drop_table("users")
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        from ._utils import get_table_name

        table_name = get_table_name(table_name_or_model, quoted=True)

        sql = f"DROP TABLE IF EXISTS {table_name}"
        await self.execute(sql)
        return self

    async def create_table_with_index(
        self, table_name: str, columns: dict, indexes: list = None, **kwargs
    ) -> "AsyncClient":
        """
            Create a table with indexes asynchronously

            Args::

                table_name: Name of the table to create
                columns: Dictionary mapping column names to their definitions
                indexes: List of index definitions
                **kwargs: Additional table creation options

            Returns::

                AsyncClient: Self for chaining

            Example

        >>> await client.create_table_with_index("users", {
                ...     "id": "int primary key",
                ...     "name": "varchar(100)",
                ...     "email": "varchar(255)"
                ... }, [
                ...     {"name": "idx_name", "columns": ["name"]},
                ...     {"name": "idx_email", "columns": ["email"], "unique": True}
                ... ])
        """
        from sqlalchemy import Index
        from sqlalchemy.schema import CreateIndex, CreateTable
        from .sqlalchemy_ext.table_builder import simplified_table

        if not self._engine:
            raise ConnectionError("Not connected to database")
        table = simplified_table(table_name, columns, **kwargs)
        definitions = []
        for definition in indexes or ():
            index = Index(
                definition['name'],
                *(table.c[name] for name in definition['columns']),
                unique=definition.get('unique', False),
            )
            definitions.append(CreateIndex(index))
        # Validate all requested columns before creating a partial table.
        await self.execute(CreateTable(table))
        for definition in definitions:
            try:
                await self.execute(definition)
            except Exception as error:
                error.table_created = True
                error.phase = 'ddl_index'
                raise

        return self

    async def create_table_orm(self, table_name: str, *columns, **kwargs) -> "AsyncClient":
        """
            Create a table using SQLAlchemy ORM asynchronously

            Args::

                table_name: Name of the table to create
                *columns: SQLAlchemy column definitions
                **kwargs: Additional table creation options

            Returns::

                AsyncClient: Self for chaining

            Example

        >>> from sqlalchemy import Column, Integer, String
                >>> await client.create_table_orm("users",
                ...     Column("id", Integer, primary_key=True),
                ...     Column("name", String(100)),
                ...     Column("email", String(255))
                ... )
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        from sqlalchemy import MetaData, Table

        metadata = MetaData()
        Table(table_name, metadata, *columns)

        # Create the table
        async with self._engine.begin() as conn:
            await conn.run_sync(metadata.create_all)

        return self

    @property
    def metadata(self) -> Optional["AsyncMetadataManager"]:
        """Get metadata manager for table metadata operations"""
        return self._metadata

    @property
    def load_data(self) -> Optional["AsyncLoadDataManager"]:
        """Get async load data manager for bulk data loading operations"""
        if self._load_data is None:
            self._load_data = AsyncLoadDataManager(self)
        return self._load_data

    @property
    def export(self):
        """Get the async CSV/JSONL export manager for this client."""
        if self._export is None:
            from .export import AsyncExportManager

            self._export = AsyncExportManager(self)
        return self._export

    @property
    def stage(self) -> Optional["AsyncStageManager"]:
        """Get async stage manager for external stage operations"""
        if self._stage is None:
            self._stage = AsyncStageManager(self)
        return self._stage

    @property
    def cdc(self) -> Optional["AsyncCDCManager"]:
        """Get async CDC manager for change data capture operations."""
        if self._cdc is None:
            self._cdc = AsyncCDCManager(self)
        return self._cdc
