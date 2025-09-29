"""
MatrixOne Client - Basic implementation
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Tuple, Union

if TYPE_CHECKING:
    from .sqlalchemy_ext import VectorOpType

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .account import AccountManager, TransactionAccountManager
from .base_client import BaseMatrixOneClient, BaseMatrixOneExecutor
from .exceptions import ConnectionError, QueryError
from .logger import MatrixOneLogger, create_default_logger
from .moctl import MoCtlManager
from .pitr import PitrManager, TransactionPitrManager
from .pubsub import PubSubManager, TransactionPubSubManager
from .restore import RestoreManager, TransactionRestoreManager
from .snapshot import CloneManager, Snapshot, SnapshotLevel, SnapshotManager
from .sqlalchemy_ext import MatrixOneDialect
from .version import get_version_manager


class ClientExecutor(BaseMatrixOneExecutor):
    """Client executor that uses Client's execute method"""

    def __init__(self, client):
        super().__init__(client)
        self.client = client

    def _execute(self, sql: str):
        return self.client.execute(sql)

    def _get_empty_result(self):
        return ResultSet([], [], affected_rows=0)


class Client(BaseMatrixOneClient):
    """
    MatrixOne Client - SQLAlchemy wrapper for database operations.

    This class provides a high-level interface for connecting to and interacting
    with MatrixOne databases using SQLAlchemy engine and connection pooling.
    It supports transaction management and various database features like
    snapshots, PITR, and account management.

    Examples:
        Basic usage::

            from matrixone import Client

            client = Client(
                host='localhost',
                port=6001,
                user='root',
                password='111',
                database='test'
            )

            result = client.execute("SELECT 1 as test")
            print(result.fetchall())

        With transaction::

            with client.transaction() as tx:
                tx.execute("INSERT INTO users (name) VALUES ('John')")
                tx.execute("INSERT INTO users (name) VALUES ('Jane')")
                # Transaction will be committed automatically

    Attributes:
        engine (Engine): SQLAlchemy engine instance
        connected (bool): Connection status
        backend_version (str): Detected backend version
    """

    def __init__(
        self,
        host: str = None,
        port: int = None,
        user: str = None,
        password: str = None,
        database: str = None,
        ssl_mode: str = "preferred",
        ssl_ca: Optional[str] = None,
        ssl_cert: Optional[str] = None,
        ssl_key: Optional[str] = None,
        account: Optional[str] = None,
        role: Optional[str] = None,
        pool_size: int = 10,
        max_overflow: int = 20,
        pool_timeout: int = 30,
        pool_recycle: int = 3600,
        connection_timeout: int = 30,
        query_timeout: int = 300,
        auto_commit: bool = True,
        charset: str = "utf8mb4",
        logger: Optional[MatrixOneLogger] = None,
        enable_performance_logging: bool = False,
        enable_sql_logging: bool = False,
        enable_full_sql_logging: bool = False,
        enable_slow_sql_logging: bool = False,
        enable_error_sql_logging: bool = False,
        slow_sql_threshold: float = 1.0,
    ):
        """
        Initialize MatrixOne client

        Args:
            host: Database host (optional, can be set later via connect)
            port: Database port (optional, can be set later via connect)
            user: Username (optional, can be set later via connect)
            password: Password (optional, can be set later via connect)
            database: Database name (optional, can be set later via connect)
            ssl_mode: SSL mode (disabled, preferred, required)
            ssl_ca: SSL CA certificate path
            ssl_cert: SSL client certificate path
            ssl_key: SSL client key path
            account: Optional account name
            role: Optional role name
            pool_size: Connection pool size
            max_overflow: Maximum overflow connections
            pool_timeout: Pool timeout in seconds
            pool_recycle: Connection recycle time in seconds
            connection_timeout: Connection timeout in seconds
            query_timeout: Query timeout in seconds
            auto_commit: Enable auto-commit mode
            charset: Character set for connection
            logger: Custom logger instance. If None, creates a default logger
            enable_performance_logging: Enable performance logging
            enable_sql_logging: Enable basic SQL query logging
            enable_full_sql_logging: Enable full SQL query logging (no truncation)
            enable_slow_sql_logging: Enable slow SQL query logging
            enable_error_sql_logging: Enable error SQL query logging
            slow_sql_threshold: Threshold in seconds for slow SQL logging
        """
        self.connection_timeout = connection_timeout
        self.query_timeout = query_timeout
        self.auto_commit = auto_commit
        self.charset = charset
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle

        # Initialize logger
        if logger is not None:
            self.logger = logger
        else:
            self.logger = create_default_logger(
                enable_performance_logging=enable_performance_logging,
                enable_sql_logging=enable_sql_logging,
                enable_full_sql_logging=enable_full_sql_logging,
                enable_slow_sql_logging=enable_slow_sql_logging,
                enable_error_sql_logging=enable_error_sql_logging,
                slow_sql_threshold=slow_sql_threshold,
            )

        self._engine = None
        self._connection_params = {}
        self._login_info = None
        self._snapshots = None
        self._clone = None
        self._moctl = None
        self._restore = None
        self._pitr = None
        self._pubsub = None
        self._account = None
        self._vector_index = None
        self._vector_query = None
        self._vector_data = None
        self._fulltext_index = None

        # Initialize version manager
        self._version_manager = get_version_manager()
        self._backend_version = None

        # Auto-connect if connection parameters are provided
        if all([host, port, user, password, database]):
            self.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                ssl_mode=ssl_mode,
                ssl_ca=ssl_ca,
                ssl_cert=ssl_cert,
                ssl_key=ssl_key,
                account=account,
                role=role,
            )

    def connect(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        ssl_mode: str = "preferred",
        ssl_ca: Optional[str] = None,
        ssl_cert: Optional[str] = None,
        ssl_key: Optional[str] = None,
        account: Optional[str] = None,
        role: Optional[str] = None,
    ) -> None:
        """
        Connect to MatrixOne database using SQLAlchemy engine

        Args:
            host: Database host
            port: Database port
            user: Username or login info in format "user", "account#user", or "account#user#role"
            password: Password
            database: Database name
            ssl_mode: SSL mode (disabled, preferred, required)
            ssl_ca: SSL CA certificate path
            ssl_cert: SSL client certificate path
            ssl_key: SSL client key path
            account: Optional account name (will be combined with user if user doesn't contain '#')
            role: Optional role name (will be combined with user if user doesn't contain '#')
        """
        # Build final login info based on user parameter and optional account/role
        final_user, parsed_info = self._build_login_info(user, account, role)

        # Store parsed info for later use
        self._login_info = parsed_info

        self._connection_params = {
            "host": host,
            "port": port,
            "user": final_user,
            "password": password,
            "database": database,
            "charset": self.charset,
            "connect_timeout": self.connection_timeout,
            "autocommit": self.auto_commit,
            "ssl_disabled": ssl_mode == "disabled",
            "ssl_verify_cert": ssl_mode == "required",
            "ssl_verify_identity": ssl_mode == "required",
        }

        # Add SSL parameters if provided
        if ssl_ca:
            self._connection_params["ssl_ca"] = ssl_ca
        if ssl_cert:
            self._connection_params["ssl_cert"] = ssl_cert
        if ssl_key:
            self._connection_params["ssl_key"] = ssl_key

        try:
            # Create SQLAlchemy engine with connection pooling
            self._engine = self._create_engine()
            self.logger.log_connection(host, port, final_user, database, success=True)

            # Initialize managers after engine is created
            self._initialize_managers()

            # Try to detect backend version after successful connection
            try:
                self._detect_backend_version()
            except Exception as e:
                self.logger.warning(f"Failed to detect backend version: {e}")

        except Exception as e:
            self.logger.log_connection(host, port, final_user, database, success=False)
            self.logger.log_error(e, context="Connection")
            raise ConnectionError(f"Failed to connect to MatrixOne: {e}")

    @classmethod
    def from_engine(cls, engine: Engine, **kwargs) -> "Client":
        """
        Create Client instance from existing SQLAlchemy Engine

        Args:
            engine: SQLAlchemy Engine instance (must use MySQL driver)
            **kwargs: Additional client configuration options

        Returns:
            Client: Configured client instance

        Raises:
            ConnectionError: If engine doesn't use MySQL driver

        Examples:
            Basic usage::

                from sqlalchemy import create_engine
                from matrixone import Client

                engine = create_engine("mysql+pymysql://user:pass@host:port/db")
                client = Client.from_engine(engine)

            With custom configuration::

                engine = create_engine("mysql+pymysql://user:pass@host:port/db")
                client = Client.from_engine(
                    engine,
                    enable_sql_logging=True,
                    slow_sql_threshold=0.5
                )
        """
        # Check if engine uses MySQL driver
        if not cls._is_mysql_engine(engine):
            raise ConnectionError(
                "MatrixOne Client only supports MySQL drivers. "
                "Please use mysql+pymysql:// or mysql+mysqlconnector:// connection strings. "
                f"Current engine uses: {engine.dialect.name}"
            )

        # Create client instance with default parameters
        client = cls(**kwargs)

        # Set the provided engine
        client._engine = engine

        # Replace the dialect with MatrixOne dialect for proper vector type support
        original_dbapi = engine.dialect.dbapi
        engine.dialect = MatrixOneDialect()
        engine.dialect.dbapi = original_dbapi

        # Initialize managers after engine is set
        client._initialize_managers()

        # Try to detect backend version
        try:
            client._detect_backend_version()
        except Exception as e:
            client.logger.warning(f"Failed to detect backend version: {e}")

        return client

    @staticmethod
    def _is_mysql_engine(engine: Engine) -> bool:
        """
        Check if the engine uses a MySQL driver

        Args:
            engine: SQLAlchemy Engine instance

        Returns:
            bool: True if engine uses MySQL driver, False otherwise
        """
        # Check dialect name
        dialect_name = engine.dialect.name.lower()

        # Check if it's a MySQL dialect
        if dialect_name == "mysql":
            return True

        # Check connection string for MySQL drivers
        url = str(engine.url)
        mysql_drivers = [
            "mysql+pymysql",
            "mysql+mysqlconnector",
            "mysql+cymysql",
            "mysql+oursql",
            "mysql+gaerdbms",
            "mysql+pyodbc",
        ]

        return any(driver in url.lower() for driver in mysql_drivers)

    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine with connection pooling"""
        # Build connection string
        connection_string = (
            f"mysql+pymysql://{self._connection_params['user']}:"
            f"{self._connection_params['password']}@"
            f"{self._connection_params['host']}:"
            f"{self._connection_params['port']}/"
            f"{self._connection_params['database']}"
        )

        # Add SSL parameters if needed
        if "ssl_ca" in self._connection_params:
            connection_string += f"?ssl_ca={self._connection_params['ssl_ca']}"

        # Create engine with connection pooling
        engine = create_engine(
            connection_string,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_timeout=self.pool_timeout,
            pool_recycle=self.pool_recycle,
            pool_pre_ping=True,  # Enable connection health checks
        )

        # Replace the dialect with MatrixOne dialect for proper vector type support
        original_dbapi = engine.dialect.dbapi
        engine.dialect = MatrixOneDialect()
        engine.dialect.dbapi = original_dbapi

        return engine

    def _initialize_managers(self) -> None:
        """Initialize all manager instances after engine is created"""
        self._snapshots = SnapshotManager(self)
        self._clone = CloneManager(self)
        self._moctl = MoCtlManager(self)
        self._restore = RestoreManager(self)
        self._pitr = PitrManager(self)
        self._pubsub = PubSubManager(self)
        self._account = AccountManager(self)
        self._vector = VectorManager(self)
        self._vector_query = VectorQueryManager(self)
        self._fulltext_index = FulltextIndexManager(self)

    def disconnect(self) -> None:
        """
        Disconnect from MatrixOne database and dispose engine.

        This method properly closes all database connections and disposes of the
        SQLAlchemy engine. It should be called when the client is no longer needed
        to free up resources.

        After calling this method, the client will need to be reconnected using
        the connect() method before any database operations can be performed.

        Raises:
            Exception: If disconnection fails (logged but re-raised)

        Example:
            >>> client = Client('localhost', 6001, 'root', '111', 'test')
            >>> client.connect()
            >>> # ... perform database operations ...
            >>> client.disconnect()  # Clean up resources
        """
        if self._engine:
            try:
                self._engine.dispose()
                self._engine = None
                self.logger.log_disconnection(success=True)
            except Exception as e:
                self.logger.log_disconnection(success=False)
                self.logger.log_error(e, context="Disconnection")
                raise

    def get_login_info(self) -> Optional[dict]:
        """
        Get parsed login information used for database connection.

        Returns the login information dictionary that was used to establish
        the database connection. This includes user, account, role, and other
        authentication details.

        Returns:
            Optional[dict]: Dictionary containing login information with keys:
                - user: Username
                - account: Account name (if specified)
                - role: Role name (if specified)
                - host: Database host
                - port: Database port
                - database: Database name
            Returns None if not connected or no login info available.

        Example:
            >>> client = Client('localhost', 6001, 'root', '111', 'test')
            >>> client.connect()
            >>> login_info = client.get_login_info()
            >>> print(f"Connected as {login_info['user']} to {login_info['database']}")
        """
        return self._login_info

    def _escape_identifier(self, identifier: str) -> str:
        """Escapes an identifier to prevent SQL injection."""
        return f"`{identifier}`"

    def _escape_string(self, value: str) -> str:
        """Escapes a string value for SQL queries."""
        return f"'{value}'"

    def _build_login_info(self, user: str, account: Optional[str] = None, role: Optional[str] = None) -> tuple[str, dict]:
        """
        Build final login info based on user parameter and optional account/role

        Args:
            user: Username or login info in format "user", "account#user", or "account#user#role"
            account: Optional account name
            role: Optional role name

        Returns:
            tuple: (final_user_string, parsed_info_dict)

        Rules:
        1. If user contains '#', it's already in format "account#user" or "account#user#role"
           - If account or role is also provided, raise error (conflict)
        2. If user doesn't contain '#', combine with optional account/role:
           - No account/role: use user as-is
           - Only role: use "sys#user#role"
           - Only account: use "account#user"
           - Both: use "account#user#role"
        """
        # Check if user already contains login format
        if "#" in user:
            # User is already in format "account#user" or "account#user#role"
            if account is not None or role is not None:
                raise ValueError(
                    f"Conflict: user parameter '{user}' already contains account/role info, "
                    f"but account='{account}' and role='{role}' are also provided. "
                    f"Use either user format or separate account/role parameters, not both."
                )

            # Parse the existing format
            parts = user.split("#")
            if len(parts) == 2:
                # "account#user" format
                final_account, final_user, final_role = parts[0], parts[1], None
            elif len(parts) == 3:
                # "account#user#role" format
                final_account, final_user, final_role = parts[0], parts[1], parts[2]
            else:
                raise ValueError(f"Invalid user format: '{user}'. Expected 'user', 'account#user', or 'account#user#role'")

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

    def execute(self, sql: str, params: Optional[Tuple] = None) -> "ResultSet":
        """
        Execute SQL query using connection pool.

        This is the primary method for executing SQL statements against the MatrixOne
        database. It supports both SELECT queries (returning data) and DML operations
        (INSERT, UPDATE, DELETE) that modify data.

        Args:
            sql (str): SQL query string. Can include '?' placeholders for parameter binding.
            params (Optional[Tuple]): Query parameters to replace '?' placeholders in the SQL.
                                    Parameters are automatically escaped to prevent SQL injection.

        Returns:
            ResultSet: Object containing query results with the following attributes:
                - columns: List of column names
                - rows: List of tuples containing row data
                - affected_rows: Number of rows affected (for DML operations)
                - fetchall(): Method to get all rows as a list
                - fetchone(): Method to get the next row
                - fetchmany(size): Method to get multiple rows

        Raises:
            ConnectionError: If not connected to database
            QueryError: If query execution fails

        Examples:
            # SELECT query
            >>> result = client.execute("SELECT * FROM users WHERE age > ?", (25,))
            >>> for row in result.fetchall():
            ...     print(row)

            # INSERT query
            >>> result = client.execute("INSERT INTO users (name, age) VALUES (?, ?)", ("John", 30))
            >>> print(f"Inserted {result.affected_rows} rows")

            # UPDATE query
            >>> result = client.execute("UPDATE users SET age = ? WHERE name = ?", (31, "John"))
            >>> print(f"Updated {result.affected_rows} rows")
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        import time

        start_time = time.time()

        try:
            # Handle parameter substitution for MatrixOne compatibility
            final_sql = self._substitute_parameters(sql, params)

            with self._engine.begin() as conn:
                result = conn.execute(text(final_sql))

                execution_time = time.time() - start_time

                if result.returns_rows:
                    # SELECT query
                    columns = list(result.keys())
                    rows = result.fetchall()
                    result_set = ResultSet(columns, rows)
                    self.logger.log_query(sql, execution_time, len(rows), success=True)
                    return result_set
                else:
                    # INSERT/UPDATE/DELETE query
                    result_set = ResultSet([], [], affected_rows=result.rowcount)
                    self.logger.log_query(sql, execution_time, result.rowcount, success=True)
                    return result_set

        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.log_query(sql, execution_time, success=False)
            self.logger.log_error(e, context="Query execution")
            raise QueryError(f"Query execution failed: {e}")

    def insert(self, table_name: str, data: dict) -> "ResultSet":
        """
        Insert a single row of data into a table.

        This method provides a convenient way to insert data using a dictionary
        where keys are column names and values are the data to insert. The method
        automatically handles SQL generation and parameter binding.

        Args:
            table_name (str): Name of the target table
            data (dict): Dictionary mapping column names to values. Example:
                        {'name': 'John', 'age': 30, 'email': 'john@example.com'}

        Returns:
            ResultSet: Object containing insertion results with:
                - affected_rows: Number of rows inserted (should be 1)
                - columns: Empty list (no columns returned for INSERT)
                - rows: Empty list (no rows returned for INSERT)

        Raises:
            ConnectionError: If not connected to database
            QueryError: If insertion fails

        Examples:
            # Insert a single user
            >>> result = client.insert('users', {
            ...     'name': 'John Doe',
            ...     'age': 30,
            ...     'email': 'john@example.com'
            ... })
            >>> print(f"Inserted {result.affected_rows} row")

            # Insert with NULL values
            >>> result = client.insert('products', {
            ...     'name': 'Product A',
            ...     'price': 99.99,
            ...     'description': None  # NULL value
            ... })
        """
        executor = ClientExecutor(self)
        return executor.insert(table_name, data)

    async def insert_async(self, table_name: str, data: dict) -> "ResultSet":
        """
        Async version of insert method.

        Args:
            table_name: Name of the table
            data: Data to insert (dict with column names as keys)

        Returns:
            ResultSet object
        """

        # Build INSERT statement
        columns = list(data.keys())
        values = list(data.values())

        # Convert vectors to string format
        formatted_values = []
        for value in values:
            if isinstance(value, list):
                formatted_values.append("[" + ",".join(map(str, value)) + "]")
            else:
                formatted_values.append(str(value))

        columns_str = ", ".join(columns)
        values_str = ", ".join([f"'{v}'" for v in formatted_values])

        sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
        return await self.execute_async(sql)

    def batch_insert(self, table_name_or_model, data_list: list) -> "ResultSet":
        """
        Batch insert multiple rows of data into a table.

        This method efficiently inserts multiple rows in a single operation,
        which is much faster than calling insert() multiple times. All rows
        must have the same column structure.

        Args:
            table_name_or_model: Either a table name (str) or a SQLAlchemy model class
            data_list (list): List of dictionaries, where each dictionary represents
                            a row to insert. All dictionaries must have the same keys.
                            Example: [
                                {'name': 'John', 'age': 30},
                                {'name': 'Jane', 'age': 25},
                                {'name': 'Bob', 'age': 35}
                            ]

        Returns:
            ResultSet: Object containing insertion results with:
                - affected_rows: Number of rows inserted
                - columns: Empty list (no columns returned for INSERT)
                - rows: Empty list (no rows returned for INSERT)

        Raises:
            ConnectionError: If not connected to database
            QueryError: If batch insertion fails
            ValueError: If data_list is empty or has inconsistent column structure

        Examples:
            # Insert multiple users
            >>> users = [
            ...     {'name': 'John Doe', 'age': 30, 'email': 'john@example.com'},
            ...     {'name': 'Jane Smith', 'age': 25, 'email': 'jane@example.com'},
            ...     {'name': 'Bob Johnson', 'age': 35, 'email': 'bob@example.com'}
            ... ]
            >>> result = client.batch_insert('users', users)
            >>> print(f"Inserted {result.affected_rows} rows")

            # Insert with some NULL values
            >>> products = [
            ...     {'name': 'Product A', 'price': 99.99, 'description': 'Great product'},
            ...     {'name': 'Product B', 'price': 149.99, 'description': None}
            ... ]
            >>> result = client.batch_insert('products', products)
        """
        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            # It's a model class
            table_name = table_name_or_model.__tablename__
        else:
            # It's a table name string
            table_name = table_name_or_model

        executor = ClientExecutor(self)
        return executor.batch_insert(table_name, data_list)

    async def batch_insert_async(self, table_name: str, data_list: list) -> "ResultSet":
        """
        Async version of batch_insert method.

        Args:
            table_name: Name of the table
            data_list: List of data dictionaries to insert

        Returns:
            ResultSet object
        """
        if not data_list:
            return ResultSet([], [], affected_rows=0)

        # Get columns from first record
        columns = list(data_list[0].keys())
        columns_str = ", ".join(columns)

        # Build VALUES clause
        values_list = []
        for data in data_list:
            formatted_values = []
            for col in columns:
                value = data[col]
                if isinstance(value, list):
                    formatted_values.append("[" + ",".join(map(str, value)) + "]")
                else:
                    formatted_values.append(str(value))
            values_str = "(" + ", ".join([f"'{v}'" for v in formatted_values]) + ")"
            values_list.append(values_str)

        values_clause = ", ".join(values_list)
        sql = f"INSERT INTO {table_name} ({columns_str}) VALUES {values_clause}"
        return await self.execute_async(sql)

    def _substitute_parameters(self, sql: str, params=None) -> str:
        """
        Substitute ? placeholders or named parameters with actual values since MatrixOne
        doesn't support prepared statements

        Args:
            sql: SQL query string with ? placeholders or named parameters (:name)
            params: Tuple of parameter values or dict of named parameters

        Returns:
            SQL string with parameters substituted
        """
        if not params:
            return sql

        final_sql = sql

        # Handle named parameters (dict)
        if isinstance(params, dict):
            for key, value in params.items():
                placeholder = f":{key}"
                if placeholder in final_sql:
                    if isinstance(value, str):
                        # Escape single quotes in string values
                        escaped_value = value.replace("'", "''")
                        final_sql = final_sql.replace(placeholder, f"'{escaped_value}'")
                    elif value is None:
                        final_sql = final_sql.replace(placeholder, "NULL")
                    else:
                        final_sql = final_sql.replace(placeholder, str(value))
        # Handle positional parameters (tuple/list)
        elif isinstance(params, (tuple, list)):
            for param in params:
                # Skip empty lists that might come from CTE queries
                if isinstance(param, list) and len(param) == 0:
                    continue
                elif isinstance(param, str):
                    # Escape single quotes in string values
                    escaped_param = param.replace("'", "''")
                    # Handle both ? and %s placeholders
                    if "?" in final_sql:
                        final_sql = final_sql.replace("?", f"'{escaped_param}'", 1)
                    elif "%s" in final_sql:
                        final_sql = final_sql.replace("%s", f"'{escaped_param}'", 1)
                elif param is None:
                    # Handle both ? and %s placeholders
                    if "?" in final_sql:
                        final_sql = final_sql.replace("?", "NULL", 1)
                    elif "%s" in final_sql:
                        final_sql = final_sql.replace("%s", "NULL", 1)
                else:
                    # Handle both ? and %s placeholders
                    if "?" in final_sql:
                        final_sql = final_sql.replace("?", str(param), 1)
                    elif "%s" in final_sql:
                        final_sql = final_sql.replace("%s", str(param), 1)

        return final_sql

    def get_sqlalchemy_engine(self) -> Engine:
        """
        Get SQLAlchemy engine

        Returns:
            SQLAlchemy Engine
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")
        return self._engine

    def query(self, *columns, snapshot: str = None):
        """Get MatrixOne query builder - SQLAlchemy style

        Args:
            *columns: Can be:
                - Single model class: query(Article) - returns all columns from model
                - Multiple columns: query(Article.id, Article.title) - returns specific columns
                - Mixed: query(Article, Article.id, some_expression.label('alias')) - model + additional columns
            snapshot: Optional snapshot name for snapshot queries

        Examples:
            # Traditional model query (all columns)
            client.query(Article).filter(...).all()

            # Column-specific query
            client.query(Article.id, Article.title).filter(...).all()

            # With fulltext score
            client.query(Article.id, boolean_match("title", "content").must("python").label("score"))

            # Snapshot query
            client.query(Article, snapshot="my_snapshot").filter(...).all()

        Returns:
            MatrixOneQuery instance configured for the specified columns
        """
        from .orm import MatrixOneQuery

        if len(columns) == 1:
            # Traditional single model class usage
            column = columns[0]
            if isinstance(column, str):
                # String table name
                return MatrixOneQuery(column, self, snapshot=snapshot)
            elif hasattr(column, '__tablename__'):
                # This is a model class
                return MatrixOneQuery(column, self, snapshot=snapshot)
            elif hasattr(column, 'name') and hasattr(column, 'as_sql'):
                # This is a CTE object
                from .orm import CTE

                if isinstance(column, CTE):
                    query = MatrixOneQuery(None, self, snapshot=snapshot)
                    query._table_name = column.name
                    query._select_columns = ["*"]  # Default to select all from CTE
                    query._ctes = [column]  # Add the CTE to the query
                    return query
            else:
                # This is a single column/expression - need to handle specially
                # For now, we'll create a query that can handle column selections
                query = MatrixOneQuery(None, self, snapshot=snapshot)
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
                query = MatrixOneQuery(model_class, self, snapshot=snapshot)
                if select_columns:
                    # Add additional columns to the model's default columns
                    query._select_columns = select_columns
                return query
            else:
                # No model class provided, need to infer table from columns
                query = MatrixOneQuery(None, self, snapshot=snapshot)
                query._select_columns = select_columns

                # Try to infer table name from first column that has table info
                for col in select_columns:
                    if hasattr(col, 'table') and hasattr(col.table, 'name'):
                        query._table_name = col.table.name
                        break

                return query

    @contextmanager
    def snapshot(self, snapshot_name: str) -> Generator[SnapshotClient, None, None]:
        """
        Snapshot context manager

        Usage:
            with client.snapshot("daily_backup") as snapshot_client:
                result = snapshot_client.execute("SELECT * FROM users")
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        # Create a snapshot client wrapper
        snapshot_client = SnapshotClient(self, snapshot_name)
        yield snapshot_client

    @contextmanager
    def transaction(self) -> Generator[TransactionWrapper, None, None]:
        """
        Transaction context manager

        Usage:
            with client.transaction() as tx:
                # MatrixOne operations
                tx.execute("INSERT INTO users ...")
                tx.execute("UPDATE users ...")

                # Snapshot and clone operations within transaction
                tx.snapshots.create("snap1", "table", database="db1", table="t1")
                tx.clone.clone_database("target_db", "source_db")

                # SQLAlchemy operations in the same transaction
                session = tx.get_sqlalchemy_session()
                session.add(user)
                session.commit()
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        tx_wrapper = None
        try:
            # Use engine's connection pool for transaction
            with self._engine.begin() as conn:
                tx_wrapper = TransactionWrapper(conn, self)
                yield tx_wrapper

                # Commit SQLAlchemy session first
                tx_wrapper.commit_sqlalchemy()

        except Exception as e:
            # Rollback SQLAlchemy session first
            if tx_wrapper:
                tx_wrapper.rollback_sqlalchemy()
            raise e
        finally:
            # Clean up SQLAlchemy resources
            if tx_wrapper:
                tx_wrapper.close_sqlalchemy()

    @property
    def snapshots(self) -> Optional[SnapshotManager]:
        """Get snapshot manager"""
        return self._snapshots

    @property
    def clone(self) -> Optional[CloneManager]:
        """Get clone manager"""
        return self._clone

    @property
    def moctl(self) -> Optional[MoCtlManager]:
        """Get mo_ctl manager"""
        return self._moctl

    @property
    def restore(self) -> Optional[RestoreManager]:
        """Get restore manager"""
        return self._restore

    @property
    def pitr(self) -> Optional[PitrManager]:
        """Get PITR manager"""
        return self._pitr

    @property
    def pubsub(self) -> Optional[PubSubManager]:
        """Get publish-subscribe manager"""
        return self._pubsub

    @property
    def account(self) -> Optional[AccountManager]:
        """Get account manager"""
        return self._account

    @property
    def vector_ops(self) -> Optional["VectorManager"]:
        """Get unified vector operations manager for vector operations (index and data)"""
        return self._vector

    def get_pinecone_index(self, table_name: str, vector_column: str):
        """
        Get a PineconeCompatibleIndex object for vector search operations.

        This method creates a Pinecone-compatible vector search interface
        that automatically parses the table schema and vector index configuration.
        The primary key column is automatically detected, and all other columns
        except the vector column will be included as metadata.

        Args:
            table_name: Name of the table containing vectors
            vector_column: Name of the vector column

        Returns:
            PineconeCompatibleIndex object with Pinecone-compatible API

        Example:
            >>> index = client.get_pinecone_index("documents", "embedding")
            >>> results = index.query([0.1, 0.2, 0.3], top_k=5)
            >>> for match in results.matches:
            ...     print(f"ID: {match.id}, Score: {match.score}")
            ...     print(f"Metadata: {match.metadata}")
        """
        from .search_vector_index import PineconeCompatibleIndex

        return PineconeCompatibleIndex(
            client=self,
            table_name=table_name,
            vector_column=vector_column,
        )

    @property
    def fulltext_index(self) -> Optional["FulltextIndexManager"]:
        """Get fulltext index manager for fulltext index operations"""
        return self._fulltext_index

    @property
    def vector_query(self) -> Optional["VectorQueryManager"]:
        """Get vector query manager for vector query operations"""
        return self._vector_query

    @property
    def connected(self) -> bool:
        """Check if client is connected to database"""
        return self._engine is not None

    def version(self) -> str:
        """
        Get MatrixOne server version

        Returns:
            str: MatrixOne server version string

        Raises:
            ConnectionError: If not connected to MatrixOne
            QueryError: If version query fails

        Example:
            >>> client = Client('localhost', 6001, 'root', '111', 'test')
            >>> version = client.version()
            >>> print(f"MatrixOne version: {version}")
        """
        if not self._engine:
            raise ConnectionError("Not connected to MatrixOne")

        try:
            result = self.execute("SELECT VERSION()")
            if result.rows:
                return result.rows[0][0]
            else:
                raise QueryError("Failed to get version information")
        except Exception as e:
            raise QueryError(f"Failed to get version: {e}")

    def git_version(self) -> str:
        """
        Get MatrixOne git version information

        Returns:
            str: MatrixOne git version string

        Raises:
            ConnectionError: If not connected to MatrixOne
            QueryError: If git version query fails

        Example:
            >>> client = Client('localhost', 6001, 'root', '111', 'test')
            >>> git_version = client.git_version()
            >>> print(f"MatrixOne git version: {git_version}")
        """
        if not self._engine:
            raise ConnectionError("Not connected to MatrixOne")

        try:
            # Use MatrixOne's built-in git_version() function
            result = self.execute("SELECT git_version()")
            if result.rows:
                return result.rows[0][0]
            else:
                raise QueryError("Failed to get git version information")
        except Exception as e:
            raise QueryError(f"Failed to get git version: {e}")

    def _detect_backend_version(self) -> None:
        """
        Detect backend version and set it in version manager

        This method attempts to get the MatrixOne version from the backend
        and sets it in the version manager for compatibility checking.

        Handles two version formats:
        1. "8.0.30-MatrixOne-v" (development version, highest priority)
        2. "8.0.30-MatrixOne-v3.0.0" (release version)
        """
        try:
            # Try to get version using version() function
            result = self.execute("SELECT version()")
            if result.rows:
                version_string = result.rows[0][0]
                version = self._parse_matrixone_version(version_string)
                if version:
                    self.set_backend_version(version)
                    self.logger.info(f"Detected backend version: {version} (from: {version_string})")
                    return

            # Fallback: try git_version()
            result = self.execute("SELECT git_version()")
            if result.rows:
                git_version = result.rows[0][0]
                version = self._parse_matrixone_version(git_version)
                if version:
                    self.set_backend_version(version)
                    self.logger.info(f"Detected backend version from git: {version} (from: {git_version})")
                    return

        except Exception as e:
            self.logger.warning(f"Could not detect backend version: {e}")

    def _parse_matrixone_version(self, version_string: str) -> Optional[str]:
        """
        Parse MatrixOne version string to extract semantic version

        Handles formats:
        1. "8.0.30-MatrixOne-v" -> "999.0.0" (development version, highest)
        2. "8.0.30-MatrixOne-v3.0.0" -> "3.0.0" (release version)
        3. "MatrixOne 3.0.1" -> "3.0.1" (fallback format)

        Args:
            version_string: Raw version string from MatrixOne

        Returns:
            Semantic version string or None if parsing fails
        """
        import re

        if not version_string:
            return None

        # Pattern 1: Development version "8.0.30-MatrixOne-v" (v后面为空)
        dev_pattern = r"(\d+\.\d+\.\d+)-MatrixOne-v$"
        dev_match = re.search(dev_pattern, version_string.strip())
        if dev_match:
            # Development version - assign highest version number
            return "999.0.0"

        # Pattern 2: Release version "8.0.30-MatrixOne-v3.0.0" (v后面有版本号)
        release_pattern = r"(\d+\.\d+\.\d+)-MatrixOne-v(\d+\.\d+\.\d+)"
        release_match = re.search(release_pattern, version_string.strip())
        if release_match:
            # Extract the semantic version part
            semantic_version = release_match.group(2)
            return semantic_version

        # Pattern 3: Fallback format "MatrixOne 3.0.1"
        fallback_pattern = r"(\d+\.\d+\.\d+)"
        fallback_match = re.search(fallback_pattern, version_string)
        if fallback_match:
            return fallback_match.group(1)

        self.logger.warning(f"Could not parse version string: {version_string}")
        return None

    def set_backend_version(self, version: str) -> None:
        """
        Manually set the backend version

        Args:
            version: Version string in format "major.minor.patch" (e.g., "3.0.1")
        """
        self._version_manager.set_backend_version(version)
        self._backend_version = version
        self.logger.info(f"Backend version set to: {version}")

    def get_backend_version(self) -> Optional[str]:
        """
        Get current backend version

        Returns:
            Version string or None if not set
        """
        backend_version = self._version_manager.get_backend_version()
        return str(backend_version) if backend_version else None

    def is_feature_available(self, feature_name: str) -> bool:
        """
        Check if a feature is available in current backend version

        Args:
            feature_name: Name of the feature to check

        Returns:
            True if feature is available, False otherwise
        """
        return self._version_manager.is_feature_available(feature_name)

    def get_feature_info(self, feature_name: str) -> Optional[Dict[str, Any]]:
        """
        Get feature requirement information

        Args:
            feature_name: Name of the feature

        Returns:
            Feature information dictionary or None if not found
        """
        requirement = self._version_manager.get_feature_info(feature_name)
        if requirement:
            return {
                "feature_name": requirement.feature_name,
                "min_version": str(requirement.min_version) if requirement.min_version else None,
                "max_version": str(requirement.max_version) if requirement.max_version else None,
                "description": requirement.description,
                "alternative": requirement.alternative,
            }
        return None

    def check_version_compatibility(self, required_version: str, operator: str = ">=") -> bool:
        """
        Check if current backend version is compatible with required version

        Args:
            required_version: Required version string (e.g., "3.0.1")
            operator: Comparison operator (">=", ">", "<=", "<", "==", "!=")

        Returns:
            True if compatible, False otherwise
        """
        return self._version_manager.is_version_compatible(required_version, operator=operator)

    def get_version_hint(self, feature_name: str, error_context: str = "") -> str:
        """
        Get helpful hint message for version-related errors

        Args:
            feature_name: Name of the feature
            error_context: Additional context for the error

        Returns:
            Helpful hint message
        """
        return self._version_manager.get_version_hint(feature_name, error_context)

    def is_development_version(self) -> bool:
        """
        Check if current backend is a development version

        Returns:
            True if backend is development version (999.x.x), False otherwise
        """
        return self._version_manager.is_development_version()

    def __enter__(self):
        return self

    def create_table(self, table_name_or_model, columns: dict = None, **kwargs) -> "Client":
        """
        Create a table with a simplified interface.

        Args:
            table_name_or_model: Either a table name (str) or a SQLAlchemy model class
            columns: Dictionary mapping column names to their types (required if table_name_or_model is str)
                    Supported formats:
                    - 'id': 'bigint' (with primary_key=True if needed)
                    - 'name': 'varchar(100)'
                    - 'embedding': 'vecf32(128)' or 'vecf64(128)'
                    - 'score': 'float'
                    - 'created_at': 'datetime'
                    - 'is_active': 'boolean'
            **kwargs: Additional table parameters

        Returns:
            Client: Self for chaining

        Example::

            client.create_table("users", {
                'id': 'bigint',
                'name': 'varchar(100)',
                'email': 'varchar(255)',
                'embedding': 'vecf32(128)',
                'score': 'float',
                'created_at': 'datetime',
                'is_active': 'boolean'
            }, primary_key='id')
        """
        from .sqlalchemy_ext import VectorTableBuilder

        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            # It's a model class
            model_class = table_name_or_model
            table_name = model_class.__tablename__
            # Use SQLAlchemy metadata to create table
            model_class.__table__.create(self.get_sqlalchemy_engine())
            return self

        # It's a table name string
        table_name = table_name_or_model
        if columns is None:
            raise ValueError("columns parameter is required when table_name_or_model is a string")

        # Parse primary key from kwargs
        primary_key = kwargs.get("primary_key", None)

        # Create table using VectorTableBuilder
        builder = VectorTableBuilder(table_name)

        # Add columns based on simplified format
        for column_name, column_def in columns.items():
            is_primary = primary_key == column_name

            if column_def.lower().startswith("vecf32(") or column_def.lower().startswith("vecf64("):
                # Parse vecf32(64) or vecf64(64) format (case insensitive)
                import re

                match = re.match(r"vecf(\d+)\((\d+)\)", column_def.lower())
                if match:
                    precision = f"f{match.group(1)}"
                    dimension = int(match.group(2))
                    builder.add_vector_column(column_name, dimension, precision)
                else:
                    raise ValueError(f"Invalid vecf format: {column_def}")

            elif column_def.startswith("varchar("):
                # Parse varchar type: varchar(100)
                import re

                match = re.match(r"varchar\((\d+)\)", column_def)
                if match:
                    length = int(match.group(1))
                    builder.add_string_column(column_name, length)
                else:
                    raise ValueError(f"Invalid varchar format: {column_def}")

            elif column_def.startswith("char("):
                # Parse char type: char(10)
                import re

                match = re.match(r"char\((\d+)\)", column_def)
                if match:
                    length = int(match.group(1))
                    builder.add_string_column(column_name, length)
                else:
                    raise ValueError(f"Invalid char format: {column_def}")

            elif column_def.startswith("decimal("):
                # Parse decimal type: decimal(10,2)
                import re

                match = re.match(r"decimal\((\d+),(\d+)\)", column_def)
                if match:
                    precision = int(match.group(1))
                    scale = int(match.group(2))
                    builder.add_numeric_column(column_name, "decimal", precision, scale)
                else:
                    raise ValueError(f"Invalid decimal format: {column_def}")

            elif column_def.startswith("float("):
                # Parse float type: float(10)
                import re

                match = re.match(r"float\((\d+)\)", column_def)
                if match:
                    precision = int(match.group(1))
                    builder.add_numeric_column(column_name, "float", precision)
                else:
                    raise ValueError(f"Invalid float format: {column_def}")

            elif column_def in ("int", "integer"):
                builder.add_int_column(column_name, primary_key=is_primary)
            elif column_def in ("bigint", "bigint unsigned"):
                builder.add_bigint_column(column_name, primary_key=is_primary)
            elif column_def in ("smallint", "tinyint"):
                if column_def == "smallint":
                    builder.add_smallint_column(column_name, primary_key=is_primary)
                else:
                    builder.add_tinyint_column(column_name, primary_key=is_primary)
            elif column_def in ("text", "longtext", "mediumtext", "tinytext"):
                builder.add_text_column(column_name)
            elif column_def in ("float", "double"):
                builder.add_numeric_column(column_name, column_def)
            elif column_def in ("date", "datetime", "timestamp", "time"):
                builder.add_datetime_column(column_name, column_def)
            elif column_def in ("boolean", "bool"):
                builder.add_boolean_column(column_name)
            elif column_def in ("json", "jsonb"):
                builder.add_json_column(column_name)
            elif column_def in (
                "blob",
                "longblob",
                "mediumblob",
                "tinyblob",
                "binary",
                "varbinary",
            ):
                builder.add_binary_column(column_name, column_def)
            else:
                raise ValueError(
                    f"Unsupported column type '{column_def}' for column '{column_name}'. "
                    f"Supported types: int, bigint, smallint, tinyint, varchar(n), char(n), "
                    f"text, float, double, decimal(p,s), date, datetime, timestamp, time, "
                    f"boolean, json, blob, vecf32(n), vecf64(n)"
                )

        # Create table
        table = builder.build()
        table.create(self.get_sqlalchemy_engine())

        return self

    def create_table_in_transaction(self, table_name: str, columns: dict, connection, **kwargs) -> "Client":
        """
        Create a table with a simplified interface within an existing SQLAlchemy transaction.

        Args:
            table_name: Name of the table
            columns: Dictionary mapping column names to their types (same format as create_table)
            connection: SQLAlchemy connection object (required for transaction support)
            **kwargs: Additional table parameters

        Returns:
            Client: Self for chaining
        """
        if connection is None:
            raise ValueError("connection parameter is required for transaction operations")

        from sqlalchemy.schema import CreateTable

        from .sqlalchemy_ext import VectorTableBuilder

        # Parse primary key from kwargs
        primary_key = kwargs.get("primary_key", None)

        # Create table using VectorTableBuilder
        builder = VectorTableBuilder(table_name)

        # Add columns based on simplified format (same logic as create_table)
        for column_name, column_def in columns.items():
            is_primary = primary_key == column_name

            if column_def.startswith("vector("):
                # Parse vector type: vector(128,f32) or vector(128)
                import re

                match = re.match(r"vector\((\d+)(?:,(\w+))?\)", column_def)
                if match:
                    dimension = int(match.group(1))
                    precision = match.group(2) or "f32"
                    builder.add_vector_column(column_name, dimension, precision)
                else:
                    raise ValueError(f"Invalid vector format: {column_def}")

            elif column_def.startswith("varchar("):
                # Parse varchar type: varchar(100)
                import re

                match = re.match(r"varchar\((\d+)\)", column_def)
                if match:
                    length = int(match.group(1))
                    builder.add_string_column(column_name, length)
                else:
                    raise ValueError(f"Invalid varchar format: {column_def}")

            elif column_def.startswith("char("):
                # Parse char type: char(10)
                import re

                match = re.match(r"char\((\d+)\)", column_def)
                if match:
                    length = int(match.group(1))
                    builder.add_string_column(column_name, length)
                else:
                    raise ValueError(f"Invalid char format: {column_def}")

            elif column_def.startswith("decimal("):
                # Parse decimal type: decimal(10,2)
                import re

                match = re.match(r"decimal\((\d+),(\d+)\)", column_def)
                if match:
                    precision = int(match.group(1))
                    scale = int(match.group(2))
                    builder.add_numeric_column(column_name, "decimal", precision, scale)
                else:
                    raise ValueError(f"Invalid decimal format: {column_def}")

            elif column_def.startswith("float("):
                # Parse float type: float(10)
                import re

                match = re.match(r"float\((\d+)\)", column_def)
                if match:
                    precision = int(match.group(1))
                    builder.add_numeric_column(column_name, "float", precision)
                else:
                    raise ValueError(f"Invalid float format: {column_def}")

            elif column_def in ("int", "integer"):
                builder.add_int_column(column_name, primary_key=is_primary)
            elif column_def in ("bigint", "bigint unsigned"):
                builder.add_bigint_column(column_name, primary_key=is_primary)
            elif column_def in ("smallint", "tinyint"):
                if column_def == "smallint":
                    builder.add_smallint_column(column_name, primary_key=is_primary)
                else:
                    builder.add_tinyint_column(column_name, primary_key=is_primary)
            elif column_def in ("text", "longtext", "mediumtext", "tinytext"):
                builder.add_text_column(column_name)
            elif column_def in ("float", "double"):
                builder.add_numeric_column(column_name, column_def)
            elif column_def in ("date", "datetime", "timestamp", "time"):
                builder.add_datetime_column(column_name, column_def)
            elif column_def in ("boolean", "bool"):
                builder.add_boolean_column(column_name)
            elif column_def in ("json", "jsonb"):
                builder.add_json_column(column_name)
            elif column_def in (
                "blob",
                "longblob",
                "mediumblob",
                "tinyblob",
                "binary",
                "varbinary",
            ):
                builder.add_binary_column(column_name, column_def)
            else:
                raise ValueError(
                    f"Unsupported column type '{column_def}' for column '{column_name}'. "
                    f"Supported types: int, bigint, smallint, tinyint, varchar(n), char(n), "
                    f"text, float, double, decimal(p,s), date, datetime, timestamp, time, "
                    f"boolean, json, blob, vecf32(n), vecf64(n)"
                )

        # Create table using the provided connection
        table = builder.build()
        create_sql = CreateTable(table)
        sql = str(create_sql.compile(dialect=connection.dialect))
        connection.execute(sql)

        return self

    def drop_table(self, table_name_or_model) -> "Client":
        """
        Drop a table.

        Args:
            table_name_or_model: Either a table name (str) or a SQLAlchemy model class

        Returns:
            Client: Self for chaining
        """
        from sqlalchemy import text

        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            # It's a model class
            table_name = table_name_or_model.__tablename__
        else:
            # It's a table name string
            table_name = table_name_or_model

        with self.get_sqlalchemy_engine().begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

        return self

    def drop_table_in_transaction(self, table_name: str, connection) -> "Client":
        """
        Drop a table within an existing SQLAlchemy transaction.

        Args:
            table_name: Name of the table to drop
            connection: SQLAlchemy connection object (required for transaction support)

        Returns:
            Client: Self for chaining
        """
        if connection is None:
            raise ValueError("connection parameter is required for transaction operations")

        from sqlalchemy import text

        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

        return self

    def create_table_with_index(self, table_name: str, columns: dict, indexes: list = None, **kwargs) -> "Client":
        """
        Create a table with vector indexes using a simplified interface.

        Args:
            table_name: Name of the table
            columns: Dictionary mapping column names to their types (same format as create_table)
            indexes: List of index definitions, each containing:
                    - 'name': Index name
                    - 'column': Column name to index
                    - 'type': Index type ('ivfflat' or 'hnsw')
                    - 'params': Dictionary of index-specific parameters
            **kwargs: Additional table parameters

        Returns:
            Client: Self for chaining

        Example::

            client.create_table_with_index("vector_docs", {
                'id': 'bigint',
                'title': 'varchar(200)',
                'embedding': 'vector(128,f32)'
            }, indexes=[
                {
                    'name': 'idx_hnsw',
                    'column': 'embedding',
                    'type': 'hnsw',
                    'params': {'m': 48, 'ef_construction': 64, 'ef_search': 64}
                }
            ], primary_key='id')
        """

        from .sqlalchemy_ext import VectorTableBuilder

        # Parse primary key from kwargs
        primary_key = kwargs.get("primary_key", None)

        # Create table using VectorTableBuilder
        builder = VectorTableBuilder(table_name)

        # Add columns based on simplified format (same logic as create_table)
        for column_name, column_def in columns.items():
            is_primary = primary_key == column_name

            if column_def.startswith("vector("):
                # Parse vector type: vector(128,f32) or vector(128)
                import re

                match = re.match(r"vector\((\d+)(?:,(\w+))?\)", column_def)
                if match:
                    dimension = int(match.group(1))
                    precision = match.group(2) or "f32"
                    builder.add_vector_column(column_name, dimension, precision)
                else:
                    raise ValueError(f"Invalid vector format: {column_def}")

            elif column_def.startswith("varchar("):
                # Parse varchar type: varchar(100)
                import re

                match = re.match(r"varchar\((\d+)\)", column_def)
                if match:
                    length = int(match.group(1))
                    builder.add_string_column(column_name, length)
                else:
                    raise ValueError(f"Invalid varchar format: {column_def}")

            elif column_def.startswith("char("):
                # Parse char type: char(10)
                import re

                match = re.match(r"char\((\d+)\)", column_def)
                if match:
                    length = int(match.group(1))
                    builder.add_string_column(column_name, length)
                else:
                    raise ValueError(f"Invalid char format: {column_def}")

            elif column_def.startswith("decimal("):
                # Parse decimal type: decimal(10,2)
                import re

                match = re.match(r"decimal\((\d+),(\d+)\)", column_def)
                if match:
                    precision = int(match.group(1))
                    scale = int(match.group(2))
                    builder.add_numeric_column(column_name, "decimal", precision, scale)
                else:
                    raise ValueError(f"Invalid decimal format: {column_def}")

            elif column_def.startswith("float("):
                # Parse float type: float(10)
                import re

                match = re.match(r"float\((\d+)\)", column_def)
                if match:
                    precision = int(match.group(1))
                    builder.add_numeric_column(column_name, "float", precision)
                else:
                    raise ValueError(f"Invalid float format: {column_def}")

            elif column_def in ("int", "integer"):
                builder.add_int_column(column_name, primary_key=is_primary)
            elif column_def in ("bigint", "bigint unsigned"):
                builder.add_bigint_column(column_name, primary_key=is_primary)
            elif column_def in ("smallint", "tinyint"):
                if column_def == "smallint":
                    builder.add_smallint_column(column_name, primary_key=is_primary)
                else:
                    builder.add_tinyint_column(column_name, primary_key=is_primary)
            elif column_def in ("text", "longtext", "mediumtext", "tinytext"):
                builder.add_text_column(column_name)
            elif column_def in ("float", "double"):
                builder.add_numeric_column(column_name, column_def)
            elif column_def in ("date", "datetime", "timestamp", "time"):
                builder.add_datetime_column(column_name, column_def)
            elif column_def in ("boolean", "bool"):
                builder.add_boolean_column(column_name)
            elif column_def in ("json", "jsonb"):
                builder.add_json_column(column_name)
            elif column_def in (
                "blob",
                "longblob",
                "mediumblob",
                "tinyblob",
                "binary",
                "varbinary",
            ):
                builder.add_binary_column(column_name, column_def)
            else:
                raise ValueError(
                    f"Unsupported column type '{column_def}' for column '{column_name}'. "
                    f"Supported types: int, bigint, smallint, tinyint, varchar(n), char(n), "
                    f"text, float, double, decimal(p,s), date, datetime, timestamp, time, "
                    f"boolean, json, blob, vecf32(n), vecf64(n)"
                )

        # Create table
        table = builder.build()
        table.create(self.get_sqlalchemy_engine())

        # Create indexes if specified
        if indexes:
            for index_def in indexes:
                index_name = index_def["name"]
                column_name = index_def["column"]
                index_type = index_def["type"]
                params = index_def.get("params", {})

                # Convert index type and enable indexing
                if index_type == "hnsw":
                    # Enable HNSW indexing
                    self.vector_index.enable_hnsw()
                elif index_type == "ivfflat":
                    # Enable IVF indexing
                    self.vector_index.enable_ivf()
                else:
                    raise ValueError(f"Unsupported index type: {index_type}")

                # Create the index using separated APIs
                if index_type == "ivfflat":
                    self.vector_index.create_ivf(table_name=table_name, name=index_name, column=column_name, **params)
                elif index_type == "hnsw":
                    self.vector_index.create_hnsw(table_name=table_name, name=index_name, column=column_name, **params)
                else:
                    raise ValueError(f"Unsupported index type: {index_type}")

        return self

    def create_table_with_index_in_transaction(
        self, table_name: str, columns: dict, connection, indexes: list = None, **kwargs
    ) -> "Client":
        """
        Create a table with vector indexes within an existing SQLAlchemy transaction.

        Args:
            table_name: Name of the table
            columns: Dictionary mapping column names to their types (same format as create_table)
            connection: SQLAlchemy connection object (required for transaction support)
            indexes: List of index definitions (same format as create_table_with_index)
            **kwargs: Additional table parameters

        Returns:
            Client: Self for chaining
        """
        if connection is None:
            raise ValueError("connection parameter is required for transaction operations")

        from sqlalchemy.schema import CreateTable

        from .sqlalchemy_ext import VectorTableBuilder

        # Parse primary key from kwargs
        primary_key = kwargs.get("primary_key", None)

        # Create table using VectorTableBuilder
        builder = VectorTableBuilder(table_name)

        # Add columns based on simplified format (same logic as create_table)
        for column_name, column_def in columns.items():
            is_primary = primary_key == column_name

            if column_def.startswith("vector("):
                # Parse vector type: vector(128,f32) or vector(128)
                import re

                match = re.match(r"vector\((\d+)(?:,(\w+))?\)", column_def)
                if match:
                    dimension = int(match.group(1))
                    precision = match.group(2) or "f32"
                    builder.add_vector_column(column_name, dimension, precision)
                else:
                    raise ValueError(f"Invalid vector format: {column_def}")

            elif column_def.startswith("varchar("):
                # Parse varchar type: varchar(100)
                import re

                match = re.match(r"varchar\((\d+)\)", column_def)
                if match:
                    length = int(match.group(1))
                    builder.add_string_column(column_name, length)
                else:
                    raise ValueError(f"Invalid varchar format: {column_def}")

            elif column_def.startswith("char("):
                # Parse char type: char(10)
                import re

                match = re.match(r"char\((\d+)\)", column_def)
                if match:
                    length = int(match.group(1))
                    builder.add_string_column(column_name, length)
                else:
                    raise ValueError(f"Invalid char format: {column_def}")

            elif column_def.startswith("decimal("):
                # Parse decimal type: decimal(10,2)
                import re

                match = re.match(r"decimal\((\d+),(\d+)\)", column_def)
                if match:
                    precision = int(match.group(1))
                    scale = int(match.group(2))
                    builder.add_numeric_column(column_name, "decimal", precision, scale)
                else:
                    raise ValueError(f"Invalid decimal format: {column_def}")

            elif column_def.startswith("float("):
                # Parse float type: float(10)
                import re

                match = re.match(r"float\((\d+)\)", column_def)
                if match:
                    precision = int(match.group(1))
                    builder.add_numeric_column(column_name, "float", precision)
                else:
                    raise ValueError(f"Invalid float format: {column_def}")

            elif column_def in ("int", "integer"):
                builder.add_int_column(column_name, primary_key=is_primary)
            elif column_def in ("bigint", "bigint unsigned"):
                builder.add_bigint_column(column_name, primary_key=is_primary)
            elif column_def in ("smallint", "tinyint"):
                if column_def == "smallint":
                    builder.add_smallint_column(column_name, primary_key=is_primary)
                else:
                    builder.add_tinyint_column(column_name, primary_key=is_primary)
            elif column_def in ("text", "longtext", "mediumtext", "tinytext"):
                builder.add_text_column(column_name)
            elif column_def in ("float", "double"):
                builder.add_numeric_column(column_name, column_def)
            elif column_def in ("date", "datetime", "timestamp", "time"):
                builder.add_datetime_column(column_name, column_def)
            elif column_def in ("boolean", "bool"):
                builder.add_boolean_column(column_name)
            elif column_def in ("json", "jsonb"):
                builder.add_json_column(column_name)
            elif column_def in (
                "blob",
                "longblob",
                "mediumblob",
                "tinyblob",
                "binary",
                "varbinary",
            ):
                builder.add_binary_column(column_name, column_def)
            else:
                raise ValueError(
                    f"Unsupported column type '{column_def}' for column '{column_name}'. "
                    f"Supported types: int, bigint, smallint, tinyint, varchar(n), char(n), "
                    f"text, float, double, decimal(p,s), date, datetime, timestamp, time, "
                    f"boolean, json, blob, vecf32(n), vecf64(n)"
                )

        # Create table using the provided connection
        table = builder.build()
        create_sql = CreateTable(table)
        sql = str(create_sql.compile(dialect=connection.dialect))
        connection.execute(sql)

        # Create indexes if specified
        if indexes:
            for index_def in indexes:
                index_name = index_def["name"]
                column_name = index_def["column"]
                index_type = index_def["type"]
                params = index_def.get("params", {})

                # Create the index using separated APIs
                if index_type == "ivfflat":
                    self.vector_index.create_ivf(table_name=table_name, name=index_name, column=column_name, **params)
                elif index_type == "hnsw":
                    self.vector_index.create_hnsw(table_name=table_name, name=index_name, column=column_name, **params)
                else:
                    raise ValueError(f"Unsupported index type: {index_type}")

        return self

    def create_table_orm(self, table_name: str, *columns, **kwargs) -> "Client":
        """
        Create a table using SQLAlchemy ORM-style column definitions.
        Similar to SQLAlchemy Table() constructor but without metadata.

        Args:
            table_name: Name of the table
            *columns: SQLAlchemy Column objects and Index objects (including VectorIndex)
            **kwargs: Additional parameters (like enable_hnsw, enable_ivf)

        Returns:
            Client: Self for chaining

        Example::

            from sqlalchemy import Column, BigInteger, Integer
            from matrixone.sqlalchemy_ext import Vectorf32, VectorIndex, VectorIndexType, VectorOpType

            client.create_table_orm(
                'vector_docs_hnsw_demo',
                Column('a', BigInteger, primary_key=True),
                Column('b', Vectorf32(128)),
                Column('c', Integer),
                VectorIndex('idx_hnsw', 'b', index_type=VectorIndexType.HNSW,
                           m=48, ef_construction=64, ef_search=64,
                           op_type=VectorOpType.VECTOR_L2_OPS)
            )
        """
        from sqlalchemy import MetaData, Table

        # Create metadata and table
        metadata = MetaData()
        table = Table(table_name, metadata, *columns)

        # Check if we need to enable HNSW or IVF indexing
        enable_hnsw = kwargs.get("enable_hnsw", False)
        enable_ivf = kwargs.get("enable_ivf", False)

        # Check if table has vector indexes that need special handling
        has_hnsw_index = False
        has_ivf_index = False

        for item in table.indexes:
            if hasattr(item, "index_type"):
                # Check for HNSW index type (string comparison)
                if str(item.index_type).lower() == "hnsw":
                    has_hnsw_index = True
                elif str(item.index_type).lower() == "ivfflat":
                    has_ivf_index = True

        # Create table using SQLAlchemy engine with proper session handling
        engine = self.get_sqlalchemy_engine()

        # Enable appropriate indexing if needed and create table in same session
        if has_hnsw_index or enable_hnsw:
            with engine.begin() as conn:
                from .sqlalchemy_ext import create_hnsw_config

                hnsw_config = create_hnsw_config(self._engine)
                hnsw_config.enable_hnsw_indexing(conn)
                # Create table and indexes in the same session
                table.create(conn)
        elif has_ivf_index or enable_ivf:
            with engine.begin() as conn:
                from .sqlalchemy_ext import create_ivf_config

                ivf_config = create_ivf_config(self._engine)
                ivf_config.enable_ivf_indexing()
                # Create table and indexes in the same session
                table.create(conn)
        else:
            # No special indexing needed, create normally
            table.create(engine)

        return self

    def create_table_orm_in_transaction(self, table_name: str, connection, *columns, **kwargs) -> "Client":
        """
        Create a table using SQLAlchemy ORM-style definitions within an existing SQLAlchemy transaction.

        Args:
            table_name: Name of the table
            connection: SQLAlchemy connection object (required for transaction support)
            *columns: SQLAlchemy Column objects and Index objects (including VectorIndex)
            **kwargs: Additional parameters (like enable_hnsw, enable_ivf)

        Returns:
            Client: Self for chaining
        """
        if connection is None:
            raise ValueError("connection parameter is required for transaction operations")

        from sqlalchemy import MetaData, Table
        from sqlalchemy.schema import CreateTable

        # Create metadata and table
        metadata = MetaData()
        table = Table(table_name, metadata, *columns)

        # Check if we need to enable HNSW or IVF indexing
        enable_hnsw = kwargs.get("enable_hnsw", False)
        enable_ivf = kwargs.get("enable_ivf", False)

        # Check if table has vector indexes that need special handling
        has_hnsw_index = False
        has_ivf_index = False

        for item in table.indexes:
            if hasattr(item, "index_type"):
                # Check for HNSW index type (string comparison)
                if str(item.index_type).lower() == "hnsw":
                    has_hnsw_index = True
                elif str(item.index_type).lower() == "ivfflat":
                    has_ivf_index = True

        # Enable appropriate indexing if needed (within transaction)
        if has_hnsw_index or enable_hnsw:
            from .sqlalchemy_ext import create_hnsw_config

            hnsw_config = create_hnsw_config(self._engine)
            hnsw_config.enable_hnsw_indexing(connection)
        if has_ivf_index or enable_ivf:
            from .sqlalchemy_ext import create_ivf_config

            ivf_config = create_ivf_config(self._engine)
            ivf_config.enable_ivf_indexing()

        # Create table using the provided connection
        create_sql = CreateTable(table)
        sql = str(create_sql.compile(dialect=connection.dialect))
        connection.execute(sql)

        return self

    def create_all(self, base_class=None):
        """
        Create all tables defined in the given base class or default Base.

        Args:
            base_class: SQLAlchemy declarative base class. If None, uses the default Base.
        """
        if base_class is None:
            from sqlalchemy.ext.declarative import declarative_base

            base_class = declarative_base()

        base_class.metadata.create_all(self._engine)
        return self

    def drop_all(self, base_class=None):
        """
        Drop all tables defined in the given base class or default Base.

        Args:
            base_class: SQLAlchemy declarative base class. If None, uses the default Base.
        """
        if base_class is None:
            from sqlalchemy.ext.declarative import declarative_base

            base_class = declarative_base()

        # Get all table names from the metadata
        table_names = list(base_class.metadata.tables.keys())

        # Drop each table individually using direct SQL for better compatibility
        for table_name in table_names:
            try:
                self.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception as e:
                # Log the error but continue with other tables
                print(f"Warning: Failed to drop table {table_name}: {e}")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


class ResultSet:
    """
    Result set wrapper for query results from MatrixOne database operations.

    This class provides a convenient interface for accessing query results
    with methods similar to database cursor objects. It supports both
    SELECT queries (returning data) and DML operations (returning affected row counts).

    Key Features:
    - Iterator interface for row-by-row access
    - Bulk data access methods (fetchall, fetchmany)
    - Column name access and metadata
    - Affected row count for DML operations
    - Cursor-like positioning for result navigation

    Attributes:
        columns (List[str]): List of column names in the result set
        rows (List[Tuple[Any, ...]]): List of tuples containing row data
        affected_rows (int): Number of rows affected by DML operations

    Usage Examples:
        # SELECT query results
        >>> result = client.execute("SELECT id, name, age FROM users WHERE age > ?", (25,))
        >>> print(f"Found {len(result.rows)} users")
        >>> for row in result.fetchall():
        ...     print(f"ID: {row[0]}, Name: {row[1]}, Age: {row[2]}")

        # Access by column name
        >>> for row in result.rows:
        ...     user_id = row[result.columns.index('id')]
        ...     user_name = row[result.columns.index('name')]

        # DML operation results
        >>> result = client.execute("INSERT INTO users (name, age) VALUES (?, ?)", ("John", 30))
        >>> print(f"Inserted {result.affected_rows} rows")

        # Iterator interface
        >>> for row in result:
        ...     print(row)

    Note: This class is automatically created by the Client's execute() method
    and provides a consistent interface for all query results.
    """

    def __init__(self, columns: List[str], rows: List[Tuple[Any, ...]], affected_rows: int = 0):
        self.columns = columns
        self.rows = rows
        self.affected_rows = affected_rows
        self._cursor = 0  # Track current position in result set

    def fetchall(self) -> List[Tuple[Any, ...]]:
        """Fetch all remaining rows"""
        remaining_rows = self.rows[self._cursor :]
        self._cursor = len(self.rows)
        return remaining_rows

    def fetchone(self) -> Optional[Tuple[Any, ...]]:
        """Fetch one row"""
        if self._cursor < len(self.rows):
            row = self.rows[self._cursor]
            self._cursor += 1
            return row
        return None

    def fetchmany(self, size: int = 1) -> List[Tuple[Any, ...]]:
        """Fetch many rows"""
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


class SnapshotClient:
    """Snapshot client wrapper for executing queries with snapshot"""

    def __init__(self, client, snapshot_name: str):
        self.client = client
        self.snapshot_name = snapshot_name

    def execute(self, sql: str, params: Optional[Tuple] = None) -> ResultSet:
        """Execute SQL with snapshot"""
        # Insert snapshot hint after the first table name in FROM clause
        import re

        # Find the first table name after FROM and insert snapshot hint
        pattern = r"(\bFROM\s+)(\w+)(\s|$)"

        def replace_func(match):
            return f"{match.group(1)}{match.group(2)}{{snapshot = '{self.snapshot_name}'}}{match.group(3)}"

        snapshot_sql = re.sub(pattern, replace_func, sql, count=1)

        # Handle parameter substitution for MatrixOne compatibility
        final_sql = self.client._substitute_parameters(snapshot_sql, params)

        return self.client.execute(final_sql)


class TransactionWrapper:
    """
    Transaction wrapper for executing queries within a MatrixOne transaction.

    This class provides a transaction context for executing multiple database
    operations atomically. It wraps a SQLAlchemy connection and provides
    access to all MatrixOne managers (snapshots, clone, restore, PITR, etc.)
    within the transaction context.

    Key Features:
    - Atomic transaction execution with automatic rollback on errors
    - Access to all MatrixOne managers within transaction context
    - SQLAlchemy session integration
    - Automatic commit/rollback handling
    - Support for nested transactions

    Available Managers:
    - snapshots: TransactionSnapshotManager for snapshot operations
    - clone: TransactionCloneManager for clone operations
    - restore: TransactionRestoreManager for restore operations
    - pitr: TransactionPitrManager for point-in-time recovery
    - pubsub: TransactionPubSubManager for pub/sub operations
    - account: TransactionAccountManager for account operations
    - vector: TransactionVectorManager for vector operations
    - fulltext_index: TransactionFulltextIndexManager for fulltext operations

    Usage Examples:
        # Basic transaction usage
        with client.transaction() as tx:
            tx.execute("INSERT INTO users (name) VALUES (?)", ("John",))
            tx.execute("INSERT INTO orders (user_id, amount) VALUES (?, ?)", (1, 100.0))
            # Transaction commits automatically on success

        # Using managers within transaction
        with client.transaction() as tx:
            # Create snapshot within transaction
            tx.snapshots.create("backup", SnapshotLevel.DATABASE, database="mydb")

            # Clone database within transaction
            tx.clone.clone_database("new_db", "source_db")

            # Vector operations within transaction
            tx.vector.create_table("vectors", {"id": "int", "embedding": "vector(384,f32)"})

        # SQLAlchemy session integration
        with client.transaction() as tx:
            session = tx.get_sqlalchemy_session()
            user = User(name="John")
            session.add(user)
            session.commit()

    Note: This class is automatically created by the Client's transaction()
    context manager and should not be instantiated directly.
    """

    def __init__(self, connection, client):
        self.connection = connection
        self.client = client
        # Create snapshot, clone, restore, PITR, pubsub, account, and vector managers that use this transaction
        self.snapshots = TransactionSnapshotManager(client, self)
        self.clone = TransactionCloneManager(client, self)
        self.restore = TransactionRestoreManager(client, self)
        self.pitr = TransactionPitrManager(client, self)
        self.pubsub = TransactionPubSubManager(client, self)
        self.account = TransactionAccountManager(self)
        self.vector_ops = TransactionVectorIndexManager(client, self)
        self.vector_query = TransactionVectorQueryManager(client, self)
        self.fulltext_index = TransactionFulltextIndexManager(client, self)
        # SQLAlchemy integration
        self._sqlalchemy_session = None

    def execute(self, sql: str, params: Optional[Tuple] = None) -> ResultSet:
        """Execute SQL within transaction"""
        try:
            result = self.connection.execute(text(sql), params or {})

            if result.returns_rows:
                columns = list(result.keys())
                rows = result.fetchall()
                return ResultSet(columns, rows)
            else:
                return ResultSet([], [], affected_rows=result.rowcount)

        except Exception as e:
            raise QueryError(f"Transaction query execution failed: {e}")

    def get_connection(self):
        """
        Get the underlying SQLAlchemy connection for direct use

        Returns:
            SQLAlchemy Connection instance bound to this transaction
        """
        return self.connection

    def get_sqlalchemy_session(self):
        """
        Get SQLAlchemy session that uses the same transaction

        Returns:
            SQLAlchemy Session instance bound to this transaction
        """
        if self._sqlalchemy_session is None:
            from sqlalchemy.orm import sessionmaker

            # Create session factory using the client's engine
            Session = sessionmaker(bind=self.client._engine)
            self._sqlalchemy_session = Session(bind=self.connection)

        return self._sqlalchemy_session

    def commit_sqlalchemy(self) -> None:
        """Commit SQLAlchemy session"""
        if self._sqlalchemy_session:
            self._sqlalchemy_session.commit()

    def rollback_sqlalchemy(self) -> None:
        """Rollback SQLAlchemy session"""
        if self._sqlalchemy_session:
            self._sqlalchemy_session.rollback()

    def close_sqlalchemy(self) -> None:
        """Close SQLAlchemy session"""
        if self._sqlalchemy_session:
            self._sqlalchemy_session.close()
            self._sqlalchemy_session = None

    def insert(self, table_name: str, data: dict[str, Any]) -> ResultSet:
        """
        Insert data into a table within transaction.

        Args:
            table_name: Name of the table
            data: Data to insert (dict with column names as keys)

        Returns:
            ResultSet object
        """
        sql = self.client._build_insert_sql(table_name, data)
        return self.execute(sql)

    def batch_insert(self, table_name: str, data_list: list[dict[str, Any]]) -> ResultSet:
        """
        Batch insert data into a table within transaction.

        Args:
            table_name: Name of the table
            data_list: List of data dictionaries to insert

        Returns:
            ResultSet object
        """
        if not data_list:
            return ResultSet([], [], affected_rows=0)

        sql = self.client._build_batch_insert_sql(table_name, data_list)
        return self.execute(sql)

    def query(self, *columns, snapshot: str = None):
        """Get MatrixOne query builder within transaction - SQLAlchemy style

        Args:
            *columns: Can be:
                - Single model class: query(Article) - returns all columns from model
                - Multiple columns: query(Article.id, Article.title) - returns specific columns
                - Mixed: query(Article, Article.id, some_expression.label('alias')) - model + additional columns
            snapshot: Optional snapshot name for snapshot queries

        Returns:
            MatrixOneQuery instance configured for the specified columns within transaction
        """
        from .orm import MatrixOneQuery

        if len(columns) == 1:
            # Traditional single model class usage
            column = columns[0]
            if hasattr(column, '__tablename__'):
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

                return query

    def create_table(self, table_name: str, columns: dict, **kwargs) -> "TransactionWrapper":
        """
        Create a table within MatrixOne transaction.

        Args:
            table_name: Name of the table
            columns: Dictionary mapping column names to their types (same format as client.create_table)
            **kwargs: Additional table parameters

        Returns:
            TransactionWrapper: Self for chaining
        """
        from sqlalchemy.schema import CreateTable

        from .sqlalchemy_ext import VectorTableBuilder

        # Parse primary key from kwargs
        primary_key = kwargs.get("primary_key", None)

        # Create table using VectorTableBuilder
        builder = VectorTableBuilder(table_name)

        # Add columns based on simplified format (same logic as client.create_table)
        for column_name, column_def in columns.items():
            is_primary = primary_key == column_name

            if column_def.startswith("vector("):
                # Parse vector type: vector(128,f32) or vector(128)
                import re

                match = re.match(r"vector\((\d+)(?:,(\w+))?\)", column_def)
                if match:
                    dimension = int(match.group(1))
                    precision = match.group(2) or "f32"
                    builder.add_vector_column(column_name, dimension, precision)
                else:
                    raise ValueError(f"Invalid vector format: {column_def}")

            elif column_def.startswith("varchar("):
                # Parse varchar type: varchar(100)
                import re

                match = re.match(r"varchar\((\d+)\)", column_def)
                if match:
                    length = int(match.group(1))
                    builder.add_string_column(column_name, length)
                else:
                    raise ValueError(f"Invalid varchar format: {column_def}")

            elif column_def.startswith("char("):
                # Parse char type: char(10)
                import re

                match = re.match(r"char\((\d+)\)", column_def)
                if match:
                    length = int(match.group(1))
                    builder.add_string_column(column_name, length)
                else:
                    raise ValueError(f"Invalid char format: {column_def}")

            elif column_def.startswith("decimal("):
                # Parse decimal type: decimal(10,2)
                import re

                match = re.match(r"decimal\((\d+),(\d+)\)", column_def)
                if match:
                    precision = int(match.group(1))
                    scale = int(match.group(2))
                    builder.add_numeric_column(column_name, "decimal", precision, scale)
                else:
                    raise ValueError(f"Invalid decimal format: {column_def}")

            elif column_def.startswith("float("):
                # Parse float type: float(10)
                import re

                match = re.match(r"float\((\d+)\)", column_def)
                if match:
                    precision = int(match.group(1))
                    builder.add_numeric_column(column_name, "float", precision)
                else:
                    raise ValueError(f"Invalid float format: {column_def}")

            elif column_def in ("int", "integer"):
                builder.add_int_column(column_name, primary_key=is_primary)
            elif column_def in ("bigint", "bigint unsigned"):
                builder.add_bigint_column(column_name, primary_key=is_primary)
            elif column_def in ("smallint", "tinyint"):
                if column_def == "smallint":
                    builder.add_smallint_column(column_name, primary_key=is_primary)
                else:
                    builder.add_tinyint_column(column_name, primary_key=is_primary)
            elif column_def in ("text", "longtext", "mediumtext", "tinytext"):
                builder.add_text_column(column_name)
            elif column_def in ("float", "double"):
                builder.add_numeric_column(column_name, column_def)
            elif column_def in ("date", "datetime", "timestamp", "time"):
                builder.add_datetime_column(column_name, column_def)
            elif column_def in ("boolean", "bool"):
                builder.add_boolean_column(column_name)
            elif column_def in ("json", "jsonb"):
                builder.add_json_column(column_name)
            elif column_def in (
                "blob",
                "longblob",
                "mediumblob",
                "tinyblob",
                "binary",
                "varbinary",
            ):
                builder.add_binary_column(column_name, column_def)
            else:
                raise ValueError(
                    f"Unsupported column type '{column_def}' for column '{column_name}'. "
                    f"Supported types: int, bigint, smallint, tinyint, varchar(n), char(n), "
                    f"text, float, double, decimal(p,s), date, datetime, timestamp, time, "
                    f"boolean, json, blob, vecf32(n), vecf64(n)"
                )

        # Create table using transaction wrapper's execute method
        table = builder.build()
        create_sql = CreateTable(table)
        sql = str(create_sql.compile(dialect=self.client.get_sqlalchemy_engine().dialect))
        self.execute(sql)

        return self

    def drop_table(self, table_name: str) -> "TransactionWrapper":
        """
        Drop a table within MatrixOne transaction.

        Args:
            table_name: Name of the table to drop

        Returns:
            TransactionWrapper: Self for chaining
        """
        sql = f"DROP TABLE IF EXISTS {table_name}"
        self.execute(sql)
        return self

    def create_table_with_index(
        self, table_name: str, columns: dict, indexes: list = None, **kwargs
    ) -> "TransactionWrapper":
        """
        Create a table with vector indexes within MatrixOne transaction.

        Args:
            table_name: Name of the table
            columns: Dictionary mapping column names to their types (same format as client.create_table)
            indexes: List of index definitions (same format as client.create_table_with_index)
            **kwargs: Additional table parameters

        Returns:
            TransactionWrapper: Self for chaining
        """
        from sqlalchemy.schema import CreateTable

        from .sqlalchemy_ext import VectorTableBuilder

        # Parse primary key from kwargs
        primary_key = kwargs.get("primary_key", None)

        # Create table using VectorTableBuilder
        builder = VectorTableBuilder(table_name)

        # Add columns based on simplified format (same logic as client.create_table)
        for column_name, column_def in columns.items():
            is_primary = primary_key == column_name

            if column_def.startswith("vector("):
                # Parse vector type: vector(128,f32) or vector(128)
                import re

                match = re.match(r"vector\((\d+)(?:,(\w+))?\)", column_def)
                if match:
                    dimension = int(match.group(1))
                    precision = match.group(2) or "f32"
                    builder.add_vector_column(column_name, dimension, precision)
                else:
                    raise ValueError(f"Invalid vector format: {column_def}")

            elif column_def.startswith("varchar("):
                # Parse varchar type: varchar(100)
                import re

                match = re.match(r"varchar\((\d+)\)", column_def)
                if match:
                    length = int(match.group(1))
                    builder.add_string_column(column_name, length)
                else:
                    raise ValueError(f"Invalid varchar format: {column_def}")

            elif column_def.startswith("char("):
                # Parse char type: char(10)
                import re

                match = re.match(r"char\((\d+)\)", column_def)
                if match:
                    length = int(match.group(1))
                    builder.add_string_column(column_name, length)
                else:
                    raise ValueError(f"Invalid char format: {column_def}")

            elif column_def.startswith("decimal("):
                # Parse decimal type: decimal(10,2)
                import re

                match = re.match(r"decimal\((\d+),(\d+)\)", column_def)
                if match:
                    precision = int(match.group(1))
                    scale = int(match.group(2))
                    builder.add_numeric_column(column_name, "decimal", precision, scale)
                else:
                    raise ValueError(f"Invalid decimal format: {column_def}")

            elif column_def.startswith("float("):
                # Parse float type: float(10)
                import re

                match = re.match(r"float\((\d+)\)", column_def)
                if match:
                    precision = int(match.group(1))
                    builder.add_numeric_column(column_name, "float", precision)
                else:
                    raise ValueError(f"Invalid float format: {column_def}")

            elif column_def in ("int", "integer"):
                builder.add_int_column(column_name, primary_key=is_primary)
            elif column_def in ("bigint", "bigint unsigned"):
                builder.add_bigint_column(column_name, primary_key=is_primary)
            elif column_def in ("smallint", "tinyint"):
                if column_def == "smallint":
                    builder.add_smallint_column(column_name, primary_key=is_primary)
                else:
                    builder.add_tinyint_column(column_name, primary_key=is_primary)
            elif column_def in ("text", "longtext", "mediumtext", "tinytext"):
                builder.add_text_column(column_name)
            elif column_def in ("float", "double"):
                builder.add_numeric_column(column_name, column_def)
            elif column_def in ("date", "datetime", "timestamp", "time"):
                builder.add_datetime_column(column_name, column_def)
            elif column_def in ("boolean", "bool"):
                builder.add_boolean_column(column_name)
            elif column_def in ("json", "jsonb"):
                builder.add_json_column(column_name)
            elif column_def in (
                "blob",
                "longblob",
                "mediumblob",
                "tinyblob",
                "binary",
                "varbinary",
            ):
                builder.add_binary_column(column_name, column_def)
            else:
                raise ValueError(
                    f"Unsupported column type '{column_def}' for column '{column_name}'. "
                    f"Supported types: int, bigint, smallint, tinyint, varchar(n), char(n), "
                    f"text, float, double, decimal(p,s), date, datetime, timestamp, time, "
                    f"boolean, json, blob, vecf32(n), vecf64(n)"
                )

        # Create table using transaction wrapper's execute method
        table = builder.build()
        create_sql = CreateTable(table)
        sql = str(create_sql.compile(dialect=self.client.get_sqlalchemy_engine().dialect))
        self.execute(sql)

        # Create indexes if specified
        if indexes:
            for index_def in indexes:
                index_name = index_def["name"]
                column_name = index_def["column"]
                index_type = index_def["type"]
                params = index_def.get("params", {})

                # Create the index using transaction wrapper's vector_index with separated APIs
                if index_type == "ivfflat":
                    self.vector_index.create_ivf(table_name=table_name, name=index_name, column=column_name, **params)
                elif index_type == "hnsw":
                    self.vector_index.create_hnsw(table_name=table_name, name=index_name, column=column_name, **params)
                else:
                    raise ValueError(f"Unsupported index type: {index_type}")

        return self

    def create_table_orm(self, table_name: str, *columns, **kwargs) -> "TransactionWrapper":
        """
        Create a table using SQLAlchemy ORM-style definitions within MatrixOne transaction.

        Args:
            table_name: Name of the table
            *columns: SQLAlchemy Column objects and Index objects (including VectorIndex)
            **kwargs: Additional parameters (like enable_hnsw, enable_ivf)

        Returns:
            TransactionWrapper: Self for chaining
        """
        from sqlalchemy import MetaData, Table
        from sqlalchemy.schema import CreateTable

        # Create metadata and table
        metadata = MetaData()
        table = Table(table_name, metadata, *columns)

        # Check if we need to enable HNSW or IVF indexing
        enable_hnsw = kwargs.get("enable_hnsw", False)
        enable_ivf = kwargs.get("enable_ivf", False)

        # Check if table has vector indexes that need special handling
        has_hnsw_index = False
        has_ivf_index = False

        for item in table.indexes:
            if hasattr(item, "index_type"):
                # Check for HNSW index type (string comparison)
                if str(item.index_type).lower() == "hnsw":
                    has_hnsw_index = True
                elif str(item.index_type).lower() == "ivfflat":
                    has_ivf_index = True

        # Enable appropriate indexing if needed (within transaction)
        if has_hnsw_index or enable_hnsw:
            from .sqlalchemy_ext import create_hnsw_config

            hnsw_config = create_hnsw_config(self.client._engine)
            hnsw_config.enable_hnsw_indexing()
        if has_ivf_index or enable_ivf:
            from .sqlalchemy_ext import create_ivf_config

            ivf_config = create_ivf_config(self.client._engine)
            ivf_config.enable_ivf_indexing()

        # Create table using transaction wrapper's execute method
        create_sql = CreateTable(table)
        sql = str(create_sql.compile(dialect=self.client.get_sqlalchemy_engine().dialect))
        self.execute(sql)

        return self


class TransactionSnapshotManager(SnapshotManager):
    """Snapshot manager that executes operations within a transaction"""

    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper

    def create(
        self,
        name: str,
        level: Union[str, SnapshotLevel],
        database: Optional[str] = None,
        table: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Snapshot:
        """Create snapshot within transaction"""
        return super().create(name, level, database, table, description, self.transaction_wrapper)

    def get(self, name: str) -> Snapshot:
        """Get snapshot within transaction"""
        return super().get(name, self.transaction_wrapper)

    def delete(self, name: str) -> None:
        """Delete snapshot within transaction"""
        return super().delete(name, self.transaction_wrapper)


class TransactionCloneManager(CloneManager):
    """Clone manager that executes operations within a transaction"""

    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper

    def clone_database(
        self,
        target_db: str,
        source_db: str,
        snapshot_name: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> None:
        """Clone database within transaction"""
        return super().clone_database(target_db, source_db, snapshot_name, if_not_exists, self.transaction_wrapper)

    def clone_table(
        self,
        target_table: str,
        source_table: str,
        snapshot_name: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> None:
        """Clone table within transaction"""
        return super().clone_table(target_table, source_table, snapshot_name, if_not_exists, self.transaction_wrapper)

    def clone_database_with_snapshot(
        self, target_db: str, source_db: str, snapshot_name: str, if_not_exists: bool = False
    ) -> None:
        """Clone database with snapshot within transaction"""
        return super().clone_database_with_snapshot(
            target_db, source_db, snapshot_name, if_not_exists, self.transaction_wrapper
        )

    def clone_table_with_snapshot(
        self, target_table: str, source_table: str, snapshot_name: str, if_not_exists: bool = False
    ) -> None:
        """Clone table with snapshot within transaction"""
        return super().clone_table_with_snapshot(
            target_table, source_table, snapshot_name, if_not_exists, self.transaction_wrapper
        )


class VectorManager:
    """
    Unified vector manager for MatrixOne vector operations and chain operations.

    This class provides comprehensive vector functionality including vector table
    creation, vector indexing, and vector data operations. It supports both
    IVF (Inverted File) and HNSW (Hierarchical Navigable Small World) indexing
    algorithms for efficient vector similarity search.

    Key Features:
    - Vector table creation with configurable dimensions and precision
    - Vector index creation and management (IVF, HNSW)
    - Vector data insertion and batch operations
    - Vector similarity search operations
    - Integration with MatrixOne's vector capabilities
    - Support for both f32 and f64 vector precision

    Supported Index Types:
    - IVF (Inverted File): Good for large datasets, requires training
    - HNSW: Good for high-dimensional vectors, no training required

    Supported Operations:
    - Vector table creation with various column types
    - Vector index creation with configurable parameters
    - Vector data insertion and batch operations
    - Vector similarity search and distance calculations
    - Vector index management and optimization

    Usage Examples:
        # Initialize vector manager
        vector = client.vector

        # Create vector table
        vector.create_table("documents", {
            "id": "int",
            "content": "text",
            "embedding": "vector(384,f32)"
        })

        # Create IVF index
        vector.create_ivf(
            table_name="documents",
            name="idx_embedding_ivf",
            column="embedding",
            lists=100
        )

        # Create HNSW index
        vector.create_hnsw(
            table_name="documents",
            name="idx_embedding_hnsw",
            column="embedding",
            m=16,
            ef_construction=200
        )

        # Insert vector data
        vector.insert("documents", {
            "id": 1,
            "content": "Sample document",
            "embedding": [0.1, 0.2, 0.3, ...]  # 384-dimensional vector
        })

        # Batch insert vector data
        documents = [
            {"id": 1, "content": "Doc 1", "embedding": [0.1, 0.2, ...]},
            {"id": 2, "content": "Doc 2", "embedding": [0.3, 0.4, ...]}
        ]
        vector.batch_insert("documents", documents)

    Note: Vector operations require appropriate vector data and indexing
    strategies. Vector dimensions and precision must match your embedding
    model requirements and indexing parameters.
    """

    def __init__(self, client):
        self.client = client

    def create_ivf(
        self,
        table_name: str,
        name: str,
        column: str,
        lists: int = 100,
        op_type: VectorOpType = None,
    ) -> "VectorManager":
        """
        Create an IVFFLAT vector index using chain operations.

        Args:
            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            lists: Number of lists for IVFFLAT (default: 100)
            op_type: Vector operation type (VectorOpType enum, default: VectorOpType.VECTOR_L2_OPS)

        Returns:
            VectorManager: Self for chaining
        """
        from .sqlalchemy_ext import IVFVectorIndex, VectorOpType

        # Use default if not provided
        if op_type is None:
            op_type = VectorOpType.VECTOR_L2_OPS

        success = IVFVectorIndex.create_index(
            engine=self.client.get_sqlalchemy_engine(),
            table_name=table_name,
            name=name,
            column=column,
            lists=lists,
            op_type=op_type,
        )

        if not success:
            raise Exception(f"Failed to create IVFFLAT vector index {name} on table {table_name}")

        return self

    def create_hnsw(
        self,
        table_name: str,
        name: str,
        column: str,
        m: int = 16,
        ef_construction: int = 200,
        ef_search: int = 50,
        op_type: VectorOpType = None,
    ) -> "VectorManager":
        """
        Create an HNSW vector index using chain operations.

        Args:
            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            m: Number of bi-directional links for HNSW (default: 16)
            ef_construction: Size of dynamic candidate list for HNSW construction (default: 200)
            ef_search: Size of dynamic candidate list for HNSW search (default: 50)
            op_type: Vector operation type (VectorOpType enum, default: VectorOpType.VECTOR_L2_OPS)

        Returns:
            VectorManager: Self for chaining
        """
        from .sqlalchemy_ext import HnswVectorIndex, VectorOpType

        # Use default if not provided
        if op_type is None:
            op_type = VectorOpType.VECTOR_L2_OPS

        success = HnswVectorIndex.create_index(
            engine=self.client.get_sqlalchemy_engine(),
            table_name=table_name,
            name=name,
            column=column,
            m=m,
            ef_construction=ef_construction,
            ef_search=ef_search,
            op_type=op_type,
        )

        if not success:
            raise Exception(f"Failed to create HNSW vector index {name} on table {table_name}")

        return self

    def create_ivf_in_transaction(
        self,
        table_name: str,
        name: str,
        column: str,
        connection,
        lists: int = 100,
        op_type: str = "vector_l2_ops",
    ) -> "VectorManager":
        """
        Create an IVFFLAT vector index within an existing SQLAlchemy transaction.

        Args:
            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            connection: SQLAlchemy connection object (required for transaction support)
            lists: Number of lists for IVFFLAT (default: 100)
            op_type: Vector operation type (default: vector_l2_ops)

        Returns:
            VectorManager: Self for chaining

        Raises:
            ValueError: If connection is not provided
        """
        if connection is None:
            raise ValueError("connection parameter is required for transaction operations")

        from sqlalchemy import text

        # Enable IVF indexing if needed
        from .sqlalchemy_ext import create_ivf_config

        ivf_config = create_ivf_config(self.client._engine)
        ivf_config.enable_ivf_indexing()
        ivf_config.set_probe_limit(1)

        # Build CREATE INDEX statement
        sql = f"CREATE INDEX {name} USING ivfflat ON {table_name}({column}) LISTS {lists} op_type '{op_type}'"

        # Create the index
        connection.execute(text(sql))

        return self

    def create_hnsw_in_transaction(
        self,
        table_name: str,
        name: str,
        column: str,
        connection,
        m: int = 16,
        ef_construction: int = 200,
        ef_search: int = 50,
        op_type: str = "vector_l2_ops",
    ) -> "VectorManager":
        """
        Create an HNSW vector index within an existing SQLAlchemy transaction.

        Args:
            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            connection: SQLAlchemy connection object (required for transaction support)
            m: Number of bi-directional links for HNSW (default: 16)
            ef_construction: Size of dynamic candidate list for HNSW construction (default: 200)
            ef_search: Size of dynamic candidate list for HNSW search (default: 50)
            op_type: Vector operation type (default: vector_l2_ops)

        Returns:
            VectorManager: Self for chaining

        Raises:
            ValueError: If connection is not provided
        """
        if connection is None:
            raise ValueError("connection parameter is required for transaction operations")

        from sqlalchemy import text

        # Enable HNSW indexing if needed
        from .sqlalchemy_ext import create_hnsw_config

        hnsw_config = create_hnsw_config(self.client._engine)
        hnsw_config.enable_hnsw_indexing(connection)

        # Build CREATE INDEX statement
        sql = (
            f"CREATE INDEX {name} USING hnsw ON {table_name}({column}) "
            f"M {m} EF_CONSTRUCTION {ef_construction} EF_SEARCH {ef_search} "
            f"op_type '{op_type}'"
        )

        # Create the index
        connection.execute(text(sql))

        return self

    def drop(self, table_name: str, name: str) -> "VectorManager":
        """
        Drop a vector index using chain operations.

        Args:
            table_name: Name of the table
            name: Name of the index to drop

        Returns:
            VectorManager: Self for chaining
        """
        from .sqlalchemy_ext import VectorIndex

        success = VectorIndex.drop_index(engine=self.client.get_sqlalchemy_engine(), table_name=table_name, name=name)

        if not success:
            raise Exception(f"Failed to drop vector index {name} from table {table_name}")

        return self

    def enable_ivf(self, probe_limit: int = 1) -> "VectorManager":
        """
        Enable IVF indexing with chain operations.

        Args:
            probe_limit: Probe limit for IVF search

        Returns:
            VectorManager: Self for chaining
        """
        from .sqlalchemy_ext import create_ivf_config

        ivf_config = create_ivf_config(self.client.get_sqlalchemy_engine())
        if not ivf_config.is_ivf_supported():
            raise Exception("IVF indexing is not supported in this MatrixOne version")

        if not ivf_config.enable_ivf_indexing():
            raise Exception("Failed to enable IVF indexing")

        if not ivf_config.set_probe_limit(probe_limit):
            raise Exception("Failed to set probe limit")

        return self

    def disable_ivf(self) -> "VectorManager":
        """
        Disable IVF indexing with chain operations.

        Returns:
            VectorManager: Self for chaining
        """
        from .sqlalchemy_ext import create_ivf_config

        ivf_config = create_ivf_config(self.client.get_sqlalchemy_engine())
        if not ivf_config.disable_ivf_indexing():
            raise Exception("Failed to disable IVF indexing")

        return self

    def enable_hnsw(self) -> "VectorManager":
        """
        Enable HNSW indexing with chain operations.

        Returns:
            VectorManager: Self for chaining
        """
        from .sqlalchemy_ext import create_hnsw_config

        hnsw_config = create_hnsw_config(self.client.get_sqlalchemy_engine())
        if not hnsw_config.enable_hnsw_indexing():
            raise Exception("Failed to enable HNSW indexing")

        return self

    def disable_hnsw(self) -> "VectorManager":
        """
        Disable HNSW indexing with chain operations.

        Returns:
            VectorManager: Self for chaining
        """
        from .sqlalchemy_ext import create_hnsw_config

        hnsw_config = create_hnsw_config(self.client.get_sqlalchemy_engine())
        if not hnsw_config.disable_hnsw_indexing():
            raise Exception("Failed to disable HNSW indexing")

        return self

    # Data operations
    def insert(self, table_name: str, data: dict) -> "VectorManager":
        """
        Insert vector data using chain operations.

        Args:
            table_name: Name of the table
            data: Data to insert (dict with column names as keys)

        Returns:
            VectorManager: Self for chaining
        """
        self.client.insert(table_name, data)
        return self

    def insert_in_transaction(self, table_name: str, data: dict, connection) -> "VectorManager":
        """
        Insert vector data within an existing SQLAlchemy transaction.

        Args:
            table_name: Name of the table
            data: Data to insert (dict with column names as keys)
            connection: SQLAlchemy connection object (required for transaction support)

        Returns:
            VectorManager: Self for chaining

        Raises:
            ValueError: If connection is not provided
        """
        if connection is None:
            raise ValueError("connection parameter is required for transaction operations")

        # Use client's insert method but execute within transaction
        from sqlalchemy import text

        # Build INSERT statement
        columns = list(data.keys())
        values = list(data.values())

        # Convert vectors to string format
        formatted_values = []
        for value in values:
            if isinstance(value, list):
                formatted_values.append("[" + ",".join(map(str, value)) + "]")
            else:
                formatted_values.append(str(value))

        columns_str = ", ".join(columns)
        values_str = ", ".join([f"'{v}'" for v in formatted_values])

        sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
        connection.execute(text(sql))

        return self

    def batch_insert(self, table_name: str, data_list: list) -> "VectorManager":
        """
        Batch insert vector data using chain operations.

        Args:
            table_name: Name of the table
            data_list: List of data dictionaries to insert

        Returns:
            VectorManager: Self for chaining
        """
        self.client.batch_insert(table_name, data_list)
        return self


class TransactionVectorIndexManager(VectorManager):
    """Vector index manager that executes operations within a transaction"""

    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper

    def execute(self, sql: str, params: Optional[Tuple] = None) -> ResultSet:
        """Execute SQL within transaction"""
        return self.transaction_wrapper.execute(sql, params)

    def create_ivf(
        self,
        table_name: str,
        name: str,
        column: str,
        lists: int = 100,
        op_type: VectorOpType = None,
    ) -> "TransactionVectorIndexManager":
        """Create an IVFFLAT vector index within transaction"""
        from .sqlalchemy_ext import VectorIndex, VectorIndexType, VectorOpType

        # Use default if not provided
        index_type = VectorIndexType.IVFFLAT
        if op_type is None:
            op_type = VectorOpType.VECTOR_L2_OPS

        # Create index using transaction wrapper's execute method
        index = VectorIndex(name, column, index_type, lists, op_type)

        try:
            # Enable IVF indexing within transaction
            self.transaction_wrapper.execute("SET experimental_ivf_index = 1")
            self.transaction_wrapper.execute("SET probe_limit = 1")

            sql = index.create_sql(table_name)
            self.transaction_wrapper.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to create IVFFLAT vector index {name} on table {table_name} in transaction: {e}")

    def create_hnsw(
        self,
        table_name: str,
        name: str,
        column: str,
        m: int = 16,
        ef_construction: int = 200,
        ef_search: int = 50,
        op_type: VectorOpType = None,
    ) -> "TransactionVectorIndexManager":
        """Create an HNSW vector index within transaction"""
        from .sqlalchemy_ext import VectorIndex, VectorIndexType, VectorOpType

        # Use default if not provided
        index_type = VectorIndexType.HNSW
        if op_type is None:
            op_type = VectorOpType.VECTOR_L2_OPS

        # Create index using transaction wrapper's execute method
        index = VectorIndex(name, column, index_type, None, op_type, m, ef_construction, ef_search)

        try:
            # Enable HNSW indexing within transaction
            self.transaction_wrapper.execute("SET experimental_hnsw_index = 1")

            sql = index.create_sql(table_name)
            self.transaction_wrapper.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to create HNSW vector index {name} on table {table_name} in transaction: {e}")

    def drop(self, table_name: str, name: str) -> "TransactionVectorIndexManager":
        """Drop a vector index within transaction"""
        # Drop index using transaction wrapper's execute method
        sql = f"DROP INDEX {name} ON {table_name}"

        try:
            self.transaction_wrapper.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to drop vector index {name} from table {table_name} in transaction: {e}")


class VectorQueryManager:
    """
    Vector query manager for MatrixOne vector similarity search operations.

    This class provides comprehensive vector query functionality including
    similarity search, distance calculations, and vector-based filtering.
    It supports various distance metrics and provides efficient vector
    search capabilities using MatrixOne's vector indexing.

    Key Features:
    - Vector similarity search with configurable distance metrics
    - Support for multiple distance functions (L2, cosine, inner product)
    - Vector-based filtering and querying
    - Integration with vector indexes for optimal performance
    - Support for both f32 and f64 vector precision
    - Chain operations for complex vector queries

    Supported Distance Metrics:
    - L2 (Euclidean) distance: Standard Euclidean distance
    - Cosine similarity: Cosine of the angle between vectors
    - Inner product: Dot product of vectors
    - Negative inner product: Negative dot product for similarity

    Supported Operations:
    - Vector similarity search with ranking
    - Vector distance calculations
    - Vector-based filtering in queries
    - Integration with fulltext and other search capabilities
    - Vector query optimization and performance tuning

    Usage Examples:
        # Initialize vector query manager
        vector_query = client.vector_query

        # Similarity search with L2 distance
        results = vector_query.similarity_search(
            table_name="documents",
            vector_column="embedding",
            query_vector=[0.1, 0.2, 0.3, ...],  # 384-dimensional vector
            limit=10,
            distance_function="l2"
        )

        # Similarity search with cosine similarity
        results = vector_query.similarity_search(
            table_name="products",
            vector_column="features",
            query_vector=[0.5, 0.6, 0.7, ...],
            limit=5,
            distance_function="cosine"
        )

        # Vector-based filtering in queries
        query = client.query(Document)
        results = (query
                  .filter(vector_query.within_distance(
                      "embedding", [0.1, 0.2, 0.3, ...], 0.5, "l2"
                  ))
                  .limit(20)
                  .all())

        # Get vector distance between two vectors
        distance = vector_query.calculate_distance(
            vector1=[0.1, 0.2, 0.3, ...],
            vector2=[0.4, 0.5, 0.6, ...],
            distance_function="cosine"
        )

    Note: Vector queries require appropriate vector indexes for optimal
    performance. Choose distance metrics based on your similarity
    requirements and data characteristics.
    """

    def __init__(self, client):
        self.client = client

    def similarity_search(
        self,
        table_name: str,
        vector_column: str,
        query_vector: list,
        limit: int = 10,
        distance_type: str = "l2",
        select_columns: list = None,
        where_conditions: list = None,
        where_params: list = None,
        connection=None,
    ) -> list:
        """
        Perform similarity search using chain operations.

        Args:
            table_name: Name of the table
            vector_column: Name of the vector column
            query_vector: Query vector as list
            limit: Number of results to return
            distance_type: Type of distance calculation (l2, cosine, inner_product)
            select_columns: List of columns to select (None means all columns)
            where_conditions: List of WHERE conditions
            where_params: List of parameters for WHERE conditions
            connection: Optional existing database connection (for transaction support)

        Returns:
            List of search results
        """
        from sqlalchemy import text

        from .sql_builder import DistanceFunction, build_vector_similarity_query

        # Convert distance type to enum
        if distance_type == "l2":
            distance_func = DistanceFunction.L2_SQ
        elif distance_type == "cosine":
            distance_func = DistanceFunction.COSINE
        elif distance_type == "inner_product":
            distance_func = DistanceFunction.INNER_PRODUCT
        else:
            raise ValueError(f"Unsupported distance type: {distance_type}")

        # Build query using unified SQL builder
        sql = build_vector_similarity_query(
            table_name=table_name,
            vector_column=vector_column,
            query_vector=query_vector,
            distance_func=distance_func,
            limit=limit,
            select_columns=select_columns,
            where_conditions=where_conditions,
            where_params=where_params,
        )

        if connection is not None:
            # Use existing connection (for transaction support)
            result = connection.execute(text(sql))
            return result.fetchall()
        else:
            # Create new connection
            with self.client.get_sqlalchemy_engine().begin() as conn:
                result = conn.execute(text(sql))
                return result.fetchall()

    def similarity_search_in_transaction(
        self,
        table_name: str,
        vector_column: str,
        query_vector: list,
        limit: int = 10,
        distance_type: str = "l2",
        select_columns: list = None,
        where_conditions: list = None,
        where_params: list = None,
        connection=None,
    ) -> list:
        """
        Perform similarity search within a transaction.

        Args:
            table_name: Name of the table
            vector_column: Name of the vector column
            query_vector: Query vector as list
            limit: Number of results to return
            distance_type: Type of distance calculation (l2, cosine, inner_product)
            select_columns: List of columns to select (None means all columns)
            where_conditions: List of WHERE conditions
            where_params: List of parameters for WHERE conditions
            connection: Database connection (required for transaction support)

        Returns:
            List of search results

        Raises:
            ValueError: If connection is not provided
        """
        if connection is None:
            raise ValueError("connection parameter is required for transaction operations")

        return self.similarity_search(
            table_name=table_name,
            vector_column=vector_column,
            query_vector=query_vector,
            limit=limit,
            distance_type=distance_type,
            select_columns=select_columns,
            where_conditions=where_conditions,
            where_params=where_params,
            connection=connection,
        )

    def range_search(
        self,
        table_name: str,
        vector_column: str,
        query_vector: list,
        max_distance: float,
        distance_type: str = "l2",
        select_columns: list = None,
        connection=None,
    ) -> list:
        """
        Perform range search using chain operations.

        Args:
            table_name: Name of the table
            vector_column: Name of the vector column
            query_vector: Query vector as list
            max_distance: Maximum distance threshold
            distance_type: Type of distance calculation
            select_columns: List of columns to select (None means all columns)
            connection: Optional existing database connection (for transaction support)

        Returns:
            List of search results within range
        """
        from sqlalchemy import text

        # Convert vector to string format
        vector_str = "[" + ",".join(map(str, query_vector)) + "]"

        # Build distance function based on type
        if distance_type == "l2":
            distance_func = "l2_distance"
        elif distance_type == "cosine":
            distance_func = "cosine_distance"
        elif distance_type == "inner_product":
            distance_func = "inner_product"
        else:
            raise ValueError(f"Unsupported distance type: {distance_type}")

        # Build SELECT clause
        if select_columns is None:
            select_clause = "*"
        else:
            # Ensure vector_column is included for distance calculation
            columns_to_select = list(select_columns)
            if vector_column not in columns_to_select:
                columns_to_select.append(vector_column)
            select_clause = ", ".join(columns_to_select)

        # Build query
        sql = f"""
        SELECT {select_clause}, {distance_func}({vector_column}, '{vector_str}') as distance
        FROM {table_name}
        WHERE {distance_func}({vector_column}, '{vector_str}') <= {max_distance}
        ORDER BY distance
        """

        if connection is not None:
            # Use existing connection (for transaction support)
            result = connection.execute(text(sql))
            return result.fetchall()
        else:
            # Create new connection
            with self.client.get_sqlalchemy_engine().begin() as conn:
                result = conn.execute(text(sql))
                return result.fetchall()

    def range_search_in_transaction(
        self,
        table_name: str,
        vector_column: str,
        query_vector: list,
        max_distance: float,
        distance_type: str = "l2",
        select_columns: list = None,
        connection=None,
    ) -> list:
        """
        Perform range search within a transaction.

        Args:
            table_name: Name of the table
            vector_column: Name of the vector column
            query_vector: Query vector as list
            max_distance: Maximum distance threshold
            distance_type: Type of distance calculation
            select_columns: List of columns to select (None means all columns)
            connection: Database connection (required for transaction support)

        Returns:
            List of search results within range

        Raises:
            ValueError: If connection is not provided
        """
        if connection is None:
            raise ValueError("connection parameter is required for transaction operations")

        return self.range_search(
            table_name=table_name,
            vector_column=vector_column,
            query_vector=query_vector,
            max_distance=max_distance,
            distance_type=distance_type,
            select_columns=select_columns,
            connection=connection,
        )


class TransactionVectorQueryManager:
    """Vector query manager for transaction chain operations"""

    def __init__(self, client, transaction_wrapper):
        self.client = client
        self.transaction_wrapper = transaction_wrapper

    def execute(self, sql: str, params: Optional[Tuple] = None) -> ResultSet:
        """Execute SQL within transaction"""
        return self.transaction_wrapper.execute(sql, params)

    def similarity_search(
        self,
        table_name: str,
        vector_column: str,
        query_vector: list,
        limit: int = 10,
        distance_type: str = "l2",
        select_columns: list = None,
    ) -> list:
        """Perform similarity search within transaction"""
        # Convert vector to string format
        vector_str = "[" + ",".join(map(str, query_vector)) + "]"

        # Build distance function based on type
        if distance_type == "l2":
            distance_func = "l2_distance"
        elif distance_type == "cosine":
            distance_func = "cosine_distance"
        elif distance_type == "inner_product":
            distance_func = "inner_product"
        else:
            raise ValueError(f"Unsupported distance type: {distance_type}")

        # Build SELECT clause
        if select_columns is None:
            select_clause = "*"
        else:
            # Ensure vector_column is included for distance calculation
            columns_to_select = list(select_columns)
            if vector_column not in columns_to_select:
                columns_to_select.append(vector_column)
            select_clause = ", ".join(columns_to_select)

        # Build query
        sql = f"""
        SELECT {select_clause}, {distance_func}({vector_column}, '{vector_str}') as distance
        FROM {table_name}
        ORDER BY distance
        LIMIT {limit}
        """

        return self.transaction_wrapper.execute(sql)


class SimpleFulltextQueryBuilder:
    """
    Simplified fulltext query builder for common search operations.

    Provides easy-to-use interfaces for basic fulltext search without requiring
    deep understanding of fulltext search internals. Focuses on the most common
    patterns supported by MatrixOne.

    Supported search patterns from MatrixOne test cases:
    - Natural language: MATCH(col1, col2) AGAINST('query')
    - Boolean mode: MATCH(col1, col2) AGAINST('+required -excluded' IN BOOLEAN MODE)
    - With scoring: SELECT *, MATCH(col1, col2) AGAINST('query') AS score
    """

    def __init__(self, client: "Client", table_or_columns):
        """
        Initialize simple fulltext query builder.

        Args:
            client: MatrixOne client instance
            table_or_columns: Either a table name/model or specific columns
        """
        self.client = client
        self._is_async = hasattr(client, '_is_async') or 'Async' in client.__class__.__name__
        self._table_name = None
        self._columns = []
        self._query_text = ""
        self._mode = "natural"  # natural or boolean
        self._with_score = False
        self._score_alias = "score"
        self._where_conditions = []
        self._order_by = None
        self._limit = None
        self._offset = None

        # Parse table or columns
        if hasattr(table_or_columns, '__tablename__'):
            # SQLAlchemy model
            self._table_name = table_or_columns.__tablename__
            # For SQLAlchemy models, we'll detect fulltext columns later if needed
        elif hasattr(table_or_columns, '__iter__') and not isinstance(table_or_columns, str):
            # List of columns - extract table from first column
            first_col = table_or_columns[0] if table_or_columns else None
            if hasattr(first_col, 'table'):
                self._table_name = first_col.table.name
                self._columns = [col.name if hasattr(col, 'name') else str(col) for col in table_or_columns]
            else:
                # String column names - need table to be set separately
                self._columns = [str(col) for col in table_or_columns]
        else:
            # Single table name
            self._table_name = str(table_or_columns)
            # For table names, we'll detect fulltext columns later if needed

    def search(self, query: str) -> "SimpleFulltextQueryBuilder":
        """
        Set natural language search query.

        Args:
            query: Natural language search terms

        Returns:
            Self for chaining

        Example:
            builder.search("machine learning tutorial")
            # Generates: MATCH(columns) AGAINST('machine learning tutorial')
        """
        self._query_text = query
        self._mode = "natural"
        return self

    def must_have(self, *terms: str) -> "SimpleFulltextQueryBuilder":
        """
        Set required terms (boolean mode with + operator).

        Args:
            terms: Required search terms

        Returns:
            Self for chaining

        Example:
            builder.must_have("python", "tutorial")
            # Generates: MATCH(columns) AGAINST('+python +tutorial' IN BOOLEAN MODE)
        """
        required_terms = " ".join(f"+{term}" for term in terms)
        self._query_text = required_terms
        self._mode = "boolean"
        return self

    def must_not_have(self, *terms: str) -> "SimpleFulltextQueryBuilder":
        """
        Add excluded terms to current query (boolean mode with - operator).

        Args:
            terms: Terms to exclude

        Returns:
            Self for chaining

        Example:
            builder.must_have("python").must_not_have("deprecated")
            # Generates: MATCH(columns) AGAINST('+python -deprecated' IN BOOLEAN MODE)
        """
        excluded_terms = " ".join(f"-{term}" for term in terms)
        if self._query_text:
            self._query_text += f" {excluded_terms}"
        else:
            self._query_text = excluded_terms
        self._mode = "boolean"
        return self

    def with_score(self, alias: str = "score") -> "SimpleFulltextQueryBuilder":
        """
        Include relevance score in results.

        Args:
            alias: Column alias for the score

        Returns:
            Self for chaining

        Example:
            builder.search("python").with_score()
            # Generates: SELECT *, MATCH(columns) AGAINST('python') AS score
        """
        self._with_score = True
        self._score_alias = alias
        return self

    def where(self, condition: str) -> "SimpleFulltextQueryBuilder":
        """
        Add additional WHERE conditions.

        Args:
            condition: SQL WHERE condition

        Returns:
            Self for chaining
        """
        self._where_conditions.append(condition)
        return self

    def order_by_score(self, desc: bool = True) -> "SimpleFulltextQueryBuilder":
        """
        Order results by relevance score.

        Args:
            desc: Whether to sort in descending order (highest score first)

        Returns:
            Self for chaining
        """
        direction = "DESC" if desc else "ASC"
        self._order_by = f"{self._score_alias} {direction}"
        return self

    def order_by(self, column: str, desc: bool = False) -> "SimpleFulltextQueryBuilder":
        """
        Order results by specified column.

        Args:
            column: Column name to sort by
            desc: Whether to sort in descending order

        Returns:
            Self for chaining
        """
        direction = "DESC" if desc else "ASC"
        self._order_by = f"{column} {direction}"
        return self

    def limit(self, count: int, offset: int = 0) -> "SimpleFulltextQueryBuilder":
        """
        Limit number of results.

        Args:
            count: Maximum number of results
            offset: Number of results to skip

        Returns:
            Self for chaining
        """
        self._limit = count
        self._offset = offset
        return self

    def columns(self, *columns: str) -> "SimpleFulltextQueryBuilder":
        """
        Specify which columns to search in.

        Args:
            columns: Column names to search

        Returns:
            Self for chaining

        Example:
            builder.columns("title", "content").search("python")
        """
        self._columns = list(columns)
        return self

    def build_sql(self) -> str:
        """Build the final SQL query."""
        if not self._table_name:
            raise ValueError("Table name is required")
        if not self._columns:
            raise ValueError("Search columns must be specified. Use .columns('col1', 'col2') to specify columns.")
        if not self._query_text:
            raise ValueError("Search query is required. Use .search('query') or .must_have('term') to specify search terms.")

        # Build MATCH AGAINST clause
        columns_str = ", ".join(self._columns)

        # Build MATCH AGAINST clause
        if self._mode == "natural":
            match_clause = f"MATCH({columns_str}) AGAINST('{self._query_text}')"
        else:  # boolean
            match_clause = f"MATCH({columns_str}) AGAINST('{self._query_text}' IN BOOLEAN MODE)"

        # Build SELECT clause - MatrixOne doesn't support SELECT * with fulltext
        # Use all columns from the table instead
        if self._with_score:
            select_clause = f"SELECT *, {match_clause} AS {self._score_alias}"
        else:
            # For now, use SELECT * but this may need to be changed based on MatrixOne requirements
            select_clause = "SELECT *"

        # Build WHERE clause
        where_parts = [match_clause]
        where_parts.extend(self._where_conditions)
        where_clause = " AND ".join(where_parts)

        # Assemble final query
        sql = f"{select_clause} FROM {self._table_name} WHERE {where_clause}"

        if self._order_by:
            sql += f" ORDER BY {self._order_by}"

        if self._limit:
            sql += f" LIMIT {self._limit}"
            if self._offset:
                sql += f" OFFSET {self._offset}"

        return sql

    def execute(self):
        """Execute the query and return results."""
        sql = self.build_sql()
        if self._is_async:
            # For async client, this should be called with await
            return self.client.execute(sql)
        else:
            # For sync client
            return self.client.execute(sql)

    def explain(self) -> str:
        """Get the SQL query without executing it."""
        return self.build_sql()


class FulltextIndexManager:
    """
    Fulltext index manager for MatrixOne fulltext search operations.

    This class provides comprehensive fulltext indexing functionality for
    enabling fast text search capabilities in MatrixOne databases. It supports
    various fulltext algorithms and provides chain operations for efficient
    index management.

    Key Features:
    - Fulltext index creation and management
    - Support for multiple fulltext algorithms (TF-IDF, BM25)
    - Multi-column fulltext indexing
    - Index optimization and maintenance
    - Integration with MatrixOne's fulltext search capabilities
    - Chain operations for efficient index management

    Supported Algorithms:
    - TF-IDF: Term Frequency-Inverse Document Frequency (default)
    - BM25: Best Matching 25 algorithm for improved relevance scoring

    Supported Operations:
    - Create fulltext indexes on single or multiple columns
    - Drop fulltext indexes
    - List and query existing fulltext indexes
    - Index optimization and maintenance
    - Integration with fulltext search queries

    Usage Examples:
        # Initialize fulltext index manager
        fulltext = client.fulltext_index

        # Create fulltext index on single column
        fulltext.create(
            table_name="documents",
            name="idx_content",
            columns="content",
            algorithm="BM25"
        )

        # Create fulltext index on multiple columns
        fulltext.create(
            table_name="articles",
            name="idx_title_content",
            columns=["title", "content"],
            algorithm="TF-IDF"
        )

        # Drop fulltext index
        fulltext.drop("documents", "idx_content")

        # List fulltext indexes
        indexes = fulltext.list("documents")

        # Check if index exists
        exists = fulltext.exists("documents", "idx_content")

        # Get index information
        info = fulltext.get_info("documents", "idx_content")

    Note: Fulltext indexes significantly improve text search performance
    but require additional storage space. Choose appropriate algorithms
    based on your search requirements and data characteristics.
    """

    def __init__(self, client: "Client"):
        """Initialize fulltext index manager"""
        self.client = client

    def create(
        self, table_name: str, name: str, columns: Union[str, List[str]], algorithm: str = "TF-IDF"
    ) -> "FulltextIndexManager":
        """
        Create a fulltext index using chain operations.

        Args:
            table_name: Target table name
            name: Index name
            columns: Column(s) to index
            algorithm: Fulltext algorithm type (TF-IDF or BM25)

        Returns:
            FulltextIndexManager: Self for chaining
        """
        from .sqlalchemy_ext import FulltextIndex

        success = FulltextIndex.create_index(
            engine=self.client.get_sqlalchemy_engine(),
            table_name=table_name,
            name=name,
            columns=columns,
            algorithm=algorithm,
        )

        if not success:
            raise Exception(f"Failed to create fulltext index {name} on table {table_name}")

        return self

    def create_in_transaction(
        self,
        transaction_wrapper,
        table_name: str,
        name: str,
        columns: Union[str, List[str]],
        algorithm: str = "TF-IDF",
    ) -> "FulltextIndexManager":
        """
        Create a fulltext index within an existing transaction.

        Args:
            transaction_wrapper: Transaction wrapper
            table_name: Target table name
            name: Index name
            columns: Column(s) to index
            algorithm: Fulltext algorithm type

        Returns:
            FulltextIndexManager: Self for chaining
        """
        from .sqlalchemy_ext import FulltextIndex

        success = FulltextIndex.create_index_in_transaction(
            connection=transaction_wrapper.connection,
            table_name=table_name,
            name=name,
            columns=columns,
            algorithm=algorithm,
        )

        if not success:
            raise Exception(f"Failed to create fulltext index {name} on table {table_name} in transaction")

        return self

    def drop(self, table_name: str, name: str) -> "FulltextIndexManager":
        """
        Drop a fulltext index using chain operations.

        Args:
            table_name: Target table name
            name: Index name

        Returns:
            FulltextIndexManager: Self for chaining
        """
        from .sqlalchemy_ext import FulltextIndex

        success = FulltextIndex.drop_index(engine=self.client.get_sqlalchemy_engine(), table_name=table_name, name=name)

        if not success:
            raise Exception(f"Failed to drop fulltext index {name} from table {table_name}")

        return self

    def enable_fulltext(self) -> "FulltextIndexManager":
        """
        Enable fulltext indexing with chain operations.

        Returns:
            FulltextIndexManager: Self for chaining
        """
        try:
            self.client.execute("SET experimental_fulltext_index = 1")
            return self
        except Exception as e:
            raise Exception(f"Failed to enable fulltext indexing: {e}")

    def disable_fulltext(self) -> "FulltextIndexManager":
        """
        Disable fulltext indexing with chain operations.

        Returns:
            FulltextIndexManager: Self for chaining
        """
        try:
            self.client.execute("SET experimental_fulltext_index = 0")
            return self
        except Exception as e:
            raise Exception(f"Failed to disable fulltext indexing: {e}")

    def simple_query(self, table_or_columns):
        """
        Create a simplified fulltext query builder for common search operations.

        This is the recommended way to perform fulltext search with MatrixOne.
        Provides an easy-to-use interface without requiring deep understanding
        of fulltext search internals.

        Args:
            table_or_columns: Either:
                - A table name (string): All fulltext-indexed columns will be searched
                - A SQLAlchemy model class: All fulltext-indexed columns will be searched
                - Specific columns (list): Table.col1, Table.col2, etc.

        Returns:
            SimpleFulltextQueryBuilder: Simplified query builder instance

        Examples:
            # Search all fulltext columns in a table
            results = client.fulltext_index.simple_query("articles") \\
                .search("machine learning") \\
                .with_score() \\
                .order_by_score() \\
                .limit(10) \\
                .execute()

            # Search specific columns
            results = client.fulltext_index.simple_query(["title", "content"]) \\
                .must_have("python", "tutorial") \\
                .must_not_have("deprecated") \\
                .execute()

            # Using SQLAlchemy model
            results = client.fulltext_index.simple_query(Article) \\
                .search("data science") \\
                .where("category = 'Technology'") \\
                .execute()
        """
        return SimpleFulltextQueryBuilder(self.client, table_or_columns)


class TransactionFulltextIndexManager(FulltextIndexManager):
    """Fulltext index manager that executes operations within a transaction"""

    def __init__(self, client: "Client", transaction_wrapper):
        """Initialize transaction fulltext index manager"""
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper

    def create(
        self, table_name: str, name: str, columns: Union[str, List[str]], algorithm: str = "TF-IDF"
    ) -> "TransactionFulltextIndexManager":
        """Create a fulltext index within transaction"""
        try:
            if isinstance(columns, str):
                columns = [columns]

            columns_str = ", ".join(columns)
            sql = f"CREATE FULLTEXT INDEX {name} ON {table_name} ({columns_str})"

            self.transaction_wrapper.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to create fulltext index {name} on table {table_name} in transaction: {e}")

    def drop(self, table_name: str, name: str) -> "TransactionFulltextIndexManager":
        """Drop a fulltext index within transaction"""
        try:
            sql = f"DROP INDEX {name} ON {table_name}"
            self.transaction_wrapper.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to drop fulltext index {name} from table {table_name} in transaction: {e}")

    def simple_query(self, table_or_columns):
        """
        Create a simplified fulltext query builder for transaction operations.

        Returns a transaction-aware query builder that executes within the current transaction.

        Args:
            table_or_columns: Either a table name, ORM model class, or list of columns

        Returns:
            TransactionSimpleFulltextQueryBuilder: Transaction-aware query builder
        """
        return TransactionSimpleFulltextQueryBuilder(self.client, table_or_columns, self.transaction_wrapper)


class TransactionSimpleFulltextQueryBuilder(SimpleFulltextQueryBuilder):
    """Transaction-aware simple fulltext query builder."""

    def __init__(self, client: "Client", table_or_columns, transaction_wrapper):
        """Initialize transaction-aware query builder."""
        super().__init__(client, table_or_columns)
        self.transaction_wrapper = transaction_wrapper

    def execute(self):
        """Execute the query within the transaction."""
        sql = self.build_sql()
        if self._is_async:
            # For async transaction, this should be called with await
            return self.transaction_wrapper.execute(sql)
        else:
            # For sync transaction
            return self.transaction_wrapper.execute(sql)
