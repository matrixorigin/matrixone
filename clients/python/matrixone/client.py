"""
MatrixOne Client - Basic implementation
"""

from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple, Union

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .account import AccountManager, TransactionAccountManager
from .exceptions import ConnectionError, QueryError
from .logger import MatrixOneLogger, create_default_logger
from .moctl import MoCtlManager
from .pitr import PitrManager, TransactionPitrManager
from .pubsub import PubSubManager, TransactionPubSubManager
from .restore import RestoreManager, TransactionRestoreManager
from .snapshot import (CloneManager, Snapshot, SnapshotLevel, SnapshotManager,
                       SnapshotQueryBuilder)
from .sqlalchemy_ext import MatrixOneDialect
from .version import get_version_manager


class Client:
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
        self._vector_index = VectorIndexManager(self)
        self._vector_query = VectorQueryManager(self)
        self._vector_data = VectorDataManager(self)

    def disconnect(self) -> None:
        """Disconnect from MatrixOne database and dispose engine"""
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
        """Get parsed login information"""
        return self._login_info

    def _escape_identifier(self, identifier: str) -> str:
        """Escapes an identifier to prevent SQL injection."""
        return f"`{identifier}`"

    def _escape_string(self, value: str) -> str:
        """Escapes a string value for SQL queries."""
        return f"'{value}'"

    def _build_login_info(
        self, user: str, account: Optional[str] = None, role: Optional[str] = None
    ) -> tuple[str, dict]:
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
                raise ValueError(
                    f"Invalid user format: '{user}'. Expected 'user', 'account#user', or 'account#user#role'"
                )

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
        Execute SQL query using connection pool

        Args:
            sql: SQL query string
            params: Query parameters

        Returns:
            ResultSet object
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        import time

        start_time = time.time()

        try:
            with self._engine.begin() as conn:
                result = conn.execute(text(sql), params or {})

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

    def get_sqlalchemy_engine(self, **pool_kwargs) -> Engine:
        """
        Get SQLAlchemy engine

        Args:
            **pool_kwargs: Optional connection pool parameters (pool_size, max_overflow, etc.)

        Returns:
            SQLAlchemy Engine
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        # If pool parameters are provided, create a new engine with those settings
        if pool_kwargs:
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

            # Create engine with custom connection pooling
            engine = create_engine(
                connection_string, pool_pre_ping=True, **pool_kwargs  # Enable connection health checks
            )

            # Replace the dialect with MatrixOne dialect for proper vector type support
            original_dbapi = engine.dialect.dbapi
            engine.dialect = MatrixOneDialect()
            engine.dialect.dbapi = original_dbapi

            return engine

        return self._engine

    def snapshot_query(self, snapshot_name: str, sql: str, params: Optional[Tuple] = None, conn=None) -> "ResultSet":
        """
        Execute query with snapshot

        Args:
            snapshot_name: Name of the snapshot
            sql: SQL query string
            params: Query parameters
            conn: Optional SQLAlchemy connection

        Returns:
            ResultSet object
        """
        if conn:
            # Use provided SQLAlchemy connection
            snapshot_sql = f"{sql} FOR SNAPSHOT '{snapshot_name}'"
            result = conn.execute(text(snapshot_sql), params or {})

            if result.returns_rows:
                columns = list(result.keys())
                rows = result.fetchall()
                return ResultSet(columns, rows)
            else:
                return ResultSet([], [], affected_rows=result.rowcount)
        else:
            # Use engine connection pool
            snapshot_sql = f"{sql} FOR SNAPSHOT '{snapshot_name}'"
            return self.execute(snapshot_sql, params)

    def snapshot_query_builder(self, snapshot_name: str) -> SnapshotQueryBuilder:
        """
        Get snapshot query builder

        Args:
            snapshot_name: Name of the snapshot

        Returns:
            SnapshotQueryBuilder instance
        """
        return SnapshotQueryBuilder(snapshot_name, self)

    @contextmanager
    def snapshot(self, snapshot_name: str):
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
    def transaction(self):
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
    def vector_index(self) -> Optional["VectorIndexManager"]:
        """Get vector index manager for vector index operations"""
        return self._vector_index

    @property
    def vector_query(self) -> Optional["VectorQueryManager"]:
        """Get vector query manager for vector query operations"""
        return self._vector_query

    @property
    def vector_data(self) -> Optional["VectorDataManager"]:
        """Get vector data manager for vector data operations"""
        return self._vector_data

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

    def create_table(self, table_name: str, columns: dict, **kwargs) -> "Client":
        """
        Create a table with a simplified interface.

        Args:
            table_name: Name of the table
            columns: Dictionary mapping column names to their types
                    Supported formats:
                    - 'id': 'bigint' (with primary_key=True if needed)
                    - 'name': 'varchar(100)'
                    - 'embedding': 'vector(128,f32)'
                    - 'score': 'float'
                    - 'created_at': 'datetime'
                    - 'is_active': 'boolean'
            **kwargs: Additional table parameters

        Returns:
            Client: Self for chaining

        Example:
            client.create_table("users", {
                'id': 'bigint',
                'name': 'varchar(100)',
                'email': 'varchar(255)',
                'embedding': 'vector(128,f32)',
                'score': 'float',
                'created_at': 'datetime',
                'is_active': 'boolean'
            }, primary_key='id')
        """
        from .sqlalchemy_ext import VectorTableBuilder

        # Parse primary key from kwargs
        primary_key = kwargs.get("primary_key", None)

        # Create table using VectorTableBuilder
        builder = VectorTableBuilder(table_name)

        # Add columns based on simplified format
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
            elif column_def in ("blob", "longblob", "mediumblob", "tinyblob", "binary", "varbinary"):
                builder.add_binary_column(column_name, column_def)
            else:
                raise ValueError(
                    f"Unsupported column type '{column_def}' for column '{column_name}'. "
                    f"Supported types: int, bigint, smallint, tinyint, varchar(n), char(n), "
                    f"text, float, double, decimal(p,s), date, datetime, timestamp, time, "
                    f"boolean, json, blob, vector(n,precision)"
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
            elif column_def in ("blob", "longblob", "mediumblob", "tinyblob", "binary", "varbinary"):
                builder.add_binary_column(column_name, column_def)
            else:
                raise ValueError(
                    f"Unsupported column type '{column_def}' for column '{column_name}'. "
                    f"Supported types: int, bigint, smallint, tinyint, varchar(n), char(n), "
                    f"text, float, double, decimal(p,s), date, datetime, timestamp, time, "
                    f"boolean, json, blob, vector(n,precision)"
                )

        # Create table using the provided connection
        table = builder.build()
        create_sql = CreateTable(table)
        sql = str(create_sql.compile(dialect=connection.dialect))
        connection.execute(sql)

        return self

    def drop_table(self, table_name: str) -> "Client":
        """
        Drop a table.

        Args:
            table_name: Name of the table to drop

        Returns:
            Client: Self for chaining
        """
        from sqlalchemy import text

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

        Example:
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
            elif column_def in ("blob", "longblob", "mediumblob", "tinyblob", "binary", "varbinary"):
                builder.add_binary_column(column_name, column_def)
            else:
                raise ValueError(
                    f"Unsupported column type '{column_def}' for column '{column_name}'. "
                    f"Supported types: int, bigint, smallint, tinyint, varchar(n), char(n), "
                    f"text, float, double, decimal(p,s), date, datetime, timestamp, time, "
                    f"boolean, json, blob, vector(n,precision)"
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

                # Create the index
                self.vector_index.create(
                    table_name=table_name, name=index_name, column=column_name, index_type=index_type, **params
                )

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
            elif column_def in ("blob", "longblob", "mediumblob", "tinyblob", "binary", "varbinary"):
                builder.add_binary_column(column_name, column_def)
            else:
                raise ValueError(
                    f"Unsupported column type '{column_def}' for column '{column_name}'. "
                    f"Supported types: int, bigint, smallint, tinyint, varchar(n), char(n), "
                    f"text, float, double, decimal(p,s), date, datetime, timestamp, time, "
                    f"boolean, json, blob, vector(n,precision)"
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

                # Create the index using _in_transaction method
                self.vector_index.create_in_transaction(
                    table_name=table_name,
                    name=index_name,
                    column=column_name,
                    index_type=index_type,
                    connection=connection,
                    **params,
                )

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

        Example:
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
                from sqlalchemy import text

                conn.execute(text("SET experimental_hnsw_index = 1"))
                # Create table and indexes in the same session
                table.create(conn)
        elif has_ivf_index or enable_ivf:
            with engine.begin() as conn:
                from sqlalchemy import text

                conn.execute(text("SET experimental_ivf_index = 1"))
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
            from sqlalchemy import text

            connection.execute(text("SET experimental_hnsw_index = 1"))
        if has_ivf_index or enable_ivf:
            from sqlalchemy import text

            connection.execute(text("SET experimental_ivf_index = 1"))

        # Create table using the provided connection
        create_sql = CreateTable(table)
        sql = str(create_sql.compile(dialect=connection.dialect))
        connection.execute(sql)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


class ResultSet:
    """Result set wrapper for query results"""

    def __init__(self, columns: List[str], rows: List[Tuple], affected_rows: int = 0):
        self.columns = columns
        self.rows = rows
        self.affected_rows = affected_rows

    def fetchall(self) -> List[Tuple]:
        """Fetch all rows"""
        return self.rows

    def fetchone(self) -> Optional[Tuple]:
        """Fetch one row"""
        return self.rows[0] if self.rows else None

    def fetchmany(self, size: int = 1) -> List[Tuple]:
        """Fetch many rows"""
        return self.rows[:size]

    def scalar(self) -> Any:
        """Get scalar value (first column of first row)"""
        if self.rows and self.columns:
            return self.rows[0][0]
        return None

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
        return self.client.snapshot_query(self.snapshot_name, sql, params)


class TransactionWrapper:
    """Transaction wrapper for executing queries within a transaction"""

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
        self.vector_index = TransactionVectorIndexManager(client, self)
        self.vector_query = TransactionVectorQueryManager(client, self)
        self.vector_data = TransactionVectorDataManager(client, self)
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

    def commit_sqlalchemy(self):
        """Commit SQLAlchemy session"""
        if self._sqlalchemy_session:
            self._sqlalchemy_session.commit()

    def rollback_sqlalchemy(self):
        """Rollback SQLAlchemy session"""
        if self._sqlalchemy_session:
            self._sqlalchemy_session.rollback()

    def close_sqlalchemy(self):
        """Close SQLAlchemy session"""
        if self._sqlalchemy_session:
            self._sqlalchemy_session.close()
            self._sqlalchemy_session = None

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
            elif column_def in ("blob", "longblob", "mediumblob", "tinyblob", "binary", "varbinary"):
                builder.add_binary_column(column_name, column_def)
            else:
                raise ValueError(
                    f"Unsupported column type '{column_def}' for column '{column_name}'. "
                    f"Supported types: int, bigint, smallint, tinyint, varchar(n), char(n), "
                    f"text, float, double, decimal(p,s), date, datetime, timestamp, time, "
                    f"boolean, json, blob, vector(n,precision)"
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
            elif column_def in ("blob", "longblob", "mediumblob", "tinyblob", "binary", "varbinary"):
                builder.add_binary_column(column_name, column_def)
            else:
                raise ValueError(
                    f"Unsupported column type '{column_def}' for column '{column_name}'. "
                    f"Supported types: int, bigint, smallint, tinyint, varchar(n), char(n), "
                    f"text, float, double, decimal(p,s), date, datetime, timestamp, time, "
                    f"boolean, json, blob, vector(n,precision)"
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

                # Create the index using transaction wrapper's vector_index
                self.vector_index.create(
                    table_name=table_name, name=index_name, column=column_name, index_type=index_type, **params
                )

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
            self.execute("SET experimental_hnsw_index = 1")
        if has_ivf_index or enable_ivf:
            self.execute("SET experimental_ivf_index = 1")

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
        self, target_db: str, source_db: str, snapshot_name: Optional[str] = None, if_not_exists: bool = False
    ) -> None:
        """Clone database within transaction"""
        return super().clone_database(target_db, source_db, snapshot_name, if_not_exists, self.transaction_wrapper)

    def clone_table(
        self, target_table: str, source_table: str, snapshot_name: Optional[str] = None, if_not_exists: bool = False
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


class VectorIndexManager:
    """Vector index manager for client chain operations"""

    def __init__(self, client):
        self.client = client

    def create(
        self,
        table_name: str,
        name: str,
        column: str,
        index_type: str = "ivfflat",
        lists: int = None,
        op_type: str = "vector_l2_ops",
        # HNSW parameters
        m: int = None,
        ef_construction: int = None,
        ef_search: int = None,
    ) -> "VectorIndexManager":
        """
        Create a vector index using chain operations.

        Args:
            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            index_type: Type of vector index (ivfflat, hnsw, etc.)
            lists: Number of lists for IVFFLAT (optional)
            op_type: Vector operation type
            m: Number of bi-directional links for HNSW (optional)
            ef_construction: Size of dynamic candidate list for HNSW construction (optional)
            ef_search: Size of dynamic candidate list for HNSW search (optional)

        Returns:
            VectorIndexManager: Self for chaining
        """
        from .sqlalchemy_ext import VectorIndex, VectorIndexType, VectorOpType

        # Convert string parameters to enum values
        if index_type == "ivfflat":
            index_type = VectorIndexType.IVFFLAT
        elif index_type == "hnsw":
            index_type = VectorIndexType.HNSW
        if op_type == "vector_l2_ops":
            op_type = VectorOpType.VECTOR_L2_OPS

        # Indexing will be enabled automatically in the same connection by VectorIndex.create_index

        success = VectorIndex.create_index(
            engine=self.client.get_sqlalchemy_engine(),
            table_name=table_name,
            name=name,
            column=column,
            index_type=index_type,
            lists=lists,
            op_type=op_type,
            m=m,
            ef_construction=ef_construction,
            ef_search=ef_search,
        )

        if not success:
            raise Exception(f"Failed to create vector index {name} on table {table_name}")

        return self

    def create_in_transaction(
        self,
        table_name: str,
        name: str,
        column: str,
        connection,
        index_type: str = "ivfflat",
        lists: int = None,
        op_type: str = "vector_l2_ops",
        # HNSW parameters
        m: int = None,
        ef_construction: int = None,
        ef_search: int = None,
    ) -> "VectorIndexManager":
        """
        Create a vector index within an existing SQLAlchemy transaction.

        Args:
            table_name: Name of the table
            name: Name of the index
            column: Vector column to index
            connection: SQLAlchemy connection object (required for transaction support)
            index_type: Type of vector index (ivfflat, hnsw, etc.)
            lists: Number of lists for IVFFLAT (optional)
            op_type: Vector operation type
            m: Number of bi-directional links for HNSW (optional)
            ef_construction: Size of dynamic candidate list for HNSW construction (optional)
            ef_search: Size of dynamic candidate list for HNSW search (optional)

        Returns:
            VectorIndexManager: Self for chaining

        Raises:
            ValueError: If connection is not provided
        """
        if connection is None:
            raise ValueError("connection parameter is required for transaction operations")

        from sqlalchemy import text

        # Build CREATE INDEX statement
        if index_type == "ivfflat":
            if lists is None:
                lists = 100  # Default value
            sql = f"CREATE INDEX {name} USING ivfflat ON {table_name}({column}) LISTS {lists} op_type '{op_type}'"
        elif index_type == "hnsw":
            if m is None:
                m = 48  # Default value
            if ef_construction is None:
                ef_construction = 64  # Default value
            if ef_search is None:
                ef_search = 64  # Default value
            sql = (
                f"CREATE INDEX {name} USING hnsw ON {table_name}({column}) "
                f"M {m} EF_CONSTRUCTION {ef_construction} EF_SEARCH {ef_search} "
                f"op_type '{op_type}'"
            )
        else:
            raise ValueError(f"Unsupported index type: {index_type}")

        # Enable appropriate indexing if needed
        if index_type == "hnsw":
            connection.execute(text("SET experimental_hnsw_index = 1"))
        elif index_type == "ivfflat":
            connection.execute(text("SET experimental_ivf_index = 1"))
            connection.execute(text("SET probe_limit = 1"))

        # Create the index
        connection.execute(text(sql))

        return self

    def drop(self, table_name: str, name: str) -> "VectorIndexManager":
        """
        Drop a vector index using chain operations.

        Args:
            table_name: Name of the table
            name: Name of the index to drop

        Returns:
            VectorIndexManager: Self for chaining
        """
        from .sqlalchemy_ext import VectorIndex

        success = VectorIndex.drop_index(engine=self.client.get_sqlalchemy_engine(), table_name=table_name, name=name)

        if not success:
            raise Exception(f"Failed to drop vector index {name} from table {table_name}")

        return self

    def enable_ivf(self, probe_limit: int = 1) -> "VectorIndexManager":
        """
        Enable IVF indexing with chain operations.

        Args:
            probe_limit: Probe limit for IVF search

        Returns:
            VectorIndexManager: Self for chaining
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

    def disable_ivf(self) -> "VectorIndexManager":
        """
        Disable IVF indexing with chain operations.

        Returns:
            VectorIndexManager: Self for chaining
        """
        from .sqlalchemy_ext import create_ivf_config

        ivf_config = create_ivf_config(self.client.get_sqlalchemy_engine())
        if not ivf_config.disable_ivf_indexing():
            raise Exception("Failed to disable IVF indexing")

        return self

    def enable_hnsw(self) -> "VectorIndexManager":
        """
        Enable HNSW indexing with chain operations.

        Returns:
            VectorIndexManager: Self for chaining
        """
        from .sqlalchemy_ext import create_hnsw_config

        hnsw_config = create_hnsw_config(self.client.get_sqlalchemy_engine())
        if not hnsw_config.enable_hnsw_indexing():
            raise Exception("Failed to enable HNSW indexing")

        return self

    def disable_hnsw(self) -> "VectorIndexManager":
        """
        Disable HNSW indexing with chain operations.

        Returns:
            VectorIndexManager: Self for chaining
        """
        from .sqlalchemy_ext import create_hnsw_config

        hnsw_config = create_hnsw_config(self.client.get_sqlalchemy_engine())
        if not hnsw_config.disable_hnsw_indexing():
            raise Exception("Failed to disable HNSW indexing")

        return self


class TransactionVectorIndexManager(VectorIndexManager):
    """Vector index manager that executes operations within a transaction"""

    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper

    def create(
        self,
        table_name: str,
        name: str,
        column: str,
        index_type: str = "ivfflat",
        lists: int = None,
        op_type: str = "vector_l2_ops",
        # HNSW parameters
        m: int = None,
        ef_construction: int = None,
        ef_search: int = None,
    ) -> "TransactionVectorIndexManager":
        """Create a vector index within transaction"""
        from .sqlalchemy_ext import VectorIndex, VectorIndexType, VectorOpType

        # Convert string parameters to enum values
        if index_type == "ivfflat":
            index_type = VectorIndexType.IVFFLAT
        elif index_type == "hnsw":
            index_type = VectorIndexType.HNSW
        if op_type == "vector_l2_ops":
            op_type = VectorOpType.VECTOR_L2_OPS

        # Create index using transaction wrapper's execute method
        index = VectorIndex(name, column, index_type, lists, op_type, m, ef_construction, ef_search)

        try:
            # Enable appropriate indexing in the same connection
            if index_type == VectorIndexType.IVFFLAT:
                self.transaction_wrapper.execute("SET experimental_ivf_index = 1")
                self.transaction_wrapper.execute("SET probe_limit = 1")
            elif index_type == VectorIndexType.HNSW:
                self.transaction_wrapper.execute("SET experimental_hnsw_index = 1")

            sql = index.create_sql(table_name)
            self.transaction_wrapper.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to create vector index {name} on table {table_name} in transaction: {e}")

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
    """Vector query manager for client chain operations"""

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
            connection: Optional existing database connection (for transaction support)

        Returns:
            List of search results
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
        ORDER BY distance
        LIMIT {limit}
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

    def similarity_search_in_transaction(
        self,
        table_name: str,
        vector_column: str,
        query_vector: list,
        limit: int = 10,
        distance_type: str = "l2",
        select_columns: list = None,
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


class VectorDataManager:
    """Vector data manager for client chain operations"""

    def __init__(self, client):
        self.client = client

    def insert(self, table_name: str, data: dict) -> "VectorDataManager":
        """
        Insert vector data using chain operations.

        Args:
            table_name: Name of the table
            data: Data to insert (dict with column names as keys)

        Returns:
            VectorDataManager: Self for chaining
        """
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

        with self.client.get_sqlalchemy_engine().begin() as conn:
            conn.execute(text(sql))

        return self

    def insert_in_transaction(self, table_name: str, data: dict, connection) -> "VectorDataManager":
        """
        Insert vector data within an existing SQLAlchemy transaction.

        Args:
            table_name: Name of the table
            data: Data to insert (dict with column names as keys)
            connection: SQLAlchemy connection object (required for transaction support)

        Returns:
            VectorDataManager: Self for chaining

        Raises:
            ValueError: If connection is not provided
        """
        if connection is None:
            raise ValueError("connection parameter is required for transaction operations")

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

    def batch_insert(self, table_name: str, data_list: list) -> "VectorDataManager":
        """
        Batch insert vector data using chain operations.

        Args:
            table_name: Name of the table
            data_list: List of data dictionaries to insert

        Returns:
            VectorDataManager: Self for chaining
        """
        from sqlalchemy import text

        if not data_list:
            return self

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

        with self.client.get_sqlalchemy_engine().begin() as conn:
            conn.execute(text(sql))

        return self

    def update(self, table_name: str, data: dict, where_clause: str) -> "VectorDataManager":
        """
        Update vector data using chain operations.

        Args:
            table_name: Name of the table
            data: Data to update
            where_clause: WHERE condition

        Returns:
            VectorDataManager: Self for chaining
        """
        from sqlalchemy import text

        # Build SET clause
        set_clauses = []
        for column, value in data.items():
            if isinstance(value, list):
                formatted_value = "[" + ",".join(map(str, value)) + "]"
            else:
                formatted_value = str(value)
            set_clauses.append(f"{column} = '{formatted_value}'")

        set_clause = ", ".join(set_clauses)
        sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"

        with self.client.get_sqlalchemy_engine().begin() as conn:
            conn.execute(text(sql))

        return self

    def delete(self, table_name: str, where_clause: str) -> "VectorDataManager":
        """
        Delete vector data using chain operations.

        Args:
            table_name: Name of the table
            where_clause: WHERE condition

        Returns:
            VectorDataManager: Self for chaining
        """
        from sqlalchemy import text

        sql = f"DELETE FROM {table_name} WHERE {where_clause}"

        with self.client.get_sqlalchemy_engine().begin() as conn:
            conn.execute(text(sql))

        return self


class TransactionVectorQueryManager:
    """Vector query manager for transaction chain operations"""

    def __init__(self, client, transaction_wrapper):
        self.client = client
        self.transaction_wrapper = transaction_wrapper

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


class TransactionVectorDataManager:
    """Vector data manager for transaction chain operations"""

    def __init__(self, client, transaction_wrapper):
        self.client = client
        self.transaction_wrapper = transaction_wrapper

    def insert(self, table_name: str, data: dict) -> "TransactionVectorDataManager":
        """Insert vector data within transaction"""
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
        self.transaction_wrapper.execute(sql)

        return self

    def batch_insert(self, table_name: str, data_list: list) -> "TransactionVectorDataManager":
        """Batch insert vector data within transaction"""
        if not data_list:
            return self

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
        self.transaction_wrapper.execute(sql)

        return self

    def update(self, table_name: str, data: dict, where_clause: str) -> "TransactionVectorDataManager":
        """Update vector data within transaction"""
        # Build SET clause
        set_clauses = []
        for column, value in data.items():
            if isinstance(value, list):
                formatted_value = "[" + ",".join(map(str, value)) + "]"
            else:
                formatted_value = str(value)
            set_clauses.append(f"{column} = '{formatted_value}'")

        set_clause = ", ".join(set_clauses)
        sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
        self.transaction_wrapper.execute(sql)

        return self

    def delete(self, table_name: str, where_clause: str) -> "TransactionVectorDataManager":
        """Delete vector data within transaction"""
        sql = f"DELETE FROM {table_name} WHERE {where_clause}"
        self.transaction_wrapper.execute(sql)

        return self
