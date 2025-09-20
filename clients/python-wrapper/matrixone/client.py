"""
MatrixOne Client - Basic implementation
"""

import pymysql
from typing import Optional, Dict, Any, List, Tuple, Union
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .exceptions import ConnectionError, QueryError, ConfigurationError, SnapshotError, CloneError, RestoreError, PitrError, PubSubError, VersionError
from .snapshot import SnapshotManager, SnapshotQueryBuilder, CloneManager, Snapshot, SnapshotLevel
from .moctl import MoCtlManager
from .restore import RestoreManager, TransactionRestoreManager
from .pitr import PitrManager, TransactionPitrManager
from .pubsub import PubSubManager, TransactionPubSubManager
from .account import AccountManager, TransactionAccountManager
from .logger import MatrixOneLogger, create_default_logger
from .version import VersionManager, get_version_manager


class Client:
    """
    MatrixOne Client - Basic implementation
    
    Provides basic connection and query functionality for MatrixOne database.
    """
    
    def __init__(self, 
                 connection_timeout: int = 30,
                 query_timeout: int = 300,
                 auto_commit: bool = True,
                 charset: str = 'utf8mb4',
                 logger: Optional[MatrixOneLogger] = None,
                 enable_performance_logging: bool = False,
                 enable_sql_logging: bool = False,
                 enable_full_sql_logging: bool = False,
                 enable_slow_sql_logging: bool = False,
                 enable_error_sql_logging: bool = False,
                 slow_sql_threshold: float = 1.0):
        """
        Initialize MatrixOne client
        
        Args:
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
                slow_sql_threshold=slow_sql_threshold
            )
        
        self._connection = None
        self._engine = None
        self._connection_params = {}
        self._login_info = None
        self._snapshots = SnapshotManager(self)
        self._clone = CloneManager(self)
        self._moctl = MoCtlManager(self)
        self._restore = RestoreManager(self)
        self._pitr = PitrManager(self)
        self._pubsub = PubSubManager(self)
        self._account = AccountManager(self)
        
        # Initialize version manager
        self._version_manager = get_version_manager()
        self._backend_version = None
    
    def connect(self, 
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
                role: Optional[str] = None) -> None:
        """
        Connect to MatrixOne database
        
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
            'host': host,
            'port': port,
            'user': final_user,
            'password': password,
            'database': database,
            'charset': self.charset,
            'connect_timeout': self.connection_timeout,
            'autocommit': self.auto_commit,
            'ssl_disabled': ssl_mode == "disabled",
            'ssl_verify_cert': ssl_mode == "required",
            'ssl_verify_identity': ssl_mode == "required"
        }
        
        # Add SSL parameters if provided
        if ssl_ca:
            self._connection_params['ssl_ca'] = ssl_ca
        if ssl_cert:
            self._connection_params['ssl_cert'] = ssl_cert
        if ssl_key:
            self._connection_params['ssl_key'] = ssl_key
        
        try:
            self._connection = pymysql.connect(**self._connection_params)
            self.logger.log_connection(host, port, final_user, database, success=True)
            
            # Try to detect backend version after successful connection
            try:
                self._detect_backend_version()
            except Exception as e:
                self.logger.warning(f"Failed to detect backend version: {e}")
                
        except Exception as e:
            self.logger.log_connection(host, port, final_user, database, success=False)
            self.logger.log_error(e, context="Connection")
            raise ConnectionError(f"Failed to connect to MatrixOne: {e}")
    
    def disconnect(self) -> None:
        """Disconnect from MatrixOne database"""
        if self._connection:
            try:
                self._connection.close()
                self._connection = None
                self.logger.log_disconnection(success=True)
            except Exception as e:
                self.logger.log_disconnection(success=False)
                self.logger.log_error(e, context="Disconnection")
                raise
        
        if self._engine:
            self._engine.dispose()
            self._engine = None
    
    def get_login_info(self) -> Optional[dict]:
        """Get parsed login information"""
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
        if '#' in user:
            # User is already in format "account#user" or "account#user#role"
            if account is not None or role is not None:
                raise ValueError(f"Conflict: user parameter '{user}' already contains account/role info, "
                               f"but account='{account}' and role='{role}' are also provided. "
                               f"Use either user format or separate account/role parameters, not both.")
            
            # Parse the existing format
            parts = user.split('#')
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
        
        parsed_info = {
            'account': final_account,
            'user': final_user,
            'role': final_role
        }
        
        return final_user_string, parsed_info
    
    def execute(self, sql: str, params: Optional[Tuple] = None) -> 'ResultSet':
        """
        Execute SQL query
        
        Args:
            sql: SQL query string
            params: Query parameters
            
        Returns:
            ResultSet object
        """
        if not self._connection:
            raise ConnectionError("Not connected to database")
        
        import time
        start_time = time.time()
        
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(sql, params)
                
                execution_time = time.time() - start_time
                
                if cursor.description:
                    # SELECT query
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    result = ResultSet(columns, rows)
                    self.logger.log_query(sql, execution_time, len(rows), success=True)
                    return result
                else:
                    # INSERT/UPDATE/DELETE query
                    result = ResultSet([], [], affected_rows=cursor.rowcount)
                    self.logger.log_query(sql, execution_time, cursor.rowcount, success=True)
                    return result
                    
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.log_query(sql, execution_time, success=False)
            self.logger.log_error(e, context="Query execution")
            raise QueryError(f"Query execution failed: {e}")
    
    def get_sqlalchemy_engine(self, 
                             pool_size: int = 10,
                             max_overflow: int = 20,
                             pool_timeout: int = 30,
                             pool_recycle: int = 3600,
                             echo: bool = False) -> Engine:
        """
        Get SQLAlchemy engine
        
        Args:
            pool_size: Connection pool size
            max_overflow: Maximum overflow connections
            pool_timeout: Pool timeout in seconds
            pool_recycle: Connection recycle time in seconds
            echo: Enable SQL logging
            
        Returns:
            SQLAlchemy Engine
        """
        if not self._connection_params:
            raise ConnectionError("Not connected to database")
        
        # Build connection string
        connection_string = (
            f"mysql+pymysql://{self._connection_params['user']}:"
            f"{self._connection_params['password']}@"
            f"{self._connection_params['host']}:"
            f"{self._connection_params['port']}/"
            f"{self._connection_params['database']}"
        )
        
        # Add SSL parameters if needed
        if 'ssl_ca' in self._connection_params:
            connection_string += f"?ssl_ca={self._connection_params['ssl_ca']}"
        
        self._engine = create_engine(
            connection_string,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle,
            echo=echo
        )
        
        return self._engine
    
    def snapshot_query(self, snapshot_name: str, sql: str, 
                      params: Optional[Tuple] = None, conn=None) -> 'ResultSet':
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
            # Use SQLAlchemy connection
            snapshot_sql = f"{sql} FOR SNAPSHOT '{snapshot_name}'"
            result = conn.execute(text(snapshot_sql), params or {})
            
            if result.returns_rows:
                columns = list(result.keys())
                rows = result.fetchall()
                return ResultSet(columns, rows)
            else:
                return ResultSet([], [], affected_rows=result.rowcount)
        else:
            # Use direct connection
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
    
    def get_snapshot_engine(self, snapshot_name: str, **kwargs) -> Engine:
        """
        Get SQLAlchemy engine with snapshot
        
        Args:
            snapshot_name: Name of the snapshot
            **kwargs: Additional engine parameters
            
        Returns:
            SQLAlchemy Engine with snapshot configuration
        """
        if not self._connection_params:
            raise ConnectionError("Not connected to database")
        
        # Build connection string with snapshot
        connection_string = (
            f"mysql+pymysql://{self._connection_params['user']}:"
            f"{self._connection_params['password']}@"
            f"{self._connection_params['host']}:"
            f"{self._connection_params['port']}/"
            f"{self._connection_params['database']}"
        )
        
        # Add snapshot parameter
        connection_string += f"?snapshot={snapshot_name}"
        
        # Add SSL parameters if needed
        if 'ssl_ca' in self._connection_params:
            connection_string += f"&ssl_ca={self._connection_params['ssl_ca']}"
        
        return create_engine(connection_string, **kwargs)
    
    @contextmanager
    def snapshot(self, snapshot_name: str):
        """
        Snapshot context manager
        
        Usage:
            with client.snapshot("daily_backup") as snapshot_client:
                result = snapshot_client.execute("SELECT * FROM users")
        """
        if not self._connection:
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
        if not self._connection:
            raise ConnectionError("Not connected to database")
        
        tx_wrapper = None
        try:
            self._connection.begin()
            tx_wrapper = TransactionWrapper(self._connection, self)
            yield tx_wrapper
            
            # Commit SQLAlchemy session first
            tx_wrapper.commit_sqlalchemy()
            # Then commit the main transaction
            self._connection.commit()
            
        except Exception as e:
            # Rollback SQLAlchemy session first
            if tx_wrapper:
                tx_wrapper.rollback_sqlalchemy()
            # Then rollback the main transaction
            self._connection.rollback()
            raise e
        finally:
            # Clean up SQLAlchemy resources
            if tx_wrapper:
                tx_wrapper.close_sqlalchemy()
    
    def get_sqlalchemy_connection(self):
        """
        Get the underlying database connection for SQLAlchemy integration
        
        Returns:
            The raw database connection that can be used with SQLAlchemy
        """
        if not self._connection:
            raise ConnectionError("Not connected to database")
        return self._connection
    
    def get_sqlalchemy_engine(self, **kwargs):
        """
        Get SQLAlchemy engine using the current connection
        
        Args:
            **kwargs: Additional engine parameters
            
        Returns:
            SQLAlchemy Engine instance
        """
        from sqlalchemy import create_engine
        
        if not self._connection_params:
            raise ConnectionError("Not connected to database")
        
        # Build connection string
        connection_string = (
            f"mysql+pymysql://{self._connection_params['user']}:"
            f"{self._connection_params['password']}@"
            f"{self._connection_params['host']}:"
            f"{self._connection_params['port']}/"
            f"{self._connection_params['database']}"
        )
        
        # Create engine with current connection
        engine = create_engine(connection_string, **kwargs)
        return engine
    
    @property
    def snapshots(self) -> SnapshotManager:
        """Get snapshot manager"""
        return self._snapshots
    
    @property
    def clone(self) -> CloneManager:
        """Get clone manager"""
        return self._clone
    
    @property
    def moctl(self) -> MoCtlManager:
        """Get mo_ctl manager"""
        return self._moctl
    
    @property
    def restore(self) -> RestoreManager:
        """Get restore manager"""
        return self._restore
    
    @property
    def pitr(self) -> PitrManager:
        """Get PITR manager"""
        return self._pitr
    
    @property
    def pubsub(self) -> PubSubManager:
        """Get publish-subscribe manager"""
        return self._pubsub
    
    @property
    def account(self) -> AccountManager:
        """Get account manager"""
        return self._account
    
    def version(self) -> str:
        """
        Get MatrixOne server version
        
        Returns:
            str: MatrixOne server version string
            
        Raises:
            ConnectionError: If not connected to MatrixOne
            QueryError: If version query fails
            
        Example:
            >>> client = Client()
            >>> client.connect('localhost', 6001, 'root', '111', 'test')
            >>> version = client.version()
            >>> print(f"MatrixOne version: {version}")
        """
        if not self._connection:
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
            >>> client = Client()
            >>> client.connect('localhost', 6001, 'root', '111', 'test')
            >>> git_version = client.git_version()
            >>> print(f"MatrixOne git version: {git_version}")
        """
        if not self._connection:
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
        dev_pattern = r'(\d+\.\d+\.\d+)-MatrixOne-v$'
        dev_match = re.search(dev_pattern, version_string.strip())
        if dev_match:
            # Development version - assign highest version number
            return "999.0.0"
        
        # Pattern 2: Release version "8.0.30-MatrixOne-v3.0.0" (v后面有版本号)
        release_pattern = r'(\d+\.\d+\.\d+)-MatrixOne-v(\d+\.\d+\.\d+)'
        release_match = re.search(release_pattern, version_string.strip())
        if release_match:
            # Extract the semantic version part
            semantic_version = release_match.group(2)
            return semantic_version
        
        # Pattern 3: Fallback format "MatrixOne 3.0.1"
        fallback_pattern = r'(\d+\.\d+\.\d+)'
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
                'feature_name': requirement.feature_name,
                'min_version': str(requirement.min_version) if requirement.min_version else None,
                'max_version': str(requirement.max_version) if requirement.max_version else None,
                'description': requirement.description,
                'alternative': requirement.alternative
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
        # Create snapshot, clone, restore, PITR, pubsub, and account managers that use this transaction
        self.snapshots = TransactionSnapshotManager(client, self)
        self.clone = TransactionCloneManager(client, self)
        self.restore = TransactionRestoreManager(client, self)
        self.pitr = TransactionPitrManager(client, self)
        self.pubsub = TransactionPubSubManager(client, self)
        self.account = TransactionAccountManager(self)
        # SQLAlchemy integration
        self._sqlalchemy_session = None
        self._sqlalchemy_engine = None
    
    def execute(self, sql: str, params: Optional[Tuple] = None) -> ResultSet:
        """Execute SQL within transaction"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql, params)
                
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    return ResultSet(columns, rows)
                else:
                    return ResultSet([], [], affected_rows=cursor.rowcount)
                    
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
            from sqlalchemy import create_engine
            
            # Create engine using the same connection parameters
            if not self.client._connection_params:
                raise ConnectionError("Not connected to database")
            
            connection_string = (
                f"mysql+pymysql://{self.client._connection_params['user']}:"
                f"{self.client._connection_params['password']}@"
                f"{self.client._connection_params['host']}:"
                f"{self.client._connection_params['port']}/"
                f"{self.client._connection_params['database']}"
            )
            
            # Create engine that will use the same connection
            self._sqlalchemy_engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=300
            )
            
            # Create session factory
            Session = sessionmaker(bind=self._sqlalchemy_engine)
            self._sqlalchemy_session = Session()
            
            # Begin SQLAlchemy transaction
            self._sqlalchemy_session.begin()
        
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
        if self._sqlalchemy_engine:
            self._sqlalchemy_engine.dispose()
            self._sqlalchemy_engine = None


class TransactionSnapshotManager(SnapshotManager):
    """Snapshot manager that executes operations within a transaction"""
    
    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper
    
    def create(self, name: str, level: Union[str, SnapshotLevel], database: Optional[str] = None, 
               table: Optional[str] = None, description: Optional[str] = None) -> Snapshot:
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
    
    def clone_database(self, target_db: str, source_db: str, 
                      snapshot_name: Optional[str] = None, 
                      if_not_exists: bool = False) -> None:
        """Clone database within transaction"""
        return super().clone_database(target_db, source_db, snapshot_name, if_not_exists, self.transaction_wrapper)
    
    def clone_table(self, target_table: str, source_table: str,
                   snapshot_name: Optional[str] = None,
                   if_not_exists: bool = False) -> None:
        """Clone table within transaction"""
        return super().clone_table(target_table, source_table, snapshot_name, if_not_exists, self.transaction_wrapper)
    
    def clone_database_with_snapshot(self, target_db: str, source_db: str, 
                                   snapshot_name: str, if_not_exists: bool = False) -> None:
        """Clone database with snapshot within transaction"""
        return super().clone_database_with_snapshot(target_db, source_db, snapshot_name, if_not_exists, self.transaction_wrapper)
    
    def clone_table_with_snapshot(self, target_table: str, source_table: str,
                                snapshot_name: str, if_not_exists: bool = False) -> None:
        """Clone table with snapshot within transaction"""
        return super().clone_table_with_snapshot(target_table, source_table, snapshot_name, if_not_exists, self.transaction_wrapper)
