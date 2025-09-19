"""
MatrixOne Client - Basic implementation
"""

import pymysql
from typing import Optional, Dict, Any, List, Tuple, Union
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .exceptions import ConnectionError, QueryError, ConfigurationError, SnapshotError, CloneError, RestoreError
from .snapshot import SnapshotManager, SnapshotQueryBuilder, CloneManager, Snapshot, SnapshotLevel
from .moctl import MoCtlManager
from .restore import RestoreManager, TransactionRestoreManager


class Client:
    """
    MatrixOne Client - Basic implementation
    
    Provides basic connection and query functionality for MatrixOne database.
    """
    
    def __init__(self, 
                 connection_timeout: int = 30,
                 query_timeout: int = 300,
                 auto_commit: bool = True,
                 charset: str = 'utf8mb4'):
        """
        Initialize MatrixOne client
        
        Args:
            connection_timeout: Connection timeout in seconds
            query_timeout: Query timeout in seconds  
            auto_commit: Enable auto-commit mode
            charset: Character set for connection
        """
        self.connection_timeout = connection_timeout
        self.query_timeout = query_timeout
        self.auto_commit = auto_commit
        self.charset = charset
        
        self._connection = None
        self._engine = None
        self._connection_params = {}
        self._snapshots = SnapshotManager(self)
        self._clone = CloneManager(self)
        self._moctl = MoCtlManager(self)
        self._restore = RestoreManager(self)
    
    def connect(self, 
                host: str, 
                port: int, 
                user: str, 
                password: str, 
                database: str,
                ssl_mode: str = "preferred",
                ssl_ca: Optional[str] = None,
                ssl_cert: Optional[str] = None,
                ssl_key: Optional[str] = None) -> None:
        """
        Connect to MatrixOne database
        
        Args:
            host: Database host
            port: Database port
            user: Username
            password: Password
            database: Database name
            ssl_mode: SSL mode (disabled, preferred, required)
            ssl_ca: SSL CA certificate path
            ssl_cert: SSL client certificate path
            ssl_key: SSL client key path
        """
        self._connection_params = {
            'host': host,
            'port': port,
            'user': user,
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
        except Exception as e:
            raise ConnectionError(f"Failed to connect to MatrixOne: {e}")
    
    def disconnect(self) -> None:
        """Disconnect from MatrixOne database"""
        if self._connection:
            self._connection.close()
            self._connection = None
        
        if self._engine:
            self._engine.dispose()
            self._engine = None
    
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
        
        try:
            with self._connection.cursor() as cursor:
                cursor.execute(sql, params)
                
                if cursor.description:
                    # SELECT query
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    return ResultSet(columns, rows)
                else:
                    # INSERT/UPDATE/DELETE query
                    return ResultSet([], [], affected_rows=cursor.rowcount)
                    
        except Exception as e:
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
        # Create snapshot, clone, and restore managers that use this transaction
        self.snapshots = TransactionSnapshotManager(client, self)
        self.clone = TransactionCloneManager(client, self)
        self.restore = TransactionRestoreManager(client, self)
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
