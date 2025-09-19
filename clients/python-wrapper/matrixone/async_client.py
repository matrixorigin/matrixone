"""
MatrixOne Async Client - Asynchronous implementation
"""

import asyncio
import aiomysql
from typing import Optional, Dict, Any, List, Tuple, Union
from contextlib import asynccontextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .exceptions import ConnectionError, QueryError, ConfigurationError, SnapshotError, CloneError, MoCtlError, RestoreError
from .snapshot import SnapshotManager, SnapshotQueryBuilder, CloneManager, Snapshot, SnapshotLevel
from .moctl import MoCtlManager
from .restore import RestoreManager


class AsyncResultSet:
    """Async result set wrapper for query results"""
    
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


class AsyncSnapshotManager:
    """Async snapshot manager"""
    
    def __init__(self, client):
        self.client = client
    
    async def create(self, name: str, level: Union[str, SnapshotLevel], 
                    database: Optional[str] = None, table: Optional[str] = None, 
                    description: Optional[str] = None) -> Snapshot:
        """Create snapshot asynchronously"""
        # Convert string level to enum if needed
        if isinstance(level, str):
            level = SnapshotLevel(level.lower())
        
        # Build SQL based on level
        if level == SnapshotLevel.CLUSTER:
            sql = f"CREATE SNAPSHOT {name} FOR CLUSTER"
        elif level == SnapshotLevel.ACCOUNT:
            sql = f"CREATE SNAPSHOT {name} FOR ACCOUNT"
        elif level == SnapshotLevel.DATABASE:
            if not database:
                raise SnapshotError("Database name is required for database level snapshot")
            sql = f"CREATE SNAPSHOT {name} FOR DATABASE {database}"
        elif level == SnapshotLevel.TABLE:
            if not database or not table:
                raise SnapshotError("Database and table names are required for table level snapshot")
            sql = f"CREATE SNAPSHOT {name} FOR TABLE {database}.{table}"
        else:
            raise SnapshotError(f"Invalid snapshot level: {level}")
        
        if description:
            sql += f" COMMENT '{description}'"
        
        await self.client.execute(sql)
        
        # Return snapshot object
        import datetime
        return Snapshot(name, level, datetime.datetime.now(), database, table, description)
    
    async def get(self, name: str) -> Snapshot:
        """Get snapshot asynchronously"""
        sql = "SELECT name, level, created_at, database_name, table_name, description FROM mo_snapshots WHERE name = %s"
        result = await self.client.execute(sql, (name,))
        
        if not result.rows:
            raise SnapshotError(f"Snapshot '{name}' not found")
        
        row = result.rows[0]
        # Convert level string to enum
        try:
            level = SnapshotLevel(row[1].lower())
        except ValueError:
            level = row[1]  # Fallback to string for backward compatibility
        
        return Snapshot(row[0], level, row[2], row[3], row[4], row[5])
    
    async def list(self) -> List[Snapshot]:
        """List all snapshots asynchronously"""
        sql = "SELECT name, level, created_at, database_name, table_name, description FROM mo_snapshots ORDER BY created_at DESC"
        result = await self.client.execute(sql)
        
        snapshots = []
        for row in result.rows:
            try:
                level = SnapshotLevel(row[1].lower())
            except ValueError:
                level = row[1]  # Fallback to string for backward compatibility
            
            snapshots.append(Snapshot(row[0], level, row[2], row[3], row[4], row[5]))
        
        return snapshots
    
    async def delete(self, name: str) -> None:
        """Delete snapshot asynchronously"""
        sql = f"DROP SNAPSHOT {name}"
        await self.client.execute(sql)
    
    async def exists(self, name: str) -> bool:
        """Check if snapshot exists asynchronously"""
        try:
            await self.get(name)
            return True
        except SnapshotError:
            return False


class AsyncCloneManager:
    """Async clone manager"""
    
    def __init__(self, client):
        self.client = client
    
    async def clone_database(self, target_db: str, source_db: str, 
                           snapshot_name: Optional[str] = None, 
                           if_not_exists: bool = False) -> None:
        """Clone database asynchronously"""
        if_not_exists_clause = "IF NOT EXISTS" if if_not_exists else ""
        
        if snapshot_name:
            sql = f"CREATE DATABASE {target_db} {if_not_exists_clause} CLONE {source_db} FOR SNAPSHOT '{snapshot_name}'"
        else:
            sql = f"CREATE DATABASE {target_db} {if_not_exists_clause} CLONE {source_db}"
        
        await self.client.execute(sql)
    
    async def clone_table(self, target_table: str, source_table: str,
                         snapshot_name: Optional[str] = None,
                         if_not_exists: bool = False) -> None:
        """Clone table asynchronously"""
        if_not_exists_clause = "IF NOT EXISTS" if if_not_exists else ""
        
        if snapshot_name:
            sql = f"CREATE TABLE {target_table} {if_not_exists_clause} CLONE {source_table} FOR SNAPSHOT '{snapshot_name}'"
        else:
            sql = f"CREATE TABLE {target_table} {if_not_exists_clause} CLONE {source_table}"
        
        await self.client.execute(sql)
    
    async def clone_database_with_snapshot(self, target_db: str, source_db: str, 
                                         snapshot_name: str, if_not_exists: bool = False) -> None:
        """Clone database with snapshot asynchronously"""
        await self.clone_database(target_db, source_db, snapshot_name, if_not_exists)
    
    async def clone_table_with_snapshot(self, target_table: str, source_table: str,
                                      snapshot_name: str, if_not_exists: bool = False) -> None:
        """Clone table with snapshot asynchronously"""
        await self.clone_table(target_table, source_table, snapshot_name, if_not_exists)


class AsyncRestoreManager:
    """Async manager for restore operations"""
    
    def __init__(self, client):
        self.client = client
    
    async def restore_cluster(self, snapshot_name: str) -> bool:
        """Restore entire cluster from snapshot asynchronously"""
        try:
            sql = f"RESTORE CLUSTER FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}"
            result = await self.client.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore cluster from snapshot '{snapshot_name}': {e}")
    
    async def restore_tenant(self, 
                           snapshot_name: str, 
                           account_name: str,
                           to_account: Optional[str] = None) -> bool:
        """Restore tenant from snapshot asynchronously"""
        try:
            if to_account:
                sql = (f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                      f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)} "
                      f"TO ACCOUNT {self.client._escape_identifier(to_account)}")
            else:
                sql = (f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                      f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}")
            
            result = await self.client.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore tenant '{account_name}' from snapshot '{snapshot_name}': {e}")
    
    async def restore_database(self, 
                             snapshot_name: str, 
                             account_name: str,
                             database_name: str,
                             to_account: Optional[str] = None) -> bool:
        """Restore database from snapshot asynchronously"""
        try:
            if to_account:
                sql = (f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                      f"DATABASE {self.client._escape_identifier(database_name)} "
                      f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)} "
                      f"TO ACCOUNT {self.client._escape_identifier(to_account)}")
            else:
                sql = (f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                      f"DATABASE {self.client._escape_identifier(database_name)} "
                      f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}")
            
            result = await self.client.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore database '{database_name}' from snapshot '{snapshot_name}': {e}")
    
    async def restore_table(self, 
                          snapshot_name: str, 
                          account_name: str,
                          database_name: str,
                          table_name: str,
                          to_account: Optional[str] = None) -> bool:
        """Restore table from snapshot asynchronously"""
        try:
            if to_account:
                sql = (f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                      f"DATABASE {self.client._escape_identifier(database_name)} "
                      f"TABLE {self.client._escape_identifier(table_name)} "
                      f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)} "
                      f"TO ACCOUNT {self.client._escape_identifier(to_account)}")
            else:
                sql = (f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                      f"DATABASE {self.client._escape_identifier(database_name)} "
                      f"TABLE {self.client._escape_identifier(table_name)} "
                      f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}")
            
            result = await self.client.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore table '{table_name}' from snapshot '{snapshot_name}': {e}")


class AsyncMoCtlManager:
    """Async mo_ctl manager"""
    
    def __init__(self, client):
        self.client = client
    
    async def _execute_moctl(self, method: str, target: str, params: str = "") -> Dict[str, Any]:
        """Execute mo_ctl command asynchronously"""
        import json
        
        try:
            # Build mo_ctl SQL command
            if params:
                sql = f"SELECT mo_ctl('{method}', '{target}', '{params}')"
            else:
                sql = f"SELECT mo_ctl('{method}', '{target}', '')"
            
            # Execute the command
            result = await self.client.execute(sql)
            
            if not result.rows:
                raise MoCtlError(f"mo_ctl command returned no results: {sql}")
            
            # Parse the JSON result
            result_str = result.rows[0][0]
            parsed_result = json.loads(result_str)
            
            # Check for errors in the result
            if "result" in parsed_result and parsed_result["result"]:
                first_result = parsed_result["result"][0]
                if "returnStr" in first_result and first_result["returnStr"] != "OK":
                    raise MoCtlError(f"mo_ctl operation failed: {first_result['returnStr']}")
            
            return parsed_result
            
        except json.JSONDecodeError as e:
            raise MoCtlError(f"Failed to parse mo_ctl result: {e}")
        except Exception as e:
            raise MoCtlError(f"mo_ctl operation failed: {e}")
    
    async def flush_table(self, database: str, table: str) -> Dict[str, Any]:
        """Force flush table asynchronously"""
        table_ref = f"{database}.{table}"
        return await self._execute_moctl('dn', 'flush', table_ref)
    
    async def checkpoint(self) -> Dict[str, Any]:
        """Force checkpoint asynchronously"""
        return await self._execute_moctl('dn', 'checkpoint', '')


class AsyncClient:
    """
    MatrixOne Async Client
    
    Provides asynchronous connection and query functionality for MatrixOne database.
    """
    
    def __init__(self, 
                 connection_timeout: int = 30,
                 query_timeout: int = 300,
                 auto_commit: bool = True,
                 charset: str = 'utf8mb4'):
        """
        Initialize MatrixOne async client
        
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
        self._connection_params = {}
        self._snapshots = AsyncSnapshotManager(self)
        self._clone = AsyncCloneManager(self)
        self._moctl = AsyncMoCtlManager(self)
        self._restore = AsyncRestoreManager(self)
    
    async def connect(self, 
                     host: str, 
                     port: int, 
                     user: str, 
                     password: str, 
                     database: str = None):
        """
        Connect to MatrixOne database asynchronously
        
        Args:
            host: Database host
            port: Database port
            user: Username
            password: Password
            database: Database name
        """
        try:
            self._connection_params = {
                'host': host,
                'port': port,
                'user': user,
                'password': password,
                'db': database,  # aiomysql uses 'db' instead of 'database'
                'charset': self.charset,
                'autocommit': self.auto_commit,
                'connect_timeout': self.connection_timeout
            }
            
            self._connection = await aiomysql.connect(**self._connection_params)
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to MatrixOne: {e}")
    
    async def disconnect(self):
        """Disconnect from MatrixOne database asynchronously"""
        if self._connection:
            self._connection.close()
            await self._connection.wait_closed()
            self._connection = None
    
    async def execute(self, sql: str, params: Optional[Tuple] = None) -> AsyncResultSet:
        """
        Execute SQL query asynchronously
        
        Args:
            sql: SQL query string
            params: Query parameters
            
        Returns:
            AsyncResultSet with query results
        """
        if not self._connection:
            raise ConnectionError("Not connected to database")
        
        try:
            async with self._connection.cursor() as cursor:
                await cursor.execute(sql, params)
                
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = await cursor.fetchall()
                    return AsyncResultSet(columns, rows)
                else:
                    return AsyncResultSet([], [], affected_rows=cursor.rowcount)
                    
        except Exception as e:
            raise QueryError(f"Query execution failed: {e}")
    
    async def snapshot_query(self, snapshot_name: str, sql: str, params: Optional[Tuple] = None) -> AsyncResultSet:
        """
        Execute query with snapshot asynchronously
        
        Args:
            snapshot_name: Name of the snapshot
            sql: SQL query string
            params: Query parameters
            
        Returns:
            AsyncResultSet with query results
        """
        if not self._connection:
            raise ConnectionError("Not connected to database")
        
        try:
            async with self._connection.cursor() as cursor:
                snapshot_sql = f"{sql} FOR SNAPSHOT '{snapshot_name}'"
                await cursor.execute(snapshot_sql, params)
                
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = await cursor.fetchall()
                    return AsyncResultSet(columns, rows)
                else:
                    return AsyncResultSet([], [], affected_rows=cursor.rowcount)
                    
        except Exception as e:
            raise QueryError(f"Snapshot query execution failed: {e}")
    
    @asynccontextmanager
    async def transaction(self):
        """
        Async transaction context manager
        
        Usage:
            async with client.transaction() as tx:
                await tx.execute("INSERT INTO users ...")
                await tx.execute("UPDATE users ...")
                # Snapshot and clone operations within transaction
                await tx.snapshots.create("snap1", "table", database="db1", table="t1")
                await tx.clone.clone_database("target_db", "source_db")
        """
        if not self._connection:
            raise ConnectionError("Not connected to database")
        
        tx_wrapper = None
        try:
            await self._connection.begin()
            tx_wrapper = AsyncTransactionWrapper(self._connection, self)
            yield tx_wrapper
            
            # Commit SQLAlchemy session first
            await tx_wrapper.commit_sqlalchemy()
            # Then commit the main transaction
            await self._connection.commit()
            
        except Exception as e:
            # Rollback SQLAlchemy session first
            if tx_wrapper:
                await tx_wrapper.rollback_sqlalchemy()
            # Then rollback the main transaction
            await self._connection.rollback()
            raise e
        finally:
            # Clean up SQLAlchemy resources
            if tx_wrapper:
                await tx_wrapper.close_sqlalchemy()
    
    @property
    def snapshots(self) -> AsyncSnapshotManager:
        """Get async snapshot manager"""
        return self._snapshots
    
    @property
    def clone(self) -> AsyncCloneManager:
        """Get async clone manager"""
        return self._clone
    
    @property
    def moctl(self) -> AsyncMoCtlManager:
        """Get async mo_ctl manager"""
        return self._moctl
    
    @property
    def restore(self) -> AsyncRestoreManager:
        """Get async restore manager"""
        return self._restore
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()


class AsyncTransactionWrapper:
    """Async transaction wrapper for executing queries within a transaction"""
    
    def __init__(self, connection, client):
        self.connection = connection
        self.client = client
        # Create snapshot, clone, and restore managers that use this transaction
        self.snapshots = AsyncTransactionSnapshotManager(client, self)
        self.clone = AsyncTransactionCloneManager(client, self)
        self.restore = AsyncTransactionRestoreManager(client, self)
        # SQLAlchemy integration
        self._sqlalchemy_session = None
        self._sqlalchemy_engine = None
    
    async def execute(self, sql: str, params: Optional[Tuple] = None) -> AsyncResultSet:
        """Execute SQL within transaction asynchronously"""
        try:
            async with self.connection.cursor() as cursor:
                await cursor.execute(sql, params)
                
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = await cursor.fetchall()
                    return AsyncResultSet(columns, rows)
                else:
                    return AsyncResultSet([], [], affected_rows=cursor.rowcount)
                    
        except Exception as e:
            raise QueryError(f"Transaction query execution failed: {e}")
    
    async def get_sqlalchemy_session(self):
        """
        Get async SQLAlchemy session that uses the same transaction asynchronously
        
        Returns:
            Async SQLAlchemy Session instance bound to this transaction
        """
        if self._sqlalchemy_session is None:
            from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
            
            # Create engine using the same connection parameters
            if not self.client._connection_params:
                raise ConnectionError("Not connected to database")
            
            connection_string = (
                f"mysql+aiomysql://{self.client._connection_params['user']}:"
                f"{self.client._connection_params['password']}@"
                f"{self.client._connection_params['host']}:"
                f"{self.client._connection_params['port']}/"
                f"{self.client._connection_params['db']}"
            )
            
            # Create async engine that will use the same connection
            self._sqlalchemy_engine = create_async_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=300
            )
            
            # Create async session factory
            AsyncSessionLocal = async_sessionmaker(
                bind=self._sqlalchemy_engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            self._sqlalchemy_session = AsyncSessionLocal()
            
            # Begin async SQLAlchemy transaction
            await self._sqlalchemy_session.begin()
        
        return self._sqlalchemy_session
    
    async def commit_sqlalchemy(self):
        """Commit async SQLAlchemy session asynchronously"""
        if self._sqlalchemy_session:
            await self._sqlalchemy_session.commit()
    
    async def rollback_sqlalchemy(self):
        """Rollback async SQLAlchemy session asynchronously"""
        if self._sqlalchemy_session:
            await self._sqlalchemy_session.rollback()
    
    async def close_sqlalchemy(self):
        """Close async SQLAlchemy session asynchronously"""
        if self._sqlalchemy_session:
            await self._sqlalchemy_session.close()
            self._sqlalchemy_session = None
        if self._sqlalchemy_engine:
            await self._sqlalchemy_engine.dispose()
            self._sqlalchemy_engine = None


class AsyncTransactionSnapshotManager(AsyncSnapshotManager):
    """Async snapshot manager that executes operations within a transaction"""
    
    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper
    
    async def create(self, name: str, level: Union[str, SnapshotLevel], database: Optional[str] = None, 
                    table: Optional[str] = None, description: Optional[str] = None) -> Snapshot:
        """Create snapshot within transaction asynchronously"""
        return await super().create(name, level, database, table, description)
    
    async def get(self, name: str) -> Snapshot:
        """Get snapshot within transaction asynchronously"""
        return await super().get(name)
    
    async def delete(self, name: str) -> None:
        """Delete snapshot within transaction asynchronously"""
        return await super().delete(name)


class AsyncTransactionRestoreManager(AsyncRestoreManager):
    """Async restore manager for use within transactions"""
    
    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper
    
    async def restore_cluster(self, snapshot_name: str) -> bool:
        """Restore cluster within transaction asynchronously"""
        try:
            sql = f"RESTORE CLUSTER FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}"
            result = await self.transaction_wrapper.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore cluster from snapshot '{snapshot_name}': {e}")
    
    async def restore_tenant(self, 
                           snapshot_name: str, 
                           account_name: str,
                           to_account: Optional[str] = None) -> bool:
        """Restore tenant within transaction asynchronously"""
        try:
            if to_account:
                sql = (f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                      f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)} "
                      f"TO ACCOUNT {self.client._escape_identifier(to_account)}")
            else:
                sql = (f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                      f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}")
            
            result = await self.transaction_wrapper.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore tenant '{account_name}' from snapshot '{snapshot_name}': {e}")
    
    async def restore_database(self, 
                             snapshot_name: str, 
                             account_name: str,
                             database_name: str,
                             to_account: Optional[str] = None) -> bool:
        """Restore database within transaction asynchronously"""
        try:
            if to_account:
                sql = (f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                      f"DATABASE {self.client._escape_identifier(database_name)} "
                      f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)} "
                      f"TO ACCOUNT {self.client._escape_identifier(to_account)}")
            else:
                sql = (f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                      f"DATABASE {self.client._escape_identifier(database_name)} "
                      f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}")
            
            result = await self.transaction_wrapper.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore database '{database_name}' from snapshot '{snapshot_name}': {e}")
    
    async def restore_table(self, 
                          snapshot_name: str, 
                          account_name: str,
                          database_name: str,
                          table_name: str,
                          to_account: Optional[str] = None) -> bool:
        """Restore table within transaction asynchronously"""
        try:
            if to_account:
                sql = (f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                      f"DATABASE {self.client._escape_identifier(database_name)} "
                      f"TABLE {self.client._escape_identifier(table_name)} "
                      f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)} "
                      f"TO ACCOUNT {self.client._escape_identifier(to_account)}")
            else:
                sql = (f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                      f"DATABASE {self.client._escape_identifier(database_name)} "
                      f"TABLE {self.client._escape_identifier(table_name)} "
                      f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}")
            
            result = await self.transaction_wrapper.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore table '{table_name}' from snapshot '{snapshot_name}': {e}")


class AsyncTransactionCloneManager(AsyncCloneManager):
    """Async clone manager that executes operations within a transaction"""
    
    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper
    
    async def clone_database(self, target_db: str, source_db: str, 
                           snapshot_name: Optional[str] = None, 
                           if_not_exists: bool = False) -> None:
        """Clone database within transaction asynchronously"""
        return await super().clone_database(target_db, source_db, snapshot_name, if_not_exists)
    
    async def clone_table(self, target_table: str, source_table: str,
                         snapshot_name: Optional[str] = None,
                         if_not_exists: bool = False) -> None:
        """Clone table within transaction asynchronously"""
        return await super().clone_table(target_table, source_table, snapshot_name, if_not_exists)
    
    async def clone_database_with_snapshot(self, target_db: str, source_db: str, 
                                         snapshot_name: str, if_not_exists: bool = False) -> None:
        """Clone database with snapshot within transaction asynchronously"""
        return await super().clone_database_with_snapshot(target_db, source_db, snapshot_name, if_not_exists)
    
    async def clone_table_with_snapshot(self, target_table: str, source_table: str,
                                      snapshot_name: str, if_not_exists: bool = False) -> None:
        """Clone table with snapshot within transaction asynchronously"""
        return await super().clone_table_with_snapshot(target_table, source_table, snapshot_name, if_not_exists)
