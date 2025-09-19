"""
MatrixOne Async Client - Asynchronous implementation
"""

import asyncio
import aiomysql
from typing import Optional, Dict, Any, List, Tuple, Union
from contextlib import asynccontextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .exceptions import ConnectionError, QueryError, ConfigurationError, SnapshotError, CloneError, MoCtlError, RestoreError, PitrError, PubSubError, AccountError
from .snapshot import SnapshotManager, SnapshotQueryBuilder, CloneManager, Snapshot, SnapshotLevel
from .moctl import MoCtlManager
from .restore import RestoreManager
from .pitr import PitrManager, Pitr
from .pubsub import PubSubManager, Publication, Subscription
from .account import AccountManager, Account, User, TransactionAccountManager


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


class AsyncPubSubManager:
    """Async manager for publish-subscribe operations"""
    
    def __init__(self, client):
        self.client = client
    
    async def create_database_publication(self, 
                                         name: str, 
                                         database: str, 
                                         account: str) -> Publication:
        """Create database-level publication asynchronously"""
        try:
            sql = (f"CREATE PUBLICATION {self.client._escape_identifier(name)} "
                   f"DATABASE {self.client._escape_identifier(database)} "
                   f"ACCOUNT {self.client._escape_identifier(account)}")
            
            result = await self.client.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create database publication '{name}'")
            
            return await self.get_publication(name)
            
        except Exception as e:
            raise PubSubError(f"Failed to create database publication '{name}': {e}")
    
    async def create_table_publication(self, 
                                      name: str, 
                                      database: str, 
                                      table: str, 
                                      account: str) -> Publication:
        """Create table-level publication asynchronously"""
        try:
            sql = (f"CREATE PUBLICATION {self.client._escape_identifier(name)} "
                   f"DATABASE {self.client._escape_identifier(database)} "
                   f"TABLE {self.client._escape_identifier(table)} "
                   f"ACCOUNT {self.client._escape_identifier(account)}")
            
            result = await self.client.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create table publication '{name}'")
            
            return await self.get_publication(name)
            
        except Exception as e:
            raise PubSubError(f"Failed to create table publication '{name}': {e}")
    
    async def get_publication(self, name: str) -> Publication:
        """Get publication by name asynchronously"""
        try:
            sql = f"SHOW PUBLICATIONS WHERE publication = {self.client._escape_string(name)}"
            result = await self.client.execute(sql)
            
            if not result or not result.rows:
                raise PubSubError(f"Publication '{name}' not found")
            
            row = result.rows[0]
            return self._row_to_publication(row)
            
        except Exception as e:
            raise PubSubError(f"Failed to get publication '{name}': {e}")
    
    async def list_publications(self, 
                               account: Optional[str] = None,
                               database: Optional[str] = None) -> List[Publication]:
        """List publications with optional filters asynchronously"""
        try:
            conditions = []
            
            if account:
                conditions.append(f"sub_account LIKE {self.client._escape_string(f'%{account}%')}")
            if database:
                conditions.append(f"database = {self.client._escape_string(database)}")
            
            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
            else:
                where_clause = ""
            
            sql = f"SHOW PUBLICATIONS{where_clause}"
            result = await self.client.execute(sql)
            
            if not result or not result.rows:
                return []
            
            return [self._row_to_publication(row) for row in result.rows]
            
        except Exception as e:
            raise PubSubError(f"Failed to list publications: {e}")
    
    async def alter_publication(self, 
                               name: str, 
                               account: Optional[str] = None,
                               database: Optional[str] = None,
                               table: Optional[str] = None) -> Publication:
        """Alter publication asynchronously"""
        try:
            # Build ALTER PUBLICATION statement
            parts = [f"ALTER PUBLICATION {self.client._escape_identifier(name)}"]
            
            if account:
                parts.append(f"ACCOUNT {self.client._escape_identifier(account)}")
            if database:
                parts.append(f"DATABASE {self.client._escape_identifier(database)}")
            if table:
                parts.append(f"TABLE {self.client._escape_identifier(table)}")
            
            sql = " ".join(parts)
            result = await self.client.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to alter publication '{name}'")
            
            return await self.get_publication(name)
            
        except Exception as e:
            raise PubSubError(f"Failed to alter publication '{name}': {e}")
    
    async def drop_publication(self, name: str) -> bool:
        """Drop publication asynchronously"""
        try:
            sql = f"DROP PUBLICATION {self.client._escape_identifier(name)}"
            result = await self.client.execute(sql)
            return result is not None
            
        except Exception as e:
            raise PubSubError(f"Failed to drop publication '{name}': {e}")
    
    async def show_create_publication(self, name: str) -> str:
        """Show CREATE PUBLICATION statement for a publication asynchronously"""
        try:
            sql = f"SHOW CREATE PUBLICATION {self.client._escape_identifier(name)}"
            result = await self.client.execute(sql)
            
            if not result or not result.rows:
                raise PubSubError(f"Publication '{name}' not found")
            
            # The result should contain the CREATE statement
            # Assuming the CREATE statement is in the first column
            return result.rows[0][0]
            
        except Exception as e:
            raise PubSubError(f"Failed to show create publication '{name}': {e}")
    
    async def create_subscription(self, 
                                 subscription_name: str, 
                                 publication_name: str, 
                                 publisher_account: str) -> Subscription:
        """Create subscription from publication asynchronously"""
        try:
            sql = (f"CREATE DATABASE {self.client._escape_identifier(subscription_name)} "
                   f"FROM {self.client._escape_identifier(publisher_account)} "
                   f"PUBLICATION {self.client._escape_identifier(publication_name)}")
            
            result = await self.client.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create subscription '{subscription_name}'")
            
            return await self.get_subscription(subscription_name)
            
        except Exception as e:
            raise PubSubError(f"Failed to create subscription '{subscription_name}': {e}")
    
    async def get_subscription(self, name: str) -> Subscription:
        """Get subscription by name asynchronously"""
        try:
            sql = f"SHOW SUBSCRIPTIONS WHERE sub_name = {self.client._escape_string(name)}"
            result = await self.client.execute(sql)
            
            if not result or not result.rows:
                raise PubSubError(f"Subscription '{name}' not found")
            
            row = result.rows[0]
            return self._row_to_subscription(row)
            
        except Exception as e:
            raise PubSubError(f"Failed to get subscription '{name}': {e}")
    
    async def list_subscriptions(self, 
                                pub_account: Optional[str] = None,
                                pub_database: Optional[str] = None) -> List[Subscription]:
        """List subscriptions with optional filters asynchronously"""
        try:
            conditions = []
            
            if pub_account:
                conditions.append(f"pub_account = {self.client._escape_string(pub_account)}")
            if pub_database:
                conditions.append(f"pub_database = {self.client._escape_string(pub_database)}")
            
            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
            else:
                where_clause = ""
            
            sql = f"SHOW SUBSCRIPTIONS{where_clause}"
            result = await self.client.execute(sql)
            
            if not result or not result.rows:
                return []
            
            return [self._row_to_subscription(row) for row in result.rows]
            
        except Exception as e:
            raise PubSubError(f"Failed to list subscriptions: {e}")
    
    def _row_to_publication(self, row: tuple) -> Publication:
        """Convert database row to Publication object"""
        # Expected columns: publication, database, tables, sub_account, subscribed_accounts, create_time, update_time, comments
        return Publication(
            name=row[0],
            database=row[1],
            tables=row[2],
            sub_account=row[3],
            subscribed_accounts=row[4],
            created_time=row[5] if len(row) > 5 else None,
            update_time=row[6] if len(row) > 6 else None,
            comments=row[7] if len(row) > 7 else None
        )
    
    def _row_to_subscription(self, row: tuple) -> Subscription:
        """Convert database row to Subscription object"""
        # Expected columns: pub_name, pub_account, pub_database, pub_tables, pub_comment, 
        # pub_time, sub_name, sub_time, status
        return Subscription(
            pub_name=row[0],
            pub_account=row[1],
            pub_database=row[2],
            pub_tables=row[3],
            pub_comment=row[4] if len(row) > 4 else None,
            pub_time=row[5] if len(row) > 5 else None,
            sub_name=row[6] if len(row) > 6 else None,
            sub_time=row[7] if len(row) > 7 else None,
            status=row[8] if len(row) > 8 else 0
        )


class AsyncPitrManager:
    """Async manager for PITR operations"""
    
    def __init__(self, client):
        self.client = client
    
    async def create_cluster_pitr(self, 
                                 name: str, 
                                 range_value: int = 1, 
                                 range_unit: str = 'd') -> Pitr:
        """Create cluster-level PITR asynchronously"""
        try:
            self._validate_range(range_value, range_unit)
            
            sql = (f"CREATE PITR {self.client._escape_identifier(name)} "
                   f"FOR CLUSTER RANGE {range_value} '{range_unit}'")
            
            result = await self.client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create cluster PITR '{name}'")
            
            return await self.get(name)
            
        except Exception as e:
            raise PitrError(f"Failed to create cluster PITR '{name}': {e}")
    
    async def create_account_pitr(self, 
                                 name: str, 
                                 account_name: Optional[str] = None,
                                 range_value: int = 1, 
                                 range_unit: str = 'd') -> Pitr:
        """Create account-level PITR asynchronously"""
        try:
            self._validate_range(range_value, range_unit)
            
            if account_name:
                sql = (f"CREATE PITR {self.client._escape_identifier(name)} "
                       f"FOR ACCOUNT {self.client._escape_identifier(account_name)} "
                       f"RANGE {range_value} '{range_unit}'")
            else:
                sql = (f"CREATE PITR {self.client._escape_identifier(name)} "
                       f"FOR ACCOUNT RANGE {range_value} '{range_unit}'")
            
            result = await self.client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create account PITR '{name}'")
            
            return await self.get(name)
            
        except Exception as e:
            raise PitrError(f"Failed to create account PITR '{name}': {e}")
    
    async def create_database_pitr(self, 
                                  name: str, 
                                  database_name: str,
                                  range_value: int = 1, 
                                  range_unit: str = 'd') -> Pitr:
        """Create database-level PITR asynchronously"""
        try:
            self._validate_range(range_value, range_unit)
            
            sql = (f"CREATE PITR {self.client._escape_identifier(name)} "
                   f"FOR DATABASE {self.client._escape_identifier(database_name)} "
                   f"RANGE {range_value} '{range_unit}'")
            
            result = await self.client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create database PITR '{name}'")
            
            return await self.get(name)
            
        except Exception as e:
            raise PitrError(f"Failed to create database PITR '{name}': {e}")
    
    async def create_table_pitr(self, 
                               name: str, 
                               database_name: str,
                               table_name: str,
                               range_value: int = 1, 
                               range_unit: str = 'd') -> Pitr:
        """Create table-level PITR asynchronously"""
        try:
            self._validate_range(range_value, range_unit)
            
            sql = (f"CREATE PITR {self.client._escape_identifier(name)} "
                   f"FOR TABLE {self.client._escape_identifier(database_name)} "
                   f"TABLE {self.client._escape_identifier(table_name)} "
                   f"RANGE {range_value} '{range_unit}'")
            
            result = await self.client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create table PITR '{name}'")
            
            return await self.get(name)
            
        except Exception as e:
            raise PitrError(f"Failed to create table PITR '{name}': {e}")
    
    async def get(self, name: str) -> Pitr:
        """Get PITR by name asynchronously"""
        try:
            sql = f"SHOW PITR WHERE pitr_name = {self.client._escape_string(name)}"
            result = await self.client.execute(sql)
            
            if not result or not result.rows:
                raise PitrError(f"PITR '{name}' not found")
            
            row = result.rows[0]
            return self._row_to_pitr(row)
            
        except Exception as e:
            raise PitrError(f"Failed to get PITR '{name}': {e}")
    
    async def list(self, 
                   level: Optional[str] = None,
                   account_name: Optional[str] = None,
                   database_name: Optional[str] = None,
                   table_name: Optional[str] = None) -> List[Pitr]:
        """List PITRs with optional filters asynchronously"""
        try:
            conditions = []
            
            if level:
                conditions.append(f"pitr_level = {self.client._escape_string(level)}")
            if account_name:
                conditions.append(f"account_name = {self.client._escape_string(account_name)}")
            if database_name:
                conditions.append(f"database_name = {self.client._escape_string(database_name)}")
            if table_name:
                conditions.append(f"table_name = {self.client._escape_string(table_name)}")
            
            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
            else:
                where_clause = ""
            
            sql = f"SHOW PITR{where_clause}"
            result = await self.client.execute(sql)
            
            if not result or not result.rows:
                return []
            
            return [self._row_to_pitr(row) for row in result.rows]
            
        except Exception as e:
            raise PitrError(f"Failed to list PITRs: {e}")
    
    async def alter(self, 
                    name: str, 
                    range_value: int, 
                    range_unit: str) -> Pitr:
        """Alter PITR range asynchronously"""
        try:
            self._validate_range(range_value, range_unit)
            
            sql = (f"ALTER PITR {self.client._escape_identifier(name)} "
                   f"RANGE {range_value} '{range_unit}'")
            
            result = await self.client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to alter PITR '{name}'")
            
            return await self.get(name)
            
        except Exception as e:
            raise PitrError(f"Failed to alter PITR '{name}': {e}")
    
    async def delete(self, name: str) -> bool:
        """Delete PITR asynchronously"""
        try:
            sql = f"DROP PITR {self.client._escape_identifier(name)}"
            result = await self.client.execute(sql)
            return result is not None
            
        except Exception as e:
            raise PitrError(f"Failed to delete PITR '{name}': {e}")
    
    def _validate_range(self, range_value: int, range_unit: str) -> None:
        """Validate PITR range parameters"""
        if not (1 <= range_value <= 100):
            raise PitrError("Range value must be between 1 and 100")
        
        valid_units = ['h', 'd', 'mo', 'y']
        if range_unit not in valid_units:
            raise PitrError(f"Range unit must be one of: {', '.join(valid_units)}")
    
    def _row_to_pitr(self, row: tuple) -> Pitr:
        """Convert database row to Pitr object"""
        # Expected columns: pitr_name, created_time, modified_time, pitr_level, 
        # account_name, database_name, table_name, pitr_length, pitr_unit
        return Pitr(
            name=row[0],
            created_time=row[1],
            modified_time=row[2],
            level=row[3],
            account_name=row[4] if row[4] != '*' else None,
            database_name=row[5] if row[5] != '*' else None,
            table_name=row[6] if row[6] != '*' else None,
            range_value=row[7],
            range_unit=row[8]
        )


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


class AsyncAccountManager:
    """Async manager for MatrixOne account operations"""
    
    def __init__(self, client):
        self.client = client
    
    async def create_account(self, 
                           account_name: str,
                           admin_name: str,
                           password: str,
                           comment: Optional[str] = None,
                           admin_comment: Optional[str] = None,
                           admin_host: str = '%',
                           admin_identified_by: Optional[str] = None) -> Account:
        """Create a new account asynchronously"""
        try:
            sql_parts = [f"CREATE ACCOUNT {self.client._escape_identifier(account_name)}"]
            sql_parts.append(f"ADMIN_NAME {self.client._escape_string(admin_name)}")
            sql_parts.append(f"IDENTIFIED BY {self.client._escape_string(password)}")
            
            if admin_host != '%':
                sql_parts.append(f"ADMIN_HOST {self.client._escape_string(admin_host)}")
            
            if comment:
                sql_parts.append(f"COMMENT {self.client._escape_string(comment)}")
            
            if admin_comment:
                sql_parts.append(f"ADMIN_COMMENT {self.client._escape_string(admin_comment)}")
            
            if admin_identified_by:
                sql_parts.append(f"ADMIN_IDENTIFIED BY {self.client._escape_string(admin_identified_by)}")
            
            sql = " ".join(sql_parts)
            await self.client.execute(sql)
            
            return await self.get_account(account_name)
            
        except Exception as e:
            raise AccountError(f"Failed to create account '{account_name}': {e}")
    
    async def drop_account(self, account_name: str) -> None:
        """Drop an account asynchronously"""
        try:
            sql = f"DROP ACCOUNT {self.client._escape_identifier(account_name)}"
            await self.client.execute(sql)
        except Exception as e:
            raise AccountError(f"Failed to drop account '{account_name}': {e}")
    
    async def alter_account(self, 
                          account_name: str,
                          comment: Optional[str] = None,
                          suspend: Optional[bool] = None,
                          suspend_reason: Optional[str] = None) -> Account:
        """Alter an account asynchronously"""
        try:
            sql_parts = [f"ALTER ACCOUNT {self.client._escape_identifier(account_name)}"]
            
            if comment is not None:
                sql_parts.append(f"COMMENT {self.client._escape_string(comment)}")
            
            if suspend is not None:
                if suspend:
                    if suspend_reason:
                        sql_parts.append(f"SUSPEND COMMENT {self.client._escape_string(suspend_reason)}")
                    else:
                        sql_parts.append("SUSPEND")
                else:
                    sql_parts.append("OPEN")
            
            sql = " ".join(sql_parts)
            await self.client.execute(sql)
            
            return await self.get_account(account_name)
            
        except Exception as e:
            raise AccountError(f"Failed to alter account '{account_name}': {e}")
    
    async def get_account(self, account_name: str) -> Account:
        """Get account by name asynchronously"""
        try:
            sql = "SHOW ACCOUNTS"
            result = await self.client.execute(sql)
            
            if not result or not result.rows:
                raise AccountError(f"Account '{account_name}' not found")
            
            for row in result.rows:
                if row[0] == account_name:
                    return self._row_to_account(row)
            
            raise AccountError(f"Account '{account_name}' not found")
            
        except Exception as e:
            raise AccountError(f"Failed to get account '{account_name}': {e}")
    
    async def list_accounts(self) -> List[Account]:
        """List all accounts asynchronously"""
        try:
            sql = "SHOW ACCOUNTS"
            result = await self.client.execute(sql)
            
            if not result or not result.rows:
                return []
            
            return [self._row_to_account(row) for row in result.rows]
            
        except Exception as e:
            raise AccountError(f"Failed to list accounts: {e}")
    
    async def create_user(self, 
                        user_name: str,
                        password: str,
                        account_name: str,
                        host: str = '%',
                        comment: Optional[str] = None,
                        identified_by: Optional[str] = None) -> User:
        """Create a new user asynchronously"""
        try:
            # Build CREATE USER statement according to MatrixOne syntax
            # MatrixOne syntax: CREATE USER user_name IDENTIFIED BY 'password'
            sql_parts = [f"CREATE USER {self.client._escape_identifier(user_name)}"]
            
            if host != '%':
                sql_parts.append(f"@{self.client._escape_string(host)}")
            
            sql_parts.append(f"IDENTIFIED BY {self.client._escape_string(password)}")
            
            # Note: MatrixOne doesn't support ACCOUNT, COMMENT, or ATTRIBUTE clauses in CREATE USER
            # sql_parts.append(f"ACCOUNT {self.client._escape_identifier(account_name)}")
            # if comment:
            #     sql_parts.append(f"COMMENT {self.client._escape_string(comment)}")
            # if identified_by:
            #     sql_parts.append(f"IDENTIFIED BY {self.client._escape_string(identified_by)}")
            
            sql = " ".join(sql_parts)
            await self.client.execute(sql)
            
            return await self.get_user(user_name, host, account_name)
            
        except Exception as e:
            raise AccountError(f"Failed to create user '{user_name}': {e}")
    
    async def drop_user(self, user_name: str, host: str = '%', account_name: Optional[str] = None) -> None:
        """Drop a user asynchronously"""
        try:
            sql_parts = [f"DROP USER {self.client._escape_identifier(user_name)}"]
            
            if host != '%':
                sql_parts.append(f"@{self.client._escape_string(host)}")
            
            if account_name:
                sql_parts.append(f"ACCOUNT {self.client._escape_identifier(account_name)}")
            
            sql = " ".join(sql_parts)
            await self.client.execute(sql)
            
        except Exception as e:
            raise AccountError(f"Failed to drop user '{user_name}': {e}")
    
    async def alter_user(self, 
                       user_name: str,
                       host: str = '%',
                       account_name: Optional[str] = None,
                       password: Optional[str] = None,
                       comment: Optional[str] = None,
                       lock: Optional[bool] = None,
                       lock_reason: Optional[str] = None) -> User:
        """Alter a user asynchronously"""
        try:
            sql_parts = [f"ALTER USER {self.client._escape_identifier(user_name)}"]
            
            if host != '%':
                sql_parts.append(f"@{self.client._escape_string(host)}")
            
            if account_name:
                sql_parts.append(f"ACCOUNT {self.client._escape_identifier(account_name)}")
            
            if password is not None:
                sql_parts.append(f"IDENTIFIED BY {self.client._escape_string(password)}")
            
            if comment is not None:
                sql_parts.append(f"COMMENT {self.client._escape_string(comment)}")
            
            if lock is not None:
                if lock:
                    if lock_reason:
                        sql_parts.append(f"ACCOUNT LOCK COMMENT {self.client._escape_string(lock_reason)}")
                    else:
                        sql_parts.append("ACCOUNT LOCK")
                else:
                    sql_parts.append("ACCOUNT UNLOCK")
            
            sql = " ".join(sql_parts)
            await self.client.execute(sql)
            
            return await self.get_user(user_name, host, account_name)
            
        except Exception as e:
            raise AccountError(f"Failed to alter user '{user_name}': {e}")
    
    async def get_user(self, user_name: str, host: str = '%', account_name: Optional[str] = None) -> User:
        """Get user by name, host, and account asynchronously"""
        try:
            sql = "SHOW GRANTS"
            result = await self.client.execute(sql)
            
            if not result or not result.rows:
                raise AccountError(f"User '{user_name}' not found")
            
            for row in result.rows:
                if (row[0] == user_name and
                    row[1] == host and
                    (not account_name or row[2] == account_name)):
                    return self._row_to_user(row)
            
            raise AccountError(f"User '{user_name}' not found")
            
        except Exception as e:
            raise AccountError(f"Failed to get user '{user_name}': {e}")
    
    async def list_users(self, account_name: Optional[str] = None) -> List[User]:
        """List users with optional account filter asynchronously"""
        try:
            sql = "SHOW GRANTS"
            result = await self.client.execute(sql)
            
            if not result or not result.rows:
                return []
            
            users = [self._row_to_user(row) for row in result.rows]
            
            if account_name:
                users = [user for user in users if user.account == account_name]
            
            return users
            
        except Exception as e:
            raise AccountError(f"Failed to list users: {e}")
    
    def _row_to_account(self, row: tuple) -> Account:
        """Convert database row to Account object"""
        return Account(
            name=row[0],
            admin_name=row[1],
            created_time=row[2] if len(row) > 2 else None,
            status=row[3] if len(row) > 3 else None,
            comment=row[4] if len(row) > 4 else None,
            suspended_time=row[5] if len(row) > 5 else None,
            suspended_reason=row[6] if len(row) > 6 else None
        )
    
    def _row_to_user(self, row: tuple) -> User:
        """Convert database row to User object"""
        return User(
            name=row[0],
            host=row[1],
            account=row[2],
            created_time=row[3] if len(row) > 3 else None,
            status=row[4] if len(row) > 4 else None,
            comment=row[5] if len(row) > 5 else None,
            locked_time=row[6] if len(row) > 6 else None,
            locked_reason=row[7] if len(row) > 7 else None
        )


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
        self._pitr = AsyncPitrManager(self)
        self._pubsub = AsyncPubSubManager(self)
        self._account = AsyncAccountManager(self)
    
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
            # Wait for connection to be properly closed
            if hasattr(self._connection, 'wait_closed'):
                await self._connection.wait_closed()
            elif hasattr(self._connection, 'ensure_closed'):
                await self._connection.ensure_closed()
            self._connection = None
    
    def _escape_identifier(self, identifier: str) -> str:
        """Escapes an identifier to prevent SQL injection."""
        return f"`{identifier}`"
    
    def _escape_string(self, value: str) -> str:
        """Escapes a string value for SQL queries."""
        return f"'{value}'"
    
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
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()


class AsyncTransactionWrapper:
    """Async transaction wrapper for executing queries within a transaction"""
    
    def __init__(self, connection, client):
        self.connection = connection
        self.client = client
        # Create snapshot, clone, restore, PITR, pubsub, and account managers that use this transaction
        self.snapshots = AsyncTransactionSnapshotManager(client, self)
        self.clone = AsyncTransactionCloneManager(client, self)
        self.restore = AsyncTransactionRestoreManager(client, self)
        self.pitr = AsyncTransactionPitrManager(client, self)
        self.pubsub = AsyncTransactionPubSubManager(client, self)
        self.account = AsyncTransactionAccountManager(self)
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


class AsyncTransactionPubSubManager(AsyncPubSubManager):
    """Async publish-subscribe manager for use within transactions"""
    
    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper
    
    async def create_database_publication(self, 
                                         name: str, 
                                         database: str, 
                                         account: str) -> Publication:
        """Create database publication within transaction asynchronously"""
        try:
            sql = (f"CREATE PUBLICATION {self.client._escape_identifier(name)} "
                   f"DATABASE {self.client._escape_identifier(database)} "
                   f"ACCOUNT {self.client._escape_identifier(account)}")
            
            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create database publication '{name}'")
            
            return await self.get_publication(name)
            
        except Exception as e:
            raise PubSubError(f"Failed to create database publication '{name}': {e}")
    
    async def create_table_publication(self, 
                                      name: str, 
                                      database: str, 
                                      table: str, 
                                      account: str) -> Publication:
        """Create table publication within transaction asynchronously"""
        try:
            sql = (f"CREATE PUBLICATION {self.client._escape_identifier(name)} "
                   f"DATABASE {self.client._escape_identifier(database)} "
                   f"TABLE {self.client._escape_identifier(table)} "
                   f"ACCOUNT {self.client._escape_identifier(account)}")
            
            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create table publication '{name}'")
            
            return await self.get_publication(name)
            
        except Exception as e:
            raise PubSubError(f"Failed to create table publication '{name}': {e}")
    
    async def get_publication(self, name: str) -> Publication:
        """Get publication within transaction asynchronously"""
        try:
            sql = f"SHOW PUBLICATIONS WHERE pub_name = {self.client._escape_string(name)}"
            result = await self.transaction_wrapper.execute(sql)
            
            if not result or not result.rows:
                raise PubSubError(f"Publication '{name}' not found")
            
            row = result.rows[0]
            return self._row_to_publication(row)
            
        except Exception as e:
            raise PubSubError(f"Failed to get publication '{name}': {e}")
    
    async def list_publications(self, 
                               account: Optional[str] = None,
                               database: Optional[str] = None) -> List[Publication]:
        """List publications within transaction asynchronously"""
        try:
            conditions = []
            
            if account:
                conditions.append(f"pub_account = {self.client._escape_string(account)}")
            if database:
                conditions.append(f"pub_database = {self.client._escape_string(database)}")
            
            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
            else:
                where_clause = ""
            
            sql = f"SHOW PUBLICATIONS{where_clause}"
            result = await self.transaction_wrapper.execute(sql)
            
            if not result or not result.rows:
                return []
            
            return [self._row_to_publication(row) for row in result.rows]
            
        except Exception as e:
            raise PubSubError(f"Failed to list publications: {e}")
    
    async def alter_publication(self, 
                               name: str, 
                               account: Optional[str] = None,
                               database: Optional[str] = None,
                               table: Optional[str] = None) -> Publication:
        """Alter publication within transaction asynchronously"""
        try:
            # Build ALTER PUBLICATION statement
            parts = [f"ALTER PUBLICATION {self.client._escape_identifier(name)}"]
            
            if account:
                parts.append(f"ACCOUNT {self.client._escape_identifier(account)}")
            if database:
                parts.append(f"DATABASE {self.client._escape_identifier(database)}")
            if table:
                parts.append(f"TABLE {self.client._escape_identifier(table)}")
            
            sql = " ".join(parts)
            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to alter publication '{name}'")
            
            return await self.get_publication(name)
            
        except Exception as e:
            raise PubSubError(f"Failed to alter publication '{name}': {e}")
    
    async def drop_publication(self, name: str) -> bool:
        """Drop publication within transaction asynchronously"""
        try:
            sql = f"DROP PUBLICATION {self.client._escape_identifier(name)}"
            result = await self.transaction_wrapper.execute(sql)
            return result is not None
            
        except Exception as e:
            raise PubSubError(f"Failed to drop publication '{name}': {e}")
    
    async def create_subscription(self, 
                                 subscription_name: str, 
                                 publication_name: str, 
                                 publisher_account: str) -> Subscription:
        """Create subscription within transaction asynchronously"""
        try:
            sql = (f"CREATE DATABASE {self.client._escape_identifier(subscription_name)} "
                   f"FROM {self.client._escape_identifier(publisher_account)} "
                   f"PUBLICATION {self.client._escape_identifier(publication_name)}")
            
            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create subscription '{subscription_name}'")
            
            return await self.get_subscription(subscription_name)
            
        except Exception as e:
            raise PubSubError(f"Failed to create subscription '{subscription_name}': {e}")
    
    async def get_subscription(self, name: str) -> Subscription:
        """Get subscription within transaction asynchronously"""
        try:
            sql = f"SHOW SUBSCRIPTIONS WHERE sub_name = {self.client._escape_string(name)}"
            result = await self.transaction_wrapper.execute(sql)
            
            if not result or not result.rows:
                raise PubSubError(f"Subscription '{name}' not found")
            
            row = result.rows[0]
            return self._row_to_subscription(row)
            
        except Exception as e:
            raise PubSubError(f"Failed to get subscription '{name}': {e}")
    
    async def list_subscriptions(self, 
                                pub_account: Optional[str] = None,
                                pub_database: Optional[str] = None) -> List[Subscription]:
        """List subscriptions within transaction asynchronously"""
        try:
            conditions = []
            
            if pub_account:
                conditions.append(f"pub_account = {self.client._escape_string(pub_account)}")
            if pub_database:
                conditions.append(f"pub_database = {self.client._escape_string(pub_database)}")
            
            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
            else:
                where_clause = ""
            
            sql = f"SHOW SUBSCRIPTIONS{where_clause}"
            result = await self.transaction_wrapper.execute(sql)
            
            if not result or not result.rows:
                return []
            
            return [self._row_to_subscription(row) for row in result.rows]
            
        except Exception as e:
            raise PubSubError(f"Failed to list subscriptions: {e}")


class AsyncTransactionPitrManager(AsyncPitrManager):
    """Async PITR manager for use within transactions"""
    
    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper
    
    async def create_cluster_pitr(self, 
                                 name: str, 
                                 range_value: int = 1, 
                                 range_unit: str = 'd') -> Pitr:
        """Create cluster PITR within transaction asynchronously"""
        try:
            self._validate_range(range_value, range_unit)
            
            sql = (f"CREATE PITR {self.client._escape_identifier(name)} "
                   f"FOR CLUSTER RANGE {range_value} '{range_unit}'")
            
            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create cluster PITR '{name}'")
            
            return await self.get(name)
            
        except Exception as e:
            raise PitrError(f"Failed to create cluster PITR '{name}': {e}")
    
    async def create_account_pitr(self, 
                                 name: str, 
                                 account_name: Optional[str] = None,
                                 range_value: int = 1, 
                                 range_unit: str = 'd') -> Pitr:
        """Create account PITR within transaction asynchronously"""
        try:
            self._validate_range(range_value, range_unit)
            
            if account_name:
                sql = (f"CREATE PITR {self.client._escape_identifier(name)} "
                       f"FOR ACCOUNT {self.client._escape_identifier(account_name)} "
                       f"RANGE {range_value} '{range_unit}'")
            else:
                sql = (f"CREATE PITR {self.client._escape_identifier(name)} "
                       f"FOR ACCOUNT RANGE {range_value} '{range_unit}'")
            
            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create account PITR '{name}'")
            
            return await self.get(name)
            
        except Exception as e:
            raise PitrError(f"Failed to create account PITR '{name}': {e}")
    
    async def create_database_pitr(self, 
                                  name: str, 
                                  database_name: str,
                                  range_value: int = 1, 
                                  range_unit: str = 'd') -> Pitr:
        """Create database PITR within transaction asynchronously"""
        try:
            self._validate_range(range_value, range_unit)
            
            sql = (f"CREATE PITR {self.client._escape_identifier(name)} "
                   f"FOR DATABASE {self.client._escape_identifier(database_name)} "
                   f"RANGE {range_value} '{range_unit}'")
            
            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create database PITR '{name}'")
            
            return await self.get(name)
            
        except Exception as e:
            raise PitrError(f"Failed to create database PITR '{name}': {e}")
    
    async def create_table_pitr(self, 
                               name: str, 
                               database_name: str,
                               table_name: str,
                               range_value: int = 1, 
                               range_unit: str = 'd') -> Pitr:
        """Create table PITR within transaction asynchronously"""
        try:
            self._validate_range(range_value, range_unit)
            
            sql = (f"CREATE PITR {self.client._escape_identifier(name)} "
                   f"FOR TABLE {self.client._escape_identifier(database_name)} "
                   f"TABLE {self.client._escape_identifier(table_name)} "
                   f"RANGE {range_value} '{range_unit}'")
            
            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create table PITR '{name}'")
            
            return await self.get(name)
            
        except Exception as e:
            raise PitrError(f"Failed to create table PITR '{name}': {e}")
    
    async def get(self, name: str) -> Pitr:
        """Get PITR within transaction asynchronously"""
        try:
            sql = f"SHOW PITR WHERE pitr_name = {self.client._escape_string(name)}"
            result = await self.transaction_wrapper.execute(sql)
            
            if not result or not result.rows:
                raise PitrError(f"PITR '{name}' not found")
            
            row = result.rows[0]
            return self._row_to_pitr(row)
            
        except Exception as e:
            raise PitrError(f"Failed to get PITR '{name}': {e}")
    
    async def list(self, 
                   level: Optional[str] = None,
                   account_name: Optional[str] = None,
                   database_name: Optional[str] = None,
                   table_name: Optional[str] = None) -> List[Pitr]:
        """List PITRs within transaction asynchronously"""
        try:
            conditions = []
            
            if level:
                conditions.append(f"pitr_level = {self.client._escape_string(level)}")
            if account_name:
                conditions.append(f"account_name = {self.client._escape_string(account_name)}")
            if database_name:
                conditions.append(f"database_name = {self.client._escape_string(database_name)}")
            if table_name:
                conditions.append(f"table_name = {self.client._escape_string(table_name)}")
            
            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
            else:
                where_clause = ""
            
            sql = f"SHOW PITR{where_clause}"
            result = await self.transaction_wrapper.execute(sql)
            
            if not result or not result.rows:
                return []
            
            return [self._row_to_pitr(row) for row in result.rows]
            
        except Exception as e:
            raise PitrError(f"Failed to list PITRs: {e}")
    
    async def alter(self, 
                    name: str, 
                    range_value: int, 
                    range_unit: str) -> Pitr:
        """Alter PITR within transaction asynchronously"""
        try:
            self._validate_range(range_value, range_unit)
            
            sql = (f"ALTER PITR {self.client._escape_identifier(name)} "
                   f"RANGE {range_value} '{range_unit}'")
            
            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PitrError(f"Failed to alter PITR '{name}'")
            
            return await self.get(name)
            
        except Exception as e:
            raise PitrError(f"Failed to alter PITR '{name}': {e}")
    
    async def delete(self, name: str) -> bool:
        """Delete PITR within transaction asynchronously"""
        try:
            sql = f"DROP PITR {self.client._escape_identifier(name)}"
            result = await self.transaction_wrapper.execute(sql)
            return result is not None
            
        except Exception as e:
            raise PitrError(f"Failed to delete PITR '{name}': {e}")


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

    
    async def clone_table_with_snapshot(self, target_table: str, source_table: str,
                                      snapshot_name: str, if_not_exists: bool = False) -> None:
        """Clone table with snapshot within transaction asynchronously"""
        return await super().clone_table_with_snapshot(target_table, source_table, snapshot_name, if_not_exists)


class AsyncTransactionAccountManager:
    """Async transaction-scoped account manager"""
    
    def __init__(self, transaction):
        self.transaction = transaction
        self.client = transaction.client
    
    async def create_account(self, 
                           account_name: str,
                           admin_name: str,
                           password: str,
                           comment: Optional[str] = None,
                           admin_comment: Optional[str] = None,
                           admin_host: str = '%',
                           admin_identified_by: Optional[str] = None) -> Account:
        """Create account within async transaction"""
        try:
            sql_parts = [f"CREATE ACCOUNT {self.client._escape_identifier(account_name)}"]
            sql_parts.append(f"ADMIN_NAME {self.client._escape_string(admin_name)}")
            sql_parts.append(f"IDENTIFIED BY {self.client._escape_string(password)}")
            
            if admin_host != '%':
                sql_parts.append(f"ADMIN_HOST {self.client._escape_string(admin_host)}")
            
            if comment:
                sql_parts.append(f"COMMENT {self.client._escape_string(comment)}")
            
            if admin_comment:
                sql_parts.append(f"ADMIN_COMMENT {self.client._escape_string(admin_comment)}")
            
            if admin_identified_by:
                sql_parts.append(f"ADMIN_IDENTIFIED BY {self.client._escape_string(admin_identified_by)}")
            
            sql = " ".join(sql_parts)
            await self.transaction.execute(sql)
            
            return await self.get_account(account_name)
            
        except Exception as e:
            raise AccountError(f"Failed to create account '{account_name}': {e}")
    
    async def drop_account(self, account_name: str) -> None:
        """Drop account within async transaction"""
        try:
            sql = f"DROP ACCOUNT {self.client._escape_identifier(account_name)}"
            await self.transaction.execute(sql)
        except Exception as e:
            raise AccountError(f"Failed to drop account '{account_name}': {e}")
    
    async def alter_account(self, 
                          account_name: str,
                          comment: Optional[str] = None,
                          suspend: Optional[bool] = None,
                          suspend_reason: Optional[str] = None) -> Account:
        """Alter account within async transaction"""
        try:
            sql_parts = [f"ALTER ACCOUNT {self.client._escape_identifier(account_name)}"]
            
            if comment is not None:
                sql_parts.append(f"COMMENT {self.client._escape_string(comment)}")
            
            if suspend is not None:
                if suspend:
                    if suspend_reason:
                        sql_parts.append(f"SUSPEND COMMENT {self.client._escape_string(suspend_reason)}")
                    else:
                        sql_parts.append("SUSPEND")
                else:
                    sql_parts.append("OPEN")
            
            sql = " ".join(sql_parts)
            await self.transaction.execute(sql)
            
            return await self.get_account(account_name)
            
        except Exception as e:
            raise AccountError(f"Failed to alter account '{account_name}': {e}")
    
    async def get_account(self, account_name: str) -> Account:
        """Get account within async transaction"""
        try:
            sql = "SHOW ACCOUNTS"
            result = await self.transaction.execute(sql)
            
            if not result or not result.rows:
                raise AccountError(f"Account '{account_name}' not found")
            
            for row in result.rows:
                if row[0] == account_name:
                    return self._row_to_account(row)
            
            raise AccountError(f"Account '{account_name}' not found")
            
        except Exception as e:
            raise AccountError(f"Failed to get account '{account_name}': {e}")
    
    async def list_accounts(self) -> List[Account]:
        """List accounts within async transaction"""
        try:
            sql = "SHOW ACCOUNTS"
            result = await self.transaction.execute(sql)
            
            if not result or not result.rows:
                return []
            
            return [self._row_to_account(row) for row in result.rows]
            
        except Exception as e:
            raise AccountError(f"Failed to list accounts: {e}")
    
    async def create_user(self, 
                        user_name: str,
                        password: str,
                        account_name: str,
                        host: str = '%',
                        comment: Optional[str] = None,
                        identified_by: Optional[str] = None) -> User:
        """Create user within async transaction"""
        try:
            # Build CREATE USER statement according to MatrixOne syntax
            # MatrixOne syntax: CREATE USER user_name IDENTIFIED BY 'password'
            sql_parts = [f"CREATE USER {self.client._escape_identifier(user_name)}"]
            
            if host != '%':
                sql_parts.append(f"@{self.client._escape_string(host)}")
            
            sql_parts.append(f"IDENTIFIED BY {self.client._escape_string(password)}")
            
            # Note: MatrixOne doesn't support ACCOUNT, COMMENT, or ATTRIBUTE clauses in CREATE USER
            # sql_parts.append(f"ACCOUNT {self.client._escape_identifier(account_name)}")
            # if comment:
            #     sql_parts.append(f"COMMENT {self.client._escape_string(comment)}")
            # if identified_by:
            #     sql_parts.append(f"IDENTIFIED BY {self.client._escape_string(identified_by)}")
            
            sql = " ".join(sql_parts)
            await self.transaction.execute(sql)
            
            return await self.get_user(user_name, host, account_name)
            
        except Exception as e:
            raise AccountError(f"Failed to create user '{user_name}': {e}")
    
    async def drop_user(self, user_name: str, host: str = '%', account_name: Optional[str] = None) -> None:
        """Drop user within async transaction"""
        try:
            sql_parts = [f"DROP USER {self.client._escape_identifier(user_name)}"]
            
            if host != '%':
                sql_parts.append(f"@{self.client._escape_string(host)}")
            
            if account_name:
                sql_parts.append(f"ACCOUNT {self.client._escape_identifier(account_name)}")
            
            sql = " ".join(sql_parts)
            await self.transaction.execute(sql)
            
        except Exception as e:
            raise AccountError(f"Failed to drop user '{user_name}': {e}")
    
    async def alter_user(self, 
                       user_name: str,
                       host: str = '%',
                       account_name: Optional[str] = None,
                       password: Optional[str] = None,
                       comment: Optional[str] = None,
                       lock: Optional[bool] = None,
                       lock_reason: Optional[str] = None) -> User:
        """Alter user within async transaction"""
        try:
            sql_parts = [f"ALTER USER {self.client._escape_identifier(user_name)}"]
            
            if host != '%':
                sql_parts.append(f"@{self.client._escape_string(host)}")
            
            if account_name:
                sql_parts.append(f"ACCOUNT {self.client._escape_identifier(account_name)}")
            
            if password is not None:
                sql_parts.append(f"IDENTIFIED BY {self.client._escape_string(password)}")
            
            if comment is not None:
                sql_parts.append(f"COMMENT {self.client._escape_string(comment)}")
            
            if lock is not None:
                if lock:
                    if lock_reason:
                        sql_parts.append(f"ACCOUNT LOCK COMMENT {self.client._escape_string(lock_reason)}")
                    else:
                        sql_parts.append("ACCOUNT LOCK")
                else:
                    sql_parts.append("ACCOUNT UNLOCK")
            
            sql = " ".join(sql_parts)
            await self.transaction.execute(sql)
            
            return await self.get_user(user_name, host, account_name)
            
        except Exception as e:
            raise AccountError(f"Failed to alter user '{user_name}': {e}")
    
    async def get_user(self, user_name: str, host: str = '%', account_name: Optional[str] = None) -> User:
        """Get user within async transaction"""
        try:
            sql = "SHOW GRANTS"
            result = await self.transaction.execute(sql)
            
            if not result or not result.rows:
                raise AccountError(f"User '{user_name}' not found")
            
            for row in result.rows:
                if (row[0] == user_name and
                    row[1] == host and
                    (not account_name or row[2] == account_name)):
                    return self._row_to_user(row)
            
            raise AccountError(f"User '{user_name}' not found")
            
        except Exception as e:
            raise AccountError(f"Failed to get user '{user_name}': {e}")
    
    async def list_users(self, account_name: Optional[str] = None) -> List[User]:
        """List users within async transaction"""
        try:
            sql = "SHOW GRANTS"
            result = await self.transaction.execute(sql)
            
            if not result or not result.rows:
                return []
            
            users = [self._row_to_user(row) for row in result.rows]
            
            if account_name:
                users = [user for user in users if user.account == account_name]
            
            return users
            
        except Exception as e:
            raise AccountError(f"Failed to list users: {e}")
    
    def _row_to_account(self, row: tuple) -> Account:
        """Convert database row to Account object"""
        return Account(
            name=row[0],
            admin_name=row[1],
            created_time=row[2] if len(row) > 2 else None,
            status=row[3] if len(row) > 3 else None,
            comment=row[4] if len(row) > 4 else None,
            suspended_time=row[5] if len(row) > 5 else None,
            suspended_reason=row[6] if len(row) > 6 else None
        )
    
    def _row_to_user(self, row: tuple) -> User:
        """Convert database row to User object"""
        return User(
            name=row[0],
            host=row[1],
            account=row[2],
            created_time=row[3] if len(row) > 3 else None,
            status=row[4] if len(row) > 4 else None,
            comment=row[5] if len(row) > 5 else None,
            locked_time=row[6] if len(row) > 6 else None,
            locked_reason=row[7] if len(row) > 7 else None
        )
