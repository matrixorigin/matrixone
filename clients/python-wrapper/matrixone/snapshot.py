"""
MatrixOne Snapshot Management
"""

from typing import Optional, List, Dict, Any, Tuple, Union
from datetime import datetime
from contextlib import contextmanager
from enum import Enum
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .exceptions import SnapshotError, ConnectionError, QueryError, CloneError, VersionError
from .version import requires_version


class SnapshotLevel(Enum):
    """Snapshot level enumeration"""
    CLUSTER = "cluster"
    ACCOUNT = "account"
    DATABASE = "database"
    TABLE = "table"


class Snapshot:
    """Snapshot information"""
    
    def __init__(self, name: str, level: Union[str, SnapshotLevel], created_at: datetime, 
                 description: Optional[str] = None, database: Optional[str] = None, 
                 table: Optional[str] = None):
        self.name = name
        # Convert string to enum if needed
        if isinstance(level, str):
            try:
                self.level = SnapshotLevel(level.lower())
            except ValueError:
                raise SnapshotError(f"Invalid snapshot level: {level}")
        else:
            self.level = level
        self.created_at = created_at
        self.description = description
        self.database = database
        self.table = table
    
    def __repr__(self):
        return f"Snapshot(name='{self.name}', level='{self.level}', created_at='{self.created_at}')"


class SnapshotManager:
    """Snapshot management for MatrixOne"""
    
    def __init__(self, client):
        self.client = client
    
    @requires_version(
        min_version="1.0.0",
        feature_name="snapshot_creation",
        description="Snapshot creation functionality",
        alternative="Use backup/restore operations instead"
    )
    def create(self, name: str, level: Union[str, SnapshotLevel], database: Optional[str] = None, 
               table: Optional[str] = None, description: Optional[str] = None,
               executor=None) -> Snapshot:
        """
        Create a snapshot
        
        Args:
            name: Snapshot name
            level: Snapshot level (SnapshotLevel enum or string)
            database: Database name (for database/table level)
            table: Table name (for table level)
            description: Snapshot description
            executor: Optional executor (e.g., transaction wrapper)
            
        Returns:
            Snapshot object
        """
        if not self.client._connection:
            raise ConnectionError("Not connected to database")
        
        # Convert string to enum if needed
        if isinstance(level, str):
            try:
                level_enum = SnapshotLevel(level.lower())
            except ValueError:
                raise SnapshotError(f"Invalid snapshot level: {level}")
        else:
            level_enum = level
        
        # Build CREATE SNAPSHOT SQL using correct MatrixOne syntax
        if level_enum == SnapshotLevel.CLUSTER:
            sql = f"CREATE SNAPSHOT {name} FOR CLUSTER"
        elif level_enum == SnapshotLevel.ACCOUNT:
            sql = f"CREATE SNAPSHOT {name} FOR ACCOUNT"
        elif level_enum == SnapshotLevel.DATABASE:
            if not database:
                raise SnapshotError("Database name required for database level snapshot")
            sql = f"CREATE SNAPSHOT {name} FOR DATABASE {database}"
        elif level_enum == SnapshotLevel.TABLE:
            if not database or not table:
                raise SnapshotError("Database and table names required for table level snapshot")
            sql = f"CREATE SNAPSHOT {name} FOR TABLE {database} {table}"
        
        # Note: MatrixOne doesn't support COMMENT in CREATE SNAPSHOT
        # if description:
        #     sql += f" COMMENT '{description}'"
        
        try:
            # Use provided executor or default client execute
            execute_func = executor.execute if executor else self.client.execute
            execute_func(sql)
            
            # Get snapshot info
            snapshot_info = self.get(name, executor=executor)
            return snapshot_info
            
        except Exception as e:
            raise SnapshotError(f"Failed to create snapshot: {e}")
    
    def list(self) -> List[Snapshot]:
        """
        List all snapshots
        
        Returns:
            List of Snapshot objects
        """
        if not self.client._connection:
            raise ConnectionError("Not connected to database")
        
        try:
            # Query snapshot information using mo_catalog.mo_snapshots
            result = self.client.execute("""
                SELECT sname, ts, level, account_name, database_name, table_name
                FROM mo_catalog.mo_snapshots
                ORDER BY ts DESC
            """)
            
            snapshots = []
            for row in result.fetchall():
                # Convert timestamp to datetime
                from datetime import datetime
                timestamp = datetime.fromtimestamp(row[1] / 1000000000)  # Convert nanoseconds to seconds
                
                # Convert level string to enum
                level_str = row[2]
                try:
                    level_enum = SnapshotLevel(level_str.lower())
                except ValueError:
                    # If enum conversion fails, keep as string for backward compatibility
                    level_enum = level_str
                
                snapshot = Snapshot(
                    name=row[0],        # sname
                    level=level_enum,   # level (now as enum)
                    created_at=timestamp,  # ts
                    description=None,   # Not available
                    database=row[4],    # database_name
                    table=row[5]        # table_name
                )
                snapshots.append(snapshot)
            
            return snapshots
            
        except Exception as e:
            raise SnapshotError(f"Failed to list snapshots: {e}")
    
    def get(self, name: str, executor=None) -> Snapshot:
        """
        Get snapshot by name
        
        Args:
            name: Snapshot name
            executor: Optional executor (e.g., transaction wrapper)
            
        Returns:
            Snapshot object
        """
        if not self.client._connection:
            raise ConnectionError("Not connected to database")
        
        try:
            # Use provided executor or default client execute
            execute_func = executor.execute if executor else self.client.execute
            result = execute_func("""
                SELECT sname, ts, level, account_name, database_name, table_name
                FROM mo_catalog.mo_snapshots
                WHERE sname = %s
            """, (name,))
            
            row = result.fetchone()
            if not row:
                raise SnapshotError(f"Snapshot '{name}' not found")
            
            # Convert timestamp to datetime
            from datetime import datetime
            timestamp = datetime.fromtimestamp(row[1] / 1000000000)  # Convert nanoseconds to seconds
            
            # Convert level string to enum
            level_str = row[2]
            try:
                level_enum = SnapshotLevel(level_str.lower())
            except ValueError:
                # If enum conversion fails, keep as string for backward compatibility
                level_enum = level_str
            
            return Snapshot(
                name=row[0],        # sname
                level=level_enum,   # level (now as enum)
                created_at=timestamp,  # ts
                description=None,   # Not available
                database=row[4],    # database_name
                table=row[5]        # table_name
            )
            
        except Exception as e:
            if "not found" in str(e):
                raise e
            raise SnapshotError(f"Failed to get snapshot: {e}")
    
    def delete(self, name: str, executor=None) -> None:
        """
        Delete snapshot
        
        Args:
            name: Snapshot name
            executor: Optional executor (e.g., transaction wrapper)
        """
        if not self.client._connection:
            raise ConnectionError("Not connected to database")
        
        try:
            # Use provided executor or default client execute
            execute_func = executor.execute if executor else self.client.execute
            execute_func(f"DROP SNAPSHOT {name}")
        except Exception as e:
            raise SnapshotError(f"Failed to delete snapshot: {e}")
    
    def exists(self, name: str) -> bool:
        """
        Check if snapshot exists
        
        Args:
            name: Snapshot name
            
        Returns:
            True if snapshot exists, False otherwise
        """
        try:
            self.get(name)
            return True
        except SnapshotError:
            return False
    


class SnapshotQueryBuilder:
    """Query builder for snapshot queries"""
    
    def __init__(self, snapshot_name: str, client):
        self.snapshot_name = snapshot_name
        self.client = client
        self._select_columns = []
        self._from_table = None
        self._joins = []
        self._where_conditions = []
        self._where_params = []
        self._group_by_columns = []
        self._having_conditions = []
        self._having_params = []
        self._order_by_columns = []
        self._limit_count = None
        self._offset_count = None
    
    def select(self, *columns) -> 'SnapshotQueryBuilder':
        """Add SELECT columns"""
        self._select_columns.extend(columns)
        return self
    
    def from_table(self, table: str) -> 'SnapshotQueryBuilder':
        """Set FROM table"""
        self._from_table = table
        return self
    
    def join(self, table: str, condition: str) -> 'SnapshotQueryBuilder':
        """Add JOIN"""
        self._joins.append(f"JOIN {table} ON {condition}")
        return self
    
    def left_join(self, table: str, condition: str) -> 'SnapshotQueryBuilder':
        """Add LEFT JOIN"""
        self._joins.append(f"LEFT JOIN {table} ON {condition}")
        return self
    
    def right_join(self, table: str, condition: str) -> 'SnapshotQueryBuilder':
        """Add RIGHT JOIN"""
        self._joins.append(f"RIGHT JOIN {table} ON {condition}")
        return self
    
    def where(self, condition: str, *params) -> 'SnapshotQueryBuilder':
        """Add WHERE condition"""
        self._where_conditions.append(condition)
        self._where_params.extend(params)
        return self
    
    def group_by(self, *columns) -> 'SnapshotQueryBuilder':
        """Add GROUP BY columns"""
        self._group_by_columns.extend(columns)
        return self
    
    def having(self, condition: str, *params) -> 'SnapshotQueryBuilder':
        """Add HAVING condition"""
        self._having_conditions.append(condition)
        self._having_params.extend(params)
        return self
    
    def order_by(self, *columns) -> 'SnapshotQueryBuilder':
        """Add ORDER BY columns"""
        self._order_by_columns.extend(columns)
        return self
    
    def limit(self, count: int) -> 'SnapshotQueryBuilder':
        """Set LIMIT"""
        self._limit_count = count
        return self
    
    def offset(self, count: int) -> 'SnapshotQueryBuilder':
        """Set OFFSET"""
        self._offset_count = count
        return self
    
    def execute(self):
        """Execute the query"""
        if not self._select_columns:
            raise SnapshotError("No SELECT columns specified")
        if not self._from_table:
            raise SnapshotError("No FROM table specified")
        
        # Build SQL
        sql = f"SELECT {', '.join(self._select_columns)} FROM {self._from_table}"
        
        # Add joins
        for join in self._joins:
            sql += f" {join}"
        
        # Add WHERE
        if self._where_conditions:
            sql += f" WHERE {' AND '.join(self._where_conditions)}"
        
        # Add GROUP BY
        if self._group_by_columns:
            sql += f" GROUP BY {', '.join(self._group_by_columns)}"
        
        # Add HAVING
        if self._having_conditions:
            sql += f" HAVING {' AND '.join(self._having_conditions)}"
        
        # Add ORDER BY
        if self._order_by_columns:
            sql += f" ORDER BY {', '.join(self._order_by_columns)}"
        
        # Add LIMIT
        if self._limit_count:
            sql += f" LIMIT {self._limit_count}"
        
        # Add OFFSET
        if self._offset_count:
            sql += f" OFFSET {self._offset_count}"
        
        # Add snapshot hint
        sql += f" FOR SNAPSHOT '{self.snapshot_name}'"
        
        # Execute with parameters
        params = tuple(self._where_params + self._having_params)
        return self.client.execute(sql, params if params else None)


class CloneManager:
    """Clone management for MatrixOne"""
    
    def __init__(self, client):
        self.client = client
    
    @requires_version(
        min_version="1.0.0",
        feature_name="database_cloning",
        description="Database cloning functionality",
        alternative="Use CREATE DATABASE and data migration instead"
    )
    def clone_database(self, target_db: str, source_db: str, 
                      snapshot_name: Optional[str] = None, 
                      if_not_exists: bool = False, executor=None) -> None:
        """
        Clone a database
        
        Args:
            target_db: Target database name
            source_db: Source database name
            snapshot_name: Optional snapshot name for point-in-time clone
            if_not_exists: Use IF NOT EXISTS clause
            executor: Optional executor (e.g., transaction wrapper)
            
        Raises:
            ConnectionError: If not connected to database
            CloneError: If clone operation fails
        """
        if not self.client._connection:
            raise ConnectionError("Not connected to database")
        
        # Build CLONE DATABASE SQL
        if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        
        if snapshot_name:
            sql = f"CREATE DATABASE {if_not_exists_clause}{target_db} CLONE {source_db} {{SNAPSHOT = '{snapshot_name}'}}"
        else:
            sql = f"CREATE DATABASE {if_not_exists_clause}{target_db} CLONE {source_db}"
        
        try:
            # Use provided executor or default client execute
            execute_func = executor.execute if executor else self.client.execute
            execute_func(sql)
        except Exception as e:
            raise CloneError(f"Failed to clone database: {e}")
    
    def clone_table(self, target_table: str, source_table: str,
                   snapshot_name: Optional[str] = None,
                   if_not_exists: bool = False, executor=None) -> None:
        """
        Clone a table
        
        Args:
            target_table: Target table name (can include database: db.table)
            source_table: Source table name (can include database: db.table)
            snapshot_name: Optional snapshot name for point-in-time clone
            if_not_exists: Use IF NOT EXISTS clause
            executor: Optional executor (e.g., transaction wrapper)
            
        Raises:
            ConnectionError: If not connected to database
            CloneError: If clone operation fails
        """
        if not self.client._connection:
            raise ConnectionError("Not connected to database")
        
        # Build CLONE TABLE SQL
        if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        
        if snapshot_name:
            sql = f"CREATE TABLE {if_not_exists_clause}{target_table} CLONE {source_table} {{SNAPSHOT = '{snapshot_name}'}}"
        else:
            sql = f"CREATE TABLE {if_not_exists_clause}{target_table} CLONE {source_table}"
        
        try:
            # Use provided executor or default client execute
            execute_func = executor.execute if executor else self.client.execute
            execute_func(sql)
        except Exception as e:
            raise CloneError(f"Failed to clone table: {e}")
    
    def clone_database_with_snapshot(self, target_db: str, source_db: str, 
                                   snapshot_name: str, if_not_exists: bool = False, executor=None) -> None:
        """
        Clone a database using a specific snapshot
        
        Args:
            target_db: Target database name
            source_db: Source database name
            snapshot_name: Snapshot name for point-in-time clone
            if_not_exists: Use IF NOT EXISTS clause
            executor: Optional executor (e.g., transaction wrapper)
            
        Raises:
            ConnectionError: If not connected to database
            CloneError: If clone operation fails or snapshot doesn't exist
        """
        # Verify snapshot exists using snapshot manager
        if not self.client.snapshots.exists(snapshot_name):
            raise CloneError(f"Snapshot '{snapshot_name}' does not exist")
        
        self.clone_database(target_db, source_db, snapshot_name, if_not_exists, executor)
    
    def clone_table_with_snapshot(self, target_table: str, source_table: str,
                                snapshot_name: str, if_not_exists: bool = False, executor=None) -> None:
        """
        Clone a table using a specific snapshot
        
        Args:
            target_table: Target table name (can include database: db.table)
            source_table: Source table name (can include database: db.table)
            snapshot_name: Snapshot name for point-in-time clone
            if_not_exists: Use IF NOT EXISTS clause
            executor: Optional executor (e.g., transaction wrapper)
            
        Raises:
            ConnectionError: If not connected to database
            CloneError: If clone operation fails or snapshot doesn't exist
        """
        # Verify snapshot exists using snapshot manager
        if not self.client.snapshots.exists(snapshot_name):
            raise CloneError(f"Snapshot '{snapshot_name}' does not exist")
        
        self.clone_table(target_table, source_table, snapshot_name, if_not_exists, executor)
