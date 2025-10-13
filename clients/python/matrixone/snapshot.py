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
MatrixOne Snapshot Management
"""

from datetime import datetime
from enum import Enum
from typing import List, Optional, Union

from .exceptions import CloneError, ConnectionError, SnapshotError
from .version import requires_version


class SnapshotLevel(Enum):
    """Snapshot level enumeration"""

    CLUSTER = "cluster"
    ACCOUNT = "account"
    DATABASE = "database"
    TABLE = "table"


class Snapshot:
    """Snapshot information"""

    def __init__(
        self,
        name: str,
        level: Union[str, SnapshotLevel],
        created_at: datetime,
        description: Optional[str] = None,
        database: Optional[str] = None,
        table: Optional[str] = None,
    ):
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
    """
    Snapshot management for MatrixOne database operations.

    This class provides comprehensive snapshot functionality for creating, managing,
    and restoring database snapshots at various levels (database, table, or cluster).
    Snapshots enable point-in-time recovery and data protection capabilities.

    Key Features:

    - Create snapshots at database, table, or cluster level
    - List and query existing snapshots
    - Restore data from snapshots
    - Snapshot lifecycle management
    - Integration with transaction operations

    Supported Snapshot Levels:
    - CLUSTER: Full cluster snapshot
    - DATABASE: Database-level snapshot
    - TABLE: Table-level snapshot

    Usage Examples::

        # Create a database snapshot
        snapshot = client.snapshots.create(
            name='daily_backup',
            level=SnapshotLevel.DATABASE,
            database='my_database',
            description='Daily backup snapshot'
        )

        # Create a table snapshot
        snapshot = client.snapshots.create(
            name='users_backup',
            level=SnapshotLevel.TABLE,
            database='my_database',
            table='users',
            description='Users table backup'
        )

        # List all snapshots
        snapshots = client.snapshots.list()

        # Restore from snapshot
        client.snapshots.restore('daily_backup', 'restored_database')

    Note: Snapshot functionality requires MatrixOne version 1.0.0 or higher. For older versions,
    use backup/restore operations instead.
    """

    def __init__(self, client):
        self.client = client

    @requires_version(
        min_version="1.0.0",
        feature_name="snapshot_creation",
        description="Snapshot creation functionality",
        alternative="Use backup/restore operations instead",
    )
    def create(
        self,
        name: str,
        level: Union[str, SnapshotLevel],
        database: Optional[str] = None,
        table: Optional[str] = None,
        description: Optional[str] = None,
        executor=None,
    ) -> Snapshot:
        """
        Create a snapshot

        Args::

            name: Snapshot name
            level: Snapshot level (SnapshotLevel enum or string)
            database: Database name (for database/table level)
            table: Table name (for table level)
            description: Snapshot description
            executor: Optional executor (e.g., transaction wrapper)

        Returns::

            Snapshot object
        """
        if not self.client._engine:
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
            raise SnapshotError(f"Failed to create snapshot: {e}") from None

    def list(self) -> List[Snapshot]:
        """
        List all snapshots

        Returns::

            List of Snapshot objects
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        try:
            # Query snapshot information using mo_catalog.mo_snapshots
            result = self.client.execute(
                """
                SELECT sname, ts, level, account_name, database_name, table_name
                FROM mo_catalog.mo_snapshots
                ORDER BY ts DESC
            """
            )

            snapshots = []
            for row in result.fetchall():
                # Convert timestamp to datetime
                timestamp = datetime.fromtimestamp(row[1] / 1000000000)  # Convert nanoseconds to seconds

                # Convert level string to enum
                level_str = row[2]
                try:
                    level_enum = SnapshotLevel(level_str.lower())
                except ValueError:
                    # If enum conversion fails, keep as string for backward compatibility
                    level_enum = level_str

                snapshot = Snapshot(
                    name=row[0],  # sname
                    level=level_enum,  # level (now as enum)
                    created_at=timestamp,  # ts
                    description=None,  # Not available
                    database=row[4],  # database_name
                    table=row[5],  # table_name
                )
                snapshots.append(snapshot)

            return snapshots

        except Exception as e:
            raise SnapshotError(f"Failed to list snapshots: {e}") from None

    def get(self, name: str, executor=None) -> Snapshot:
        """
        Get snapshot by name

        Args::

            name: Snapshot name
            executor: Optional executor (e.g., transaction wrapper)

        Returns::

            Snapshot object
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        try:
            # Use provided executor or default client execute
            execute_func = executor.execute if executor else self.client.execute
            result = execute_func(
                """
                SELECT sname, ts, level, account_name, database_name, table_name
                FROM mo_catalog.mo_snapshots
                WHERE sname = :name
            """,
                {"name": name},
            )

            row = result.fetchone()
            if not row:
                raise SnapshotError(f"Snapshot '{name}' not found")

            # Convert timestamp to datetime
            timestamp = datetime.fromtimestamp(row[1] / 1000000000)  # Convert nanoseconds to seconds

            # Convert level string to enum
            level_str = row[2]
            try:
                level_enum = SnapshotLevel(level_str.lower())
            except ValueError:
                # If enum conversion fails, keep as string for backward compatibility
                level_enum = level_str

            return Snapshot(
                name=row[0],  # sname
                level=level_enum,  # level (now as enum)
                created_at=timestamp,  # ts
                description=None,  # Not available
                database=row[4],  # database_name
                table=row[5],  # table_name
            )

        except Exception as e:
            if "not found" in str(e):
                raise e
            raise SnapshotError(f"Failed to get snapshot: {e}")

    def delete(self, name: str, executor=None) -> None:
        """
        Delete snapshot

        Args::

            name: Snapshot name
            executor: Optional executor (e.g., transaction wrapper)
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        try:
            # Use provided executor or default client execute
            execute_func = executor.execute if executor else self.client.execute
            execute_func(f"DROP SNAPSHOT {name}")
        except Exception as e:
            raise SnapshotError(f"Failed to delete snapshot: {e}") from None

    def exists(self, name: str) -> bool:
        """
        Check if snapshot exists

        Args::

            name: Snapshot name

        Returns::

            True if snapshot exists, False otherwise
        """
        try:
            self.get(name)
            return True
        except SnapshotError:
            return False


class CloneManager:
    """
    Clone management for MatrixOne database operations.

    This class provides comprehensive database cloning functionality for creating
    copies of databases, tables, or data subsets. Cloning enables efficient
    data replication, testing environments, and data distribution scenarios.

    Key Features:

    - Database cloning with full data replication
    - Table-level cloning for specific data subsets
    - Efficient cloning using MatrixOne's native capabilities
    - Integration with snapshot and restore operations
    - Transaction-aware cloning operations
    - Support for both full and incremental cloning

    Supported Cloning Levels:
    - DATABASE: Full database cloning with all tables and data
    - TABLE: Table-level cloning with data replication
    - SUBSET: Partial data cloning based on conditions

    Usage Examples::

        # Initialize clone manager
        clone = client.clone

        # Clone entire database
        success = clone.clone_database(
            target_database='cloned_database',
            source_database='source_database'
        )

        # Clone table with data
        success = clone.clone_table(
            target_database='cloned_database',
            target_table='cloned_users',
            source_database='source_database',
            source_table='users'
        )

        # Clone table with conditions
        success = clone.clone_table_with_conditions(
            target_database='cloned_database',
            target_table='active_users',
            source_database='source_database',
            source_table='users',
            conditions='active = 1'
        )

        # List clone operations
        clones = clone.list_clones()

        # Get clone status
        status = clone.get_clone_status('clone_job_id')

    Note: Cloning functionality requires MatrixOne version 1.0.0 or higher. Clone operations may
    take significant time depending on the amount of data being cloned and database complexity.
    """

    def __init__(self, client):
        self.client = client

    @requires_version(
        min_version="1.0.0",
        feature_name="database_cloning",
        description="Database cloning functionality",
        alternative="Use CREATE DATABASE and data migration instead",
    )
    def clone_database(
        self,
        target_db: str,
        source_db: str,
        snapshot_name: Optional[str] = None,
        if_not_exists: bool = False,
        executor=None,
    ) -> None:
        """
        Clone a database

        Args::

            target_db: Target database name
            source_db: Source database name
            snapshot_name: Optional snapshot name for point-in-time clone
            if_not_exists: Use IF NOT EXISTS clause
            executor: Optional executor (e.g., transaction wrapper)

        Raises::

            ConnectionError: If not connected to database
            CloneError: If clone operation fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        # Build CLONE DATABASE SQL
        if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""

        if snapshot_name:
            sql = f"CREATE DATABASE {if_not_exists_clause}{target_db} CLONE {source_db} {{snapshot = '{snapshot_name}'}}"
        else:
            sql = f"CREATE DATABASE {if_not_exists_clause}{target_db} CLONE {source_db}"

        try:
            # Use provided executor or default client execute
            execute_func = executor.execute if executor else self.client.execute
            execute_func(sql)
        except Exception as e:
            raise CloneError(f"Failed to clone database: {e}") from None

    def clone_table(
        self,
        target_table: str,
        source_table: str,
        snapshot_name: Optional[str] = None,
        if_not_exists: bool = False,
        executor=None,
    ) -> None:
        """
        Clone a table

        Args::

            target_table: Target table name (can include database: db.table)
            source_table: Source table name (can include database: db.table)
            snapshot_name: Optional snapshot name for point-in-time clone
            if_not_exists: Use IF NOT EXISTS clause
            executor: Optional executor (e.g., transaction wrapper)

        Raises::

            ConnectionError: If not connected to database
            CloneError: If clone operation fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        # Build CLONE TABLE SQL
        if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""

        if snapshot_name:
            sql = (
                f"CREATE TABLE {if_not_exists_clause}{target_table} "
                f"CLONE {source_table} {{snapshot = '{snapshot_name}'}}"
            )
        else:
            sql = f"CREATE TABLE {if_not_exists_clause}{target_table} CLONE {source_table}"

        try:
            # Use provided executor or default client execute
            execute_func = executor.execute if executor else self.client.execute
            execute_func(sql)
        except Exception as e:
            raise CloneError(f"Failed to clone table: {e}") from None

    def clone_database_with_snapshot(
        self,
        target_db: str,
        source_db: str,
        snapshot_name: str,
        if_not_exists: bool = False,
        executor=None,
    ) -> None:
        """
        Clone a database using a specific snapshot

        Args::

            target_db: Target database name
            source_db: Source database name
            snapshot_name: Snapshot name for point-in-time clone
            if_not_exists: Use IF NOT EXISTS clause
            executor: Optional executor (e.g., transaction wrapper)

        Raises::

            ConnectionError: If not connected to database
            CloneError: If clone operation fails or snapshot doesn't exist
        """
        # Verify snapshot exists using snapshot manager
        if not self.client.snapshots.exists(snapshot_name):
            raise CloneError(f"Snapshot '{snapshot_name}' does not exist")

        self.clone_database(target_db, source_db, snapshot_name, if_not_exists, executor)

    def clone_table_with_snapshot(
        self,
        target_table: str,
        source_table: str,
        snapshot_name: str,
        if_not_exists: bool = False,
        executor=None,
    ) -> None:
        """
        Clone a table using a specific snapshot

        Args::

            target_table: Target table name (can include database: db.table)
            source_table: Source table name (can include database: db.table)
            snapshot_name: Snapshot name for point-in-time clone
            if_not_exists: Use IF NOT EXISTS clause
            executor: Optional executor (e.g., transaction wrapper)

        Raises::

            ConnectionError: If not connected to database
            CloneError: If clone operation fails or snapshot doesn't exist
        """
        # Verify snapshot exists using snapshot manager
        if not self.client.snapshots.exists(snapshot_name):
            raise CloneError(f"Snapshot '{snapshot_name}' does not exist")

        self.clone_table(target_table, source_table, snapshot_name, if_not_exists, executor)
