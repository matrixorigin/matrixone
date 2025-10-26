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

Unified snapshot management supporting sync/async and client/session executors.
"""

from datetime import datetime
from enum import Enum
from typing import List, Optional, Union

from .exceptions import ConnectionError, SnapshotError
from .version import requires_version


class SnapshotLevel(Enum):
    """
    Snapshot level enumeration for specifying snapshot granularity.

    MatrixOne supports four levels of snapshots:
    - CLUSTER: Entire cluster snapshot (all databases and tables)
    - ACCOUNT: Account-level snapshot (all databases in an account)
    - DATABASE: Database-level snapshot (specific database)
    - TABLE: Table-level snapshot (specific table in a database)
    """

    CLUSTER = "cluster"
    ACCOUNT = "account"
    DATABASE = "database"
    TABLE = "table"


class Snapshot:
    """
    Snapshot information object representing a MatrixOne database snapshot.

    This class encapsulates metadata about a database snapshot including
    its name, level, creation time, and associated database/table information.

    Attributes::

        name (str): Snapshot name (unique identifier)
        level (SnapshotLevel): Snapshot level (CLUSTER, ACCOUNT, DATABASE, or TABLE)
        created_at (datetime): Snapshot creation timestamp
        description (Optional[str]): Optional description/comment
        database (Optional[str]): Database name (for DATABASE/TABLE level snapshots)
        table (Optional[str]): Table name (for TABLE level snapshots)

    Examples::

        >>> snapshot = Snapshot(
        ...     name='daily_backup',
        ...     level=SnapshotLevel.DATABASE,
        ...     created_at=datetime.now(),
        ...     database='my_database'
        ... )
        >>> print(snapshot.name)
        'daily_backup'
        >>> print(snapshot.level)
        SnapshotLevel.DATABASE
    """

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


class BaseSnapshotManager:
    """
    Base snapshot manager with shared logic for sync and async implementations.

    This base class contains all the SQL building logic that is shared between
    sync and async implementations. Subclasses only need to implement the
    execution methods.
    """

    def __init__(self, client, executor=None):
        """
        Initialize base snapshot manager.

        Args:
            client: MatrixOne client instance
            executor: Optional executor (e.g., session) for executing SQL.
                     If None, uses client as executor
        """
        self.client = client
        self.executor = executor

    def _get_executor(self):
        """Get the executor for SQL execution (session or client)"""
        return self.executor if self.executor else self.client

    def _build_create_sql(
        self,
        name: str,
        level: Union[str, SnapshotLevel],
        database: Optional[str] = None,
        table: Optional[str] = None,
    ) -> str:
        """Build CREATE SNAPSHOT SQL statement"""
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
            return f"CREATE SNAPSHOT {name} FOR CLUSTER"
        elif level_enum == SnapshotLevel.ACCOUNT:
            return f"CREATE SNAPSHOT {name} FOR ACCOUNT"
        elif level_enum == SnapshotLevel.DATABASE:
            if not database:
                raise SnapshotError("Database name required for database level snapshot")
            return f"CREATE SNAPSHOT {name} FOR DATABASE {database}"
        elif level_enum == SnapshotLevel.TABLE:
            if not database or not table:
                raise SnapshotError("Database and table names required for table level snapshot")
            return f"CREATE SNAPSHOT {name} FOR TABLE {database} {table}"
        else:
            raise SnapshotError(f"Unsupported snapshot level: {level_enum}")

    def _parse_snapshot_row(self, row) -> Snapshot:
        """Parse a snapshot row from database query result"""
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


class SnapshotManager(BaseSnapshotManager):
    """
    Synchronous snapshot management for MatrixOne database operations.

    This class provides comprehensive snapshot functionality for creating, managing,
    and restoring database snapshots at various levels (database, table, or cluster).
    Snapshots enable point-in-time recovery and data protection capabilities.

    Key Features:

    - **Multi-level snapshots**: Create snapshots at CLUSTER, ACCOUNT, DATABASE, or TABLE level
    - **Snapshot lifecycle management**: Create, list, get, delete, and check existence of snapshots
    - **Transaction-aware operations**: Use via executor pattern for both client and session contexts
    - **Unified interface**: Same API works in both client and session contexts
    - **Point-in-time recovery**: Restore databases/tables to specific snapshot states

    Executor Pattern:

    - If executor is None, uses self.client.execute (default client-level executor)
    - If executor is provided (e.g., session), uses executor.execute (transaction-aware)
    - This allows the same logic to work in both client and session contexts
    - All operations can participate in transactions when used via session

    Usage Examples::

        from matrixone import Client, SnapshotLevel

        client = Client(host='localhost', port=6001, user='root', password='111', database='test')

        # Create database-level snapshot
        snapshot = client.snapshots.create(
            name='daily_backup',
            level=SnapshotLevel.DATABASE,
            database='my_database'
        )
        print(f"Snapshot created: {snapshot.name} at {snapshot.created_at}")

        # Create table-level snapshot
        table_snapshot = client.snapshots.create(
            name='table_backup',
            level=SnapshotLevel.TABLE,
            database='my_database',
            table='users'
        )

        # List all snapshots
        all_snapshots = client.snapshots.list()
        for snap in all_snapshots:
            print(f"{snap.name}: {snap.level} - {snap.created_at}")

        # Get specific snapshot
        snapshot = client.snapshots.get('daily_backup')
        print(f"Snapshot level: {snapshot.level}")

        # Check if snapshot exists
        if client.snapshots.exists('daily_backup'):
            print("Snapshot exists!")

        # Delete snapshot
        client.snapshots.delete('daily_backup')

        # Using within a transaction (all operations are atomic)
        with client.session() as session:
            # Create multiple snapshots in a transaction
            session.snapshots.create(
                name='backup_1',
                level=SnapshotLevel.DATABASE,
                database='db1'
            )
            session.snapshots.create(
                name='backup_2',
                level=SnapshotLevel.DATABASE,
                database='db2'
            )
            # Both snapshots created atomically when transaction commits

    Version Requirements:

        Snapshot functionality requires MatrixOne version 1.0.0 or higher.
        Earlier versions do not support snapshot operations.

    See Also:

        - RestoreManager: For restoring databases from snapshots
        - CloneManager: For cloning databases using snapshots
        - PitrManager: For point-in-time recovery operations
    """

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
    ) -> Snapshot:
        """
        Create a snapshot.

        Args:
            name: Snapshot name
            level: Snapshot level (SnapshotLevel enum or string)
            database: Database name (for database/table level)
            table: Table name (for table level)
            description: Snapshot description (not currently used by MatrixOne)

        Returns:
            Snapshot object
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_create_sql(name, level, database, table)

        try:
            self._get_executor().execute(sql)
            # Get snapshot info
            return self.get(name)
        except Exception as e:
            raise SnapshotError(f"Failed to create snapshot: {e}") from None

    def list(self) -> List[Snapshot]:
        """
        List all snapshots.

        Returns:
            List of Snapshot objects
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        try:
            result = self._get_executor().execute(
                "SELECT sname, ts, level, account_name, database_name, table_name FROM mo_catalog.mo_snapshots"
            )

            snapshots = []
            rows = result.fetchall()
            for row in rows:
                snapshots.append(self._parse_snapshot_row(row))

            return snapshots

        except Exception as e:
            raise SnapshotError(f"Failed to list snapshots: {e}") from None

    def get(self, name: str) -> Snapshot:
        """
        Get snapshot by name.

        Args:
            name: Snapshot name

        Returns:
            Snapshot object
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        try:
            result = self._get_executor().execute(
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

            return self._parse_snapshot_row(row)

        except Exception as e:
            if "not found" in str(e):
                raise e
            raise SnapshotError(f"Failed to get snapshot: {e}")

    def delete(self, name: str) -> None:
        """
        Delete snapshot.

        Args:
            name: Snapshot name
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        try:
            self._get_executor().execute(f"DROP SNAPSHOT {name}")
        except Exception as e:
            raise SnapshotError(f"Failed to delete snapshot: {e}") from None

    def exists(self, name: str) -> bool:
        """
        Check if snapshot exists.

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


class AsyncSnapshotManager(BaseSnapshotManager):
    """
    Asynchronous snapshot management for MatrixOne database operations.

    Provides the same functionality as SnapshotManager but with async/await support.
    Uses the same executor pattern to support both client and session contexts.
    Shares SQL building logic with the synchronous version via BaseSnapshotManager.
    """

    @requires_version(
        min_version="1.0.0",
        feature_name="snapshot_creation",
        description="Snapshot creation functionality",
        alternative="Use backup/restore operations instead",
    )
    async def create(
        self,
        name: str,
        level: Union[str, SnapshotLevel],
        database: Optional[str] = None,
        table: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Snapshot:
        """
        Create a snapshot asynchronously.

        Args:
            name: Snapshot name
            level: Snapshot level (SnapshotLevel enum or string)
            database: Database name (for database/table level)
            table: Table name (for table level)
            description: Snapshot description (not currently used by MatrixOne)

        Returns:
            Snapshot object
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_create_sql(name, level, database, table)

        try:
            await self._get_executor().execute(sql)
            # Get snapshot info
            return await self.get(name)
        except Exception as e:
            raise SnapshotError(f"Failed to create snapshot: {e}") from None

    async def list(self) -> List[Snapshot]:
        """
        List all snapshots asynchronously.

        Returns:
            List of Snapshot objects
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        try:
            result = await self._get_executor().execute(
                "SELECT sname, ts, level, account_name, database_name, table_name FROM mo_catalog.mo_snapshots"
            )

            snapshots = []
            rows = result.fetchall()
            for row in rows:
                snapshots.append(self._parse_snapshot_row(row))

            return snapshots

        except Exception as e:
            raise SnapshotError(f"Failed to list snapshots: {e}") from None

    async def get(self, name: str) -> Snapshot:
        """
        Get snapshot by name asynchronously.

        Args:
            name: Snapshot name

        Returns:
            Snapshot object
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        try:
            result = await self._get_executor().execute(
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

            return self._parse_snapshot_row(row)

        except Exception as e:
            if "not found" in str(e):
                raise e
            raise SnapshotError(f"Failed to get snapshot: {e}")

    async def delete(self, name: str) -> None:
        """
        Delete snapshot asynchronously.

        Args:
            name: Snapshot name
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        try:
            await self._get_executor().execute(f"DROP SNAPSHOT {name}")
        except Exception as e:
            raise SnapshotError(f"Failed to delete snapshot: {e}") from None

    async def exists(self, name: str) -> bool:
        """
        Check if snapshot exists asynchronously.

        Args:
            name: Snapshot name

        Returns:
            True if snapshot exists, False otherwise
        """
        try:
            await self.get(name)
            return True
        except SnapshotError:
            return False
