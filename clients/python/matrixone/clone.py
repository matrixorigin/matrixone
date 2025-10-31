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
MatrixOne Clone Management

Unified clone management supporting sync/async and client/session executors.
"""

from typing import Optional

from .exceptions import CloneError, ConnectionError
from .version import requires_version


class BaseCloneManager:
    """
    Base clone manager with shared logic for sync and async implementations.

    This base class contains all the SQL building logic that is shared between
    sync and async implementations. Subclasses only need to implement the
    execution methods.
    """

    def __init__(self, client, executor=None):
        """
        Initialize base clone manager.

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

    def _build_clone_database_sql(
        self,
        target_db: str,
        source_db: str,
        snapshot_name: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> str:
        """Build CLONE DATABASE SQL statement"""
        if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""

        if snapshot_name:
            return f"CREATE DATABASE {if_not_exists_clause}{target_db} CLONE {source_db} {{snapshot = '{snapshot_name}'}}"
        else:
            return f"CREATE DATABASE {if_not_exists_clause}{target_db} CLONE {source_db}"

    def _build_clone_table_sql(
        self,
        target_table: str,
        source_table: str,
        snapshot_name: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> str:
        """Build CLONE TABLE SQL statement"""
        if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""

        if snapshot_name:
            return (
                f"CREATE TABLE {if_not_exists_clause}{target_table} "
                f"CLONE {source_table} {{snapshot = '{snapshot_name}'}}"
            )
        else:
            return f"CREATE TABLE {if_not_exists_clause}{target_table} CLONE {source_table}"


class CloneManager(BaseCloneManager):
    """
    Synchronous clone management for MatrixOne database operations.

    This class provides comprehensive database and table cloning functionality, enabling
    efficient data replication, testing environments, and data distribution scenarios.
    Cloning operations can work with current data or point-in-time snapshots.

    Key Features:

    - **Database cloning**: Clone entire databases with all tables and data
    - **Table-level cloning**: Clone specific tables for targeted data replication
    - **Snapshot-based cloning**: Clone from specific point-in-time snapshots
    - **Efficient native operations**: Uses MatrixOne's native CLONE functionality for performance
    - **Transaction-aware**: Full integration with transaction contexts via executor pattern
    - **Flexible naming**: Support for IF NOT EXISTS clause to prevent conflicts
    - **Cross-database cloning**: Clone tables between different databases

    Executor Pattern:

    - If executor is None, uses self.client.execute (default client-level executor)
    - If executor is provided (e.g., session), uses executor.execute (transaction-aware)
    - This allows the same logic to work in both client and session contexts
    - All operations can participate in transactions when used via session

    Usage Examples::

        from matrixone import Client

        client = Client(host='localhost', port=6001, user='root', password='111', database='test')

        # Clone entire database (current state)
        client.clone.clone_database(
            target_db='production_backup',
            source_db='production'
        )

        # Clone database with IF NOT EXISTS
        client.clone.clone_database(
            target_db='dev_environment',
            source_db='production',
            if_not_exists=True
        )

        # Clone database from a specific snapshot
        client.clone.clone_database(
            target_db='test_environment',
            source_db='production',
            snapshot_name='daily_backup_2024_01_01'
        )

        # Clone specific table
        client.clone.clone_table(
            target_table='users_backup',
            source_table='users'
        )

        # Clone table across databases
        client.clone.clone_table(
            target_table='dev_db.users',
            source_table='prod_db.users'
        )

        # Clone table from snapshot with validation
        client.clone.clone_table_with_snapshot(
            target_table='users_202401',
            source_table='users',
            snapshot_name='monthly_backup',
            if_not_exists=True
        )

        # Using within a transaction (all operations are atomic)
        with client.session() as session:
            # Clone multiple databases atomically
            session.clone.clone_database('backup_db1', 'source_db1')
            session.clone.clone_database('backup_db2', 'source_db2')
            # Both clones succeed or fail together

    Version Requirements:

        Clone functionality requires MatrixOne version 1.0.0 or higher.
        Earlier versions do not support native cloning operations.

    See Also:

        - SnapshotManager: For creating snapshots before cloning
        - RestoreManager: For restoring databases from snapshots
        - MetadataManager: For managing database metadata
    """

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
    ) -> None:
        """
        Clone a database.

        Args:
            target_db: Target database name
            source_db: Source database name
            snapshot_name: Optional snapshot name for point-in-time clone
            if_not_exists: Use IF NOT EXISTS clause

        Raises:
            ConnectionError: If not connected to database
            CloneError: If clone operation fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_clone_database_sql(target_db, source_db, snapshot_name, if_not_exists)

        try:
            self._get_executor().execute(sql)
        except Exception as e:
            raise CloneError(f"Failed to clone database: {e}") from None

    def clone_table(
        self,
        target_table: str,
        source_table: str,
        snapshot_name: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> None:
        """
        Clone a table.

        Args:
            target_table: Target table name (can include database: db.table)
            source_table: Source table name (can include database: db.table)
            snapshot_name: Optional snapshot name for point-in-time clone
            if_not_exists: Use IF NOT EXISTS clause

        Raises:
            ConnectionError: If not connected to database
            CloneError: If clone operation fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_clone_table_sql(target_table, source_table, snapshot_name, if_not_exists)

        try:
            self._get_executor().execute(sql)
        except Exception as e:
            raise CloneError(f"Failed to clone table: {e}") from None

    def clone_database_with_snapshot(
        self,
        target_db: str,
        source_db: str,
        snapshot_name: str,
        if_not_exists: bool = False,
    ) -> None:
        """
        Clone a database using a specific snapshot.

        Args:
            target_db: Target database name
            source_db: Source database name
            snapshot_name: Snapshot name for point-in-time clone
            if_not_exists: Use IF NOT EXISTS clause

        Raises:
            ConnectionError: If not connected to database
            CloneError: If clone operation fails or snapshot doesn't exist
        """
        # Verify snapshot exists using snapshot manager
        if not self.client.snapshots.exists(snapshot_name):
            raise CloneError(f"Snapshot '{snapshot_name}' does not exist")

        self.clone_database(target_db, source_db, snapshot_name, if_not_exists)

    def clone_table_with_snapshot(
        self,
        target_table: str,
        source_table: str,
        snapshot_name: str,
        if_not_exists: bool = False,
    ) -> None:
        """
        Clone a table using a specific snapshot.

        Args:
            target_table: Target table name (can include database: db.table)
            source_table: Source table name (can include database: db.table)
            snapshot_name: Snapshot name for point-in-time clone
            if_not_exists: Use IF NOT EXISTS clause

        Raises:
            ConnectionError: If not connected to database
            CloneError: If clone operation fails or snapshot doesn't exist
        """
        # Verify snapshot exists using snapshot manager
        if not self.client.snapshots.exists(snapshot_name):
            raise CloneError(f"Snapshot '{snapshot_name}' does not exist")

        self.clone_table(target_table, source_table, snapshot_name, if_not_exists)


class AsyncCloneManager(BaseCloneManager):
    """
    Asynchronous clone management for MatrixOne database operations.

    Provides the same comprehensive cloning functionality as CloneManager but with
    full async/await support for non-blocking I/O operations. Ideal for high-concurrency
    applications and async web frameworks.

    Key Features:

    - **Non-blocking operations**: All clone operations use async/await
    - **Database cloning**: Asynchronously clone entire databases
    - **Table-level cloning**: Asynchronously clone specific tables
    - **Snapshot-based cloning**: Async cloning from point-in-time snapshots
    - **Concurrent operations**: Clone multiple databases/tables concurrently
    - **Transaction-aware**: Full integration with async transaction contexts
    - **Executor pattern**: Works with both async client and async session

    Executor Pattern:

    - If executor is None, uses self.client.execute (default async client-level executor)
    - If executor is provided (e.g., async session), uses executor.execute (async transaction-aware)
    - All operations are non-blocking and use async/await
    - Enables concurrent cloning operations when not in a transaction

    Usage Examples::

        from matrixone import AsyncClient
        import asyncio

        async def main():
            client = AsyncClient()
            await client.connect(host='localhost', port=6001, user='root', password='111', database='test')

            # Clone entire database asynchronously
            await client.clone.clone_database(
                target_db='production_backup',
                source_db='production'
            )

            # Clone database from snapshot
            await client.clone.clone_database(
                target_db='test_environment',
                source_db='production',
                snapshot_name='daily_backup'
            )

            # Clone specific table asynchronously
            await client.clone.clone_table(
                target_table='users_backup',
                source_table='users'
            )

            # Concurrent cloning of multiple databases
            await asyncio.gather(
                client.clone.clone_database('backup1', 'source1'),
                client.clone.clone_database('backup2', 'source2'),
                client.clone.clone_database('backup3', 'source3')
            )

            # Using within async transaction
            async with client.session() as session:
                await session.clone.clone_database('backup_db', 'source_db')
                await session.clone.clone_table('backup_table', 'source_table')
                # Both operations commit atomically

            await client.disconnect()

        asyncio.run(main())

    Version Requirements:

        Clone functionality requires MatrixOne version 1.0.0 or higher.
        Requires async database drivers (aiomysql or asyncmy).

    See Also:

        - AsyncSnapshotManager: For async snapshot operations
        - AsyncRestoreManager: For async restore operations
        - AsyncSession: For async transaction management
    """

    @requires_version(
        min_version="1.0.0",
        feature_name="database_cloning",
        description="Database cloning functionality",
        alternative="Use CREATE DATABASE and data migration instead",
    )
    async def clone_database(
        self,
        target_db: str,
        source_db: str,
        snapshot_name: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> None:
        """
        Clone a database asynchronously.

        Args:
            target_db: Target database name
            source_db: Source database name
            snapshot_name: Optional snapshot name for point-in-time clone
            if_not_exists: Use IF NOT EXISTS clause

        Raises:
            ConnectionError: If not connected to database
            CloneError: If clone operation fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_clone_database_sql(target_db, source_db, snapshot_name, if_not_exists)

        try:
            await self._get_executor().execute(sql)
        except Exception as e:
            raise CloneError(f"Failed to clone database: {e}") from None

    async def clone_table(
        self,
        target_table: str,
        source_table: str,
        snapshot_name: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> None:
        """
        Clone a table asynchronously.

        Args:
            target_table: Target table name (can include database: db.table)
            source_table: Source table name (can include database: db.table)
            snapshot_name: Optional snapshot name for point-in-time clone
            if_not_exists: Use IF NOT EXISTS clause

        Raises:
            ConnectionError: If not connected to database
            CloneError: If clone operation fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_clone_table_sql(target_table, source_table, snapshot_name, if_not_exists)

        try:
            await self._get_executor().execute(sql)
        except Exception as e:
            raise CloneError(f"Failed to clone table: {e}") from None

    async def clone_database_with_snapshot(
        self,
        target_db: str,
        source_db: str,
        snapshot_name: str,
        if_not_exists: bool = False,
    ) -> None:
        """
        Clone a database using a specific snapshot asynchronously.

        Args:
            target_db: Target database name
            source_db: Source database name
            snapshot_name: Snapshot name for point-in-time clone
            if_not_exists: Use IF NOT EXISTS clause

        Raises:
            ConnectionError: If not connected to database
            CloneError: If clone operation fails or snapshot doesn't exist
        """
        # Verify snapshot exists using snapshot manager
        # Note: exists() needs to be awaited for async version
        if not await self.client.snapshots.exists(snapshot_name):
            raise CloneError(f"Snapshot '{snapshot_name}' does not exist")

        await self.clone_database(target_db, source_db, snapshot_name, if_not_exists)

    async def clone_table_with_snapshot(
        self,
        target_table: str,
        source_table: str,
        snapshot_name: str,
        if_not_exists: bool = False,
    ) -> None:
        """
        Clone a table using a specific snapshot asynchronously.

        Args:
            target_table: Target table name (can include database: db.table)
            source_table: Source table name (can include database: db.table)
            snapshot_name: Snapshot name for point-in-time clone
            if_not_exists: Use IF NOT EXISTS clause

        Raises:
            ConnectionError: If not connected to database
            CloneError: If clone operation fails or snapshot doesn't exist
        """
        # Verify snapshot exists using snapshot manager
        # Note: exists() needs to be awaited for async version
        if not await self.client.snapshots.exists(snapshot_name):
            raise CloneError(f"Snapshot '{snapshot_name}' does not exist")

        await self.clone_table(target_table, source_table, snapshot_name, if_not_exists)
