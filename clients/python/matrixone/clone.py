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

    This class provides comprehensive database cloning functionality for creating
    copies of databases, tables, or data subsets. Cloning enables efficient
    data replication, testing environments, and data distribution scenarios.

    Key Features:
    - Database cloning with full data replication
    - Table-level cloning for specific data subsets
    - Efficient cloning using MatrixOne's native capabilities
    - Integration with snapshot and restore operations
    - Transaction-aware cloning operations via executor pattern
    - Unified interface for client and session contexts

    Executor Pattern:
    - If executor is None, uses self.client.execute (default executor)
    - If executor is provided (e.g., session), uses executor.execute
    - This allows the same logic to work in both client and session contexts

    Usage Examples::

        # Using client directly
        client.clone.clone_database(
            target_db='cloned_db',
            source_db='source_db'
        )

        # Using within a session (transaction)
        with client.session() as session:
            session.clone.clone_database(
                target_db='cloned_db',
                source_db='source_db'
            )

        # Clone with snapshot
        client.clone.clone_database(
            target_db='cloned_db',
            source_db='source_db',
            snapshot_name='my_snapshot'
        )

    Note: Cloning functionality requires MatrixOne version 1.0.0 or higher.
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

    Provides the same functionality as CloneManager but with async/await support.
    Uses the same executor pattern to support both client and session contexts.
    Shares SQL building logic with the synchronous version via BaseCloneManager.
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
