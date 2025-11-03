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
MatrixOne Restore Management

Unified restore management supporting sync/async and client/session executors.
"""

from typing import Optional

from .exceptions import RestoreError


class RestoreManager:
    """
    Synchronous restore management for MatrixOne snapshots.

    This class provides comprehensive restore functionality for recovering data
    from snapshots at various granularities (cluster, tenant, database, table).
    It enables disaster recovery, data rollback, and point-in-time data recovery
    scenarios with full support for cross-tenant restore operations.

    Key Features:

    - **Multi-level restore**: Restore clusters, tenants, databases, or tables
    - **Cross-tenant restore**: Restore data from one tenant to another
    - **Snapshot-based recovery**: Restore from any existing snapshot
    - **Transaction-aware**: Full integration with transaction contexts
    - **Executor pattern**: Works with both client and session contexts
    - **Atomic operations**: All restore operations are transactional
    - **Disaster recovery**: Quick recovery from data loss or corruption

    Restore Levels:

    - **Cluster**: Restore entire cluster state from a cluster-level snapshot
    - **Tenant**: Restore entire tenant (account) from an account-level snapshot
    - **Database**: Restore specific database from a database-level snapshot
    - **Table**: Restore specific table from a table-level snapshot

    Executor Pattern:

    - If executor is None, uses self.client.execute (default client-level executor)
    - If executor is provided (e.g., session), uses executor.execute (transaction-aware)
    - This allows the same logic to work in both client and session contexts
    - All operations can participate in transactions when used via session

    Usage Examples::

        from matrixone import Client

        client = Client(host='localhost', port=6001, user='root', password='111', database='test')

        # Restore entire cluster from snapshot
        success = client.restore.restore_cluster('cluster_backup_snapshot')
        if success:
            print("Cluster restored successfully")

        # Restore tenant/account from snapshot
        client.restore.restore_tenant(
            snapshot_name='daily_backup',
            account_name='production_account'
        )

        # Restore tenant to a different account (cross-tenant restore)
        client.restore.restore_tenant(
            snapshot_name='prod_backup',
            account_name='production',
            to_account='staging'  # Restore prod data to staging account
        )

        # Restore specific database
        client.restore.restore_database(
            snapshot_name='daily_backup',
            account_name='production',
            database_name='analytics'
        )

        # Restore database to different account
        client.restore.restore_database(
            snapshot_name='prod_db_backup',
            account_name='production',
            database_name='orders',
            to_account='test_environment'
        )

        # Restore specific table
        client.restore.restore_table(
            snapshot_name='table_backup',
            account_name='production',
            database_name='orders',
            table_name='customers'
        )

        # Using within a transaction
        with client.session() as session:
            # Restore multiple objects atomically
            session.restore.restore_database('backup1', 'acc1', 'db1')
            session.restore.restore_table('backup2', 'acc1', 'db2', 'table1')
            # Both restore operations commit together

    Prerequisites:

        - Snapshots must exist before restore operations can be performed
        - Use SnapshotManager to create snapshots before restoring
        - Appropriate permissions are required for cross-tenant restore operations

    See Also:

        - SnapshotManager: For creating snapshots
        - CloneManager: For cloning from snapshots
        - PitrManager: For point-in-time recovery operations
    """

    def __init__(self, client, executor=None):
        """
        Initialize restore manager.

        Args:
            client: MatrixOne client instance
            executor: Optional executor (e.g., session) for executing SQL.
                     If None, uses client.execute
        """
        self.client = client
        self.executor = executor

    def _get_executor(self):
        """Get the executor for SQL execution (session or client)"""
        return self.executor if self.executor else self.client

    def restore_cluster(self, snapshot_name: str) -> bool:
        """
        Restore entire cluster from snapshot.

        Args:
            snapshot_name: Name of the snapshot to restore from

        Returns:
            bool: True if restore was successful

        Raises:
            RestoreError: If restore operation fails

        Example::

            >>> success = client.restore.restore_cluster("cluster_snapshot_1")
        """
        try:
            sql = f"RESTORE CLUSTER FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}"
            result = self._get_executor().execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore cluster from snapshot '{snapshot_name}': {e}") from None

    def restore_tenant(self, snapshot_name: str, account_name: str, to_account: Optional[str] = None) -> bool:
        """
        Restore tenant from snapshot.

        Args:
            snapshot_name: Name of the snapshot to restore from
            account_name: Name of the account to restore
            to_account: Optional target account name (for cross-tenant restore)

        Returns:
            bool: True if restore was successful

        Raises:
            RestoreError: If restore operation fails

        Example::

            >>> # Restore tenant to itself
            >>> success = client.restore.restore_tenant("acc1_snap1", "acc1")
            >>>
            >>> # Restore tenant to new tenant
            >>> success = client.restore.restore_tenant("acc1_snap1", "acc1", "acc2")
        """
        try:
            if to_account:
                sql = (
                    f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                    f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)} "
                    f"TO ACCOUNT {self.client._escape_identifier(to_account)}"
                )
            else:
                sql = (
                    f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                    f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}"
                )

            result = self._get_executor().execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore tenant '{account_name}' from snapshot '{snapshot_name}': {e}") from None

    def restore_database(
        self,
        snapshot_name: str,
        account_name: str,
        database_name: str,
        to_account: Optional[str] = None,
    ) -> bool:
        """
        Restore database from snapshot.

        Args:
            snapshot_name: Name of the snapshot to restore from
            account_name: Name of the account
            database_name: Name of the database to restore
            to_account: Optional target account name (for cross-tenant restore)

        Returns:
            bool: True if restore was successful

        Raises:
            RestoreError: If restore operation fails

        Example::

            >>> success = client.restore.restore_database("acc1_db_snap1", "acc1", "db1")
        """
        try:
            if to_account:
                sql = (
                    f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                    f"DATABASE {self.client._escape_identifier(database_name)} "
                    f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)} "
                    f"TO ACCOUNT {self.client._escape_identifier(to_account)}"
                )
            else:
                sql = (
                    f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                    f"DATABASE {self.client._escape_identifier(database_name)} "
                    f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}"
                )

            result = self._get_executor().execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(
                f"Failed to restore database '{database_name}' from snapshot '{snapshot_name}': {e}"
            ) from None

    def restore_table(
        self,
        snapshot_name: str,
        account_name: str,
        database_name: str,
        table_name: str,
        to_account: Optional[str] = None,
    ) -> bool:
        """
        Restore table from snapshot.

        Args:
            snapshot_name: Name of the snapshot to restore from
            account_name: Name of the account
            database_name: Name of the database
            table_name: Name of the table to restore
            to_account: Optional target account name (for cross-tenant restore)

        Returns:
            bool: True if restore was successful

        Raises:
            RestoreError: If restore operation fails

        Example::

            >>> success = client.restore.restore_table("acc1_tab_snap1", "acc1", "db1", "t1")
        """
        try:
            if to_account:
                sql = (
                    f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                    f"DATABASE {self.client._escape_identifier(database_name)} "
                    f"TABLE {self.client._escape_identifier(table_name)} "
                    f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)} "
                    f"TO ACCOUNT {self.client._escape_identifier(to_account)}"
                )
            else:
                sql = (
                    f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                    f"DATABASE {self.client._escape_identifier(database_name)} "
                    f"TABLE {self.client._escape_identifier(table_name)} "
                    f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}"
                )

            result = self._get_executor().execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore table '{table_name}' from snapshot '{snapshot_name}': {e}") from None


class AsyncRestoreManager:
    """
    Asynchronous restore management for MatrixOne snapshots.

    Provides the same comprehensive restore functionality as RestoreManager but with
    full async/await support for non-blocking I/O operations. Ideal for high-concurrency
    disaster recovery scenarios and async web applications.

    Key Features:

    - **Non-blocking operations**: All restore operations use async/await
    - **Multi-level async restore**: Restore clusters, tenants, databases, or tables
    - **Async cross-tenant restore**: Non-blocking cross-tenant restore operations
    - **Concurrent operations**: Restore multiple objects concurrently
    - **Transaction-aware**: Full integration with async transaction contexts
    - **Executor pattern**: Works with both async client and async session

    Executor Pattern:

    - If executor is None, uses self.client.execute (default async client-level executor)
    - If executor is provided (e.g., async session), uses executor.execute (async transaction-aware)
    - All operations are non-blocking and use async/await
    - Enables concurrent restore operations when not in a transaction

    Usage Examples::

        from matrixone import AsyncClient
        import asyncio

        async def main():
            client = AsyncClient()
            await client.connect(host='localhost', port=6001, user='root', password='111', database='test')

            # Restore entire cluster asynchronously
            success = await client.restore.restore_cluster('cluster_backup')
            if success:
                print("Cluster restored successfully")

            # Restore tenant asynchronously
            await client.restore.restore_tenant(
                snapshot_name='tenant_backup',
                account_name='production'
            )

            # Restore with cross-tenant (async)
            await client.restore.restore_tenant(
                snapshot_name='prod_snapshot',
                account_name='production',
                to_account='staging'
            )

            # Restore database asynchronously
            await client.restore.restore_database(
                snapshot_name='db_backup',
                account_name='production',
                database_name='analytics'
            )

            # Restore table asynchronously
            await client.restore.restore_table(
                snapshot_name='table_backup',
                account_name='production',
                database_name='orders',
                table_name='customers'
            )

            # Concurrent restore of multiple databases
            await asyncio.gather(
                client.restore.restore_database('snap1', 'acc1', 'db1'),
                client.restore.restore_database('snap2', 'acc1', 'db2'),
                client.restore.restore_database('snap3', 'acc1', 'db3')
            )

            # Using within async transaction
            async with client.session() as session:
                await session.restore.restore_database('backup1', 'acc1', 'db1')
                await session.restore.restore_table('backup2', 'acc1', 'db2', 'table1')
                # Both operations commit atomically

            await client.disconnect()

        asyncio.run(main())

    Prerequisites:

        - Snapshots must exist before restore operations can be performed
        - Use AsyncSnapshotManager to create snapshots asynchronously
        - Requires async database drivers (aiomysql or asyncmy)

    See Also:

        - AsyncSnapshotManager: For async snapshot operations
        - AsyncCloneManager: For async cloning from snapshots
        - AsyncPitrManager: For async point-in-time recovery
        - AsyncSession: For async transaction management
    """

    def __init__(self, client, executor=None):
        """
        Initialize async restore manager.

        Args:
            client: MatrixOne async client instance
            executor: Optional executor (e.g., async session) for executing SQL.
                     If None, uses client.execute
        """
        self.client = client
        self.executor = executor

    def _get_executor(self):
        """Get the executor for SQL execution (session or client)"""
        return self.executor if self.executor else self.client

    async def restore_cluster(self, snapshot_name: str) -> bool:
        """
        Restore entire cluster from snapshot asynchronously.

        Args:
            snapshot_name: Name of the snapshot to restore from

        Returns:
            bool: True if restore was successful

        Raises:
            RestoreError: If restore operation fails
        """
        try:
            sql = f"RESTORE CLUSTER FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}"
            result = await self._get_executor().execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore cluster from snapshot '{snapshot_name}': {e}") from None

    async def restore_tenant(self, snapshot_name: str, account_name: str, to_account: Optional[str] = None) -> bool:
        """
        Restore tenant from snapshot asynchronously.

        Args:
            snapshot_name: Name of the snapshot to restore from
            account_name: Name of the account to restore
            to_account: Optional target account name (for cross-tenant restore)

        Returns:
            bool: True if restore was successful

        Raises:
            RestoreError: If restore operation fails
        """
        try:
            if to_account:
                sql = (
                    f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                    f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)} "
                    f"TO ACCOUNT {self.client._escape_identifier(to_account)}"
                )
            else:
                sql = (
                    f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                    f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}"
                )

            result = await self._get_executor().execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore tenant '{account_name}' from snapshot '{snapshot_name}': {e}") from None

    async def restore_database(
        self,
        snapshot_name: str,
        account_name: str,
        database_name: str,
        to_account: Optional[str] = None,
    ) -> bool:
        """
        Restore database from snapshot asynchronously.

        Args:
            snapshot_name: Name of the snapshot to restore from
            account_name: Name of the account
            database_name: Name of the database to restore
            to_account: Optional target account name (for cross-tenant restore)

        Returns:
            bool: True if restore was successful

        Raises:
            RestoreError: If restore operation fails
        """
        try:
            if to_account:
                sql = (
                    f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                    f"DATABASE {self.client._escape_identifier(database_name)} "
                    f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)} "
                    f"TO ACCOUNT {self.client._escape_identifier(to_account)}"
                )
            else:
                sql = (
                    f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                    f"DATABASE {self.client._escape_identifier(database_name)} "
                    f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}"
                )

            result = await self._get_executor().execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(
                f"Failed to restore database '{database_name}' from snapshot '{snapshot_name}': {e}"
            ) from None

    async def restore_table(
        self,
        snapshot_name: str,
        account_name: str,
        database_name: str,
        table_name: str,
        to_account: Optional[str] = None,
    ) -> bool:
        """
        Restore table from snapshot asynchronously.

        Args:
            snapshot_name: Name of the snapshot to restore from
            account_name: Name of the account
            database_name: Name of the database
            table_name: Name of the table to restore
            to_account: Optional target account name (for cross-tenant restore)

        Returns:
            bool: True if restore was successful

        Raises:
            RestoreError: If restore operation fails
        """
        try:
            if to_account:
                sql = (
                    f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                    f"DATABASE {self.client._escape_identifier(database_name)} "
                    f"TABLE {self.client._escape_identifier(table_name)} "
                    f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)} "
                    f"TO ACCOUNT {self.client._escape_identifier(to_account)}"
                )
            else:
                sql = (
                    f"RESTORE ACCOUNT {self.client._escape_identifier(account_name)} "
                    f"DATABASE {self.client._escape_identifier(database_name)} "
                    f"TABLE {self.client._escape_identifier(table_name)} "
                    f"FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}"
                )

            result = await self._get_executor().execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore table '{table_name}' from snapshot '{snapshot_name}': {e}") from None
