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
    from snapshots. It supports restoring entire clusters, databases, or tables
    from previously created snapshots, enabling data recovery and disaster
    recovery scenarios.

    Key Features:
    - Restore entire clusters from snapshots
    - Restore specific databases from snapshots
    - Restore individual tables from snapshots
    - Integration with snapshot management
    - Transaction-aware operations via executor pattern
    - Unified interface for client and session contexts

    Executor Pattern:
    - If executor is None, uses self.client.execute (default executor)
    - If executor is provided (e.g., session), uses executor.execute
    - This allows the same logic to work in both client and session contexts

    Usage Examples::

        # Using client directly
        success = client.restore.restore_cluster('daily_backup_snapshot')

        # Using within a session (transaction)
        with client.session() as session:
            success = session.restore.restore_cluster('daily_backup_snapshot')
            session.commit()

        # Restore database
        client.restore.restore_database(
            snapshot_name='daily_backup',
            account_name='acc1',
            database_name='my_database'
        )

    Note: Restore operations require appropriate snapshots to be available.
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

    Provides the same functionality as RestoreManager but with async/await support.
    Uses the same executor pattern to support both client and session contexts.
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
