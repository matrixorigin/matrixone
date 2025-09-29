"""
MatrixOne Python SDK - Restore Manager
Provides restore functionality for MatrixOne snapshots
"""

from typing import Optional

from .exceptions import RestoreError


class RestoreManager:
    """Manager for restore operations from snapshots"""

    def __init__(self, client):
        """Initialize RestoreManager with client connection"""
        self._client = client

    def restore_cluster(self, snapshot_name: str) -> bool:
        """
        Restore entire cluster from snapshot

        Args:
            snapshot_name: Name of the snapshot to restore from

        Returns:
            bool: True if restore was successful

        Raises:
            RestoreError: If restore operation fails

        Example:
            >>> client = Client()
            >>> client.connect(...)
            >>> success = client.restore.restore_cluster("cluster_snapshot_1")
        """
        try:
            sql = f"RESTORE CLUSTER FROM SNAPSHOT {self._client._escape_identifier(snapshot_name)}"
            result = self._client.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore cluster from snapshot '{snapshot_name}': {e}")

    def restore_tenant(self, snapshot_name: str, account_name: str, to_account: Optional[str] = None) -> bool:
        """
        Restore tenant from snapshot

        Args:
            snapshot_name: Name of the snapshot to restore from
            account_name: Name of the account to restore
            to_account: Optional target account name (for cross-tenant restore)

        Returns:
            bool: True if restore was successful

        Raises:
            RestoreError: If restore operation fails

        Example:
            >>> # Restore tenant to itself
            >>> success = client.restore.restore_tenant("acc1_snap1", "acc1")
            >>>
            >>> # Restore tenant to new tenant
            >>> success = client.restore.restore_tenant("acc1_snap1", "acc1", "acc2")
        """
        try:
            if to_account:
                # Cross-tenant restore
                sql = (
                    f"RESTORE ACCOUNT {self._client._escape_identifier(account_name)} "
                    f"FROM SNAPSHOT {self._client._escape_identifier(snapshot_name)} "
                    f"TO ACCOUNT {self._client._escape_identifier(to_account)}"
                )
            else:
                # Restore to same tenant
                sql = (
                    f"RESTORE ACCOUNT {self._client._escape_identifier(account_name)} "
                    f"FROM SNAPSHOT {self._client._escape_identifier(snapshot_name)}"
                )

            result = self._client.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore tenant '{account_name}' from snapshot '{snapshot_name}': {e}")

    def restore_database(
        self,
        snapshot_name: str,
        account_name: str,
        database_name: str,
        to_account: Optional[str] = None,
    ) -> bool:
        """
        Restore database from snapshot

        Args:
            snapshot_name: Name of the snapshot to restore from
            account_name: Name of the account
            database_name: Name of the database to restore
            to_account: Optional target account name (for cross-tenant restore)

        Returns:
            bool: True if restore was successful

        Raises:
            RestoreError: If restore operation fails

        Example:
            >>> success = client.restore.restore_database("acc1_db_snap1", "acc1", "db1")
        """
        try:
            if to_account:
                # Cross-tenant restore
                sql = (
                    f"RESTORE ACCOUNT {self._client._escape_identifier(account_name)} "
                    f"DATABASE {self._client._escape_identifier(database_name)} "
                    f"FROM SNAPSHOT {self._client._escape_identifier(snapshot_name)} "
                    f"TO ACCOUNT {self._client._escape_identifier(to_account)}"
                )
            else:
                # Restore to same tenant
                sql = (
                    f"RESTORE ACCOUNT {self._client._escape_identifier(account_name)} "
                    f"DATABASE {self._client._escape_identifier(database_name)} "
                    f"FROM SNAPSHOT {self._client._escape_identifier(snapshot_name)}"
                )

            result = self._client.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore database '{database_name}' from snapshot '{snapshot_name}': {e}")

    def restore_table(
        self,
        snapshot_name: str,
        account_name: str,
        database_name: str,
        table_name: str,
        to_account: Optional[str] = None,
    ) -> bool:
        """
        Restore table from snapshot

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

        Example:
            >>> success = client.restore.restore_table("acc1_tab_snap1", "acc1", "db1", "t1")
        """
        try:
            if to_account:
                # Cross-tenant restore
                sql = (
                    f"RESTORE ACCOUNT {self._client._escape_identifier(account_name)} "
                    f"DATABASE {self._client._escape_identifier(database_name)} "
                    f"TABLE {self._client._escape_identifier(table_name)} "
                    f"FROM SNAPSHOT {self._client._escape_identifier(snapshot_name)} "
                    f"TO ACCOUNT {self._client._escape_identifier(to_account)}"
                )
            else:
                # Restore to same tenant
                sql = (
                    f"RESTORE ACCOUNT {self._client._escape_identifier(account_name)} "
                    f"DATABASE {self._client._escape_identifier(database_name)} "
                    f"TABLE {self._client._escape_identifier(table_name)} "
                    f"FROM SNAPSHOT {self._client._escape_identifier(snapshot_name)}"
                )

            result = self._client.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore table '{table_name}' from snapshot '{snapshot_name}': {e}")

    def restore_with_executor(
        self,
        restore_type: str,
        snapshot_name: str,
        account_name: Optional[str] = None,
        database_name: Optional[str] = None,
        table_name: Optional[str] = None,
        to_account: Optional[str] = None,
        executor=None,
    ) -> bool:
        """
        Restore with custom executor (for transaction support)

        Args:
            restore_type: Type of restore ('cluster', 'tenant', 'database', 'table')
            snapshot_name: Name of the snapshot to restore from
            account_name: Name of the account (required for tenant/database/table)
            database_name: Name of the database (required for database/table)
            table_name: Name of the table (required for table)
            to_account: Optional target account name
            executor: Custom executor (transaction wrapper)

        Returns:
            bool: True if restore was successful
        """
        try:
            if restore_type == "cluster":
                sql = f"RESTORE CLUSTER FROM SNAPSHOT {self._client._escape_identifier(snapshot_name)}"
            elif restore_type == "tenant":
                if not account_name:
                    raise RestoreError("Account name is required for tenant restore")
                if to_account:
                    sql = (
                        f"RESTORE ACCOUNT {self._client._escape_identifier(account_name)} "
                        f"FROM SNAPSHOT {self._client._escape_identifier(snapshot_name)} "
                        f"TO ACCOUNT {self._client._escape_identifier(to_account)}"
                    )
                else:
                    sql = (
                        f"RESTORE ACCOUNT {self._client._escape_identifier(account_name)} "
                        f"FROM SNAPSHOT {self._client._escape_identifier(snapshot_name)}"
                    )
            elif restore_type == "database":
                if not account_name or not database_name:
                    raise RestoreError("Account name and database name are required for database restore")
                if to_account:
                    sql = (
                        f"RESTORE ACCOUNT {self._client._escape_identifier(account_name)} "
                        f"DATABASE {self._client._escape_identifier(database_name)} "
                        f"FROM SNAPSHOT {self._client._escape_identifier(snapshot_name)} "
                        f"TO ACCOUNT {self._client._escape_identifier(to_account)}"
                    )
                else:
                    sql = (
                        f"RESTORE ACCOUNT {self._client._escape_identifier(account_name)} "
                        f"DATABASE {self._client._escape_identifier(database_name)} "
                        f"FROM SNAPSHOT {self._client._escape_identifier(snapshot_name)}"
                    )
            elif restore_type == "table":
                if not all([account_name, database_name, table_name]):
                    raise RestoreError("Account name, database name, and table name are required for table restore")
                if to_account:
                    sql = (
                        f"RESTORE ACCOUNT {self._client._escape_identifier(account_name)} "
                        f"DATABASE {self._client._escape_identifier(database_name)} "
                        f"TABLE {self._client._escape_identifier(table_name)} "
                        f"FROM SNAPSHOT {self._client._escape_identifier(snapshot_name)} "
                        f"TO ACCOUNT {self._client._escape_identifier(to_account)}"
                    )
                else:
                    sql = (
                        f"RESTORE ACCOUNT {self._client._escape_identifier(account_name)} "
                        f"DATABASE {self._client._escape_identifier(database_name)} "
                        f"TABLE {self._client._escape_identifier(table_name)} "
                        f"FROM SNAPSHOT {self._client._escape_identifier(snapshot_name)}"
                    )
            else:
                raise RestoreError(f"Invalid restore type: {restore_type}")

            if executor:
                result = executor.execute(sql)
            else:
                result = self._client.execute(sql)

            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore {restore_type} from snapshot '{snapshot_name}': {e}")


class TransactionRestoreManager(RestoreManager):
    """RestoreManager for use within transactions"""

    def __init__(self, client, transaction_wrapper):
        """Initialize TransactionRestoreManager with client and transaction wrapper"""
        super().__init__(client)
        self._transaction_wrapper = transaction_wrapper

    def restore_cluster(self, snapshot_name: str) -> bool:
        """Restore cluster within transaction"""
        return self.restore_with_executor("cluster", snapshot_name, executor=self._transaction_wrapper)

    def restore_tenant(self, snapshot_name: str, account_name: str, to_account: Optional[str] = None) -> bool:
        """Restore tenant within transaction"""
        return self.restore_with_executor(
            "tenant",
            snapshot_name,
            account_name,
            to_account=to_account,
            executor=self._transaction_wrapper,
        )

    def restore_database(
        self,
        snapshot_name: str,
        account_name: str,
        database_name: str,
        to_account: Optional[str] = None,
    ) -> bool:
        """Restore database within transaction"""
        return self.restore_with_executor(
            "database",
            snapshot_name,
            account_name,
            database_name,
            to_account=to_account,
            executor=self._transaction_wrapper,
        )

    def restore_table(
        self,
        snapshot_name: str,
        account_name: str,
        database_name: str,
        table_name: str,
        to_account: Optional[str] = None,
    ) -> bool:
        """Restore table within transaction"""
        return self.restore_with_executor(
            "table",
            snapshot_name,
            account_name,
            database_name,
            table_name,
            to_account=to_account,
            executor=self._transaction_wrapper,
        )
