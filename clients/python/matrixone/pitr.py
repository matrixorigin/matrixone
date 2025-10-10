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
MatrixOne Python SDK - PITR Manager
Provides Point-in-Time Recovery functionality for MatrixOne
"""

from datetime import datetime
from typing import List, Optional

from .exceptions import PitrError
from .version import requires_version


class Pitr:
    """PITR (Point-in-Time Recovery) object"""

    def __init__(
        self,
        name: str,
        created_time: datetime,
        modified_time: datetime,
        level: str,
        account_name: Optional[str] = None,
        database_name: Optional[str] = None,
        table_name: Optional[str] = None,
        range_value: int = 1,
        range_unit: str = "d",
    ):
        """
        Initialize PITR object

        Args::

            name: PITR name
            created_time: Creation time
            modified_time: Last modification time
            level: PITR level (cluster, account, database, table)
            account_name: Account name (for account/database/table level)
            database_name: Database name (for database/table level)
            table_name: Table name (for table level)
            range_value: Time range value (1-100)
            range_unit: Time range unit (h, d, mo, y)
        """
        self.name = name
        self.created_time = created_time
        self.modified_time = modified_time
        self.level = level
        self.account_name = account_name
        self.database_name = database_name
        self.table_name = table_name
        self.range_value = range_value
        self.range_unit = range_unit

    def __repr__(self):
        return f"<Pitr(name='{self.name}', level='{self.level}', " f"range={self.range_value}{self.range_unit})>"


class PitrManager:
    """
    Manager for Point-in-Time Recovery (PITR) operations in MatrixOne.

    This class provides comprehensive PITR functionality for recovering data
    to specific points in time. PITR allows you to restore databases or
    tables to their state at any point in time, providing granular recovery
    capabilities for data protection and disaster recovery.

    Key Features:

    - Point-in-time recovery for databases and tables
    - Recovery to specific timestamps
    - Integration with backup and snapshot systems
    - Transaction-aware recovery operations
    - Support for both cluster and table-level recovery

    Supported Recovery Levels:
    - CLUSTER: Full cluster recovery to a specific point in time
    - DATABASE: Database-level recovery to a specific point in time
    - TABLE: Table-level recovery to a specific point in time

    Usage Examples::

        # Initialize PITR manager
        pitr = client.pitr

        # Recover database to specific timestamp
        pitr.recover_database(
            database='my_database',
            timestamp='2024-01-15 10:30:00',
            target_database='recovered_database'
        )

        # Recover table to specific timestamp
        pitr.recover_table(
            database='my_database',
            table='users',
            timestamp='2024-01-15 10:30:00',
            target_database='recovered_database',
            target_table='recovered_users'
        )

        # List available recovery points
        recovery_points = pitr.list_recovery_points('my_database')

        # Get recovery status
        status = pitr.get_recovery_status('recovery_job_id')

    Note: PITR functionality requires MatrixOne version 1.0.0 or higher and
    appropriate backup infrastructure. Recovery operations may take significant
    time depending on the amount of data and the target timestamp.
    """

    def __init__(self, client):
        """Initialize PitrManager with client connection"""
        self._client = client

    @requires_version(
        min_version="1.0.0",
        feature_name="pitr_cluster_level",
        description="Cluster-level Point-in-Time Recovery functionality",
        alternative="Use snapshot restore instead",
    )
    def create_cluster_pitr(self, name: str, range_value: int = 1, range_unit: str = "d") -> Pitr:
        """
            Create cluster-level PITR

            Args::

                name: PITR name
                range_value: Time range value (1-100)
                range_unit: Time range unit (h, d, mo, y)

            Returns::

                Pitr: Created PITR object

            Raises::

                PitrError: If PITR creation fails

            Example

        >>> pitr = client.pitr.create_cluster_pitr("cluster_pitr1", 1, "d")
        """
        try:
            self._validate_range(range_value, range_unit)

            sql = f"CREATE PITR {self._client._escape_identifier(name)} " f"FOR CLUSTER RANGE {range_value} '{range_unit}'"

            result = self._client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create cluster PITR '{name}'") from None

            # Return PITR object (we'll get the actual details via SHOW PITR)
            return self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create cluster PITR '{name}': {e}") from None

    def create_account_pitr(
        self,
        name: str,
        account_name: Optional[str] = None,
        range_value: int = 1,
        range_unit: str = "d",
    ) -> Pitr:
        """
            Create account-level PITR

            Args::

                name: PITR name
                account_name: Account name (None for current account)
                range_value: Time range value (1-100)
                range_unit: Time range unit (h, d, mo, y)

            Returns::

                Pitr: Created PITR object

            Raises::

                PitrError: If PITR creation fails

            Example

        >>> # For current account
                >>> pitr = client.pitr.create_account_pitr("account_pitr1", range_value=2, range_unit="h")
                >>>
                >>> # For specific account (cluster admin only)
                >>> pitr = client.pitr.create_account_pitr("account_pitr1", "acc1", 1, "d")
        """
        try:
            self._validate_range(range_value, range_unit)

            if account_name:
                sql = (
                    f"CREATE PITR {self._client._escape_identifier(name)} "
                    f"FOR ACCOUNT {self._client._escape_identifier(account_name)} "
                    f"RANGE {range_value} '{range_unit}'"
                )
            else:
                sql = (
                    f"CREATE PITR {self._client._escape_identifier(name)} " f"FOR ACCOUNT RANGE {range_value} '{range_unit}'"
                )

            result = self._client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create account PITR '{name}'") from None

            return self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create account PITR '{name}': {e}") from None

    def create_database_pitr(self, name: str, database_name: str, range_value: int = 1, range_unit: str = "d") -> Pitr:
        """
            Create database-level PITR

            Args::

                name: PITR name
                database_name: Database name
                range_value: Time range value (1-100)
                range_unit: Time range unit (h, d, mo, y)

            Returns::

                Pitr: Created PITR object

            Raises::

                PitrError: If PITR creation fails

            Example

        >>> pitr = client.pitr.create_database_pitr("db_pitr1", "db1", 1, "y")
        """
        try:
            self._validate_range(range_value, range_unit)

            sql = (
                f"CREATE PITR {self._client._escape_identifier(name)} "
                f"FOR DATABASE {self._client._escape_identifier(database_name)} "
                f"RANGE {range_value} '{range_unit}'"
            )

            result = self._client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create database PITR '{name}'") from None

            return self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create database PITR '{name}': {e}") from None

    def create_table_pitr(
        self,
        name: str,
        database_name: str,
        table_name: str,
        range_value: int = 1,
        range_unit: str = "d",
    ) -> Pitr:
        """
            Create table-level PITR

            Args::

                name: PITR name
                database_name: Database name
                table_name: Table name
                range_value: Time range value (1-100)
                range_unit: Time range unit (h, d, mo, y)

            Returns::

                Pitr: Created PITR object

            Raises::

                PitrError: If PITR creation fails

            Example

        >>> pitr = client.pitr.create_table_pitr("tab_pitr1", "db1", "t1", 1, "y")
        """
        try:
            self._validate_range(range_value, range_unit)

            sql = (
                f"CREATE PITR {self._client._escape_identifier(name)} "
                f"FOR TABLE {self._client._escape_identifier(database_name)} "
                f"{self._client._escape_identifier(table_name)} "
                f"RANGE {range_value} '{range_unit}'"
            )

            result = self._client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create table PITR '{name}'") from None

            return self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create table PITR '{name}': {e}") from None

    def get(self, name: str) -> Pitr:
        """
        Get PITR by name

        Args::

            name: PITR name

        Returns::

            Pitr: PITR object

        Raises::

            PitrError: If PITR not found
        """
        try:
            sql = f"SHOW PITR WHERE pitr_name = {self._client._escape_string(name)}"
            result = self._client.execute(sql)

            if not result or not result.rows:
                raise PitrError(f"PITR '{name}' not found") from None

            row = result.rows[0]
            return self._row_to_pitr(row)

        except Exception as e:
            raise PitrError(f"Failed to get PITR '{name}': {e}") from None

    def list(
        self,
        level: Optional[str] = None,
        account_name: Optional[str] = None,
        database_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> List[Pitr]:
        """
        List PITRs with optional filters

        Args::

            level: Filter by PITR level (cluster, account, database, table)
            account_name: Filter by account name
            database_name: Filter by database name
            table_name: Filter by table name

        Returns::

            List[Pitr]: List of PITR objects
        """
        try:
            conditions = []

            if level:
                conditions.append(f"pitr_level = {self._client._escape_string(level)}")
            if account_name:
                conditions.append(f"account_name = {self._client._escape_string(account_name)}")
            if database_name:
                conditions.append(f"database_name = {self._client._escape_string(database_name)}")
            if table_name:
                conditions.append(f"table_name = {self._client._escape_string(table_name)}")

            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
            else:
                where_clause = ""

            sql = f"SHOW PITR{where_clause}"
            result = self._client.execute(sql)

            if not result or not result.rows:
                return []

            return [self._row_to_pitr(row) for row in result.rows]

        except Exception as e:
            raise PitrError(f"Failed to list PITRs: {e}") from None

    def alter(self, name: str, range_value: int, range_unit: str) -> Pitr:
        """
        Alter PITR range

        Args::

            name: PITR name
            range_value: New time range value (1-100)
            range_unit: New time range unit (h, d, mo, y)

        Returns::

            Pitr: Updated PITR object

        Raises::

            PitrError: If PITR alteration fails
        """
        try:
            self._validate_range(range_value, range_unit)

            sql = f"ALTER PITR {self._client._escape_identifier(name)} " f"RANGE {range_value} '{range_unit}'"

            result = self._client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to alter PITR '{name}'") from None

            return self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to alter PITR '{name}': {e}") from None

    def delete(self, name: str) -> bool:
        """
        Delete PITR

        Args::

            name: PITR name

        Returns::

            bool: True if deletion was successful

        Raises::

            PitrError: If PITR deletion fails
        """
        try:
            sql = f"DROP PITR {self._client._escape_identifier(name)}"
            result = self._client.execute(sql)
            return result is not None

        except Exception as e:
            raise PitrError(f"Failed to delete PITR '{name}': {e}") from None

    def _validate_range(self, range_value: int, range_unit: str) -> None:
        """Validate PITR range parameters"""
        if not (1 <= range_value <= 100):
            raise PitrError("Range value must be between 1 and 100") from None

        valid_units = ["h", "d", "mo", "y"]
        if range_unit not in valid_units:
            raise PitrError(f"Range unit must be one of: {', '.join(valid_units)}") from None

    def _row_to_pitr(self, row: tuple) -> Pitr:
        """Convert database row to Pitr object"""
        # Expected columns: pitr_name, created_time, modified_time, pitr_level,
        # account_name, database_name, table_name, pitr_length, pitr_unit
        return Pitr(
            name=row[0],
            created_time=row[1],
            modified_time=row[2],
            level=row[3],
            account_name=row[4] if row[4] != "*" else None,
            database_name=row[5] if row[5] != "*" else None,
            table_name=row[6] if row[6] != "*" else None,
            range_value=row[7],
            range_unit=row[8],
        )


class TransactionPitrManager(PitrManager):
    """PitrManager for use within transactions"""

    def __init__(self, client, transaction_wrapper):
        """Initialize TransactionPitrManager with client and transaction wrapper"""
        super().__init__(client)
        self._transaction_wrapper = transaction_wrapper

    def create_cluster_pitr(self, name: str, range_value: int = 1, range_unit: str = "d") -> Pitr:
        """Create cluster PITR within transaction"""
        return self._create_pitr_with_executor("cluster", name, range_value, range_unit)

    def create_account_pitr(
        self,
        name: str,
        account_name: Optional[str] = None,
        range_value: int = 1,
        range_unit: str = "d",
    ) -> Pitr:
        """Create account PITR within transaction"""
        return self._create_pitr_with_executor("account", name, range_value, range_unit, account_name)

    def create_database_pitr(self, name: str, database_name: str, range_value: int = 1, range_unit: str = "d") -> Pitr:
        """Create database PITR within transaction"""
        return self._create_pitr_with_executor("database", name, range_value, range_unit, None, database_name)

    def create_table_pitr(
        self,
        name: str,
        database_name: str,
        table_name: str,
        range_value: int = 1,
        range_unit: str = "d",
    ) -> Pitr:
        """Create table PITR within transaction"""
        return self._create_pitr_with_executor("table", name, range_value, range_unit, None, database_name, table_name)

    def _create_pitr_with_executor(
        self,
        level: str,
        name: str,
        range_value: int,
        range_unit: str,
        account_name: Optional[str] = None,
        database_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> Pitr:
        """Create PITR with custom executor (for transaction support)"""
        try:
            self._validate_range(range_value, range_unit)

            if level == "cluster":
                sql = (
                    f"CREATE PITR {self._client._escape_identifier(name)} " f"FOR CLUSTER RANGE {range_value} '{range_unit}'"
                )
            elif level == "account":
                if account_name:
                    sql = (
                        f"CREATE PITR {self._client._escape_identifier(name)} "
                        f"FOR ACCOUNT {self._client._escape_identifier(account_name)} "
                        f"RANGE {range_value} '{range_unit}'"
                    )
                else:
                    sql = (
                        f"CREATE PITR {self._client._escape_identifier(name)} "
                        f"FOR ACCOUNT RANGE {range_value} '{range_unit}'"
                    )
            elif level == "database":
                sql = (
                    f"CREATE PITR {self._client._escape_identifier(name)} "
                    f"FOR DATABASE {self._client._escape_identifier(database_name)} "
                    f"RANGE {range_value} '{range_unit}'"
                )
            elif level == "table":
                sql = (
                    f"CREATE PITR {self._client._escape_identifier(name)} "
                    f"FOR TABLE {self._client._escape_identifier(database_name)} "
                    f"TABLE {self._client._escape_identifier(table_name)} "
                    f"RANGE {range_value} '{range_unit}'"
                )
            else:
                raise PitrError(f"Invalid PITR level: {level}") from None

            result = self._transaction_wrapper.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create {level} PITR '{name}'") from None

            return self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create {level} PITR '{name}': {e}") from None

    def get(self, name: str) -> Pitr:
        """Get PITR within transaction"""
        try:
            sql = f"SHOW PITR WHERE pitr_name = {self._client._escape_string(name)}"
            result = self._transaction_wrapper.execute(sql)

            if not result or not result.rows:
                raise PitrError(f"PITR '{name}' not found") from None

            row = result.rows[0]
            return self._row_to_pitr(row)

        except Exception as e:
            raise PitrError(f"Failed to get PITR '{name}': {e}") from None

    def list(
        self,
        level: Optional[str] = None,
        account_name: Optional[str] = None,
        database_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> List[Pitr]:
        """List PITRs within transaction"""
        try:
            conditions = []

            if level:
                conditions.append(f"pitr_level = {self._client._escape_string(level)}")
            if account_name:
                conditions.append(f"account_name = {self._client._escape_string(account_name)}")
            if database_name:
                conditions.append(f"database_name = {self._client._escape_string(database_name)}")
            if table_name:
                conditions.append(f"table_name = {self._client._escape_string(table_name)}")

            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
            else:
                where_clause = ""

            sql = f"SHOW PITR{where_clause}"
            result = self._transaction_wrapper.execute(sql)

            if not result or not result.rows:
                return []

            return [self._row_to_pitr(row) for row in result.rows]

        except Exception as e:
            raise PitrError(f"Failed to list PITRs: {e}") from None

    def alter(self, name: str, range_value: int, range_unit: str) -> Pitr:
        """Alter PITR within transaction"""
        try:
            self._validate_range(range_value, range_unit)

            sql = f"ALTER PITR {self._client._escape_identifier(name)} " f"RANGE {range_value} '{range_unit}'"

            result = self._transaction_wrapper.execute(sql)
            if result is None:
                raise PitrError(f"Failed to alter PITR '{name}'") from None

            return self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to alter PITR '{name}': {e}") from None

    def delete(self, name: str) -> bool:
        """Delete PITR within transaction"""
        try:
            sql = f"DROP PITR {self._client._escape_identifier(name)}"
            result = self._transaction_wrapper.execute(sql)
            return result is not None

        except Exception as e:
            raise PitrError(f"Failed to delete PITR '{name}': {e}") from None
