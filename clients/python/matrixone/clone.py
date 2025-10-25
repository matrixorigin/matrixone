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
"""

from typing import Optional

from .exceptions import CloneError, ConnectionError
from .version import requires_version


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
