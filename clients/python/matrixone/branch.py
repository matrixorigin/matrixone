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
MatrixOne Branch Management

Unified branch management supporting sync/async and client/session executors.
"""

from typing import Optional, Union, Any, Dict, List, Type
from enum import Enum

from .exceptions import BranchError, ConnectionError
from .version import requires_version

try:
    from sqlalchemy.orm import DeclarativeMeta

    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    DeclarativeMeta = type  # Fallback type


class DiffOutput(str, Enum):
    """Diff output format options"""

    ROWS = 'rows'
    COUNT = 'count'


class MergeConflictStrategy(str, Enum):
    """Merge conflict resolution strategies"""

    SKIP = 'skip'
    ACCEPT = 'accept'


class BaseBranchManager:
    """
    Base branch manager with shared logic for sync and async implementations.

    This base class contains all the SQL building logic that is shared between
    sync and async implementations. Subclasses only need to implement the
    execution methods.
    """

    def __init__(self, client, executor=None):
        """
        Initialize base branch manager.

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

    def _get_table_name(self, table: Union[str, Type]) -> str:
        """
        Extract table name from string or ORM model.

        Supports:
        - "table_name"
        - "db.table_name"
        - TableModel (SQLAlchemy ORM)

        Args:
            table: Table name string or ORM model class

        Returns:
            Fully qualified table name string
        """
        if isinstance(table, str):
            return table

        # Handle SQLAlchemy ORM model
        if SQLALCHEMY_AVAILABLE and hasattr(table, '__tablename__'):
            table_name = table.__tablename__
            # Check if model has __table_args__ with schema
            if hasattr(table, '__table_args__'):
                table_args = table.__table_args__
                if isinstance(table_args, dict) and 'schema' in table_args:
                    return f"{table_args['schema']}.{table_name}"
            return table_name

        raise BranchError(f"Invalid table parameter: {table}. Expected string or ORM model.")

    def _build_create_table_branch_sql(
        self,
        target_table: str,
        source_table: str,
        snapshot_name: Optional[str] = None,
    ) -> str:
        """Build DATA BRANCH CREATE TABLE SQL statement"""
        if not target_table or not isinstance(target_table, str):
            raise BranchError("target_table must be a non-empty string")
        if not source_table or not isinstance(source_table, str):
            raise BranchError("source_table must be a non-empty string")
        if snapshot_name is not None and not isinstance(snapshot_name, str):
            raise BranchError("snapshot_name must be a string or None")

        if snapshot_name:
            return f"data branch create table {target_table} from {source_table}{{snapshot=\"{snapshot_name}\"}}"
        else:
            return f"data branch create table {target_table} from {source_table}"

    def _build_delete_table_branch_sql(self, table: str) -> str:
        """Build DATA BRANCH DELETE TABLE SQL statement"""
        if not table or not isinstance(table, str):
            raise BranchError("table must be a non-empty string")
        return f"data branch delete table {table}"

    def _build_create_database_branch_sql(
        self,
        target_database: str,
        source_database: str,
    ) -> str:
        """Build DATA BRANCH CREATE DATABASE SQL statement"""
        if not target_database or not isinstance(target_database, str):
            raise BranchError("target_database must be a non-empty string")
        if not source_database or not isinstance(source_database, str):
            raise BranchError("source_database must be a non-empty string")
        return f"data branch create database {target_database} from {source_database}"

    def _build_delete_database_branch_sql(self, database: str) -> str:
        """Build DATA BRANCH DELETE DATABASE SQL statement"""
        if not database or not isinstance(database, str):
            raise BranchError("database must be a non-empty string")
        return f"data branch delete database {database}"

    def _build_diff_table_sql(
        self,
        table: str,
        against_table: str,
        snapshot_name: Optional[str] = None,
        output: DiffOutput = DiffOutput.ROWS,
    ) -> str:
        """Build DATA BRANCH DIFF SQL statement"""
        base_sql = f"data branch diff {table} against {against_table}"

        if snapshot_name:
            base_sql += f"{{snapshot=\"{snapshot_name}\"}}"

        if output == DiffOutput.COUNT:
            base_sql += " output count"

        return base_sql

    def _build_merge_table_sql(
        self,
        source_table: str,
        target_table: str,
        on_conflict: Union[str, MergeConflictStrategy] = "skip",
    ) -> str:
        """Build DATA BRANCH MERGE SQL statement"""
        if not source_table or not isinstance(source_table, str):
            raise BranchError("source_table must be a non-empty string")
        if not target_table or not isinstance(target_table, str):
            raise BranchError("target_table must be a non-empty string")

        # Convert enum to string if needed
        if isinstance(on_conflict, MergeConflictStrategy):
            on_conflict = on_conflict.value

        if on_conflict not in ("skip", "accept"):
            raise BranchError(f"on_conflict must be 'skip' or 'accept', got '{on_conflict}'")
        return f"data branch merge {source_table} into {target_table} when conflict {on_conflict}"


class BranchManager(BaseBranchManager):
    """
    Synchronous branch management for MatrixOne database operations.

    This class provides comprehensive branch functionality for Git-style version control,
    enabling table and database branching, diffing, and merging operations.

    **Version Requirement:** MatrixOne 3.0.5 or higher

    Key Features:

    - **Table branching**: Create branches from tables with optional snapshots
    - **Database branching**: Create branches from entire databases
    - **Diff operations**: Compare differences between tables
    - **Merge operations**: Merge branches with conflict resolution (skip/accept)
    - **Snapshot support**: Branch from specific point-in-time snapshots
    - **Transaction-aware**: Full integration with transaction contexts via executor pattern
    - **Flexible naming**: Support for database-qualified table names

    Executor Pattern:

    - If executor is None, uses self.client.execute (default client-level executor)
    - If executor is provided (e.g., session), uses executor.execute (transaction-aware)
    - This allows the same logic to work in both client and session contexts
    - All operations can participate in transactions when used via session

    Usage Examples::

        from matrixone import Client

        client = Client()
        client.connect(database='test')

        # Create table branch
        client.branch.create_table_branch(
            target_table='users_branch',
            source_table='users'
        )

        # Create table branch from snapshot
        client.branch.create_table_branch(
            target_table='users_historical',
            source_table='users',
            snapshot_name='daily_backup_2024_01_01'
        )

        # Create database branch
        client.branch.create_database_branch(
            target_database='dev_db',
            source_database='production'
        )

        # Simplified API (auto-detects table vs database)
        client.branch.create('users_branch', 'users')
        client.branch.create('dev_db', 'production', database=True)

        # Compare tables
        diffs = client.branch.diff_table(
            table='users_branch',
            against_table='users'
        )

        # Merge with conflict resolution
        client.branch.merge_table(
            source_table='users_branch',
            target_table='users',
            conflict_strategy='accept'
        )

        # Delete branches
        client.branch.delete_table_branch('users_branch')
        client.branch.delete_database_branch('dev_db')

        # Using within a transaction (all operations are atomic)
        with client.session() as session:
            session.branch.create_table_branch('branch1', 'source_table')
            session.branch.create_table_branch('branch2', 'source_table')
            # Both operations succeed or fail together

    See Also:

        - CloneManager: For table/database cloning
        - SnapshotManager: For creating snapshots before branching
    """

    @requires_version(
        min_version="3.0.5",
        feature_name="table_branching",
        description="Table branching functionality",
        alternative="Use CREATE TABLE and data migration instead",
    )
    def create_table_branch(
        self,
        target_table: str,
        source_table: str,
        snapshot_name: Optional[str] = None,
    ) -> None:
        """
        Create a table branch from another table.

        Args:
            target_table: Target table name (can include database: db.table)
            source_table: Source table name (can include database: db.table)
            snapshot_name: Optional snapshot name for point-in-time branch

        Raises:
            ConnectionError: If not connected to database
            BranchError: If branch creation fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_create_table_branch_sql(target_table, source_table, snapshot_name)

        try:
            self._get_executor().execute(sql)
        except Exception as e:
            raise BranchError(f"Failed to create table branch: {e}") from e

    def delete_table_branch(self, table: str) -> None:
        """
        Delete a table branch.

        Args:
            table: Table name to delete (can include database: db.table)

        Raises:
            ConnectionError: If not connected to database
            BranchError: If branch deletion fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_delete_table_branch_sql(table)

        try:
            self._get_executor().execute(sql)
        except Exception as e:
            raise BranchError(f"Failed to delete table branch: {e}") from e

    @requires_version(
        min_version="3.0.5",
        feature_name="database_branching",
        description="Database branching functionality",
        alternative="Use CREATE DATABASE and data migration instead",
    )
    def create_database_branch(
        self,
        target_database: str,
        source_database: str,
    ) -> None:
        """
        Create a database branch from another database.

        Args:
            target_database: Target database name
            source_database: Source database name

        Raises:
            ConnectionError: If not connected to database
            BranchError: If branch creation fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_create_database_branch_sql(target_database, source_database)

        try:
            self._get_executor().execute(sql)
        except Exception as e:
            raise BranchError(f"Failed to create database branch: {e}") from e

    def delete_database_branch(self, database: str) -> None:
        """
        Delete a database branch.

        Args:
            database: Database name to delete

        Raises:
            ConnectionError: If not connected to database
            BranchError: If branch deletion fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_delete_database_branch_sql(database)

        try:
            self._get_executor().execute(sql)
        except Exception as e:
            raise BranchError(f"Failed to delete database branch: {e}") from e

    @requires_version(
        min_version="3.0.5",
        feature_name="table_diffing",
        description="Table diffing functionality",
        alternative="Use manual comparison instead",
    )
    def diff_table(
        self,
        table: str,
        against_table: str,
        snapshot_name: Optional[str] = None,
        output: DiffOutput = DiffOutput.ROWS,
    ) -> List[Dict[str, Any]]:
        """
        Compare differences between tables.

        Args:
            table: Source table name (can include database: db.table)
            against_table: Target table name for comparison (can include database: db.table)
            snapshot_name: Optional snapshot name for point-in-time comparison
            output: Output format - ROWS for detailed differences, COUNT for count only

        Returns:
            List of differences between tables

        Raises:
            ConnectionError: If not connected to database
            BranchError: If diff operation fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_diff_table_sql(table, against_table, snapshot_name, output)

        try:
            result = self._get_executor().execute(sql)
            return result.fetchall()
        except Exception as e:
            raise BranchError(f"Failed to diff tables: {e}") from e

    @requires_version(
        min_version="3.0.5",
        feature_name="table_merging",
        description="Table merging functionality",
        alternative="Use manual merge instead",
    )
    def merge_table(
        self,
        source_table: str,
        target_table: str,
        on_conflict: Union[str, MergeConflictStrategy] = "skip",
    ) -> None:
        """
        Merge one table into another with conflict resolution.

        Args:
            source_table: Source table name (can include database: db.table)
            target_table: Target table name (can include database: db.table)
            on_conflict: How to handle conflicts - "skip" to skip conflicting rows,
                        "accept" to accept source values

        Raises:
            ConnectionError: If not connected to database
            BranchError: If merge operation fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_merge_table_sql(source_table, target_table, on_conflict)

        try:
            self._get_executor().execute(sql)
        except Exception as e:
            raise BranchError(f"Failed to merge tables: {e}") from e

    # Simplified API - Pythonic method names
    def create(
        self,
        target: Union[str, Type],
        source: Union[str, Type],
        snapshot: Optional[str] = None,
        database: bool = False,
    ) -> None:
        """
        Create a branch (table or database).

        Simplified API that auto-detects whether to create table or database branch.

        Args:
            target: Target name (string or ORM model)
            source: Source name (string or ORM model)
            snapshot: Optional snapshot name
            database: If True, create database branch; if False, create table branch

        Examples:
            # Table branch
            client.branch.create('new_table', 'source_table')
            client.branch.create('new_table', TableModel)
            client.branch.create('new_table', 'source', snapshot='snap1')

            # Database branch
            client.branch.create('new_db', 'source_db', database=True)
        """
        if database:
            self.create_database_branch(str(target), str(source))
        else:
            target_name = self._get_table_name(target)
            source_name = self._get_table_name(source)
            self.create_table_branch(target_name, source_name, snapshot)

    def delete(self, name: Union[str, Type], database: bool = False) -> None:
        """
        Delete a branch (table or database).

        Args:
            name: Branch name (string or ORM model)
            database: If True, delete database branch; if False, delete table branch

        Examples:
            client.branch.delete('branch_table')
            client.branch.delete(BranchModel)
            client.branch.delete('branch_db', database=True)
        """
        if database:
            self.delete_database_branch(str(name))
        else:
            table_name = self._get_table_name(name)
            self.delete_table_branch(table_name)

    def diff(
        self,
        table: Union[str, Type],
        against: Union[str, Type],
        snapshot: Optional[str] = None,
        output: DiffOutput = DiffOutput.ROWS,
    ) -> List[Dict[str, Any]]:
        """
        Compare differences between two tables.

        Args:
            table: First table (string or ORM model)
            against: Second table to compare against (string or ORM model)
            snapshot: Optional snapshot name
            output: Output format - ROWS for detailed differences, COUNT for count only

        Returns:
            List of differences

        Examples:
            diffs = client.branch.diff('table1', 'table2')
            diffs = client.branch.diff(Model1, Model2)
            diffs = client.branch.diff('table1', 'table2', snapshot='snap1')
            count = client.branch.diff('table1', 'table2', output='count')
        """
        table_name = self._get_table_name(table)
        against_name = self._get_table_name(against)
        return self.diff_table(table_name, against_name, snapshot, output)

    def merge(
        self,
        source: Union[str, Type],
        target: Union[str, Type],
        on_conflict: Union[str, MergeConflictStrategy] = "skip",
    ) -> None:
        """
        Merge source table into target table.

        Args:
            source: Source table (string or ORM model)
            target: Target table (string or ORM model)
            on_conflict: Conflict resolution strategy: 'skip' or 'accept'

        Examples:
            client.branch.merge('source', 'target')
            client.branch.merge(SourceModel, TargetModel)
            client.branch.merge('source', 'target', on_conflict='accept')
        """
        source_name = self._get_table_name(source)
        target_name = self._get_table_name(target)
        self.merge_table(source_name, target_name, on_conflict)


class AsyncBranchManager(BaseBranchManager):
    """
    Asynchronous branch management for MatrixOne database operations.

    Provides the same comprehensive branch functionality as BranchManager but with
    full async/await support for non-blocking I/O operations. Ideal for high-concurrency
    applications and async web frameworks.

    **Version Requirement:** MatrixOne 3.0.5 or higher

    Key Features:

    - **Non-blocking operations**: All branch operations use async/await
    - **Table branching**: Asynchronously create branches from tables
    - **Database branching**: Asynchronously create branches from databases
    - **Diff operations**: Asynchronously compare differences between tables
    - **Merge operations**: Asynchronously merge branches with conflict resolution
    - **Concurrent operations**: Branch multiple tables/databases concurrently
    - **Transaction-aware**: Full integration with async transaction contexts

    Executor Pattern:

    - If executor is None, uses self.client.execute (default async client-level executor)
    - If executor is provided (e.g., async session), uses executor.execute (async transaction-aware)
    - All operations are non-blocking and use async/await
    - Enables concurrent branch operations when not in a transaction

    Usage Examples::

        from matrixone import AsyncClient
        import asyncio

        async def main():
            client = AsyncClient()
            await client.connect(database='test')

            # Create table branch asynchronously
            await client.branch.create_table_branch(
                target_table='users_branch',
                source_table='users'
            )

            # Create database branch asynchronously
            await client.branch.create_database_branch(
                target_database='dev_db',
                source_database='production'
            )

            # Diff tables asynchronously
            diffs = await client.branch.diff_table(
                table='users_branch',
                against_table='users'
            )

            # Merge with conflict resolution asynchronously
            await client.branch.merge_table(
                source_table='users_branch',
                target_table='users',
                conflict_strategy='accept'
            )

            # Concurrent branching of multiple tables
            await asyncio.gather(
                client.branch.create_table_branch('branch1', 'source_table'),
                client.branch.create_table_branch('branch2', 'source_table'),
                client.branch.create_table_branch('branch3', 'source_table')
            )

            # Using within async transaction
            async with client.session() as session:
                await session.branch.create_table_branch('branch1', 'source_table')
                await session.branch.create_table_branch('branch2', 'source_table')
                # Both operations commit atomically

            await client.disconnect()

        asyncio.run(main())

    See Also:

        - AsyncCloneManager: For async clone operations
        - AsyncSnapshotManager: For async snapshot operations
        - AsyncSession: For async transaction management
    """

    @requires_version(
        min_version="3.0.5",
        feature_name="table_branching",
        description="Table branching functionality",
        alternative="Use CREATE TABLE and data migration instead",
    )
    async def create_table_branch(
        self,
        target_table: str,
        source_table: str,
        snapshot_name: Optional[str] = None,
    ) -> None:
        """
        Create a table branch from another table asynchronously.

        Args:
            target_table: Target table name (can include database: db.table)
            source_table: Source table name (can include database: db.table)
            snapshot_name: Optional snapshot name for point-in-time branch

        Raises:
            ConnectionError: If not connected to database
            BranchError: If branch creation fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_create_table_branch_sql(target_table, source_table, snapshot_name)

        try:
            await self._get_executor().execute(sql)
        except Exception as e:
            raise BranchError(f"Failed to create table branch: {e}") from e

    async def delete_table_branch(self, table: str) -> None:
        """
        Delete a table branch asynchronously.

        Args:
            table: Table name to delete (can include database: db.table)

        Raises:
            ConnectionError: If not connected to database
            BranchError: If branch deletion fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_delete_table_branch_sql(table)

        try:
            await self._get_executor().execute(sql)
        except Exception as e:
            raise BranchError(f"Failed to delete table branch: {e}") from e

    @requires_version(
        min_version="3.0.5",
        feature_name="database_branching",
        description="Database branching functionality",
        alternative="Use CREATE DATABASE and data migration instead",
    )
    async def create_database_branch(
        self,
        target_database: str,
        source_database: str,
    ) -> None:
        """
        Create a database branch from another database asynchronously.

        Args:
            target_database: Target database name
            source_database: Source database name

        Raises:
            ConnectionError: If not connected to database
            BranchError: If branch creation fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_create_database_branch_sql(target_database, source_database)

        try:
            await self._get_executor().execute(sql)
        except Exception as e:
            raise BranchError(f"Failed to create database branch: {e}") from e

    async def delete_database_branch(self, database: str) -> None:
        """
        Delete a database branch asynchronously.

        Args:
            database: Database name to delete

        Raises:
            ConnectionError: If not connected to database
            BranchError: If branch deletion fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_delete_database_branch_sql(database)

        try:
            await self._get_executor().execute(sql)
        except Exception as e:
            raise BranchError(f"Failed to delete database branch: {e}") from e

    @requires_version(
        min_version="3.0.5",
        feature_name="table_diffing",
        description="Table diffing functionality",
        alternative="Use manual comparison instead",
    )
    async def diff_table(
        self,
        table: str,
        against_table: str,
        snapshot_name: Optional[str] = None,
        output: DiffOutput = DiffOutput.ROWS,
    ) -> List[Dict[str, Any]]:
        """
        Compare differences between tables asynchronously.

        Args:
            table: Source table name (can include database: db.table)
            against_table: Target table name for comparison (can include database: db.table)
            snapshot_name: Optional snapshot name for point-in-time comparison
            output: Output format - ROWS for detailed differences, COUNT for count only

        Returns:
            List of differences between tables

        Raises:
            ConnectionError: If not connected to database
            BranchError: If diff operation fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_diff_table_sql(table, against_table, snapshot_name, output)

        try:
            result = await self._get_executor().execute(sql)
            return result.fetchall()
        except Exception as e:
            raise BranchError(f"Failed to diff tables: {e}") from e

    @requires_version(
        min_version="3.0.5",
        feature_name="table_merging",
        description="Table merging functionality",
        alternative="Use manual merge instead",
    )
    async def merge_table(
        self,
        source_table: str,
        target_table: str,
        on_conflict: Union[str, MergeConflictStrategy] = "skip",
    ) -> None:
        """
        Merge one table into another with conflict resolution asynchronously.

        Args:
            source_table: Source table name (can include database: db.table)
            target_table: Target table name (can include database: db.table)
            on_conflict: How to handle conflicts - "skip" to skip conflicting rows,
                        "accept" to accept source values

        Raises:
            ConnectionError: If not connected to database
            BranchError: If merge operation fails
        """
        if not self.client._engine:
            raise ConnectionError("Not connected to database")

        sql = self._build_merge_table_sql(source_table, target_table, on_conflict)

        try:
            await self._get_executor().execute(sql)
        except Exception as e:
            raise BranchError(f"Failed to merge tables: {e}") from e

    # Simplified API - Pythonic method names
    async def create(
        self,
        target: Union[str, Type],
        source: Union[str, Type],
        snapshot: Optional[str] = None,
        database: bool = False,
    ) -> None:
        """
        Create a branch (table or database) asynchronously.

        Args:
            target: Target name (string or ORM model)
            source: Source name (string or ORM model)
            snapshot: Optional snapshot name
            database: If True, create database branch; if False, create table branch
        """
        if database:
            await self.create_database_branch(str(target), str(source))
        else:
            target_name = self._get_table_name(target)
            source_name = self._get_table_name(source)
            await self.create_table_branch(target_name, source_name, snapshot)

    async def delete(self, name: Union[str, Type], database: bool = False) -> None:
        """
        Delete a branch (table or database) asynchronously.

        Args:
            name: Branch name (string or ORM model)
            database: If True, delete database branch; if False, delete table branch
        """
        if database:
            await self.delete_database_branch(str(name))
        else:
            table_name = self._get_table_name(name)
            await self.delete_table_branch(table_name)

    async def diff(
        self,
        table: Union[str, Type],
        against: Union[str, Type],
        snapshot: Optional[str] = None,
        output: DiffOutput = DiffOutput.ROWS,
    ) -> List[Dict[str, Any]]:
        """
        Compare differences between two tables asynchronously.

        Args:
            table: First table (string or ORM model)
            against: Second table to compare against (string or ORM model)
            snapshot: Optional snapshot name
            output: Output format - ROWS for detailed differences, COUNT for count only

        Returns:
            List of differences
        """
        table_name = self._get_table_name(table)
        against_name = self._get_table_name(against)
        return await self.diff_table(table_name, against_name, snapshot, output)

    async def merge(
        self,
        source: Union[str, Type],
        target: Union[str, Type],
        on_conflict: Union[str, MergeConflictStrategy] = "skip",
    ) -> None:
        """
        Merge source table into target table asynchronously.

        Args:
            source: Source table (string or ORM model)
            target: Target table (string or ORM model)
            on_conflict: Conflict resolution strategy: 'skip' or 'accept'
        """
        source_name = self._get_table_name(source)
        target_name = self._get_table_name(target)
        await self.merge_table(source_name, target_name, on_conflict)
