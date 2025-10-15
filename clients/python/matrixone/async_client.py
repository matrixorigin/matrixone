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
MatrixOne Async Client - Asynchronous implementation
"""

try:
    import aiomysql
except ImportError:
    aiomysql = None

try:
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
except ImportError:
    create_async_engine = None
    AsyncEngine = None
    text = None

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from .account import Account, User
from .async_metadata_manager import AsyncMetadataManager
from .async_vector_index_manager import AsyncVectorManager
from .base_client import BaseMatrixOneClient, BaseMatrixOneExecutor
from .connection_hooks import ConnectionAction, ConnectionHook, create_connection_hook
from .exceptions import (
    AccountError,
    ConnectionError,
    MoCtlError,
    PitrError,
    PubSubError,
    QueryError,
    RestoreError,
    SnapshotError,
)
from .logger import MatrixOneLogger, create_default_logger
from .pitr import Pitr
from .pubsub import Publication, Subscription
from .snapshot import Snapshot, SnapshotLevel


class AsyncResultSet:
    """Async result set wrapper for query results"""

    def __init__(self, columns: List[str], rows: List[Tuple], affected_rows: int = 0):
        self.columns = columns
        self.rows = rows
        self.affected_rows = affected_rows
        self._cursor = 0  # Track current position in result set

    def fetchall(self) -> List[Tuple]:
        """Fetch all remaining rows"""
        remaining_rows = self.rows[self._cursor :]
        self._cursor = len(self.rows)
        return remaining_rows

    def fetchone(self) -> Optional[Tuple]:
        """Fetch one row"""
        if self._cursor < len(self.rows):
            row = self.rows[self._cursor]
            self._cursor += 1
            return row
        return None

    def fetchmany(self, size: int = 1) -> List[Tuple]:
        """Fetch many rows"""
        start = self._cursor
        end = min(start + size, len(self.rows))
        rows = self.rows[start:end]
        self._cursor = end
        return rows

    def scalar(self) -> Any:
        """Get scalar value (first column of first row)"""
        if self.rows and self.columns:
            return self.rows[0][0]
        return None

    def keys(self):
        """Get column names"""
        return iter(self.columns)

    def __iter__(self):
        return iter(self.rows)

    def __len__(self):
        return len(self.rows)


class AsyncSnapshotManager:
    """Async snapshot manager"""

    def __init__(self, client):
        self.client = client

    async def create(
        self,
        name: str,
        level: Union[str, SnapshotLevel],
        database: Optional[str] = None,
        table: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Snapshot:
        """Create snapshot asynchronously"""
        # Convert string level to enum if needed
        if isinstance(level, str):
            level = SnapshotLevel(level.lower())

        # Build SQL based on level
        if level == SnapshotLevel.CLUSTER:
            sql = f"CREATE SNAPSHOT {name} FOR CLUSTER"
        elif level == SnapshotLevel.ACCOUNT:
            sql = f"CREATE SNAPSHOT {name} FOR ACCOUNT"
        elif level == SnapshotLevel.DATABASE:
            if not database:
                raise SnapshotError("Database name is required for database level snapshot")
            sql = f"CREATE SNAPSHOT {name} FOR DATABASE {database}"
        elif level == SnapshotLevel.TABLE:
            if not database or not table:
                raise SnapshotError("Database and table names are required for table level snapshot")
            sql = f"CREATE SNAPSHOT {name} FOR TABLE {database} {table}"
        else:
            raise SnapshotError(f"Invalid snapshot level: {level}")

        if description:
            sql += f" COMMENT '{description}'"

        await self.client.execute(sql)

        # Return snapshot object
        import datetime

        return Snapshot(name, level, datetime.datetime.now(), description, database, table)

    async def get(self, name: str) -> Snapshot:
        """Get snapshot asynchronously"""
        # Use mo_catalog.mo_snapshots table like sync client
        sql = """
            SELECT sname, ts, level, account_name, database_name, table_name
            FROM mo_catalog.mo_snapshots
            WHERE sname = :name
        """
        result = await self.client.execute(sql, {"name": name})

        if not result.rows:
            raise SnapshotError(f"Snapshot '{name}' not found")

        row = result.rows[0]
        # Convert timestamp to datetime
        from datetime import datetime

        timestamp = datetime.fromtimestamp(row[1] / 1000000000)  # Convert nanoseconds to seconds

        # Convert level string to enum
        level_str = row[2]
        try:
            level = SnapshotLevel(level_str.lower())
        except ValueError:
            level = level_str  # Fallback to string for backward compatibility

        return Snapshot(row[0], level, timestamp, None, row[4], row[5])

    async def list(self) -> List[Snapshot]:
        """List all snapshots asynchronously"""
        # Use mo_catalog.mo_snapshots table like sync client
        sql = """
            SELECT sname, ts, level, account_name, database_name, table_name
            FROM mo_catalog.mo_snapshots
            ORDER BY ts DESC
        """
        result = await self.client.execute(sql)

        snapshots = []
        for row in result.rows:
            # Convert timestamp to datetime
            timestamp = datetime.fromtimestamp(row[1] / 1000000000)  # Convert nanoseconds to seconds

            # Convert level string to enum
            level_str = row[2]
            try:
                level = SnapshotLevel(level_str.lower())
            except ValueError:
                level = level_str  # Fallback to string for backward compatibility

            snapshots.append(Snapshot(row[0], level, timestamp, None, row[4], row[5]))

        return snapshots

    async def delete(self, name: str) -> None:
        """Delete snapshot asynchronously"""
        sql = f"DROP SNAPSHOT {name}"
        await self.client.execute(sql)

    async def exists(self, name: str) -> bool:
        """Check if snapshot exists asynchronously"""
        try:
            await self.get(name)
            return True
        except SnapshotError:
            return False


class AsyncCloneManager:
    """Async clone manager"""

    def __init__(self, client):
        self.client = client

    async def clone_database(
        self,
        target_db: str,
        source_db: str,
        snapshot_name: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> None:
        """Clone database asynchronously"""
        if_not_exists_clause = "IF NOT EXISTS" if if_not_exists else ""

        if snapshot_name:
            sql = f"CREATE DATABASE {target_db} {if_not_exists_clause} CLONE {source_db} FOR SNAPSHOT '{snapshot_name}'"
        else:
            sql = f"CREATE DATABASE {target_db} {if_not_exists_clause} CLONE {source_db}"

        await self.client.execute(sql)

    async def clone_table(
        self,
        target_table: str,
        source_table: str,
        snapshot_name: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> None:
        """Clone table asynchronously"""
        if_not_exists_clause = "IF NOT EXISTS" if if_not_exists else ""

        if snapshot_name:
            sql = (
                f"CREATE TABLE {target_table} {if_not_exists_clause} " f"CLONE {source_table} FOR SNAPSHOT '{snapshot_name}'"
            )
        else:
            sql = f"CREATE TABLE {target_table} {if_not_exists_clause} CLONE {source_table}"

        await self.client.execute(sql)

    async def clone_database_with_snapshot(
        self, target_db: str, source_db: str, snapshot_name: str, if_not_exists: bool = False
    ) -> None:
        """Clone database with snapshot asynchronously"""
        await self.clone_database(target_db, source_db, snapshot_name, if_not_exists)

    async def clone_table_with_snapshot(
        self, target_table: str, source_table: str, snapshot_name: str, if_not_exists: bool = False
    ) -> None:
        """Clone table with snapshot asynchronously"""
        await self.clone_table(target_table, source_table, snapshot_name, if_not_exists)


class AsyncPubSubManager:
    """Async manager for publish-subscribe operations"""

    def __init__(self, client):
        self.client = client

    async def create_database_publication(self, name: str, database: str, account: str) -> Publication:
        """Create database-level publication asynchronously"""
        try:
            sql = (
                f"CREATE PUBLICATION {self.client._escape_identifier(name)} "
                f"DATABASE {self.client._escape_identifier(database)} "
                f"ACCOUNT {self.client._escape_identifier(account)}"
            )

            result = await self.client.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create database publication '{name}'")

            return await self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to create database publication '{name}': {e}")

    async def create_table_publication(self, name: str, database: str, table: str, account: str) -> Publication:
        """Create table-level publication asynchronously"""
        try:
            sql = (
                f"CREATE PUBLICATION {self.client._escape_identifier(name)} "
                f"DATABASE {self.client._escape_identifier(database)} "
                f"TABLE {self.client._escape_identifier(table)} "
                f"ACCOUNT {self.client._escape_identifier(account)}"
            )

            result = await self.client.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create table publication '{name}'")

            return await self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to create table publication '{name}': {e}")

    async def get_publication(self, name: str) -> Publication:
        """Get publication by name asynchronously"""
        try:
            # SHOW PUBLICATIONS doesn't support WHERE clause, so we need to list all and filter
            sql = "SHOW PUBLICATIONS"
            result = await self.client.execute(sql)

            if not result or not result.rows:
                raise PubSubError(f"Publication '{name}' not found")

            # Find publication with matching name
            for row in result.rows:
                if row[0] == name:  # publication name is in first column
                    return self._row_to_publication(row)

            raise PubSubError(f"Publication '{name}' not found")

        except Exception as e:
            raise PubSubError(f"Failed to get publication '{name}': {e}")

    async def list_publications(self, account: Optional[str] = None, database: Optional[str] = None) -> List[Publication]:
        """List publications with optional filters asynchronously"""
        try:
            # SHOW PUBLICATIONS doesn't support WHERE clause, so we need to list all and filter
            sql = "SHOW PUBLICATIONS"
            result = await self.client.execute(sql)

            if not result or not result.rows:
                return []

            publications = []
            for row in result.rows:
                pub = self._row_to_publication(row)

                # Apply filters
                if account and account not in pub.sub_account:
                    continue
                if database and pub.database != database:
                    continue

                publications.append(pub)

            return publications

        except Exception as e:
            raise PubSubError(f"Failed to list publications: {e}")

    async def alter_publication(
        self,
        name: str,
        account: Optional[str] = None,
        database: Optional[str] = None,
        table: Optional[str] = None,
    ) -> Publication:
        """Alter publication asynchronously"""
        try:
            # Build ALTER PUBLICATION statement
            parts = [f"ALTER PUBLICATION {self.client._escape_identifier(name)}"]

            if account:
                parts.append(f"ACCOUNT {self.client._escape_identifier(account)}")
            if database:
                parts.append(f"DATABASE {self.client._escape_identifier(database)}")
            if table:
                parts.append(f"TABLE {self.client._escape_identifier(table)}")

            sql = " ".join(parts)
            result = await self.client.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to alter publication '{name}'")

            return await self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to alter publication '{name}': {e}")

    async def drop_publication(self, name: str) -> bool:
        """Drop publication asynchronously"""
        try:
            sql = f"DROP PUBLICATION {self.client._escape_identifier(name)}"
            result = await self.client.execute(sql)
            return result is not None

        except Exception as e:
            raise PubSubError(f"Failed to drop publication '{name}': {e}")

    async def show_create_publication(self, name: str) -> str:
        """Show CREATE PUBLICATION statement for a publication asynchronously"""
        try:
            sql = f"SHOW CREATE PUBLICATION {self.client._escape_identifier(name)}"
            result = await self.client.execute(sql)

            if not result or not result.rows:
                raise PubSubError(f"Publication '{name}' not found")

            # The result should contain the CREATE statement
            # Assuming the CREATE statement is in the first column
            return result.rows[0][0]

        except Exception as e:
            raise PubSubError(f"Failed to show create publication '{name}': {e}")

    async def create_subscription(
        self, subscription_name: str, publication_name: str, publisher_account: str
    ) -> Subscription:
        """Create subscription from publication asynchronously"""
        try:
            sql = (
                f"CREATE DATABASE {self.client._escape_identifier(subscription_name)} "
                f"FROM {self.client._escape_identifier(publisher_account)} "
                f"PUBLICATION {self.client._escape_identifier(publication_name)}"
            )

            result = await self.client.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create subscription '{subscription_name}'")

            return await self.get_subscription(subscription_name)

        except Exception as e:
            raise PubSubError(f"Failed to create subscription '{subscription_name}': {e}")

    async def get_subscription(self, name: str) -> Subscription:
        """Get subscription by name asynchronously"""
        try:
            # SHOW SUBSCRIPTIONS doesn't support WHERE clause, so we need to list all and filter
            sql = "SHOW SUBSCRIPTIONS"
            result = await self.client.execute(sql)

            if not result or not result.rows:
                raise PubSubError(f"Subscription '{name}' not found")

            # Find subscription with matching name
            for row in result.rows:
                if row[6] == name:  # sub_name is in 7th column (index 6)
                    return self._row_to_subscription(row)

            raise PubSubError(f"Subscription '{name}' not found")

        except Exception as e:
            raise PubSubError(f"Failed to get subscription '{name}': {e}")

    async def list_subscriptions(
        self, pub_account: Optional[str] = None, pub_database: Optional[str] = None
    ) -> List[Subscription]:
        """List subscriptions with optional filters asynchronously"""
        try:
            conditions = []

            if pub_account:
                conditions.append(f"pub_account = {self.client._escape_string(pub_account)}")
            if pub_database:
                conditions.append(f"pub_database = {self.client._escape_string(pub_database)}")

            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
            else:
                where_clause = ""

            sql = f"SHOW SUBSCRIPTIONS{where_clause}"
            result = await self.client.execute(sql)

            if not result or not result.rows:
                return []

            return [self._row_to_subscription(row) for row in result.rows]

        except Exception as e:
            raise PubSubError(f"Failed to list subscriptions: {e}")

    def _row_to_publication(self, row: tuple) -> Publication:
        """Convert database row to Publication object"""
        # Expected columns: publication, database, tables, sub_account, subscribed_accounts,
        # create_time, update_time, comments
        # Based on MatrixOne official documentation:
        # https://docs.matrixorigin.cn/en/v25.2.2.2/MatrixOne/Reference/SQL-Reference/Other/SHOW-Statements/show-publications/
        return Publication(
            name=row[0],  # publication
            database=row[1],  # database
            tables=row[2],  # tables
            sub_account=row[3],  # sub_account
            subscribed_accounts=row[4],  # subscribed_accounts
            created_time=row[5] if len(row) > 5 else None,  # create_time
            update_time=row[6] if len(row) > 6 else None,  # update_time
            comments=row[7] if len(row) > 7 else None,  # comments
        )

    def _row_to_subscription(self, row: tuple) -> Subscription:
        """Convert database row to Subscription object"""
        # Expected columns: pub_name, pub_account, pub_database, pub_tables, pub_comment,
        # pub_time, sub_name, sub_time, status
        return Subscription(
            pub_name=row[0],
            pub_account=row[1],
            pub_database=row[2],
            pub_tables=row[3],
            pub_comment=row[4] if len(row) > 4 else None,
            pub_time=row[5] if len(row) > 5 else None,
            sub_name=row[6] if len(row) > 6 else None,
            sub_time=row[7] if len(row) > 7 else None,
            status=row[8] if len(row) > 8 else 0,
        )


class AsyncPitrManager:
    """Async manager for PITR operations"""

    def __init__(self, client):
        self.client = client

    async def create_cluster_pitr(self, name: str, range_value: int = 1, range_unit: str = "d") -> Pitr:
        """Create cluster-level PITR asynchronously"""
        try:
            self._validate_range(range_value, range_unit)

            sql = f"CREATE PITR {self.client._escape_identifier(name)} " f"FOR CLUSTER RANGE {range_value} '{range_unit}'"

            result = await self.client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create cluster PITR '{name}'")

            return await self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create cluster PITR '{name}': {e}")

    async def create_account_pitr(
        self,
        name: str,
        account_name: Optional[str] = None,
        range_value: int = 1,
        range_unit: str = "d",
    ) -> Pitr:
        """Create account-level PITR asynchronously"""
        try:
            self._validate_range(range_value, range_unit)

            if account_name:
                sql = (
                    f"CREATE PITR {self.client._escape_identifier(name)} "
                    f"FOR ACCOUNT {self.client._escape_identifier(account_name)} "
                    f"RANGE {range_value} '{range_unit}'"
                )
            else:
                sql = (
                    f"CREATE PITR {self.client._escape_identifier(name)} " f"FOR ACCOUNT RANGE {range_value} '{range_unit}'"
                )

            result = await self.client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create account PITR '{name}'")

            return await self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create account PITR '{name}': {e}")

    async def create_database_pitr(self, name: str, database_name: str, range_value: int = 1, range_unit: str = "d") -> Pitr:
        """Create database-level PITR asynchronously"""
        try:
            self._validate_range(range_value, range_unit)

            sql = (
                f"CREATE PITR {self.client._escape_identifier(name)} "
                f"FOR DATABASE {self.client._escape_identifier(database_name)} "
                f"RANGE {range_value} '{range_unit}'"
            )

            result = await self.client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create database PITR '{name}'")

            return await self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create database PITR '{name}': {e}")

    async def create_table_pitr(
        self,
        name: str,
        database_name: str,
        table_name: str,
        range_value: int = 1,
        range_unit: str = "d",
    ) -> Pitr:
        """Create table-level PITR asynchronously"""
        try:
            self._validate_range(range_value, range_unit)

            sql = (
                f"CREATE PITR {self.client._escape_identifier(name)} "
                f"FOR TABLE {self.client._escape_identifier(database_name)} "
                f"{self.client._escape_identifier(table_name)} "
                f"RANGE {range_value} '{range_unit}'"
            )

            result = await self.client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create table PITR '{name}'")

            return await self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create table PITR '{name}': {e}")

    async def get(self, name: str) -> Pitr:
        """Get PITR by name asynchronously"""
        try:
            sql = f"SHOW PITR WHERE pitr_name = {self.client._escape_string(name)}"
            result = await self.client.execute(sql)

            if not result or not result.rows:
                raise PitrError(f"PITR '{name}' not found")

            row = result.rows[0]
            return self._row_to_pitr(row)

        except Exception as e:
            raise PitrError(f"Failed to get PITR '{name}': {e}")

    async def list(
        self,
        level: Optional[str] = None,
        account_name: Optional[str] = None,
        database_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> List[Pitr]:
        """List PITRs with optional filters asynchronously"""
        try:
            conditions = []

            if level:
                conditions.append(f"pitr_level = {self.client._escape_string(level)}")
            if account_name:
                conditions.append(f"account_name = {self.client._escape_string(account_name)}")
            if database_name:
                conditions.append(f"database_name = {self.client._escape_string(database_name)}")
            if table_name:
                conditions.append(f"table_name = {self.client._escape_string(table_name)}")

            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
            else:
                where_clause = ""

            sql = f"SHOW PITR{where_clause}"
            result = await self.client.execute(sql)

            if not result or not result.rows:
                return []

            return [self._row_to_pitr(row) for row in result.rows]

        except Exception as e:
            raise PitrError(f"Failed to list PITRs: {e}")

    async def alter(self, name: str, range_value: int, range_unit: str) -> Pitr:
        """Alter PITR range asynchronously"""
        try:
            self._validate_range(range_value, range_unit)

            sql = f"ALTER PITR {self.client._escape_identifier(name)} " f"RANGE {range_value} '{range_unit}'"

            result = await self.client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to alter PITR '{name}'")

            return await self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to alter PITR '{name}': {e}")

    async def delete(self, name: str) -> bool:
        """Delete PITR asynchronously"""
        try:
            sql = f"DROP PITR {self.client._escape_identifier(name)}"
            result = await self.client.execute(sql)
            return result is not None

        except Exception as e:
            raise PitrError(f"Failed to delete PITR '{name}': {e}")

    def _validate_range(self, range_value: int, range_unit: str) -> None:
        """Validate PITR range parameters"""
        if not (1 <= range_value <= 100):
            raise PitrError("Range value must be between 1 and 100")

        valid_units = ["h", "d", "mo", "y"]
        if range_unit not in valid_units:
            raise PitrError(f"Range unit must be one of: {', '.join(valid_units)}")

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


class AsyncRestoreManager:
    """Async manager for restore operations"""

    def __init__(self, client):
        self.client = client

    async def restore_cluster(self, snapshot_name: str) -> bool:
        """Restore entire cluster from snapshot asynchronously"""
        try:
            sql = f"RESTORE CLUSTER FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}"
            result = await self.client.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore cluster from snapshot '{snapshot_name}': {e}")

    async def restore_tenant(self, snapshot_name: str, account_name: str, to_account: Optional[str] = None) -> bool:
        """Restore tenant from snapshot asynchronously"""
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

            result = await self.client.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore tenant '{account_name}' from snapshot '{snapshot_name}': {e}")

    async def restore_database(
        self,
        snapshot_name: str,
        account_name: str,
        database_name: str,
        to_account: Optional[str] = None,
    ) -> bool:
        """Restore database from snapshot asynchronously"""
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

            result = await self.client.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore database '{database_name}' from snapshot '{snapshot_name}': {e}")

    async def restore_table(
        self,
        snapshot_name: str,
        account_name: str,
        database_name: str,
        table_name: str,
        to_account: Optional[str] = None,
    ) -> bool:
        """Restore table from snapshot asynchronously"""
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

            result = await self.client.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore table '{table_name}' from snapshot '{snapshot_name}': {e}")


class AsyncMoCtlManager:
    """Async mo_ctl manager"""

    def __init__(self, client):
        self.client = client

    async def _execute_moctl(self, method: str, target: str, params: str = "") -> Dict[str, Any]:
        """Execute mo_ctl command asynchronously"""
        import json

        try:
            # Build mo_ctl SQL command
            if params:
                sql = f"SELECT mo_ctl('{method}', '{target}', '{params}')"
            else:
                sql = f"SELECT mo_ctl('{method}', '{target}', '')"

            # Execute the command
            result = await self.client.execute(sql)

            if not result.rows:
                raise MoCtlError(f"mo_ctl command returned no results: {sql}")

            # Parse the JSON result
            result_str = result.rows[0][0]
            parsed_result = json.loads(result_str)

            # Check for errors in the result
            if "result" in parsed_result and parsed_result["result"]:
                first_result = parsed_result["result"][0]
                if "returnStr" in first_result and first_result["returnStr"] != "OK":
                    raise MoCtlError(f"mo_ctl operation failed: {first_result['returnStr']}")

            return parsed_result

        except json.JSONDecodeError as e:
            raise MoCtlError(f"Failed to parse mo_ctl result: {e}")
        except Exception as e:
            raise MoCtlError(f"mo_ctl operation failed: {e}")

    async def flush_table(self, database: str, table: str) -> Dict[str, Any]:
        """Force flush table asynchronously"""
        table_ref = f"{database}.{table}"
        return await self._execute_moctl("dn", "flush", table_ref)

    async def increment_checkpoint(self) -> Dict[str, Any]:
        """Force incremental checkpoint asynchronously"""
        return await self._execute_moctl("dn", "checkpoint", "")

    async def global_checkpoint(self) -> Dict[str, Any]:
        """Force global checkpoint asynchronously"""
        return await self._execute_moctl("dn", "globalcheckpoint", "")


class AsyncAccountManager:
    """Async manager for MatrixOne account operations"""

    def __init__(self, client):
        self.client = client

    async def create_account(
        self,
        account_name: str,
        admin_name: str,
        password: str,
        comment: Optional[str] = None,
        admin_comment: Optional[str] = None,
        admin_host: str = "%",
        admin_identified_by: Optional[str] = None,
    ) -> Account:
        """Create a new account asynchronously"""
        try:
            sql_parts = [f"CREATE ACCOUNT {self.client._escape_identifier(account_name)}"]
            sql_parts.append(f"ADMIN_NAME {self.client._escape_string(admin_name)}")
            sql_parts.append(f"IDENTIFIED BY {self.client._escape_string(password)}")

            if admin_host != "%":
                sql_parts.append(f"ADMIN_HOST {self.client._escape_string(admin_host)}")

            if comment:
                sql_parts.append(f"COMMENT {self.client._escape_string(comment)}")

            if admin_comment:
                sql_parts.append(f"ADMIN_COMMENT {self.client._escape_string(admin_comment)}")

            if admin_identified_by:
                sql_parts.append(f"ADMIN_IDENTIFIED BY {self.client._escape_string(admin_identified_by)}")

            sql = " ".join(sql_parts)
            await self.client.execute(sql)

            return await self.get_account(account_name)

        except Exception as e:
            raise AccountError(f"Failed to create account '{account_name}': {e}")

    async def drop_account(self, account_name: str) -> None:
        """Drop an account asynchronously"""
        try:
            sql = f"DROP ACCOUNT {self.client._escape_identifier(account_name)}"
            await self.client.execute(sql)
        except Exception as e:
            raise AccountError(f"Failed to drop account '{account_name}': {e}")

    async def alter_account(
        self,
        account_name: str,
        comment: Optional[str] = None,
        suspend: Optional[bool] = None,
        suspend_reason: Optional[str] = None,
    ) -> Account:
        """Alter an account asynchronously"""
        try:
            sql_parts = [f"ALTER ACCOUNT {self.client._escape_identifier(account_name)}"]

            if comment is not None:
                sql_parts.append(f"COMMENT {self.client._escape_string(comment)}")

            if suspend is not None:
                if suspend:
                    if suspend_reason:
                        sql_parts.append(f"SUSPEND COMMENT {self.client._escape_string(suspend_reason)}")
                    else:
                        sql_parts.append("SUSPEND")
                else:
                    sql_parts.append("OPEN")

            sql = " ".join(sql_parts)
            await self.client.execute(sql)

            return await self.get_account(account_name)

        except Exception as e:
            raise AccountError(f"Failed to alter account '{account_name}': {e}")

    async def get_account(self, account_name: str) -> Account:
        """Get account by name asynchronously"""
        try:
            sql = "SHOW ACCOUNTS"
            result = await self.client.execute(sql)

            if not result or not result.rows:
                raise AccountError(f"Account '{account_name}' not found")

            for row in result.rows:
                if row[0] == account_name:
                    return self._row_to_account(row)

            raise AccountError(f"Account '{account_name}' not found")

        except Exception as e:
            raise AccountError(f"Failed to get account '{account_name}': {e}")

    async def list_accounts(self) -> List[Account]:
        """List all accounts asynchronously"""
        try:
            sql = "SHOW ACCOUNTS"
            result = await self.client.execute(sql)

            if not result or not result.rows:
                return []

            return [self._row_to_account(row) for row in result.rows]

        except Exception as e:
            raise AccountError(f"Failed to list accounts: {e}")

    async def create_user(self, user_name: str, password: str, comment: Optional[str] = None) -> User:
        """
        Create a new user asynchronously according to MatrixOne CREATE USER syntax:
        CREATE USER [IF NOT EXISTS] user auth_option [, user auth_option] ...
        [DEFAULT ROLE rolename] [COMMENT 'comment_string' | ATTRIBUTE 'json_object']

        Args::

            user_name: Name of the user to create
            password: Password for the user
            comment: Comment for the user (not supported in MatrixOne)

        Returns::

            User: Created user object
        """
        try:
            # Build CREATE USER statement according to MatrixOne syntax
            # MatrixOne syntax: CREATE USER user_name IDENTIFIED BY 'password'
            sql_parts = [f"CREATE USER {self.client._escape_identifier(user_name)}"]

            sql_parts.append(f"IDENTIFIED BY {self.client._escape_string(password)}")

            # Note: MatrixOne doesn't support COMMENT or ATTRIBUTE clauses in CREATE USER
            # if comment:
            #     sql_parts.append(f"COMMENT {self.client._escape_string(comment)}")
            # if identified_by:
            #     sql_parts.append(f"IDENTIFIED BY {self.client._escape_string(identified_by)}")

            sql = " ".join(sql_parts)
            await self.client.execute(sql)

            # Return a User object with current account context
            return User(
                name=user_name,
                host="%",  # Default host
                account="sys",  # Default account
                created_time=datetime.now(),
                status="ACTIVE",
                comment=comment,
            )

        except Exception as e:
            raise AccountError(f"Failed to create user '{user_name}': {e}")

    async def drop_user(self, user_name: str, if_exists: bool = False) -> None:
        """
        Drop a user asynchronously according to MatrixOne DROP USER syntax:
        DROP USER [IF EXISTS] user [, user] ...

        Args::

            user_name: Name of the user to drop
            if_exists: If True, add IF EXISTS clause to avoid errors when user doesn't exist
        """
        try:
            sql_parts = ["DROP USER"]
            if if_exists:
                sql_parts.append("IF EXISTS")

            sql_parts.append(self.client._escape_identifier(user_name))
            sql = " ".join(sql_parts)
            await self.client.execute(sql)

        except Exception as e:
            raise AccountError(f"Failed to drop user '{user_name}': {e}")

    async def alter_user(
        self,
        user_name: str,
        password: Optional[str] = None,
        comment: Optional[str] = None,
        lock: Optional[bool] = None,
        lock_reason: Optional[str] = None,
    ) -> User:
        """Alter a user asynchronously"""
        try:
            sql_parts = [f"ALTER USER {self.client._escape_identifier(user_name)}"]

            if password is not None:
                sql_parts.append(f"IDENTIFIED BY {self.client._escape_string(password)}")

            if comment is not None:
                sql_parts.append(f"COMMENT {self.client._escape_string(comment)}")

            if lock is not None:
                if lock:
                    if lock_reason:
                        sql_parts.append(f"ACCOUNT LOCK COMMENT {self.client._escape_string(lock_reason)}")
                    else:
                        sql_parts.append("ACCOUNT LOCK")
                else:
                    sql_parts.append("ACCOUNT UNLOCK")

            sql = " ".join(sql_parts)
            await self.client.execute(sql)

            return await self.get_user(user_name)

        except Exception as e:
            raise AccountError(f"Failed to alter user '{user_name}': {e}")

    async def get_user(self, user_name: str) -> User:
        """Get user by name asynchronously"""
        try:
            sql = "SHOW GRANTS"
            result = await self.client.execute(sql)

            if not result or not result.rows:
                raise AccountError(f"User '{user_name}' not found")

            for row in result.rows:
                if row[0] == user_name:
                    return self._row_to_user(row)

            raise AccountError(f"User '{user_name}' not found")

        except Exception as e:
            raise AccountError(f"Failed to get user '{user_name}': {e}")

    async def list_users(self, account_name: Optional[str] = None) -> List[User]:
        """List users with optional account filter asynchronously"""
        try:
            sql = "SHOW GRANTS"
            result = await self.client.execute(sql)

            if not result or not result.rows:
                return []

            users = [self._row_to_user(row) for row in result.rows]

            if account_name:
                users = [user for user in users if user.account == account_name]

            return users

        except Exception as e:
            raise AccountError(f"Failed to list users: {e}")

    def _row_to_account(self, row: tuple) -> Account:
        """Convert database row to Account object"""
        return Account(
            name=row[0],
            admin_name=row[1],
            created_time=row[2] if len(row) > 2 else None,
            status=row[3] if len(row) > 3 else None,
            comment=row[4] if len(row) > 4 else None,
            suspended_time=row[5] if len(row) > 5 else None,
            suspended_reason=row[6] if len(row) > 6 else None,
        )

    def _row_to_user(self, row: tuple) -> User:
        """Convert database row to User object"""
        return User(
            name=row[0],
            host=row[1],
            account=row[2],
            created_time=row[3] if len(row) > 3 else None,
            status=row[4] if len(row) > 4 else None,
            comment=row[5] if len(row) > 5 else None,
            locked_time=row[6] if len(row) > 6 else None,
            locked_reason=row[7] if len(row) > 7 else None,
        )


class AsyncFulltextIndexManager:
    """Async fulltext index manager for client chain operations"""

    def __init__(self, client: "AsyncClient"):
        """Initialize async fulltext index manager"""
        self.client = client

    async def create(
        self, table_name_or_model, name: str, columns: Union[str, List[str]], algorithm: str = "TF-IDF"
    ) -> "AsyncFulltextIndexManager":
        """
            Create a fulltext index using chain operations.

            Args::

                table_name_or_model: Either a table name (str) or a SQLAlchemy model class
                name: Index name
                columns: Column(s) to index
                algorithm: Fulltext algorithm type (TF-IDF or BM25)

            Returns::

                AsyncFulltextIndexManager: Self for chaining

            Example

        # Create fulltext index by table name
                await client.fulltext_index.create("articles", name="idx_content", columns=["title", "content"])

                # Create fulltext index by model class
                await client.fulltext_index.create(ArticleModel, name="idx_content", columns=["title", "content"])
        """
        from sqlalchemy import text

        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            table_name = table_name_or_model.__tablename__
        else:
            table_name = table_name_or_model

        try:
            if isinstance(columns, str):
                columns = [columns]

            columns_str = ", ".join(columns)
            sql = f"CREATE FULLTEXT INDEX {name} ON {table_name} ({columns_str})"

            async with self.client.get_sqlalchemy_engine().begin() as conn:
                await conn.execute(text(sql))

            return self
        except Exception as e:
            raise Exception(f"Failed to create fulltext index {name} on table {table_name}: {e}")

    async def drop(self, table_name_or_model, name: str) -> "AsyncFulltextIndexManager":
        """
            Drop a fulltext index using chain operations.

            Args::

                table_name_or_model: Either a table name (str) or a SQLAlchemy model class
                name: Index name

            Returns::

                AsyncFulltextIndexManager: Self for chaining

            Example

        # Drop fulltext index by table name
                await client.fulltext_index.drop("articles", "idx_content")

                # Drop fulltext index by model class
                await client.fulltext_index.drop(ArticleModel, "idx_content")
        """
        from sqlalchemy import text

        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            table_name = table_name_or_model.__tablename__
        else:
            table_name = table_name_or_model

        try:
            sql = f"DROP INDEX {name} ON {table_name}"

            async with self.client.get_sqlalchemy_engine().begin() as conn:
                await conn.execute(text(sql))

            return self
        except Exception as e:
            raise Exception(f"Failed to drop fulltext index {name} from table {table_name}: {e}")

    async def enable_fulltext(self) -> "AsyncFulltextIndexManager":
        """
        Enable fulltext indexing with chain operations.

        Returns::

            AsyncFulltextIndexManager: Self for chaining
        """
        try:
            await self.client.execute("SET experimental_fulltext_index = 1")
            return self
        except Exception as e:
            raise Exception(f"Failed to enable fulltext indexing: {e}")

    async def disable_fulltext(self) -> "AsyncFulltextIndexManager":
        """
        Disable fulltext indexing with chain operations.

        Returns::

            AsyncFulltextIndexManager: Self for chaining
        """
        try:
            await self.client.execute("SET experimental_fulltext_index = 0")
            return self
        except Exception as e:
            raise Exception(f"Failed to disable fulltext indexing: {e}")


class AsyncTransactionFulltextIndexManager(AsyncFulltextIndexManager):
    """Async fulltext index manager that executes operations within a transaction"""

    def __init__(self, client: "AsyncClient", transaction_wrapper):
        """Initialize async transaction fulltext index manager"""
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper

    async def create(
        self, table_name: str, name: str, columns: Union[str, List[str]], algorithm: str = "TF-IDF"
    ) -> "AsyncTransactionFulltextIndexManager":
        """Create a fulltext index within transaction"""
        try:
            if isinstance(columns, str):
                columns = [columns]

            columns_str = ", ".join(columns)
            sql = f"CREATE FULLTEXT INDEX {name} ON {table_name} ({columns_str})"

            await self.transaction_wrapper.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to create fulltext index {name} on table {table_name} in transaction: {e}")

    async def drop(self, table_name: str, name: str) -> "AsyncTransactionFulltextIndexManager":
        """Drop a fulltext index within transaction"""
        try:
            sql = f"DROP INDEX {name} ON {table_name}"
            await self.transaction_wrapper.execute(sql)
            return self
        except Exception as e:
            raise Exception(f"Failed to drop fulltext index {name} from table {table_name} in transaction: {e}")


class AsyncClientExecutor(BaseMatrixOneExecutor):
    """Async client executor that uses AsyncClient's execute method"""

    def __init__(self, client):
        super().__init__(client)
        self.client = client

    async def _execute(self, sql: str):
        return await self.client.execute(sql)

    def _get_empty_result(self):
        return AsyncResultSet([], [], affected_rows=0)

    async def insert(self, table_name: str, data: dict):
        """Async insert method"""
        sql = self.base_client._build_insert_sql(table_name, data)
        return await self._execute(sql)

    async def batch_insert(self, table_name: str, data_list: list):
        """Async batch insert method"""
        if not data_list:
            return self._get_empty_result()

        sql = self.base_client._build_batch_insert_sql(table_name, data_list)
        return await self._execute(sql)


class AsyncClient(BaseMatrixOneClient):
    """
    MatrixOne Async Client - Asynchronous interface for MatrixOne database operations.

    This class provides a comprehensive asynchronous interface for connecting to and
    interacting with MatrixOne databases. It supports modern async/await patterns
    including table creation, data insertion, querying, vector operations, and
    transaction management.

    Key Features:

    - Asynchronous connection management with connection pooling
    - High-level table operations (create_table, drop_table, insert, batch_insert)
    - Query builder interface for complex async queries
    - Vector operations (similarity search, range search, indexing)
    - Async transaction management with context managers
    - Snapshot and restore operations
    - Account and user management
    - Fulltext search capabilities
    - Non-blocking I/O operations

    Supported Operations:

    - Async connection and disconnection
    - Async query execution (SELECT, INSERT, UPDATE, DELETE)
    - Async batch operations
    - Async transaction management
    - Async table creation and management
    - Async vector and fulltext operations
    - Async snapshot and restore operations

    Usage Examples::

        Basic async usage::

            async def main():
                client = AsyncClient()
                await client.connect('localhost', 6001, 'root', '111', 'test')

                # Create table using high-level API
                await client.create_table("users", {
                    "id": "int primary key",
                    "name": "varchar(100)",
                    "email": "varchar(255)"
                })

                # Insert data
                await client.insert("users", {"id": 1, "name": "John", "email": "john@example.com"})

                # Query data
                result = await client.query("users").where("id = ?", 1).all()
                print(result.rows)

                await client.disconnect()

        Vector operations::

            async def vector_example():
                client = AsyncClient()
                await client.connect('localhost', 6001, 'root', '111', 'test')

                # Create vector table
                await client.create_table("documents", {
                    "id": "int primary key",
                    "content": "text",
                    "embedding": "vecf32(384)"
                })

                # Vector similarity search
                results = await client.vector_ops.similarity_search(
                    "documents",
                    vector_column="embedding",
                    query_vector=[0.1, 0.2, 0.3, ...],  # 384-dimensional vector
                    limit=10,
                    distance_type="l2"
                )

                await client.disconnect()

        Async transaction usage::

            async def transaction_example():
                client = AsyncClient()
                await client.connect('localhost', 6001, 'root', '111', 'test')

                async with client.transaction() as tx:
                    await tx.execute("INSERT INTO users (name) VALUES (?)", ("John",))
                await tx.execute("INSERT INTO orders (user_id, amount) VALUES (?, ?)", (1, 100.0))
                # Transaction commits automatically on success

    Note: This class requires asyncio and async database drivers. Use the synchronous Client class
    for blocking operations or when async support is not needed.
    """

    def __init__(
        self,
        connection_timeout: int = 30,
        query_timeout: int = 300,
        auto_commit: bool = True,
        charset: str = "utf8mb4",
        logger: Optional[MatrixOneLogger] = None,
        sql_log_mode: str = "auto",
        slow_query_threshold: float = 1.0,
        max_sql_display_length: int = 500,
    ):
        """
        Initialize MatrixOne async client

        Args::

            connection_timeout: Connection timeout in seconds
            query_timeout: Query timeout in seconds
            auto_commit: Enable auto-commit mode
            charset: Character set for connection
            logger: Custom logger instance. If None, creates a default logger
            sql_log_mode: SQL logging mode ('off', 'auto', 'simple', 'full')
                - 'off': No SQL logging
                - 'auto': Smart logging - short SQL shown fully, long SQL summarized (default)
                - 'simple': Show operation summary only
                - 'full': Show complete SQL regardless of length
            slow_query_threshold: Threshold in seconds for slow query warnings (default: 1.0)
            max_sql_display_length: Maximum SQL length in auto mode before summarizing (default: 500)
        """
        self.connection_timeout = connection_timeout
        self.query_timeout = query_timeout
        self.auto_commit = auto_commit
        self.charset = charset

        # Initialize logger
        if logger is not None:
            self.logger = logger
        else:
            self.logger = create_default_logger(
                sql_log_mode=sql_log_mode,
                slow_query_threshold=slow_query_threshold,
                max_sql_display_length=max_sql_display_length,
            )

        # Connection management - using SQLAlchemy async engine instead of direct aiomysql connection
        self._engine = None
        self._connection = None  # Keep for backward compatibility, but will be managed by engine
        self._connection_params = {}
        self._login_info = None

        # Initialize managers
        self._snapshots = AsyncSnapshotManager(self)
        self._clone = AsyncCloneManager(self)
        self._moctl = AsyncMoCtlManager(self)
        self._restore = AsyncRestoreManager(self)
        self._pitr = AsyncPitrManager(self)
        self._pubsub = AsyncPubSubManager(self)
        self._account = AsyncAccountManager(self)
        self._fulltext_index = None
        self._metadata = None

    async def connect(
        self,
        *,
        host: str = "localhost",
        port: int = 6001,
        user: str = "root",
        password: str = "111",
        database: str,
        account: Optional[str] = None,
        role: Optional[str] = None,
        charset: str = "utf8mb4",
        connection_timeout: int = 30,
        auto_commit: bool = True,
        on_connect: Optional[Union[ConnectionHook, List[Union[ConnectionAction, str]], Callable]] = None,
    ):
        """
        Connect to MatrixOne database asynchronously

        Args::

            host: Database host
            port: Database port
            user: Username or login info in format "user", "account#user", or "account#user#role"
            password: Password
            database: Database name
            account: Optional account name (will be combined with user if user doesn't contain '#')
            role: Optional role name (will be combined with user if user doesn't contain '#')
            charset: Character set for the connection (default: utf8mb4)
            connection_timeout: Connection timeout in seconds (default: 30)
            auto_commit: Enable autocommit (default: True)
            on_connect: Connection hook to execute after successful connection.
                       Can be:
                       - ConnectionHook instance
                       - List of ConnectionAction or string action names
                       - Custom callback function (async or sync)

        Examples::

            # Enable all features after connection
            await client.connect(host, port, user, password, database,
                               on_connect=[ConnectionAction.ENABLE_ALL])

            # Enable only vector operations with custom charset
            await client.connect(host, port, user, password, database,
                               charset="utf8mb4",
                               on_connect=[ConnectionAction.ENABLE_VECTOR])

            # Custom async callback
            async def my_callback(client):
                print(f"Connected to {client._connection_params['host']}")

            await client.connect(host, port, user, password, database,
                               on_connect=my_callback)
        """
        try:
            # Build final login info based on user parameter and optional account/role
            final_user, parsed_info = self._build_login_info(user, account, role)

            # Store parsed info for later use
            self._login_info = parsed_info

            # Store connection parameters for engine creation
            self._connection_params = {
                "host": host,
                "port": port,
                "user": final_user,
                "password": password,
                "database": database,
                "charset": charset,
                "autocommit": auto_commit,
                "connect_timeout": connection_timeout,
            }

            # Create SQLAlchemy async engine instead of direct aiomysql connection
            self._engine = self._create_async_engine()

            # Test the connection by executing a simple query
            async with self._engine.begin() as conn:
                await conn.execute(text("SELECT 1"))

            # Initialize vector managers after successful connection
            self._initialize_vector_managers()

            # Initialize metadata manager after successful connection
            self._metadata = AsyncMetadataManager(self)

            self.logger.log_connection(host, port, final_user, database or "default", success=True)

            # Setup connection hook (default to ENABLE_ALL if not provided)
            # Allow empty list [] to explicitly disable hooks
            if on_connect is None:
                on_connect = [ConnectionAction.ENABLE_ALL]

            if on_connect:  # Only setup if not empty list
                self._setup_connection_hook(on_connect)
                # Execute the hook once immediately for the initial connection
                await self._execute_connection_hook_immediately(on_connect)

        except Exception as e:
            self.logger.log_connection(host, port, final_user, database or "default", success=False)
            self.logger.log_error(e, context="Async connection")

            # Provide user-friendly error messages for common issues
            error_msg = str(e)
            if 'Unknown database' in error_msg or '1049' in error_msg:
                db_name = database or "default"
                raise ConnectionError(
                    f"Database '{db_name}' does not exist. Please create it first:\n"
                    f"  mysql -h{host} -P{port} -u{user.split('#')[0] if '#' in user else user} -p{password} "
                    f"-e \"CREATE DATABASE {db_name}\""
                ) from e
            else:
                raise ConnectionError(f"Failed to connect to MatrixOne: {e}") from e

    def _setup_connection_hook(
        self, on_connect: Union[ConnectionHook, List[Union[ConnectionAction, str]], Callable]
    ) -> None:
        """Setup connection hook to be executed on each new connection"""
        try:
            if isinstance(on_connect, ConnectionHook):
                # Direct ConnectionHook instance
                hook = on_connect
            elif isinstance(on_connect, list):
                # List of actions - create a hook
                hook = create_connection_hook(actions=on_connect)
            elif callable(on_connect):
                # Custom callback function
                hook = create_connection_hook(custom_hook=on_connect)
            else:
                self.logger.warning(f"Invalid on_connect parameter type: {type(on_connect)}")
                return

            # Set the client reference and attach to engine
            hook.set_client(self)
            hook.attach_to_engine(self._engine)

        except Exception as e:
            self.logger.warning(f"Connection hook setup failed: {e}")

    async def _execute_connection_hook_immediately(
        self, on_connect: Union[ConnectionHook, List[Union[ConnectionAction, str]], Callable]
    ) -> None:
        """Execute connection hook immediately for the initial connection"""
        try:
            if isinstance(on_connect, ConnectionHook):
                # Direct ConnectionHook instance
                hook = on_connect
            elif isinstance(on_connect, list):
                # List of actions - create a hook
                hook = create_connection_hook(actions=on_connect)
            elif callable(on_connect):
                # Custom callback function
                hook = create_connection_hook(custom_hook=on_connect)
            else:
                self.logger.warning(f"Invalid on_connect parameter type: {type(on_connect)}")
                return

            # Execute the hook immediately
            await hook.execute_async(self)

        except Exception as e:
            self.logger.warning(f"Immediate connection hook execution failed: {e}")

    @classmethod
    def from_engine(cls, engine: AsyncEngine, **kwargs) -> "AsyncClient":
        """
            Create AsyncClient instance from existing SQLAlchemy AsyncEngine

            Args::

                engine: SQLAlchemy AsyncEngine instance (must use MySQL driver)
                **kwargs: Additional client configuration options

            Returns::

                AsyncClient: Configured async client instance

            Raises::

                ConnectionError: If engine doesn't use MySQL driver

            Examples

        Basic usage::

                    from sqlalchemy.ext.asyncio import create_async_engine
                    from matrixone import AsyncClient

                    engine = create_async_engine("mysql+aiomysql://user:pass@host:port/db")
                    client = AsyncClient.from_engine(engine)

                With custom configuration::

                    engine = create_async_engine("mysql+aiomysql://user:pass@host:port/db")
                    client = AsyncClient.from_engine(
                        engine,
                        sql_log_mode='auto',
                        slow_query_threshold=0.5
                    )
        """
        # Check if engine uses MySQL driver
        if not cls._is_mysql_async_engine(engine):
            raise ConnectionError(
                "MatrixOne AsyncClient only supports MySQL drivers. "
                "Please use mysql+aiomysql:// connection strings. "
                f"Current engine uses: {engine.dialect.name}"
            )

        # Create client instance with default parameters
        client = cls(**kwargs)

        # Set the provided engine
        client._engine = engine

        # Initialize vector managers after engine is set
        client._initialize_vector_managers()

        return client

    @staticmethod
    def _is_mysql_async_engine(engine: AsyncEngine) -> bool:
        """
        Check if the async engine uses a MySQL driver

        Args::

            engine: SQLAlchemy AsyncEngine instance

        Returns::

            bool: True if engine uses MySQL driver, False otherwise
        """
        # Check dialect name
        dialect_name = engine.dialect.name.lower()

        # Check if it's a MySQL dialect
        if dialect_name == "mysql":
            return True

        # Check connection string for MySQL async drivers
        url = str(engine.url)
        mysql_async_drivers = [
            "mysql+aiomysql",
            "mysql+asyncmy",
            "mysql+aiopg",  # Note: aiopg is PostgreSQL, but included for completeness
        ]

        return any(driver in url.lower() for driver in mysql_async_drivers)

    def _create_async_engine(self) -> AsyncEngine:
        """Create SQLAlchemy async engine with connection pooling"""
        if not create_async_engine:
            raise ConnectionError("SQLAlchemy async engine not available. Please install sqlalchemy[asyncio]")

        # Build connection string for async engine
        connection_string = (
            f"mysql+aiomysql://{self._connection_params['user']}:"
            f"{self._connection_params['password']}@"
            f"{self._connection_params['host']}:"
            f"{self._connection_params['port']}/"
            f"{self._connection_params['database'] or ''}"
            f"?charset={self._connection_params['charset']}"
        )

        # Create async engine with connection pooling
        engine = create_async_engine(
            connection_string,
            pool_size=5,  # Smaller pool size for testing
            max_overflow=10,  # Smaller max overflow
            pool_timeout=30,  # Default pool timeout
            pool_recycle=3600,  # Recycle connections after 1 hour
            pool_pre_ping=True,  # Verify connections before use
            pool_reset_on_return="commit",  # Reset connections on return
            echo=False,  # Set to True for SQL logging
        )

        return engine

    def _initialize_vector_managers(self) -> None:
        """Initialize vector managers after successful connection"""
        try:
            from .async_vector_index_manager import AsyncVectorManager

            self._vector = AsyncVectorManager(self)
            self._fulltext_index = AsyncFulltextIndexManager(self)
        except ImportError:
            # Vector managers not available
            self._vector = None
            self._fulltext_index = None

    async def disconnect(self):
        """Disconnect from MatrixOne database asynchronously"""
        if self._engine:
            try:
                # First, try to close any active connections in the pool
                if hasattr(self._engine, "_pool") and self._engine._pool is not None:
                    # Close all connections in the pool
                    await self._engine._pool.close()
                    # Wait for all connections to be properly closed
                    await self._engine._pool.wait_closed()

                # Then dispose of the engine - this closes all connections in the pool
                await self._engine.dispose()

                # Force garbage collection to ensure connections are cleaned up
                import gc

                gc.collect()

                self.logger.log_disconnection(success=True)
            except Exception as e:
                self.logger.log_disconnection(success=False)
                self.logger.log_error(e, context="Async disconnection")
            finally:
                # Ensure all references are cleared
                self._engine = None
                self._connection = None
                # Clear any cached managers
                self._fulltext_index = None

    def disconnect_sync(self):
        """Synchronous disconnect for cleanup when event loop is closed"""
        if self._engine:
            try:
                # Try to close the connection pool synchronously
                # SQLAlchemy AsyncEngine has a sync_engine property that can be disposed
                if hasattr(self._engine, "sync_engine") and self._engine.sync_engine is not None:
                    # Close all connections in the pool synchronously
                    self._engine.sync_engine.dispose()
                elif hasattr(self._engine, "_pool") and self._engine._pool is not None:
                    # Direct access to connection pool for cleanup
                    try:
                        self._engine._pool.close()
                    except Exception:
                        pass

                self.logger.log_disconnection(success=True)
            except Exception as e:
                self.logger.log_disconnection(success=False)
                self.logger.log_error(e, context="Sync disconnection")
            finally:
                # Ensure all references are cleared regardless of success/failure
                self._engine = None
                self._connection = None
                # Clear any cached managers
                self._fulltext_index = None

    def __del__(self):
        """Cleanup when object is garbage collected"""
        # Don't try to cleanup in __del__ as it can cause issues with event loops
        # The fixture should handle proper cleanup

    def get_sqlalchemy_engine(self) -> AsyncEngine:
        """
        Get SQLAlchemy async engine

        Returns::

            SQLAlchemy AsyncEngine
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")
        return self._engine

    async def create_all(self, base_class=None):
        """
        Create all tables defined in the given base class or default Base.

        Args::

            base_class: SQLAlchemy declarative base class. If None, uses the default Base.
        """
        if base_class is None:
            from matrixone.orm import declarative_base

            base_class = declarative_base()

        async with self._engine.begin() as conn:
            await conn.run_sync(base_class.metadata.create_all)
        return self

    async def drop_all(self, base_class=None):
        """
        Drop all tables defined in the given base class or default Base.

        Args::

            base_class: SQLAlchemy declarative base class. If None, uses the default Base.
        """
        if base_class is None:
            from matrixone.orm import declarative_base

            base_class = declarative_base()

        # Get all table names from the metadata
        table_names = list(base_class.metadata.tables.keys())

        # Drop each table individually using direct SQL for better compatibility
        for table_name in table_names:
            try:
                await self.execute(f"DROP TABLE IF EXISTS {table_name}")
            except Exception as e:
                # Log the error but continue with other tables
                print(f"Warning: Failed to drop table {table_name}: {e}")

        return self

    async def _execute_with_logging(
        self, connection, sql: str, context: str = "Async SQL execution", override_sql_log_mode: str = None
    ):
        """
        Execute SQL asynchronously with proper logging through the client's logger.

        This is an internal helper method used by all SDK components to ensure
        consistent SQL logging across async vector operations, transactions, and other features.

        Args::

            connection: SQLAlchemy async connection object
            sql: SQL query string
            context: Context description for error logging (default: "Async SQL execution")
            override_sql_log_mode: Temporarily override sql_log_mode for this query only

        Returns::

            SQLAlchemy result object

        Note:

            This method is used internally by AsyncVectorManager, AsyncTransactionWrapper,
            and other SDK components. External users should use execute() instead.
        """
        import time

        from sqlalchemy import text

        start_time = time.time()
        try:
            result = await connection.execute(text(sql))
            execution_time = time.time() - start_time

            # Try to get row count if available
            try:
                if result.returns_rows:
                    # For SELECT queries, we can't consume the result to count rows
                    # So we just log without row count
                    self.logger.log_query(
                        sql, execution_time, None, success=True, override_sql_log_mode=override_sql_log_mode
                    )
                else:
                    # For DML queries (INSERT/UPDATE/DELETE), we can get rowcount
                    self.logger.log_query(
                        sql, execution_time, result.rowcount, success=True, override_sql_log_mode=override_sql_log_mode
                    )
            except Exception:
                # Fallback: just log the query without row count
                self.logger.log_query(sql, execution_time, None, success=True, override_sql_log_mode=override_sql_log_mode)

            return result
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.log_query(sql, execution_time, success=False, override_sql_log_mode=override_sql_log_mode)
            self.logger.log_error(e, context=context)
            raise

    async def execute(self, sql: str, params: Optional[Tuple] = None) -> AsyncResultSet:
        """
        Execute SQL query asynchronously using SQLAlchemy async engine

        Args::

            sql: SQL query string
            params: Query parameters

        Returns::

            AsyncResultSet with query results
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        import time

        start_time = time.time()

        try:
            # Handle parameter substitution for MatrixOne compatibility
            final_sql = self._substitute_parameters(sql, params)

            async with self._engine.begin() as conn:
                # Use exec_driver_sql() to bypass SQLAlchemy's bind parameter parsing
                # This prevents JSON strings like {"a":1} from being parsed as :1 bind params
                if hasattr(conn, 'exec_driver_sql'):
                    # Escape % to %% for pymysql's format string handling
                    escaped_sql = final_sql.replace('%', '%%')
                    result = await conn.exec_driver_sql(escaped_sql)
                else:
                    # Fallback for testing or older SQLAlchemy versions
                    from sqlalchemy import text

                    result = await conn.execute(text(final_sql))

                execution_time = time.time() - start_time

                if result.returns_rows:
                    rows = result.fetchall()
                    columns = list(result.keys()) if hasattr(result, "keys") else []
                    async_result = AsyncResultSet(columns, rows)
                    self.logger.log_query(final_sql, execution_time, len(rows), success=True)
                    return async_result
                else:
                    async_result = AsyncResultSet([], [], affected_rows=result.rowcount)
                    self.logger.log_query(final_sql, execution_time, result.rowcount, success=True)
                    return async_result

        except Exception as e:
            execution_time = time.time() - start_time

            # Log error FIRST, before any error processing
            # Wrap in try-except to ensure logging failure doesn't hide the real error
            try:
                self.logger.log_query(final_sql, execution_time, success=False)
                self.logger.log_error(e, context="Async query execution")
            except Exception as log_err:
                # If logging fails, print to stderr as fallback but continue with error handling
                import sys

                print(f"Warning: Error logging failed: {log_err}", file=sys.stderr)

            # Extract user-friendly error message
            error_msg = str(e)

            # Handle common database errors with helpful messages
            # Check for "does not exist" first before "syntax error"
            if (
                'does not exist' in error_msg.lower()
                or 'no such table' in error_msg.lower()
                or 'doesn\'t exist' in error_msg.lower()
            ):
                # Table doesn't exist
                import re

                match = re.search(r"(?:table|database)\s+[\"']?(\w+)[\"']?\s+does not exist", error_msg, re.IGNORECASE)
                if match:
                    obj_name = match.group(1)
                    raise QueryError(
                        f"Table or database '{obj_name}' does not exist. "
                        f"Create it first using client.create_table() or CREATE TABLE/DATABASE statement."
                    ) from None
                else:
                    raise QueryError(f"Object not found: {error_msg}") from None

            elif 'already exists' in error_msg.lower() and '1050' in error_msg:
                # Table already exists
                import re

                match = re.search(r"table\s+(\w+)\s+already\s+exists", error_msg, re.IGNORECASE)
                if match:
                    table_name = match.group(1)
                    raise QueryError(
                        f"Table '{table_name}' already exists. "
                        f"Use DROP TABLE {table_name} or client.drop_table() to remove it first."
                    ) from None
                else:
                    raise QueryError(f"Object already exists: {error_msg}") from None

            elif 'duplicate' in error_msg.lower() and ('1062' in error_msg or '1061' in error_msg):
                # Duplicate key/entry
                raise QueryError(
                    f"Duplicate entry error: {error_msg}. "
                    f"Check for duplicate primary key or unique constraint violations."
                ) from None

            elif 'syntax error' in error_msg.lower() or '1064' in error_msg:
                # SQL syntax error
                sql_preview = final_sql[:200] + '...' if len(final_sql) > 200 else final_sql
                raise QueryError(f"SQL syntax error: {error_msg}\n" f"Query: {sql_preview}") from None

            elif 'column' in error_msg.lower() and ('unknown' in error_msg.lower() or 'not found' in error_msg.lower()):
                # Column doesn't exist
                raise QueryError(f"Column not found: {error_msg}. " f"Check your column names and table schema.") from None

            elif 'cannot be null' in error_msg.lower() or '1048' in error_msg:
                # NULL constraint violation
                raise QueryError(
                    f"NULL constraint violation: {error_msg}. " f"Some columns require non-NULL values."
                ) from None

            elif 'not supported' in error_msg.lower() and '20105' in error_msg:
                # MatrixOne-specific: feature not supported
                raise QueryError(
                    f"MatrixOne feature limitation: {error_msg}. "
                    f"This feature may require additional configuration or is not yet supported."
                ) from None

            elif 'bind parameter' in error_msg.lower() or 'InvalidRequestError' in error_msg:
                # SQLAlchemy bind parameter error
                raise QueryError(
                    f"Parameter binding error: {error_msg}. "
                    f"This might be caused by special characters in your data (colons in JSON, etc.)"
                ) from None

            else:
                # Generic error - cleaner message without full SQLAlchemy stack
                raise QueryError(f"Query execution failed: {error_msg}") from None

    def _substitute_parameters(self, sql: str, params: Optional[Tuple] = None) -> str:
        """
        Substitute ? placeholders with actual values since MatrixOne doesn't support prepared statements

        Args::

            sql: SQL query string with ? placeholders
            params: Tuple of parameter values

        Returns::

            SQL string with parameters substituted
        """
        if not params:
            return sql

        final_sql = sql
        for param in params:
            if isinstance(param, str):
                # Escape single quotes in string values
                escaped_param = param.replace("'", "''")
                final_sql = final_sql.replace("?", f"'{escaped_param}'", 1)
            elif param is None:
                final_sql = final_sql.replace("?", "NULL", 1)
            else:
                final_sql = final_sql.replace("?", str(param), 1)

        return final_sql

    def _build_login_info(self, user: str, account: Optional[str] = None, role: Optional[str] = None) -> tuple[str, dict]:
        """
        Build final login info based on user parameter and optional account/role

        Args::

            user: Username or login info in format "user", "account#user", or "account#user#role"
            account: Optional account name
            role: Optional role name

        Returns::

            tuple: (final_user_string, parsed_info_dict)

        Rules:
        1. If user contains '#', it's already in format "account#user" or "account#user#role"
           - If account or role is also provided, raise error (conflict)
        2. If user doesn't contain '#', combine with optional account/role:
           - No account/role: use user as-is
           - Only role: use "sys#user#role"
           - Only account: use "account#user"
           - Both: use "account#user#role"
        """
        # Check if user already contains login format
        if "#" in user:
            # User is already in format "account#user" or "account#user#role"
            if account is not None or role is not None:
                raise ValueError(
                    f"Conflict: user parameter '{user}' already contains account/role info, "
                    f"but account='{account}' and role='{role}' are also provided. "
                    f"Use either user format or separate account/role parameters, not both."
                )

            # Parse the existing format
            parts = user.split("#")
            if len(parts) == 2:
                # "account#user" format
                final_account, final_user, final_role = parts[0], parts[1], None
            elif len(parts) == 3:
                # "account#user#role" format
                final_account, final_user, final_role = parts[0], parts[1], parts[2]
            else:
                raise ValueError(f"Invalid user format: '{user}'. Expected 'user', 'account#user', or 'account#user#role'")

            final_user_string = user

        else:
            # User is just a username, combine with optional account/role
            if account is None and role is None:
                # No account/role provided, use user as-is
                final_account, final_user, final_role = "sys", user, None
                final_user_string = user
            elif account is None and role is not None:
                # Only role provided, use sys account
                final_account, final_user, final_role = "sys", user, role
                final_user_string = f"sys#{user}#{role}"
            elif account is not None and role is None:
                # Only account provided, no role
                final_account, final_user, final_role = account, user, None
                final_user_string = f"{account}#{user}"
            else:
                # Both account and role provided
                final_account, final_user, final_role = account, user, role
                final_user_string = f"{account}#{user}#{role}"

        parsed_info = {"account": final_account, "user": final_user, "role": final_role}

        return final_user_string, parsed_info

    def get_login_info(self) -> Optional[dict]:
        """Get parsed login information"""
        return self._login_info

    def _escape_identifier(self, identifier: str) -> str:
        """Escapes an identifier to prevent SQL injection."""
        return f"`{identifier}`"

    def _escape_string(self, value: str) -> str:
        """Escapes a string value for SQL queries."""
        return f"'{value}'"

    def query(self, *columns, snapshot: str = None):
        """Get async MatrixOne query builder - SQLAlchemy style

            Args::

                *columns: Can be:
                    - Single model class: query(Article) - returns all columns from model
                    - Multiple columns: query(Article.id, Article.title) - returns specific columns
                    - Mixed: query(Article, Article.id, some_expression.label('alias')) - model + additional columns
                snapshot: Optional snapshot name for snapshot queries

            Examples

        # Traditional model query (all columns)
                await client.query(Article).filter(...).all()

                # Column-specific query
                await client.query(Article.id, Article.title).filter(...).all()

                # With fulltext score
                await client.query(Article.id, boolean_match("title", "content").must("python").label("score"))

                # Snapshot query
                await client.query(Article, snapshot="my_snapshot").filter(...).all()

            Returns::

                AsyncMatrixOneQuery instance configured for the specified columns
        """
        from .async_orm import AsyncMatrixOneQuery

        if len(columns) == 1:
            # Traditional single model class usage
            column = columns[0]
            if isinstance(column, str):
                # String table name
                return AsyncMatrixOneQuery(column, self, None, None, snapshot)
            elif hasattr(column, '__tablename__'):
                # This is a model class
                return AsyncMatrixOneQuery(column, self, None, None, snapshot)
            elif hasattr(column, 'name') and hasattr(column, 'as_sql'):
                # This is a CTE object
                from .orm import CTE

                if isinstance(column, CTE):
                    query = AsyncMatrixOneQuery(None, self, None, None, snapshot)
                    query._table_name = column.name
                    query._select_columns = ["*"]  # Default to select all from CTE
                    query._ctes = [column]  # Add the CTE to the query
                    return query
            else:
                # This is a single column/expression - need to handle specially
                # For now, we'll create a query that can handle column selections
                query = AsyncMatrixOneQuery(None, self, None, None, snapshot)
                query._select_columns = [column]
                # Try to infer table name from column
                if hasattr(column, 'table') and hasattr(column.table, 'name'):
                    query._table_name = column.table.name
                return query
        else:
            # Multiple columns/expressions
            model_class = None
            select_columns = []

            for column in columns:
                if hasattr(column, '__tablename__'):
                    # This is a model class - use its table
                    model_class = column
                else:
                    # This is a column or expression
                    select_columns.append(column)

            if model_class:
                query = AsyncMatrixOneQuery(model_class, self, None, None, snapshot)
                if select_columns:
                    # Add additional columns to the model's default columns
                    query._select_columns = select_columns
                return query
            else:
                # No model class provided, need to infer table from columns
                query = AsyncMatrixOneQuery(None, self, None, None, snapshot)
                query._select_columns = select_columns

                # Try to infer table name from first column that has table info
                for col in select_columns:
                    if hasattr(col, 'table') and hasattr(col.table, 'name'):
                        query._table_name = col.table.name
                        break
                    elif isinstance(col, str) and '.' in col:
                        # String column like "table.column" - extract table name
                        parts = col.split('.')
                        if len(parts) >= 2:
                            # For "db.table.column" format, use "db.table"
                            # For "table.column" format, use "table"
                            table_name = '.'.join(parts[:-1])
                            query._table_name = table_name
                            break

                return query

    @asynccontextmanager
    async def snapshot(self, snapshot_name: str):
        """
        Snapshot context manager

        Usage

            async with client.snapshot("daily_backup") as snapshot_client:
            result = await snapshot_client.execute("SELECT * FROM users")
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        # Create a snapshot client wrapper
        from .client import SnapshotClient

        snapshot_client = SnapshotClient(self, snapshot_name)
        yield snapshot_client

    async def insert(self, table_name_or_model, data: dict) -> "AsyncResultSet":
        """
        Insert data into a table asynchronously.

        Args::

            table_name_or_model: Either a table name (str) or a SQLAlchemy model class
            data: Data to insert (dict with column names as keys)

        Returns::

            AsyncResultSet object
        """
        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            # It's a model class
            table_name = table_name_or_model.__tablename__
        else:
            # It's a table name string
            table_name = table_name_or_model

        executor = AsyncClientExecutor(self)
        return await executor.insert(table_name, data)

    async def batch_insert(self, table_name_or_model, data_list: list) -> "AsyncResultSet":
        """
        Batch insert data into a table asynchronously.

        Args::

            table_name_or_model: Either a table name (str) or a SQLAlchemy model class
            data_list: List of data dictionaries to insert

        Returns::

            AsyncResultSet object
        """
        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            # It's a model class
            table_name = table_name_or_model.__tablename__
        else:
            # It's a table name string
            table_name = table_name_or_model

        executor = AsyncClientExecutor(self)
        return await executor.batch_insert(table_name, data_list)

    @asynccontextmanager
    async def transaction(self):
        """
        Async transaction context manager

        Usage

            async with client.transaction() as tx:
            await tx.execute("INSERT INTO users ...")
            await tx.execute("UPDATE users ...")
            # Snapshot and clone operations within transaction
            await tx.snapshots.create("snap1", "table", database="db1", table="t1")
            await tx.clone.clone_database("target_db", "source_db")
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        tx_wrapper = None
        try:
            # Use SQLAlchemy async engine for transaction
            async with self._engine.begin() as conn:
                tx_wrapper = AsyncTransactionWrapper(conn, self)
                yield tx_wrapper

        except Exception as e:
            # Transaction will be automatically rolled back by SQLAlchemy
            raise e
        finally:
            # Clean up transaction wrapper
            if tx_wrapper:
                await tx_wrapper.close_sqlalchemy()

    @property
    def snapshots(self) -> AsyncSnapshotManager:
        """Get async snapshot manager"""
        return self._snapshots

    @property
    def clone(self) -> AsyncCloneManager:
        """Get async clone manager"""
        return self._clone

    @property
    def moctl(self) -> AsyncMoCtlManager:
        """Get async mo_ctl manager"""
        return self._moctl

    @property
    def restore(self) -> AsyncRestoreManager:
        """Get async restore manager"""
        return self._restore

    @property
    def pitr(self) -> AsyncPitrManager:
        """Get async PITR manager"""
        return self._pitr

    @property
    def pubsub(self) -> AsyncPubSubManager:
        """Get async publish-subscribe manager"""
        return self._pubsub

    @property
    def account(self) -> AsyncAccountManager:
        """Get async account manager"""
        return self._account

    def connected(self) -> bool:
        """Check if client is connected to database"""
        return self._engine is not None

    @property
    def vector_ops(self):
        """Get unified vector operations manager for vector operations (index and data)"""
        return self._vector

    def get_pinecone_index(self, table_name_or_model, vector_column: str):
        """
        Get a PineconeCompatibleIndex object for vector search operations.

        This method creates a Pinecone-compatible vector search interface
        that automatically parses the table schema and vector index configuration.
        The primary key column is automatically detected, and all other columns
        except the vector column will be included as metadata.

        Args::

            table_name_or_model: Either a table name (str) or a SQLAlchemy model class
            vector_column: Name of the vector column

        Returns::

            PineconeCompatibleIndex object with Pinecone-compatible API

        Example::

            index = await client.get_pinecone_index("documents", "embedding")
            results = await index.query_async([0.1, 0.2, 0.3], top_k=5)
            for match in results.matches:
                print(f"ID: {match.id}, Score: {match.score}")
        """
        from .search_vector_index import PineconeCompatibleIndex

        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            table_name = table_name_or_model.__tablename__
        else:
            table_name = table_name_or_model

        return PineconeCompatibleIndex(
            client=self,
            table_name=table_name,
            vector_column=vector_column,
        )

    @property
    def fulltext_index(self):
        """Get fulltext index manager for fulltext index operations"""
        return self._fulltext_index

    async def version(self) -> str:
        """
            Get MatrixOne server version asynchronously

            Returns::

                str: MatrixOne server version string

            Raises::

                ConnectionError: If not connected to MatrixOne
                QueryError: If version query fails

            Example

        >>> client = AsyncClient()
                >>> await client.connect('localhost', 6001, 'root', '111', 'test')
                >>> version = await client.version()
                >>> print(f"MatrixOne version: {version}")
        """
        if not self.connected():
            raise ConnectionError("Not connected to MatrixOne")

        try:
            result = await self.execute("SELECT VERSION()")
            if result.rows:
                return result.rows[0][0]
            else:
                raise QueryError("Failed to get version information")
        except Exception as e:
            raise QueryError(f"Failed to get version: {e}")

    async def git_version(self) -> str:
        """
            Get MatrixOne git version information asynchronously

            Returns::

                str: MatrixOne git version string

            Raises::

                ConnectionError: If not connected to MatrixOne
                QueryError: If git version query fails

            Example

        >>> client = AsyncClient()
                >>> await client.connect('localhost', 6001, 'root', '111', 'test')
                >>> git_version = await client.git_version()
                >>> print(f"MatrixOne git version: {git_version}")
        """
        if not self.connected():
            raise ConnectionError("Not connected to MatrixOne")

        try:
            # Use MatrixOne's built-in git_version() function
            result = await self.execute("SELECT git_version()")
            if result.rows:
                return result.rows[0][0]
            else:
                raise QueryError("Failed to get git version information")
        except Exception as e:
            raise QueryError(f"Failed to get git version: {e}")

    async def get_secondary_index_tables(self, table_name: str) -> List[str]:
        """
        Get all secondary index table names for a given table in the current database (async version).

        Args:
            table_name: Name of the table to get secondary indexes for

        Returns:
            List of secondary index table names

        Examples::

            >>> async with AsyncClient() as client:
            ...     await client.connect(host='localhost', port=6001, user='root', password='111', database='test')
            ...     index_tables = await client.get_secondary_index_tables('cms_all_content_chunk_info')
            ...     print(index_tables)
        """
        from .index_utils import build_get_index_tables_sql

        # Get current database from connection params
        database = self._connection_params.get('database') if hasattr(self, '_connection_params') else None

        sql, params = build_get_index_tables_sql(table_name, database)
        result = await self.execute(sql, params)
        return [row[0] for row in result.fetchall()]

    async def get_secondary_index_table_by_name(self, table_name: str, index_name: str) -> Optional[str]:
        """
        Get the physical table name of a secondary index by its index name in the current database (async version).

        Args:
            table_name: Name of the table
            index_name: Name of the secondary index

        Returns:
            Physical table name of the secondary index, or None if not found

        Examples::

            >>> async with AsyncClient() as client:
            ...     await client.connect(host='localhost', port=6001, user='root', password='111', database='test')
            ...     index_table = await client.get_secondary_index_table_by_name('cms_all_content_chunk_info', 'cms_id')
            ...     print(index_table)
        """
        from .index_utils import build_get_index_table_by_name_sql

        # Get current database from connection params
        database = self._connection_params.get('database') if hasattr(self, '_connection_params') else None

        sql, params = build_get_index_table_by_name_sql(table_name, index_name, database)
        result = await self.execute(sql, params)
        row = result.fetchone()
        return row[0] if row else None

    async def verify_table_index_counts(self, table_name: str) -> int:
        """
        Verify that the main table and all its secondary index tables have the same row count (async version).

        This method compares the COUNT(*) of the main table with all its secondary index tables
        in a single SQL query for consistency. If counts don't match, raises an exception.

        Args:
            table_name: Name of the table to verify

        Returns:
            Row count (int) if verification succeeds

        Raises:
            ValueError: If any secondary index table has a different count than the main table,
                       with details about all counts in the error message

        Examples::

            >>> async with AsyncClient() as client:
            ...     await client.connect(host='localhost', port=6001, user='root', password='111', database='test')
            ...     count = await client.verify_table_index_counts('cms_all_content_chunk_info')
            ...     print(f"✓ Verification passed, row count: {count}")

            >>> # If verification fails:
            >>> try:
            ...     count = await client.verify_table_index_counts('some_table')
            ... except ValueError as e:
            ...     print(f"Verification failed: {e}")
        """
        from .index_utils import build_verify_counts_sql, process_verify_result

        # Get all secondary index tables
        index_tables = await self.get_secondary_index_tables(table_name)

        # Build and execute verification SQL
        sql = build_verify_counts_sql(table_name, index_tables)
        result = await self.execute(sql)
        row = result.fetchone()

        # Process result and raise exception if verification fails
        return process_verify_result(table_name, index_tables, row)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Only disconnect if we're actually connected
        if self.connected():
            await self.disconnect()

    async def create_table(self, table_name_or_model, columns: dict = None, **kwargs) -> "AsyncClient":
        """
            Create a table asynchronously

            Args::

                table_name_or_model: Either a table name (str) or a SQLAlchemy model class
                columns: Dictionary mapping column names to their definitions (required if table_name_or_model is str)
                **kwargs: Additional table creation options

            Returns::

                AsyncClient: Self for chaining

            Example

        >>> await client.create_table("users", {
                ...     "id": "int primary key",
                ...     "name": "varchar(100)",
                ...     "email": "varchar(255)"
                ... })
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            # It's a model class
            model_class = table_name_or_model
            table_name = model_class.__tablename__
            # Use SQLAlchemy metadata to create table
            async with self._engine.begin() as conn:
                await conn.run_sync(model_class.__table__.create)
            return self

        # It's a table name string
        table_name = table_name_or_model
        if columns is None:
            raise ValueError("columns parameter is required when table_name_or_model is a string")

        # Build CREATE TABLE SQL
        column_definitions = []
        for column_name, column_def in columns.items():
            # Ensure vecf32/vecf64 format is lowercase for consistency
            if column_def.lower().startswith("vecf32(") or column_def.lower().startswith("vecf64("):
                column_def = column_def.lower()

            column_definitions.append(f"{column_name} {column_def}")

        sql = f"CREATE TABLE {table_name} ({', '.join(column_definitions)})"

        await self.execute(sql)
        return self

    async def drop_table(self, table_name_or_model) -> "AsyncClient":
        """
            Drop a table asynchronously

            Args::

                table_name_or_model: Either a table name (str) or a SQLAlchemy model class

            Returns::

                AsyncClient: Self for chaining

            Example

        >>> await client.drop_table("users")
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            # It's a model class
            table_name = table_name_or_model.__tablename__
        else:
            # It's a table name string
            table_name = table_name_or_model

        sql = f"DROP TABLE IF EXISTS {table_name}"
        await self.execute(sql)
        return self

    async def create_table_with_index(self, table_name: str, columns: dict, indexes: list = None, **kwargs) -> "AsyncClient":
        """
            Create a table with indexes asynchronously

            Args::

                table_name: Name of the table to create
                columns: Dictionary mapping column names to their definitions
                indexes: List of index definitions
                **kwargs: Additional table creation options

            Returns::

                AsyncClient: Self for chaining

            Example

        >>> await client.create_table_with_index("users", {
                ...     "id": "int primary key",
                ...     "name": "varchar(100)",
                ...     "email": "varchar(255)"
                ... }, [
                ...     {"name": "idx_name", "columns": ["name"]},
                ...     {"name": "idx_email", "columns": ["email"], "unique": True}
                ... ])
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        # Build CREATE TABLE SQL
        column_definitions = []
        for column_name, column_def in columns.items():
            column_definitions.append(f"{column_name} {column_def}")

        sql = f"CREATE TABLE {table_name} ({', '.join(column_definitions)})"
        await self.execute(sql)

        # Create indexes if provided
        if indexes:
            for index_def in indexes:
                index_name = index_def["name"]
                index_columns = ", ".join(index_def["columns"])
                unique = "UNIQUE " if index_def.get("unique", False) else ""
                index_sql = f"CREATE {unique}INDEX {index_name} ON {table_name} ({index_columns})"
                await self.execute(index_sql)

        return self

    async def create_table_orm(self, table_name: str, *columns, **kwargs) -> "AsyncClient":
        """
            Create a table using SQLAlchemy ORM asynchronously

            Args::

                table_name: Name of the table to create
                *columns: SQLAlchemy column definitions
                **kwargs: Additional table creation options

            Returns::

                AsyncClient: Self for chaining

            Example

        >>> from sqlalchemy import Column, Integer, String
                >>> await client.create_table_orm("users",
                ...     Column("id", Integer, primary_key=True),
                ...     Column("name", String(100)),
                ...     Column("email", String(255))
                ... )
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        from sqlalchemy import MetaData, Table

        metadata = MetaData()
        Table(table_name, metadata, *columns)

        # Create the table
        async with self._engine.begin() as conn:
            await conn.run_sync(metadata.create_all)

        return self

    @property
    def metadata(self) -> Optional["AsyncMetadataManager"]:
        """Get metadata manager for table metadata operations"""
        return self._metadata


class AsyncTransactionVectorIndexManager(AsyncVectorManager):
    """Async transaction-aware vector index manager"""

    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper

    async def execute(self, sql: str, params: Optional[Tuple] = None) -> AsyncResultSet:
        """Execute SQL within transaction"""
        return await self.transaction_wrapper.execute(sql, params)

    async def get_ivf_stats(self, table_name_or_model, column_name: str = None) -> Dict[str, Any]:
        """
            Get IVF index statistics for a table within transaction.

            Args::

                table_name_or_model: Either a table name (str) or a SQLAlchemy model class
                column_name: Name of the vector column (optional, will be inferred if not provided)

            Returns::

                Dict containing IVF index statistics including:
                - index_tables: Dictionary mapping table types to table names
                - distribution: Dictionary containing bucket distribution data
                - database: Database name
                - table_name: Table name
                - column_name: Vector column name

            Raises::

                Exception: If IVF index is not found or if there are errors retrieving stats

            Examples

        # Get stats for a table with vector column within transaction
                async with client.transaction() as tx:
                    stats = await tx.vector_ops.get_ivf_stats("my_table", "embedding")
                    print(f"Index tables: {stats['index_tables']}")
                    print(f"Distribution: {stats['distribution']}")
        """
        from sqlalchemy import text

        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            table_name = table_name_or_model.__tablename__
        else:
            table_name = table_name_or_model

        # Get database name from connection params
        database = self.client._connection_params.get('database')
        if not database:
            raise Exception("No database connection found. Please connect to a database first.")

        # If column_name is not provided, try to infer it
        if not column_name:
            # Query the table schema to find vector columns using transaction connection
            schema_sql = text(
                f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = '{database}'
                AND table_name = '{table_name}'
                AND (data_type LIKE '%VEC%' OR data_type LIKE '%vec%')
            """
            )
            result = await self.transaction_wrapper.execute(schema_sql)
            vector_columns = result.fetchall()

            if not vector_columns:
                raise Exception(f"No vector columns found in table {table_name}")
            elif len(vector_columns) == 1:
                column_name = vector_columns[0][0]
            else:
                # Multiple vector columns found, raise error asking user to specify
                column_names = [col[0] for col in vector_columns]
                raise Exception(
                    f"Multiple vector columns found in table {table_name}: {column_names}. "
                    f"Please specify the column_name parameter."
                )

        # Get connection from transaction wrapper
        connection = self.transaction_wrapper.connection

        # Get IVF index table names
        index_tables = await self._get_ivf_index_table_names(database, table_name, column_name, connection)

        if not index_tables:
            raise Exception(f"No IVF index found for table {table_name}, column {column_name}")

        # Get the entries table name for distribution analysis
        entries_table = index_tables.get('entries')
        if not entries_table:
            raise Exception("No entries table found in IVF index")

        # Get bucket distribution
        distribution = await self._get_ivf_buckets_distribution(database, entries_table, connection)

        return {
            'index_tables': index_tables,
            'distribution': distribution,
            'database': database,
            'table_name': table_name,
            'column_name': column_name,
        }


class AsyncTransactionWrapper:
    """Async transaction wrapper for executing queries within a transaction"""

    def __init__(self, connection, client):
        self.connection = connection
        self.client = client
        # Create snapshot, clone, restore, PITR, pubsub, account, and fulltext managers that use this transaction
        self.snapshots = AsyncTransactionSnapshotManager(client, self)
        self.clone = AsyncTransactionCloneManager(client, self)
        self.restore = AsyncTransactionRestoreManager(client, self)
        self.pitr = AsyncTransactionPitrManager(client, self)
        self.pubsub = AsyncTransactionPubSubManager(client, self)
        self.account = AsyncTransactionAccountManager(self)
        self.vector_ops = AsyncTransactionVectorIndexManager(client, self)
        self.fulltext_index = AsyncTransactionFulltextIndexManager(client, self)
        # SQLAlchemy integration
        self._sqlalchemy_session = None
        self._sqlalchemy_engine = None

    async def execute(self, sql: str, params: Optional[Tuple] = None) -> AsyncResultSet:
        """Execute SQL within transaction asynchronously"""
        import time

        start_time = time.time()

        try:
            # Handle parameter substitution for MatrixOne compatibility
            final_sql = self.client._substitute_parameters(sql, params)
            # Use exec_driver_sql() to bypass SQLAlchemy's bind parameter parsing
            # This prevents JSON strings like {"a":1} from being parsed as :1 bind params
            if hasattr(self.connection, 'exec_driver_sql'):
                # Escape % to %% for pymysql's format string handling
                escaped_sql = final_sql.replace('%', '%%')
                result = await self.connection.exec_driver_sql(escaped_sql)
            else:
                # Fallback for testing or older SQLAlchemy versions
                from sqlalchemy import text

                result = await self.connection.execute(text(final_sql))
            execution_time = time.time() - start_time

            if result.returns_rows:
                rows = result.fetchall()
                columns = list(result.keys()) if hasattr(result, "keys") else []
                self.client.logger.log_query(sql, execution_time, len(rows), success=True)
                return AsyncResultSet(columns, rows)
            else:
                self.client.logger.log_query(sql, execution_time, result.rowcount, success=True)
                return AsyncResultSet([], [], affected_rows=result.rowcount)

        except Exception as e:
            execution_time = time.time() - start_time
            self.client.logger.log_query(sql, execution_time, success=False)
            self.client.logger.log_error(e, context="Async transaction query execution")
            raise QueryError(f"Transaction query execution failed: {e}")

    def get_connection(self):
        """
        Get the underlying SQLAlchemy async connection for direct use

        Returns::

            SQLAlchemy AsyncConnection instance bound to this transaction
        """
        return self.connection

    async def get_sqlalchemy_session(self):
        """
        Get async SQLAlchemy session that uses the same transaction asynchronously

        Returns::

            Async SQLAlchemy Session instance bound to this transaction
        """
        if self._sqlalchemy_session is None:
            try:
                from sqlalchemy.ext.asyncio import (
                    AsyncSession,
                    async_sessionmaker,
                    create_async_engine,
                )
            except ImportError:
                # Fallback for older SQLAlchemy versions
                from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
                from sqlalchemy.orm import sessionmaker as sync_sessionmaker

                # Create a simple async sessionmaker equivalent
                def async_sessionmaker(bind=None, **kwargs):
                    # Remove class_ from kwargs if present to avoid conflicts
                    kwargs.pop('class_', None)
                    return sync_sessionmaker(bind=bind, class_=AsyncSession, **kwargs)

            # Create engine using the same connection parameters
            if not self.client._connection_params:
                raise ConnectionError("Not connected to database")

            connection_string = (
                f"mysql+aiomysql://{self.client._connection_params['user']}:"
                f"{self.client._connection_params['password']}@"
                f"{self.client._connection_params['host']}:"
                f"{self.client._connection_params['port']}/"
                f"{self.client._connection_params['database']}"
            )

            # Create async engine that will use the same connection
            self._sqlalchemy_engine = create_async_engine(connection_string, pool_pre_ping=True, pool_recycle=300)

            # Create async session factory
            AsyncSessionLocal = async_sessionmaker(bind=self._sqlalchemy_engine, class_=AsyncSession, expire_on_commit=False)
            self._sqlalchemy_session = AsyncSessionLocal()

            # Begin async SQLAlchemy transaction
            await self._sqlalchemy_session.begin()

        return self._sqlalchemy_session

    async def commit_sqlalchemy(self):
        """Commit async SQLAlchemy session asynchronously"""
        if self._sqlalchemy_session:
            await self._sqlalchemy_session.commit()

    async def rollback_sqlalchemy(self):
        """Rollback async SQLAlchemy session asynchronously"""
        if self._sqlalchemy_session:
            await self._sqlalchemy_session.rollback()

    async def close_sqlalchemy(self):
        """Close async SQLAlchemy session asynchronously"""
        if self._sqlalchemy_session:
            await self._sqlalchemy_session.close()
            self._sqlalchemy_session = None
        if self._sqlalchemy_engine:
            await self._sqlalchemy_engine.dispose()
            self._sqlalchemy_engine = None

    async def insert(self, table_name_or_model, data: dict) -> AsyncResultSet:
        """
        Insert data into a table within transaction asynchronously.

        Args::

            table_name_or_model: Either a table name (str) or a SQLAlchemy model class
            data: Data to insert (dict with column names as keys)

        Returns::

            AsyncResultSet object
        """
        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            # It's a model class
            table_name = table_name_or_model.__tablename__
        else:
            # It's a table name string
            table_name = table_name_or_model

        sql = self.client._build_insert_sql(table_name, data)
        return await self.execute(sql)

    async def batch_insert(self, table_name_or_model, data_list: list) -> AsyncResultSet:
        """
        Batch insert data into a table within transaction asynchronously.

        Args::

            table_name_or_model: Either a table name (str) or a SQLAlchemy model class
            data_list: List of data dictionaries to insert

        Returns::

            AsyncResultSet object
        """
        if not data_list:
            return AsyncResultSet([], [], affected_rows=0)

        # Handle model class input
        if hasattr(table_name_or_model, '__tablename__'):
            # It's a model class
            table_name = table_name_or_model.__tablename__
        else:
            # It's a table name string
            table_name = table_name_or_model

        sql = self.client._build_batch_insert_sql(table_name, data_list)
        return await self.execute(sql)

    def query(self, *columns, snapshot: str = None):
        """Get async MatrixOne query builder within transaction - SQLAlchemy style

        Args::

            *columns: Can be:
                - Single model class: query(Article) - returns all columns from model
                - Multiple columns: query(Article.id, Article.title) - returns specific columns
                - Mixed: query(Article, Article.id, some_expression.label('alias')) - model + additional columns
            snapshot: Optional snapshot name for snapshot queries

        Returns::

            AsyncMatrixOneQuery instance configured for the specified columns within transaction
        """
        from .async_orm import AsyncMatrixOneQuery

        if len(columns) == 1:
            # Traditional single model class usage
            column = columns[0]
            if isinstance(column, str):
                # String table name
                return AsyncMatrixOneQuery(column, self.client, None, transaction_wrapper=self, snapshot=snapshot)
            elif hasattr(column, '__tablename__'):
                # This is a model class
                return AsyncMatrixOneQuery(column, self.client, None, transaction_wrapper=self, snapshot=snapshot)
            elif hasattr(column, 'name') and hasattr(column, 'as_sql'):
                # This is a CTE object
                from .orm import CTE

                if isinstance(column, CTE):
                    query = AsyncMatrixOneQuery(None, self.client, None, transaction_wrapper=self, snapshot=snapshot)
                    query._table_name = column.name
                    query._select_columns = ["*"]  # Default to select all from CTE
                    query._ctes = [column]  # Add the CTE to the query
                    return query
            else:
                # This is a single column/expression - need to handle specially
                # For now, we'll create a query that can handle column selections
                query = AsyncMatrixOneQuery(None, self.client, None, transaction_wrapper=self, snapshot=snapshot)
                query._select_columns = [column]
                # Try to infer table name from column
                if hasattr(column, 'table') and hasattr(column.table, 'name'):
                    query._table_name = column.table.name
                return query
        else:
            # Multiple columns/expressions
            model_class = None
            select_columns = []

            for column in columns:
                if hasattr(column, '__tablename__'):
                    # This is a model class - use its table
                    model_class = column
                else:
                    # This is a column or expression
                    select_columns.append(column)

            if model_class:
                query = AsyncMatrixOneQuery(model_class, self.client, None, transaction_wrapper=self, snapshot=snapshot)
                if select_columns:
                    # Add additional columns to the model's default columns
                    query._select_columns = select_columns
                return query
            else:
                # No model class provided, need to infer table from columns
                query = AsyncMatrixOneQuery(None, self.client, None, transaction_wrapper=self, snapshot=snapshot)
                query._select_columns = select_columns

                # Try to infer table name from first column that has table info
                for col in select_columns:
                    if hasattr(col, 'table') and hasattr(col.table, 'name'):
                        query._table_name = col.table.name
                        break
                    elif isinstance(col, str) and '.' in col:
                        # String column like "table.column" - extract table name
                        parts = col.split('.')
                        if len(parts) >= 2:
                            # For "db.table.column" format, use "db.table"
                            # For "table.column" format, use "table"
                            table_name = '.'.join(parts[:-1])
                            query._table_name = table_name
                            break

                return query


class AsyncTransactionSnapshotManager(AsyncSnapshotManager):
    """Async snapshot manager that executes operations within a transaction"""

    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper

    async def create(
        self,
        name: str,
        level: Union[str, SnapshotLevel],
        database: Optional[str] = None,
        table: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Snapshot:
        """Create snapshot within transaction asynchronously"""
        return await super().create(name, level, database, table, description)

    async def get(self, name: str) -> Snapshot:
        """Get snapshot within transaction asynchronously"""
        return await super().get(name)

    async def delete(self, name: str) -> None:
        """Delete snapshot within transaction asynchronously"""
        return await super().delete(name)


class AsyncTransactionPubSubManager(AsyncPubSubManager):
    """Async publish-subscribe manager for use within transactions"""

    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper

    async def create_database_publication(self, name: str, database: str, account: str) -> Publication:
        """Create database publication within transaction asynchronously"""
        try:
            sql = (
                f"CREATE PUBLICATION {self.client._escape_identifier(name)} "
                f"DATABASE {self.client._escape_identifier(database)} "
                f"ACCOUNT {self.client._escape_identifier(account)}"
            )

            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create database publication '{name}'")

            return await self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to create database publication '{name}': {e}")

    async def create_table_publication(self, name: str, database: str, table: str, account: str) -> Publication:
        """Create table publication within transaction asynchronously"""
        try:
            sql = (
                f"CREATE PUBLICATION {self.client._escape_identifier(name)} "
                f"DATABASE {self.client._escape_identifier(database)} "
                f"TABLE {self.client._escape_identifier(table)} "
                f"ACCOUNT {self.client._escape_identifier(account)}"
            )

            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create table publication '{name}'")

            return await self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to create table publication '{name}': {e}")

    async def get_publication(self, name: str) -> Publication:
        """Get publication within transaction asynchronously"""
        try:
            sql = f"SHOW PUBLICATIONS WHERE pub_name = {self.client._escape_string(name)}"
            result = await self.transaction_wrapper.execute(sql)

            if not result or not result.rows:
                raise PubSubError(f"Publication '{name}' not found")

            row = result.rows[0]
            return self._row_to_publication(row)

        except Exception as e:
            raise PubSubError(f"Failed to get publication '{name}': {e}")

    async def list_publications(self, account: Optional[str] = None, database: Optional[str] = None) -> List[Publication]:
        """List publications within transaction asynchronously"""
        try:
            # SHOW PUBLICATIONS doesn't support WHERE clause, so we need to list all and filter
            sql = "SHOW PUBLICATIONS"
            result = await self.transaction_wrapper.execute(sql)

            if not result or not result.rows:
                return []

            publications = []
            for row in result.rows:
                pub = self._row_to_publication(row)

                # Apply filters
                if account and account not in pub.sub_account:
                    continue
                if database and pub.database != database:
                    continue

                publications.append(pub)

            return publications

        except Exception as e:
            raise PubSubError(f"Failed to list publications: {e}")

    async def alter_publication(
        self,
        name: str,
        account: Optional[str] = None,
        database: Optional[str] = None,
        table: Optional[str] = None,
    ) -> Publication:
        """Alter publication within transaction asynchronously"""
        try:
            # Build ALTER PUBLICATION statement
            parts = [f"ALTER PUBLICATION {self.client._escape_identifier(name)}"]

            if account:
                parts.append(f"ACCOUNT {self.client._escape_identifier(account)}")
            if database:
                parts.append(f"DATABASE {self.client._escape_identifier(database)}")
            if table:
                parts.append(f"TABLE {self.client._escape_identifier(table)}")

            sql = " ".join(parts)
            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to alter publication '{name}'")

            return await self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to alter publication '{name}': {e}")

    async def drop_publication(self, name: str) -> bool:
        """Drop publication within transaction asynchronously"""
        try:
            sql = f"DROP PUBLICATION {self.client._escape_identifier(name)}"
            result = await self.transaction_wrapper.execute(sql)
            return result is not None

        except Exception as e:
            raise PubSubError(f"Failed to drop publication '{name}': {e}")

    async def create_subscription(
        self, subscription_name: str, publication_name: str, publisher_account: str
    ) -> Subscription:
        """Create subscription within transaction asynchronously"""
        try:
            sql = (
                f"CREATE DATABASE {self.client._escape_identifier(subscription_name)} "
                f"FROM {self.client._escape_identifier(publisher_account)} "
                f"PUBLICATION {self.client._escape_identifier(publication_name)}"
            )

            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create subscription '{subscription_name}'")

            return await self.get_subscription(subscription_name)

        except Exception as e:
            raise PubSubError(f"Failed to create subscription '{subscription_name}': {e}")

    async def get_subscription(self, name: str) -> Subscription:
        """Get subscription within transaction asynchronously"""
        try:
            # SHOW SUBSCRIPTIONS doesn't support WHERE clause, so we need to list all and filter
            sql = "SHOW SUBSCRIPTIONS"
            result = await self.transaction_wrapper.execute(sql)

            if not result or not result.rows:
                raise PubSubError(f"Subscription '{name}' not found")

            # Find subscription with matching name
            for row in result.rows:
                if row[6] == name:  # sub_name is in 7th column (index 6)
                    return self._row_to_subscription(row)

            raise PubSubError(f"Subscription '{name}' not found")

        except Exception as e:
            raise PubSubError(f"Failed to get subscription '{name}': {e}")

    async def list_subscriptions(
        self, pub_account: Optional[str] = None, pub_database: Optional[str] = None
    ) -> List[Subscription]:
        """List subscriptions within transaction asynchronously"""
        try:
            conditions = []

            if pub_account:
                conditions.append(f"pub_account = {self.client._escape_string(pub_account)}")
            if pub_database:
                conditions.append(f"pub_database = {self.client._escape_string(pub_database)}")

            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
            else:
                where_clause = ""

            sql = f"SHOW SUBSCRIPTIONS{where_clause}"
            result = await self.transaction_wrapper.execute(sql)

            if not result or not result.rows:
                return []

            return [self._row_to_subscription(row) for row in result.rows]

        except Exception as e:
            raise PubSubError(f"Failed to list subscriptions: {e}")


class AsyncTransactionPitrManager(AsyncPitrManager):
    """Async PITR manager for use within transactions"""

    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper

    async def create_cluster_pitr(self, name: str, range_value: int = 1, range_unit: str = "d") -> Pitr:
        """Create cluster PITR within transaction asynchronously"""
        try:
            self._validate_range(range_value, range_unit)

            sql = f"CREATE PITR {self.client._escape_identifier(name)} " f"FOR CLUSTER RANGE {range_value} '{range_unit}'"

            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create cluster PITR '{name}'")

            return await self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create cluster PITR '{name}': {e}")

    async def create_account_pitr(
        self,
        name: str,
        account_name: Optional[str] = None,
        range_value: int = 1,
        range_unit: str = "d",
    ) -> Pitr:
        """Create account PITR within transaction asynchronously"""
        try:
            self._validate_range(range_value, range_unit)

            if account_name:
                sql = (
                    f"CREATE PITR {self.client._escape_identifier(name)} "
                    f"FOR ACCOUNT {self.client._escape_identifier(account_name)} "
                    f"RANGE {range_value} '{range_unit}'"
                )
            else:
                sql = (
                    f"CREATE PITR {self.client._escape_identifier(name)} " f"FOR ACCOUNT RANGE {range_value} '{range_unit}'"
                )

            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create account PITR '{name}'")

            return await self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create account PITR '{name}': {e}")

    async def create_database_pitr(self, name: str, database_name: str, range_value: int = 1, range_unit: str = "d") -> Pitr:
        """Create database PITR within transaction asynchronously"""
        try:
            self._validate_range(range_value, range_unit)

            sql = (
                f"CREATE PITR {self.client._escape_identifier(name)} "
                f"FOR DATABASE {self.client._escape_identifier(database_name)} "
                f"RANGE {range_value} '{range_unit}'"
            )

            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create database PITR '{name}'")

            return await self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create database PITR '{name}': {e}")

    async def create_table_pitr(
        self,
        name: str,
        database_name: str,
        table_name: str,
        range_value: int = 1,
        range_unit: str = "d",
    ) -> Pitr:
        """Create table PITR within transaction asynchronously"""
        try:
            self._validate_range(range_value, range_unit)

            sql = (
                f"CREATE PITR {self.client._escape_identifier(name)} "
                f"FOR TABLE {self.client._escape_identifier(database_name)} "
                f"{self.client._escape_identifier(table_name)} "
                f"RANGE {range_value} '{range_unit}'"
            )

            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create table PITR '{name}'")

            return await self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create table PITR '{name}': {e}")

    async def get(self, name: str) -> Pitr:
        """Get PITR within transaction asynchronously"""
        try:
            sql = f"SHOW PITR WHERE pitr_name = {self.client._escape_string(name)}"
            result = await self.transaction_wrapper.execute(sql)

            if not result or not result.rows:
                raise PitrError(f"PITR '{name}' not found")

            row = result.rows[0]
            return self._row_to_pitr(row)

        except Exception as e:
            raise PitrError(f"Failed to get PITR '{name}': {e}")

    async def list(
        self,
        level: Optional[str] = None,
        account_name: Optional[str] = None,
        database_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> List[Pitr]:
        """List PITRs within transaction asynchronously"""
        try:
            conditions = []

            if level:
                conditions.append(f"pitr_level = {self.client._escape_string(level)}")
            if account_name:
                conditions.append(f"account_name = {self.client._escape_string(account_name)}")
            if database_name:
                conditions.append(f"database_name = {self.client._escape_string(database_name)}")
            if table_name:
                conditions.append(f"table_name = {self.client._escape_string(table_name)}")

            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
            else:
                where_clause = ""

            sql = f"SHOW PITR{where_clause}"
            result = await self.transaction_wrapper.execute(sql)

            if not result or not result.rows:
                return []

            return [self._row_to_pitr(row) for row in result.rows]

        except Exception as e:
            raise PitrError(f"Failed to list PITRs: {e}")

    async def alter(self, name: str, range_value: int, range_unit: str) -> Pitr:
        """Alter PITR within transaction asynchronously"""
        try:
            self._validate_range(range_value, range_unit)

            sql = f"ALTER PITR {self.client._escape_identifier(name)} " f"RANGE {range_value} '{range_unit}'"

            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PitrError(f"Failed to alter PITR '{name}'")

            return await self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to alter PITR '{name}': {e}")

    async def delete(self, name: str) -> bool:
        """Delete PITR within transaction asynchronously"""
        try:
            sql = f"DROP PITR {self.client._escape_identifier(name)}"
            result = await self.transaction_wrapper.execute(sql)
            return result is not None

        except Exception as e:
            raise PitrError(f"Failed to delete PITR '{name}': {e}")


class AsyncTransactionRestoreManager(AsyncRestoreManager):
    """Async restore manager for use within transactions"""

    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper

    async def restore_cluster(self, snapshot_name: str) -> bool:
        """Restore cluster within transaction asynchronously"""
        try:
            sql = f"RESTORE CLUSTER FROM SNAPSHOT {self.client._escape_identifier(snapshot_name)}"
            result = await self.transaction_wrapper.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore cluster from snapshot '{snapshot_name}': {e}")

    async def restore_tenant(self, snapshot_name: str, account_name: str, to_account: Optional[str] = None) -> bool:
        """Restore tenant within transaction asynchronously"""
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

            result = await self.transaction_wrapper.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore tenant '{account_name}' from snapshot '{snapshot_name}': {e}")

    async def restore_database(
        self,
        snapshot_name: str,
        account_name: str,
        database_name: str,
        to_account: Optional[str] = None,
    ) -> bool:
        """Restore database within transaction asynchronously"""
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

            result = await self.transaction_wrapper.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore database '{database_name}' from snapshot '{snapshot_name}': {e}")

    async def restore_table(
        self,
        snapshot_name: str,
        account_name: str,
        database_name: str,
        table_name: str,
        to_account: Optional[str] = None,
    ) -> bool:
        """Restore table within transaction asynchronously"""
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

            result = await self.transaction_wrapper.execute(sql)
            return result is not None
        except Exception as e:
            raise RestoreError(f"Failed to restore table '{table_name}' from snapshot '{snapshot_name}': {e}")


class AsyncTransactionCloneManager(AsyncCloneManager):
    """Async clone manager that executes operations within a transaction"""

    def __init__(self, client, transaction_wrapper):
        super().__init__(client)
        self.transaction_wrapper = transaction_wrapper

    async def clone_database(
        self,
        target_db: str,
        source_db: str,
        snapshot_name: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> None:
        """Clone database within transaction asynchronously"""
        return await super().clone_database(target_db, source_db, snapshot_name, if_not_exists)

    async def clone_table(
        self,
        target_table: str,
        source_table: str,
        snapshot_name: Optional[str] = None,
        if_not_exists: bool = False,
    ) -> None:
        """Clone table within transaction asynchronously"""
        return await super().clone_table(target_table, source_table, snapshot_name, if_not_exists)

    async def clone_table_with_snapshot(
        self, target_table: str, source_table: str, snapshot_name: str, if_not_exists: bool = False
    ) -> None:
        """Clone table with snapshot within transaction asynchronously"""
        return await super().clone_table_with_snapshot(target_table, source_table, snapshot_name, if_not_exists)


class AsyncTransactionAccountManager:
    """Async transaction-scoped account manager"""

    def __init__(self, transaction):
        self.transaction = transaction
        self.client = transaction.client

    async def create_account(
        self,
        account_name: str,
        admin_name: str,
        password: str,
        comment: Optional[str] = None,
        admin_comment: Optional[str] = None,
        admin_host: str = "%",
        admin_identified_by: Optional[str] = None,
    ) -> Account:
        """Create account within async transaction"""
        try:
            sql_parts = [f"CREATE ACCOUNT {self.client._escape_identifier(account_name)}"]
            sql_parts.append(f"ADMIN_NAME {self.client._escape_string(admin_name)}")
            sql_parts.append(f"IDENTIFIED BY {self.client._escape_string(password)}")

            if admin_host != "%":
                sql_parts.append(f"ADMIN_HOST {self.client._escape_string(admin_host)}")

            if comment:
                sql_parts.append(f"COMMENT {self.client._escape_string(comment)}")

            if admin_comment:
                sql_parts.append(f"ADMIN_COMMENT {self.client._escape_string(admin_comment)}")

            if admin_identified_by:
                sql_parts.append(f"ADMIN_IDENTIFIED BY {self.client._escape_string(admin_identified_by)}")

            sql = " ".join(sql_parts)
            await self.transaction.execute(sql)

            return await self.get_account(account_name)

        except Exception as e:
            raise AccountError(f"Failed to create account '{account_name}': {e}")

    async def drop_account(self, account_name: str) -> None:
        """Drop account within async transaction"""
        try:
            sql = f"DROP ACCOUNT {self.client._escape_identifier(account_name)}"
            await self.transaction.execute(sql)
        except Exception as e:
            raise AccountError(f"Failed to drop account '{account_name}': {e}")

    async def alter_account(
        self,
        account_name: str,
        comment: Optional[str] = None,
        suspend: Optional[bool] = None,
        suspend_reason: Optional[str] = None,
    ) -> Account:
        """Alter account within async transaction"""
        try:
            sql_parts = [f"ALTER ACCOUNT {self.client._escape_identifier(account_name)}"]

            if comment is not None:
                sql_parts.append(f"COMMENT {self.client._escape_string(comment)}")

            if suspend is not None:
                if suspend:
                    if suspend_reason:
                        sql_parts.append(f"SUSPEND COMMENT {self.client._escape_string(suspend_reason)}")
                    else:
                        sql_parts.append("SUSPEND")
                else:
                    sql_parts.append("OPEN")

            sql = " ".join(sql_parts)
            await self.transaction.execute(sql)

            return await self.get_account(account_name)

        except Exception as e:
            raise AccountError(f"Failed to alter account '{account_name}': {e}")

    async def get_account(self, account_name: str) -> Account:
        """Get account within async transaction"""
        try:
            sql = "SHOW ACCOUNTS"
            result = await self.transaction.execute(sql)

            if not result or not result.rows:
                raise AccountError(f"Account '{account_name}' not found")

            for row in result.rows:
                if row[0] == account_name:
                    return self._row_to_account(row)

            raise AccountError(f"Account '{account_name}' not found")

        except Exception as e:
            raise AccountError(f"Failed to get account '{account_name}': {e}")

    async def list_accounts(self) -> List[Account]:
        """List accounts within async transaction"""
        try:
            sql = "SHOW ACCOUNTS"
            result = await self.transaction.execute(sql)

            if not result or not result.rows:
                return []

            return [self._row_to_account(row) for row in result.rows]

        except Exception as e:
            raise AccountError(f"Failed to list accounts: {e}")

    async def create_user(self, user_name: str, password: str, comment: Optional[str] = None) -> User:
        """
        Create user within async transaction according to MatrixOne CREATE USER syntax:
        CREATE USER [IF NOT EXISTS] user auth_option [, user auth_option] ...
        [DEFAULT ROLE rolename] [COMMENT 'comment_string' | ATTRIBUTE 'json_object']

        Args::

            user_name: Name of the user to create
            password: Password for the user
            comment: Comment for the user (not supported in MatrixOne)

        Returns::

            User: Created user object
        """
        try:
            # Build CREATE USER statement according to MatrixOne syntax
            # MatrixOne syntax: CREATE USER user_name IDENTIFIED BY 'password'
            sql_parts = [f"CREATE USER {self.client._escape_identifier(user_name)}"]

            sql_parts.append(f"IDENTIFIED BY {self.client._escape_string(password)}")

            # Note: MatrixOne doesn't support COMMENT or ATTRIBUTE clauses in CREATE USER
            # sql_parts.append(f"ACCOUNT {self.client._escape_identifier(account_name)}")
            # if comment:
            #     sql_parts.append(f"COMMENT {self.client._escape_string(comment)}")
            # if identified_by:
            #     sql_parts.append(f"IDENTIFIED BY {self.client._escape_string(identified_by)}")

            sql = " ".join(sql_parts)
            await self.transaction.execute(sql)

            # Return a User object with current account context
            return User(
                name=user_name,
                host="%",  # Default host
                account="sys",  # Default account
                created_time=datetime.now(),
                status="ACTIVE",
                comment=comment,
            )

        except Exception as e:
            raise AccountError(f"Failed to create user '{user_name}': {e}")

    async def drop_user(self, user_name: str, if_exists: bool = False) -> None:
        """
        Drop user within async transaction according to MatrixOne DROP USER syntax:
        DROP USER [IF EXISTS] user [, user] ...

        Args::

            user_name: Name of the user to drop
            if_exists: If True, add IF EXISTS clause to avoid errors when user doesn't exist
        """
        try:
            sql_parts = ["DROP USER"]
            if if_exists:
                sql_parts.append("IF EXISTS")

            sql_parts.append(self.client._escape_identifier(user_name))
            sql = " ".join(sql_parts)
            await self.transaction.execute(sql)

        except Exception as e:
            raise AccountError(f"Failed to drop user '{user_name}': {e}")

    async def alter_user(
        self,
        user_name: str,
        password: Optional[str] = None,
        comment: Optional[str] = None,
        lock: Optional[bool] = None,
        lock_reason: Optional[str] = None,
    ) -> User:
        """Alter user within async transaction"""
        try:
            sql_parts = [f"ALTER USER {self.client._escape_identifier(user_name)}"]

            if password is not None:
                sql_parts.append(f"IDENTIFIED BY {self.client._escape_string(password)}")

            if comment is not None:
                sql_parts.append(f"COMMENT {self.client._escape_string(comment)}")

            if lock is not None:
                if lock:
                    if lock_reason:
                        sql_parts.append(f"ACCOUNT LOCK COMMENT {self.client._escape_string(lock_reason)}")
                    else:
                        sql_parts.append("ACCOUNT LOCK")
                else:
                    sql_parts.append("ACCOUNT UNLOCK")

            sql = " ".join(sql_parts)
            await self.transaction.execute(sql)

            return await self.get_user(user_name)

        except Exception as e:
            raise AccountError(f"Failed to alter user '{user_name}': {e}")

    async def get_user(self, user_name: str) -> User:
        """Get user within async transaction"""
        try:
            sql = "SHOW GRANTS"
            result = await self.transaction.execute(sql)

            if not result or not result.rows:
                raise AccountError(f"User '{user_name}' not found")

            for row in result.rows:
                if row[0] == user_name:
                    return self._row_to_user(row)

            raise AccountError(f"User '{user_name}' not found")

        except Exception as e:
            raise AccountError(f"Failed to get user '{user_name}': {e}")

    async def list_users(self, account_name: Optional[str] = None) -> List[User]:
        """List users within async transaction"""
        try:
            sql = "SHOW GRANTS"
            result = await self.transaction.execute(sql)

            if not result or not result.rows:
                return []

            users = [self._row_to_user(row) for row in result.rows]

            if account_name:
                users = [user for user in users if user.account == account_name]

            return users

        except Exception as e:
            raise AccountError(f"Failed to list users: {e}")

    def _row_to_account(self, row: tuple) -> Account:
        """Convert database row to Account object"""
        return Account(
            name=row[0],
            admin_name=row[1],
            created_time=row[2] if len(row) > 2 else None,
            status=row[3] if len(row) > 3 else None,
            comment=row[4] if len(row) > 4 else None,
            suspended_time=row[5] if len(row) > 5 else None,
            suspended_reason=row[6] if len(row) > 6 else None,
        )

    def _row_to_user(self, row: tuple) -> User:
        """Convert database row to User object"""
        return User(
            name=row[0],
            host=row[1],
            account=row[2],
            created_time=row[3] if len(row) > 3 else None,
            status=row[4] if len(row) > 4 else None,
            comment=row[5] if len(row) > 5 else None,
            locked_time=row[6] if len(row) > 6 else None,
            locked_reason=row[7] if len(row) > 7 else None,
        )
