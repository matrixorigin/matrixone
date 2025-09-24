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
from typing import Any, Dict, List, Optional, Tuple, Union

from .account import Account, User
from .exceptions import (AccountError, ConnectionError, MoCtlError, PitrError,
                         PubSubError, QueryError, RestoreError, SnapshotError)
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

    def fetchall(self) -> List[Tuple]:
        """Fetch all rows"""
        return self.rows

    def fetchone(self) -> Optional[Tuple]:
        """Fetch one row"""
        return self.rows[0] if self.rows else None

    def fetchmany(self, size: int = 1) -> List[Tuple]:
        """Fetch many rows"""
        return self.rows[:size]

    def scalar(self) -> Any:
        """Get scalar value (first column of first row)"""
        if self.rows and self.columns:
            return self.rows[0][0]
        return None

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
        self, target_db: str, source_db: str, snapshot_name: Optional[str] = None, if_not_exists: bool = False
    ) -> None:
        """Clone database asynchronously"""
        if_not_exists_clause = "IF NOT EXISTS" if if_not_exists else ""

        if snapshot_name:
            sql = f"CREATE DATABASE {target_db} {if_not_exists_clause} CLONE {source_db} FOR SNAPSHOT '{snapshot_name}'"
        else:
            sql = f"CREATE DATABASE {target_db} {if_not_exists_clause} CLONE {source_db}"

        await self.client.execute(sql)

    async def clone_table(
        self, target_table: str, source_table: str, snapshot_name: Optional[str] = None, if_not_exists: bool = False
    ) -> None:
        """Clone table asynchronously"""
        if_not_exists_clause = "IF NOT EXISTS" if if_not_exists else ""

        if snapshot_name:
            sql = (
                f"CREATE TABLE {target_table} {if_not_exists_clause} "
                f"CLONE {source_table} FOR SNAPSHOT '{snapshot_name}'"
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

    async def list_publications(
        self, account: Optional[str] = None, database: Optional[str] = None
    ) -> List[Publication]:
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
        self, name: str, account: Optional[str] = None, database: Optional[str] = None, table: Optional[str] = None
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

            sql = (
                f"CREATE PITR {self.client._escape_identifier(name)} " f"FOR CLUSTER RANGE {range_value} '{range_unit}'"
            )

            result = await self.client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create cluster PITR '{name}'")

            return await self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create cluster PITR '{name}': {e}")

    async def create_account_pitr(
        self, name: str, account_name: Optional[str] = None, range_value: int = 1, range_unit: str = "d"
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
                    f"CREATE PITR {self.client._escape_identifier(name)} "
                    f"FOR ACCOUNT RANGE {range_value} '{range_unit}'"
                )

            result = await self.client.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create account PITR '{name}'")

            return await self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create account PITR '{name}': {e}")

    async def create_database_pitr(
        self, name: str, database_name: str, range_value: int = 1, range_unit: str = "d"
    ) -> Pitr:
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
        self, name: str, database_name: str, table_name: str, range_value: int = 1, range_unit: str = "d"
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
        self, snapshot_name: str, account_name: str, database_name: str, to_account: Optional[str] = None
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

    async def checkpoint(self) -> Dict[str, Any]:
        """Force checkpoint asynchronously"""
        return await self._execute_moctl("dn", "checkpoint", "")


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

        Args:
            user_name: Name of the user to create
            password: Password for the user
            comment: Comment for the user (not supported in MatrixOne)

        Returns:
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

        Args:
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
        self, table_name: str, name: str, columns: Union[str, List[str]], algorithm: str = "TF-IDF"
    ) -> "AsyncFulltextIndexManager":
        """
        Create a fulltext index using chain operations.

        Args:
            table_name: Target table name
            name: Index name
            columns: Column(s) to index
            algorithm: Fulltext algorithm type (TF-IDF or BM25)

        Returns:
            AsyncFulltextIndexManager: Self for chaining
        """
        from sqlalchemy import text

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

    async def drop(self, table_name: str, name: str) -> "AsyncFulltextIndexManager":
        """
        Drop a fulltext index using chain operations.

        Args:
            table_name: Target table name
            name: Index name

        Returns:
            AsyncFulltextIndexManager: Self for chaining
        """
        from sqlalchemy import text

        try:
            sql = f"DROP INDEX {name} ON {table_name}"

            async with self.client.get_sqlalchemy_engine().begin() as conn:
                await conn.execute(text(sql))

            return self
        except Exception as e:
            raise Exception(f"Failed to drop fulltext index {name} on table {table_name}: {e}")

    async def fulltext_search(
        self, table_name: str, columns: Union[str, List[str]], search_term: str, mode: str = "natural language mode"
    ):
        """
        Execute a fulltext search and return results.

        Args:
            table_name: Table to search in
            columns: Column(s) to search in
            search_term: Search term
            mode: Search mode

        Returns:
            AsyncResultSet: Search results
        """
        from .sqlalchemy_ext import FulltextSearchBuilder

        builder = FulltextSearchBuilder(table_name, columns).search(search_term).set_mode(mode)
        sql = builder.build_sql()
        return await self.client.execute(sql)

    async def fulltext_search_in_transaction(
        self,
        table_name: str,
        columns: Union[str, List[str]],
        search_term: str,
        mode: str = "natural language mode",
        connection=None,
    ):
        """
        Execute a fulltext search within an existing transaction and return results.

        Args:
            table_name: Table to search in
            columns: Column(s) to search in
            search_term: Search term
            mode: Search mode
            connection: Database connection (required for transaction support)

        Returns:
            AsyncResultSet: Search results

        Raises:
            ValueError: If connection is not provided
        """
        if connection is None:
            raise ValueError("connection parameter is required for transaction operations")

        from sqlalchemy import text

        from .sqlalchemy_ext import FulltextSearchBuilder

        builder = FulltextSearchBuilder(table_name, columns).search(search_term).set_mode(mode)
        sql = builder.build_sql()

        result = await connection.execute(text(sql))
        if result.returns_rows:
            columns = list(result.keys())
            rows = result.fetchall()
            return AsyncResultSet(columns, rows)
        else:
            return AsyncResultSet([], [], affected_rows=result.rowcount)


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

    async def fulltext_search(
        self, table_name: str, columns: Union[str, List[str]], search_term: str, mode: str = "natural language mode"
    ):
        """
        Execute a fulltext search within transaction and return results.

        Args:
            table_name: Table to search in
            columns: Column(s) to search in
            search_term: Search term
            mode: Search mode

        Returns:
            AsyncResultSet: Search results
        """
        from .sqlalchemy_ext import FulltextSearchBuilder

        builder = FulltextSearchBuilder(table_name, columns).search(search_term).set_mode(mode)
        sql = builder.build_sql()
        return await self.transaction_wrapper.execute(sql)


class AsyncClient:
    """
    MatrixOne Async Client

    Provides asynchronous connection and query functionality for MatrixOne database.
    """

    def __init__(
        self,
        connection_timeout: int = 30,
        query_timeout: int = 300,
        auto_commit: bool = True,
        charset: str = "utf8mb4",
        logger: Optional[MatrixOneLogger] = None,
        enable_performance_logging: bool = False,
        enable_sql_logging: bool = False,
        enable_full_sql_logging: bool = False,
        enable_slow_sql_logging: bool = False,
        enable_error_sql_logging: bool = False,
        slow_sql_threshold: float = 1.0,
    ):
        """
        Initialize MatrixOne async client

        Args:
            connection_timeout: Connection timeout in seconds
            query_timeout: Query timeout in seconds
            auto_commit: Enable auto-commit mode
            charset: Character set for connection
            logger: Custom logger instance. If None, creates a default logger
            enable_performance_logging: Enable performance logging
            enable_sql_logging: Enable basic SQL query logging
            enable_full_sql_logging: Enable full SQL query logging (no truncation)
            enable_slow_sql_logging: Enable slow SQL query logging
            enable_error_sql_logging: Enable error SQL query logging
            slow_sql_threshold: Threshold in seconds for slow SQL logging
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
                enable_performance_logging=enable_performance_logging,
                enable_sql_logging=enable_sql_logging,
                enable_full_sql_logging=enable_full_sql_logging,
                enable_slow_sql_logging=enable_slow_sql_logging,
                enable_error_sql_logging=enable_error_sql_logging,
                slow_sql_threshold=slow_sql_threshold,
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
        self._vector_index = None
        self._vector_query = None
        self._vector_data = None
        self._fulltext_index = None

    async def connect(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str = None,
        account: Optional[str] = None,
        role: Optional[str] = None,
    ):
        """
        Connect to MatrixOne database asynchronously

        Args:
            host: Database host
            port: Database port
            user: Username or login info in format "user", "account#user", or "account#user#role"
            password: Password
            database: Database name
            account: Optional account name (will be combined with user if user doesn't contain '#')
            role: Optional role name (will be combined with user if user doesn't contain '#')
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
                "charset": self.charset,
                "autocommit": self.auto_commit,
                "connect_timeout": self.connection_timeout,
            }

            # Create SQLAlchemy async engine instead of direct aiomysql connection
            self._engine = self._create_async_engine()

            # Test the connection by executing a simple query
            async with self._engine.begin() as conn:
                await conn.execute(text("SELECT 1"))

            # Initialize vector managers after successful connection
            self._initialize_vector_managers()

            self.logger.log_connection(host, port, final_user, database or "default", success=True)

        except Exception as e:
            self.logger.log_connection(host, port, final_user, database or "default", success=False)
            self.logger.log_error(e, context="Async connection")
            raise ConnectionError(f"Failed to connect to MatrixOne: {e}")

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
            from .async_vector_index_manager import AsyncVectorIndexManager
            from .client import VectorDataManager, VectorQueryManager

            self._vector_index = AsyncVectorIndexManager(self)
            self._vector_query = VectorQueryManager(self)
            self._vector_data = VectorDataManager(self)
            self._fulltext_index = AsyncFulltextIndexManager(self)
        except ImportError:
            # Vector managers not available
            self._vector_index = None
            self._vector_query = None
            self._vector_data = None
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
                self._vector_index = None
                self._vector_query = None
                self._vector_data = None
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
                self._vector_index = None
                self._vector_query = None
                self._vector_data = None
                self._fulltext_index = None

    def __del__(self):
        """Cleanup when object is garbage collected"""
        # Don't try to cleanup in __del__ as it can cause issues with event loops
        # The fixture should handle proper cleanup
        pass

    def get_sqlalchemy_engine(self) -> AsyncEngine:
        """
        Get SQLAlchemy async engine

        Returns:
            SQLAlchemy AsyncEngine
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")
        return self._engine

    async def execute(self, sql: str, params: Optional[Tuple] = None) -> AsyncResultSet:
        """
        Execute SQL query asynchronously using SQLAlchemy async engine

        Args:
            sql: SQL query string
            params: Query parameters

        Returns:
            AsyncResultSet with query results
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        import time

        start_time = time.time()

        try:
            async with self._engine.begin() as conn:
                result = await conn.execute(text(sql), params or {})

                execution_time = time.time() - start_time

                if result.returns_rows:
                    rows = result.fetchall()
                    columns = list(result.keys()) if hasattr(result, "keys") else []
                    async_result = AsyncResultSet(columns, rows)
                    self.logger.log_query(sql, execution_time, len(rows), success=True)
                    return async_result
                else:
                    async_result = AsyncResultSet([], [], affected_rows=result.rowcount)
                    self.logger.log_query(sql, execution_time, result.rowcount, success=True)
                    return async_result

        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.log_query(sql, execution_time, success=False)
            self.logger.log_error(e, context="Async query execution")
            raise QueryError(f"Query execution failed: {e}")

    def _build_login_info(
        self, user: str, account: Optional[str] = None, role: Optional[str] = None
    ) -> tuple[str, dict]:
        """
        Build final login info based on user parameter and optional account/role

        Args:
            user: Username or login info in format "user", "account#user", or "account#user#role"
            account: Optional account name
            role: Optional role name

        Returns:
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
                raise ValueError(
                    f"Invalid user format: '{user}'. Expected 'user', 'account#user', or 'account#user#role'"
                )

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

    def snapshot_query_builder(self, snapshot_name: str):
        """
        Get snapshot query builder

        Args:
            snapshot_name: Name of the snapshot

        Returns:
            SnapshotQueryBuilder instance
        """
        from .client import SnapshotQueryBuilder

        return SnapshotQueryBuilder(snapshot_name, self)

    @asynccontextmanager
    async def snapshot(self, snapshot_name: str):
        """
        Snapshot context manager

        Usage:
            async with client.snapshot("daily_backup") as snapshot_client:
                result = await snapshot_client.execute("SELECT * FROM users")
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        # Create a snapshot client wrapper
        from .client import SnapshotClient

        snapshot_client = SnapshotClient(self, snapshot_name)
        yield snapshot_client

    async def snapshot_query(self, snapshot_name: str, sql: str, params: Optional[Tuple] = None) -> AsyncResultSet:
        """
        Execute query with snapshot asynchronously

        Args:
            snapshot_name: Name of the snapshot
            sql: SQL query string
            params: Query parameters

        Returns:
            AsyncResultSet with query results
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        try:
            # Use MatrixOne snapshot syntax: {SNAPSHOT = 'name'}
            snapshot_sql = f"{sql} {{SNAPSHOT = '{snapshot_name}'}}"
            async with self._engine.begin() as conn:
                result = await conn.execute(text(snapshot_sql), params or {})

                if result.returns_rows:
                    rows = result.fetchall()
                    columns = list(result.keys()) if hasattr(result, "keys") else []
                    return AsyncResultSet(columns, rows)
                else:
                    return AsyncResultSet([], [], affected_rows=result.rowcount)

        except Exception as e:
            raise QueryError(f"Snapshot query execution failed: {e}")

    @asynccontextmanager
    async def transaction(self):
        """
        Async transaction context manager

        Usage:
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
    def vector_index(self):
        """Get vector index manager for vector index operations"""
        return self._vector_index

    def get_pinecone_index(self, table_name: str, vector_column: str):
        """
        Get a PineconeCompatibleIndex object for vector search operations.

        This method creates a Pinecone-compatible vector search interface
        that automatically parses the table schema and vector index configuration.
        The primary key column is automatically detected, and all other columns
        except the vector column will be included as metadata.

        Args:
            table_name: Name of the table containing vectors
            vector_column: Name of the vector column

        Returns:
            PineconeCompatibleIndex object with Pinecone-compatible API

        Example:
            >>> index = await client.get_pinecone_index("documents", "embedding")
            >>> results = await index.query_async([0.1, 0.2, 0.3], top_k=5)
            >>> for match in results.matches:
            ...     print(f"ID: {match.id}, Score: {match.score}")
            ...     print(f"Metadata: {match.metadata}")
        """
        from .search_vector_index import PineconeCompatibleIndex

        return PineconeCompatibleIndex(
            client=self,
            table_name=table_name,
            vector_column=vector_column,
        )

    @property
    def fulltext_index(self):
        """Get fulltext index manager for fulltext index operations"""
        return self._fulltext_index

    @property
    def vector_query(self):
        """Get vector query manager for vector query operations"""
        return self._vector_query

    @property
    def vector_data(self):
        """Get vector data manager for vector data operations"""
        return self._vector_data

    async def version(self) -> str:
        """
        Get MatrixOne server version asynchronously

        Returns:
            str: MatrixOne server version string

        Raises:
            ConnectionError: If not connected to MatrixOne
            QueryError: If version query fails

        Example:
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

        Returns:
            str: MatrixOne git version string

        Raises:
            ConnectionError: If not connected to MatrixOne
            QueryError: If git version query fails

        Example:
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

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Only disconnect if we're actually connected
        if self.connected():
            await self.disconnect()

    async def create_table(self, table_name: str, columns: dict, **kwargs) -> "AsyncClient":
        """
        Create a table asynchronously

        Args:
            table_name: Name of the table to create
            columns: Dictionary mapping column names to their definitions
            **kwargs: Additional table creation options

        Returns:
            AsyncClient: Self for chaining

        Example:
            >>> await client.create_table("users", {
            ...     "id": "int primary key",
            ...     "name": "varchar(100)",
            ...     "email": "varchar(255)"
            ... })
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        # Build CREATE TABLE SQL
        column_definitions = []
        for column_name, column_def in columns.items():
            column_definitions.append(f"{column_name} {column_def}")

        sql = f"CREATE TABLE {table_name} ({', '.join(column_definitions)})"

        await self.execute(sql)
        return self

    async def drop_table(self, table_name: str) -> "AsyncClient":
        """
        Drop a table asynchronously

        Args:
            table_name: Name of the table to drop

        Returns:
            AsyncClient: Self for chaining

        Example:
            >>> await client.drop_table("users")
        """
        if not self._engine:
            raise ConnectionError("Not connected to database")

        sql = f"DROP TABLE IF EXISTS {table_name}"
        await self.execute(sql)
        return self

    async def create_table_with_index(
        self, table_name: str, columns: dict, indexes: list = None, **kwargs
    ) -> "AsyncClient":
        """
        Create a table with indexes asynchronously

        Args:
            table_name: Name of the table to create
            columns: Dictionary mapping column names to their definitions
            indexes: List of index definitions
            **kwargs: Additional table creation options

        Returns:
            AsyncClient: Self for chaining

        Example:
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

        Args:
            table_name: Name of the table to create
            *columns: SQLAlchemy column definitions
            **kwargs: Additional table creation options

        Returns:
            AsyncClient: Self for chaining

        Example:
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
        self.fulltext_index = AsyncTransactionFulltextIndexManager(client, self)
        # SQLAlchemy integration
        self._sqlalchemy_session = None
        self._sqlalchemy_engine = None

    async def execute(self, sql: str, params: Optional[Tuple] = None) -> AsyncResultSet:
        """Execute SQL within transaction asynchronously"""
        try:
            result = await self.connection.execute(text(sql), params or {})

            if result.returns_rows:
                rows = result.fetchall()
                columns = list(result.keys()) if hasattr(result, "keys") else []
                return AsyncResultSet(columns, rows)
            else:
                return AsyncResultSet([], [], affected_rows=result.rowcount)

        except Exception as e:
            raise QueryError(f"Transaction query execution failed: {e}")

    async def get_sqlalchemy_session(self):
        """
        Get async SQLAlchemy session that uses the same transaction asynchronously

        Returns:
            Async SQLAlchemy Session instance bound to this transaction
        """
        if self._sqlalchemy_session is None:
            from sqlalchemy.ext.asyncio import (AsyncSession,
                                                async_sessionmaker,
                                                create_async_engine)

            # Create engine using the same connection parameters
            if not self.client._connection_params:
                raise ConnectionError("Not connected to database")

            connection_string = (
                f"mysql+aiomysql://{self.client._connection_params['user']}:"
                f"{self.client._connection_params['password']}@"
                f"{self.client._connection_params['host']}:"
                f"{self.client._connection_params['port']}/"
                f"{self.client._connection_params['db']}"
            )

            # Create async engine that will use the same connection
            self._sqlalchemy_engine = create_async_engine(connection_string, pool_pre_ping=True, pool_recycle=300)

            # Create async session factory
            AsyncSessionLocal = async_sessionmaker(
                bind=self._sqlalchemy_engine, class_=AsyncSession, expire_on_commit=False
            )
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

    async def list_publications(
        self, account: Optional[str] = None, database: Optional[str] = None
    ) -> List[Publication]:
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
        self, name: str, account: Optional[str] = None, database: Optional[str] = None, table: Optional[str] = None
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

            sql = (
                f"CREATE PITR {self.client._escape_identifier(name)} " f"FOR CLUSTER RANGE {range_value} '{range_unit}'"
            )

            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create cluster PITR '{name}'")

            return await self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create cluster PITR '{name}': {e}")

    async def create_account_pitr(
        self, name: str, account_name: Optional[str] = None, range_value: int = 1, range_unit: str = "d"
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
                    f"CREATE PITR {self.client._escape_identifier(name)} "
                    f"FOR ACCOUNT RANGE {range_value} '{range_unit}'"
                )

            result = await self.transaction_wrapper.execute(sql)
            if result is None:
                raise PitrError(f"Failed to create account PITR '{name}'")

            return await self.get(name)

        except Exception as e:
            raise PitrError(f"Failed to create account PITR '{name}': {e}")

    async def create_database_pitr(
        self, name: str, database_name: str, range_value: int = 1, range_unit: str = "d"
    ) -> Pitr:
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
        self, name: str, database_name: str, table_name: str, range_value: int = 1, range_unit: str = "d"
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
        self, snapshot_name: str, account_name: str, database_name: str, to_account: Optional[str] = None
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
        self, target_db: str, source_db: str, snapshot_name: Optional[str] = None, if_not_exists: bool = False
    ) -> None:
        """Clone database within transaction asynchronously"""
        return await super().clone_database(target_db, source_db, snapshot_name, if_not_exists)

    async def clone_table(
        self, target_table: str, source_table: str, snapshot_name: Optional[str] = None, if_not_exists: bool = False
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

        Args:
            user_name: Name of the user to create
            password: Password for the user
            comment: Comment for the user (not supported in MatrixOne)

        Returns:
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

        Args:
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
