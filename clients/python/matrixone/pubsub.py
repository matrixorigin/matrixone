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
MatrixOne Publish-Subscribe Management

Unified PubSub management supporting sync/async and client/session executors.
"""

from datetime import datetime
from typing import List, Optional

from .exceptions import PubSubError


class Publication:
    """Publication object"""

    def __init__(
        self,
        name: str,
        database: str,
        tables: str,
        sub_account: str,
        subscribed_accounts: str,
        created_time: Optional[datetime] = None,
        update_time: Optional[datetime] = None,
        comments: Optional[str] = None,
    ):
        """Initialize Publication object"""
        self.name = name
        self.database = database
        self.tables = tables
        self.sub_account = sub_account
        self.subscribed_accounts = subscribed_accounts
        self.created_time = created_time
        self.update_time = update_time
        self.comments = comments

    def __repr__(self):
        return (
            f"<Publication(name='{self.name}', database='{self.database}', "
            f"tables='{self.tables}', sub_account='{self.sub_account}')>"
        )


class Subscription:
    """Subscription object"""

    def __init__(
        self,
        pub_name: str,
        pub_account: str,
        pub_database: str,
        pub_tables: str,
        sub_name: str,
        pub_comment: Optional[str] = None,
        pub_time: Optional[datetime] = None,
        sub_time: Optional[datetime] = None,
        status: int = 0,
    ):
        """Initialize Subscription object"""
        self.pub_name = pub_name
        self.pub_account = pub_account
        self.pub_database = pub_database
        self.pub_tables = pub_tables
        self.pub_comment = pub_comment
        self.pub_time = pub_time
        self.sub_name = sub_name
        self.sub_time = sub_time
        self.status = status

    def __repr__(self):
        return (
            f"<Subscription(pub_name='{self.pub_name}', pub_account='{self.pub_account}', "
            f"pub_database='{self.pub_database}', sub_name='{self.sub_name}')>"
        )


class BasePubSubManager:
    """
    Base PubSub manager with shared logic for sync and async implementations.

    This base class contains all the SQL building and data parsing logic that is
    shared between sync and async implementations.
    """

    def __init__(self, client, executor=None):
        """
        Initialize base PubSub manager.

        Args:
            client: MatrixOne client instance
            executor: Optional executor (e.g., session) for executing SQL.
                     If None, uses client as executor
        """
        self._client = client
        self.executor = executor

    def _get_executor(self):
        """Get the executor for SQL execution (session or client)"""
        return self.executor if self.executor else self._client

    def _build_create_database_publication_sql(self, name: str, database: str, account: str) -> str:
        """Build CREATE PUBLICATION DATABASE SQL"""
        return (
            f"CREATE PUBLICATION {self._client._escape_identifier(name)} "
            f"DATABASE {self._client._escape_identifier(database)} "
            f"ACCOUNT {self._client._escape_identifier(account)}"
        )

    def _build_create_table_publication_sql(self, name: str, database: str, table: str, account: str) -> str:
        """Build CREATE PUBLICATION TABLE SQL"""
        return (
            f"CREATE PUBLICATION {self._client._escape_identifier(name)} "
            f"DATABASE {self._client._escape_identifier(database)} "
            f"TABLE {self._client._escape_identifier(table)} "
            f"ACCOUNT {self._client._escape_identifier(account)}"
        )

    def _build_alter_publication_sql(
        self,
        name: str,
        account: Optional[str] = None,
        database: Optional[str] = None,
        table: Optional[str] = None,
    ) -> str:
        """Build ALTER PUBLICATION SQL"""
        parts = [f"ALTER PUBLICATION {self._client._escape_identifier(name)}"]

        if account:
            parts.append(f"ACCOUNT {self._client._escape_identifier(account)}")
        if database:
            parts.append(f"DATABASE {self._client._escape_identifier(database)}")
        if table:
            parts.append(f"TABLE {self._client._escape_identifier(table)}")

        return " ".join(parts)

    def _build_create_subscription_sql(self, subscription_name: str, publication_name: str, publisher_account: str) -> str:
        """Build CREATE DATABASE (subscription) SQL"""
        return (
            f"CREATE DATABASE {self._client._escape_identifier(subscription_name)} "
            f"FROM {self._client._escape_identifier(publisher_account)} "
            f"PUBLICATION {self._client._escape_identifier(publication_name)}"
        )

    def _row_to_publication(self, row: tuple) -> Publication:
        """Convert database row to Publication object"""
        return Publication(
            name=row[0],
            database=row[1],
            tables=row[2] if len(row) > 2 else '*',
            sub_account=row[3] if len(row) > 3 else '',
            subscribed_accounts=row[4] if len(row) > 4 else '',
            created_time=row[5] if len(row) > 5 else None,
            update_time=row[6] if len(row) > 6 else None,
            comments=row[7] if len(row) > 7 else None,
        )

    def _row_to_subscription(self, row: tuple) -> Subscription:
        """Convert database row to Subscription object"""
        return Subscription(
            pub_name=row[0],
            pub_account=row[1],
            pub_database=row[2],
            pub_tables=row[3] if row[3] else '*',
            sub_name=row[6] if len(row) > 6 else '',
            pub_comment=row[4] if len(row) > 4 else None,
            pub_time=row[5] if len(row) > 5 else None,
            sub_time=row[7] if len(row) > 7 else None,
            status=0,
        )


class PubSubManager(BasePubSubManager):
    """
    Synchronous Publish-Subscribe management for MatrixOne.

    Provides comprehensive publish-subscribe functionality for real-time data distribution
    and event-driven architectures. Enables data sharing between accounts through publications
    and subscriptions, supporting both database and table-level granularity.

    Key Features:

    - **Database and table-level publications**: Publish entire databases or specific tables
    - **Cross-account data sharing**: Share data between different MatrixOne accounts
    - **Subscription management**: Create and manage data subscriptions
    - **Real-time data distribution**: Enable event-driven data flow
    - **Transaction-aware**: Full integration with transaction contexts
    - **Flexible granularity**: Choose database or table level for publications

    Executor Pattern:

    - If executor is None, uses self.client.execute (default client-level executor)
    - If executor is provided (e.g., session), uses executor.execute (transaction-aware)
    - All operations can participate in transactions when used via session

    Usage Examples::

        from matrixone import Client

        # Publisher account creates a publication
        publisher = Client(host='localhost', port=6001, user='publisher#root',
                          password='111', database='data')

        # Create database-level publication
        pub = publisher.pubsub.create_database_publication(
            name='analytics_data',
            database='analytics',
            account='subscriber_account'  # Allow this account to subscribe
        )
        print(f"Publication created: {pub.name}")

        # Create table-level publication
        pub_table = publisher.pubsub.create_table_publication(
            name='orders_data',
            database='production',
            table='orders',
            account='subscriber_account'
        )

        # List all publications
        publications = publisher.pubsub.list_publications()
        for p in publications:
            print(f"{p.name}: {p.database}.{p.tables}")

        # Alter publication (change account permissions)
        publisher.pubsub.alter_publication(
            name='analytics_data',
            account='new_subscriber_account'
        )

        # Subscriber account creates a subscription
        subscriber = Client(host='localhost', port=6001, user='subscriber#root',
                           password='222', database='sub_db')

        # Create subscription to published data
        sub = subscriber.pubsub.create_subscription(
            subscription_name='analytics_copy',  # Local database name
            publication_name='analytics_data',
            publisher_account='publisher_account'
        )

        # List all subscriptions
        subscriptions = subscriber.pubsub.list_subscriptions()
        for s in subscriptions:
            print(f"Subscribed to: {s.pub_account}.{s.pub_database}")

        # Drop subscription when no longer needed
        subscriber.pubsub.drop_subscription('analytics_copy')

        # Publisher drops publication
        publisher.pubsub.drop_publication('analytics_data')

        # Using within a transaction
        with publisher.session() as session:
            # Create multiple publications atomically
            session.pubsub.create_database_publication('pub1', 'db1', 'acc1')
            session.pubsub.create_table_publication('pub2', 'db2', 'table1', 'acc2')

    Implementation Notes:

        - SHOW PUBLICATIONS does not support WHERE clause, so filtering is done client-side
        - Publications enable cross-account data sharing in MatrixOne
        - Subscriptions appear as regular databases to the subscriber
        - Changes to published data are reflected in subscriptions

    See Also:

        - SnapshotManager: For point-in-time data sharing via snapshots
        - CloneManager: For one-time data cloning
        - AccountManager: For account and permission management
    """

    def create_database_publication(self, name: str, database: str, account: str) -> Publication:
        """Create database-level publication"""
        try:
            sql = self._build_create_database_publication_sql(name, database, account)
            result = self._get_executor().execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create database publication '{name}'")

            return self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to create database publication '{name}': {e}") from None

    def create_table_publication(self, name: str, database: str, table: str, account: str) -> Publication:
        """Create table-level publication"""
        try:
            sql = self._build_create_table_publication_sql(name, database, table, account)
            result = self._get_executor().execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create table publication '{name}'")

            return self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to create table publication '{name}': {e}") from None

    def get_publication(self, name: str) -> Publication:
        """Get publication by name"""
        try:
            # SHOW PUBLICATIONS doesn't support WHERE clause
            sql = "SHOW PUBLICATIONS"
            result = self._get_executor().execute(sql)

            if not result or not result.rows:
                raise PubSubError(f"Publication '{name}' not found")

            # Find publication with matching name
            for row in result.rows:
                if row[0] == name:
                    return self._row_to_publication(row)

            raise PubSubError(f"Publication '{name}' not found")

        except Exception as e:
            raise PubSubError(f"Failed to get publication '{name}': {e}") from None

    def list_publications(self, account: Optional[str] = None, database: Optional[str] = None) -> List[Publication]:
        """List publications with optional filters"""
        try:
            sql = "SHOW PUBLICATIONS"
            result = self._get_executor().execute(sql)

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
            raise PubSubError(f"Failed to list publications: {e}") from None

    def alter_publication(
        self,
        name: str,
        account: Optional[str] = None,
        database: Optional[str] = None,
        table: Optional[str] = None,
    ) -> Publication:
        """Alter publication"""
        try:
            sql = self._build_alter_publication_sql(name, account, database, table)
            result = self._get_executor().execute(sql)
            if result is None:
                raise PubSubError(f"Failed to alter publication '{name}'")

            return self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to alter publication '{name}': {e}") from None

    def drop_publication(self, name: str) -> bool:
        """Drop publication"""
        try:
            sql = f"DROP PUBLICATION {self._client._escape_identifier(name)}"
            result = self._get_executor().execute(sql)
            return result is not None

        except Exception as e:
            raise PubSubError(f"Failed to drop publication '{name}': {e}") from None

    def show_create_publication(self, name: str) -> str:
        """Show CREATE PUBLICATION statement"""
        try:
            sql = f"SHOW CREATE PUBLICATION {self._client._escape_identifier(name)}"
            result = self._get_executor().execute(sql)

            if not result or not result.rows:
                raise PubSubError(f"Publication '{name}' not found")

            return result.rows[0][1]  # CREATE statement is in second column

        except Exception as e:
            raise PubSubError(f"Failed to show create publication '{name}': {e}") from None

    def create_subscription(self, subscription_name: str, publication_name: str, publisher_account: str) -> Subscription:
        """Create subscription"""
        try:
            sql = self._build_create_subscription_sql(subscription_name, publication_name, publisher_account)
            result = self._get_executor().execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create subscription '{subscription_name}'")

            return self.get_subscription(subscription_name)

        except Exception as e:
            raise PubSubError(f"Failed to create subscription '{subscription_name}': {e}") from None

    def get_subscription(self, name: str) -> Subscription:
        """Get subscription by name"""
        try:
            # SHOW SUBSCRIPTIONS doesn't support WHERE clause
            sql = "SHOW SUBSCRIPTIONS"
            result = self._get_executor().execute(sql)

            if not result or not result.rows:
                raise PubSubError(f"Subscription '{name}' not found")

            # Find subscription with matching name
            for row in result.rows:
                if row[6] == name:  # sub_name is in 7th column (index 6)
                    return self._row_to_subscription(row)

            raise PubSubError(f"Subscription '{name}' not found")

        except Exception as e:
            raise PubSubError(f"Failed to get subscription '{name}': {e}") from None

    def list_subscriptions(
        self, pub_account: Optional[str] = None, pub_database: Optional[str] = None
    ) -> List[Subscription]:
        """List subscriptions with optional filters"""
        try:
            conditions = []

            if pub_account:
                conditions.append(f"pub_account = {self._client._escape_string(pub_account)}")
            if pub_database:
                conditions.append(f"pub_database = {self._client._escape_string(pub_database)}")

            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
            else:
                where_clause = ""

            sql = f"SHOW SUBSCRIPTIONS{where_clause}"
            result = self._get_executor().execute(sql)

            if not result or not result.rows:
                return []

            return [self._row_to_subscription(row) for row in result.rows]

        except Exception as e:
            raise PubSubError(f"Failed to list subscriptions: {e}") from None


class AsyncPubSubManager(BasePubSubManager):
    """
    Asynchronous Publish-Subscribe management for MatrixOne.

    Provides the same functionality as PubSubManager but with async/await support.
    Uses the same executor pattern to support both client and session contexts.
    Shares SQL building logic with the synchronous version via BasePubSubManager.
    """

    async def create_database_publication(self, name: str, database: str, account: str) -> Publication:
        """Create database-level publication asynchronously"""
        try:
            sql = self._build_create_database_publication_sql(name, database, account)
            result = await self._get_executor().execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create database publication '{name}'")

            return await self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to create database publication '{name}': {e}")

    async def create_table_publication(self, name: str, database: str, table: str, account: str) -> Publication:
        """Create table-level publication asynchronously"""
        try:
            sql = self._build_create_table_publication_sql(name, database, table, account)
            result = await self._get_executor().execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create table publication '{name}'")

            return await self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to create table publication '{name}': {e}")

    async def get_publication(self, name: str) -> Publication:
        """Get publication by name asynchronously"""
        try:
            # SHOW PUBLICATIONS doesn't support WHERE clause
            sql = "SHOW PUBLICATIONS"
            result = await self._get_executor().execute(sql)

            if not result or not result.rows:
                raise PubSubError(f"Publication '{name}' not found")

            # Find publication with matching name
            for row in result.rows:
                if row[0] == name:
                    return self._row_to_publication(row)

            raise PubSubError(f"Publication '{name}' not found")

        except Exception as e:
            raise PubSubError(f"Failed to get publication '{name}': {e}")

    async def list_publications(self, account: Optional[str] = None, database: Optional[str] = None) -> List[Publication]:
        """List publications with optional filters asynchronously"""
        try:
            sql = "SHOW PUBLICATIONS"
            result = await self._get_executor().execute(sql)

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
            sql = self._build_alter_publication_sql(name, account, database, table)
            result = await self._get_executor().execute(sql)
            if result is None:
                raise PubSubError(f"Failed to alter publication '{name}'")

            return await self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to alter publication '{name}': {e}")

    async def drop_publication(self, name: str) -> bool:
        """Drop publication asynchronously"""
        try:
            sql = f"DROP PUBLICATION {self._client._escape_identifier(name)}"
            result = await self._get_executor().execute(sql)
            return result is not None

        except Exception as e:
            raise PubSubError(f"Failed to drop publication '{name}': {e}")

    async def show_create_publication(self, name: str) -> str:
        """Show CREATE PUBLICATION statement asynchronously"""
        try:
            sql = f"SHOW CREATE PUBLICATION {self._client._escape_identifier(name)}"
            result = await self._get_executor().execute(sql)

            if not result or not result.rows:
                raise PubSubError(f"Publication '{name}' not found")

            return result.rows[0][1]  # CREATE statement is in second column

        except Exception as e:
            raise PubSubError(f"Failed to show create publication '{name}': {e}")

    async def create_subscription(
        self, subscription_name: str, publication_name: str, publisher_account: str
    ) -> Subscription:
        """Create subscription asynchronously"""
        try:
            sql = self._build_create_subscription_sql(subscription_name, publication_name, publisher_account)
            result = await self._get_executor().execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create subscription '{subscription_name}'")

            return await self.get_subscription(subscription_name)

        except Exception as e:
            raise PubSubError(f"Failed to create subscription '{subscription_name}': {e}")

    async def get_subscription(self, name: str) -> Subscription:
        """Get subscription by name asynchronously"""
        try:
            # SHOW SUBSCRIPTIONS doesn't support WHERE clause
            sql = "SHOW SUBSCRIPTIONS"
            result = await self._get_executor().execute(sql)

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
                conditions.append(f"pub_account = {self._client._escape_string(pub_account)}")
            if pub_database:
                conditions.append(f"pub_database = {self._client._escape_string(pub_database)}")

            if conditions:
                where_clause = " WHERE " + " AND ".join(conditions)
            else:
                where_clause = ""

            sql = f"SHOW SUBSCRIPTIONS{where_clause}"
            result = await self._get_executor().execute(sql)

            if not result or not result.rows:
                return []

            return [self._row_to_subscription(row) for row in result.rows]

        except Exception as e:
            raise PubSubError(f"Failed to list subscriptions: {e}")
