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
MatrixOne Python SDK - Publish-Subscribe Manager
Provides Publish-Subscribe functionality for MatrixOne
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
        """
        Initialize Publication object

        Args::

            name: Publication name
            database: Database name
            tables: Tables (can be '*' for all tables or specific table names)
            sub_account: Subscriber account names (comma-separated)
            subscribed_accounts: Currently subscribed accounts
            created_time: Creation time
            update_time: Last update time
            comments: Publication comments
        """
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
        """
        Initialize Subscription object

        Args::

            pub_name: Publication name
            pub_account: Publisher account name
            pub_database: Publisher database name
            pub_tables: Publisher tables
            pub_comment: Publication comment
            pub_time: Publication creation time
            sub_name: Subscription name
            sub_time: Subscription creation time
            status: Subscription status
        """
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
        return f"<Subscription(pub_name='{self.pub_name}', sub_name='{self.sub_name}', " f"status={self.status})>"


class PubSubManager:
    """
    Manager for Publish-Subscribe operations in MatrixOne.

    This class provides comprehensive pub/sub functionality for real-time data
    distribution and event-driven architectures. It supports creating publications
    and subscriptions for database changes, enabling efficient data replication
    and real-time updates across multiple systems.

    Key Features:

    - Database publication creation and management
    - Subscription management for real-time updates
    - Event-driven data distribution
    - Integration with MatrixOne's replication system
    - Support for both database and table-level publications
    - Transaction-aware pub/sub operations

    Supported Operations:

    - Create and manage database publications
    - Create and manage table publications
    - Create and manage subscriptions
    - List and query publications and subscriptions
    - Monitor pub/sub status and performance

    Usage Examples::

        # Initialize pub/sub manager
        pubsub = client.pubsub

        # Create database publication
        publication = pubsub.create_database_publication(
            name='user_changes',
            database='my_database',
            account='my_account'
        )

        # Create table publication
        publication = pubsub.create_table_publication(
            name='user_updates',
            database='my_database',
            table='users',
            account='my_account'
        )

        # Create subscription
        subscription = pubsub.create_subscription(
            name='user_subscription',
            publication='user_changes',
            target_database='replica_database',
            account='my_account'
        )

        # List publications
        publications = pubsub.list_publications()

        # List subscriptions
        subscriptions = pubsub.list_subscriptions()

        # Get pub/sub status
        status = pubsub.get_publication_status('user_changes')

    Note: Pub/sub functionality requires MatrixOne version 1.0.0 or higher and
    appropriate replication infrastructure. Pub/sub operations may impact
    database performance and should be used judiciously.
    """

    def __init__(self, client):
        """Initialize PubSubManager with client connection"""
        self._client = client

    # Publication Operations

    def create_database_publication(self, name: str, database: str, account: str) -> Publication:
        """
            Create database-level publication

            Args::

                name: Publication name
                database: Database name to publish
                account: Subscriber account name

            Returns::

                Publication: Created publication object

            Raises::

                PubSubError: If publication creation fails

            Example

        >>> pub = client.pubsub.create_database_publication("db_pub", "central_db", "acc1")
        """
        try:
            sql = (
                f"CREATE PUBLICATION {self._client._escape_identifier(name)} "
                f"DATABASE {self._client._escape_identifier(database)} "
                f"ACCOUNT {self._client._escape_identifier(account)}"
            )

            result = self._client.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create database publication '{name}'") from None

            # Return publication object (we'll get the actual details via SHOW PUBLICATIONS)
            return self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to create database publication '{name}': {e}") from None

    def create_table_publication(self, name: str, database: str, table: str, account: str) -> Publication:
        """
            Create table-level publication

            Args::

                name: Publication name
                database: Database name
                table: Table name to publish
                account: Subscriber account name

            Returns::

                Publication: Created publication object

            Raises::

                PubSubError: If publication creation fails

            Example

        >>> pub = client.pubsub.create_table_publication("table_pub", "central_db", "products", "acc1")
        """
        try:
            sql = (
                f"CREATE PUBLICATION {self._client._escape_identifier(name)} "
                f"DATABASE {self._client._escape_identifier(database)} "
                f"TABLE {self._client._escape_identifier(table)} "
                f"ACCOUNT {self._client._escape_identifier(account)}"
            )

            result = self._client.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create table publication '{name}'") from None

            return self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to create table publication '{name}': {e}") from None

    def get_publication(self, name: str) -> Publication:
        """
        Get publication by name

        Args::

            name: Publication name

        Returns::

            Publication: Publication object

        Raises::

            PubSubError: If publication not found
        """
        try:
            # List all publications and find the one with matching name
            sql = "SHOW PUBLICATIONS"
            result = self._client.execute(sql)

            if not result or not result.rows:
                raise PubSubError(f"Publication '{name}' not found") from None

            # Find publication with matching name
            for row in result.rows:
                if row[0] == name:  # publication name is in first column
                    return self._row_to_publication(row)

            raise PubSubError(f"Publication '{name}' not found") from None

        except Exception as e:
            raise PubSubError(f"Failed to get publication '{name}': {e}") from None

    def list_publications(self, account: Optional[str] = None, database: Optional[str] = None) -> List[Publication]:
        """
        List publications with optional filters

        Args::

            account: Filter by subscriber account
            database: Filter by database name

        Returns::

            List[Publication]: List of publication objects
        """
        try:
            # For now, just list all publications since WHERE clause syntax may vary
            # In a real implementation, you would need to check the exact syntax supported
            sql = "SHOW PUBLICATIONS"
            result = self._client.execute(sql)

            if not result or not result.rows:
                return []

            publications = [self._row_to_publication(row) for row in result.rows]

            # Apply filters in Python if needed
            if account:
                publications = [pub for pub in publications if account in pub.sub_account]
            if database:
                publications = [pub for pub in publications if pub.database == database]

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
        """
        Alter publication

        Args::

            name: Publication name
            account: New subscriber account
            database: New database name
            table: New table name (for table-level publications)

        Returns::

            Publication: Updated publication object

        Raises::

            PubSubError: If publication alteration fails
        """
        try:
            # Build ALTER PUBLICATION statement
            parts = [f"ALTER PUBLICATION {self._client._escape_identifier(name)}"]

            if account:
                parts.append(f"ACCOUNT {self._client._escape_identifier(account)}")
            if database:
                parts.append(f"DATABASE {self._client._escape_identifier(database)}")
            if table:
                parts.append(f"TABLE {self._client._escape_identifier(table)}")

            sql = " ".join(parts)
            result = self._client.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to alter publication '{name}'") from None

            return self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to alter publication '{name}': {e}") from None

    def drop_publication(self, name: str) -> bool:
        """
        Drop publication

        Args::

            name: Publication name

        Returns::

            bool: True if deletion was successful

        Raises::

            PubSubError: If publication deletion fails
        """
        try:
            sql = f"DROP PUBLICATION {self._client._escape_identifier(name)}"
            result = self._client.execute(sql)
            return result is not None

        except Exception as e:
            raise PubSubError(f"Failed to drop publication '{name}': {e}") from None

    def show_create_publication(self, name: str) -> str:
        """
        Show CREATE PUBLICATION statement for a publication

        Args::

            name: Publication name

        Returns::

            str: CREATE PUBLICATION statement

        Raises::

            PubSubError: If publication not found or retrieval fails
        """
        try:
            sql = f"SHOW CREATE PUBLICATION {self._client._escape_identifier(name)}"
            result = self._client.execute(sql)

            if not result or not result.rows:
                raise PubSubError(f"Publication '{name}' not found") from None

            # The result should contain the CREATE statement
            # Assuming the CREATE statement is in the first column
            return result.rows[0][0]

        except Exception as e:
            raise PubSubError(f"Failed to show create publication '{name}': {e}") from None

    # Subscription Operations

    def create_subscription(self, subscription_name: str, publication_name: str, publisher_account: str) -> Subscription:
        """
            Create subscription from publication

            Args::

                subscription_name: Name for the subscription database
                publication_name: Name of the publication to subscribe to
                publisher_account: Publisher account name

            Returns::

                Subscription: Created subscription object

            Raises::

                PubSubError: If subscription creation fails

            Example

        >>> sub = client.pubsub.create_subscription("sub_db", "pub_name", "sys")
        """
        try:
            sql = (
                f"CREATE DATABASE {self._client._escape_identifier(subscription_name)} "
                f"FROM {self._client._escape_identifier(publisher_account)} "
                f"PUBLICATION {self._client._escape_identifier(publication_name)}"
            )

            result = self._client.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create subscription '{subscription_name}'") from None

            return self.get_subscription(subscription_name)

        except Exception as e:
            raise PubSubError(f"Failed to create subscription '{subscription_name}': {e}") from None

    def get_subscription(self, name: str) -> Subscription:
        """
        Get subscription by name

        Args::

            name: Subscription name

        Returns::

            Subscription: Subscription object

        Raises::

            PubSubError: If subscription not found
        """
        try:
            # List all subscriptions and find the one with matching name
            sql = "SHOW SUBSCRIPTIONS"
            result = self._client.execute(sql)

            if not result or not result.rows:
                raise PubSubError(f"Subscription '{name}' not found") from None

            # Find subscription with matching name
            for row in result.rows:
                if len(row) > 6 and row[6] == name:  # sub_name is in 7th column (index 6)
                    return self._row_to_subscription(row)

            raise PubSubError(f"Subscription '{name}' not found") from None

        except Exception as e:
            raise PubSubError(f"Failed to get subscription '{name}': {e}") from None

    def list_subscriptions(
        self, pub_account: Optional[str] = None, pub_database: Optional[str] = None
    ) -> List[Subscription]:
        """
        List subscriptions with optional filters

        Args::

            pub_account: Filter by publisher account
            pub_database: Filter by publisher database

        Returns::

            List[Subscription]: List of subscription objects
        """
        try:
            # List all subscriptions since WHERE clause syntax may vary
            sql = "SHOW SUBSCRIPTIONS"
            result = self._client.execute(sql)

            if not result or not result.rows:
                return []

            subscriptions = [self._row_to_subscription(row) for row in result.rows]

            # Apply filters in Python if needed
            if pub_account:
                subscriptions = [sub for sub in subscriptions if sub.pub_account == pub_account]
            if pub_database:
                subscriptions = [sub for sub in subscriptions if sub.pub_database == pub_database]

            return subscriptions

        except Exception as e:
            raise PubSubError(f"Failed to list subscriptions: {e}") from None

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


class TransactionPubSubManager(PubSubManager):
    """PubSubManager for use within transactions"""

    def __init__(self, client, transaction_wrapper):
        """Initialize TransactionPubSubManager with client and transaction wrapper"""
        super().__init__(client)
        self._transaction_wrapper = transaction_wrapper

    def create_database_publication(self, name: str, database: str, account: str) -> Publication:
        """Create database publication within transaction"""
        return self._create_publication_with_executor("database", name, database, account)

    def create_table_publication(self, name: str, database: str, table: str, account: str) -> Publication:
        """Create table publication within transaction"""
        return self._create_publication_with_executor("table", name, database, account, table)

    def _create_publication_with_executor(
        self, level: str, name: str, database: str, account: str, table: Optional[str] = None
    ) -> Publication:
        """Create publication with custom executor (for transaction support)"""
        try:
            if level == "database":
                sql = (
                    f"CREATE PUBLICATION {self._client._escape_identifier(name)} "
                    f"DATABASE {self._client._escape_identifier(database)} "
                    f"ACCOUNT {self._client._escape_identifier(account)}"
                )
            elif level == "table":
                sql = (
                    f"CREATE PUBLICATION {self._client._escape_identifier(name)} "
                    f"DATABASE {self._client._escape_identifier(database)} "
                    f"TABLE {self._client._escape_identifier(table)} "
                    f"ACCOUNT {self._client._escape_identifier(account)}"
                )
            else:
                raise PubSubError(f"Invalid publication level: {level}") from None

            result = self._transaction_wrapper.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create {level} publication '{name}'") from None

            return self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to create {level} publication '{name}': {e}") from None

    def get_publication(self, name: str) -> Publication:
        """Get publication within transaction"""
        try:
            sql = f"SHOW PUBLICATIONS WHERE pub_name = {self._client._escape_string(name)}"
            result = self._transaction_wrapper.execute(sql)

            if not result or not result.rows:
                raise PubSubError(f"Publication '{name}' not found") from None

            row = result.rows[0]
            return self._row_to_publication(row)

        except Exception as e:
            raise PubSubError(f"Failed to get publication '{name}': {e}") from None

    def list_publications(self, account: Optional[str] = None, database: Optional[str] = None) -> List[Publication]:
        """List publications within transaction"""
        try:
            # SHOW PUBLICATIONS doesn't support WHERE clause, so we need to list all and filter
            sql = "SHOW PUBLICATIONS"
            result = self._transaction_wrapper.execute(sql)

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
        """Alter publication within transaction"""
        try:
            # Build ALTER PUBLICATION statement
            parts = [f"ALTER PUBLICATION {self._client._escape_identifier(name)}"]

            if account:
                parts.append(f"ACCOUNT {self._client._escape_identifier(account)}")
            if database:
                parts.append(f"DATABASE {self._client._escape_identifier(database)}")
            if table:
                parts.append(f"TABLE {self._client._escape_identifier(table)}")

            sql = " ".join(parts)
            result = self._transaction_wrapper.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to alter publication '{name}'") from None

            return self.get_publication(name)

        except Exception as e:
            raise PubSubError(f"Failed to alter publication '{name}': {e}") from None

    def drop_publication(self, name: str) -> bool:
        """Drop publication within transaction"""
        try:
            sql = f"DROP PUBLICATION {self._client._escape_identifier(name)}"
            result = self._transaction_wrapper.execute(sql)
            return result is not None

        except Exception as e:
            raise PubSubError(f"Failed to drop publication '{name}': {e}") from None

    def create_subscription(self, subscription_name: str, publication_name: str, publisher_account: str) -> Subscription:
        """Create subscription within transaction"""
        try:
            sql = (
                f"CREATE DATABASE {self._client._escape_identifier(subscription_name)} "
                f"FROM {self._client._escape_identifier(publisher_account)} "
                f"PUBLICATION {self._client._escape_identifier(publication_name)}"
            )

            result = self._transaction_wrapper.execute(sql)
            if result is None:
                raise PubSubError(f"Failed to create subscription '{subscription_name}'") from None

            return self.get_subscription(subscription_name)

        except Exception as e:
            raise PubSubError(f"Failed to create subscription '{subscription_name}': {e}") from None

    def get_subscription(self, name: str) -> Subscription:
        """Get subscription within transaction"""
        try:
            # SHOW SUBSCRIPTIONS doesn't support WHERE clause, so we need to list all and filter
            sql = "SHOW SUBSCRIPTIONS"
            result = self._transaction_wrapper.execute(sql)

            if not result or not result.rows:
                raise PubSubError(f"Subscription '{name}' not found") from None

            # Find subscription with matching name
            for row in result.rows:
                if row[6] == name:  # sub_name is in 7th column (index 6)
                    return self._row_to_subscription(row)

            raise PubSubError(f"Subscription '{name}' not found") from None

        except Exception as e:
            raise PubSubError(f"Failed to get subscription '{name}': {e}") from None

    def list_subscriptions(
        self, pub_account: Optional[str] = None, pub_database: Optional[str] = None
    ) -> List[Subscription]:
        """List subscriptions within transaction"""
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
            result = self._transaction_wrapper.execute(sql)

            if not result or not result.rows:
                return []

            return [self._row_to_subscription(row) for row in result.rows]

        except Exception as e:
            raise PubSubError(f"Failed to list subscriptions: {e}") from None
