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
MatrixOne Account Management - Corrected implementation based on actual MatrixOne behavior

This module provides proper account, user, and role management for MatrixOne database.
Based on MatrixOne v25.2.2.2 documentation and actual testing.
"""

import re
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from matrixone.exceptions import AccountError

if TYPE_CHECKING:
    from matrixone.client import Client, TransactionWrapper


@dataclass
class Account:
    """MatrixOne Account information"""

    name: str
    admin_name: str
    created_time: Optional[datetime] = None
    status: Optional[str] = None
    comment: Optional[str] = None
    suspended_time: Optional[datetime] = None
    suspended_reason: Optional[str] = None

    def __str__(self) -> str:
        return f"Account(name='{self.name}', admin='{self.admin_name}', status='{self.status}')"

    def __repr__(self) -> str:
        return self.__str__()


@dataclass
class User:
    """MatrixOne User information"""

    name: str
    host: str
    account: str
    created_time: Optional[datetime] = None
    status: Optional[str] = None
    comment: Optional[str] = None
    locked_time: Optional[datetime] = None
    locked_reason: Optional[str] = None

    def __str__(self) -> str:
        return f"User(name='{self.name}', host='{self.host}', account='{self.account}', status='{self.status}')"

    def __repr__(self) -> str:
        return self.__str__()


@dataclass
class Role:
    """MatrixOne Role information"""

    name: str
    id: int
    created_time: Optional[datetime] = None
    comment: Optional[str] = None

    def __str__(self) -> str:
        return f"Role(name='{self.name}', id={self.id})"

    def __repr__(self) -> str:
        return self.__str__()


@dataclass
class Grant:
    """MatrixOne Grant (permission) information"""

    grant_statement: str
    privilege: Optional[str] = None
    object_type: Optional[str] = None
    object_name: Optional[str] = None
    user: Optional[str] = None

    def __str__(self) -> str:
        return f"Grant(privilege='{self.privilege}', object='{self.object_name}', user='{self.user}')"

    def __repr__(self) -> str:
        return self.__str__()


class AccountManager:
    """
    MatrixOne Account Manager for user and account management operations.

    This class provides comprehensive account and user management functionality
    for MatrixOne databases, including account creation, user management, role
    assignments, and permission grants.

    Key Features:

    - Account creation and management
    - User creation and authentication
    - Role-based access control (RBAC)
    - Permission grants and revocations
    - Account and user listing and querying
    - Integration with MatrixOne's security model

    Supported Operations:

    - Create and manage accounts with administrators
    - Create users within accounts
    - Assign roles to users
    - Grant and revoke permissions
    - List accounts, users, and roles
    - Query account and user information

    Usage Examples::

        # Create a new account
        account = client.account.create_account(
            account_name='company_account',
            admin_name='admin_user',
            password='secure_password',
            comment='Company main account'
        )

        # Create a user within an account
        user = client.account.create_user(
            username='john_doe',
            password='user_password',
            account='company_account',
            comment='Employee user'
        )

        # Grant permissions to a user
        client.account.grant_privilege(
            username='john_doe',
            account='company_account',
            privilege='SELECT',
            object_type='TABLE',
            object_name='employees'
        )

        # List all accounts
        accounts = client.account.list_accounts()

    Note: Account management operations require appropriate administrative
    privileges in MatrixOne.
    """

    def __init__(self, client: "Client"):
        self._client = client

    # Account Management
    def create_account(self, account_name: str, admin_name: str, password: str, comment: Optional[str] = None) -> Account:
        """
        Create a new account in MatrixOne

        Args::

            account_name: Name of the account to create
            admin_name: Name of the admin user for the account
            password: Password for the admin user
            comment: Comment for the account

        Returns::

            Account: Created account object

        Raises::

            AccountError: If account creation fails
        """
        try:
            # Build CREATE ACCOUNT statement according to MatrixOne syntax
            sql_parts = [f"CREATE ACCOUNT {self._client._escape_identifier(account_name)}"]
            sql_parts.append(f"ADMIN_NAME {self._client._escape_string(admin_name)}")
            sql_parts.append(f"IDENTIFIED BY {self._client._escape_string(password)}")

            if comment:
                sql_parts.append(f"COMMENT {self._client._escape_string(comment)}")

            sql = " ".join(sql_parts)
            self._client.execute(sql)

            # Return the created account
            return self.get_account(account_name)

        except Exception as e:
            raise AccountError(f"Failed to create account '{account_name}': {e}") from None

    def drop_account(self, account_name: str, if_exists: bool = False) -> None:
        """
        Drop an account

        Args::

            account_name: Name of the account to drop
            if_exists: If True, add IF EXISTS clause to avoid errors when account doesn't exist
        """
        try:
            sql_parts = ["DROP ACCOUNT"]
            if if_exists:
                sql_parts.append("IF EXISTS")
            sql_parts.append(self._client._escape_identifier(account_name))

            sql = " ".join(sql_parts)
            self._client.execute(sql)
        except Exception as e:
            raise AccountError(f"Failed to drop account '{account_name}': {e}") from None

    def alter_account(
        self,
        account_name: str,
        comment: Optional[str] = None,
        suspend: Optional[bool] = None,
        suspend_reason: Optional[str] = None,
    ) -> Account:
        """Alter an account"""
        try:
            sql_parts = [f"ALTER ACCOUNT {self._client._escape_identifier(account_name)}"]

            if comment is not None:
                sql_parts.append(f"COMMENT {self._client._escape_string(comment)}")

            if suspend is not None:
                if suspend:
                    if suspend_reason:
                        sql_parts.append(f"SUSPEND COMMENT {self._client._escape_string(suspend_reason)}")
                    else:
                        sql_parts.append("SUSPEND")
                else:
                    sql_parts.append("OPEN")

            sql = " ".join(sql_parts)
            self._client.execute(sql)

            return self.get_account(account_name)

        except Exception as e:
            raise AccountError(f"Failed to alter account '{account_name}': {e}") from None

    def get_account(self, account_name: str) -> Account:
        """Get account by name"""
        try:
            sql = "SHOW ACCOUNTS"
            result = self._client.execute(sql)

            if not result or not result.rows:
                raise AccountError(f"Account '{account_name}' not found") from None

            for row in result.rows:
                if row[0] == account_name:
                    return self._row_to_account(row)

            raise AccountError(f"Account '{account_name}' not found") from None

        except Exception as e:
            raise AccountError(f"Failed to get account '{account_name}': {e}") from None

    def list_accounts(self) -> List[Account]:
        """List all accounts"""
        try:
            sql = "SHOW ACCOUNTS"
            result = self._client.execute(sql)

            if not result or not result.rows:
                return []

            return [self._row_to_account(row) for row in result.rows]

        except Exception as e:
            raise AccountError(f"Failed to list accounts: {e}") from None

    # User Management
    def create_user(self, user_name: str, password: str, comment: Optional[str] = None) -> User:
        """
        Create a new user in MatrixOne

        Note: MatrixOne CREATE USER syntax is: CREATE USER user_name IDENTIFIED BY 'password'
        The user is created in the current account context.

        Args::

            user_name: Name of the user to create
            password: Password for the user
            comment: Comment for the user (not supported in MatrixOne)

        Returns::

            User: Created user object

        Raises::

            AccountError: If user creation fails
        """
        try:
            # MatrixOne CREATE USER syntax
            sql_parts = [f"CREATE USER {self._client._escape_identifier(user_name)}"]
            sql_parts.append(f"IDENTIFIED BY {self._client._escape_string(password)}")

            # Note: MatrixOne doesn't support COMMENT in CREATE USER
            # if comment:
            #     sql_parts.append(f"COMMENT {self._client._escape_string(comment)}")

            sql = " ".join(sql_parts)
            self._client.execute(sql)

            # Return a User object with current account context
            current_account = self._get_current_account()
            return User(
                name=user_name,
                host="%",  # Default host
                account=current_account,
                created_time=datetime.now(),
                status="ACTIVE",
                comment=comment,
            )

        except Exception as e:
            raise AccountError(f"Failed to create user '{user_name}': {e}") from None

    def drop_user(self, user_name: str, if_exists: bool = False) -> None:
        """
        Drop a user according to MatrixOne DROP USER syntax:
        DROP USER [IF EXISTS] user [, user] ...

        Args::

            user_name: Name of the user to drop
            if_exists: If True, add IF EXISTS clause to avoid errors when user doesn't exist
        """
        try:
            sql_parts = ["DROP USER"]
            if if_exists:
                sql_parts.append("IF EXISTS")

            sql_parts.append(self._client._escape_identifier(user_name))
            sql = " ".join(sql_parts)
            self._client.execute(sql)

        except Exception as e:
            raise AccountError(f"Failed to drop user '{user_name}': {e}") from None

    def alter_user(
        self,
        user_name: str,
        password: Optional[str] = None,
        comment: Optional[str] = None,
        lock: Optional[bool] = None,
        lock_reason: Optional[str] = None,
    ) -> User:
        """
        Alter a user

        Note: MatrixOne ALTER USER supports:
        - ✅ ALTER USER user IDENTIFIED BY 'password' - Password modification
        - ✅ ALTER USER user LOCK - Lock user
        - ✅ ALTER USER user UNLOCK - Unlock user
        - ❌ ALTER USER user COMMENT 'comment' - Not supported
        """
        try:
            # Check if there are any operations to perform
            has_operations = False
            sql_parts = [f"ALTER USER {self._client._escape_identifier(user_name)}"]

            if password is not None:
                sql_parts.append(f"IDENTIFIED BY {self._client._escape_string(password)}")
                has_operations = True

            # MatrixOne doesn't support COMMENT in ALTER USER
            if comment is not None:
                raise AccountError(f"MatrixOne doesn't support COMMENT in ALTER USER. Comment: '{comment}'") from None

            # MatrixOne supports LOCK/UNLOCK in ALTER USER
            if lock is not None:
                if lock:
                    sql_parts.append("LOCK")
                else:
                    sql_parts.append("UNLOCK")
                has_operations = True

            # Only execute if there are operations to perform
            if has_operations:
                sql = " ".join(sql_parts)
                self._client.execute(sql)
            else:
                # If no operations, just return current user info
                pass

            # Return updated user info
            current_account = self._get_current_account()
            return User(
                name=user_name,
                host="%",  # Default host
                account=current_account,
                created_time=datetime.now(),
                status="LOCKED" if lock else "ACTIVE",
                comment=comment,
                locked_time=datetime.now() if lock else None,
                locked_reason=lock_reason,
            )

        except Exception as e:
            raise AccountError(f"Failed to alter user '{user_name}': {e}") from None

    def get_current_user(self) -> User:
        """Get current user information"""
        try:
            sql = "SELECT USER()"
            result = self._client.execute(sql)

            if not result or not result.rows:
                raise AccountError("Failed to get current user") from None

            # Parse current user from USER() function result
            current_user_str = result.rows[0][0]  # e.g., 'root@localhost'
            if "@" in current_user_str:
                username, host = current_user_str.split("@", 1)
            else:
                username = current_user_str
                host = "%"

            current_account = self._get_current_account()

            return User(
                name=username,
                host=host,
                account=current_account,
                created_time=None,
                status="ACTIVE",
                comment=None,
                locked_time=None,
                locked_reason=None,
            )

        except Exception as e:
            raise AccountError(f"Failed to get current user: {e}") from None

    def list_users(self) -> List[User]:
        """
        List users in current account

        Note: MatrixOne doesn't provide a direct way to list all users.
        This method returns the current user's information.
        """
        try:
            current_user = self.get_current_user()
            return [current_user]

        except Exception as e:
            raise AccountError(f"Failed to list users: {e}") from None

    # Role Management
    def create_role(self, role_name: str, comment: Optional[str] = None) -> Role:
        """Create a new role"""
        try:
            # MatrixOne CREATE ROLE syntax doesn't support COMMENT
            sql = f"CREATE ROLE {self._client._escape_identifier(role_name)}"
            self._client.execute(sql)

            return self.get_role(role_name)

        except Exception as e:
            raise AccountError(f"Failed to create role '{role_name}': {e}") from None

    def drop_role(self, role_name: str, if_exists: bool = False) -> None:
        """
        Drop a role

        Args::

            role_name: Name of the role to drop
            if_exists: If True, add IF EXISTS clause to avoid errors when role doesn't exist
        """
        try:
            sql_parts = ["DROP ROLE"]
            if if_exists:
                sql_parts.append("IF EXISTS")
            sql_parts.append(self._client._escape_identifier(role_name))

            sql = " ".join(sql_parts)
            self._client.execute(sql)
        except Exception as e:
            raise AccountError(f"Failed to drop role '{role_name}': {e}") from None

    def get_role(self, role_name: str) -> Role:
        """Get role by name"""
        try:
            sql = "SHOW ROLES"
            result = self._client.execute(sql)

            if not result or not result.rows:
                raise AccountError(f"Role '{role_name}' not found") from None

            for row in result.rows:
                if row[0] == role_name:
                    return self._row_to_role(row)

            raise AccountError(f"Role '{role_name}' not found") from None

        except Exception as e:
            raise AccountError(f"Failed to get role '{role_name}': {e}") from None

    def list_roles(self) -> List[Role]:
        """List all roles"""
        try:
            sql = "SHOW ROLES"
            result = self._client.execute(sql)

            if not result or not result.rows:
                return []

            return [self._row_to_role(row) for row in result.rows]

        except Exception as e:
            raise AccountError(f"Failed to list roles: {e}") from None

    # Permission Management
    def grant_privilege(
        self,
        privilege: str,
        object_type: str,
        object_name: str,
        to_user: Optional[str] = None,
        to_role: Optional[str] = None,
    ) -> None:
        """
        Grant privilege to user or role

        Note: In MatrixOne, users are treated as roles for permission purposes.

        Args::

            privilege: Privilege to grant (e.g., 'CREATE DATABASE', 'SELECT')
            object_type: Type of object (e.g., 'ACCOUNT', 'DATABASE', 'TABLE')
            object_name: Name of the object (e.g., 'test_db', '*')
            to_user: User to grant to (treated as role in MatrixOne)
            to_role: Role to grant to
        """
        try:
            if not to_user and not to_role:
                raise AccountError("Must specify either to_user or to_role") from None

            # In MatrixOne, users are treated as roles
            target = to_user if to_user else to_role

            sql_parts = [f"GRANT {privilege} ON {object_type} {self._client._escape_identifier(object_name)}"]
            sql_parts.append(f"TO {self._client._escape_identifier(target)}")

            sql = " ".join(sql_parts)
            self._client.execute(sql)

        except Exception as e:
            raise AccountError(f"Failed to grant privilege: {e}") from None

    def revoke_privilege(
        self,
        privilege: str,
        object_type: str,
        object_name: str,
        from_user: Optional[str] = None,
        from_role: Optional[str] = None,
    ) -> None:
        """Revoke privilege from user or role"""
        try:
            if not from_user and not from_role:
                raise AccountError("Must specify either from_user or from_role") from None

            # In MatrixOne, users are treated as roles
            target = from_user if from_user else from_role

            sql_parts = [f"REVOKE {privilege} ON {object_type} {self._client._escape_identifier(object_name)}"]
            sql_parts.append(f"FROM {self._client._escape_identifier(target)}")

            sql = " ".join(sql_parts)
            self._client.execute(sql)

        except Exception as e:
            raise AccountError(f"Failed to revoke privilege: {e}") from None

    def grant_role(self, role_name: str, to_user: str) -> None:
        """Grant role to user"""
        try:
            # MatrixOne syntax: GRANT role_name TO user_name
            sql = f"GRANT {self._client._escape_identifier(role_name)} TO {self._client._escape_identifier(to_user)}"
            self._client.execute(sql)
        except Exception as e:
            raise AccountError(f"Failed to grant role '{role_name}' to user '{to_user}': {e}") from None

    def revoke_role(self, role_name: str, from_user: str) -> None:
        """Revoke role from user"""
        try:
            # MatrixOne syntax: REVOKE role_name FROM user_name
            sql = f"REVOKE {self._client._escape_identifier(role_name)} FROM {self._client._escape_identifier(from_user)}"
            self._client.execute(sql)
        except Exception as e:
            raise AccountError(f"Failed to revoke role '{role_name}' from user '{from_user}': {e}") from None

    def list_grants(self, user: Optional[str] = None) -> List[Grant]:
        """List grants for current user or specified user"""
        try:
            if user:
                sql = f"SHOW GRANTS FOR {self._client._escape_identifier(user)}"
            else:
                sql = "SHOW GRANTS"

            result = self._client.execute(sql)

            if not result or not result.rows:
                return []

            grants = []
            for row in result.rows:
                grant = self._parse_grant_statement(row[0])
                grants.append(grant)

            return grants

        except Exception as e:
            raise AccountError(f"Failed to list grants: {e}") from None

    # Helper methods
    def _get_current_account(self) -> str:
        """Get current account name"""
        try:
            # Try to get account from connection context
            # This is a simplified approach - in practice, you might need to
            # parse the connection string or use other methods
            return "sys"  # Default account
        except Exception:
            return "sys"

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

    def _row_to_role(self, row: tuple) -> Role:
        """Convert database row to Role object"""
        return Role(
            name=row[0],
            id=row[1] if len(row) > 1 else 0,
            created_time=row[2] if len(row) > 2 else None,
            comment=row[3] if len(row) > 3 else None,
        )

    def _parse_grant_statement(self, grant_statement: str) -> Grant:
        """Parse grant statement to extract components"""
        # Example: "GRANT create account ON account  `root`@`localhost`"
        try:
            # Simple parsing - can be enhanced
            parts = grant_statement.split()
            privilege = parts[1] if len(parts) > 1 else None
            object_type = parts[3] if len(parts) > 3 else None
            object_name = parts[4] if len(parts) > 4 else None

            # Extract user from the end
            user_match = re.search(r"`([^`]+)`@`([^`]+)`", grant_statement)
            user = f"{user_match.group(1)}@{user_match.group(2)}" if user_match else None

            return Grant(
                grant_statement=grant_statement,
                privilege=privilege,
                object_type=object_type,
                object_name=object_name,
                user=user,
            )
        except Exception:
            return Grant(grant_statement=grant_statement)


class TransactionAccountManager(AccountManager):
    """Transaction-scoped account manager"""

    def __init__(self, transaction: "TransactionWrapper"):
        super().__init__(transaction.client)
        self._transaction = transaction

    def _execute_sql(self, sql: str):
        """Execute SQL within transaction"""
        return self._transaction.execute(sql)

    # Override all methods to use transaction
    def create_account(self, account_name: str, admin_name: str, password: str, comment: Optional[str] = None) -> Account:
        try:
            sql_parts = [f"CREATE ACCOUNT {self._client._escape_identifier(account_name)}"]
            sql_parts.append(f"ADMIN_NAME {self._client._escape_string(admin_name)}")
            sql_parts.append(f"IDENTIFIED BY {self._client._escape_string(password)}")

            if comment:
                sql_parts.append(f"COMMENT {self._client._escape_string(comment)}")

            sql = " ".join(sql_parts)
            self._transaction.execute(sql)

            return self.get_account(account_name)

        except Exception as e:
            raise AccountError(f"Failed to create account '{account_name}': {e}") from None

    # Add other transaction methods as needed...
