# MatrixOne Account Management Guide

## Overview

This guide documents the correct implementation of account, user, and role management for MatrixOne database based on actual testing and official documentation.

## MatrixOne Account/User/Role System

### Key Concepts

1. **Account**: A tenant in MatrixOne that provides resource isolation
2. **User**: A login identity within an account
3. **Role**: A collection of privileges that can be assigned to users
4. **Permission**: Specific access rights (CREATE, SELECT, etc.)

### Important Findings

Based on testing with MatrixOne v25.2.2.2:

1. **User Creation**: Users are created with `CREATE USER user_name IDENTIFIED BY 'password'`
2. **Role Creation**: Roles are created with `CREATE ROLE role_name` (no COMMENT support)
3. **Permission System**: MatrixOne has limitations in direct user permission granting
4. **Connection Format**: Users connect as `user@host`, not `account:user`

## Supported Operations

### ✅ Working Operations

#### Account Management
```sql
-- Create account
CREATE ACCOUNT account_name ADMIN_NAME admin_name IDENTIFIED BY 'password' [COMMENT 'comment']

-- Drop account  
DROP ACCOUNT account_name

-- Alter account
ALTER ACCOUNT account_name [COMMENT 'comment'] [SUSPEND|OPEN] [SUSPEND COMMENT 'reason']

-- List accounts
SHOW ACCOUNTS
```

#### User Management
```sql
-- Create user
CREATE USER user_name IDENTIFIED BY 'password'

-- Drop user
DROP USER user_name [@host]

-- Alter user
ALTER USER user_name [@host] [IDENTIFIED BY 'password'] [COMMENT 'comment'] [ACCOUNT LOCK|UNLOCK]

-- List current user
SELECT USER(), CURRENT_USER()
```

#### Role Management
```sql
-- Create role
CREATE ROLE role_name

-- Drop role
DROP ROLE role_name

-- List roles
SHOW ROLES
```

#### Permission Management
```sql
-- List grants for current user
SHOW GRANTS

-- List grants for specific user
SHOW GRANTS FOR user_name
```

### ⚠️ Limited Operations

#### Permission Granting
MatrixOne has limitations in granting permissions directly to users:

```sql
-- This may not work in all cases
GRANT privilege ON object_type object_name TO user_name

-- Users are not automatically roles
-- Direct permission granting to users is limited
```

### ❌ Not Supported

1. **User Listing**: No direct way to list all users in an account
2. **User Query**: Cannot query specific user information directly
3. **Complex Permission Management**: Limited support for fine-grained permissions
4. **Account:User Connection Format**: Users connect as `user@host`, not `account:user`

## Implementation Notes

### Account Manager V2

The `AccountManagerV2` class provides a corrected implementation:

```python
from matrixone.account_v2 import AccountManagerV2

# Initialize
account_manager = AccountManagerV2(client)

# Account operations
account = account_manager.create_account('test_account', 'admin', 'password')
accounts = account_manager.list_accounts()

# User operations  
user = account_manager.create_user('test_user', 'password')
current_user = account_manager.get_current_user()

# Role operations
role = account_manager.create_role('test_role')
roles = account_manager.list_roles()

# Permission operations (limited)
grants = account_manager.list_grants()
```

### Connection Management

MatrixOne users connect using standard MySQL format:

```python
# Correct connection format
client.connect('host', port, 'username', 'password', 'database')

# NOT: client.connect('host', port, 'account:username', 'password', 'database')
```

### Current User Information

To get current user information:

```python
# Get current user
result = client.execute("SELECT USER()")
# Returns: 'username@hostname'

# Parse user and host
user_str = result.rows[0][0]  # 'root@localhost'
username, host = user_str.split('@', 1)
```

## Best Practices

### 1. Account Management
- Use accounts for tenant isolation
- Each account has its own admin user
- Account operations require MOADMIN privileges

### 2. User Management
- Users are created in the current account context
- Users have basic CONNECT privileges by default
- User permission management is limited

### 3. Role Management
- Use roles for permission grouping
- Grant roles to users for permission inheritance
- Roles provide more flexible permission management

### 4. Permission Management
- Use `SHOW GRANTS` to check current permissions
- Permission granting to users directly is limited
- Consider using role-based permission management

## Limitations and Workarounds

### 1. User Listing Limitation
**Problem**: No direct way to list all users in an account.

**Workaround**: 
- Use `SELECT USER()` to get current user
- Maintain user list in application layer
- Use role-based management instead

### 2. Permission Granting Limitation
**Problem**: Direct permission granting to users is limited.

**Workaround**:
- Use role-based permission management
- Grant permissions to roles, then assign roles to users
- Use account-level permissions where possible

### 3. User Information Query Limitation
**Problem**: Cannot query detailed user information.

**Workaround**:
- Use `SHOW GRANTS FOR user_name` to check permissions
- Maintain user metadata in application layer
- Use current user information for basic details

## Example Usage

```python
from matrixone import Client
from matrixone.account_v2 import AccountManagerV2

# Connect as admin
client = Client()
client.connect('127.0.0.1', 6001, 'root', '111', 'test')

# Initialize account manager
account_manager = AccountManagerV2(client)

# Create account
account = account_manager.create_account(
    account_name='tenant1',
    admin_name='admin1', 
    password='adminpass123',
    comment='Tenant 1 account'
)

# Create user in current account
user = account_manager.create_user(
    user_name='user1',
    password='userpass123'
)

# Create role
role = account_manager.create_role('developer')

# Check current user
current_user = account_manager.get_current_user()
print(f"Current user: {current_user}")

# List available roles
roles = account_manager.list_roles()
for role in roles:
    print(f"Role: {role.name}")

# Check grants
grants = account_manager.list_grants()
for grant in grants[:5]:  # Show first 5 grants
    print(f"Grant: {grant.privilege} ON {grant.object_type}")

# Cleanup
account_manager.drop_role('developer')
account_manager.drop_user('user1')
account_manager.drop_account('tenant1')

client.disconnect()
```

## Conclusion

MatrixOne provides basic account, user, and role management capabilities, but with some limitations compared to traditional databases. The key is to understand these limitations and work within them, using role-based permission management and maintaining user metadata at the application layer when needed.

The `AccountManagerV2` class provides a corrected implementation that works with MatrixOne's actual behavior rather than assuming MySQL-compatible features that may not be fully supported.
