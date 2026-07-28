Account Management Guide
========================

This guide covers account management operations in the MatrixOne Python SDK, including user creation, authentication, permissions, and security features.

Overview
--------

MatrixOne's account management system provides:

* **User Management**: Create, update, and delete user accounts
* **Authentication**: Secure login and session management
* **Role-based Access Control**: Granular permissions and role management
* **Database Access Control**: Manage database-level permissions
* **Security Features**: Password policies, account locking, and audit logging
* **Multi-tenant Support**: Isolate data and operations by tenant

Getting Started
---------------

Basic Setup
~~~~~~~~~~~

.. code-block:: python

   from matrixone import Client
   from matrixone.config import get_connection_params

   # Connect as administrator
   connection_params = get_connection_params()
   client = Client(*connection_params)
   client.connect(*connection_params)

   # Get account manager
   account_manager = client.account

User Management
---------------

Creating Users
~~~~~~~~~~~~~~

.. code-block:: python

   # Create a basic user
   user = account_manager.create_user(
       username="john_doe",
       password="secure_password123",
       email="john@example.com"
   )
   print(f"Created user: {user.username}")

   # Create user with additional properties
   user = account_manager.create_user(
       username="jane_smith",
       password="secure_password456",
       email="jane@example.com",
       full_name="Jane Smith",
       department="Engineering",
       phone="+1-555-0123"
   )

   # Create user with specific roles
   user = account_manager.create_user(
       username="admin_user",
       password="admin_password789",
       email="admin@example.com",
       roles=["admin", "developer"]
   )

Listing Users
~~~~~~~~~~~~~

.. code-block:: python

   # List all users
   users = account_manager.list_users()
   for user in users:
       print(f"User: {user.username}, Email: {user.email}, Status: {user.status}")

   # Get specific user
   user = account_manager.get_user("john_doe")
   if user:
       print(f"User found: {user.username}")
       print(f"Created: {user.created_at}")
       print(f"Last login: {user.last_login}")

   # Search users by criteria
   users = account_manager.search_users(
       department="Engineering",
       status="active"
   )

Updating Users
~~~~~~~~~~~~~~

.. code-block:: python

   # Update user information
   updated_user = account_manager.update_user(
       username="john_doe",
       email="john.doe@newcompany.com",
       full_name="John Doe Jr.",
       department="Product Management"
   )

   # Change user password
   account_manager.change_password(
       username="john_doe",
       new_password="new_secure_password123"
   )

   # Update user status
   account_manager.update_user_status(
       username="john_doe",
       status="inactive"  # or "active", "locked", "suspended"
   )

Deleting Users
~~~~~~~~~~~~~~

.. code-block:: python

   # Delete a user
   account_manager.delete_user("john_doe")
   print("User deleted")

   # Soft delete (disable account)
   account_manager.soft_delete_user("jane_smith")
   print("User account disabled")

Role Management
---------------

Creating Roles
~~~~~~~~~~~~~~~

.. code-block:: python

   # Create a custom role
   role = account_manager.create_role(
       name="data_analyst",
       description="Role for data analysis tasks",
       permissions=[
           "SELECT",
           "CREATE_VIEW",
           "EXECUTE_PROCEDURE"
       ]
   )

   # Create role with database-specific permissions
   role = account_manager.create_role(
       name="db_admin",
       description="Database administrator role",
       permissions=[
           "CREATE_TABLE",
           "DROP_TABLE",
           "ALTER_TABLE",
           "CREATE_INDEX",
           "DROP_INDEX",
           "GRANT_PRIVILEGES"
       ],
       databases=["analytics_db", "reporting_db"]
   )

Listing Roles
~~~~~~~~~~~~~

.. code-block:: python

   # List all roles
   roles = account_manager.list_roles()
   for role in roles:
       print(f"Role: {role.name}, Users: {role.user_count}")

   # Get specific role
   role = account_manager.get_role("data_analyst")
   if role:
       print(f"Role permissions: {role.permissions}")

Assigning Roles
~~~~~~~~~~~~~~~

.. code-block:: python

   # Assign role to user
   account_manager.assign_role("john_doe", "data_analyst")
   print("Role assigned")

   # Assign multiple roles
   account_manager.assign_roles("jane_smith", ["developer", "tester"])
   print("Multiple roles assigned")

   # Remove role from user
   account_manager.remove_role("john_doe", "data_analyst")
   print("Role removed")

Database Permissions
--------------------

Granting Permissions
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Grant database access
   account_manager.grant_database_access(
       username="john_doe",
       database="analytics_db",
       permissions=["SELECT", "INSERT", "UPDATE"]
   )

   # Grant table-specific permissions
   account_manager.grant_table_permissions(
       username="jane_smith",
       database="analytics_db",
       table="user_data",
       permissions=["SELECT", "INSERT"]
   )

   # Grant schema permissions
   account_manager.grant_schema_permissions(
       username="data_analyst",
       database="analytics_db",
       schema="public",
       permissions=["CREATE_TABLE", "DROP_TABLE"]
   )

Revoking Permissions
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Revoke database access
   account_manager.revoke_database_access(
       username="john_doe",
       database="analytics_db"
   )

   # Revoke specific permissions
   account_manager.revoke_permissions(
       username="jane_smith",
       database="analytics_db",
       table="user_data",
       permissions=["INSERT"]
   )

Checking Permissions
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Check user permissions
   permissions = account_manager.get_user_permissions("john_doe")
   print(f"User permissions: {permissions}")

   # Check database access
   has_access = account_manager.has_database_access(
       username="john_doe",
       database="analytics_db"
   )
   print(f"Has database access: {has_access}")

   # Check specific permission
   can_select = account_manager.has_permission(
       username="john_doe",
       database="analytics_db",
       table="user_data",
       permission="SELECT"
   )
   print(f"Can SELECT: {can_select}")

Authentication
--------------

User Login
~~~~~~~~~~

.. code-block:: python

   # Authenticate user
   auth_result = account_manager.authenticate(
       username="john_doe",
       password="secure_password123"
   )
   
   if auth_result.success:
       print(f"Login successful for {auth_result.user.username}")
       print(f"Session token: {auth_result.session_token}")
   else:
       print(f"Login failed: {auth_result.error_message}")

   # Login with additional security
   auth_result = account_manager.authenticate(
       username="john_doe",
       password="secure_password123",
       ip_address="192.168.1.100",
       user_agent="Mozilla/5.0..."
   )

Session Management
~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Create session for authenticated user
   session = account_manager.create_session(
       username="john_doe",
       expires_in_hours=8
   )
   print(f"Session created: {session.session_id}")

   # Validate session
   is_valid = account_manager.validate_session(session.session_id)
   print(f"Session valid: {is_valid}")

   # Refresh session
   new_session = account_manager.refresh_session(session.session_id)
   print(f"Session refreshed: {new_session.session_id}")

   # Terminate session
   account_manager.terminate_session(session.session_id)
   print("Session terminated")

Security Features
-----------------

Password Policies
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Set password policy
   policy = account_manager.set_password_policy(
       min_length=12,
       require_uppercase=True,
       require_lowercase=True,
       require_numbers=True,
       require_special_chars=True,
       max_age_days=90,
       prevent_reuse_count=5
   )

   # Validate password against policy
   is_valid = account_manager.validate_password(
       password="MySecure123!",
       username="john_doe"
   )
   print(f"Password valid: {is_valid}")

Account Locking
~~~~~~~~~~~~~~~

.. code-block:: python

   # Lock account after failed attempts
   account_manager.lock_account(
       username="john_doe",
       reason="too_many_failed_attempts",
       duration_minutes=30
   )

   # Unlock account
   account_manager.unlock_account("john_doe")

   # Check account lock status
   lock_status = account_manager.get_account_lock_status("john_doe")
   print(f"Account locked: {lock_status.is_locked}")
   if lock_status.is_locked:
       print(f"Lock reason: {lock_status.reason}")
       print(f"Lock expires: {lock_status.expires_at}")

Audit Logging
~~~~~~~~~~~~~

.. code-block:: python

   # Get audit logs
   logs = account_manager.get_audit_logs(
       username="john_doe",
       action="login",
       start_date="2024-01-01",
       end_date="2024-01-31"
   )

   for log in logs:
       print(f"Action: {log.action}, Time: {log.timestamp}")
       print(f"IP: {log.ip_address}, Success: {log.success}")

   # Get failed login attempts
   failed_logins = account_manager.get_failed_login_attempts(
       username="john_doe",
       hours=24
   )

Multi-tenant Support
--------------------

Tenant Management
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Create tenant
   tenant = account_manager.create_tenant(
       name="company_a",
       description="Company A tenant",
       admin_username="admin_a"
   )

   # Assign user to tenant
   account_manager.assign_user_to_tenant(
       username="john_doe",
       tenant="company_a"
   )

   # Get tenant users
   tenant_users = account_manager.get_tenant_users("company_a")
   for user in tenant_users:
       print(f"Tenant user: {user.username}")

   # Set tenant isolation
   account_manager.set_tenant_isolation(
       tenant="company_a",
       isolated=True
   )

Real-world Examples
-------------------

User Onboarding System
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   class UserOnboardingSystem:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.account_manager = self.client.account

       def onboard_new_user(self, user_data):
           # Create user account
           user = self.account_manager.create_user(
               username=user_data["username"],
               password=user_data["password"],
               email=user_data["email"],
               full_name=user_data["full_name"],
               department=user_data["department"]
           )

           # Assign default role based on department
           role = self.get_default_role(user_data["department"])
           self.account_manager.assign_role(user.username, role)

           # Grant database access
           self.account_manager.grant_database_access(
               username=user.username,
               database="company_db",
               permissions=["SELECT", "INSERT", "UPDATE"]
           )

           # Send welcome email
           self.send_welcome_email(user.email, user.username)

           return user

       def get_default_role(self, department):
           role_mapping = {
               "Engineering": "developer",
               "Marketing": "marketer",
               "Sales": "sales_rep",
               "HR": "hr_user"
           }
           return role_mapping.get(department, "basic_user")

       def send_welcome_email(self, email, username):
           # Email sending logic here
           print(f"Welcome email sent to {email} for user {username}")

   # Usage
   onboarding = UserOnboardingSystem()
   new_user = onboarding.onboard_new_user({
       "username": "new_employee",
       "password": "temp_password123",
       "email": "new@company.com",
       "full_name": "New Employee",
       "department": "Engineering"
   })

Role-based Access Control
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   class RBACSystem:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.account_manager = self.client.account
           self.setup_roles()

       def setup_roles(self):
           # Define role hierarchy
           roles = [
               {
                   "name": "admin",
                   "permissions": ["ALL"],
                   "description": "Full system access"
               },
               {
                   "name": "manager",
                   "permissions": ["SELECT", "INSERT", "UPDATE", "CREATE_TABLE"],
                   "description": "Management level access"
               },
               {
                   "name": "analyst",
                   "permissions": ["SELECT", "CREATE_VIEW"],
                   "description": "Data analysis access"
               },
               {
                   "name": "readonly",
                   "permissions": ["SELECT"],
                   "description": "Read-only access"
               }
           ]

           for role_data in roles:
               try:
                   self.account_manager.create_role(**role_data)
               except Exception:
                   # Role might already exist
                   pass

       def check_access(self, username, resource, action):
           # Check if user has permission for specific action
           return self.account_manager.has_permission(
               username=username,
               database=resource.get("database"),
               table=resource.get("table"),
               permission=action
           )

       def enforce_access_control(self, username, resource, action):
           if not self.check_access(username, resource, action):
               raise PermissionError(
                   f"User {username} does not have {action} permission for {resource}"
               )

   # Usage
   rbac = RBACSystem()
   
   # Check access before operation
   if rbac.check_access("john_doe", {"database": "analytics", "table": "sales"}, "SELECT"):
       # Perform operation
       print("Access granted")
   else:
       print("Access denied")

Security Monitoring
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   class SecurityMonitor:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.account_manager = self.client.account

       def monitor_failed_logins(self, threshold=5):
           # Get recent failed login attempts
           failed_attempts = self.account_manager.get_failed_login_attempts(hours=1)
           
           # Group by username
           attempts_by_user = {}
           for attempt in failed_attempts:
               username = attempt.username
               if username not in attempts_by_user:
                   attempts_by_user[username] = 0
               attempts_by_user[username] += 1

           # Lock accounts with too many failed attempts
           for username, count in attempts_by_user.items():
               if count >= threshold:
                   self.account_manager.lock_account(
                       username=username,
                       reason="too_many_failed_attempts",
                       duration_minutes=60
                   )
                   print(f"Locked account {username} due to {count} failed attempts")

       def audit_user_activity(self, username, days=7):
           # Get user activity logs
           logs = self.account_manager.get_audit_logs(
               username=username,
               start_date=datetime.now() - timedelta(days=days)
           )

           # Analyze activity patterns
           login_count = len([log for log in logs if log.action == "login"])
           failed_count = len([log for log in logs if not log.success])
           
           print(f"User {username} activity summary:")
           print(f"  Successful logins: {login_count}")
           print(f"  Failed attempts: {failed_count}")
           
           if failed_count > login_count * 0.5:
               print(f"  WARNING: High failure rate for {username}")

   # Usage
   monitor = SecurityMonitor()
   monitor.monitor_failed_logins(threshold=3)
   monitor.audit_user_activity("john_doe", days=30)

Error Handling
--------------

Robust error handling for production applications:

.. code-block:: python

   from matrixone.exceptions import AccountError, AuthenticationError, PermissionError

   try:
       # Account operations
       user = account_manager.create_user("test_user", "password123")
   except AccountError as e:
       print(f"Account error: {e}")
   except AuthenticationError as e:
       print(f"Authentication error: {e}")
   except PermissionError as e:
       print(f"Permission error: {e}")
   except Exception as e:
       print(f"Unexpected error: {e}")

   # Retry mechanism for authentication
   def authenticate_with_retry(username, password, max_retries=3):
       for attempt in range(max_retries):
           try:
               return account_manager.authenticate(username, password)
           except AuthenticationError as e:
               if attempt == max_retries - 1:
                   raise
               time.sleep(2 ** attempt)  # Exponential backoff

Performance Optimization
------------------------

Best practices for optimal performance:

.. code-block:: python

   # Batch user operations
   def batch_create_users(account_manager, users_data):
       created_users = []
       for user_data in users_data:
           try:
               user = account_manager.create_user(**user_data)
               created_users.append(user)
           except Exception as e:
               print(f"Failed to create user {user_data['username']}: {e}")
       return created_users

   # Efficient permission checking
   def check_multiple_permissions(account_manager, username, permissions):
       user_permissions = account_manager.get_user_permissions(username)
       return {perm: perm in user_permissions for perm in permissions}

   # Connection pooling for high-throughput applications
   class AccountService:
       def __init__(self):
           self.client = Client(*get_connection_params())
           self.client.connect(*get_connection_params())
           self.account_manager = self.client.account
           self.lock = threading.Lock()

       def thread_safe_authenticate(self, username, password):
           with self.lock:
               return self.account_manager.authenticate(username, password)

Troubleshooting
---------------

Common issues and solutions:

**Authentication failures**
   - Verify username and password
   - Check account status (active, locked, suspended)
   - Validate password policy compliance

**Permission denied errors**
   - Verify user has required permissions
   - Check role assignments
   - Validate database/table access rights

**Account creation failures**
   - Check username uniqueness
   - Validate password policy
   - Ensure required fields are provided

**Session management issues**
   - Verify session token validity
   - Check session expiration
   - Ensure proper session cleanup

For more information, see the :doc:`api/client` and :doc:`best_practices`.
