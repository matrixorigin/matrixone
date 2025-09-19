#!/usr/bin/env python3
"""
MatrixOne Account Management Examples

This example demonstrates comprehensive account, user, and role management:
1. Account creation and management
2. User creation and management  
3. Role creation and assignment
4. Permission management
5. Role-based login
6. Account cleanup

This example shows the complete account management capabilities of MatrixOne.
"""

import logging
import asyncio
from matrixone import Client, AsyncClient
from matrixone.account import AccountManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


def demo_account_management():
    """Demonstrate account management operations"""
    logger.info("🚀 MatrixOne Account Management Demo")
    logger.info("=" * 60)
    
    # Connect as root for account management
    root_client = Client()
    root_client.connect('127.0.0.1', 6001, 'root', '111', 'test')
    account_manager = AccountManager(root_client)
    
    # Clean up any existing demo accounts
    account_manager.drop_account('demo_account', if_exists=True)
    account_manager.drop_account('tenant_account', if_exists=True)
    
    # Test 1: Account Creation
    logger.info("\n=== Test 1: Account Creation ===")
    try:
        # Create demo account
        account1 = account_manager.create_account(
            account_name='demo_account',
            admin_name='demo_admin',
            password='adminpass123',
            comment='Demo account for testing'
        )
        logger.info(f"✅ Created account: {account1.name}")
        
        # Create tenant account
        account2 = account_manager.create_account(
            account_name='tenant_account',
            admin_name='tenant_admin',
            password='tenantpass123',
            comment='Tenant account for multi-tenancy'
        )
        logger.info(f"✅ Created account: {account2.name}")
        
        # List all accounts
        accounts = account_manager.list_accounts()
        logger.info(f"📋 Total accounts: {len(accounts)}")
        for account in accounts:
            logger.info(f"   - {account.name} (Admin: {account.admin_name}, Status: {account.status})")
        
    except Exception as e:
        logger.error(f"❌ Account creation failed: {e}")
    
    # Test 2: User Management
    logger.info("\n=== Test 2: User Management ===")
    try:
        # Connect as demo account admin
        demo_admin_client = Client()
        demo_admin_client.connect('127.0.0.1', 6001, 'demo_account#demo_admin', 'adminpass123', 'mo_catalog')
        demo_account_manager = AccountManager(demo_admin_client)
        
        # Create users in demo account
        users_to_create = [
            ('developer1', 'devpass123', 'Developer user 1'),
            ('developer2', 'devpass456', 'Developer user 2'),
            ('analyst1', 'analystpass123', 'Analyst user 1'),
            ('readonly_user', 'readonlypass123', 'Read-only user')
        ]
        
        for username, password, comment in users_to_create:
            try:
                user = demo_account_manager.create_user(username, password, comment)
                logger.info(f"   ✅ Created user: {user.name}")
            except Exception as e:
                logger.error(f"   ❌ Failed to create user {username}: {e}")
        
        # List users
        users = demo_account_manager.list_users()
        logger.info(f"📋 Users in demo_account: {len(users)}")
        for user in users:
            logger.info(f"   - {user.name}@{user.host} (Account: {user.account})")
        
        demo_admin_client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ User management failed: {e}")
    
    # Test 3: Role Management
    logger.info("\n=== Test 3: Role Management ===")
    try:
        # Connect as demo account admin
        demo_admin_client = Client()
        demo_admin_client.connect('127.0.0.1', 6001, 'demo_account#demo_admin', 'adminpass123', 'mo_catalog')
        demo_account_manager = AccountManager(demo_admin_client)
        
        # Create roles
        roles_to_create = [
            'developer_role',
            'analyst_role',
            'readonly_role',
            'admin_role'
        ]
        
        for role_name in roles_to_create:
            try:
                role = demo_account_manager.create_role(role_name)
                logger.info(f"   ✅ Created role: {role.name}")
            except Exception as e:
                logger.error(f"   ❌ Failed to create role {role_name}: {e}")
        
        # List roles
        roles = demo_account_manager.list_roles()
        logger.info(f"📋 Roles in demo_account: {len(roles)}")
        for role in roles:
            logger.info(f"   - {role.name} (ID: {role.id})")
        
        demo_admin_client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Role management failed: {e}")
    
    # Test 4: Role Assignment
    logger.info("\n=== Test 4: Role Assignment ===")
    try:
        # Connect as demo account admin
        demo_admin_client = Client()
        demo_admin_client.connect('127.0.0.1', 6001, 'demo_account#demo_admin', 'adminpass123', 'mo_catalog')
        demo_account_manager = AccountManager(demo_admin_client)
        
        # Assign roles to users
        role_assignments = [
            ('developer_role', 'developer1'),
            ('developer_role', 'developer2'),
            ('analyst_role', 'analyst1'),
            ('readonly_role', 'readonly_user'),
            ('admin_role', 'demo_admin')  # Admin gets admin role
        ]
        
        for role_name, username in role_assignments:
            try:
                demo_account_manager.grant_role(role_name, username)
                logger.info(f"   ✅ Granted role '{role_name}' to user '{username}'")
            except Exception as e:
                logger.error(f"   ❌ Failed to grant role '{role_name}' to user '{username}': {e}")
        
        demo_admin_client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Role assignment failed: {e}")
    
    # Test 5: Role-based Login
    logger.info("\n=== Test 5: Role-based Login ===")
    
    # Test developer1 with developer_role
    logger.info("\n🔌 Test developer1 with developer_role")
    try:
        client = Client()
        client.connect('127.0.0.1', 6001, 'demo_account#developer1#developer_role', 'devpass123', 'mo_catalog')
        login_info = client.get_login_info()
        logger.info(f"   ✅ Login successful: {login_info}")
        
        result = client.execute("SELECT USER(), CURRENT_USER()")
        logger.info(f"   User context: {result.rows[0]}")
        
        client.disconnect()
    except Exception as e:
        logger.error(f"   ❌ Role-based login failed: {e}")
    
    # Test analyst1 with analyst_role
    logger.info("\n🔌 Test analyst1 with analyst_role")
    try:
        client = Client()
        client.connect('127.0.0.1', 6001, 'analyst1', 'analystpass123', 'mo_catalog', 
                      account='demo_account', role='analyst_role')
        login_info = client.get_login_info()
        logger.info(f"   ✅ Login successful: {login_info}")
        
        result = client.execute("SELECT USER(), CURRENT_USER()")
        logger.info(f"   User context: {result.rows[0]}")
        
        client.disconnect()
    except Exception as e:
        logger.error(f"   ❌ Role-based login failed: {e}")
    
    # Test 6: Permission Management
    logger.info("\n=== Test 6: Permission Management ===")
    try:
        # Connect as demo account admin
        demo_admin_client = Client()
        demo_admin_client.connect('127.0.0.1', 6001, 'demo_account#demo_admin', 'adminpass123', 'mo_catalog')
        demo_account_manager = AccountManager(demo_admin_client)
        
        # List grants for current user
        grants = demo_account_manager.list_grants()
        logger.info(f"📋 Grants for demo_admin: {len(grants)}")
        for i, grant in enumerate(grants[:10]):  # Show first 10 grants
            logger.info(f"   {i+1:2d}. {grant.privilege} ON {grant.object_type} {grant.object_name}")
        if len(grants) > 10:
            logger.info(f"   ... and {len(grants) - 10} more grants")
        
        demo_admin_client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Permission management failed: {e}")
    
    # Test 7: Multi-tenant Scenario
    logger.info("\n=== Test 7: Multi-tenant Scenario ===")
    try:
        # Connect as tenant account admin
        tenant_admin_client = Client()
        tenant_admin_client.connect('127.0.0.1', 6001, 'tenant_account#tenant_admin', 'tenantpass123', 'mo_catalog')
        tenant_account_manager = AccountManager(tenant_admin_client)
        
        # Create tenant-specific user
        tenant_user = tenant_account_manager.create_user('tenant_user1', 'tenantuserpass123', 'Tenant user 1')
        logger.info(f"✅ Created tenant user: {tenant_user.name}")
        
        # Create tenant-specific role
        tenant_role = tenant_account_manager.create_role('tenant_role')
        logger.info(f"✅ Created tenant role: {tenant_role.name}")
        
        # Assign role to user
        tenant_account_manager.grant_role('tenant_role', 'tenant_user1')
        logger.info("✅ Granted tenant role to tenant user")
        
        # Test tenant user login
        tenant_client = Client()
        tenant_client.connect('127.0.0.1', 6001, 'tenant_account#tenant_user1#tenant_role', 'tenantuserpass123', 'mo_catalog')
        login_info = tenant_client.get_login_info()
        logger.info(f"✅ Tenant user login successful: {login_info}")
        
        tenant_client.disconnect()
        tenant_admin_client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Multi-tenant scenario failed: {e}")
    
    # Test 8: Account Information
    logger.info("\n=== Test 8: Account Information ===")
    try:
        # Get current user info
        current_user = account_manager.get_current_user()
        logger.info(f"📋 Current user: {current_user.name}@{current_user.host}")
        logger.info(f"   Account: {current_user.account}")
        logger.info(f"   Status: {current_user.status}")
        
        # List all accounts with details
        accounts = account_manager.list_accounts()
        logger.info(f"📋 All accounts ({len(accounts)}):")
        for account in accounts:
            logger.info(f"   - {account.name}")
            logger.info(f"     Admin: {account.admin_name}")
            logger.info(f"     Status: {account.status}")
            logger.info(f"     Created: {account.created_time}")
        
    except Exception as e:
        logger.error(f"❌ Account information failed: {e}")
    
    # Cleanup
    logger.info("\n=== Cleanup ===")
    try:
        # Clean up demo accounts
        account_manager.drop_account('demo_account', if_exists=True)
        logger.info("✅ Cleaned up demo_account")
        
        account_manager.drop_account('tenant_account', if_exists=True)
        logger.info("✅ Cleaned up tenant_account")
        
    except Exception as e:
        logger.warning(f"⚠️ Cleanup warning: {e}")
    
    root_client.disconnect()
    
    logger.info("\n🎉 Account management demo completed!")
    logger.info("\nKey achievements:")
    logger.info("- ✅ Account creation and management")
    logger.info("- ✅ User creation and management")
    logger.info("- ✅ Role creation and assignment")
    logger.info("- ✅ Role-based login with multiple formats")
    logger.info("- ✅ Permission management")
    logger.info("- ✅ Multi-tenant scenario")
    logger.info("- ✅ Complete cleanup")


async def demo_async_account_management():
    """Demonstrate async account management"""
    logger.info("\n🚀 MatrixOne Async Account Management Demo")
    logger.info("=" * 60)
    
    try:
        # Connect as root
        root_client = AsyncClient()
        await root_client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        logger.info("✅ Async connection successful")
        
        # Test basic async operations
        result = await root_client.execute("SELECT USER(), CURRENT_USER()")
        logger.info(f"   User context: {result.rows[0]}")
        
        # Test account listing
        result = await root_client.execute("SHOW ACCOUNTS")
        logger.info(f"   Found {len(result.rows)} accounts")
        for row in result.rows:
            logger.info(f"     - {row[0]} (Admin: {row[1]}, Status: {row[3]})")
        
        await root_client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Async account management failed: {e}")


def main():
    """Main demo function"""
    logger.info("🚀 MatrixOne Account Management Examples")
    logger.info("=" * 60)
    
    # Run synchronous account management demo
    demo_account_management()
    
    # Run async account management demo
    asyncio.run(demo_async_account_management())
    
    logger.info("\n🎉 All account management examples completed!")
    logger.info("\nSummary:")
    logger.info("- Complete account lifecycle management")
    logger.info("- User and role management with assignments")
    logger.info("- Role-based authentication and authorization")
    logger.info("- Multi-tenant account isolation")
    logger.info("- Permission and grant management")
    logger.info("- Async account operations")


if __name__ == '__main__':
    main()
