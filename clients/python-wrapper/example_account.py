#!/usr/bin/env python3
"""
MatrixOne Account Management Example

This example demonstrates the correct usage of MatrixOne account, user, and role management
based on actual MatrixOne behavior and limitations.
"""

import logging
import asyncio
from matrixone import Client, AsyncClient
from matrixone.account import AccountManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


def demo_sync_account_management():
    """Demonstrate synchronous account management"""
    logger.info("MatrixOne Account Management - Synchronous Demo")
    logger.info("=" * 60)
    
    client = Client()
    
    try:
        # Connect to MatrixOne
        client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        logger.info("✅ Connected to MatrixOne!")
        
        # Initialize account manager
        account_manager = AccountManager(client)
        
        # Demo 1: Account Management
        logger.info("\n=== Demo 1: Account Management ===")
        
        # List existing accounts
        logger.info("📋 Listing existing accounts:")
        accounts = account_manager.list_accounts()
        for account in accounts:
            logger.info(f"   - {account.name} (Admin: {account.admin_name}, Status: {account.status})")
        
        # Create new account
        logger.info("\n🏗️  Creating new account:")
        try:
            account = account_manager.create_account(
                account_name='demo_account_v2',
                admin_name='demo_admin',
                password='adminpass123',
                comment='Demo account for V2'
            )
            logger.info(f"   ✅ Created account: {account.name}")
        except Exception as e:
            logger.error(f"   ❌ Account creation failed: {e}")
        
        # Demo 2: User Management
        logger.info("\n=== Demo 2: User Management ===")
        
        # Get current user
        logger.info("👤 Current user information:")
        try:
            current_user = account_manager.get_current_user()
            logger.info(f"   - Name: {current_user.name}")
            logger.info(f"   - Host: {current_user.host}")
            logger.info(f"   - Account: {current_user.account}")
            logger.info(f"   - Status: {current_user.status}")
        except Exception as e:
            logger.error(f"   ❌ Failed to get current user: {e}")
        
        # Create new user
        logger.info("\n👥 Creating new user:")
        try:
            user = account_manager.create_user(
                user_name='demo_user_v2',
                password='userpass123',
                comment='Demo user for V2'
            )
            logger.info(f"   ✅ Created user: {user.name}")
        except Exception as e:
            logger.error(f"   ❌ User creation failed: {e}")
        
        # List users (limited in MatrixOne)
        logger.info("\n📋 Listing users (current user only):")
        try:
            users = account_manager.list_users()
            for user in users:
                logger.info(f"   - {user.name}@{user.host} (Account: {user.account})")
        except Exception as e:
            logger.error(f"   ❌ Failed to list users: {e}")
        
        # Demo 3: Role Management
        logger.info("\n=== Demo 3: Role Management ===")
        
        # List existing roles
        logger.info("📋 Listing existing roles:")
        try:
            roles = account_manager.list_roles()
            for role in roles:
                logger.info(f"   - {role.name} (ID: {role.id})")
        except Exception as e:
            logger.error(f"   ❌ Failed to list roles: {e}")
        
        # Create new role
        logger.info("\n🎭 Creating new role:")
        try:
            role = account_manager.create_role('demo_role_v2')
            logger.info(f"   ✅ Created role: {role.name}")
        except Exception as e:
            logger.error(f"   ❌ Role creation failed: {e}")
        
        # Demo 4: Permission Management
        logger.info("\n=== Demo 4: Permission Management ===")
        
        # List current grants
        logger.info("🔐 Current user grants:")
        try:
            grants = account_manager.list_grants()
            logger.info(f"   Found {len(grants)} grants:")
            for i, grant in enumerate(grants[:10]):  # Show first 10 grants
                logger.info(f"   {i+1:2d}. {grant.privilege} ON {grant.object_type} {grant.object_name}")
            if len(grants) > 10:
                logger.info(f"   ... and {len(grants) - 10} more grants")
        except Exception as e:
            logger.error(f"   ❌ Failed to list grants: {e}")
        
        # Demo 5: Connection Testing
        logger.info("\n=== Demo 5: Connection Testing ===")
        
        # Test connection with new user
        logger.info("🔌 Testing connection with new user:")
        try:
            client2 = Client()
            client2.connect('127.0.0.1', 6001, 'demo_user_v2', 'userpass123', 'test')
            logger.info("   ✅ Connection with new user succeeded")
            
            # Check user context
            result = client2.execute("SELECT USER(), CURRENT_USER()")
            logger.info(f"   - USER(): {result.rows[0][0]}")
            logger.info(f"   - CURRENT_USER(): {result.rows[0][1]}")
            
            client2.disconnect()
        except Exception as e:
            logger.error(f"   ❌ Connection with new user failed: {e}")
        
        # Demo 6: Cleanup
        logger.info("\n=== Demo 6: Cleanup ===")
        
        cleanup_operations = [
            ("Drop role", lambda: account_manager.drop_role('demo_role_v2')),
            ("Drop user", lambda: account_manager.drop_user('demo_user_v2')),
            ("Drop account", lambda: account_manager.drop_account('demo_account_v2'))
        ]
        
        for operation_name, operation_func in cleanup_operations:
            try:
                operation_func()
                logger.info(f"   ✅ {operation_name} succeeded")
            except Exception as e:
                logger.warning(f"   ⚠️  {operation_name} failed: {e}")
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ Account management demo completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Demo failed: {e}")
    finally:
        client.disconnect()
        logger.info("🔌 Disconnected from MatrixOne")


async def demo_async_account_management():
    """Demonstrate asynchronous account management"""
    logger.info("\nMatrixOne Account Management - Asynchronous Demo")
    logger.info("=" * 60)
    
    client = AsyncClient()
    
    try:
        # Connect to MatrixOne
        await client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        logger.info("✅ Connected to MatrixOne (async)!")
        
        # Note: AsyncAccountManager would need to be implemented
        # For now, we'll demonstrate basic async operations
        logger.info("📝 Note: Async account manager not yet implemented")
        logger.info("   Demonstrating basic async operations")
        
        # Quick async demo with direct SQL execution
        logger.info("\n🏗️  Testing async operations:")
        try:
            # Test basic async query
            result = await client.execute("SELECT USER(), CURRENT_USER()")
            logger.info(f"   ✅ Async query succeeded: {result.rows[0][0]}")
            
            # Test account listing
            result = await client.execute("SHOW ACCOUNTS")
            logger.info(f"   ✅ Found {len(result.rows)} accounts")
            for row in result.rows:
                logger.info(f"     - {row[0]} (Admin: {row[1]}, Status: {row[3]})")
            
            # Test role listing
            result = await client.execute("SHOW ROLES")
            logger.info(f"   ✅ Found {len(result.rows)} roles")
            for row in result.rows:
                logger.info(f"     - {row[0]}")
            
        except Exception as e:
            logger.error(f"   ❌ Async demo failed: {e}")
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ Async account management demo completed!")
        
    except Exception as e:
        logger.error(f"❌ Async demo failed: {e}")
    finally:
        try:
            await client.disconnect()
            logger.info("🔌 Disconnected from MatrixOne (async)")
        except Exception as e:
            logger.warning(f"⚠️  Disconnect warning: {e}")


def demo_transaction_account_management():
    """Demonstrate transaction-scoped account management"""
    logger.info("\nMatrixOne Account Management - Transaction Demo")
    logger.info("=" * 60)
    
    client = Client()
    
    try:
        # Connect to MatrixOne
        client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        logger.info("✅ Connected to MatrixOne!")
        
        # Demo transaction with account operations
        logger.info("\n🔄 Transaction with account operations:")
        
        with client.transaction() as tx:
            try:
                # Note: Account operations may not be supported in transactions
                # This is a limitation of MatrixOne
                logger.info("   📝 Note: Account operations may not be supported in transactions")
                logger.info("   This is a known limitation of MatrixOne")
                
                # Try a simple operation
                result = tx.execute("SELECT USER()")
                logger.info(f"   ✅ Transaction query succeeded: {result.rows[0][0]}")
                
            except Exception as e:
                logger.error(f"   ❌ Transaction operation failed: {e}")
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ Transaction account management demo completed!")
        
    except Exception as e:
        logger.error(f"❌ Transaction demo failed: {e}")
    finally:
        client.disconnect()
        logger.info("🔌 Disconnected from MatrixOne")


def main():
    """Main demo function"""
    logger.info("🚀 MatrixOne Account Management Examples")
    logger.info("=" * 60)
    
    # Run synchronous demo
    demo_sync_account_management()
    
    # Run asynchronous demo
    asyncio.run(demo_async_account_management())
    
    # Run transaction demo
    demo_transaction_account_management()
    
    logger.info("\n🎉 All MatrixOne account management examples completed!")
    logger.info("\nKey takeaways:")
    logger.info("- MatrixOne supports basic account, user, and role management")
    logger.info("- User permission granting has limitations")
    logger.info("- Role-based permission management is recommended")
    logger.info("- Account operations may not work in transactions")
    logger.info("- Use AccountManager for correct MatrixOne behavior")


if __name__ == '__main__':
    main()
