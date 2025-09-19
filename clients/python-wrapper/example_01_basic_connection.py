#!/usr/bin/env python3
"""
MatrixOne Basic Connection Examples

This example demonstrates all basic connection methods and login formats:
1. Basic connection with different formats
2. Login format variations
3. Connection error handling
4. Connection information retrieval

This is the foundation example for all MatrixOne Python client usage.
"""

import logging
import asyncio
from matrixone import Client, AsyncClient
from matrixone.account import AccountManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


def demo_basic_connection():
    """Demonstrate basic connection methods"""
    logger.info("🚀 MatrixOne Basic Connection Demo")
    logger.info("=" * 60)
    
    # Test 1: Simple connection
    logger.info("\n=== Test 1: Simple Connection ===")
    try:
        client = Client()
        client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        logger.info("✅ Basic connection successful")
        
        # Test basic query
        result = client.execute("SELECT 1 as test_value, USER() as user_info")
        logger.info(f"   Test query result: {result.rows[0]}")
        
        # Get connection info
        login_info = client.get_login_info()
        logger.info(f"   Login info: {login_info}")
        
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Basic connection failed: {e}")


def demo_login_formats():
    """Demonstrate all supported login formats"""
    logger.info("\n=== Test 2: Login Format Variations ===")
    
    # Format 1: Legacy format (simple username)
    logger.info("\n🔌 Format 1: Legacy format (simple username)")
    try:
        client = Client()
        client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        login_info = client.get_login_info()
        logger.info(f"   ✅ Login info: {login_info}")
        client.disconnect()
    except Exception as e:
        logger.error(f"   ❌ Failed: {e}")
    
    # Format 2: Direct format (account#user)
    logger.info("\n🔌 Format 2: Direct format (account#user)")
    try:
        client = Client()
        client.connect('127.0.0.1', 6001, 'sys#root', '111', 'test')
        login_info = client.get_login_info()
        logger.info(f"   ✅ Login info: {login_info}")
        client.disconnect()
    except Exception as e:
        logger.error(f"   ❌ Failed: {e}")
    
    # Format 3: User with role (separate parameters)
    logger.info("\n🔌 Format 3: User with role (separate parameters)")
    try:
        client = Client()
        client.connect('127.0.0.1', 6001, 'root', '111', 'test', role='admin')
        login_info = client.get_login_info()
        logger.info(f"   ✅ Login info: {login_info}")
        client.disconnect()
    except Exception as e:
        logger.info(f"   ⚠️ Expected failure: {e}")
    
    # Format 4: Account with separate parameters
    logger.info("\n🔌 Format 4: Account with separate parameters")
    try:
        client = Client()
        client.connect('127.0.0.1', 6001, 'root', '111', 'test', account='sys')
        login_info = client.get_login_info()
        logger.info(f"   ✅ Login info: {login_info}")
        client.disconnect()
    except Exception as e:
        logger.error(f"   ❌ Failed: {e}")


def demo_connection_error_handling():
    """Demonstrate connection error handling"""
    logger.info("\n=== Test 3: Connection Error Handling ===")
    
    # Test invalid credentials
    logger.info("\n🔌 Test invalid credentials")
    try:
        client = Client()
        client.connect('127.0.0.1', 6001, 'invalid_user', 'invalid_pass', 'test')
        logger.error("   ❌ Should have failed but didn't!")
    except Exception as e:
        logger.info(f"   ✅ Correctly failed: {e}")
    
    # Test invalid host
    logger.info("\n🔌 Test invalid host")
    try:
        client = Client()
        client.connect('192.168.1.999', 6001, 'root', '111', 'test')
        logger.error("   ❌ Should have failed but didn't!")
    except Exception as e:
        logger.info(f"   ✅ Correctly failed: {e}")
    
    # Test invalid port
    logger.info("\n🔌 Test invalid port")
    try:
        client = Client()
        client.connect('127.0.0.1', 9999, 'root', '111', 'test')
        logger.error("   ❌ Should have failed but didn't!")
    except Exception as e:
        logger.info(f"   ✅ Correctly failed: {e}")


def demo_connection_info():
    """Demonstrate connection information retrieval"""
    logger.info("\n=== Test 4: Connection Information ===")
    
    try:
        client = Client()
        client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        
        # Get login info
        login_info = client.get_login_info()
        logger.info(f"   Login info: {login_info}")
        
        # Get user information
        result = client.execute("SELECT USER(), CURRENT_USER(), DATABASE()")
        user_info = result.rows[0]
        logger.info(f"   USER(): {user_info[0]}")
        logger.info(f"   CURRENT_USER(): {user_info[1]}")
        logger.info(f"   DATABASE(): {user_info[2]}")
        
        # Get connection parameters
        logger.info(f"   Connection parameters: {client._connection_params}")
        
        client.disconnect()
        
    except Exception as e:
        logger.error(f"❌ Connection info demo failed: {e}")


async def demo_async_connection():
    """Demonstrate async connection"""
    logger.info("\n=== Test 5: Async Connection ===")
    
    client = None
    try:
        client = AsyncClient()
        await client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        logger.info("✅ Async connection successful")
        
        # Test async query
        result = await client.execute("SELECT 1 as async_test, USER() as user_info")
        logger.info(f"   Async query result: {result.rows[0]}")
        
        # Get login info
        login_info = client.get_login_info()
        logger.info(f"   Login info: {login_info}")
        
    except Exception as e:
        logger.error(f"❌ Async connection failed: {e}")
    finally:
        # Ensure proper cleanup
        if client:
            try:
                await client.disconnect()
            except Exception as e:
                logger.debug(f"Async disconnect warning: {e}")


def demo_connection_pooling():
    """Demonstrate multiple connections"""
    logger.info("\n=== Test 6: Multiple Connections ===")
    
    clients = []
    try:
        # Create multiple connections
        for i in range(3):
            client = Client()
            client.connect('127.0.0.1', 6001, 'root', '111', 'test')
            clients.append(client)
            logger.info(f"   ✅ Created connection {i+1}")
        
        # Test each connection
        for i, client in enumerate(clients):
            result = client.execute(f"SELECT {i+1} as connection_id, USER() as user")
            logger.info(f"   Connection {i+1} result: {result.rows[0]}")
        
        # Close all connections
        for i, client in enumerate(clients):
            client.disconnect()
            logger.info(f"   ✅ Closed connection {i+1}")
        
    except Exception as e:
        logger.error(f"❌ Multiple connections demo failed: {e}")
        # Cleanup on error
        for client in clients:
            try:
                client.disconnect()
            except:
                pass


def main():
    """Main demo function"""
    logger.info("🚀 MatrixOne Basic Connection Examples")
    logger.info("=" * 60)
    
    # Run synchronous demos
    demo_basic_connection()
    demo_login_formats()
    demo_connection_error_handling()
    demo_connection_info()
    demo_connection_pooling()
    
    # Run async demo
    asyncio.run(demo_async_connection())
    
    logger.info("\n🎉 Basic connection examples completed!")
    logger.info("\nKey takeaways:")
    logger.info("- ✅ Basic connection works with simple credentials")
    logger.info("- ✅ Multiple login formats are supported")
    logger.info("- ✅ Error handling works correctly")
    logger.info("- ✅ Connection information is accessible")
    logger.info("- ✅ Async connections work")
    logger.info("- ✅ Multiple connections can be managed")


if __name__ == '__main__':
    main()
