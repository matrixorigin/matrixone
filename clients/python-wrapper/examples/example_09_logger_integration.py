#!/usr/bin/env python3
"""
MatrixOne Logger Integration Examples

This example demonstrates how to use the MatrixOne Python SDK with custom logging:
1. Default logger configuration
2. Custom logger integration
3. Performance logging
4. SQL query logging
5. Structured logging
6. Error tracking

This example shows how to integrate MatrixOne SDK logging with your business application.
"""

import logging
import asyncio
from matrixone import Client, AsyncClient
from matrixone.logger import MatrixOneLogger, create_default_logger, create_custom_logger
from matrixone.config import get_connection_params, print_config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


def demo_default_logger():
    """Demonstrate default logger configuration"""
    logger.info("🚀 MatrixOne Default Logger Demo")
    logger.info("=" * 60)
    
    # Create client with default logger
    client = Client(
        enable_performance_logging=True,
        enable_sql_logging=True,
        enable_full_sql_logging=True
    )
    
    try:
        # Connect to MatrixOne
        client.connect(host, port, user, password, database)
        
        # Execute some queries to see logging in action
        client.execute("SELECT 1 as test_value")
        client.execute("SELECT VERSION() as version")
        client.execute("SHOW DATABASES")
        
        # Test error logging
        try:
            client.execute("SELECT * FROM non_existent_table")
        except Exception as e:
            logger.info(f"Expected error caught: {e}")
        
        client.disconnect()
        
    except Exception as e:
        logger.error(f"Demo failed: {e}")


def demo_custom_logger():
    """Demonstrate custom logger integration"""
    logger.info("\n🚀 MatrixOne Custom Logger Demo")
    logger.info("=" * 60)
    
    # Create custom logger
    custom_logger = logging.getLogger('my_app.matrixone')
    custom_logger.setLevel(logging.DEBUG)
    
    # Create custom handler
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    
    # Create custom formatter
    formatter = logging.Formatter(
        '%(asctime)s [%(name)s] %(levelname)s: %(message)s'
    )
    handler.setFormatter(formatter)
    
    # Add handler to logger
    custom_logger.addHandler(handler)
    custom_logger.propagate = False
    
    # Create MatrixOne logger from custom logger
    matrixone_logger = create_custom_logger(
        logger=custom_logger,
        enable_performance_logging=True,
        enable_sql_logging=True
    )
    
    # Create client with custom logger
    client = Client(logger=matrixone_logger, enable_full_sql_logging=True)
    
    try:
        # Connect to MatrixOne
        client.connect(host, port, user, password, database)
        
        # Execute queries with custom logging
        client.execute("SELECT 'Custom Logger Test' as message")
        client.execute("SELECT COUNT(*) as db_count FROM information_schema.schemata")
        
        client.disconnect()
        
    except Exception as e:
        logger.error(f"Custom logger demo failed: {e}")


def demo_structured_logging():
    """Demonstrate structured logging with context"""
    logger.info("\n🚀 MatrixOne Structured Logging Demo")
    logger.info("=" * 60)
    
    # Create logger with custom format
    matrixone_logger = create_default_logger(
        level=logging.INFO,
        format_string='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        enable_performance_logging=True,
        enable_sql_logging=True
    )
    
    # Create client with structured logger
    client = Client(logger=matrixone_logger, enable_full_sql_logging=True)
    
    try:
        # Connect with detailed logging
        client.connect(host, port, user, password, database)
        
        # Test account operations with structured logging
        client.account.create_account(
            account_name="logger_test_account",
            admin_name="logger_admin",
            password="test_pass",
            comment="Account for logger testing"
        )
        
        # Test user operations
        client.account.create_user(
            user_name="logger_test_user",
            password="user_pass",
            comment="User for logger testing"
        )
        
        # Test snapshot operations
        client.snapshots.create(
            name="logger_test_snapshot",
            database="test",
            level="database",
            comment="Snapshot for logger testing"
        )
        
        # Cleanup
        client.snapshots.delete("logger_test_snapshot")
        client.account.drop_user("logger_test_user", if_exists=True)
        client.account.drop_account("logger_test_account")
        
        client.disconnect()
        
    except Exception as e:
        logger.error(f"Structured logging demo failed: {e}")


async def demo_async_logger():
    """Demonstrate async client with logger"""
    logger.info("\n🚀 MatrixOne Async Logger Demo")
    logger.info("=" * 60)
    
    # Create async client with default logger
    client = AsyncClient(
        enable_performance_logging=True,
        enable_sql_logging=True,
        enable_full_sql_logging=True
    )
    
    try:
        # Connect to MatrixOne
        await client.connect(host, port, user, password, database)
        
        # Execute async queries
        result1 = await client.execute("SELECT 'Async Logger Test' as message")
        result2 = await client.execute("SELECT VERSION() as version")
        
        # Test async account operations
        await client.account.create_account(
            account_name="async_logger_account",
            admin_name="async_admin",
            password="async_pass",
            comment="Async account for logger testing"
        )
        
        # Cleanup
        await client.account.drop_account("async_logger_account")
        
        await client.disconnect()
        
    except Exception as e:
        logger.error(f"Async logger demo failed: {e}")


def demo_logger_levels():
    """Demonstrate different logger levels"""
    logger.info("\n🚀 MatrixOne Logger Levels Demo")
    logger.info("=" * 60)
    
    # Test different log levels
    matrixone_logger = create_default_logger(
        level=logging.DEBUG,
        enable_performance_logging=True,
        enable_sql_logging=True
    )
    
    client = Client(logger=matrixone_logger, enable_full_sql_logging=True)
    
    try:
        client.connect(host, port, user, password, database)
        
        # Test different operations to see various log levels
        client.execute("SELECT 1")  # INFO level
        
        # Test error scenario
        try:
            client.execute("INVALID SQL SYNTAX")
        except Exception:
            pass  # ERROR level
        
        client.disconnect()
        
    except Exception as e:
        logger.error(f"Logger levels demo failed: {e}")


def demo_business_integration():
    """Demonstrate logger integration in business application"""
    logger.info("\n🚀 MatrixOne Business Integration Demo")
    logger.info("=" * 60)
    
    # Simulate business application logger
    app_logger = logging.getLogger('my_business_app')
    app_logger.setLevel(logging.INFO)
    
    # Create handler for business app
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s [BUSINESS] %(levelname)s: %(message)s'
    )
    handler.setFormatter(formatter)
    app_logger.addHandler(handler)
    app_logger.propagate = False
    
    # Create MatrixOne logger from business logger
    matrixone_logger = create_custom_logger(
        logger=app_logger,
        enable_performance_logging=True,
        enable_sql_logging=False  # Disable SQL logging in production
    )
    
    # Simulate business operations
    app_logger.info("Starting business operation")
    
    client = Client(logger=matrixone_logger, enable_full_sql_logging=True)
    
    try:
        # Business operation: Get system information
        app_logger.info("Connecting to database for system check")
        client.connect(host, port, user, password, database)
        
        # Business operation: Check system health
        app_logger.info("Checking system health")
        result = client.execute("SELECT VERSION() as version, NOW() as current_time")
        app_logger.info(f"System check completed: {result.rows[0]}")
        
        # Business operation: Get database statistics
        app_logger.info("Gathering database statistics")
        result = client.execute("SHOW DATABASES")
        app_logger.info(f"Found {len(result.rows)} databases")
        
        client.disconnect()
        app_logger.info("Business operation completed successfully")
        
    except Exception as e:
        app_logger.error(f"Business operation failed: {e}")


def demo_performance_monitoring():
    """Demonstrate performance monitoring with logger"""
    logger.info("\n🚀 MatrixOne Performance Monitoring Demo")
    logger.info("=" * 60)
    
    # Create logger with performance monitoring
    matrixone_logger = create_default_logger(
        enable_performance_logging=True,
        enable_sql_logging=True
    )
    
    client = Client(logger=matrixone_logger, enable_full_sql_logging=True)
    
    try:
        client.connect(host, port, user, password, database)
        
        # Test performance logging with different operations
        operations = [
            "SELECT 1",
            "SELECT VERSION()",
            "SHOW DATABASES",
            "SHOW TABLES",
            "SELECT COUNT(*) FROM information_schema.schemata"
        ]
        
        for operation in operations:
            client.execute(operation)
        
        # Test account operations performance
        client.account.create_account(
            account_name="perf_test_account",
            admin_name="perf_admin",
            password="perf_pass",
            comment="Performance test account"
        )
        
        client.account.drop_account("perf_test_account")
        
        client.disconnect()
        
    except Exception as e:
        logger.error(f"Performance monitoring demo failed: {e}")


def main():
    """Main function to run all logger integration examples"""
    logger.info("🚀 MatrixOne Logger Integration Examples")
    logger.info("=" * 70)
    
    # Run all logger demos
    demo_default_logger()
    demo_custom_logger()
    demo_structured_logging()
    demo_logger_levels()
    demo_business_integration()
    demo_performance_monitoring()
    
    # Run async demo
    asyncio.run(demo_async_logger())
    
    logger.info("\n🎉 All logger integration examples completed!")
    logger.info("\nKey features demonstrated:")
    logger.info("- ✅ Default logger configuration")
    logger.info("- ✅ Custom logger integration")
    logger.info("- ✅ Structured logging with context")
    logger.info("- ✅ Async client logging")
    logger.info("- ✅ Different logger levels")
    logger.info("- ✅ Business application integration")
    logger.info("- ✅ Performance monitoring")
    logger.info("- ✅ SQL query logging")
    logger.info("- ✅ Error tracking and reporting")


if __name__ == '__main__':
    main()
