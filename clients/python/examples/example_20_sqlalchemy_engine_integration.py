#!/usr/bin/env python3
"""
MatrixOne SQLAlchemy Engine Integration Example

This example demonstrates how to use MatrixOne Client and AsyncClient 
with existing SQLAlchemy engines, making it easy to integrate MatrixOne 
into existing SQLAlchemy-based projects.
"""

import asyncio
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine
from matrixone import Client, AsyncClient
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger

# Create MatrixOne logger for all logging
logger = create_default_logger(
    enable_performance_logging=True,
    enable_sql_logging=True
)


def demo_sync_engine_integration():
    """Demonstrate sync SQLAlchemy engine integration"""
    logger.info("🚀 Starting sync SQLAlchemy engine integration demo")
    
    # Print current configuration
    print_config()
    
    # Get connection parameters from config (supports environment variables)
    host, port, user, password, database = get_connection_params()
    
    # Create SQLAlchemy engine
    connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    
    try:
        # Create MatrixOne Client from existing SQLAlchemy engine
        client = Client.from_engine(engine)
        logger.info("✅ Created MatrixOne Client from SQLAlchemy engine")
        
        # Test basic functionality
        result = client.execute("SELECT 1 as test_value")
        rows = result.fetchall()
        logger.info(f"📊 Test query result: {rows[0][0]}")
        
        # Test database operations
        test_db = "demo_engine_db"
        client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        client.execute(f"USE {test_db}")
        
        # Create table
        client.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                price DECIMAL(10,2),
                category VARCHAR(50)
            )
        """)
        
        # Insert sample data
        client.execute("""
            INSERT INTO products VALUES 
            (1, 'Laptop', 999.99, 'Electronics'),
            (2, 'Mouse', 29.99, 'Electronics'),
            (3, 'Book', 19.99, 'Education')
        """)
        
        logger.info("📊 Sample data inserted")
        
        # Test MatrixOne-specific features
        logger.info("🔍 Testing MatrixOne-specific features:")
        
        # Clean up any existing snapshot first
        try:
            client.execute("DROP SNAPSHOT demo_snapshot")
        except:
            pass  # Snapshot might not exist
        
        # Test snapshot functionality
        snapshot_manager = client.snapshots
        snapshot = snapshot_manager.create(
            name="demo_snapshot",
            level="table",
            database=test_db,
            table="products"
        )
        logger.info(f"📸 Snapshot '{snapshot.name}' created at {snapshot.created_at}")
        
        # Test account management
        account_manager = client.account
        accounts = account_manager.list_accounts()
        logger.info(f"👥 Found {len(accounts)} accounts")
        
        # Test query with snapshot
        from matrixone.snapshot import SnapshotQueryBuilder
        builder = SnapshotQueryBuilder("demo_snapshot", client)
        result = (builder
                 .select("id", "name", "price")
                 .from_table(f"{test_db}.products")
                 .where("price > ?", 50)
                 .execute())
        
        rows = result.fetchall()
        logger.info(f"🔍 Products with price > 50 from snapshot: {len(rows)} items")
        for row in rows:
            logger.info(f"   - {row[1]}: ${row[2]}")
        
        # Clean up
        try:
            client.execute("DROP SNAPSHOT demo_snapshot")
        except:
            pass
        client.execute(f"DROP DATABASE IF EXISTS {test_db}")
        
        logger.info("✅ Sync SQLAlchemy engine integration demo completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Sync demo failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
    finally:
        if client.connected:
            client.disconnect()
        engine.dispose()


async def demo_async_engine_integration():
    """Demonstrate async SQLAlchemy engine integration"""
    logger.info("🚀 Starting async SQLAlchemy engine integration demo")
    
    # Get connection parameters from config (supports environment variables)
    host, port, user, password, database = get_connection_params()
    
    # Create SQLAlchemy async engine
    connection_string = f"mysql+aiomysql://{user}:{password}@{host}:{port}/{database}"
    engine = create_async_engine(connection_string)
    
    try:
        # Create MatrixOne AsyncClient from existing SQLAlchemy async engine
        client = AsyncClient.from_engine(engine)
        logger.info("✅ Created MatrixOne AsyncClient from SQLAlchemy async engine")
        
        # Test basic functionality
        result = await client.execute("SELECT 2 as test_value")
        rows = result.fetchall()
        logger.info(f"📊 Test query result: {rows[0][0]}")
        
        # Test database operations
        test_db = "demo_async_engine_db"
        await client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        await client.execute(f"USE {test_db}")
        
        # Create table
        await client.execute("""
            CREATE TABLE IF NOT EXISTS async_products (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                price DECIMAL(10,2),
                category VARCHAR(50)
            )
        """)
        
        # Clear existing data and insert sample data
        await client.execute("DELETE FROM async_products")
        await client.execute("""
            INSERT INTO async_products VALUES 
            (1, 'Async Laptop', 999.99, 'Electronics'),
            (2, 'Async Mouse', 29.99, 'Electronics'),
            (3, 'Async Book', 19.99, 'Education')
        """)
        
        logger.info("📊 Sample data inserted (async)")
        
        # Test MatrixOne-specific features
        logger.info("🔍 Testing MatrixOne-specific async features:")
        
        # Test async snapshot functionality
        from matrixone.async_client import AsyncSnapshotManager
        from matrixone.snapshot import SnapshotLevel
        
        # Clean up any existing snapshot first
        try:
            await client.execute("DROP SNAPSHOT demo_async_snapshot")
        except:
            pass  # Snapshot might not exist
        
        async_snapshot_manager = AsyncSnapshotManager(client)
        async_snapshot = await async_snapshot_manager.create(
            name="demo_async_snapshot",
            level=SnapshotLevel.TABLE,
            database=test_db,
            table="async_products"
        )
        logger.info(f"📸 Async Snapshot '{async_snapshot.name}' created at {async_snapshot.created_at}")
        
        # Test async query with snapshot
        from matrixone.snapshot import SnapshotQueryBuilder
        builder = SnapshotQueryBuilder("demo_async_snapshot", client)
        result = await (builder
                       .select("id", "name", "price")
                       .from_table(f"{test_db}.async_products")
                       .where("price > ?", 50)
                       .execute())
        
        rows = result.fetchall()
        logger.info(f"🔍 Async products with price > 50 from snapshot: {len(rows)} items")
        for row in rows:
            logger.info(f"   - {row[1]}: ${row[2]}")
        
        # Clean up
        try:
            await client.execute("DROP SNAPSHOT demo_async_snapshot")
        except:
            pass
        await client.execute(f"DROP DATABASE IF EXISTS {test_db}")
        
        logger.info("✅ Async SQLAlchemy engine integration demo completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Async demo failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
    finally:
        if client.connected():
            await client.disconnect()
        await engine.dispose()


def demo_engine_reuse():
    """Demonstrate reusing the same engine for multiple clients"""
    logger.info("🚀 Starting engine reuse demo")
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    # Create SQLAlchemy engine
    connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    
    try:
        # Create multiple clients from the same engine
        client1 = Client.from_engine(engine, enable_sql_logging=True)
        client2 = Client.from_engine(engine, slow_sql_threshold=0.1)
        
        logger.info("✅ Created multiple clients from the same engine")
        
        # Test that both clients work independently
        result1 = client1.execute("SELECT 'Client 1' as client_name")
        result2 = client2.execute("SELECT 'Client 2' as client_name")
        
        rows1 = result1.fetchall()
        rows2 = result2.fetchall()
        
        logger.info(f"📊 Client 1 result: {rows1[0][0]}")
        logger.info(f"📊 Client 2 result: {rows2[0][0]}")
        
        # Test that both clients can access MatrixOne features
        accounts1 = client1.account.list_accounts()
        accounts2 = client2.account.list_accounts()
        
        logger.info(f"👥 Client 1 found {len(accounts1)} accounts")
        logger.info(f"👥 Client 2 found {len(accounts2)} accounts")
        
        logger.info("✅ Engine reuse demo completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Engine reuse demo failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
    finally:
        if client1.connected:
            client1.disconnect()
        if client2.connected:
            client2.disconnect()
        engine.dispose()


def demo_custom_engine_configuration():
    """Demonstrate using custom engine configuration with MatrixOne"""
    logger.info("🚀 Starting custom engine configuration demo")
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    # Create SQLAlchemy engine with custom configuration
    connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(
        connection_string,
        pool_size=20,  # Custom pool size
        max_overflow=30,  # Custom max overflow
        pool_timeout=60,  # Custom pool timeout
        pool_recycle=7200,  # Custom pool recycle time
        echo=True,  # Enable SQL logging
    )
    
    try:
        # Create MatrixOne Client with custom configuration
        client = Client.from_engine(
            engine,
            enable_sql_logging=True,
            enable_performance_logging=True,
            slow_sql_threshold=0.5
        )
        
        logger.info("✅ Created MatrixOne Client with custom engine configuration")
        
        # Test functionality
        result = client.execute("SELECT 'Custom Engine Config' as test_value")
        rows = result.fetchall()
        logger.info(f"📊 Test query result: {rows[0][0]}")
        
        # Test that custom configuration is working
        logger.info("🔧 Custom engine configuration is active")
        
        logger.info("✅ Custom engine configuration demo completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Custom engine configuration demo failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
    finally:
        if client.connected:
            client.disconnect()
        engine.dispose()


def main():
    """Main function"""
    logger.info("🎯 MatrixOne SQLAlchemy Engine Integration Demo")
    logger.info("=" * 60)
    
    # Run sync engine integration demo
    demo_sync_engine_integration()
    
    logger.info("\n" + "=" * 60)
    
    # Run async engine integration demo
    asyncio.run(demo_async_engine_integration())
    
    logger.info("\n" + "=" * 60)
    
    # Run engine reuse demo
    demo_engine_reuse()
    
    logger.info("\n" + "=" * 60)
    
    # Run custom engine configuration demo
    demo_custom_engine_configuration()
    
    logger.info("\n🎉 All demos completed!")


if __name__ == "__main__":
    main()
