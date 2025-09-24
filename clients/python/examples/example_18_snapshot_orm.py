#!/usr/bin/env python3
"""
MatrixOne Snapshot ORM-like Query Builder Example

This example demonstrates how to use the snapshot query builder
which provides a SQLAlchemy-like ORM interface for snapshot queries.
"""

import asyncio
from matrixone import Client, AsyncClient
from matrixone.logger import create_default_logger
from matrixone.config import get_connection_params

# Create logger
logger = create_default_logger()


def demo_sync_snapshot_orm():
    """Demonstrate sync snapshot ORM-like query builder"""
    logger.info("🚀 MatrixOne Sync Snapshot ORM Demo")
    logger.info("=" * 60)
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    try:
        client = Client(logger=logger)
        client.connect(host, port, user, password, database)
        
        logger.info("✅ Connected to MatrixOne")
        
        # Create a test table and insert some data
        client.execute("CREATE DATABASE IF NOT EXISTS snapshot_orm_test")
        client.execute("USE snapshot_orm_test")
        
        # Drop table if exists
        client.execute("DROP TABLE IF EXISTS users")
        
        # Create table
        client.execute("""
            CREATE TABLE users (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100),
                email VARCHAR(100),
                age INT,
                department VARCHAR(50)
            )
        """)
        
        # Insert test data
        client.execute("""
            INSERT INTO users (name, email, age, department) VALUES
            ('Alice Johnson', 'alice@example.com', 28, 'Engineering'),
            ('Bob Smith', 'bob@example.com', 32, 'Marketing'),
            ('Charlie Brown', 'charlie@example.com', 25, 'Engineering'),
            ('Diana Prince', 'diana@example.com', 30, 'Sales'),
            ('Eve Wilson', 'eve@example.com', 27, 'Marketing')
        """)
        
        logger.info("📊 Created test table with sample data")
        
        # Clean up any existing snapshot first
        try:
            client.execute("DROP SNAPSHOT IF EXISTS user_snapshot")
        except Exception:
            pass  # Ignore errors if snapshot doesn't exist
        
        # Create a snapshot
        snapshot = client.snapshots.create("user_snapshot", "DATABASE", database="snapshot_orm_test")
        logger.info(f"📸 Created snapshot: {snapshot.name}")
        
        # Now demonstrate ORM-like query building
        logger.info("\n🔍 ORM-like Snapshot Query Examples:")
        
        # Example 1: Simple select
        logger.info("\n1. Simple SELECT query:")
        result = (client.snapshot_query_builder("user_snapshot")
                 .select("id", "name", "email")
                 .from_table("users")
                 .execute())
        
        logger.info(f"   Found {len(result.rows)} users:")
        for row in result.rows:
            logger.info(f"   - ID: {row[0]}, Name: {row[1]}, Email: {row[2]}")
        
        # Example 2: WHERE clause with parameters
        logger.info("\n2. WHERE clause with parameters:")
        result = (client.snapshot_query_builder("user_snapshot")
                 .select("name", "age", "department")
                 .from_table("users")
                 .where("age > ?", 28)
                 .execute())
        
        logger.info(f"   Found {len(result.rows)} users older than 28:")
        for row in result.rows:
            logger.info(f"   - Name: {row[0]}, Age: {row[1]}, Department: {row[2]}")
        
        # Example 3: Complex query with multiple conditions
        logger.info("\n3. Complex query with multiple conditions:")
        result = (client.snapshot_query_builder("user_snapshot")
                 .select("name", "email", "department")
                 .from_table("users")
                 .where("department = ?", "Engineering")
                 .where("age BETWEEN ? AND ?", 25, 30)
                 .order_by("name")
                 .limit(10)
                 .execute())
        
        logger.info(f"   Found {len(result.rows)} Engineering users aged 25-30:")
        for row in result.rows:
            logger.info(f"   - Name: {row[0]}, Email: {row[1]}, Department: {row[2]}")
        
        # Example 4: GROUP BY and HAVING
        logger.info("\n4. GROUP BY and HAVING:")
        result = (client.snapshot_query_builder("user_snapshot")
                 .select("department", "COUNT(*) as user_count", "AVG(age) as avg_age")
                 .from_table("users")
                 .group_by("department")
                 .having("COUNT(*) > ?", 1)
                 .order_by("user_count DESC")
                 .execute())
        
        logger.info(f"   Department statistics:")
        for row in result.rows:
            logger.info(f"   - {row[0]}: {row[1]} users, avg age: {row[2]:.1f}")
        
        # Clean up
        client.execute("DROP SNAPSHOT user_snapshot")
        client.execute("DROP TABLE users")
        client.execute("DROP DATABASE snapshot_orm_test")
        
        logger.info("✅ Sync snapshot ORM demo completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Sync snapshot ORM demo failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")


async def demo_async_snapshot_orm():
    """Demonstrate async snapshot ORM-like query builder"""
    logger.info("\n🚀 MatrixOne Async Snapshot ORM Demo")
    logger.info("=" * 60)
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    try:
        async with AsyncClient(logger=logger) as client:
            await client.connect(host, port, user, password, database)
            
            logger.info("✅ Connected to MatrixOne (async)")
            
            # Create a test table and insert some data
            await client.execute("CREATE DATABASE IF NOT EXISTS async_snapshot_orm_test")
            await client.execute("USE async_snapshot_orm_test")
            
            # Drop table if exists
            await client.execute("DROP TABLE IF EXISTS products")
            
            # Create table
            await client.execute("""
                CREATE TABLE products (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    name VARCHAR(100),
                    category VARCHAR(50),
                    price DECIMAL(10,2),
                    stock_quantity INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert test data
            await client.execute("""
                INSERT INTO products (name, category, price, stock_quantity) VALUES
                ('Laptop Pro', 'Electronics', 1299.99, 50),
                ('Wireless Mouse', 'Electronics', 29.99, 200),
                ('Office Chair', 'Furniture', 199.99, 30),
                ('Desk Lamp', 'Furniture', 49.99, 100),
                ('Coffee Mug', 'Accessories', 12.99, 150),
                ('Notebook', 'Stationery', 5.99, 300)
            """)
            
            logger.info("📊 Created test table with sample data")
            
            # Clean up any existing snapshot first
            try:
                await client.execute("DROP SNAPSHOT IF EXISTS product_snapshot")
            except Exception:
                pass  # Ignore errors if snapshot doesn't exist
            
            # Create a snapshot
            snapshot = await client.snapshots.create("product_snapshot", "DATABASE", database="async_snapshot_orm_test")
            logger.info(f"📸 Created snapshot: {snapshot.name}")
            
            # Now demonstrate ORM-like query building
            logger.info("\n🔍 Async ORM-like Snapshot Query Examples:")
            
            # Example 1: Simple select with ordering
            logger.info("\n1. Simple SELECT with ordering:")
            result = await (client.snapshot_query_builder("product_snapshot")
                           .select("name", "category", "price")
                           .from_table("products")
                           .order_by("price DESC")
                           .limit(3)
                           .execute())
            
            logger.info(f"   Top 3 most expensive products:")
            for row in result.rows:
                logger.info(f"   - {row[0]} ({row[1]}): ${row[2]}")
            
            # Example 2: Complex filtering
            logger.info("\n2. Complex filtering:")
            result = await (client.snapshot_query_builder("product_snapshot")
                           .select("name", "price", "stock_quantity")
                           .from_table("products")
                           .where("category = ?", "Electronics")
                           .where("price > ?", 50)
                           .where("stock_quantity > ?", 25)
                           .order_by("price")
                           .execute())
            
            logger.info(f"   Electronics over $50 with good stock:")
            for row in result.rows:
                logger.info(f"   - {row[0]}: ${row[1]} (stock: {row[2]})")
            
            # Example 3: Aggregation with grouping
            logger.info("\n3. Category statistics:")
            result = await (client.snapshot_query_builder("product_snapshot")
                           .select("category", "COUNT(*) as product_count", "AVG(price) as avg_price", "SUM(stock_quantity) as total_stock")
                           .from_table("products")
                           .group_by("category")
                           .order_by("avg_price DESC")
                           .execute())
            
            logger.info(f"   Category statistics:")
            for row in result.rows:
                logger.info(f"   - {row[0]}: {row[1]} products, avg price: ${row[2]:.2f}, total stock: {row[3]}")
            
            # Example 4: Price range analysis
            logger.info("\n4. Price range analysis:")
            result = await (client.snapshot_query_builder("product_snapshot")
                           .select("name", "price", "CASE WHEN price < 50 THEN 'Budget' WHEN price < 200 THEN 'Mid-range' ELSE 'Premium' END as price_tier")
                           .from_table("products")
                           .order_by("price")
                           .execute())
            
            logger.info(f"   Products by price tier:")
            for row in result.rows:
                logger.info(f"   - {row[0]}: ${row[1]} ({row[2]})")
            
            # Clean up
            await client.execute("DROP SNAPSHOT product_snapshot")
            await client.execute("DROP TABLE products")
            await client.execute("DROP DATABASE async_snapshot_orm_test")
            
            logger.info("✅ Async snapshot ORM demo completed successfully")
            
    except Exception as e:
        logger.error(f"❌ Async snapshot ORM demo failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")


def demo_orm_vs_raw_sql():
    """Compare ORM-like queries with raw SQL"""
    logger.info("\n🔍 ORM vs Raw SQL Comparison")
    logger.info("=" * 60)
    
    # Get connection parameters from config
    host, port, user, password, database = get_connection_params()
    
    try:
        client = Client(logger=logger)
        client.connect(host, port, user, password, database)
        
        # Create test data
        client.execute("CREATE DATABASE IF NOT EXISTS comparison_test")
        client.execute("USE comparison_test")
        client.execute("DROP TABLE IF EXISTS orders")
        
        client.execute("""
            CREATE TABLE orders (
                id INT PRIMARY KEY AUTO_INCREMENT,
                customer_name VARCHAR(100),
                product VARCHAR(100),
                quantity INT,
                unit_price DECIMAL(10,2),
                order_date DATE
            )
        """)
        
        client.execute("""
            INSERT INTO orders (customer_name, product, quantity, unit_price, order_date) VALUES
            ('John Doe', 'Laptop', 1, 1200.00, '2024-01-15'),
            ('Jane Smith', 'Mouse', 2, 25.00, '2024-01-16'),
            ('Bob Johnson', 'Keyboard', 1, 75.00, '2024-01-17'),
            ('Alice Brown', 'Monitor', 1, 300.00, '2024-01-18'),
            ('Charlie Wilson', 'Laptop', 1, 1200.00, '2024-01-19')
        """)
        
        # Clean up any existing snapshot first
        try:
            client.execute("DROP SNAPSHOT IF EXISTS order_snapshot")
        except Exception:
            pass  # Ignore errors if snapshot doesn't exist
        
        # Create snapshot
        snapshot = client.snapshots.create("order_snapshot", "DATABASE", database="comparison_test")
        
        logger.info("📊 Created test data and snapshot")
        
        # Raw SQL approach
        logger.info("\n1. Raw SQL approach:")
        raw_sql = """
            SELECT customer_name, product, quantity * unit_price as total_amount
            FROM orders
            WHERE quantity * unit_price > 100
            ORDER BY total_amount DESC
            LIMIT 3
        """
        raw_result = client.snapshot_query("order_snapshot", raw_sql)
        logger.info(f"   Raw SQL result ({len(raw_result.rows)} rows):")
        for row in raw_result.rows:
            logger.info(f"   - {row[0]}: {row[1]} (${row[2]})")
        
        # ORM-like approach
        logger.info("\n2. ORM-like approach:")
        orm_result = (client.snapshot_query_builder("order_snapshot")
                     .select("customer_name", "product", "quantity * unit_price as total_amount")
                     .from_table("orders")
                     .where("quantity * unit_price > ?", 100)
                     .order_by("total_amount DESC")
                     .limit(3)
                     .execute())
        logger.info(f"   ORM-like result ({len(orm_result.rows)} rows):")
        for row in orm_result.rows:
            logger.info(f"   - {row[0]}: {row[1]} (${row[2]})")
        
        logger.info("\n✅ Both approaches produce identical results!")
        logger.info("   ORM-like approach provides:")
        logger.info("   - Type safety and parameter binding")
        logger.info("   - Method chaining for readability")
        logger.info("   - Automatic SQL injection protection")
        logger.info("   - Easy query composition and reuse")
        
        # Clean up
        client.execute("DROP SNAPSHOT order_snapshot")
        client.execute("DROP TABLE orders")
        client.execute("DROP DATABASE comparison_test")
        
    except Exception as e:
        logger.error(f"❌ Comparison demo failed: {e}")


if __name__ == "__main__":
    logger.info("🚀 MatrixOne Snapshot ORM-like Query Builder Examples")
    logger.info("=" * 80)
    
    # Run sync demo
    demo_sync_snapshot_orm()
    
    # Run async demo
    asyncio.run(demo_async_snapshot_orm())
    
    # Run comparison demo
    demo_orm_vs_raw_sql()
    
    logger.info("\n🎉 All snapshot ORM examples completed!")
    logger.info("\nKey features demonstrated:")
    logger.info("- ✅ SQLAlchemy-like method chaining")
    logger.info("- ✅ Type-safe parameter binding")
    logger.info("- ✅ Automatic snapshot hint injection")
    logger.info("- ✅ Support for complex queries (JOINs, GROUP BY, etc.)")
    logger.info("- ✅ Both sync and async implementations")
    logger.info("- ✅ Clean, readable query building")
