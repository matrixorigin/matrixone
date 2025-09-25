#!/usr/bin/env python3
"""
MatrixOne SQLAlchemy-style ORM Example

This example demonstrates how to use MatrixOne with a SQLAlchemy-like ORM interface.
You can use client.query(Model).snapshot("snapshot_name").filter_by(...).all() syntax.
"""

import asyncio
import logging
from matrixone import Client, AsyncClient, SnapshotManager, SnapshotLevel
from sqlalchemy import Column, Integer, String, DECIMAL, TIMESTAMP, desc
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
from matrixone.config import get_connection_params, print_config
from matrixone.logger import create_default_logger

# Create MatrixOne logger for all logging
logger = create_default_logger(
    enable_performance_logging=True,
    enable_sql_logging=True
)

# Define models using SQLAlchemy-style syntax
class Product(Base):
    """Product model"""
    __tablename__ = "products"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    price = Column(DECIMAL(10, 2))
    category = Column(String(50))
    created_at = Column(TIMESTAMP)

class Order(Base):
    """Order model"""
    __tablename__ = "orders"
    
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer)
    quantity = Column(Integer)
    total_amount = Column(DECIMAL(10, 2))
    order_date = Column(TIMESTAMP)

def demo_sync_sqlalchemy_style_orm():
    """Demonstrate sync SQLAlchemy-style ORM usage"""
    logger.info("🚀 Starting sync SQLAlchemy-style ORM demo")
    
    # Print current configuration
    print_config()
    
    # Get connection parameters from config (supports environment variables)
    host, port, user, password, database = get_connection_params()
    
    try:
        # Create client
        client = Client()
        client.connect(host, port, user, password, database)
        
        logger.info("✅ Connected to MatrixOne")
        
        # Clean up any existing snapshots and tables
        try:
            client.execute("DROP SNAPSHOT snapshot_products")
        except:
            pass  # Snapshot might not exist
        try:
            client.execute("DROP TABLE orders")
        except:
            pass  # Table might not exist
        try:
            client.execute("DROP TABLE products")
        except:
            pass  # Table might not exist
        
        # Create tables
        client.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                price DECIMAL(10,2),
                category VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        client.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INT PRIMARY KEY,
                product_id INT,
                quantity INT,
                total_amount DECIMAL(10,2),
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Clear existing data and insert sample data
        client.execute("DELETE FROM products")
        client.execute("DELETE FROM orders")
        client.execute("""
            INSERT INTO products (id, name, price, category) VALUES
            (1, 'Laptop', 999.99, 'Electronics'),
            (2, 'Mouse', 29.99, 'Electronics'),
            (3, 'Keyboard', 79.99, 'Electronics'),
            (4, 'Book', 19.99, 'Books'),
            (5, 'Pen', 2.99, 'Office')
        """)
        
        client.execute("""
            INSERT INTO orders (id, product_id, quantity, total_amount) VALUES
            (1, 1, 1, 999.99),
            (2, 2, 2, 59.98),
            (3, 3, 1, 79.99),
            (4, 4, 3, 59.97),
            (5, 5, 10, 29.90)
        """)
        
        logger.info("📊 Sample data inserted")
        
        # Create a snapshot using SnapshotManager API
        snapshot_manager = SnapshotManager(client)
        snapshot = snapshot_manager.create(
            name="snapshot_products",
            level=SnapshotLevel.TABLE,
            database="test",
            table="products"
        )
        logger.info(f"📸 Snapshot '{snapshot.name}' created at {snapshot.created_at}")
        
        # SQLAlchemy-style queries
        logger.info("🔍 SQLAlchemy-style queries:")
        
        # 1. Basic query
        logger.info("1. Get all products:")
        products = client.query(Product).all()
        for product in products:
            logger.info(f"   - {product.name}: ${product.price}")
        
        # 2. Query with filter_by
        logger.info("2. Get electronics products:")
        electronics = client.query(Product).filter_by(category="Electronics").all()
        for product in electronics:
            logger.info(f"   - {product.name}: ${product.price}")
        
        # 3. Query with snapshot
        logger.info("3. Get products from snapshot:")
        snapshot_products = client.query(Product).snapshot("snapshot_products").all()
        for product in snapshot_products:
            logger.info(f"   - {product.name}: ${product.price}")
        
        # 4. Query with snapshot and filter
        logger.info("4. Get electronics from snapshot:")
        snapshot_electronics = (client.query(Product)
                               .snapshot("snapshot_products")
                               .filter_by(category="Electronics")
                               .all())
        for product in snapshot_electronics:
            logger.info(f"   - {product.name}: ${product.price}")
        
        # 5. Query with ordering
        logger.info("5. Get products ordered by price (descending):")
        ordered_products = (client.query(Product)
                           .order_by(desc("price"))
                           .all())
        for product in ordered_products:
            logger.info(f"   - {product.name}: ${product.price}")
        
        # 6. Query with limit
        logger.info("6. Get top 3 most expensive products:")
        top_products = (client.query(Product)
                       .order_by(desc("price"))
                       .limit(3)
                       .all())
        for product in top_products:
            logger.info(f"   - {product.name}: ${product.price}")
        
        # 7. Query with complex filter
        logger.info("7. Get products with price > 50:")
        expensive_products = (client.query(Product)
                             .filter("price > ?", 50.0)
                             .all())
        for product in expensive_products:
            logger.info(f"   - {product.name}: ${product.price}")
        
        # 8. Count query
        logger.info("8. Count total products:")
        total_count = client.query(Product).count()
        logger.info(f"   Total products: {total_count}")
        
        # 9. Count with filter
        logger.info("9. Count electronics products:")
        electronics_count = client.query(Product).filter_by(category="Electronics").count()
        logger.info(f"   Electronics products: {electronics_count}")
        
        # 10. First result
        logger.info("10. Get first product:")
        first_product = client.query(Product).first()
        if first_product:
            logger.info(f"   First product: {first_product.name}")
        
        # Clean up
        try:
            client.execute("DROP SNAPSHOT snapshot_products")
        except:
            pass  # Snapshot might not exist
        client.execute("DROP TABLE orders")
        client.execute("DROP TABLE products")
        
        logger.info("✅ Sync SQLAlchemy-style ORM demo completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Sync demo failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
    finally:
        if client.connected:
            client.disconnect()

async def demo_async_sqlalchemy_style_orm():
    """Demonstrate async SQLAlchemy-style ORM usage"""
    logger.info("🚀 Starting async SQLAlchemy-style ORM demo")
    
    # Get connection parameters from config (supports environment variables)
    host, port, user, password, database = get_connection_params()
    
    try:
        # Create async client
        async with AsyncClient() as client:
            await client.connect(host, port, user, password, database)
            
            logger.info("✅ Connected to MatrixOne (async)")
            
            # Clean up any existing snapshots and tables
            try:
                await client.execute("DROP SNAPSHOT snapshot_async_products")
            except:
                pass  # Snapshot might not exist
            try:
                await client.execute("DROP TABLE async_products")
            except:
                pass  # Table might not exist
            
            # Create tables
            await client.execute("""
                CREATE TABLE IF NOT EXISTS async_products (
                    id INT PRIMARY KEY,
                    name VARCHAR(100),
                    price DECIMAL(10,2),
                    category VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Clear existing data and insert sample data
            await client.execute("DELETE FROM async_products")
            await client.execute("""
                INSERT INTO async_products (id, name, price, category) VALUES
                (1, 'Async Laptop', 999.99, 'Electronics'),
                (2, 'Async Mouse', 29.99, 'Electronics'),
                (3, 'Async Book', 19.99, 'Books')
            """)
            
            logger.info("📊 Sample data inserted (async)")
            
            # Create a snapshot using AsyncSnapshotManager API
            from matrixone.async_client import AsyncSnapshotManager
            async_snapshot_manager = AsyncSnapshotManager(client)
            async_snapshot = await async_snapshot_manager.create(
                name="snapshot_async_products",
                level=SnapshotLevel.TABLE,
                database="test",
                table="async_products"
            )
            logger.info(f"📸 Async Snapshot '{async_snapshot.name}' created at {async_snapshot.created_at}")
            
            # SQLAlchemy-style async queries
            logger.info("🔍 SQLAlchemy-style async queries:")
            
            # Create a model for async products
            class AsyncProduct(Base):
                """Async Product model"""
                __tablename__ = "async_products"
                
                id = Column(Integer, primary_key=True)
                name = Column(String(100))
                price = Column(DECIMAL(10, 2))
                category = Column(String(50))
                created_at = Column(TIMESTAMP)
            
            # 1. Basic async query
            logger.info("1. Get all products (async):")
            products = await client.query(AsyncProduct).all()
            for product in products:
                logger.info(f"   - {product.name}: ${product.price}")
            
            # 2. Async query with snapshot
            logger.info("2. Get products from snapshot (async):")
            snapshot_products = await client.query(AsyncProduct).snapshot("snapshot_async_products").all()
            for product in snapshot_products:
                logger.info(f"   - {product.name}: ${product.price}")
            
            # 3. Async query with filter and ordering
            logger.info("3. Get electronics ordered by price (async):")
            electronics = (await client.query(AsyncProduct)
                          .filter_by(category="Electronics")
                          .order_by(desc("price"))
                          .all())
            for product in electronics:
                logger.info(f"   - {product.name}: ${product.price}")
            
            # 4. Async count query
            logger.info("4. Count total products (async):")
            total_count = await client.query(AsyncProduct).count()
            logger.info(f"   Total products: {total_count}")
            
            # Clean up
            try:
                await client.execute("DROP SNAPSHOT snapshot_async_products")
            except:
                pass  # Snapshot might not exist
            await client.execute("DROP TABLE async_products")
            
            logger.info("✅ Async SQLAlchemy-style ORM demo completed successfully")
            
    except Exception as e:
        logger.error(f"❌ Async demo failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")

def main():
    """Main function"""
    logger.info("🎯 MatrixOne SQLAlchemy-style ORM Demo")
    logger.info("=" * 50)
    
    # Run sync demo
    demo_sync_sqlalchemy_style_orm()
    
    logger.info("\n" + "=" * 50)
    
    # Run async demo
    asyncio.run(demo_async_sqlalchemy_style_orm())
    
    logger.info("\n🎉 All demos completed!")

if __name__ == "__main__":
    main()
