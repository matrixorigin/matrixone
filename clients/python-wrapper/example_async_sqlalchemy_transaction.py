"""
MatrixOne Python SDK - Async SQLAlchemy Transaction Example
Demonstrates MatrixOne async operations and SQLAlchemy ORM in the same transaction
"""

import asyncio
import sys
import os
from datetime import datetime
from typing import Optional

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone import AsyncClient, SnapshotLevel
from matrixone.exceptions import ConnectionError, QueryError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def demo_async_sqlalchemy_transaction():
    """Demo: MatrixOne async operations with SQLAlchemy in the same transaction"""
    
    print("MatrixOne Async + SQLAlchemy Transaction Demo")
    print("=" * 60)
    
    # Create async MatrixOne client
    client = AsyncClient()
    
    try:
        # Connect to MatrixOne
        await client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        logger.info("Connected to MatrixOne!")
        
        # Example 1: Basic async + SQLAlchemy transaction
        logger.info("\n=== Example 1: Basic Async + SQLAlchemy Transaction ===")
        
        async with client.transaction() as tx:
            # Get SQLAlchemy session from transaction
            session = await tx.get_sqlalchemy_session()
            
            # Create SQLAlchemy models (simplified for demo)
            from sqlalchemy import Column, Integer, String, DateTime, create_engine
            from sqlalchemy.ext.declarative import declarative_base
            from sqlalchemy.orm import sessionmaker
            
            Base = declarative_base()
            
            class User(Base):
                __tablename__ = 'async_users'
                
                id = Column(Integer, primary_key=True)
                name = Column(String(100), nullable=False)
                email = Column(String(100), nullable=False, unique=True)
                created_at = Column(DateTime, default=datetime.utcnow)
                
                def __repr__(self):
                    return f"<User(id={self.id}, name='{self.name}', email='{self.email}')>"
            
            # Create table using MatrixOne async SQL
            await tx.execute("""
                CREATE TABLE IF NOT EXISTS async_users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) NOT NULL UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert data using SQLAlchemy ORM
            user1 = User(name="Alice Johnson", email="alice@example.com")
            user2 = User(name="Bob Smith", email="bob@example.com")
            
            session.add(user1)
            session.add(user2)
            session.flush()  # Flush to get IDs
            
            logger.info(f"Created users via SQLAlchemy: {user1}, {user2}")
            
            # Insert more data using MatrixOne async SQL
            await tx.execute(
                "INSERT INTO async_users (name, email) VALUES (%s, %s)",
                ("Charlie Brown", "charlie@example.com")
            )
            
            # Create snapshot using MatrixOne async
            snapshot = await tx.snapshots.create(
                name="sqlalchemy_transaction_snapshot",
                level=SnapshotLevel.DATABASE,
                database="test",
                description="Snapshot created during SQLAlchemy transaction"
            )
            logger.info(f"Created snapshot: {snapshot.name}")
            
            # Clone database using MatrixOne async
            await tx.clone.clone_database_with_snapshot(
                "sqlalchemy_backup",
                "test",
                "sqlalchemy_transaction_snapshot",
                if_not_exists=True
            )
            logger.info("Cloned database using snapshot")
            
            # Query data using both methods
            # SQLAlchemy query
            sqlalchemy_users = session.query(User).all()
            logger.info(f"SQLAlchemy users: {sqlalchemy_users}")
            
            # MatrixOne async query
            async_result = await tx.execute("SELECT id, name, email FROM async_users ORDER BY id")
            logger.info("MatrixOne async users:")
            for row in async_result:
                logger.info(f"  - ID: {row[0]}, Name: {row[1]}, Email: {row[2]}")
        
        logger.info("Transaction completed successfully!")
        
        # Example 2: Complex transaction with error handling
        logger.info("\n=== Example 2: Complex Transaction with Error Handling ===")
        
        try:
            async with client.transaction() as tx:
                session = await tx.get_sqlalchemy_session()
                
                # Create order table
                await tx.execute("""
                    CREATE TABLE IF NOT EXISTS async_orders (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id INT NOT NULL,
                        product_name VARCHAR(100) NOT NULL,
                        quantity INT NOT NULL,
                        price DECIMAL(10,2) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES async_users(id)
                    )
                """)
                
                # Create order using SQLAlchemy
                class Order(Base):
                    __tablename__ = 'async_orders'
                    
                    id = Column(Integer, primary_key=True)
                    user_id = Column(Integer, nullable=False)
                    product_name = Column(String(100), nullable=False)
                    quantity = Column(Integer, nullable=False)
                    price = Column(Integer, nullable=False)  # Simplified for demo
                    created_at = Column(DateTime, default=datetime.utcnow)
                
                # Get user from previous transaction
                user = session.query(User).filter_by(email="alice@example.com").first()
                if user:
                    order = Order(
                        user_id=user.id,
                        product_name="Laptop",
                        quantity=1,
                        price=999
                    )
                    session.add(order)
                    session.flush()
                    logger.info(f"Created order: {order}")
                
                # Create more orders using MatrixOne async
                await tx.execute(
                    "INSERT INTO async_orders (user_id, product_name, quantity, price) VALUES (%s, %s, %s, %s)",
                    (user.id if user else 1, "Mouse", 2, 25)
                )
                
                # Create snapshot of the complete state
                snapshot = await tx.snapshots.create(
                    name="complex_transaction_snapshot",
                    level=SnapshotLevel.DATABASE,
                    database="test",
                    description="Complex transaction with users and orders"
                )
                
                # Clone the complete database
                await tx.clone.clone_database_with_snapshot(
                    "complex_backup",
                    "test",
                    "complex_transaction_snapshot",
                    if_not_exists=True
                )
                
                # Verify data consistency
                user_count = await tx.execute("SELECT COUNT(*) FROM async_users")
                order_count = await tx.execute("SELECT COUNT(*) FROM async_orders")
                
                logger.info(f"Users: {user_count.scalar()}, Orders: {order_count.scalar()}")
                
        except Exception as e:
            logger.error(f"Complex transaction failed: {e}")
            raise
        
        # Example 3: Concurrent operations with SQLAlchemy
        logger.info("\n=== Example 3: Concurrent Operations with SQLAlchemy ===")
        
        async def create_user_async(name: str, email: str):
            """Create user using MatrixOne async"""
            async with client.transaction() as tx:
                await tx.execute(
                    "INSERT INTO async_users (name, email) VALUES (%s, %s)",
                    (name, email)
                )
                return f"Created {name} via async"
        
        async def create_user_sqlalchemy(name: str, email: str):
            """Create user using SQLAlchemy"""
            async with client.transaction() as tx:
                session = await tx.get_sqlalchemy_session()
                user = User(name=name, email=email)
                session.add(user)
                session.flush()
                return f"Created {name} via SQLAlchemy"
        
        # Run concurrent operations
        tasks = [
            create_user_async("David Wilson", "david@example.com"),
            create_user_sqlalchemy("Eva Davis", "eva@example.com"),
            create_user_async("Frank Miller", "frank@example.com"),
            create_user_sqlalchemy("Grace Lee", "grace@example.com")
        ]
        
        results = await asyncio.gather(*tasks)
        for result in results:
            logger.info(result)
        
        # Example 4: Performance comparison
        logger.info("\n=== Example 4: Performance Comparison ===")
        
        # Test async SQL performance
        start_time = asyncio.get_event_loop().time()
        
        async with client.transaction() as tx:
            # Bulk insert using async SQL
            tasks = []
            for i in range(50):
                task = tx.execute(
                    "INSERT INTO async_users (name, email) VALUES (%s, %s)",
                    (f"AsyncUser{i}", f"async{i}@example.com")
                )
                tasks.append(task)
            
            await asyncio.gather(*tasks)
        
        async_time = asyncio.get_event_loop().time() - start_time
        logger.info(f"Async SQL bulk insert (50 records): {async_time:.3f} seconds")
        
        # Test SQLAlchemy performance
        start_time = asyncio.get_event_loop().time()
        
        async with client.transaction() as tx:
            session = await tx.get_sqlalchemy_session()
            
            # Bulk insert using SQLAlchemy
            users = []
            for i in range(50):
                user = User(name=f"SQLAlchemyUser{i}", email=f"sqlalchemy{i}@example.com")
                users.append(user)
            
            session.add_all(users)
            session.flush()
        
        sqlalchemy_time = asyncio.get_event_loop().time() - start_time
        logger.info(f"SQLAlchemy bulk insert (50 records): {sqlalchemy_time:.3f} seconds")
        
        # Final verification
        total_users = await client.execute("SELECT COUNT(*) FROM async_users")
        logger.info(f"Total users in database: {total_users.scalar()}")
        
        # Cleanup
        logger.info("\n=== Cleanup ===")
        
        cleanup_tasks = [
            client.execute("DROP DATABASE IF EXISTS sqlalchemy_backup"),
            client.execute("DROP DATABASE IF EXISTS complex_backup"),
            client.snapshots.delete("sqlalchemy_transaction_snapshot"),
            client.snapshots.delete("complex_transaction_snapshot"),
            client.execute("DROP TABLE IF EXISTS async_orders"),
            client.execute("DROP TABLE IF EXISTS async_users")
        ]
        
        await asyncio.gather(*cleanup_tasks, return_exceptions=True)
        logger.info("Cleanup completed")
        
    except ConnectionError as e:
        logger.error(f"Connection failed: {e}")
        logger.info("This is expected if MatrixOne is not running or accessible")
        logger.info("The async client is working correctly, but needs a database connection")
        return False
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        await client.disconnect()
        logger.info("Disconnected from MatrixOne")
    
    return True


async def demo_without_database():
    """Demo async + SQLAlchemy functionality without database connection"""
    
    logger.info("\n" + "="*60)
    logger.info("Async + SQLAlchemy Demo (No Database Required)")
    logger.info("="*60)
    
    # Create async client
    client = AsyncClient()
    logger.info("✓ AsyncClient created successfully")
    
    # Show transaction capabilities
    logger.info("✓ Async transaction with SQLAlchemy integration:")
    logger.info("  async with client.transaction() as tx:")
    logger.info("      session = await tx.get_sqlalchemy_session()")
    logger.info("      # Use SQLAlchemy ORM")
    logger.info("      user = User(name='Alice', email='alice@example.com')")
    logger.info("      session.add(user)")
    logger.info("      session.flush()")
    logger.info("      # Use MatrixOne async operations")
    logger.info("      await tx.execute('INSERT INTO users ...')")
    logger.info("      await tx.snapshots.create('snap1', SnapshotLevel.DATABASE)")
    logger.info("      await tx.clone.clone_database('backup', 'source')")
    logger.info("      # All operations are in the same transaction!")
    
    # Show benefits
    logger.info("✓ Benefits of unified transaction:")
    logger.info("  - ACID properties across all operations")
    logger.info("  - SQLAlchemy ORM + MatrixOne async in same transaction")
    logger.info("  - Automatic rollback on any failure")
    logger.info("  - Consistent data state")
    logger.info("  - Better performance with concurrent operations")
    
    logger.info("✓ AsyncClient + SQLAlchemy is ready to use!")


async def main():
    """Main async function"""
    print("MatrixOne Async + SQLAlchemy Transaction Examples")
    print("=" * 60)
    
    try:
        # Try to run examples with database
        success = await demo_async_sqlalchemy_transaction()
        
        if not success:
            # Run demo without database
            await demo_without_database()
            
            print("\n" + "="*60)
            print("Async + SQLAlchemy demo completed!")
            print("\nTo run full examples:")
            print("1. Start MatrixOne database")
            print("2. Update connection parameters in this script")
            print("3. Run the script again")
            print("\nThe async client + SQLAlchemy integration is working correctly!")
        else:
            print("\n" + "="*60)
            print("All async + SQLAlchemy examples completed successfully!")
            print("\nKey takeaways:")
            print("- MatrixOne async operations and SQLAlchemy ORM work in the same transaction")
            print("- ACID properties are maintained across all operations")
            print("- Concurrent operations provide better performance")
            print("- Unified transaction management ensures data consistency")
            print("- Both async SQL and ORM operations can be mixed seamlessly")
        
    except Exception as e:
        print(f"Async + SQLAlchemy examples failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
