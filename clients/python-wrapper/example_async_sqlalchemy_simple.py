"""
MatrixOne Python SDK - Simple Async SQLAlchemy Transaction Example
Demonstrates MatrixOne async operations and SQLAlchemy ORM in the same transaction
"""

import asyncio
import sys
import os
from datetime import datetime

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'matrixone'))

from matrixone import AsyncClient, SnapshotLevel
from matrixone.exceptions import ConnectionError
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def simple_async_sqlalchemy_example():
    """Simple example of MatrixOne async + SQLAlchemy in same transaction"""
    
    print("MatrixOne Async + SQLAlchemy Simple Example")
    print("=" * 50)
    
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
        
        # Single transaction with both MatrixOne async and SQLAlchemy
        async with client.transaction() as tx:
            logger.info("Starting unified transaction...")
            
            # Get SQLAlchemy session from transaction
            session = await tx.get_sqlalchemy_session()
            
            # Create table using MatrixOne async SQL
            await tx.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) NOT NULL UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert data using async SQLAlchemy ORM
            from sqlalchemy import Column, Integer, String, DateTime
            from sqlalchemy.ext.asyncio import AsyncSession
            from sqlalchemy.ext.declarative import declarative_base
            
            Base = declarative_base()
            
            class User(Base):
                __tablename__ = 'users'
                
                id = Column(Integer, primary_key=True)
                name = Column(String(100), nullable=False)
                email = Column(String(100), nullable=False, unique=True)
                created_at = Column(DateTime, default=datetime.utcnow)
                
                def __repr__(self):
                    return f"<User(id={self.id}, name='{self.name}', email='{self.email}')>"
            
            # Create user using async SQLAlchemy
            user1 = User(name="Alice Johnson", email="alice@example.com")
            user2 = User(name="Bob Smith", email="bob@example.com")
            
            session.add(user1)
            session.add(user2)
            await session.flush()  # Async flush to get IDs
            
            logger.info(f"Created users via async SQLAlchemy: {user1}, {user2}")
            
            # Insert more data using MatrixOne async SQL
            await tx.execute(
                "INSERT INTO users (name, email) VALUES (%s, %s)",
                ("Charlie Brown", "charlie@example.com")
            )
            
            # Create snapshot using MatrixOne async
            snapshot = await tx.snapshots.create(
                name="unified_transaction_snapshot",
                level=SnapshotLevel.DATABASE,
                database="test",
                description="Snapshot from unified transaction"
            )
            logger.info(f"Created snapshot: {snapshot.name}")
            
            # Clone database using MatrixOne async
            await tx.clone.clone_database_with_snapshot(
                "unified_backup",
                "test",
                "unified_transaction_snapshot",
                if_not_exists=True
            )
            logger.info("Cloned database using snapshot")
            
            # Query data using both methods in the same transaction
            # Async SQLAlchemy query
            from sqlalchemy import select
            result = await session.execute(select(User))
            sqlalchemy_users = result.scalars().all()
            logger.info(f"Async SQLAlchemy users: {sqlalchemy_users}")
            
            # MatrixOne async query
            async_result = await tx.execute("SELECT id, name, email FROM users ORDER BY id")
            logger.info("MatrixOne async users:")
            for row in async_result:
                logger.info(f"  - ID: {row[0]}, Name: {row[1]}, Email: {row[2]}")
            
            # Verify data consistency
            total_count = await tx.execute("SELECT COUNT(*) FROM users")
            logger.info(f"Total users: {total_count.scalar()}")
        
        logger.info("Unified transaction completed successfully!")
        
        # Cleanup
        logger.info("\n=== Cleanup ===")
        await client.execute("DROP DATABASE IF EXISTS unified_backup")
        await client.snapshots.delete("unified_transaction_snapshot")
        await client.execute("DROP TABLE IF EXISTS users")
        logger.info("Cleanup completed")
        
    except ConnectionError as e:
        logger.error(f"Connection failed: {e}")
        logger.info("This is expected if MatrixOne is not running or accessible")
        return False
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        await client.disconnect()
        logger.info("Disconnected from MatrixOne")
    
    return True


async def demo_transaction_benefits():
    """Demo the benefits of unified transaction"""
    
    logger.info("\n" + "="*50)
    logger.info("Unified Transaction Benefits")
    logger.info("="*50)
    
    logger.info("✓ ACID Properties:")
    logger.info("  - Atomicity: All operations succeed or all fail")
    logger.info("  - Consistency: Data remains in valid state")
    logger.info("  - Isolation: Concurrent transactions don't interfere")
    logger.info("  - Durability: Committed changes are permanent")
    
    logger.info("\n✓ Unified Operations:")
    logger.info("  - SQLAlchemy ORM operations")
    logger.info("  - MatrixOne async SQL operations")
    logger.info("  - Snapshot creation and management")
    logger.info("  - Database and table cloning")
    logger.info("  - All in the same transaction!")
    
    logger.info("\n✓ Performance Benefits:")
    logger.info("  - Single transaction reduces overhead")
    logger.info("  - Concurrent operations within transaction")
    logger.info("  - Better resource utilization")
    logger.info("  - Reduced network round trips")
    
    logger.info("\n✓ Error Handling:")
    logger.info("  - Automatic rollback on any failure")
    logger.info("  - Consistent error handling")
    logger.info("  - Data integrity maintained")
    logger.info("  - No partial state corruption")


async def demo_without_database():
    """Demo without database connection"""
    
    logger.info("\n" + "="*50)
    logger.info("Async + SQLAlchemy Demo (No Database Required)")
    logger.info("="*50)
    
    client = AsyncClient()
    logger.info("✓ AsyncClient created successfully")
    
    logger.info("✓ Unified async transaction syntax:")
    logger.info("  async with client.transaction() as tx:")
    logger.info("      # Get async SQLAlchemy session")
    logger.info("      session = await tx.get_sqlalchemy_session()")
    logger.info("      ")
    logger.info("      # Async SQLAlchemy ORM operations")
    logger.info("      user = User(name='Alice', email='alice@example.com')")
    logger.info("      session.add(user)")
    logger.info("      await session.flush()  # Async flush!")
    logger.info("      ")
    logger.info("      # Async SQLAlchemy queries")
    logger.info("      result = await session.execute(select(User))")
    logger.info("      users = result.scalars().all()")
    logger.info("      ")
    logger.info("      # MatrixOne async operations")
    logger.info("      await tx.execute('INSERT INTO users ...')")
    logger.info("      await tx.snapshots.create('snap1', SnapshotLevel.DATABASE)")
    logger.info("      await tx.clone.clone_database('backup', 'source')")
    logger.info("      ")
    logger.info("      # All operations are truly async and atomic!")
    
    await demo_transaction_benefits()


async def main():
    """Main function"""
    print("MatrixOne Async + SQLAlchemy Transaction Example")
    print("=" * 50)
    
    try:
        # Try to run with database
        success = await simple_async_sqlalchemy_example()
        
        if not success:
            # Run demo without database
            await demo_without_database()
            
            print("\n" + "="*50)
            print("Demo completed!")
            print("\nTo run with database:")
            print("1. Start MatrixOne database")
            print("2. Update connection parameters")
            print("3. Run the script again")
        else:
            print("\n" + "="*50)
            print("✅ All examples completed successfully!")
            print("\n🎉 MatrixOne async + SQLAlchemy integration works perfectly!")
            print("\nKey benefits:")
            print("- Unified transaction management")
            print("- ACID properties across all operations")
            print("- SQLAlchemy ORM + MatrixOne async in same transaction")
            print("- Automatic rollback on failures")
            print("- Better performance and consistency")
        
    except Exception as e:
        print(f"❌ Examples failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
