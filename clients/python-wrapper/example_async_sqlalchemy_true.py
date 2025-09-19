"""
MatrixOne Python SDK - True Async SQLAlchemy Integration Example
Demonstrates MatrixOne async operations with truly async SQLAlchemy ORM in the same transaction
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


async def true_async_sqlalchemy_example():
    """Example of MatrixOne async + truly async SQLAlchemy in same transaction"""
    
    print("MatrixOne + True Async SQLAlchemy Integration Example")
    print("=" * 60)
    
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
        
        # Single transaction with both MatrixOne async and truly async SQLAlchemy
        async with client.transaction() as tx:
            logger.info("Starting unified async transaction...")
            
            # Get truly async SQLAlchemy session from transaction
            session = await tx.get_sqlalchemy_session()
            
            # Create table using MatrixOne async SQL
            await tx.execute("""
                CREATE TABLE IF NOT EXISTS async_users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100) NOT NULL UNIQUE,
                    status VARCHAR(20) DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert data using truly async SQLAlchemy ORM
            from sqlalchemy import Column, Integer, String, DateTime
            from sqlalchemy.ext.asyncio import AsyncSession
            from sqlalchemy.ext.declarative import declarative_base
            
            Base = declarative_base()
            
            class User(Base):
                __tablename__ = 'async_users'
                
                id = Column(Integer, primary_key=True)
                name = Column(String(100), nullable=False)
                email = Column(String(100), nullable=False, unique=True)
                status = Column(String(20), default='active')
                created_at = Column(DateTime, default=datetime.utcnow)
                
                def __repr__(self):
                    return f"<User(id={self.id}, name='{self.name}', email='{self.email}', status='{self.status}')>"
            
            # Create users using truly async SQLAlchemy
            user1 = User(name="Alice Johnson", email="alice@example.com")
            user2 = User(name="Bob Smith", email="bob@example.com")
            
            session.add(user1)
            session.add(user2)
            await session.flush()  # Truly async flush!
            
            logger.info(f"Created users via truly async SQLAlchemy: {user1}, {user2}")
            
            # Insert more data using MatrixOne async SQL
            await tx.execute(
                "INSERT INTO async_users (name, email) VALUES (%s, %s)",
                ("Charlie Brown", "charlie@example.com")
            )
            
            # Create snapshot using MatrixOne async
            snapshot = await tx.snapshots.create(
                name="true_async_transaction_snapshot",
                level=SnapshotLevel.DATABASE,
                database="test",
                description="Snapshot from truly async transaction"
            )
            logger.info(f"Created snapshot: {snapshot.name}")
            
            # Clone database using MatrixOne async
            await tx.clone.clone_database_with_snapshot(
                "true_async_backup",
                "test",
                "true_async_transaction_snapshot",
                if_not_exists=True
            )
            logger.info("Cloned database using snapshot")
            
            # Query data using both methods in the same transaction
            # Truly async SQLAlchemy query
            from sqlalchemy import select
            result = await session.execute(select(User))
            sqlalchemy_users = result.scalars().all()
            logger.info(f"Truly async SQLAlchemy users: {sqlalchemy_users}")
            
            # MatrixOne async query
            async_result = await tx.execute("SELECT id, name, email, status FROM async_users ORDER BY id")
            logger.info("MatrixOne async users:")
            for row in async_result:
                logger.info(f"  - ID: {row[0]}, Name: {row[1]}, Email: {row[2]}, Status: {row[3]}")
            
            # Update data using truly async SQLAlchemy
            user1.status = 'updated'
            session.add(user1)
            await session.flush()  # Truly async flush!
            logger.info(f"Updated user via truly async SQLAlchemy: {user1}")
            
            # Update data using MatrixOne async SQL
            await tx.execute(
                "UPDATE async_users SET status = %s WHERE email = %s",
                ("updated", "bob@example.com")
            )
            logger.info("Updated user via MatrixOne async SQL")
            
            # Verify data consistency
            total_count = await tx.execute("SELECT COUNT(*) FROM async_users")
            updated_count = await tx.execute("SELECT COUNT(*) FROM async_users WHERE status = 'updated'")
            logger.info(f"Total users: {total_count.scalar()}, Updated users: {updated_count.scalar()}")
        
        logger.info("Truly async unified transaction completed successfully!")
        
        # Cleanup
        logger.info("\n=== Cleanup ===")
        await client.execute("DROP DATABASE IF EXISTS true_async_backup")
        await client.snapshots.delete("true_async_transaction_snapshot")
        await client.execute("DROP TABLE IF EXISTS async_users")
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


async def demo_true_async_benefits():
    """Demo the benefits of truly async SQLAlchemy integration"""
    
    logger.info("\n" + "="*60)
    logger.info("True Async SQLAlchemy Benefits")
    logger.info("="*60)
    
    logger.info("✓ Truly Async Operations:")
    logger.info("  - await session.flush() - Async flush operations")
    logger.info("  - await session.execute() - Async query execution")
    logger.info("  - await session.commit() - Async commit operations")
    logger.info("  - await session.rollback() - Async rollback operations")
    logger.info("  - await session.close() - Async session cleanup")
    
    logger.info("\n✓ Performance Benefits:")
    logger.info("  - Non-blocking I/O for all SQLAlchemy operations")
    logger.info("  - True concurrency with MatrixOne async operations")
    logger.info("  - Better resource utilization")
    logger.info("  - Reduced latency for database operations")
    
    logger.info("\n✓ Integration Benefits:")
    logger.info("  - Seamless mixing of async SQLAlchemy and MatrixOne async")
    logger.info("  - Single transaction for all operations")
    logger.info("  - Consistent async/await pattern throughout")
    logger.info("  - Better error handling and rollback")
    
    logger.info("\n✓ Modern Python Benefits:")
    logger.info("  - Full asyncio compatibility")
    logger.info("  - Modern async/await syntax")
    logger.info("  - Better integration with async frameworks")
    logger.info("  - Future-proof architecture")


async def demo_without_database():
    """Demo without database connection"""
    
    logger.info("\n" + "="*60)
    logger.info("True Async SQLAlchemy Demo (No Database Required)")
    logger.info("="*60)
    
    client = AsyncClient()
    logger.info("✓ AsyncClient created successfully")
    
    logger.info("✓ Truly async transaction syntax:")
    logger.info("  async with client.transaction() as tx:")
    logger.info("      # Get truly async SQLAlchemy session")
    logger.info("      session = await tx.get_sqlalchemy_session()")
    logger.info("      ")
    logger.info("      # Truly async SQLAlchemy ORM operations")
    logger.info("      user = User(name='Alice', email='alice@example.com')")
    logger.info("      session.add(user)")
    logger.info("      await session.flush()  # Truly async!")
    logger.info("      ")
    logger.info("      # Truly async SQLAlchemy queries")
    logger.info("      result = await session.execute(select(User))")
    logger.info("      users = result.scalars().all()")
    logger.info("      ")
    logger.info("      # Truly async SQLAlchemy updates")
    logger.info("      user.status = 'updated'")
    logger.info("      session.add(user)")
    logger.info("      await session.flush()  # Truly async!")
    logger.info("      ")
    logger.info("      # MatrixOne async operations")
    logger.info("      await tx.execute('INSERT INTO users ...')")
    logger.info("      await tx.snapshots.create('snap1', SnapshotLevel.DATABASE)")
    logger.info("      await tx.clone.clone_database('backup', 'source')")
    logger.info("      ")
    logger.info("      # All operations are truly async and atomic!")
    
    await demo_true_async_benefits()


async def main():
    """Main function"""
    print("MatrixOne + True Async SQLAlchemy Integration Example")
    print("=" * 60)
    
    try:
        # Try to run with database
        success = await true_async_sqlalchemy_example()
        
        if not success:
            # Run demo without database
            await demo_without_database()
            
            print("\n" + "="*60)
            print("Demo completed!")
            print("\nTo run with database:")
            print("1. Start MatrixOne database")
            print("2. Update connection parameters")
            print("3. Run the script again")
        else:
            print("\n" + "="*60)
            print("✅ All truly async examples completed successfully!")
            print("\n🎉 MatrixOne + truly async SQLAlchemy integration works perfectly!")
            print("\nKey benefits:")
            print("- Truly async SQLAlchemy operations (await session.flush(), etc.)")
            print("- Unified transaction management")
            print("- ACID properties across all operations")
            print("- Seamless mixing of async SQLAlchemy and MatrixOne async")
            print("- Better performance and consistency")
            print("- Modern async/await pattern throughout")
        
    except Exception as e:
        print(f"❌ Examples failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
