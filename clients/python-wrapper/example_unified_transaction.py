"""
MatrixOne Python SDK - Unified Transaction Example
Demonstrates how SQLAlchemy and MatrixOne operations work in the same transaction
"""

from matrixone import Client
from matrixone.snapshot import SnapshotLevel
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQLAlchemy base class
Base = declarative_base()

# Define models
class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.now)
    
    def __repr__(self):
        return f"<User(id={self.id}, name='{self.name}', email='{self.email}')>"


def unified_transaction_example():
    """Example of unified transaction with SQLAlchemy and MatrixOne"""
    
    # Create MatrixOne client
    client = Client()
    
    try:
        # Connect to MatrixOne
        client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        logger.info("Connected to MatrixOne!")
        
        # Create tables using MatrixOne client
        client.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) NOT NULL UNIQUE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        logger.info("Created users table")
        
        # Example 1: Unified transaction with SQLAlchemy and MatrixOne
        logger.info("\n=== Example 1: Unified Transaction ===")
        
        with client.transaction() as tx:
            # Get SQLAlchemy session within the transaction
            session = tx.get_sqlalchemy_session()
            
            try:
                # SQLAlchemy operations
                user1 = User(name="Alice Johnson", email="alice@example.com")
                user2 = User(name="Bob Smith", email="bob@example.com")
                session.add_all([user1, user2])
                session.flush()  # Flush to get IDs
                
                logger.info("Added users via SQLAlchemy")
                
                # MatrixOne snapshot operation in the same transaction
                snapshot = tx.snapshots.create(
                    name="unified_snapshot",
                    level=SnapshotLevel.DATABASE,
                    database="test",
                    description="Snapshot after SQLAlchemy operations"
                )
                logger.info(f"Created snapshot: {snapshot.name}")
                
                # MatrixOne clone operation in the same transaction
                tx.clone.clone_database_with_snapshot(
                    "backup_test",
                    "test",
                    "unified_snapshot",
                    if_not_exists=True
                )
                logger.info("Cloned database within transaction")
                
                # More SQLAlchemy operations
                user3 = User(name="Charlie Brown", email="charlie@example.com")
                session.add(user3)
                session.commit()  # Commit SQLAlchemy session
                
                logger.info("Committed SQLAlchemy operations")
                
            except Exception as e:
                session.rollback()
                logger.error(f"SQLAlchemy operation failed: {e}")
                raise
        
        logger.info("✓ Unified transaction completed successfully")
        
        # Example 2: Error handling and rollback
        logger.info("\n=== Example 2: Error Handling and Rollback ===")
        
        try:
            with client.transaction() as tx:
                # Get SQLAlchemy session
                session = tx.get_sqlalchemy_session()
                
                try:
                    # SQLAlchemy operations
                    user4 = User(name="Diana Prince", email="diana@example.com")
                    session.add(user4)
                    session.flush()
                    
                    # Create snapshot
                    error_snapshot = tx.snapshots.create(
                        name="error_test_snapshot",
                        level=SnapshotLevel.DATABASE,
                        database="test"
                    )
                    logger.info(f"Created snapshot: {error_snapshot.name}")
                    
                    # This will cause an error (duplicate email)
                    duplicate_user = User(name="Duplicate", email="alice@example.com")
                    session.add(duplicate_user)
                    session.commit()
                    
                except Exception as e:
                    session.rollback()
                    logger.error(f"SQLAlchemy error (expected): {e}")
                    raise
                
                # This should not be reached
                tx.clone.clone_database("should_not_exist", "test")
                
        except Exception as e:
            logger.info(f"Transaction failed as expected: {e}")
        
        # Verify rollback
        try:
            client.snapshots.get("error_test_snapshot")
            logger.error("ERROR: Snapshot should not exist after rollback!")
        except Exception:
            logger.info("✓ Snapshot correctly rolled back")
        
        # Example 3: Data verification
        logger.info("\n=== Example 3: Data Verification ===")
        
        # Check original database
        result = client.execute("SELECT COUNT(*) FROM users")
        user_count = result.scalar()
        logger.info(f"Original database has {user_count} users")
        
        # Check backup database
        backup_count = client.execute("SELECT COUNT(*) FROM backup_test.users").scalar()
        logger.info(f"Backup database has {backup_count} users")
        
        # List users in original database
        users_result = client.execute("SELECT id, name, email FROM users ORDER BY id")
        logger.info("Users in original database:")
        for row in users_result:
            logger.info(f"  - ID: {row[0]}, Name: {row[1]}, Email: {row[2]}")
        
        # List users in backup database
        backup_users_result = client.execute("SELECT id, name, email FROM backup_test.users ORDER BY id")
        logger.info("Users in backup database:")
        for row in backup_users_result:
            logger.info(f"  - ID: {row[0]}, Name: {row[1]}, Email: {row[2]}")
        
        # Example 4: Complex operations
        logger.info("\n=== Example 4: Complex Operations ===")
        
        with client.transaction() as tx:
            # Get SQLAlchemy session
            session = tx.get_sqlalchemy_session()
            
            try:
                # Update existing user via SQLAlchemy
                user_to_update = session.query(User).filter_by(email="alice@example.com").first()
                if user_to_update:
                    user_to_update.name = "Alice Johnson-Updated"
                    session.add(user_to_update)
                
                # Add new user
                new_user = User(name="Eve Wilson", email="eve@example.com")
                session.add(new_user)
                session.flush()
                
                # Create snapshot after updates
                update_snapshot = tx.snapshots.create(
                    name="update_snapshot",
                    level=SnapshotLevel.DATABASE,
                    database="test",
                    description="Snapshot after user updates"
                )
                logger.info(f"Created update snapshot: {update_snapshot.name}")
                
                # Clone to another backup
                tx.clone.clone_database_with_snapshot(
                    "backup_test_2",
                    "test",
                    "update_snapshot",
                    if_not_exists=True
                )
                logger.info("Created second backup database")
                
                # Commit SQLAlchemy changes
                session.commit()
                
            except Exception as e:
                session.rollback()
                raise
        
        logger.info("✓ Complex operations completed")
        
        # Example 5: Cleanup
        logger.info("\n=== Example 5: Cleanup ===")
        
        # Drop backup databases
        client.execute("DROP DATABASE IF EXISTS backup_test")
        client.execute("DROP DATABASE IF EXISTS backup_test_2")
        logger.info("Dropped backup databases")
        
        # Delete snapshots
        snapshots_to_delete = [
            "unified_snapshot",
            "update_snapshot"
        ]
        
        for snapshot_name in snapshots_to_delete:
            try:
                client.snapshots.delete(snapshot_name)
                logger.info(f"Deleted snapshot: {snapshot_name}")
            except Exception as e:
                logger.warning(f"Could not delete snapshot {snapshot_name}: {e}")
        
        # Drop original table
        client.execute("DROP TABLE IF EXISTS users")
        logger.info("Dropped users table")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        client.disconnect()
        logger.info("Disconnected from MatrixOne")


def advanced_unified_patterns():
    """Advanced patterns for unified transactions"""
    
    logger.info("\n" + "="*60)
    logger.info("Advanced Unified Transaction Patterns")
    logger.info("="*60)
    
    client = Client()
    
    try:
        client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        # Create tables
        client.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        client.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                product_id INT NOT NULL,
                quantity INT NOT NULL,
                total DECIMAL(10,2) NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Pattern 1: Batch operations with snapshots
        logger.info("\n--- Pattern 1: Batch Operations with Snapshots ---")
        
        with client.transaction() as tx:
            session = tx.get_sqlalchemy_session()
            
            try:
                # Batch insert products
                products_data = [
                    {"name": f"Product_{i}", "price": 10.0 + i}
                    for i in range(1, 11)  # 10 products
                ]
                
                session.bulk_insert_mappings(
                    type('Product', (Base,), {
                        '__tablename__': 'products',
                        'id': Column(Integer, primary_key=True, autoincrement=True),
                        'name': Column(String(100), nullable=False),
                        'price': Column(Integer, nullable=False),
                        'created_at': Column(DateTime, default=datetime.now)
                    }),
                    products_data
                )
                
                session.commit()
                logger.info("Batch inserted 10 products")
                
                # Create snapshot
                batch_snapshot = tx.snapshots.create(
                    name="batch_products_snapshot",
                    level=SnapshotLevel.DATABASE,
                    database="test"
                )
                logger.info(f"Created batch snapshot: {batch_snapshot.name}")
                
            except Exception as e:
                session.rollback()
                raise
        
        # Pattern 2: Nested operations
        logger.info("\n--- Pattern 2: Nested Operations ---")
        
        with client.transaction() as tx:
            session = tx.get_sqlalchemy_session()
            
            try:
                # Create initial snapshot
                initial_snapshot = tx.snapshots.create(
                    name="nested_initial_snapshot",
                    level=SnapshotLevel.DATABASE,
                    database="test"
                )
                
                # Add orders
                orders_data = [
                    {"product_id": i, "quantity": i * 2, "total": (10.0 + i) * i * 2}
                    for i in range(1, 6)  # 5 orders
                ]
                
                session.bulk_insert_mappings(
                    type('Order', (Base,), {
                        '__tablename__': 'orders',
                        'id': Column(Integer, primary_key=True, autoincrement=True),
                        'product_id': Column(Integer, nullable=False),
                        'quantity': Column(Integer, nullable=False),
                        'total': Column(Integer, nullable=False),
                        'created_at': Column(DateTime, default=datetime.now)
                    }),
                    orders_data
                )
                
                session.commit()
                
                # Create final snapshot
                final_snapshot = tx.snapshots.create(
                    name="nested_final_snapshot",
                    level=SnapshotLevel.DATABASE,
                    database="test"
                )
                
                # Clone using initial snapshot
                tx.clone.clone_database_with_snapshot(
                    "nested_backup",
                    "test",
                    "nested_initial_snapshot",
                    if_not_exists=True
                )
                
                logger.info("Nested operations completed")
                
            except Exception as e:
                session.rollback()
                raise
        
        # Pattern 3: Data validation
        logger.info("\n--- Pattern 3: Data Validation ---")
        
        # Verify data consistency
        products_count = client.execute("SELECT COUNT(*) FROM products").scalar()
        orders_count = client.execute("SELECT COUNT(*) FROM orders").scalar()
        backup_products_count = client.execute("SELECT COUNT(*) FROM nested_backup.products").scalar()
        
        logger.info(f"Products: {products_count}, Orders: {orders_count}")
        logger.info(f"Backup products: {backup_products_count}")
        
        if backup_products_count == 10:  # Should match initial snapshot
            logger.info("✓ Data validation passed")
        else:
            logger.error("✗ Data validation failed")
        
        # Cleanup
        logger.info("\n--- Cleanup ---")
        
        # Drop databases
        client.execute("DROP DATABASE IF EXISTS nested_backup")
        
        # Delete snapshots
        snapshots_to_delete = [
            "batch_products_snapshot",
            "nested_initial_snapshot",
            "nested_final_snapshot"
        ]
        
        for snapshot_name in snapshots_to_delete:
            try:
                client.snapshots.delete(snapshot_name)
                logger.info(f"Deleted snapshot: {snapshot_name}")
            except Exception as e:
                logger.warning(f"Could not delete snapshot {snapshot_name}: {e}")
        
        # Drop tables
        client.execute("DROP TABLE IF EXISTS orders")
        client.execute("DROP TABLE IF EXISTS products")
        
    except Exception as e:
        logger.error(f"Error in advanced patterns: {e}")
        raise
    finally:
        client.disconnect()


if __name__ == "__main__":
    print("MatrixOne Unified Transaction Examples")
    print("="*50)
    
    try:
        unified_transaction_example()
        advanced_unified_patterns()
        
        print("\n" + "="*50)
        print("All examples completed successfully!")
        print("\nKey takeaways:")
        print("- SQLAlchemy and MatrixOne operations now work in the same transaction")
        print("- Proper error handling ensures both SQLAlchemy and MatrixOne rollback")
        print("- Snapshots and clones can be created within SQLAlchemy transactions")
        print("- Unified transaction management provides ACID guarantees")
        
    except Exception as e:
        print(f"Examples failed: {e}")
        raise
