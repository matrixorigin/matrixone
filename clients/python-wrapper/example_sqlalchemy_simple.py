"""
MatrixOne Python SDK - Simple SQLAlchemy Transaction Example
A simplified example showing SQLAlchemy transactions with MatrixOne snapshots and clones
"""

from matrixone import Client
from matrixone.snapshot import SnapshotLevel
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime

# SQLAlchemy setup
Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    email = Column(String(100), nullable=False, unique=True)
    created_at = Column(DateTime, default=datetime.now)
    
    def __repr__(self):
        return f"<User(id={self.id}, name='{self.name}', email='{self.email}')>"


def simple_sqlalchemy_example():
    """Simple SQLAlchemy transaction with MatrixOne snapshots"""
    
    # Initialize clients
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
        print("✓ Connected to MatrixOne")
        
        # Create SQLAlchemy engine
        engine = create_engine(
            f"mysql+pymysql://root:111@localhost:6001/test",
            echo=False  # Set to True to see SQL queries
        )
        
        # Create session factory
        Session = sessionmaker(bind=engine)
        
        # Create tables
        Base.metadata.create_all(engine)
        print("✓ Database tables created")
        
        # Example 1: Basic SQLAlchemy transaction
        print("\n=== Example 1: Basic SQLAlchemy Transaction ===")
        
        session = Session()
        try:
            # Insert users using SQLAlchemy ORM
            users = [
                User(name="Alice Johnson", email="alice@example.com"),
                User(name="Bob Smith", email="bob@example.com"),
                User(name="Charlie Brown", email="charlie@example.com")
            ]
            
            session.add_all(users)
            session.commit()
            print(f"✓ Inserted {len(users)} users using SQLAlchemy")
            
        except Exception as e:
            session.rollback()
            print(f"✗ SQLAlchemy transaction failed: {e}")
            raise
        finally:
            session.close()
        
        # Create snapshot after SQLAlchemy operations
        snapshot = client.snapshots.create(
            name="sqlalchemy_users_snapshot",
            level=SnapshotLevel.DATABASE,
            database="test",
            description="Snapshot after SQLAlchemy user insertion"
        )
        print(f"✓ Created snapshot: {snapshot.name}")
        
        # Example 2: SQLAlchemy within MatrixOne transaction
        print("\n=== Example 2: SQLAlchemy within MatrixOne Transaction ===")
        
        with client.transaction() as tx:
            # Create snapshot in transaction
            tx_snapshot = tx.snapshots.create(
                name="transaction_snapshot",
                level=SnapshotLevel.DATABASE,
                database="test"
            )
            print(f"✓ Created snapshot in transaction: {tx_snapshot.name}")
            
            # Use SQLAlchemy within the transaction
            session = Session()
            try:
                # Add more users
                new_users = [
                    User(name="Diana Prince", email="diana@example.com"),
                    User(name="Eve Wilson", email="eve@example.com")
                ]
                
                session.add_all(new_users)
                session.commit()
                print(f"✓ Added {len(new_users)} users in transaction")
                
            except Exception as e:
                session.rollback()
                print(f"✗ SQLAlchemy operation failed: {e}")
                raise
            finally:
                session.close()
            
            # Clone database using the snapshot
            tx.clone.clone_database_with_snapshot(
                "backup_test",
                "test",
                "transaction_snapshot",
                if_not_exists=True
            )
            print("✓ Cloned database within transaction")
        
        print("✓ MatrixOne transaction completed")
        
        # Example 3: Error handling and rollback
        print("\n=== Example 3: Error Handling and Rollback ===")
        
        try:
            with client.transaction() as tx:
                # Create snapshot
                error_snapshot = tx.snapshots.create(
                    name="error_test_snapshot",
                    level=SnapshotLevel.DATABASE,
                    database="test"
                )
                print(f"✓ Created snapshot: {error_snapshot.name}")
                
                # SQLAlchemy operation that will fail
                session = Session()
                try:
                    # Try to insert duplicate email (will fail)
                    duplicate_user = User(name="Duplicate", email="alice@example.com")
                    session.add(duplicate_user)
                    session.commit()
                    
                except Exception as e:
                    session.rollback()
                    print(f"✓ SQLAlchemy error caught (expected): {e}")
                    raise
                finally:
                    session.close()
                
                # This should not be reached
                tx.clone.clone_database("should_not_exist", "test")
                
        except Exception as e:
            print(f"✓ Transaction failed as expected: {e}")
        
        # Verify rollback
        try:
            client.snapshots.get("error_test_snapshot")
            print("✗ ERROR: Snapshot should not exist after rollback!")
        except Exception:
            print("✓ Snapshot correctly rolled back")
        
        # Example 4: Data verification
        print("\n=== Example 4: Data Verification ===")
        
        # Check original database
        session = Session()
        try:
            user_count = session.query(User).count()
            print(f"✓ Original database has {user_count} users")
            
            # List all users
            users = session.query(User).all()
            for user in users:
                print(f"  - {user}")
                
        finally:
            session.close()
        
        # Check backup database
        backup_count = client.execute("SELECT COUNT(*) FROM backup_test.users").scalar()
        print(f"✓ Backup database has {backup_count} users")
        
        # Example 5: Cleanup
        print("\n=== Example 5: Cleanup ===")
        
        # Drop backup database
        client.execute("DROP DATABASE IF EXISTS backup_test")
        print("✓ Dropped backup database")
        
        # Delete snapshots
        snapshots_to_delete = [
            "sqlalchemy_users_snapshot",
            "transaction_snapshot"
        ]
        
        for snapshot_name in snapshots_to_delete:
            try:
                client.snapshots.delete(snapshot_name)
                print(f"✓ Deleted snapshot: {snapshot_name}")
            except Exception as e:
                print(f"⚠ Could not delete snapshot {snapshot_name}: {e}")
        
        # Drop tables
        Base.metadata.drop_all(engine)
        print("✓ Dropped database tables")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        raise
    finally:
        client.disconnect()
        print("✓ Disconnected from MatrixOne")


def sqlalchemy_best_practices():
    """Demonstrate SQLAlchemy best practices with MatrixOne"""
    
    print("\n" + "="*60)
    print("SQLAlchemy Best Practices with MatrixOne")
    print("="*60)
    
    client = Client()
    
    try:
        client.connect(
            host="localhost",
            port=6001,
            user="root",
            password="111",
            database="test"
        )
        
        # Best Practice 1: Engine configuration
        print("\n--- Best Practice 1: Engine Configuration ---")
        
        engine = create_engine(
            f"mysql+pymysql://root:111@localhost:6001/test",
            echo=False,  # Set to True for development
            pool_pre_ping=True,  # Verify connections before use
            pool_recycle=300,    # Recycle connections every 5 minutes
            pool_size=5,         # Number of connections to maintain
            max_overflow=10,     # Additional connections when needed
            isolation_level="READ_COMMITTED"  # Transaction isolation
        )
        
        Session = sessionmaker(bind=engine)
        print("✓ Engine configured with best practices")
        
        # Best Practice 2: Context managers
        print("\n--- Best Practice 2: Context Managers ---")
        
        # Create tables
        Base.metadata.create_all(engine)
        
        # Use context manager for session
        with Session() as session:
            try:
                # Create snapshot
                snapshot = client.snapshots.create(
                    name="best_practice_snapshot",
                    level=SnapshotLevel.DATABASE,
                    database="test"
                )
                
                # SQLAlchemy operations
                user = User(name="Best Practice User", email="best@example.com")
                session.add(user)
                session.commit()
                
                print("✓ Used context manager for session")
                
            except Exception as e:
                session.rollback()
                raise
        
        # Best Practice 3: Batch operations
        print("\n--- Best Practice 3: Batch Operations ---")
        
        with Session() as session:
            try:
                # Batch insert
                users_data = [
                    {"name": f"Batch User {i}", "email": f"batch{i}@example.com"}
                    for i in range(1, 11)  # 10 users
                ]
                
                session.bulk_insert_mappings(User, users_data)
                session.commit()
                
                print(f"✓ Batch inserted {len(users_data)} users")
                
            except Exception as e:
                session.rollback()
                raise
        
        # Best Practice 4: Error handling
        print("\n--- Best Practice 4: Error Handling ---")
        
        def safe_operation():
            """Demonstrate safe operation with proper error handling"""
            session = Session()
            try:
                with client.transaction() as tx:
                    # Create snapshot
                    tx.snapshots.create(
                        name="safe_operation_snapshot",
                        level=SnapshotLevel.DATABASE,
                        database="test"
                    )
                    
                    # SQLAlchemy operations
                    session.begin()
                    
                    # Try to insert duplicate email
                    duplicate_user = User(name="Duplicate", email="best@example.com")
                    session.add(duplicate_user)
                    session.commit()
                    
            except Exception as e:
                session.rollback()
                print(f"✓ Caught and handled error: {e}")
                return False
            finally:
                session.close()
            return True
        
        success = safe_operation()
        print(f"✓ Safe operation completed: {success}")
        
        # Best Practice 5: Connection pooling
        print("\n--- Best Practice 5: Connection Pooling ---")
        
        # Multiple sessions using the same engine
        sessions = [Session() for _ in range(3)]
        
        try:
            for i, session in enumerate(sessions):
                with client.transaction() as tx:
                    # Create snapshot for each session
                    tx.snapshots.create(
                        name=f"pool_snapshot_{i}",
                        level=SnapshotLevel.DATABASE,
                        database="test"
                    )
                    
                    # SQLAlchemy operations
                    user = User(name=f"Pool User {i}", email=f"pool{i}@example.com")
                    session.add(user)
                    session.commit()
                    
                    print(f"✓ Session {i} completed successfully")
        
        finally:
            for session in sessions:
                session.close()
        
        # Cleanup
        print("\n--- Cleanup ---")
        
        # Delete snapshots
        snapshots_to_delete = [
            "best_practice_snapshot",
            "safe_operation_snapshot",
            "pool_snapshot_0", "pool_snapshot_1", "pool_snapshot_2"
        ]
        
        for snapshot_name in snapshots_to_delete:
            try:
                client.snapshots.delete(snapshot_name)
                print(f"✓ Deleted snapshot: {snapshot_name}")
            except Exception as e:
                print(f"⚠ Could not delete snapshot {snapshot_name}: {e}")
        
        # Drop tables
        Base.metadata.drop_all(engine)
        print("✓ Cleanup completed")
        
    except Exception as e:
        print(f"✗ Error in best practices: {e}")
        raise
    finally:
        client.disconnect()


if __name__ == "__main__":
    print("MatrixOne SQLAlchemy Transaction Examples")
    print("="*50)
    
    try:
        simple_sqlalchemy_example()
        sqlalchemy_best_practices()
        
        print("\n" + "="*50)
        print("All examples completed successfully!")
        print("\nKey takeaways:")
        print("- SQLAlchemy ORM works seamlessly with MatrixOne transactions")
        print("- Use context managers for proper resource management")
        print("- Snapshots and clones can be created within transaction contexts")
        print("- Proper error handling ensures clean rollbacks")
        print("- Connection pooling improves performance")
        
    except Exception as e:
        print(f"Examples failed: {e}")
        raise
