#!/usr/bin/env python3
"""
MatrixOne SQLAlchemy Integration Examples

This example demonstrates comprehensive SQLAlchemy integration:
1. Basic SQLAlchemy setup and connection
2. ORM model definitions and operations
3. SQLAlchemy with transactions
4. Async SQLAlchemy operations
5. SQLAlchemy with MatrixOne-specific features
6. Performance optimization with SQLAlchemy

This example shows the complete SQLAlchemy integration capabilities.
"""

import logging
import asyncio
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, ForeignKey, text
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.pool import QueuePool
from matrixone import Client, AsyncClient
from matrixone.logger import create_default_logger

# Create MatrixOne logger for all logging
logger = create_default_logger(
    enable_performance_logging=True,
    enable_sql_logging=True
)

# SQLAlchemy setup
Base = declarative_base()

# Define models
class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    created_at = Column(DateTime)
    
    # Relationship
    posts = relationship("Post", back_populates="author")

class Post(Base):
    __tablename__ = 'posts'
    
    id = Column(Integer, primary_key=True)
    title = Column(String(200), nullable=False)
    content = Column(Text)
    author_id = Column(Integer, ForeignKey('users.id'))
    created_at = Column(DateTime)
    
    # Relationship
    author = relationship("User", back_populates="posts")

class Category(Base):
    __tablename__ = 'categories'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    description = Column(Text)


def demo_basic_sqlalchemy_setup():
    """Demonstrate basic SQLAlchemy setup"""
    logger.info("🚀 MatrixOne Basic SQLAlchemy Setup Demo")
    logger.info("=" * 60)
    
    try:
        # Create engine
        logger.info("\n=== Test 1: Create SQLAlchemy Engine ===")
        engine = create_engine(
            'mysql+pymysql://root:111@127.0.0.1:6001/test',
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False  # Set to True for SQL logging
        )
        logger.info("   ✅ SQLAlchemy engine created")
        
        # Create session factory
        SessionLocal = sessionmaker(bind=engine)
        logger.info("   ✅ Session factory created")
        
        # Test connection
        logger.info("\n=== Test 2: Test Connection ===")
        session = SessionLocal()
        try:
            result = session.execute(text("SELECT 1 as test_value, USER() as user_info"))
            row = result.fetchone()
            logger.info(f"   ✅ Connection test successful: {row}")
        finally:
            session.close()
        
        # Create tables
        logger.info("\n=== Test 3: Create Tables ===")
        Base.metadata.create_all(engine)
        logger.info("   ✅ Tables created successfully")
        
        # List tables
        logger.info("\n=== Test 4: List Tables ===")
        result = session.execute(text("SHOW TABLES"))
        tables = [row[0] for row in result.fetchall()]
        logger.info(f"   Tables: {tables}")
        
        session.close()
        
    except Exception as e:
        logger.error(f"❌ Basic SQLAlchemy setup failed: {e}")


def demo_orm_operations():
    """Demonstrate ORM operations"""
    logger.info("\n=== Test 5: ORM Operations ===")
    
    try:
        # Create engine and session
        engine = create_engine('mysql+pymysql://root:111@127.0.0.1:6001/test')
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()
        
        # Create users
        logger.info("\n👥 Create Users")
        users_data = [
            {'username': 'alice', 'email': 'alice@example.com'},
            {'username': 'bob', 'email': 'bob@example.com'},
            {'username': 'charlie', 'email': 'charlie@example.com'}
        ]
        
        created_users = []
        for user_data in users_data:
            # Check if user already exists
            existing_user = session.query(User).filter(User.username == user_data['username']).first()
            if not existing_user:
                user = User(**user_data)
                session.add(user)
                created_users.append(user)
            else:
                created_users.append(existing_user)
        
        session.commit()
        logger.info(f"   ✅ Created {len(created_users)} users")
        
        # Create categories
        logger.info("\n📂 Create Categories")
        categories_data = [
            {'name': 'Technology', 'description': 'Tech-related posts'},
            {'name': 'Science', 'description': 'Science-related posts'},
            {'name': 'Art', 'description': 'Art-related posts'}
        ]
        
        created_categories = []
        for cat_data in categories_data:
            # Check if category already exists
            existing_category = session.query(Category).filter(Category.name == cat_data['name']).first()
            if not existing_category:
                category = Category(**cat_data)
                session.add(category)
                created_categories.append(category)
            else:
                created_categories.append(existing_category)
        
        session.commit()
        logger.info(f"   ✅ Created {len(created_categories)} categories")
        
        # Create posts
        logger.info("\n📝 Create Posts")
        posts_data = [
            {'title': 'Introduction to MatrixOne', 'content': 'MatrixOne is a modern database...', 'author_id': 1},
            {'title': 'SQLAlchemy Best Practices', 'content': 'Here are some best practices...', 'author_id': 2},
            {'title': 'Database Performance Tips', 'content': 'Performance optimization tips...', 'author_id': 1}
        ]
        
        created_posts = []
        for post_data in posts_data:
            # Check if post already exists
            existing_post = session.query(Post).filter(Post.title == post_data['title']).first()
            if not existing_post:
                post = Post(**post_data)
                session.add(post)
                created_posts.append(post)
            else:
                created_posts.append(existing_post)
        
        session.commit()
        logger.info(f"   ✅ Created {len(created_posts)} posts")
        
        # Query operations
        logger.info("\n🔍 Query Operations")
        
        # Get all users
        users = session.query(User).all()
        logger.info(f"   Total users: {len(users)}")
        for user in users:
            logger.info(f"     - {user.username} ({user.email})")
        
        # Get user with posts
        user_with_posts = session.query(User).filter(User.username == 'alice').first()
        if user_with_posts:
            logger.info(f"   User {user_with_posts.username} has {len(user_with_posts.posts)} posts")
            for post in user_with_posts.posts:
                logger.info(f"     - {post.title}")
        
        # Get posts with author
        posts_with_author = session.query(Post).join(User).all()
        logger.info(f"   Posts with authors: {len(posts_with_author)}")
        for post in posts_with_author:
            logger.info(f"     - '{post.title}' by {post.author.username}")
        
        # Update operations
        logger.info("\n✏️ Update Operations")
        user_to_update = session.query(User).filter(User.username == 'alice').first()
        if user_to_update:
            user_to_update.email = 'alice.updated@example.com'
            session.commit()
            logger.info(f"   ✅ Updated user {user_to_update.username}")
        
        # Delete operations
        logger.info("\n🗑️ Delete Operations")
        post_to_delete = session.query(Post).filter(Post.title == 'Database Performance Tips').first()
        if post_to_delete:
            session.delete(post_to_delete)
            session.commit()
            logger.info(f"   ✅ Deleted post: {post_to_delete.title}")
        
        session.close()
        
    except Exception as e:
        logger.error(f"❌ ORM operations failed: {e}")


def demo_sqlalchemy_transactions():
    """Demonstrate SQLAlchemy transactions"""
    logger.info("\n=== Test 6: SQLAlchemy Transactions ===")
    
    try:
        engine = create_engine('mysql+pymysql://root:111@127.0.0.1:6001/test')
        SessionLocal = sessionmaker(bind=engine)
        
        # Test transaction with commit
        logger.info("\n🔄 Transaction with Commit")
        session = SessionLocal()
        try:
            # Check if user already exists
            existing_user = session.query(User).filter(User.username == 'transaction_user').first()
            if existing_user:
                user = existing_user
            else:
                user = User(username='transaction_user', email='transaction@example.com')
                session.add(user)
                session.flush()  # Get the ID
            
            # Check if post already exists
            existing_post = session.query(Post).filter(Post.title == 'Transaction Test Post').first()
            if not existing_post:
                post = Post(title='Transaction Test Post', content='This post was created in a transaction', author_id=user.id)
                session.add(post)
            
            session.commit()
            logger.info("   ✅ Transaction committed successfully")
            
        except Exception as e:
            session.rollback()
            logger.error(f"   ❌ Transaction rolled back: {e}")
        finally:
            session.close()
        
        # Test transaction with rollback
        logger.info("\n🔄 Transaction with Rollback")
        session = SessionLocal()
        try:
            # Create user in transaction
            user = User(username='rollback_user', email='rollback@example.com')
            session.add(user)
            session.flush()
            
            # Create post for the user
            post = Post(title='Rollback Test Post', content='This post should be rolled back', author_id=user.id)
            session.add(post)
            
            # Simulate error
            raise Exception("Simulated transaction error")
            
        except Exception as e:
            session.rollback()
            logger.info(f"   ✅ Transaction rolled back: {e}")
        finally:
            session.close()
        
        # Verify rollback
        session = SessionLocal()
        rollback_user = session.query(User).filter(User.username == 'rollback_user').first()
        if rollback_user:
            logger.error("   ❌ Rollback user should not exist!")
        else:
            logger.info("   ✅ Rollback user correctly removed")
        
        session.close()
        
    except Exception as e:
        logger.error(f"❌ SQLAlchemy transactions failed: {e}")


async def demo_async_sqlalchemy():
    """Demonstrate async SQLAlchemy operations"""
    logger.info("\n=== Test 7: Async SQLAlchemy ===")
    
    async_engine = None
    try:
        # Create async engine
        async_engine = create_async_engine(
            'mysql+aiomysql://root:111@127.0.0.1:6001/test',
            echo=False
        )
        
        # Create async session factory
        AsyncSessionLocal = sessionmaker(
            async_engine, class_=AsyncSession, expire_on_commit=False
        )
        
        # Test async operations
        logger.info("\n🔄 Async Operations")
        async with AsyncSessionLocal() as session:
            # Check if user already exists
            existing_user = await session.execute(text("SELECT id FROM users WHERE username = 'async_user'"))
            if existing_user.fetchone():
                logger.info("   ✅ User already exists, skipping creation")
                # Get the existing user
                result = await session.execute(text("SELECT username, email FROM users WHERE username = 'async_user'"))
                row = result.fetchone()
                if row:
                    logger.info(f"   ✅ Queried existing user: {row[0]} ({row[1]})")
            else:
                # Create user
                user = User(username='async_user', email='async@example.com')
                session.add(user)
                await session.commit()
                logger.info("   ✅ Created user with async SQLAlchemy")
                
                # Query user
                result = await session.execute(text("SELECT username, email FROM users WHERE username = 'async_user'"))
                row = result.fetchone()
                if row:
                    logger.info(f"   ✅ Queried user: {row[0]} ({row[1]})")
                
                # Update user
                user.email = 'async.updated@example.com'
                await session.commit()
                logger.info("   ✅ Updated user with async SQLAlchemy")
                
                # Delete user
                await session.delete(user)
                await session.commit()
                logger.info("   ✅ Deleted user with async SQLAlchemy")
        
    except Exception as e:
        logger.error(f"❌ Async SQLAlchemy failed: {e}")
    finally:
        # Ensure proper cleanup
        if async_engine:
            try:
                await async_engine.dispose()
            except Exception as e:
                logger.debug(f"Async engine dispose warning: {e}")


def demo_sqlalchemy_performance():
    """Demonstrate SQLAlchemy performance optimization"""
    logger.info("\n=== Test 8: SQLAlchemy Performance ===")
    
    try:
        engine = create_engine('mysql+pymysql://root:111@127.0.0.1:6001/test')
        SessionLocal = sessionmaker(bind=engine)
        
        # Test bulk operations
        logger.info("\n⚡ Bulk Operations")
        session = SessionLocal()
        
        # Bulk insert
        import time
        start_time = time.time()
        
        users_data = []
        for i in range(100):
            users_data.append({
                'username': f'bulk_user_{i}',
                'email': f'bulk_user_{i}@example.com'
            })
        
        # Use bulk_insert_mappings for better performance
        session.bulk_insert_mappings(User, users_data)
        session.commit()
        
        end_time = time.time()
        logger.info(f"   ✅ Bulk inserted 100 users in {end_time - start_time:.3f} seconds")
        
        # Test query optimization
        logger.info("\n⚡ Query Optimization")
        start_time = time.time()
        
        # Use joinedload to avoid N+1 queries
        from sqlalchemy.orm import joinedload
        users_with_posts = session.query(User).options(joinedload(User.posts)).all()
        
        end_time = time.time()
        logger.info(f"   ✅ Loaded {len(users_with_posts)} users with posts in {end_time - start_time:.3f} seconds")
        
        # Cleanup bulk data
        session.query(User).filter(User.username.like('bulk_user_%')).delete()
        session.commit()
        logger.info("   ✅ Cleaned up bulk data")
        
        session.close()
        
    except Exception as e:
        logger.error(f"❌ SQLAlchemy performance test failed: {e}")


def demo_sqlalchemy_with_matrixone_features():
    """Demonstrate SQLAlchemy with MatrixOne-specific features"""
    logger.info("\n=== Test 9: SQLAlchemy with MatrixOne Features ===")
    
    try:
        engine = create_engine('mysql+pymysql://root:111@127.0.0.1:6001/test')
        SessionLocal = sessionmaker(bind=engine)
        session = SessionLocal()
        
        # Test MatrixOne-specific SQL
        logger.info("\n🔧 MatrixOne-specific Features")
        
        # Test SHOW commands
        result = session.execute(text("SHOW TABLES"))
        tables = [row[0] for row in result.fetchall()]
        logger.info(f"   Tables: {tables}")
        
        # Test MatrixOne functions
        result = session.execute(text("SELECT USER(), CURRENT_USER(), DATABASE()"))
        row = result.fetchone()
        logger.info(f"   MatrixOne info: {row}")
        
        # Test with MatrixOne client integration
        logger.info("\n🔧 MatrixOne Client Integration")
        client = Client(logger=logger)
        client.connect('127.0.0.1', 6001, 'root', '111', 'test')
        
        # Use MatrixOne client for account operations
        from matrixone.account import AccountManager
        account_manager = AccountManager(client)
        
        # Get current user info
        current_user = account_manager.get_current_user()
        logger.info(f"   Current user: {current_user.name}@{current_user.host}")
        
        # Use SQLAlchemy for data operations
        users = session.query(User).all()
        logger.info(f"   Users in database: {len(users)}")
        
        client.disconnect()
        session.close()
        
    except Exception as e:
        logger.error(f"❌ SQLAlchemy with MatrixOne features failed: {e}")


def demo_sqlalchemy_best_practices():
    """Demonstrate SQLAlchemy best practices"""
    logger.info("\n=== Test 10: SQLAlchemy Best Practices ===")
    
    try:
        # Best Practice 1: Use connection pooling
        logger.info("\n📋 Best Practice 1: Connection Pooling")
        engine = create_engine(
            'mysql+pymysql://root:111@127.0.0.1:6001/test',
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600
        )
        logger.info("   ✅ Engine configured with connection pooling")
        
        # Best Practice 2: Use context managers
        logger.info("\n📋 Best Practice 2: Context Managers")
        SessionLocal = sessionmaker(bind=engine)
        
        with SessionLocal() as session:
            # Operations within context
            result = session.execute(text("SELECT COUNT(*) FROM users"))
            count = result.scalar()
            logger.info(f"   ✅ Users count: {count}")
            # Session automatically closed
        
        # Best Practice 3: Use transactions properly
        logger.info("\n📋 Best Practice 3: Proper Transaction Usage")
        with SessionLocal() as session:
            try:
                # Check if user already exists
                existing_user = session.query(User).filter(User.username == 'best_practice_user').first()
                if existing_user:
                    user = existing_user
                    logger.info("   ✅ User already exists, using existing user")
                else:
                    user = User(username='best_practice_user', email='best@example.com')
                    session.add(user)
                    session.flush()
                    logger.info("   ✅ Created new user")
                
                # Check if post already exists
                existing_post = session.query(Post).filter(Post.title == 'Best Practice Post').first()
                if not existing_post:
                    post = Post(title='Best Practice Post', content='This post follows best practices', author_id=user.id)
                    session.add(post)
                    logger.info("   ✅ Created new post")
                else:
                    logger.info("   ✅ Post already exists")
                
                session.commit()
                logger.info("   ✅ Transaction completed successfully")
                
            except Exception as e:
                session.rollback()
                logger.error(f"   ❌ Transaction failed: {e}")
        
        # Best Practice 4: Use lazy loading appropriately
        logger.info("\n📋 Best Practice 4: Lazy Loading")
        with SessionLocal() as session:
            user = session.query(User).filter(User.username == 'best_practice_user').first()
            if user:
                # Access posts (lazy loading)
                posts_count = len(user.posts)
                logger.info(f"   ✅ User has {posts_count} posts (lazy loaded)")
        
        # Cleanup
        with SessionLocal() as session:
            # First delete posts by the user to avoid foreign key constraint
            user = session.query(User).filter(User.username == 'best_practice_user').first()
            if user:
                session.query(Post).filter(Post.author_id == user.id).delete()
                session.delete(user)
                session.commit()
                logger.info("   ✅ Cleaned up test data")
        
    except Exception as e:
        logger.error(f"❌ SQLAlchemy best practices failed: {e}")


def main():
    """Main demo function"""
    logger.info("🚀 MatrixOne SQLAlchemy Integration Examples")
    logger.info("=" * 60)
    
    # Run SQLAlchemy demos
    demo_basic_sqlalchemy_setup()
    demo_orm_operations()
    demo_sqlalchemy_transactions()
    demo_sqlalchemy_performance()
    demo_sqlalchemy_with_matrixone_features()
    demo_sqlalchemy_best_practices()
    
    # Run async SQLAlchemy demo
    asyncio.run(demo_async_sqlalchemy())
    
    logger.info("\n🎉 SQLAlchemy integration examples completed!")
    logger.info("\nKey achievements:")
    logger.info("- ✅ Basic SQLAlchemy setup and configuration")
    logger.info("- ✅ ORM model definitions and operations")
    logger.info("- ✅ SQLAlchemy transaction management")
    logger.info("- ✅ Async SQLAlchemy operations")
    logger.info("- ✅ Performance optimization techniques")
    logger.info("- ✅ MatrixOne-specific feature integration")
    logger.info("- ✅ SQLAlchemy best practices")


if __name__ == '__main__':
    main()
