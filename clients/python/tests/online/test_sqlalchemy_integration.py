"""
Online tests for SQLAlchemy integration

These tests are inspired by example_06_sqlalchemy_integration.py
"""

import pytest
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, ForeignKey, text
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.pool import QueuePool
from matrixone import Client, AsyncClient
from matrixone.logger import create_default_logger

# SQLAlchemy setup - will be created per test to avoid conflicts


@pytest.mark.online
class TestSQLAlchemyIntegration:
    """Test SQLAlchemy integration functionality"""

    def test_basic_sqlalchemy_operations(self, test_client):
        """Test basic SQLAlchemy operations"""
        # Create SQLAlchemy engine using MatrixOne client
        engine = test_client.get_sqlalchemy_engine()

        # Create independent Base and models for this test
        Base = declarative_base()

        class User(Base):
            __tablename__ = 'test_users_basic'

            id = Column(Integer, primary_key=True)
            username = Column(String(50), unique=True, nullable=False)
            email = Column(String(100), unique=True, nullable=False)
            created_at = Column(DateTime)

            # Relationship
            posts = relationship("Post", back_populates="author")

        class Post(Base):
            __tablename__ = 'test_posts_basic'

            id = Column(Integer, primary_key=True)
            title = Column(String(200), nullable=False)
            content = Column(Text)
            author_id = Column(Integer, ForeignKey('test_users_basic.id'))
            created_at = Column(DateTime)

            # Relationship
            author = relationship("User", back_populates="posts")

        # Drop tables if exist and create new ones
        with engine.begin() as conn:
            conn.execute(text('DROP TABLE IF EXISTS test_posts_basic'))
            conn.execute(text('DROP TABLE IF EXISTS test_users_basic'))

        # Create tables
        Base.metadata.create_all(engine)

        # Create session
        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            # Test basic CRUD operations
            # Create user
            user = User(username="testuser", email="test@example.com")
            session.add(user)
            session.commit()

            # Query user
            found_user = session.query(User).filter_by(username="testuser").first()
            assert found_user is not None
            assert found_user.username == "testuser"
            assert found_user.email == "test@example.com"

            # Create post
            post = Post(title="Test Post", content="This is a test post", author_id=found_user.id)
            session.add(post)
            session.commit()

            # Query post with relationship
            found_post = session.query(Post).filter_by(title="Test Post").first()
            assert found_post is not None
            assert found_post.title == "Test Post"
            assert found_post.author.username == "testuser"

            # Update user
            found_user.email = "updated@example.com"
            session.commit()

            # Verify update
            updated_user = session.query(User).filter_by(username="testuser").first()
            assert updated_user.email == "updated@example.com"

            # Delete post
            session.delete(found_post)
            session.commit()

            # Verify deletion
            deleted_post = session.query(Post).filter_by(title="Test Post").first()
            assert deleted_post is None

        finally:
            session.close()
            # Cleanup
            Base.metadata.drop_all(engine)

    def test_sqlalchemy_with_transactions(self, test_client):
        """Test SQLAlchemy with transactions"""
        engine = test_client.get_sqlalchemy_engine()

        # Create independent Base and models for this test
        Base = declarative_base()

        class User(Base):
            __tablename__ = 'test_users_transactions'

            id = Column(Integer, primary_key=True)
            username = Column(String(50), unique=True, nullable=False)
            email = Column(String(100), unique=True, nullable=False)
            created_at = Column(DateTime)

            # Relationship
            posts = relationship("Post", back_populates="author")

        class Post(Base):
            __tablename__ = 'test_posts_transactions'

            id = Column(Integer, primary_key=True)
            title = Column(String(200), nullable=False)
            content = Column(Text)
            author_id = Column(Integer, ForeignKey('test_users_transactions.id'))
            created_at = Column(DateTime)

            # Relationship
            author = relationship("User", back_populates="posts")

        # Drop tables if exist and create new ones
        with engine.begin() as conn:
            conn.execute(text('DROP TABLE IF EXISTS test_posts_transactions'))
            conn.execute(text('DROP TABLE IF EXISTS test_users_transactions'))

        Base.metadata.create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            # Test transaction rollback
            user = User(username="transaction_user", email="transaction@example.com")
            session.add(user)
            session.flush()  # Flush to get ID

            post = Post(title="Transaction Post", content="This will be rolled back", author_id=user.id)
            session.add(post)

            # Rollback transaction
            session.rollback()

            # Verify rollback
            found_user = session.query(User).filter_by(username="transaction_user").first()
            assert found_user is None

            # Test successful transaction
            user = User(username="success_user", email="success@example.com")
            session.add(user)
            session.commit()

            # Verify success
            found_user = session.query(User).filter_by(username="success_user").first()
            assert found_user is not None

        finally:
            session.close()
            Base.metadata.drop_all(engine)

    def test_sqlalchemy_raw_sql(self, test_client):
        """Test SQLAlchemy with raw SQL"""
        engine = test_client.get_sqlalchemy_engine()

        # Create independent Base and models for this test
        Base = declarative_base()

        class User(Base):
            __tablename__ = 'test_users_raw_sql'

            id = Column(Integer, primary_key=True)
            username = Column(String(50), unique=True, nullable=False)
            email = Column(String(100), unique=True, nullable=False)
            created_at = Column(DateTime)

        # Drop table if exists and create new one
        with engine.begin() as conn:
            conn.execute(text('DROP TABLE IF EXISTS test_users_raw_sql'))

        Base.metadata.create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            # Test raw SQL execution
            result = session.execute(text("SELECT 1 as test_value, USER() as user_info"))
            row = result.fetchone()
            assert row[0] == 1  # test_value
            assert row[1] is not None  # user_info

            # Test raw SQL with parameters
            result = session.execute(text("SELECT :value as param_value"), {"value": 42})
            row = result.fetchone()
            assert row[0] == 42

            # Test raw SQL for table operations
            session.execute(
                text("INSERT INTO test_users_raw_sql (username, email) VALUES (:username, :email)"),
                {"username": "raw_user", "email": "raw@example.com"},
            )
            session.commit()

            # Verify insertion
            found_user = session.query(User).filter_by(username="raw_user").first()
            assert found_user is not None

        finally:
            session.close()
            Base.metadata.drop_all(engine)

    def test_sqlalchemy_connection_pooling(self, test_client):
        """Test SQLAlchemy connection pooling"""
        # Get the default engine (which already has connection pooling configured)
        engine = test_client.get_sqlalchemy_engine()

        # Create independent Base and models for this test
        Base = declarative_base()

        class User(Base):
            __tablename__ = 'test_users_pooling'

            id = Column(Integer, primary_key=True)
            username = Column(String(50), unique=True, nullable=False)
            email = Column(String(100), unique=True, nullable=False)
            created_at = Column(DateTime)

        # Drop table if exists and create new one
        with engine.begin() as conn:
            conn.execute(text('DROP TABLE IF EXISTS test_users_pooling'))

        Base.metadata.create_all(engine)

        Session = sessionmaker(bind=engine)

        # Test multiple sessions
        sessions = []
        try:
            for i in range(3):
                session = Session()
                sessions.append(session)

                user = User(username=f"pool_user_{i}", email=f"pool{i}@example.com")
                session.add(user)
                session.commit()

                # Verify user was created
                found_user = session.query(User).filter_by(username=f"pool_user_{i}").first()
                assert found_user is not None

        finally:
            for session in sessions:
                session.close()
            Base.metadata.drop_all(engine)

    @pytest.mark.asyncio
    async def test_async_sqlalchemy_operations(self, test_async_client):
        """Test async SQLAlchemy operations"""
        try:
            # Test async CRUD operations using raw SQL
            await test_async_client.execute(
                "CREATE TABLE IF NOT EXISTS async_users (id INT PRIMARY KEY AUTO_INCREMENT, username VARCHAR(50), email VARCHAR(100))"
            )

            # Clear any existing data
            await test_async_client.execute("DELETE FROM async_users")

            # Insert user
            await test_async_client.execute(
                "INSERT INTO async_users (username, email) VALUES ('async_user', 'async@example.com')"
            )

            # Query user
            result = await test_async_client.execute("SELECT * FROM async_users WHERE username = 'async_user'")
            assert result is not None
            assert len(result.rows) > 0
            assert result.rows[0][1] == "async_user"  # username column

            # Update user
            await test_async_client.execute(
                "UPDATE async_users SET email = 'updated_async@example.com' WHERE username = 'async_user'"
            )

            # Verify update
            result = await test_async_client.execute("SELECT email FROM async_users WHERE username = 'async_user'")
            assert result.rows[0][0] == "updated_async@example.com"

        finally:
            # Cleanup
            await test_async_client.execute("DROP TABLE IF EXISTS async_users")

    def test_sqlalchemy_with_matrixone_features(self, test_client):
        """Test SQLAlchemy with MatrixOne-specific features"""
        engine = test_client.get_sqlalchemy_engine()

        # Create independent Base and models for this test
        Base = declarative_base()

        class User(Base):
            __tablename__ = 'test_users_matrixone'

            id = Column(Integer, primary_key=True)
            username = Column(String(50), unique=True, nullable=False)
            email = Column(String(100), unique=True, nullable=False)
            created_at = Column(DateTime)

        # Drop table if exists and create new one
        with engine.begin() as conn:
            conn.execute(text('DROP TABLE IF EXISTS test_users_matrixone'))

        Base.metadata.create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            # Test MatrixOne-specific SQL through SQLAlchemy
            result = session.execute(text("SHOW DATABASES"))
            databases = result.fetchall()
            assert len(databases) > 0

            # Test MatrixOne version info
            result = session.execute(text("SELECT VERSION()"))
            version = result.fetchone()
            assert version is not None
            assert "MatrixOne" in version[0] or "mysql" in version[0].lower()

            # Test MatrixOne user info
            result = session.execute(text("SELECT USER()"))
            user_info = result.fetchone()
            assert user_info is not None

        finally:
            session.close()
            Base.metadata.drop_all(engine)

    def test_sqlalchemy_error_handling(self, test_client):
        """Test SQLAlchemy error handling"""
        engine = test_client.get_sqlalchemy_engine()

        # Create independent Base and models for this test
        Base = declarative_base()

        class User(Base):
            __tablename__ = 'test_users_errors'

            id = Column(Integer, primary_key=True)
            username = Column(String(50), unique=True, nullable=False)
            email = Column(String(100), unique=True, nullable=False)
            created_at = Column(DateTime)

        # Drop table if exists and create new one
        with engine.begin() as conn:
            conn.execute(text('DROP TABLE IF EXISTS test_users_errors'))

        Base.metadata.create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            # Test duplicate key error
            user1 = User(username="duplicate_user", email="duplicate1@example.com")
            session.add(user1)
            session.commit()

            user2 = User(username="duplicate_user", email="duplicate2@example.com")
            session.add(user2)

            try:
                session.commit()
                assert False, "Should have failed with duplicate key"
            except Exception as e:
                # Expected to fail
                session.rollback()
                assert "duplicate" in str(e).lower() or "unique" in str(e).lower()

            # Test invalid SQL
            try:
                session.execute(text("INVALID SQL STATEMENT"))
                assert False, "Should have failed with invalid SQL"
            except Exception as e:
                # Expected to fail
                assert "syntax" in str(e).lower() or "error" in str(e).lower()

        finally:
            session.close()
            Base.metadata.drop_all(engine)
