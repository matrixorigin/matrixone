"""
Test that main interfaces work in both transaction contexts:
1. MatrixOne Client transaction context: with client.transaction() as tx:
2. SQLAlchemy transaction context: with client.get_sqlalchemy_engine().begin() as conn:
"""

import pytest
import pytest_asyncio
from matrixone import Client, AsyncClient
from contextlib import contextmanager
from .test_config import online_config


class TestSyncTransactionContexts:
    """Test sync client transaction contexts"""

    @pytest.fixture(scope="function")
    def sync_client_setup(self):
        """Setup sync client for testing"""
        client = Client()
        host, port, user, password, database = online_config.get_connection_params()
        client.connect(host, port, user, password, database)
        
        # Enable fulltext indexing
        client.execute("SET experimental_fulltext_index=1")
        
        # Create test database and table
        test_db = "sync_tx_context_test"
        client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        client.execute(f"USE {test_db}")
        
        client.execute("DROP TABLE IF EXISTS sync_tx_docs")
        client.execute("""
            CREATE TABLE sync_tx_docs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                category VARCHAR(100) NOT NULL
            )
        """)
        
        # Insert test data
        test_docs = [
            ("Python Programming", "Learn Python programming basics", "Programming"),
            ("JavaScript Development", "Modern JavaScript development patterns", "Programming"),
            ("Database Design", "Database design principles and best practices", "Database"),
            ("Web Development", "Web development with modern frameworks", "Web"),
            ("Testing Strategies", "Software testing strategies and methodologies", "Testing")
        ]
        
        for title, content, category in test_docs:
            client.execute(f"""
                INSERT INTO sync_tx_docs (title, content, category) 
                VALUES ('{title}', '{content}', '{category}')
            """)
        
        # Create fulltext index
        client.fulltext_index.create(
            "sync_tx_docs", 
            "ftidx_sync_tx", 
            ["title", "content"]
        )
        
        yield client, test_db
        
        # Cleanup
        try:
            client.fulltext_index.drop("sync_tx_docs", "ftidx_sync_tx")
            client.execute("DROP TABLE sync_tx_docs")
            client.execute(f"DROP DATABASE {test_db}")
        except Exception as e:
            print(f"Cleanup warning: {e}")
        finally:
            client.disconnect()

    def test_sync_client_transaction_context(self, sync_client_setup):
        """Test sync client transaction context"""
        client, test_db = sync_client_setup
        
        with client.transaction() as tx:
            # Test basic execute
            result = tx.execute("SELECT COUNT(*) FROM sync_tx_docs")
            assert result.rows[0][0] == 5
            
            # Test fulltext search
            result = tx.fulltext_index.simple_query("sync_tx_docs").columns("title", "content").search("python").execute()
            assert len(result.rows) > 0
            
            # Test snapshot operations (commented out as MatrixOne doesn't support snapshots in transactions)
            # tx.snapshots.create("test_snap", "table", database=test_db, table="sync_tx_docs")
            
            # Test SQLAlchemy session
            session = tx.get_sqlalchemy_session()
            assert session is not None
            
            # Test get_connection for direct connection access
            conn = tx.get_connection()
            assert conn is not None
            
            # Test direct connection usage
            from sqlalchemy import text
            result = conn.execute(text("SELECT COUNT(*) FROM sync_tx_docs"))
            rows = result.fetchall()
            assert rows[0][0] == 5
            
            # Test account operations
            # Note: Account operations might require special permissions
            # tx.account.create_user("test_user", "test_password")

    def test_sync_sqlalchemy_transaction_context(self, sync_client_setup):
        """Test sync SQLAlchemy transaction context"""
        client, test_db = sync_client_setup
        
        with client.get_sqlalchemy_engine().begin() as conn:
            # Test direct SQL execution
            from sqlalchemy import text
            result = conn.execute(text("SELECT COUNT(*) FROM sync_tx_docs"))
            rows = result.fetchall()
            assert rows[0][0] == 5
            
            # Test SQLAlchemy session in transaction
            from sqlalchemy.orm import sessionmaker
            Session = sessionmaker(bind=conn)
            session = Session()
            assert session is not None
            session.close()


class TestAsyncTransactionContexts:
    """Test async client transaction contexts"""

    @pytest_asyncio.fixture(scope="function")
    async def async_client_setup(self):
        """Setup async client for testing"""
        client = AsyncClient()
        host, port, user, password, database = online_config.get_connection_params()
        await client.connect(host, port, user, password, database)
        
        # Enable fulltext indexing
        await client.execute("SET experimental_fulltext_index=1")
        
        # Create test database and table
        test_db = "async_tx_context_test"
        await client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        await client.execute(f"USE {test_db}")
        
        await client.execute("DROP TABLE IF EXISTS async_tx_docs")
        await client.execute("""
            CREATE TABLE async_tx_docs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                category VARCHAR(100) NOT NULL
            )
        """)
        
        # Insert test data
        test_docs = [
            ("Python Async Programming", "Learn Python async programming with asyncio", "Programming"),
            ("JavaScript Async Patterns", "Modern JavaScript async patterns and promises", "Programming"),
            ("Database Async Operations", "Async database operations and connection pooling", "Database"),
            ("Web Async Development", "Async web development with modern frameworks", "Web"),
            ("Async Testing Strategies", "Testing async code and handling async test cases", "Testing")
        ]
        
        for title, content, category in test_docs:
            await client.execute(f"""
                INSERT INTO async_tx_docs (title, content, category) 
                VALUES ('{title}', '{content}', '{category}')
            """)
        
        # Create fulltext index
        await client.fulltext_index.create(
            "async_tx_docs", 
            "ftidx_async_tx", 
            ["title", "content"]
        )
        
        yield client, test_db
        
        # Cleanup
        try:
            await client.fulltext_index.drop("async_tx_docs", "ftidx_async_tx")
            await client.execute("DROP TABLE async_tx_docs")
            await client.execute(f"DROP DATABASE {test_db}")
        except Exception as e:
            print(f"Cleanup warning: {e}")
        finally:
            await client.disconnect()

    @pytest.mark.asyncio
    async def test_async_client_transaction_context(self, async_client_setup):
        """Test async client transaction context"""
        client, test_db = async_client_setup
        
        async with client.transaction() as tx:
            # Test basic execute
            result = await tx.execute("SELECT COUNT(*) FROM async_tx_docs")
            assert result.rows[0][0] == 5
            
            # Test fulltext search
            result = await tx.fulltext_index.simple_query("async_tx_docs").columns("title", "content").search("python").execute()
            assert len(result.rows) > 0
            
            # Test snapshot operations (commented out as MatrixOne doesn't support snapshots in transactions)
            # await tx.snapshots.create("test_snap", "table", database=test_db, table="async_tx_docs")
            
            # Test SQLAlchemy session
            session = await tx.get_sqlalchemy_session()
            assert session is not None
            
            # Test get_connection for direct connection access
            conn = tx.get_connection()
            assert conn is not None
            
            # Test direct connection usage
            from sqlalchemy import text
            result = await conn.execute(text("SELECT COUNT(*) FROM async_tx_docs"))
            rows = result.fetchall()
            assert rows[0][0] == 5

    @pytest.mark.asyncio
    async def test_async_sqlalchemy_transaction_context(self, async_client_setup):
        """Test async SQLAlchemy transaction context"""
        client, test_db = async_client_setup
        
        async with client.get_sqlalchemy_engine().begin() as conn:
            # Test direct SQL execution
            from sqlalchemy import text
            result = await conn.execute(text("SELECT COUNT(*) FROM async_tx_docs"))
            rows = result.fetchall()
            assert rows[0][0] == 5
            
            # Test SQLAlchemy session in transaction
            try:
                from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
                AsyncSessionLocal = async_sessionmaker(bind=conn, class_=AsyncSession, expire_on_commit=False)
            except ImportError:
                # Fallback for older SQLAlchemy versions
                from sqlalchemy.ext.asyncio import AsyncSession
                from sqlalchemy.orm import sessionmaker
                AsyncSessionLocal = sessionmaker(bind=conn, class_=AsyncSession, expire_on_commit=False)
            session = AsyncSessionLocal()
            assert session is not None
            await session.close()


class TestTransactionContextCompatibility:
    """Test compatibility between different transaction contexts"""

    @pytest.fixture(scope="function")
    def sync_client_setup(self):
        """Setup sync client for testing"""
        client = Client()
        host, port, user, password, database = online_config.get_connection_params()
        client.connect(host, port, user, password, database)
        
        # Enable fulltext indexing
        client.execute("SET experimental_fulltext_index=1")
        
        # Create test database and table
        test_db = "compat_tx_test"
        client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        client.execute(f"USE {test_db}")
        
        client.execute("DROP TABLE IF EXISTS compat_tx_docs")
        client.execute("""
            CREATE TABLE compat_tx_docs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL
            )
        """)
        
        # Insert test data
        client.execute("INSERT INTO compat_tx_docs (title, content) VALUES ('Test Title', 'Test Content')")
        
        # Create fulltext index
        client.fulltext_index.create(
            "compat_tx_docs", 
            "ftidx_compat", 
            ["title", "content"]
        )
        
        yield client, test_db
        
        # Cleanup
        try:
            client.fulltext_index.drop("compat_tx_docs", "ftidx_compat")
            client.execute("DROP TABLE compat_tx_docs")
            client.execute(f"DROP DATABASE {test_db}")
        except Exception as e:
            print(f"Cleanup warning: {e}")
        finally:
            client.disconnect()

    def test_sync_transaction_wrapper_has_all_managers(self, sync_client_setup):
        """Test that sync transaction wrapper has all necessary managers"""
        client, test_db = sync_client_setup
        
        with client.transaction() as tx:
            # Check that all managers are available
            assert hasattr(tx, 'execute')
            assert hasattr(tx, 'snapshots')
            assert hasattr(tx, 'clone')
            assert hasattr(tx, 'restore')
            assert hasattr(tx, 'pitr')
            assert hasattr(tx, 'pubsub')
            assert hasattr(tx, 'account')
            assert hasattr(tx, 'vector_ops')
            assert hasattr(tx, 'vector_query')
            assert hasattr(tx, 'fulltext_index')
            assert hasattr(tx, 'get_sqlalchemy_session')
            
            # Test that fulltext_index has simple_query
            assert hasattr(tx.fulltext_index, 'simple_query')
            
            # Test that simple_query returns transaction-aware builder
            builder = tx.fulltext_index.simple_query("compat_tx_docs")
            assert hasattr(builder, 'execute')
            assert hasattr(builder, 'columns')
            assert hasattr(builder, 'search')

    @pytest.mark.asyncio
    async def test_async_transaction_wrapper_has_all_managers(self):
        """Test that async transaction wrapper has all necessary managers"""
        client = AsyncClient()
        host, port, user, password, database = online_config.get_connection_params()
        await client.connect(host, port, user, password, database)
        
        try:
            # Enable fulltext indexing
            await client.execute("SET experimental_fulltext_index=1")
            
            # Create test database and table
            test_db = "async_compat_tx_test"
            await client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
            await client.execute(f"USE {test_db}")
            
            await client.execute("DROP TABLE IF EXISTS async_compat_tx_docs")
            await client.execute("""
                CREATE TABLE async_compat_tx_docs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    content TEXT NOT NULL
                )
            """)
            
            # Insert test data
            await client.execute("INSERT INTO async_compat_tx_docs (title, content) VALUES ('Test Title', 'Test Content')")
            
            # Create fulltext index
            await client.fulltext_index.create(
                "async_compat_tx_docs", 
                "ftidx_async_compat", 
                ["title", "content"]
            )
            
            async with client.transaction() as tx:
                # Check that all managers are available
                assert hasattr(tx, 'execute')
                assert hasattr(tx, 'snapshots')
                assert hasattr(tx, 'clone')
                assert hasattr(tx, 'restore')
                assert hasattr(tx, 'pitr')
                assert hasattr(tx, 'pubsub')
                assert hasattr(tx, 'account')
                assert hasattr(tx, 'fulltext_index')
                assert hasattr(tx, 'get_sqlalchemy_session')
                
                # Test that fulltext_index has simple_query
                assert hasattr(tx.fulltext_index, 'simple_query')
                
                # Test that simple_query returns transaction-aware builder
                builder = tx.fulltext_index.simple_query("async_compat_tx_docs")
                assert hasattr(builder, 'execute')
                assert hasattr(builder, 'columns')
                assert hasattr(builder, 'search')
                
                # Test execution
                result = await builder.columns("title", "content").search("test").execute()
                assert len(result.rows) > 0
            
        finally:
            # Cleanup
            try:
                await client.fulltext_index.drop("async_compat_tx_docs", "ftidx_async_compat")
                await client.execute("DROP TABLE async_compat_tx_docs")
                await client.execute(f"DROP DATABASE {test_db}")
            except Exception as e:
                print(f"Cleanup warning: {e}")
            finally:
                await client.disconnect()


class TestTransactionContextFeatures:
    """Test specific features in transaction contexts"""

    @pytest.fixture(scope="function")
    def sync_client_setup(self):
        """Setup sync client for testing"""
        client = Client()
        host, port, user, password, database = online_config.get_connection_params()
        client.connect(host, port, user, password, database)
        
        # Enable fulltext indexing
        client.execute("SET experimental_fulltext_index=1")
        
        # Create test database and table
        test_db = "features_tx_test"
        client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        client.execute(f"USE {test_db}")
        
        client.execute("DROP TABLE IF EXISTS features_tx_docs")
        client.execute("""
            CREATE TABLE features_tx_docs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                category VARCHAR(100) NOT NULL
            )
        """)
        
        # Insert test data
        test_docs = [
            ("Feature Test 1", "Content for feature test 1", "Category1"),
            ("Feature Test 2", "Content for feature test 2", "Category2"),
            ("Feature Test 3", "Content for feature test 3", "Category1")
        ]
        
        for title, content, category in test_docs:
            client.execute(f"""
                INSERT INTO features_tx_docs (title, content, category) 
                VALUES ('{title}', '{content}', '{category}')
            """)
        
        # Create fulltext index
        client.fulltext_index.create(
            "features_tx_docs", 
            "ftidx_features", 
            ["title", "content"]
        )
        
        yield client, test_db
        
        # Cleanup
        try:
            client.fulltext_index.drop("features_tx_docs", "ftidx_features")
            client.execute("DROP TABLE features_tx_docs")
            client.execute(f"DROP DATABASE {test_db}")
        except Exception as e:
            print(f"Cleanup warning: {e}")
        finally:
            client.disconnect()

    def test_sync_fulltext_features_in_transaction(self, sync_client_setup):
        """Test fulltext features in sync transaction context"""
        client, test_db = sync_client_setup
        
        with client.transaction() as tx:
            # Test basic search
            result = tx.fulltext_index.simple_query("features_tx_docs").columns("title", "content").search("feature").execute()
            assert len(result.rows) > 0
            
            # Test with score
            result = tx.fulltext_index.simple_query("features_tx_docs").columns("title", "content").search("test").with_score().execute()
            assert len(result.rows) > 0
            # Check that score column is present
            assert len(result.columns) > 2  # id, title, content, score
            
            # Test boolean mode
            result = tx.fulltext_index.simple_query("features_tx_docs").columns("title", "content").must_have("feature").execute()
            assert len(result.rows) > 0
            
            # Test with WHERE conditions
            result = tx.fulltext_index.simple_query("features_tx_docs").columns("title", "content").search("test").where("category = 'Category1'").execute()
            assert len(result.rows) > 0
            
            # Test ordering and limit
            result = tx.fulltext_index.simple_query("features_tx_docs").columns("title", "content").search("test").order_by("title").limit(2).execute()
            assert len(result.rows) <= 2
            
            # Test explain
            builder = tx.fulltext_index.simple_query("features_tx_docs").columns("title", "content").search("test")
            sql = builder.explain()
            assert "SELECT" in sql.upper()
            assert "MATCH" in sql.upper()
            assert "AGAINST" in sql.upper()

    @pytest.mark.asyncio
    async def test_async_fulltext_features_in_transaction(self):
        """Test fulltext features in async transaction context"""
        client = AsyncClient()
        host, port, user, password, database = online_config.get_connection_params()
        await client.connect(host, port, user, password, database)
        
        try:
            # Enable fulltext indexing
            await client.execute("SET experimental_fulltext_index=1")
            
            # Create test database and table
            test_db = "async_features_tx_test"
            await client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
            await client.execute(f"USE {test_db}")
            
            await client.execute("DROP TABLE IF EXISTS async_features_tx_docs")
            await client.execute("""
                CREATE TABLE async_features_tx_docs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    content TEXT NOT NULL,
                    category VARCHAR(100) NOT NULL
                )
            """)
            
            # Insert test data
            test_docs = [
                ("Async Feature Test 1", "Content for async feature test 1", "Category1"),
                ("Async Feature Test 2", "Content for async feature test 2", "Category2"),
                ("Async Feature Test 3", "Content for async feature test 3", "Category1")
            ]
            
            for title, content, category in test_docs:
                await client.execute(f"""
                    INSERT INTO async_features_tx_docs (title, content, category) 
                    VALUES ('{title}', '{content}', '{category}')
                """)
            
            # Create fulltext index
            await client.fulltext_index.create(
                "async_features_tx_docs", 
                "ftidx_async_features", 
                ["title", "content"]
            )
            
            async with client.transaction() as tx:
                # Test basic search
                result = await tx.fulltext_index.simple_query("async_features_tx_docs").columns("title", "content").search("async").execute()
                assert len(result.rows) > 0
                
                # Test with score
                result = await tx.fulltext_index.simple_query("async_features_tx_docs").columns("title", "content").search("test").with_score().execute()
                assert len(result.rows) > 0
                # Check that score column is present
                assert len(result.columns) > 2  # id, title, content, score
                
                # Test boolean mode
                result = await tx.fulltext_index.simple_query("async_features_tx_docs").columns("title", "content").must_have("async").execute()
                assert len(result.rows) > 0
                
                # Test with WHERE conditions
                result = await tx.fulltext_index.simple_query("async_features_tx_docs").columns("title", "content").search("test").where("category = 'Category1'").execute()
                assert len(result.rows) > 0
                
                # Test ordering and limit
                result = await tx.fulltext_index.simple_query("async_features_tx_docs").columns("title", "content").search("test").order_by("title").limit(2).execute()
                assert len(result.rows) <= 2
                
                # Test explain
                builder = tx.fulltext_index.simple_query("async_features_tx_docs").columns("title", "content").search("test")
                sql = builder.explain()
                assert "SELECT" in sql.upper()
                assert "MATCH" in sql.upper()
                assert "AGAINST" in sql.upper()
            
        finally:
            # Cleanup
            try:
                await client.fulltext_index.drop("async_features_tx_docs", "ftidx_async_features")
                await client.execute("DROP TABLE async_features_tx_docs")
                await client.execute(f"DROP DATABASE {test_db}")
            except Exception as e:
                print(f"Cleanup warning: {e}")
            finally:
                await client.disconnect()


class TestGetConnectionInterface:
    """Test get_connection interface in transaction contexts"""

    @pytest.fixture(scope="function")
    def sync_client_setup(self):
        """Setup sync client for testing"""
        client = Client()
        host, port, user, password, database = online_config.get_connection_params()
        client.connect(host, port, user, password, database)
        
        # Create test database and table
        test_db = "get_conn_test"
        client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        client.execute(f"USE {test_db}")
        
        client.execute("DROP TABLE IF EXISTS get_conn_docs")
        client.execute("""
            CREATE TABLE get_conn_docs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert test data
        test_docs = [
            ("Connection Test 1", "Content for connection test 1"),
            ("Connection Test 2", "Content for connection test 2"),
            ("Connection Test 3", "Content for connection test 3")
        ]
        
        for title, content in test_docs:
            client.execute(f"""
                INSERT INTO get_conn_docs (title, content) 
                VALUES ('{title}', '{content}')
            """)
        
        yield client, test_db
        
        # Cleanup
        try:
            client.execute("DROP TABLE get_conn_docs")
            client.execute(f"DROP DATABASE {test_db}")
        except Exception as e:
            print(f"Cleanup warning: {e}")
        finally:
            client.disconnect()

    def test_sync_get_connection_basic_usage(self, sync_client_setup):
        """Test basic usage of get_connection in sync transaction"""
        client, test_db = sync_client_setup
        
        with client.transaction() as tx:
            # Get connection
            conn = tx.get_connection()
            assert conn is not None
            
            # Test basic query execution
            from sqlalchemy import text
            result = conn.execute(text(f"SELECT COUNT(*) FROM {test_db}.get_conn_docs"))
            rows = result.fetchall()
            assert rows[0][0] == 3
            
            # Test parameterized query
            result = conn.execute(text(f"SELECT * FROM {test_db}.get_conn_docs WHERE title LIKE :pattern"), 
                                {"pattern": "%Test 1%"})
            rows = result.fetchall()
            assert len(rows) == 1
            assert "Test 1" in rows[0][1]  # title column

    def test_sync_get_connection_transaction_isolation(self, sync_client_setup):
        """Test that get_connection maintains transaction isolation"""
        client, test_db = sync_client_setup
        
        # First, verify initial state
        result = client.execute(f"SELECT COUNT(*) FROM {test_db}.get_conn_docs")
        initial_count = result.rows[0][0]
        
        with client.transaction() as tx:
            conn = tx.get_connection()
            
            # Insert a new record within transaction
            from sqlalchemy import text
            conn.execute(text(f"INSERT INTO {test_db}.get_conn_docs (title, content) VALUES ('Transaction Test', 'Content in transaction')"))
            
            # Verify record exists within transaction
            result = conn.execute(text(f"SELECT COUNT(*) FROM {test_db}.get_conn_docs"))
            count_in_tx = result.scalar()
            assert count_in_tx == initial_count + 1
            
            # Verify record doesn't exist outside transaction (from another connection)
            result = client.execute(f"SELECT COUNT(*) FROM {test_db}.get_conn_docs")
            count_outside = result.rows[0][0]
            assert count_outside == initial_count  # Should still be original count
        
        # After transaction commit, verify record exists
        result = client.execute(f"SELECT COUNT(*) FROM {test_db}.get_conn_docs")
        final_count = result.rows[0][0]
        assert final_count == initial_count + 1

    @pytest.mark.asyncio
    async def test_async_get_connection_basic_usage(self):
        """Test basic usage of get_connection in async transaction"""
        client = AsyncClient()
        host, port, user, password, database = online_config.get_connection_params()
        await client.connect(host, port, user, password, database)
        
        try:
            # Create test database and table
            test_db = "async_get_conn_test"
            await client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
            await client.execute(f"USE {test_db}")
            
            await client.execute("DROP TABLE IF EXISTS async_get_conn_docs")
            await client.execute("""
                CREATE TABLE async_get_conn_docs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    content TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert test data
            test_docs = [
                ("Async Connection Test 1", "Content for async connection test 1"),
                ("Async Connection Test 2", "Content for async connection test 2"),
                ("Async Connection Test 3", "Content for async connection test 3")
            ]
            
            for title, content in test_docs:
                await client.execute(f"""
                    INSERT INTO async_get_conn_docs (title, content) 
                    VALUES ('{title}', '{content}')
                """)
            
            async with client.transaction() as tx:
                # Get connection
                conn = tx.get_connection()
                assert conn is not None
                
                # Test basic query execution
                from sqlalchemy import text
                result = await conn.execute(text("SELECT COUNT(*) FROM async_get_conn_docs"))
                rows = result.fetchall()
                assert rows[0][0] == 3
                
                # Test parameterized query
                result = await conn.execute(text("SELECT * FROM async_get_conn_docs WHERE title LIKE :pattern"), 
                                          {"pattern": "%Test 1%"})
                rows = result.fetchall()
                assert len(rows) == 1
                assert "Test 1" in rows[0][1]  # title column
                
        finally:
            # Cleanup
            try:
                await client.execute("DROP TABLE async_get_conn_docs")
                await client.execute(f"DROP DATABASE {test_db}")
            except Exception as e:
                print(f"Cleanup warning: {e}")
            finally:
                await client.disconnect()

    def test_sync_get_connection_with_other_managers(self, sync_client_setup):
        """Test that get_connection works alongside other transaction managers"""
        client, test_db = sync_client_setup
        
        with client.transaction() as tx:
            # Test that we can use both get_connection and other managers
            conn = tx.get_connection()
            
            # Use connection directly
            from sqlalchemy import text
            result = conn.execute(text(f"SELECT COUNT(*) FROM {test_db}.get_conn_docs"))
            direct_count = result.scalar()
            
            # Use transaction wrapper execute
            result = tx.execute(f"SELECT COUNT(*) FROM {test_db}.get_conn_docs")
            wrapper_count = result.rows[0][0]
            
            # Both should return the same result
            assert direct_count == wrapper_count
            
            # Test that both are in the same transaction
            conn.execute(text(f"INSERT INTO {test_db}.get_conn_docs (title, content) VALUES ('Direct Insert', 'Using connection directly')"))
            tx.execute(f"INSERT INTO {test_db}.get_conn_docs (title, content) VALUES ('Wrapper Insert', 'Using transaction wrapper')")
            
            # Both inserts should be visible within the transaction
            result = conn.execute(text(f"SELECT COUNT(*) FROM {test_db}.get_conn_docs"))
            final_count = result.scalar()
            assert final_count == direct_count + 2
