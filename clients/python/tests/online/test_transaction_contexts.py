# Copyright 2021 - 2022 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Test that main interfaces work in both transaction contexts:
1. MatrixOne Client transaction context: with client.session() as tx:
2. SQLAlchemy transaction context: with client.get_sqlalchemy_engine().begin() as conn:
"""

import pytest
import pytest_asyncio
from matrixone import Client, AsyncClient
from .test_config import online_config
from matrixone.sqlalchemy_ext import boolean_match


class TestSyncTransactionContexts:
    """Test sync client transaction contexts"""

    @pytest.fixture(scope="function")
    def sync_client_setup(self):
        """Setup sync client for testing"""
        client = Client()
        host, port, user, password, database = online_config.get_connection_params()
        client.connect(host=host, port=port, user=user, password=password, database=database)

        # Enable fulltext indexing
        client.execute("SET experimental_fulltext_index=1")

        # Create test database and table
        test_db = "sync_tx_context_test"
        client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        client.execute(f"USE {test_db}")

        client.execute("DROP TABLE IF EXISTS sync_tx_docs")
        client.execute(
            """
            CREATE TABLE sync_tx_docs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                category VARCHAR(100) NOT NULL
            )
        """
        )

        # Insert test data
        test_docs = [
            ("Python Programming", "Learn Python programming basics", "Programming"),
            ("JavaScript Development", "Modern JavaScript development patterns", "Programming"),
            ("Database Design", "Database design principles and best practices", "Database"),
            ("Web Development", "Web development with modern frameworks", "Web"),
            ("Testing Strategies", "Software testing strategies and methodologies", "Testing"),
        ]

        for title, content, category in test_docs:
            client.execute(
                f"""
                INSERT INTO sync_tx_docs (title, content, category)
                VALUES ('{title}', '{content}', '{category}')
            """
            )

        # Create fulltext index
        client.fulltext_index.create("sync_tx_docs", "ftidx_sync_tx", ["title", "content"])

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

        with client.session() as tx:
            # Test basic execute
            result = tx.execute("SELECT COUNT(*) FROM sync_tx_docs")
            assert result.fetchall()[0][0] == 5

            # Test fulltext search
            result = tx.query(
                "sync_tx_docs.title", "sync_tx_docs.content", boolean_match("title", "content").encourage("python")
            ).execute()
            assert len(result.fetchall()) > 0

            # Test snapshot operations (commented out as MatrixOne doesn't support snapshots in transactions)
            # tx.snapshots.create("test_snap", "table", database=test_db, table="sync_tx_docs")

            # Test SQLAlchemy session
            session = tx
            assert session is not None

            # Test direct SQL execution via session
            from sqlalchemy import text

            result = tx.execute(text("SELECT COUNT(*) FROM sync_tx_docs"))
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
        await client.connect(host=host, port=port, user=user, password=password, database=database)

        # Enable fulltext indexing
        await client.execute("SET experimental_fulltext_index=1")

        # Create test database and table
        test_db = "async_tx_context_test"
        await client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        await client.execute(f"USE {test_db}")

        await client.execute("DROP TABLE IF EXISTS async_tx_docs")
        await client.execute(
            """
            CREATE TABLE async_tx_docs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                category VARCHAR(100) NOT NULL
            )
        """
        )

        # Insert test data
        test_docs = [
            (
                "Python Async Programming",
                "Learn Python async programming with asyncio",
                "Programming",
            ),
            (
                "JavaScript Async Patterns",
                "Modern JavaScript async patterns and promises",
                "Programming",
            ),
            (
                "Database Async Operations",
                "Async database operations and connection pooling",
                "Database",
            ),
            ("Web Async Development", "Async web development with modern frameworks", "Web"),
            (
                "Async Testing Strategies",
                "Testing async code and handling async test cases",
                "Testing",
            ),
        ]

        for title, content, category in test_docs:
            await client.execute(
                f"""
                INSERT INTO async_tx_docs (title, content, category)
                VALUES ('{title}', '{content}', '{category}')
            """
            )

        # Create fulltext index
        await client.fulltext_index.create("async_tx_docs", "ftidx_async_tx", ["title", "content"])

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

        async with client.session() as tx:
            # Test basic execute
            result = await tx.execute("SELECT COUNT(*) FROM async_tx_docs")
            assert result.fetchall()[0][0] == 5

            # Test fulltext search
            result = await tx.query(
                "async_tx_docs.title", "async_tx_docs.content", boolean_match("title", "content").encourage("python")
            ).execute()
            assert len(result.fetchall()) > 0

            # Test snapshot operations (commented out as MatrixOne doesn't support snapshots in transactions)
            # await tx.snapshots.create("test_snap", "table", database=test_db, table="async_tx_docs")

            # Test SQLAlchemy session
            session = tx
            assert session is not None

            # Test get_connection for direct connection access

            # Test direct connection usage
            from sqlalchemy import text

            result = await tx.execute(text("SELECT COUNT(*) FROM async_tx_docs"))
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
        client.connect(host=host, port=port, user=user, password=password, database=database)

        # Enable fulltext indexing
        client.execute("SET experimental_fulltext_index=1")

        # Create test database and table
        test_db = "compat_tx_test"
        client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        client.execute(f"USE {test_db}")

        client.execute("DROP TABLE IF EXISTS compat_tx_docs")
        client.execute(
            """
            CREATE TABLE compat_tx_docs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL
            )
        """
        )

        # Insert test data
        client.execute("INSERT INTO compat_tx_docs (title, content) VALUES ('Test Title', 'Test Content')")

        # Create fulltext index
        client.fulltext_index.create("compat_tx_docs", "ftidx_compat", ["title", "content"])

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

        with client.session() as tx:
            # Check that all managers are available
            assert hasattr(tx, 'execute')
            assert hasattr(tx, 'snapshots')
            assert hasattr(tx, 'clone')
            assert hasattr(tx, 'restore')
            assert hasattr(tx, 'pitr')
            assert hasattr(tx, 'pubsub')
            assert hasattr(tx, 'account')
            assert hasattr(tx, 'vector_ops')
            assert hasattr(tx, 'fulltext_index')
            # Session now IS a SQLAlchemy session, no need for get_sqlalchemy_session()

            # Test that fulltext_index has create and drop methods
            assert hasattr(tx.fulltext_index, 'create')
            assert hasattr(tx.fulltext_index, 'drop')

            # Test that query method works in transaction context
            result = tx.query("compat_tx_docs.title", "compat_tx_docs.content").execute()
            assert len(result.fetchall()) > 0

    @pytest.mark.asyncio
    async def test_async_transaction_wrapper_has_all_managers(self):
        """Test that async transaction wrapper has all necessary managers"""
        client = AsyncClient()
        host, port, user, password, database = online_config.get_connection_params()
        await client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Enable fulltext indexing
            await client.execute("SET experimental_fulltext_index=1")

            # Create test database and table
            test_db = "async_compat_tx_test"
            await client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
            await client.execute(f"USE {test_db}")

            await client.execute("DROP TABLE IF EXISTS async_compat_tx_docs")
            await client.execute(
                """
                CREATE TABLE async_compat_tx_docs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    content TEXT NOT NULL
                )
            """
            )

            # Insert test data
            await client.execute("INSERT INTO async_compat_tx_docs (title, content) VALUES ('Test Title', 'Test Content')")

            # Create fulltext index
            await client.fulltext_index.create("async_compat_tx_docs", "ftidx_async_compat", ["title", "content"])

            async with client.session() as tx:
                # Check that all managers are available
                assert hasattr(tx, 'execute')
                assert hasattr(tx, 'snapshots')
                assert hasattr(tx, 'clone')
                assert hasattr(tx, 'restore')
                assert hasattr(tx, 'pitr')
                assert hasattr(tx, 'pubsub')
                assert hasattr(tx, 'account')
                assert hasattr(tx, 'fulltext_index')
                # AsyncSession now IS a SQLAlchemy AsyncSession, no need for get_sqlalchemy_session()

                # Test that fulltext_index has create and drop methods
                assert hasattr(tx.fulltext_index, 'create')
                assert hasattr(tx.fulltext_index, 'drop')

                # Test that query method works in transaction context
                result = await tx.query("async_compat_tx_docs.title", "async_compat_tx_docs.content").execute()
                assert len(result.fetchall()) > 0

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
        client.connect(host=host, port=port, user=user, password=password, database=database)

        # Enable fulltext indexing
        client.execute("SET experimental_fulltext_index=1")

        # Create test database and table
        test_db = "features_tx_test"
        client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        client.execute(f"USE {test_db}")

        client.execute("DROP TABLE IF EXISTS features_tx_docs")
        client.execute(
            """
            CREATE TABLE features_tx_docs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                category VARCHAR(100) NOT NULL
            )
        """
        )

        # Insert test data
        test_docs = [
            ("Feature Test 1", "Content for feature test 1", "Category1"),
            ("Feature Test 2", "Content for feature test 2", "Category2"),
            ("Feature Test 3", "Content for feature test 3", "Category1"),
        ]

        for title, content, category in test_docs:
            client.execute(
                f"""
                INSERT INTO features_tx_docs (title, content, category)
                VALUES ('{title}', '{content}', '{category}')
            """
            )

        # Create fulltext index
        client.fulltext_index.create("features_tx_docs", "ftidx_features", ["title", "content"])

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

        with client.session() as tx:
            # Test basic search
            result = tx.query(
                "features_tx_docs.title", "features_tx_docs.content", boolean_match("title", "content").encourage("feature")
            ).execute()
            assert len(result.fetchall()) > 0

            # Test with score
            result = tx.query(
                "features_tx_docs.title",
                "features_tx_docs.content",
                boolean_match("title", "content").encourage("test").label("score"),
            ).execute()
            assert len(result.fetchall()) > 0
            # Check that score column is present
            assert len(list(result.keys())) > 2  # title, content, score

            # Test boolean mode
            result = tx.query(
                "features_tx_docs.title", "features_tx_docs.content", boolean_match("title", "content").must("feature")
            ).execute()
            assert len(result.fetchall()) > 0

            # Test with WHERE conditions
            result = (
                tx.query("features_tx_docs.title", "features_tx_docs.content")
                .filter(boolean_match("title", "content").encourage("test"), "features_tx_docs.category = 'Category1'")
                .execute()
            )
            assert len(result.fetchall()) > 0

            # Test ordering and limit
            result = (
                tx.query(
                    "features_tx_docs.title",
                    "features_tx_docs.content",
                    boolean_match("title", "content").encourage("test").label("score"),
                )
                .order_by("score ASC")
                .limit(2)
                .execute()
            )
            assert len(result.fetchall()) <= 2

    @pytest.mark.asyncio
    async def test_async_fulltext_features_in_transaction(self):
        """Test fulltext features in async transaction context"""
        client = AsyncClient()
        host, port, user, password, database = online_config.get_connection_params()
        await client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Enable fulltext indexing
            await client.execute("SET experimental_fulltext_index=1")

            # Create test database and table
            test_db = "async_features_tx_test"
            await client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
            await client.execute(f"USE {test_db}")

            await client.execute("DROP TABLE IF EXISTS async_features_tx_docs")
            await client.execute(
                """
                CREATE TABLE async_features_tx_docs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    content TEXT NOT NULL,
                    category VARCHAR(100) NOT NULL
                )
            """
            )

            # Insert test data
            test_docs = [
                ("Async Feature Test 1", "Content for async feature test 1", "Category1"),
                ("Async Feature Test 2", "Content for async feature test 2", "Category2"),
                ("Async Feature Test 3", "Content for async feature test 3", "Category1"),
            ]

            for title, content, category in test_docs:
                await client.execute(
                    f"""
                    INSERT INTO async_features_tx_docs (title, content, category)
                    VALUES ('{title}', '{content}', '{category}')
                """
                )

            # Create fulltext index
            await client.fulltext_index.create("async_features_tx_docs", "ftidx_async_features", ["title", "content"])

            async with client.session() as tx:
                # Test basic search
                result = await tx.query(
                    "async_features_tx_docs.title",
                    "async_features_tx_docs.content",
                    boolean_match("title", "content").encourage("async"),
                ).execute()
                assert len(result.fetchall()) > 0

                # Test with score
                result = await tx.query(
                    "async_features_tx_docs.title",
                    "async_features_tx_docs.content",
                    boolean_match("title", "content").encourage("test").label("score"),
                ).execute()
                assert len(result.fetchall()) > 0
                # Check that score column is present
                assert len(list(result.keys())) > 2  # title, content, score

                # Test boolean mode
                result = await tx.query(
                    "async_features_tx_docs.title",
                    "async_features_tx_docs.content",
                    boolean_match("title", "content").must("async"),
                ).execute()
                assert len(result.fetchall()) > 0

                # Test with WHERE conditions
                result = (
                    await tx.query("async_features_tx_docs.title", "async_features_tx_docs.content")
                    .filter(
                        boolean_match("title", "content").encourage("test"), "async_features_tx_docs.category = 'Category1'"
                    )
                    .execute()
                )
                assert len(result.fetchall()) > 0

                # Test ordering and limit
                result = (
                    await tx.query(
                        "async_features_tx_docs.title",
                        "async_features_tx_docs.content",
                        boolean_match("title", "content").encourage("test").label("score"),
                    )
                    .order_by("score ASC")
                    .limit(2)
                    .execute()
                )
                assert len(result.fetchall()) <= 2

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
        client.connect(host=host, port=port, user=user, password=password, database=database)

        # Create test database and table
        test_db = "get_conn_test"
        client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        client.execute(f"USE {test_db}")

        client.execute("DROP TABLE IF EXISTS get_conn_docs")
        client.execute(
            """
            CREATE TABLE get_conn_docs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert test data
        test_docs = [
            ("Connection Test 1", "Content for connection test 1"),
            ("Connection Test 2", "Content for connection test 2"),
            ("Connection Test 3", "Content for connection test 3"),
        ]

        for title, content in test_docs:
            client.execute(
                f"""
                INSERT INTO get_conn_docs (title, content)
                VALUES ('{title}', '{content}')
            """
            )

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

        with client.session() as tx:
            # Get connection

            # Test basic query execution
            from sqlalchemy import text

            result = tx.execute(text(f"SELECT COUNT(*) FROM {test_db}.get_conn_docs"))
            rows = result.fetchall()
            assert rows[0][0] == 3

            # Test parameterized query
            result = tx.execute(
                text(f"SELECT * FROM {test_db}.get_conn_docs WHERE title LIKE :pattern"),
                {"pattern": "%Test 1%"},
            )
            rows = result.fetchall()
            assert len(rows) == 1
            assert "Test 1" in rows[0][1]  # title column

    def test_sync_get_connection_transaction_isolation(self, sync_client_setup):
        """Test that get_connection maintains transaction isolation"""
        client, test_db = sync_client_setup

        # First, verify initial state
        result = client.execute(f"SELECT COUNT(*) FROM {test_db}.get_conn_docs")
        initial_count = result.fetchall()[0][0]

        with client.session() as tx:

            # Insert a new record within transaction
            from sqlalchemy import text

            tx.execute(
                text(
                    f"INSERT INTO {test_db}.get_conn_docs (title, content) VALUES ('Transaction Test', 'Content in transaction')"
                )
            )

            # Verify record exists within transaction
            result = tx.execute(text(f"SELECT COUNT(*) FROM {test_db}.get_conn_docs"))
            count_in_tx = result.scalar()
            assert count_in_tx == initial_count + 1

            # Verify record doesn't exist outside transaction (from another connection)
            result = client.execute(f"SELECT COUNT(*) FROM {test_db}.get_conn_docs")
            count_outside = result.fetchall()[0][0]
            assert count_outside == initial_count  # Should still be original count

        # After transaction commit, verify record exists
        result = client.execute(f"SELECT COUNT(*) FROM {test_db}.get_conn_docs")
        final_count = result.fetchall()[0][0]
        assert final_count == initial_count + 1

    @pytest.mark.asyncio
    async def test_async_get_connection_basic_usage(self):
        """Test basic usage of get_connection in async transaction"""
        client = AsyncClient()
        host, port, user, password, database = online_config.get_connection_params()
        await client.connect(host=host, port=port, user=user, password=password, database=database)

        try:
            # Create test database and table
            test_db = "async_get_conn_test"
            await client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
            await client.execute(f"USE {test_db}")

            await client.execute("DROP TABLE IF EXISTS async_get_conn_docs")
            await client.execute(
                """
                CREATE TABLE async_get_conn_docs (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    content TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Insert test data
            test_docs = [
                ("Async Connection Test 1", "Content for async connection test 1"),
                ("Async Connection Test 2", "Content for async connection test 2"),
                ("Async Connection Test 3", "Content for async connection test 3"),
            ]

            for title, content in test_docs:
                await client.execute(
                    f"""
                    INSERT INTO async_get_conn_docs (title, content)
                    VALUES ('{title}', '{content}')
                """
                )

            async with client.session() as tx:
                # Get connection

                # Test basic query execution
                from sqlalchemy import text

                result = await tx.execute(text("SELECT COUNT(*) FROM async_get_conn_docs"))
                rows = result.fetchall()
                assert rows[0][0] == 3

                # Test parameterized query
                result = await tx.execute(
                    text("SELECT * FROM async_get_conn_docs WHERE title LIKE :pattern"),
                    {"pattern": "%Test 1%"},
                )
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

        with client.session() as tx:
            # Test that we can use both get_connection and other managers

            # Use connection directly
            from sqlalchemy import text

            result = tx.execute(text(f"SELECT COUNT(*) FROM {test_db}.get_conn_docs"))
            direct_count = result.scalar()

            # Use transaction wrapper execute
            result = tx.execute(f"SELECT COUNT(*) FROM {test_db}.get_conn_docs")
            wrapper_count = result.fetchall()[0][0]

            # Both should return the same result
            assert direct_count == wrapper_count

            # Test that both are in the same transaction
            tx.execute(
                text(
                    f"INSERT INTO {test_db}.get_conn_docs (title, content) VALUES ('Direct Insert', 'Using connection directly')"
                )
            )
            tx.execute(
                f"INSERT INTO {test_db}.get_conn_docs (title, content) VALUES ('Wrapper Insert', 'Using transaction wrapper')"
            )

            # Both inserts should be visible within the transaction
            result = tx.execute(text(f"SELECT COUNT(*) FROM {test_db}.get_conn_docs"))
            final_count = result.scalar()
            assert final_count == direct_count + 2


class TestAsyncTransactionManagerConsistency:
    """Test that async transaction managers behave consistently with sync versions"""

    @pytest.fixture(scope="function")
    def sync_client_setup(self):
        """Setup sync client for testing"""
        client = Client()
        host, port, user, password, database = online_config.get_connection_params()
        client.connect(host=host, port=port, user=user, password=password, database=database)

        # Create test database and table
        test_db = "sync_async_consistency_test"
        client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        client.execute(f"USE {test_db}")

        client.execute("DROP TABLE IF EXISTS consistency_docs")
        client.execute(
            """
            CREATE TABLE consistency_docs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                category VARCHAR(100) NOT NULL,
                priority INT DEFAULT 1
            )
        """
        )

        # Insert test data
        test_docs = [
            ("Sync Test 1", "Content for sync test 1", "Category1", 1),
            ("Sync Test 2", "Content for sync test 2", "Category2", 2),
            ("Sync Test 3", "Content for sync test 3", "Category1", 3),
        ]

        for title, content, category, priority in test_docs:
            client.execute(
                f"""
                INSERT INTO consistency_docs (title, content, category, priority)
                VALUES ('{title}', '{content}', '{category}', {priority})
            """
            )

        yield client, test_db

        # Cleanup
        try:
            client.execute("DROP TABLE consistency_docs")
            client.execute(f"DROP DATABASE {test_db}")
        except Exception as e:
            print(f"Cleanup warning: {e}")
        finally:
            client.disconnect()

    @pytest_asyncio.fixture(scope="function")
    async def async_client_setup(self):
        """Setup async client for testing"""
        client = AsyncClient()
        host, port, user, password, database = online_config.get_connection_params()
        await client.connect(host=host, port=port, user=user, password=password, database=database)

        # Create test database and table
        test_db = "async_consistency_test"
        await client.execute(f"CREATE DATABASE IF NOT EXISTS {test_db}")
        await client.execute(f"USE {test_db}")

        await client.execute("DROP TABLE IF EXISTS async_consistency_docs")
        await client.execute(
            """
            CREATE TABLE async_consistency_docs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                category VARCHAR(100) NOT NULL,
                priority INT DEFAULT 1
            )
        """
        )

        # Insert test data
        test_docs = [
            ("Async Test 1", "Content for async test 1", "Category1", 1),
            ("Async Test 2", "Content for async test 2", "Category2", 2),
            ("Async Test 3", "Content for async test 3", "Category1", 3),
        ]

        for title, content, category, priority in test_docs:
            await client.execute(
                f"""
                INSERT INTO async_consistency_docs (title, content, category, priority)
                VALUES ('{title}', '{content}', '{category}', {priority})
            """
            )

        yield client, test_db

        # Cleanup
        try:
            await client.execute("DROP TABLE async_consistency_docs")
            await client.execute(f"DROP DATABASE {test_db}")
        except Exception as e:
            print(f"Cleanup warning: {e}")
        finally:
            await client.disconnect()

    def test_sync_transaction_managers_availability(self, sync_client_setup):
        """Test that all expected transaction managers are available in sync client"""
        client, test_db = sync_client_setup

        with client.session() as tx:
            # Test that all expected managers are available
            assert hasattr(tx, 'snapshots')
            assert hasattr(tx, 'clone')
            assert hasattr(tx, 'restore')
            assert hasattr(tx, 'pitr')
            assert hasattr(tx, 'pubsub')
            assert hasattr(tx, 'account')
            assert hasattr(tx, 'vector_ops')
            assert hasattr(tx, 'fulltext_index')
            # Session now IS a SQLAlchemy session, no need for get_sqlalchemy_session()

            # Test that managers are the correct types
            from matrixone.snapshot import SnapshotManager
            from matrixone.clone import CloneManager
            from matrixone.restore import RestoreManager
            from matrixone.pitr import PitrManager
            from matrixone.pubsub import PubSubManager
            from matrixone.account import AccountManager
            from matrixone.client import (
                VectorManager,
                FulltextIndexManager,
            )

            assert isinstance(tx.snapshots, SnapshotManager)
            assert isinstance(tx.clone, CloneManager)
            assert isinstance(tx.restore, RestoreManager)
            assert isinstance(tx.pitr, PitrManager)
            assert isinstance(tx.pubsub, PubSubManager)
            assert isinstance(tx.account, AccountManager)
            assert isinstance(tx.vector_ops, VectorManager)
            assert isinstance(tx.fulltext_index, FulltextIndexManager)

    @pytest.mark.asyncio
    async def test_async_transaction_managers_availability(self, async_client_setup):
        """Test that all expected transaction managers are available in async client"""
        client, test_db = async_client_setup

        async with client.session() as tx:
            # Test that all expected managers are available
            assert hasattr(tx, 'snapshots')
            assert hasattr(tx, 'clone')
            assert hasattr(tx, 'restore')
            assert hasattr(tx, 'pitr')
            assert hasattr(tx, 'pubsub')
            assert hasattr(tx, 'account')
            assert hasattr(tx, 'vector_ops')
            assert hasattr(tx, 'fulltext_index')
            # Session now IS a SQLAlchemy session, no need for get_sqlalchemy_session()

            # Test that managers are the correct types
            from matrixone.snapshot import AsyncSnapshotManager
            from matrixone.clone import AsyncCloneManager
            from matrixone.restore import AsyncRestoreManager
            from matrixone.pitr import AsyncPitrManager
            from matrixone.pubsub import AsyncPubSubManager
            from matrixone.account import AsyncAccountManager
            from matrixone.vector_manager import AsyncVectorManager
            from matrixone.fulltext_manager import AsyncFulltextIndexManager

            assert isinstance(tx.snapshots, AsyncSnapshotManager)
            assert isinstance(tx.clone, AsyncCloneManager)
            assert isinstance(tx.restore, AsyncRestoreManager)
            assert isinstance(tx.pitr, AsyncPitrManager)
            assert isinstance(tx.pubsub, AsyncPubSubManager)
            assert isinstance(tx.account, AsyncAccountManager)
            assert isinstance(tx.vector_ops, AsyncVectorManager)
            assert isinstance(tx.fulltext_index, AsyncFulltextIndexManager)

    def test_sync_vector_ops_transaction_behavior(self, sync_client_setup):
        """Test sync vector_ops manager behavior in transaction"""
        client, test_db = sync_client_setup

        with client.session() as tx:
            # Test that vector_ops manager has executor (new executor pattern)
            assert hasattr(tx.vector_ops, 'executor')

            # Vector managers use the executor pattern, so SQL is executed via tx.execute()
            # Test basic query execution
            result = tx.execute(f"SELECT COUNT(*) FROM {test_db}.consistency_docs")
            assert result.fetchall()[0][0] == 3

            # Test that operations use the same transaction
            tx.execute(
                f"INSERT INTO {test_db}.consistency_docs (title, content, category, priority) VALUES ('Vector Ops Test', 'Content from vector ops', 'Vector', 1)"
            )

            # Verify the insert is visible within the transaction
            result = tx.execute(f"SELECT COUNT(*) FROM {test_db}.consistency_docs")
            assert result.fetchall()[0][0] == 4

    @pytest.mark.asyncio
    async def test_async_vector_ops_transaction_behavior(self, async_client_setup):
        """Test async vector_ops manager behavior in transaction"""
        client, test_db = async_client_setup

        async with client.session() as tx:
            # Test that vector_ops manager has executor (new executor pattern)
            assert hasattr(tx.vector_ops, 'executor')

            # Vector managers use the executor pattern, so SQL is executed via tx.execute()
            # Test basic query execution
            result = await tx.execute(f"SELECT COUNT(*) FROM {test_db}.async_consistency_docs")
            assert result.fetchall()[0][0] == 3

            # Test that operations use the same transaction
            await tx.execute(
                f"INSERT INTO {test_db}.async_consistency_docs (title, content, category, priority) VALUES ('Async Vector Ops Test', 'Content from async vector ops', 'Vector', 1)"
            )

            # Verify the insert is visible within the transaction
            result = await tx.execute(f"SELECT COUNT(*) FROM {test_db}.async_consistency_docs")
            assert result.fetchall()[0][0] == 4

    def test_sync_transaction_isolation_consistency(self, sync_client_setup):
        """Test that sync transaction managers maintain proper isolation"""
        client, test_db = sync_client_setup

        # Get initial count
        result = client.execute(f"SELECT COUNT(*) FROM {test_db}.consistency_docs")
        initial_count = result.fetchall()[0][0]

        with client.session() as tx:
            # Use tx.execute() for all operations (executor pattern)
            tx.execute(
                f"INSERT INTO {test_db}.consistency_docs (title, content, category, priority) VALUES ('Vector Ops Insert', 'Content', 'Test', 1)"
            )
            tx.execute(
                f"INSERT INTO {test_db}.consistency_docs (title, content, category, priority) VALUES ('Direct Insert', 'Content', 'Test', 1)"
            )

            # Check count within transaction
            result1 = tx.execute(f"SELECT COUNT(*) FROM {test_db}.consistency_docs")
            result2 = tx.execute(f"SELECT COUNT(*) FROM {test_db}.consistency_docs")

            expected_count = initial_count + 2
            assert result1.fetchall()[0][0] == expected_count
            assert result2.fetchall()[0][0] == expected_count

            # Outside transaction should not see the changes yet
            result_outside = client.execute(f"SELECT COUNT(*) FROM {test_db}.consistency_docs")
            assert result_outside.rows[0][0] == initial_count

        # After transaction commit, all changes should be visible
        result_final = client.execute(f"SELECT COUNT(*) FROM {test_db}.consistency_docs")
        assert result_final.rows[0][0] == initial_count + 2

    @pytest.mark.asyncio
    async def test_async_transaction_isolation_consistency(self, async_client_setup):
        """Test that async transaction managers maintain proper isolation"""
        client, test_db = async_client_setup

        # Get initial count
        result = await client.execute(f"SELECT COUNT(*) FROM {test_db}.async_consistency_docs")
        initial_count = result.fetchall()[0][0]

        async with client.session() as tx:
            # Use tx.execute() for all operations (executor pattern)
            await tx.execute(
                f"INSERT INTO {test_db}.async_consistency_docs (title, content, category, priority) VALUES ('Async Vector Ops Insert', 'Content', 'Test', 1)"
            )
            await tx.execute(
                f"INSERT INTO {test_db}.async_consistency_docs (title, content, category, priority) VALUES ('Async Direct Insert', 'Content', 'Test', 1)"
            )

            # Check count within transaction
            result1 = await tx.execute(f"SELECT COUNT(*) FROM {test_db}.async_consistency_docs")
            result2 = await tx.execute(f"SELECT COUNT(*) FROM {test_db}.async_consistency_docs")

            expected_count = initial_count + 2
            assert result1.fetchall()[0][0] == expected_count
            assert result2.fetchall()[0][0] == expected_count

            # Outside transaction should not see the changes yet
            result_outside = await client.execute(f"SELECT COUNT(*) FROM {test_db}.async_consistency_docs")
            assert result_outside.rows[0][0] == initial_count

        # After transaction commit, all changes should be visible
        result_final = await client.execute(f"SELECT COUNT(*) FROM {test_db}.async_consistency_docs")
        assert result_final.rows[0][0] == initial_count + 2

    def test_sync_async_behavior_consistency(self, sync_client_setup):
        """Test that sync and async transaction managers behave consistently"""
        client, test_db = sync_client_setup

        # Get initial count first
        result = client.execute(f"SELECT COUNT(*) FROM {test_db}.consistency_docs")
        initial_count = result.fetchall()[0][0]

        with client.session() as tx:
            # All operations use tx.execute() (executor pattern)
            # Vector managers use the executor internally, not by exposing execute()

            # Test that vector_ops manager exists and has executor
            assert hasattr(tx, 'vector_ops')
            assert hasattr(tx.vector_ops, 'executor')

            # Check initial count
            result = tx.execute(f"SELECT COUNT(*) FROM {test_db}.consistency_docs")
            assert result.fetchall()[0][0] == initial_count

            # Test insert
            tx.execute(
                f"INSERT INTO {test_db}.consistency_docs (title, content, category, priority) VALUES ('Consistency Test', 'Content', 'Test', 1)"
            )

            # Verify insert is visible
            result = tx.execute(f"SELECT COUNT(*) FROM {test_db}.consistency_docs")
            assert result.fetchall()[0][0] == initial_count + 1

    @pytest.mark.asyncio
    async def test_async_async_behavior_consistency(self, async_client_setup):
        """Test that async transaction managers behave consistently"""
        client, test_db = async_client_setup

        # Get initial count first
        result = await client.execute(f"SELECT COUNT(*) FROM {test_db}.async_consistency_docs")
        initial_count = result.fetchall()[0][0]

        async with client.session() as tx:
            # All operations use tx.execute() (executor pattern)
            # Vector managers use the executor internally, not by exposing execute()

            # Test that vector_ops manager exists and has executor
            assert hasattr(tx, 'vector_ops')
            assert hasattr(tx.vector_ops, 'executor')

            # Check initial count
            result = await tx.execute(f"SELECT COUNT(*) FROM {test_db}.async_consistency_docs")
            assert result.fetchall()[0][0] == initial_count

            # Test insert
            await tx.execute(
                f"INSERT INTO {test_db}.async_consistency_docs (title, content, category, priority) VALUES ('Async Consistency Test', 'Content', 'Test', 1)"
            )

            # Verify insert is visible
            result = await tx.execute(f"SELECT COUNT(*) FROM {test_db}.async_consistency_docs")
            assert result.fetchall()[0][0] == initial_count + 1
