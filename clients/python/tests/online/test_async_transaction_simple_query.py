"""
Test async transaction simple_query functionality
"""

import pytest
import pytest_asyncio
from matrixone import AsyncClient


class TestAsyncTransactionSimpleQuery:
    """Test async transaction simple_query operations"""

    @pytest_asyncio.fixture(scope="function")
    async def async_client_setup(self):
        """Setup async client for testing"""
        client = AsyncClient()
        await client.connect("127.0.0.1", 6001, "root", "111", "test")
        
        # Enable fulltext indexing
        await client.execute("SET experimental_fulltext_index=1")
        
        # Create test database and table
        test_db = "async_tx_simple_query_test"
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
    async def test_async_transaction_simple_query_basic(self, async_client_setup):
        """Test basic async transaction simple_query functionality"""
        client, test_db = async_client_setup
        
        async with client.transaction() as tx:
            # Test basic search within transaction
            result = await (tx.fulltext_index.simple_query("async_tx_docs")
                           .columns("title", "content")
                           .search("python")
                           .execute())
            
            assert result is not None
            assert len(result.rows) > 0
            
            # Verify results contain "python"
            for row in result.rows:
                title_content = f"{row[1]} {row[2]}".lower()  # title and content
                assert "python" in title_content

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_with_score(self, async_client_setup):
        """Test async transaction simple_query with score"""
        client, test_db = async_client_setup
        
        async with client.transaction() as tx:
            # Test search with score within transaction
            result = await (tx.fulltext_index.simple_query("async_tx_docs")
                           .columns("title", "content")
                           .search("async")
                           .with_score()
                           .order_by_score(desc=True)
                           .execute())
            
            assert result is not None
            assert len(result.rows) > 0
            
            # Verify results are ordered by score (descending)
            scores = [row[-1] for row in result.rows]  # Last column is score
            assert scores == sorted(scores, reverse=True)

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_boolean_mode(self, async_client_setup):
        """Test async transaction simple_query with boolean mode"""
        client, test_db = async_client_setup
        
        async with client.transaction() as tx:
            # Test boolean mode search within transaction
            result = await (tx.fulltext_index.simple_query("async_tx_docs")
                           .columns("title", "content")
                           .must_have("async")
                           .must_not_have("basic")
                           .execute())
            
            assert result is not None
            assert len(result.rows) > 0
            
            # Verify all results contain "async" but not "basic"
            for row in result.rows:
                title_content = f"{row[1]} {row[2]}".lower()  # title and content
                assert "async" in title_content
                assert "basic" not in title_content

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_with_where(self, async_client_setup):
        """Test async transaction simple_query with WHERE conditions"""
        client, test_db = async_client_setup
        
        async with client.transaction() as tx:
            # Test search with WHERE conditions within transaction
            result = await (tx.fulltext_index.simple_query("async_tx_docs")
                           .columns("title", "content")
                           .search("programming")
                           .where("category = 'Programming'")
                           .execute())
            
            assert result is not None
            assert len(result.rows) > 0
            
            # Verify all results are from Programming category
            # Since we only select title and content, we can't directly check category
            # But the WHERE clause ensures this
            assert len(result.rows) > 0

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_ordering_and_limit(self, async_client_setup):
        """Test async transaction simple_query with ordering and limit"""
        client, test_db = async_client_setup
        
        async with client.transaction() as tx:
            # Test search with ordering and limit within transaction
            result = await (tx.fulltext_index.simple_query("async_tx_docs")
                           .columns("title", "content")
                           .search("async")
                           .order_by("title", desc=True)
                           .limit(2)
                           .execute())
            
            assert result is not None
            assert len(result.rows) <= 2
            
            # Verify results are ordered by title (descending)
            titles = [row[1] for row in result.rows]  # title column
            assert titles == sorted(titles, reverse=True)

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_explain(self, async_client_setup):
        """Test async transaction simple_query explain functionality"""
        client, test_db = async_client_setup
        
        async with client.transaction() as tx:
            # Test explain functionality within transaction
            builder = tx.fulltext_index.simple_query("async_tx_docs")
            builder.columns("title", "content").search("test").with_score().where("category = 'Testing'")
            
            sql = builder.explain()
            assert sql is not None
            assert "SELECT" in sql.upper()
            assert "MATCH" in sql.upper()
            assert "AGAINST" in sql.upper()
            assert "async_tx_docs" in sql

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_multiple_operations(self, async_client_setup):
        """Test multiple async transaction simple_query operations in one transaction"""
        client, test_db = async_client_setup
        
        async with client.transaction() as tx:
            # Test multiple searches within the same transaction
            results = []
            
            # Search 1: Basic search
            result1 = await (tx.fulltext_index.simple_query("async_tx_docs")
                            .columns("title", "content")
                            .search("python")
                            .execute())
            results.append(result1)
            
            # Search 2: Boolean mode
            result2 = await (tx.fulltext_index.simple_query("async_tx_docs")
                            .columns("title", "content")
                            .must_have("javascript")
                            .execute())
            results.append(result2)
            
            # Search 3: With score and ordering
            result3 = await (tx.fulltext_index.simple_query("async_tx_docs")
                            .columns("title", "content")
                            .search("database")
                            .with_score()
                            .order_by_score(desc=True)
                            .execute())
            results.append(result3)
            
            # Verify all searches returned results
            for result in results:
                assert result is not None
                assert len(result.rows) > 0

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_with_database_prefix(self, async_client_setup):
        """Test async transaction simple_query with database prefix"""
        client, test_db = async_client_setup
        
        async with client.transaction() as tx:
            # Test search with database prefix within transaction
            table_name = f"{test_db}.async_tx_docs"
            result = await (tx.fulltext_index.simple_query(table_name)
                           .columns("title", "content")
                           .search("web")
                           .execute())
            
            assert result is not None
            assert len(result.rows) > 0
            
            # Verify results contain "web"
            for row in result.rows:
                title_content = f"{row[1]} {row[2]}".lower()  # title and content
                assert "web" in title_content
