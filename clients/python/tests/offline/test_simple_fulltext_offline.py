"""
Offline tests for SimpleFulltextQueryBuilder.

Tests SQL generation correctness without requiring database connection.
Verifies that generated SQL matches expected MatrixOne syntax.
"""

import pytest
from matrixone.client import Client, SimpleFulltextQueryBuilder, FulltextIndexManager


class TestSimpleFulltextQueryBuilderOffline:
    """Test SimpleFulltextQueryBuilder SQL generation offline."""

    def setup_method(self):
        """Set up test client."""
        self.client = Client()

    def test_basic_natural_language_search(self):
        """Test basic natural language search SQL generation."""
        builder = SimpleFulltextQueryBuilder(self.client, "articles")
        builder.columns("title", "content").search("machine learning")

        sql = builder.build_sql()
        expected = "SELECT * FROM articles WHERE MATCH(title, content) AGAINST('machine learning')"
        assert sql == expected

    def test_boolean_mode_required_terms(self):
        """Test boolean mode with required terms."""
        builder = SimpleFulltextQueryBuilder(self.client, "articles")
        builder.columns("title", "content").must_have("python", "tutorial")

        sql = builder.build_sql()
        expected = "SELECT * FROM articles WHERE MATCH(title, content) AGAINST('+python +tutorial' IN BOOLEAN MODE)"
        assert sql == expected

    def test_boolean_mode_excluded_terms(self):
        """Test boolean mode with excluded terms."""
        builder = SimpleFulltextQueryBuilder(self.client, "articles")
        builder.columns("title", "content").must_have("python").must_not_have("deprecated", "legacy")

        sql = builder.build_sql()
        expected = (
            "SELECT * FROM articles WHERE MATCH(title, content) AGAINST('+python -deprecated -legacy' IN BOOLEAN MODE)"
        )
        assert sql == expected

    def test_search_with_score(self):
        """Test search with relevance score."""
        builder = SimpleFulltextQueryBuilder(self.client, "articles")
        builder.columns("title", "content").search("data science").with_score()

        sql = builder.build_sql()
        expected = "SELECT *, MATCH(title, content) AGAINST('data science') AS score FROM articles WHERE MATCH(title, content) AGAINST('data science')"
        assert sql == expected

    def test_search_with_custom_score_alias(self):
        """Test search with custom score alias."""
        builder = SimpleFulltextQueryBuilder(self.client, "articles")
        builder.columns("title", "content").search("artificial intelligence").with_score("relevance")

        sql = builder.build_sql()
        expected = "SELECT *, MATCH(title, content) AGAINST('artificial intelligence') AS relevance FROM articles WHERE MATCH(title, content) AGAINST('artificial intelligence')"
        assert sql == expected

    def test_search_with_additional_where_conditions(self):
        """Test search with additional WHERE conditions."""
        builder = SimpleFulltextQueryBuilder(self.client, "articles")
        builder.columns("title", "content").search("python").where("category = 'Programming'").where("status = 'published'")

        sql = builder.build_sql()
        expected = "SELECT * FROM articles WHERE MATCH(title, content) AGAINST('python') AND category = 'Programming' AND status = 'published'"
        assert sql == expected

    def test_search_with_order_by_score(self):
        """Test search with order by score."""
        builder = SimpleFulltextQueryBuilder(self.client, "articles")
        builder.columns("title", "content").search("machine learning").with_score().order_by_score()

        sql = builder.build_sql()
        expected = "SELECT *, MATCH(title, content) AGAINST('machine learning') AS score FROM articles WHERE MATCH(title, content) AGAINST('machine learning') ORDER BY score DESC"
        assert sql == expected

    def test_search_with_order_by_score_asc(self):
        """Test search with order by score ascending."""
        builder = SimpleFulltextQueryBuilder(self.client, "articles")
        builder.columns("title", "content").search("tutorial").with_score().order_by_score(desc=False)

        sql = builder.build_sql()
        expected = "SELECT *, MATCH(title, content) AGAINST('tutorial') AS score FROM articles WHERE MATCH(title, content) AGAINST('tutorial') ORDER BY score ASC"
        assert sql == expected

    def test_search_with_order_by_column(self):
        """Test search with order by specific column."""
        builder = SimpleFulltextQueryBuilder(self.client, "articles")
        builder.columns("title", "content").search("python").order_by("created_at", desc=True)

        sql = builder.build_sql()
        expected = "SELECT * FROM articles WHERE MATCH(title, content) AGAINST('python') ORDER BY created_at DESC"
        assert sql == expected

    def test_search_with_limit(self):
        """Test search with limit."""
        builder = SimpleFulltextQueryBuilder(self.client, "articles")
        builder.columns("title", "content").search("javascript").limit(10)

        sql = builder.build_sql()
        expected = "SELECT * FROM articles WHERE MATCH(title, content) AGAINST('javascript') LIMIT 10"
        assert sql == expected

    def test_search_with_limit_and_offset(self):
        """Test search with limit and offset."""
        builder = SimpleFulltextQueryBuilder(self.client, "articles")
        builder.columns("title", "content").search("web development").limit(20, 5)

        sql = builder.build_sql()
        expected = "SELECT * FROM articles WHERE MATCH(title, content) AGAINST('web development') LIMIT 20 OFFSET 5"
        assert sql == expected

    def test_complete_query_chain(self):
        """Test complete query with all features."""
        builder = SimpleFulltextQueryBuilder(self.client, "documents")
        builder.columns("title", "body", "tags").must_have("machine", "learning").must_not_have("deprecated").with_score(
            "relevance"
        ).where("category = 'AI'").where("published = 1").order_by_score().limit(15, 10)

        sql = builder.build_sql()
        expected = (
            "SELECT *, MATCH(title, body, tags) AGAINST('+machine +learning -deprecated' IN BOOLEAN MODE) AS relevance "
            "FROM documents "
            "WHERE MATCH(title, body, tags) AGAINST('+machine +learning -deprecated' IN BOOLEAN MODE) AND category = 'AI' AND published = 1 "
            "ORDER BY relevance DESC "
            "LIMIT 15 OFFSET 10"
        )
        assert sql == expected

    def test_matrixone_syntax_compatibility(self):
        """Test compatibility with MatrixOne fulltext syntax from test cases."""
        # Test case from fulltext.result: select * from src where match(body, title) against('red');
        builder = SimpleFulltextQueryBuilder(self.client, "src")
        builder.columns("body", "title").search("red")

        sql = builder.build_sql()
        expected = "SELECT * FROM src WHERE MATCH(body, title) AGAINST('red')"
        assert sql == expected

        # Test case with score: select *, match(body, title) against('is red' in natural language mode) as score from src;
        builder = SimpleFulltextQueryBuilder(self.client, "src")
        builder.columns("body", "title").search("is red").with_score()

        sql = builder.build_sql()
        expected = (
            "SELECT *, MATCH(body, title) AGAINST('is red') AS score FROM src WHERE MATCH(body, title) AGAINST('is red')"
        )
        assert sql == expected

        # Test boolean mode: match(body, title) against('+red -blue' in boolean mode)
        builder = SimpleFulltextQueryBuilder(self.client, "src")
        builder.columns("body", "title").must_have("red").must_not_have("blue")

        sql = builder.build_sql()
        expected = "SELECT * FROM src WHERE MATCH(body, title) AGAINST('+red -blue' IN BOOLEAN MODE)"
        assert sql == expected

    def test_error_handling(self):
        """Test error handling for invalid configurations."""
        # No table name
        with pytest.raises(ValueError, match="Table name is required"):
            builder = SimpleFulltextQueryBuilder(self.client, "")
            builder.columns("title").search("test").build_sql()

        # No columns specified
        with pytest.raises(ValueError, match="Search columns must be specified"):
            builder = SimpleFulltextQueryBuilder(self.client, "articles")
            builder.search("test").build_sql()

        # No search query
        with pytest.raises(ValueError, match="Search query is required"):
            builder = SimpleFulltextQueryBuilder(self.client, "articles")
            builder.columns("title").build_sql()

    def test_multiple_columns(self):
        """Test with multiple search columns."""
        builder = SimpleFulltextQueryBuilder(self.client, "articles")
        builder.columns("title", "content", "summary", "tags").search("python programming")

        sql = builder.build_sql()
        expected = "SELECT * FROM articles WHERE MATCH(title, content, summary, tags) AGAINST('python programming')"
        assert sql == expected

    def test_chinese_text_search(self):
        """Test search with Chinese text (from MatrixOne test cases)."""
        builder = SimpleFulltextQueryBuilder(self.client, "src")
        builder.columns("body", "title").search("教學指引")

        sql = builder.build_sql()
        expected = "SELECT * FROM src WHERE MATCH(body, title) AGAINST('教學指引')"
        assert sql == expected


class TestSimpleQueryInterface:
    """Test the simple_query interface on FulltextIndexManager."""

    def setup_method(self):
        """Set up test client with fulltext index manager."""
        self.client = Client()
        self.client._fulltext_index = FulltextIndexManager(self.client)

    def test_simple_query_with_table_name(self):
        """Test simple_query with table name."""
        builder = self.client.fulltext_index.simple_query("articles")
        builder.columns("title", "content").search("python")

        sql = builder.build_sql()
        expected = "SELECT * FROM articles WHERE MATCH(title, content) AGAINST('python')"
        assert sql == expected

    def test_simple_query_chaining(self):
        """Test method chaining with simple_query."""
        sql = (
            self.client.fulltext_index.simple_query("documents")
            .columns("title", "body")
            .must_have("machine", "learning")
            .must_not_have("deprecated")
            .with_score()
            .order_by_score()
            .limit(10)
            .build_sql()
        )

        expected = (
            "SELECT *, MATCH(title, body) AGAINST('+machine +learning -deprecated' IN BOOLEAN MODE) AS score "
            "FROM documents "
            "WHERE MATCH(title, body) AGAINST('+machine +learning -deprecated' IN BOOLEAN MODE) "
            "ORDER BY score DESC "
            "LIMIT 10"
        )
        assert sql == expected

    def test_explain_method(self):
        """Test explain method returns SQL without execution."""
        sql = (
            self.client.fulltext_index.simple_query("articles")
            .columns("title", "content")
            .search("artificial intelligence")
            .explain()
        )

        expected = "SELECT * FROM articles WHERE MATCH(title, content) AGAINST('artificial intelligence')"
        assert sql == expected

    def test_table_vs_columns_parsing(self):
        """Test different ways to specify table and columns."""
        # Method 1: Table name + explicit columns
        builder1 = self.client.fulltext_index.simple_query("articles")
        builder1.columns("title", "content").search("test")
        sql1 = builder1.build_sql()

        # Method 2: Direct initialization with columns
        builder2 = SimpleFulltextQueryBuilder(self.client, "articles")
        builder2.columns("title", "content").search("test")
        sql2 = builder2.build_sql()

        # Should generate identical SQL
        assert sql1 == sql2
        expected = "SELECT * FROM articles WHERE MATCH(title, content) AGAINST('test')"
        assert sql1 == expected

    def test_async_execute_behavior(self):
        """Test execute method behavior with different client types."""
        from matrixone.client import Client, FulltextIndexManager
        from matrixone.async_client import AsyncClient, AsyncFulltextIndexManager

        # Test with sync client
        sync_client = Client()
        sync_client._fulltext_index = FulltextIndexManager(sync_client)
        sync_builder = sync_client.fulltext_index.simple_query("articles")
        sync_builder.columns("title", "content").search("test")

        # Should have execute method
        assert hasattr(sync_builder, 'execute')

        # Test with async client
        async_client = AsyncClient()
        async_client._fulltext_index = AsyncFulltextIndexManager(async_client)
        async_builder = async_client.fulltext_index.simple_query("articles")
        async_builder.columns("title", "content").search("test")

        # Should have execute method but it should raise error for sync calls
        assert hasattr(async_builder, 'execute')

        # Test that execute returns a coroutine for async client
        import asyncio

        result = async_builder.execute()
        assert asyncio.iscoroutine(result), "execute() should return a coroutine for async client"
        # Clean up the coroutine to avoid warning
        result.close()

    def test_transaction_simple_query_interface(self):
        """Test TransactionSimpleFulltextQueryBuilder interface."""
        from matrixone.client import Client, TransactionFulltextIndexManager

        # Mock transaction wrapper
        class MockTransaction:
            def execute(self, sql):
                return f"TRANSACTION: {sql}"

        client = Client()
        mock_tx = MockTransaction()
        tx_manager = TransactionFulltextIndexManager(client, mock_tx)

        # Test simple_query returns TransactionSimpleFulltextQueryBuilder
        tx_builder = tx_manager.simple_query("articles")
        assert hasattr(tx_builder, 'execute')
        assert hasattr(tx_builder, 'build_sql')

        # Test that it can build SQL
        tx_builder.columns("title", "content").search("test")
        sql = tx_builder.build_sql()
        expected = "SELECT * FROM articles WHERE MATCH(title, content) AGAINST('test')"
        assert sql == expected

    def test_async_transaction_simple_query_interface(self):
        """Test AsyncTransactionSimpleFulltextQueryBuilder interface."""
        from matrixone.async_client import AsyncClient, AsyncTransactionFulltextIndexManager

        # Mock async transaction wrapper
        class MockAsyncTransaction:
            async def execute(self, sql):
                return f"ASYNC_TRANSACTION: {sql}"

        async_client = AsyncClient()
        mock_tx = MockAsyncTransaction()
        tx_manager = AsyncTransactionFulltextIndexManager(async_client, mock_tx)

        # Test simple_query returns AsyncTransactionSimpleFulltextQueryBuilder
        tx_builder = tx_manager.simple_query("articles")
        assert hasattr(tx_builder, 'execute')
        assert hasattr(tx_builder, 'build_sql')

        # Test that execute returns a coroutine for async transaction
        import asyncio

        result = tx_builder.execute()
        assert asyncio.iscoroutine(result), "execute() should return a coroutine for async transaction"
        # Clean up the coroutine to avoid warning
        result.close()

        # Test that it can build SQL
        tx_builder.columns("title", "content").search("test")
        sql = tx_builder.build_sql()
        expected = "SELECT * FROM articles WHERE MATCH(title, content) AGAINST('test')"
        assert sql == expected


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
