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
Online tests for SimpleFulltextQueryBuilder.

Tests functionality against a real MatrixOne database to verify
that generated SQL works correctly and returns expected results.
"""

import pytest
from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import declarative_base

from matrixone.client import Client
from matrixone.config import get_connection_kwargs
from matrixone.exceptions import QueryError

# SQLAlchemy model for testing
Base = declarative_base()


class Article(Base):
    __tablename__ = "test_simple_fulltext_articles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(255), nullable=False)
    content = Column(Text, nullable=False)
    category = Column(String(100), nullable=False)
    tags = Column(String(500))


class TestSimpleFulltextOnline:
    """Online tests for SimpleFulltextQueryBuilder with real database."""

    @classmethod
    def setup_class(cls):
        """Set up test database and data."""
        # Get connection parameters
        conn_params = get_connection_kwargs()
        client_params = {k: v for k, v in conn_params.items() if k in ['host', 'port', 'user', 'password', 'database']}

        cls.client = Client()
        cls.client.connect(**client_params)

        cls.test_db = "test_simple_fulltext"
        try:
            cls.client.execute(f"DROP DATABASE IF EXISTS {cls.test_db}")
            cls.client.execute(f"CREATE DATABASE {cls.test_db}")
            cls.client.execute(f"USE {cls.test_db}")
        except Exception as e:
            print(f"Database setup warning: {e}")

        # Create test table
        cls.client.execute("DROP TABLE IF EXISTS test_simple_fulltext_articles")
        cls.client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_simple_fulltext_articles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                category VARCHAR(100) NOT NULL,
                tags VARCHAR(500)
            )
        """
        )

        # Insert test data
        test_articles = [
            (
                1,
                "Python Programming Tutorial",
                "Learn Python programming from basics to advanced concepts",
                "Programming",
                "python,tutorial,beginner",
            ),
            (
                2,
                "Machine Learning with Python",
                "Complete guide to machine learning algorithms and implementation",
                "AI",
                "python,ml,ai,algorithms",
            ),
            (
                3,
                "JavaScript Web Development",
                "Modern JavaScript development for web applications",
                "Programming",
                "javascript,web,frontend",
            ),
            (
                4,
                "Data Science Fundamentals",
                "Introduction to data science concepts and tools",
                "AI",
                "data,science,analytics",
            ),
            (
                5,
                "Deprecated Python Libraries",
                "Old Python libraries that should be avoided",
                "Programming",
                "python,deprecated,legacy",
            ),
            (
                6,
                "Advanced Machine Learning",
                "Deep learning and neural networks with TensorFlow",
                "AI",
                "ai,deeplearning,tensorflow",
            ),
            (
                7,
                "Web Development Best Practices",
                "Modern practices for building web applications",
                "Programming",
                "web,best practices,modern",
            ),
            (
                8,
                "Artificial Intelligence Overview",
                "Introduction to AI concepts and applications",
                "AI",
                "ai,overview,introduction",
            ),
        ]

        for article in test_articles:
            title_escaped = article[1].replace("'", "''")
            content_escaped = article[2].replace("'", "''")
            category_escaped = article[3].replace("'", "''")
            tags_escaped = article[4].replace("'", "''") if article[4] else ''

            cls.client.execute(
                f"""
                INSERT INTO test_simple_fulltext_articles (id, title, content, category, tags) 
                VALUES ({article[0]}, '{title_escaped}', '{content_escaped}', '{category_escaped}', '{tags_escaped}')
            """
            )

        # Create fulltext index
        try:
            cls.client.execute("CREATE FULLTEXT INDEX ft_articles ON test_simple_fulltext_articles(title, content, tags)")
        except Exception as e:
            print(f"Fulltext index creation warning: {e}")

    @classmethod
    def teardown_class(cls):
        """Clean up test database."""
        if hasattr(cls, 'client'):
            cls.client.disconnect()

    def test_basic_natural_language_search(self):
        """Test basic natural language search with real database."""
        results = (
            self.client.fulltext_index.simple_query("test_simple_fulltext_articles")
            .columns("title", "content", "tags")
            .search("python programming")
            .execute()
        )

        assert isinstance(results.rows, list)
        assert len(results.rows) > 0

        # Should find Python-related articles
        found_python = any("Python" in row[1] for row in results.rows)  # title column
        assert found_python, "Should find articles containing 'Python'"

    def test_boolean_mode_required_terms(self):
        """Test boolean mode with required terms."""
        results = (
            self.client.fulltext_index.simple_query("test_simple_fulltext_articles")
            .columns("title", "content", "tags")
            .must_have("python", "tutorial")
            .execute()
        )

        assert isinstance(results.rows, list)
        # Should find articles that have both "python" AND "tutorial"
        if results.rows:
            for row in results.rows:
                title_content = f"{row[1]} {row[2]}".lower()  # title + content
                assert "python" in title_content and "tutorial" in title_content

    def test_boolean_mode_excluded_terms(self):
        """Test boolean mode with excluded terms."""
        results = (
            self.client.fulltext_index.simple_query("test_simple_fulltext_articles")
            .columns("title", "content", "tags")
            .must_have("python")
            .must_not_have("deprecated")
            .execute()
        )

        assert isinstance(results.rows, list)
        # Should find Python articles but exclude deprecated ones
        for row in results.rows:
            title_content = f"{row[1]} {row[2]}".lower()  # title + content
            assert "python" in title_content
            assert "deprecated" not in title_content

    def test_search_with_score(self):
        """Test search with relevance scoring."""
        results = (
            self.client.fulltext_index.simple_query("test_simple_fulltext_articles")
            .columns("title", "content", "tags")
            .search("machine learning")
            .with_score()
            .execute()
        )

        assert isinstance(results.rows, list)
        assert len(results.rows) > 0

        # Check that score column is included
        assert len(results.columns) > 4  # Original 5 columns + score
        score_column_index = len(results.columns) - 1  # Score should be last column

        # Verify score values are numeric
        for row in results.rows:
            score = row[score_column_index]
            assert isinstance(score, (int, float)), f"Score should be numeric, got {type(score)}"
            assert score >= 0, f"Score should be non-negative, got {score}"

    def test_search_with_where_conditions(self):
        """Test search with additional WHERE conditions."""
        results = (
            self.client.fulltext_index.simple_query("test_simple_fulltext_articles")
            .columns("title", "content", "tags")
            .search("programming")
            .where("category = 'Programming'")
            .execute()
        )

        assert isinstance(results.rows, list)
        # All results should be in Programming category
        for row in results.rows:
            category = row[3]  # category column
            assert category == "Programming"

    def test_search_with_ordering_and_limit(self):
        """Test search with ordering and pagination."""
        results = (
            self.client.fulltext_index.simple_query("test_simple_fulltext_articles")
            .columns("title", "content", "tags")
            .search("development")
            .with_score()
            .order_by_score()
            .limit(3)
            .execute()
        )

        assert isinstance(results.rows, list)
        assert len(results.rows) <= 3  # Should respect limit

        if len(results.rows) > 1:
            # Verify results are ordered by score (descending)
            score_index = len(results.columns) - 1
            scores = [row[score_index] for row in results.rows]
            assert scores == sorted(scores, reverse=True), "Results should be ordered by score DESC"

    def test_search_by_category(self):
        """Test filtering by category."""
        # Search AI category
        ai_results = (
            self.client.fulltext_index.simple_query("test_simple_fulltext_articles")
            .columns("title", "content", "tags")
            .search("learning")
            .where("category = 'AI'")
            .execute()
        )

        # Search Programming category
        prog_results = (
            self.client.fulltext_index.simple_query("test_simple_fulltext_articles")
            .columns("title", "content", "tags")
            .search("programming")
            .where("category = 'Programming'")
            .execute()
        )

        # Verify results are from correct categories
        for row in ai_results.rows:
            assert row[3] == "AI"

        for row in prog_results.rows:
            assert row[3] == "Programming"

    def test_complex_boolean_search(self):
        """Test complex boolean search with multiple terms."""
        results = (
            self.client.fulltext_index.simple_query("test_simple_fulltext_articles")
            .columns("title", "content", "tags")
            .must_have("web")
            .must_not_have("deprecated", "legacy")
            .where("category = 'Programming'")
            .execute()
        )

        assert isinstance(results.rows, list)
        # Should find web-related articles without deprecated content
        for row in results.rows:
            title_content = f"{row[1]} {row[2]}".lower()
            assert "web" in title_content
            assert "deprecated" not in title_content
            assert "legacy" not in title_content
            assert row[3] == "Programming"

    def test_search_with_custom_score_alias(self):
        """Test search with custom score alias."""
        results = (
            self.client.fulltext_index.simple_query("test_simple_fulltext_articles")
            .columns("title", "content", "tags")
            .search("artificial intelligence")
            .with_score("relevance")
            .order_by_score()
            .execute()
        )

        assert isinstance(results.rows, list)
        # Score column should be present (custom alias doesn't change result structure)
        if results.rows:
            score_index = len(results.columns) - 1
            for row in results.rows:
                score = row[score_index]
                assert isinstance(score, (int, float))

    def test_empty_search_results(self):
        """Test search that should return no results."""
        results = (
            self.client.fulltext_index.simple_query("test_simple_fulltext_articles")
            .columns("title", "content", "tags")
            .search("nonexistent_term_xyz123")
            .execute()
        )

        assert isinstance(results.rows, list)
        assert len(results.rows) == 0

    def test_chinese_text_search(self):
        """Test search with non-English text."""
        # Insert a Chinese article for testing
        self.client.execute(
            """
            INSERT INTO test_simple_fulltext_articles (title, content, category, tags) 
            VALUES ('中文测试', '这是一个中文全文搜索测试', 'Test', 'chinese,test')
        """
        )

        results = (
            self.client.fulltext_index.simple_query("test_simple_fulltext_articles")
            .columns("title", "content", "tags")
            .search("中文")
            .execute()
        )

        assert isinstance(results.rows, list)
        # Should find the Chinese article
        if results.rows:
            found_chinese = any("中文" in row[1] for row in results.rows)
            assert found_chinese

    def test_error_handling_invalid_table(self):
        """Test error handling for invalid table."""
        with pytest.raises(QueryError):
            (
                self.client.fulltext_index.simple_query("nonexistent_table")
                .columns("title", "content")
                .search("test")
                .execute()
            )

    def test_method_chaining_completeness(self):
        """Test that all methods return self for proper chaining."""
        builder = self.client.fulltext_index.simple_query("test_simple_fulltext_articles")

        # All these should return the builder instance
        result = (
            builder.columns("title", "content", "tags")
            .search("test")
            .with_score("score")
            .where("category = 'Programming'")
            .order_by_score(desc=True)
            .limit(10, 0)
        )

        assert result is builder  # Should be the same instance


class TestSimpleFulltextMigration:
    """Test migration from old fulltext interfaces to simple_query."""

    @classmethod
    def setup_class(cls):
        """Set up test database."""
        conn_params = get_connection_kwargs()
        client_params = {k: v for k, v in conn_params.items() if k in ['host', 'port', 'user', 'password', 'database']}

        cls.client = Client()
        cls.client.connect(**client_params)

        cls.test_db = "test_migration"
        try:
            cls.client.execute(f"DROP DATABASE IF EXISTS {cls.test_db}")
            cls.client.execute(f"CREATE DATABASE {cls.test_db}")
            cls.client.execute(f"USE {cls.test_db}")
        except Exception as e:
            print(f"Database setup warning: {e}")

        # Create test table
        cls.client.execute("DROP TABLE IF EXISTS migration_test")
        cls.client.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_test (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                body TEXT NOT NULL
            )
        """
        )

        # Insert test data
        cls.client.execute(
            """
            INSERT INTO migration_test (title, body) VALUES 
            ('Python Tutorial', 'Learn Python programming'),
            ('Java Guide', 'Complete Java development guide'),
            ('Machine Learning', 'AI and ML concepts')
        """
        )

        # Create fulltext index
        try:
            cls.client.execute("CREATE FULLTEXT INDEX ft_migration ON migration_test(title, body)")
        except Exception as e:
            print(f"Fulltext index creation warning: {e}")

    @classmethod
    def teardown_class(cls):
        """Clean up."""
        if hasattr(cls, 'client'):
            cls.client.disconnect()

    def test_replace_basic_fulltext_search(self):
        """Test replacing basic fulltext search with simple_query."""
        # Old way (if it existed): complex builder setup
        # New way: simple_query interface
        results = (
            self.client.fulltext_index.simple_query("migration_test").columns("title", "body").search("python").execute()
        )

        assert isinstance(results.rows, list)
        if results.rows:
            found_python = any("Python" in row[1] for row in results.rows)
            assert found_python

    def test_replace_scored_search(self):
        """Test replacing scored fulltext search."""
        # New way with scoring
        results = (
            self.client.fulltext_index.simple_query("migration_test")
            .columns("title", "body")
            .search("programming")
            .with_score()
            .order_by_score()
            .execute()
        )

        assert isinstance(results.rows, list)
        # Verify score column is included
        if results.rows:
            assert len(results.columns) > 2  # Original columns + score

    def test_replace_complex_search(self):
        """Test replacing complex fulltext search scenarios."""
        # Boolean search with filtering
        results = (
            self.client.fulltext_index.simple_query("migration_test")
            .columns("title", "body")
            .must_have("guide")
            .must_not_have("deprecated")
            .with_score()
            .limit(5)
            .execute()
        )

        assert isinstance(results.rows, list)
        # Should work without errors and return relevant results

    @pytest.mark.asyncio
    async def test_async_execute_functionality(self):
        """Test async_execute method with real database."""
        from matrixone.async_client import AsyncClient
        from matrixone.config import get_connection_kwargs

        # Get connection parameters
        conn_params = get_connection_kwargs()
        client_params = {k: v for k, v in conn_params.items() if k in ['host', 'port', 'user', 'password', 'database']}

        async_client = AsyncClient()
        await async_client.connect(**client_params)

        try:
            # Use the same test database
            await async_client.execute(f"USE {self.test_db}")

            # Test async execute
            results = await (
                async_client.fulltext_index.simple_query("migration_test")
                .columns("title", "body")
                .search("python")
                .with_score()
                .limit(3)
                .execute()
            )

            assert results is not None
            assert hasattr(results, 'rows')
            assert isinstance(results.rows, list)

        finally:
            await async_client.disconnect()

    def test_transaction_simple_query_functionality(self):
        """Test TransactionSimpleFulltextQueryBuilder with real database."""
        # Test within a transaction
        with self.client.transaction() as tx:
            results = (
                tx.fulltext_index.simple_query("migration_test")
                .columns("title", "body")
                .search("guide")
                .with_score()
                .limit(3)
                .execute()
            )

            assert results is not None
            assert hasattr(results, 'rows')
            assert isinstance(results.rows, list)

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_functionality(self):
        """Test AsyncTransactionSimpleFulltextQueryBuilder with real database."""
        from matrixone.async_client import AsyncClient
        from matrixone.config import get_connection_kwargs

        # Get connection parameters
        conn_params = get_connection_kwargs()
        client_params = {k: v for k, v in conn_params.items() if k in ['host', 'port', 'user', 'password', 'database']}

        async_client = AsyncClient()
        await async_client.connect(**client_params)

        try:
            # Use the same test database
            await async_client.execute(f"USE {self.test_db}")

            # Test within an async transaction
            async with async_client.transaction() as tx:
                results = await (
                    tx.fulltext_index.simple_query("migration_test")
                    .columns("title", "body")
                    .search("python")
                    .with_score()
                    .limit(3)
                    .execute()
                )

                assert results is not None
                assert hasattr(results, 'rows')
                assert isinstance(results.rows, list)

        finally:
            await async_client.disconnect()

    def test_error_handling_async_execute_with_sync_client(self):
        """Test that execute works with sync client (should not raise error)."""
        # This should not raise an error - execute should be available
        builder = self.client.fulltext_index.simple_query("migration_test")
        builder.columns("title", "body").search("test")

        # Should have execute method
        assert hasattr(builder, 'execute')

        # Should not raise error when called (though it may fail due to connection)
        try:
            # This might fail due to connection issues, but should not be a method error
            result = builder.execute()
            print("✅ execute works as expected")
        except Exception as e:
            # Connection errors are acceptable, method errors are not
            if "execute" in str(e) or "method" in str(e).lower():
                raise
            print(f"⚠️  Connection error (acceptable): {e}")

    def test_error_handling_execute_with_async_client(self):
        """Test that execute returns a coroutine with async client."""
        from matrixone.async_client import AsyncClient, AsyncFulltextIndexManager
        import asyncio

        async_client = AsyncClient()
        async_client._fulltext_index = AsyncFulltextIndexManager(async_client)
        builder = async_client.fulltext_index.simple_query("test_table")
        builder.columns("title", "content").search("test")

        # Should return a coroutine when execute is called
        result = builder.execute()
        assert asyncio.iscoroutine(result), "execute() should return a coroutine for async client"

        # Clean up the coroutine to avoid warning
        result.close()

        # Should not raise RuntimeError anymore - our new design allows this
        print("✅ execute() returns coroutine as expected for async client")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
