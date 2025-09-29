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

Online tests for AsyncClient SimpleFulltextQueryBuilder functionality.

Tests async simple_query methods against a real MatrixOne database to verify
that generated SQL works correctly and returns expected results.
"""

import pytest
import pytest_asyncio
from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import declarative_base

from matrixone.async_client import AsyncClient
from matrixone.config import get_connection_kwargs
from matrixone.exceptions import QueryError

# SQLAlchemy model for testing
Base = declarative_base()


class AsyncArticle(Base):
    __tablename__ = "test_async_simple_query_articles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(255), nullable=False)
    content = Column(Text, nullable=False)
    category = Column(String(100), nullable=False)
    tags = Column(String(500))
    author = Column(String(100))
    published_date = Column(String(20))


class TestAsyncSimpleQueryOnline:
    """Online tests for AsyncSimpleFulltextQueryBuilder with real database."""

    @pytest_asyncio.fixture(scope="function")
    async def async_client_setup(self):
        """Set up test database and data."""
        # Get connection parameters
        conn_params = get_connection_kwargs()
        client_params = {k: v for k, v in conn_params.items() if k in ['host', 'port', 'user', 'password', 'database']}

        client = AsyncClient()
        await client.connect(**client_params)

        test_db = "test_async_simple_query"

        # Enable experimental fulltext index
        try:
            await client.execute("SET experimental_fulltext_index=1")
        except Exception as e:
            print(f"Fulltext index setup warning: {e}")

        test_db = "test_async_simple_query"
        try:
            await client.execute(f"DROP DATABASE IF EXISTS {test_db}")
            await client.execute(f"CREATE DATABASE {test_db}")
            await client.execute(f"USE {test_db}")
        except Exception as e:
            print(f"Database setup warning: {e}")

        # Create test table
        await client.execute("DROP TABLE IF EXISTS test_async_simple_query_articles")
        await client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_async_simple_query_articles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                category VARCHAR(100) NOT NULL,
                tags VARCHAR(500),
                author VARCHAR(100),
                published_date VARCHAR(20)
            )
        """
        )

        # Insert test data
        test_articles = [
            (
                1,
                "Python Async Programming",
                "Learn Python async programming with asyncio and async/await",
                "Programming",
                "python,async,asyncio,programming",
                "Alice",
                "2024-01-15",
            ),
            (
                2,
                "Machine Learning with Python",
                "Complete guide to machine learning algorithms and implementation",
                "AI",
                "python,ml,ai,algorithms",
                "Bob",
                "2024-01-20",
            ),
            (
                3,
                "JavaScript Async Development",
                "Modern JavaScript development with async/await and promises",
                "Programming",
                "javascript,async,promises,web",
                "Charlie",
                "2024-01-25",
            ),
            (
                4,
                "Data Science Fundamentals",
                "Introduction to data science concepts and tools",
                "AI",
                "data,science,analytics,python",
                "David",
                "2024-02-01",
            ),
            (
                5,
                "Deprecated Python Libraries",
                "Old Python libraries that should be avoided",
                "Programming",
                "python,deprecated,legacy",
                "Eve",
                "2024-02-05",
            ),
            (
                6,
                "Advanced Machine Learning",
                "Deep learning and neural networks with TensorFlow",
                "AI",
                "ai,deeplearning,tensorflow,python",
                "Frank",
                "2024-02-10",
            ),
            (
                7,
                "Web Development Best Practices",
                "Modern practices for building web applications",
                "Programming",
                "web,best practices,modern,async",
                "Grace",
                "2024-02-15",
            ),
            (
                8,
                "Artificial Intelligence Overview",
                "Introduction to AI concepts and applications",
                "AI",
                "ai,overview,introduction",
                "Henry",
                "2024-02-20",
            ),
            (
                9,
                "Async Database Operations",
                "How to perform async database operations efficiently",
                "Database",
                "async,database,operations,performance",
                "Ivy",
                "2024-02-25",
            ),
            (
                10,
                "Python Web Frameworks",
                "Comparison of Python web frameworks including async support",
                "Programming",
                "python,web,frameworks,async,fastapi",
                "Jack",
                "2024-03-01",
            ),
        ]

        for article in test_articles:
            title_escaped = article[1].replace("'", "''")
            content_escaped = article[2].replace("'", "''")
            category_escaped = article[3].replace("'", "''")
            tags_escaped = article[4].replace("'", "''") if article[4] else ''
            author_escaped = article[5].replace("'", "''")
            date_escaped = article[6].replace("'", "''")

            await client.execute(
                f"""
                INSERT INTO test_async_simple_query_articles (id, title, content, category, tags, author, published_date) 
                VALUES ({article[0]}, '{title_escaped}', '{content_escaped}', '{category_escaped}', '{tags_escaped}', '{author_escaped}', '{date_escaped}')
            """
            )

        # Create fulltext index - use only the columns that simple_query will search
        try:
            await client.execute(
                "CREATE FULLTEXT INDEX ft_async_articles ON test_async_simple_query_articles(title, content)"
            )
        except Exception as e:
            print(f"Fulltext index creation warning: {e}")

        yield client, test_db

        # Cleanup
        try:
            await client.execute(f"DROP DATABASE IF EXISTS {test_db}")
            await client.disconnect()
        except Exception as e:
            print(f"Cleanup warning: {e}")

    @pytest.mark.asyncio
    async def test_async_basic_natural_language_search(self, async_client_setup):
        """Test basic natural language search with async client."""
        client, test_db = async_client_setup
        results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
            .search("python")
            .execute()
        )

        assert isinstance(results.rows, list)
        assert len(results.rows) > 0

        # Should find Python-related articles
        found_python = any("Python" in row[1] for row in results.rows)  # title column
        assert found_python, "Should find articles containing 'Python'"

    @pytest.mark.asyncio
    async def test_async_boolean_mode_required_terms(self, async_client_setup):
        """Test boolean mode with required terms using async client."""
        client, test_db = async_client_setup
        results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
            .must_have("python", "async")
            .execute()
        )

        assert isinstance(results.rows, list)
        # Should find articles that have both "python" AND "async"
        if results.rows:
            for row in results.rows:
                title_content = f"{row[1]} {row[2]}".lower()  # title + content
                assert "python" in title_content and "async" in title_content

    @pytest.mark.asyncio
    async def test_async_boolean_mode_excluded_terms(self, async_client_setup):
        """Test boolean mode with excluded terms using async client."""
        client, test_db = async_client_setup
        results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
            .must_have("python")
            .must_not_have("deprecated")
            .execute()
        )

        assert isinstance(results.rows, list)
        # Should find articles but exclude deprecated ones
        for row in results.rows:
            title_content = f"{row[1]} {row[2]}".lower()  # title + content
            assert "deprecated" not in title_content

    @pytest.mark.asyncio
    async def test_async_search_with_score(self, async_client_setup):
        """Test search with relevance scoring using async client."""
        client, test_db = async_client_setup
        results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
            .search("machine learning")
            .with_score()
            .execute()
        )

        assert isinstance(results.rows, list)
        assert len(results.rows) > 0

        # Check that score column is included
        assert len(results.columns) > 7  # Original 7 columns + score
        score_column_index = len(results.columns) - 1  # Score should be last column

        # Verify score values are numeric
        for row in results.rows:
            score = row[score_column_index]
            assert isinstance(score, (int, float)), f"Score should be numeric, got {type(score)}"
            assert score >= 0, f"Score should be non-negative, got {score}"

    @pytest.mark.asyncio
    async def test_async_search_with_custom_score_alias(self, async_client_setup):
        """Test search with custom score alias using async client."""
        client, test_db = async_client_setup
        results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
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

    @pytest.mark.asyncio
    async def test_async_search_with_where_conditions(self, async_client_setup):
        """Test search with additional WHERE conditions using async client."""
        client, test_db = async_client_setup
        results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
            .search("programming")
            .where("category = 'Programming'")
            .execute()
        )

        assert isinstance(results.rows, list)
        # All results should be in Programming category
        for row in results.rows:
            category = row[3]  # category column
            assert category == "Programming"

    @pytest.mark.asyncio
    async def test_async_search_with_ordering_and_limit(self, async_client_setup):
        """Test search with ordering and pagination using async client."""
        client, test_db = async_client_setup
        results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
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

    @pytest.mark.asyncio
    async def test_async_search_by_category(self, async_client_setup):
        """Test filtering by category using async client."""
        client, test_db = async_client_setup
        # Search AI category
        ai_results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
            .search("learning")
            .where("category = 'AI'")
            .execute()
        )

        # Search Programming category
        prog_results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
            .search("programming")
            .where("category = 'Programming'")
            .execute()
        )

        # Verify results are from correct categories
        for row in ai_results.rows:
            assert row[3] == "AI"

        for row in prog_results.rows:
            assert row[3] == "Programming"

    @pytest.mark.asyncio
    async def test_async_complex_boolean_search(self, async_client_setup):
        """Test complex boolean search with multiple terms using async client."""
        client, test_db = async_client_setup
        results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
            .must_have("web")
            .must_not_have("deprecated", "legacy")
            .where("category = 'Programming'")
            .execute()
        )

        assert isinstance(results.rows, list)
        # Should find articles without deprecated content
        for row in results.rows:
            title_content = f"{row[1]} {row[2]}".lower()
            assert "deprecated" not in title_content
            assert "legacy" not in title_content
            assert row[3] == "Programming"

    @pytest.mark.asyncio
    async def test_async_empty_search_results(self, async_client_setup):
        """Test search that should return no results using async client."""
        client, test_db = async_client_setup
        results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
            .search("nonexistent_term_xyz123")
            .execute()
        )

        assert isinstance(results.rows, list)
        assert len(results.rows) == 0

    @pytest.mark.asyncio
    async def test_async_chinese_text_search(self, async_client_setup):
        """Test search with non-English text using async client."""
        # Insert a Chinese article for testing
        client, test_db = async_client_setup
        await client.execute(
            """
            INSERT INTO test_async_simple_query_articles (title, content, category, tags) 
            VALUES ('中文异步测试', '这是一个中文异步全文搜索测试', 'Test', 'chinese,async,test')
        """
        )

        results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
            .search("中文")
            .execute()
        )

        assert isinstance(results.rows, list)
        # Should find the Chinese article
        if results.rows:
            found_chinese = any("中文" in row[1] for row in results.rows)
            assert found_chinese

    @pytest.mark.asyncio
    async def test_async_error_handling_invalid_table(self, async_client_setup):
        """Test error handling for invalid table using async client."""
        client, test_db = async_client_setup
        with pytest.raises(QueryError):
            await (
                client.fulltext_index.simple_query("nonexistent_table").columns("title", "content").search("test").execute()
            )

    @pytest.mark.asyncio
    async def test_async_method_chaining_completeness(self, async_client_setup):
        """Test that all methods return self for proper chaining using async client."""
        client, test_db = async_client_setup
        builder = client.fulltext_index.simple_query("test_async_simple_query_articles")

        # All these should return the builder instance
        result = (
            builder.columns("title", "content")
            .search("test")
            .with_score("score")
            .where("category = 'Programming'")
            .order_by_score(desc=True)
            .limit(10, 0)
        )

        assert result is builder  # Should be the same instance

    @pytest.mark.asyncio
    async def test_async_basic_search_with_columns(self, async_client_setup):
        """Test basic search with specific columns using async client."""
        client, test_db = async_client_setup
        results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
            .search("python")
            .execute()
        )

        assert isinstance(results.rows, list)
        assert len(results.rows) > 0
        # Verify we get results (SimpleFulltextQueryBuilder uses SELECT *)
        assert len(results.columns) > 0

    @pytest.mark.asyncio
    async def test_async_search_programming(self, async_client_setup):
        """Test search for programming-related content using async client."""
        client, test_db = async_client_setup
        results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
            .search("programming")
            .execute()
        )

        assert isinstance(results.rows, list)
        assert len(results.rows) > 0
        # Verify we get programming-related results
        for row in results.rows:
            title_content = f"{row[0]} {row[1]}".lower()
            assert "programming" in title_content

    @pytest.mark.asyncio
    async def test_async_limit_with_offset(self, async_client_setup):
        """Test limit with offset using async client."""
        client, test_db = async_client_setup
        # Get first 3 results
        first_results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
            .search("python")
            .limit(3, 0)
            .execute()
        )

        # Get next 3 results (offset 3)
        next_results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
            .search("python")
            .limit(3, 3)
            .execute()
        )

        assert isinstance(first_results.rows, list)
        assert isinstance(next_results.rows, list)

        # Results should be different (if there are enough results)
        if len(first_results.rows) > 0 and len(next_results.rows) > 0:
            assert first_results.rows[0] != next_results.rows[0], "Offset should return different results"

    @pytest.mark.asyncio
    async def test_async_multiple_where_conditions(self, async_client_setup):
        """Test multiple WHERE conditions using async client."""
        client, test_db = async_client_setup
        results = await (
            client.fulltext_index.simple_query("test_async_simple_query_articles")
            .columns("title", "content")
            .search("python")
            .where("category = 'Programming'")
            .execute()
        )

        assert isinstance(results.rows, list)
        # All results should match the category condition
        for row in results.rows:
            assert row[3] == "Programming"  # category

    @pytest.mark.asyncio
    async def test_async_explain_query(self, async_client_setup):
        """Test explain functionality using async client."""
        client, test_db = async_client_setup
        builder = client.fulltext_index.simple_query("test_async_simple_query_articles")
        builder.columns("title", "content").search("test").with_score().where("category = 'Programming'")

        sql = builder.explain()
        assert isinstance(sql, str)
        assert "SELECT" in sql.upper()
        assert "MATCH" in sql.upper()
        assert "AGAINST" in sql.upper()

    @pytest.mark.asyncio
    async def test_async_concurrent_searches(self, async_client_setup):
        """Test concurrent async searches."""
        import asyncio

        client, test_db = async_client_setup

        # Create multiple search tasks using database prefix to avoid connection pool issues
        async def search_task(search_term):
            # Use database prefix instead of USE statement to avoid connection pool issues
            table_name = f"{test_db}.test_async_simple_query_articles"
            return await (
                client.fulltext_index.simple_query(table_name).columns("title", "content").search(search_term).execute()
            )

        # Execute all searches concurrently
        tasks = [search_task("python"), search_task("async"), search_task("machine learning")]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check that all searches completed (either successfully or with expected exceptions)
        assert len(results) == 3
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                print(f"Search {i} failed with: {result}")
                # If it's a table not found error, that's the issue we're investigating
                if "does not exist" in str(result):
                    pytest.fail(f"Table not found during concurrent search {i}: {result}")
            else:
                assert isinstance(result.rows, list)


class TestAsyncTransactionSimpleQueryOnline:
    """Online tests for AsyncTransactionSimpleFulltextQueryBuilder with real database."""

    @pytest_asyncio.fixture(scope="function")
    async def async_tx_client_setup(self):
        """Set up test database and data."""
        # Get connection parameters
        conn_params = get_connection_kwargs()
        client_params = {k: v for k, v in conn_params.items() if k in ['host', 'port', 'user', 'password', 'database']}

        client = AsyncClient()
        await client.connect(**client_params)

        test_db = "test_async_tx_simple_query"

        # Enable experimental fulltext index
        try:
            await client.execute("SET experimental_fulltext_index=1")
        except Exception as e:
            print(f"Fulltext index setup warning: {e}")

        test_db = "test_async_tx_simple_query"
        try:
            await client.execute(f"DROP DATABASE IF EXISTS {test_db}")
            await client.execute(f"CREATE DATABASE {test_db}")
            await client.execute(f"USE {test_db}")
        except Exception as e:
            print(f"Database setup warning: {e}")

        # Create test table
        await client.execute("DROP TABLE IF EXISTS test_async_tx_articles")
        await client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_async_tx_articles (
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
                "Transaction Python Guide",
                "Learn Python database transactions",
                "Programming",
                "python,transaction,database",
            ),
            (
                2,
                "Async Transaction Patterns",
                "Modern async transaction patterns",
                "Programming",
                "async,transaction,patterns",
            ),
            (
                3,
                "Database Transaction Best Practices",
                "Best practices for database transactions",
                "Database",
                "database,transaction,best practices",
            ),
            (
                4,
                "Python Async Database",
                "Async database operations in Python",
                "Programming",
                "python,async,database",
            ),
            (
                5,
                "Transaction Isolation Levels",
                "Understanding transaction isolation levels",
                "Database",
                "transaction,isolation,levels",
            ),
        ]

        for article in test_articles:
            title_escaped = article[1].replace("'", "''")
            content_escaped = article[2].replace("'", "''")
            category_escaped = article[3].replace("'", "''")
            tags_escaped = article[4].replace("'", "''") if article[4] else ''

            await client.execute(
                f"""
                INSERT INTO test_async_tx_articles (id, title, content, category, tags) 
                VALUES ({article[0]}, '{title_escaped}', '{content_escaped}', '{category_escaped}', '{tags_escaped}')
            """
            )

        # Create fulltext index - use only the columns that simple_query will search
        try:
            await client.execute("CREATE FULLTEXT INDEX ft_async_tx_articles ON test_async_tx_articles(title, content)")
        except Exception as e:
            print(f"Fulltext index creation warning: {e}")

        yield client, test_db

        # Cleanup
        try:
            await client.execute(f"DROP DATABASE IF EXISTS {test_db}")
            await client.disconnect()
        except Exception as e:
            print(f"Cleanup warning: {e}")

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_basic(self, async_tx_client_setup):
        """Test basic async transaction simple query."""
        client, test_db = async_tx_client_setup
        async with client.transaction() as tx:
            results = await (
                tx.fulltext_index.simple_query("test_async_tx_articles")
                .columns("title", "content")
                .search("python")
                .execute()
            )

            assert isinstance(results.rows, list)
            assert len(results.rows) > 0

            # Should find Python transaction related articles
            found_python = any("Python" in row[1] for row in results.rows)
            assert found_python

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_with_score(self, async_tx_client_setup):
        """Test async transaction simple query with scoring."""
        client, test_db = async_tx_client_setup
        async with client.transaction() as tx:
            results = await (
                tx.fulltext_index.simple_query("test_async_tx_articles")
                .columns("title", "content")
                .search("async database")
                .with_score()
                .order_by_score()
                .execute()
            )

            assert isinstance(results.rows, list)
            assert len(results.rows) > 0

            # Check that score column is included
            assert len(results.columns) > 5  # Original 5 columns + score
            score_column_index = len(results.columns) - 1

            # Verify score values are numeric
            for row in results.rows:
                score = row[score_column_index]
                assert isinstance(score, (int, float))
                assert score >= 0

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_boolean(self, async_tx_client_setup):
        """Test async transaction simple query with boolean mode."""
        client, test_db = async_tx_client_setup
        async with client.transaction() as tx:
            results = await (
                tx.fulltext_index.simple_query("test_async_tx_articles")
                .columns("title", "content")
                .must_have("transaction")
                .must_not_have("deprecated")
                .where("category = 'Programming'")
                .execute()
            )

            assert isinstance(results.rows, list)
            # Should find transaction articles in Programming category without deprecated content
            for row in results.rows:
                title_content = f"{row[1]} {row[2]}".lower()
                assert "transaction" in title_content
                assert "deprecated" not in title_content
                assert row[3] == "Programming"

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_complex(self, async_tx_client_setup):
        """Test complex async transaction simple query with multiple conditions."""
        client, test_db = async_tx_client_setup
        async with client.transaction() as tx:
            results = await (
                tx.fulltext_index.simple_query("test_async_tx_articles")
                .columns("title", "content")
                .search("database")
                .with_score("relevance")
                .where("category IN ('Programming', 'Database')")
                .order_by_score(desc=True)
                .limit(3)
                .execute()
            )

            assert isinstance(results.rows, list)
            assert len(results.rows) <= 3

            # All results should be in specified categories
            for row in results.rows:
                assert row[3] in ['Programming', 'Database']

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_explain(self, async_tx_client_setup):
        """Test explain functionality in async transaction."""
        client, test_db = async_tx_client_setup
        async with client.transaction() as tx:
            builder = tx.fulltext_index.simple_query("test_async_tx_articles")
            builder.columns("title", "content").search("test").with_score().where("category = 'Programming'")

            sql = builder.explain()
            assert isinstance(sql, str)
            assert "SELECT" in sql.upper()
            assert "MATCH" in sql.upper()
            assert "AGAINST" in sql.upper()

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_error_handling(self, async_tx_client_setup):
        """Test error handling in async transaction simple query."""
        client, test_db = async_tx_client_setup
        async with client.transaction() as tx:
            with pytest.raises(QueryError):
                await (
                    tx.fulltext_index.simple_query("nonexistent_table").columns("title", "content").search("test").execute()
                )

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_method_chaining(self, async_tx_client_setup):
        """Test method chaining in async transaction simple query."""
        client, test_db = async_tx_client_setup
        async with client.transaction() as tx:
            builder = tx.fulltext_index.simple_query("test_async_tx_articles")

            # All these should return the builder instance
            result = (
                builder.columns("title", "content")
                .search("test")
                .with_score("score")
                .where("category = 'Programming'")
                .order_by_score(desc=True)
                .limit(10, 0)
            )

            assert result is builder  # Should be the same instance


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
