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
Test async transaction simple_query functionality
"""

import pytest
import pytest_asyncio
from matrixone import AsyncClient
from matrixone.sqlalchemy_ext import boolean_match, natural_match


class TestAsyncTransactionSimpleQuery:
    """Test async transaction simple_query operations"""

    @pytest_asyncio.fixture(scope="function")
    async def async_client_setup(self):
        """Setup async client for testing"""
        client = AsyncClient()
        await client.connect(host="127.0.0.1", port=6001, user="root", password="111", database="test")

        # Enable fulltext indexing
        await client.execute("SET experimental_fulltext_index=1")

        # Create test database and table
        test_db = "async_tx_simple_query_test"
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
    async def test_async_transaction_simple_query_basic(self, async_client_setup):
        """Test basic async transaction simple_query functionality"""
        client, test_db = async_client_setup

        async with client.transaction() as tx:
            # Test basic search within transaction
            result = await tx.query("async_tx_docs").filter(boolean_match("title", "content").encourage("python")).execute()

            assert result is not None
            rows = result.fetchall()
            assert len(rows) > 0

            # Verify results contain "python"
            for row in rows:
                title_content = f"{row[0]} {row[1]}".lower()  # title and content
                assert "python" in title_content

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_with_score(self, async_client_setup):
        """Test async transaction simple_query with score"""
        client, test_db = async_client_setup

        async with client.transaction() as tx:
            # Test search with score within transaction
            result = await tx.query(
                "async_tx_docs.title",
                "async_tx_docs.content",
                boolean_match("title", "content").encourage("async").label("score"),
            ).execute()

            assert result is not None
            rows = result.fetchall()
            assert len(rows) > 0

            # Verify results have score column
            assert "score" in result.columns
            score_column_index = result.columns.index("score")

            # Verify score values are numeric
            for row in rows:
                score = row[score_column_index]
                assert isinstance(score, (int, float))
                assert score >= 0

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_boolean_mode(self, async_client_setup):
        """Test async transaction simple_query with boolean mode"""
        client, test_db = async_client_setup

        async with client.transaction() as tx:
            # Test boolean mode search within transaction
            result = (
                await tx.query("async_tx_docs.title", "async_tx_docs.content")
                .filter(boolean_match("title", "content").must("async").must_not("basic"))
                .execute()
            )

            assert result is not None
            rows = result.fetchall()
            assert len(rows) > 0

            # Verify all results contain "async" but not "basic"
            for row in rows:
                title_content = f"{row[0]} {row[1]}".lower()  # title and content
                assert "async" in title_content
                assert "basic" not in title_content

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_with_where(self, async_client_setup):
        """Test async transaction simple_query with WHERE conditions"""
        client, test_db = async_client_setup

        async with client.transaction() as tx:
            # Test search with WHERE conditions within transaction
            result = (
                await tx.query("async_tx_docs.title", "async_tx_docs.content")
                .filter(boolean_match("title", "content").encourage("programming"), "async_tx_docs.category = 'Programming'")
                .execute()
            )

            assert result is not None
            rows = result.fetchall()
            assert len(rows) > 0

            # Verify all results contain "programming"
            for row in rows:
                title_content = f"{row[0]} {row[1]}".lower()
                assert "programming" in title_content

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_ordering_and_limit(self, async_client_setup):
        """Test async transaction simple_query with ordering and limit"""
        client, test_db = async_client_setup

        async with client.transaction() as tx:
            # Test search with ordering and limit within transaction
            result = (
                await tx.query("async_tx_docs.title", "async_tx_docs.content")
                .filter(boolean_match("title", "content").encourage("async"))
                .order_by("async_tx_docs.title DESC")
                .limit(2)
                .execute()
            )

            assert result is not None
            rows = result.fetchall()
            assert len(rows) <= 2

            # Verify results are ordered by title (descending)
            titles = [row[0] for row in rows]  # title column
            assert titles == sorted(titles, reverse=True)

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_explain(self, async_client_setup):
        """Test async transaction simple_query explain functionality"""
        client, test_db = async_client_setup

        async with client.transaction() as tx:
            # Test basic query functionality within transaction
            result = (
                await tx.query("async_tx_docs.title", "async_tx_docs.content")
                .filter(boolean_match("title", "content").encourage("test"), "async_tx_docs.category = 'Testing'")
                .execute()
            )

            assert result is not None
            rows = result.fetchall()
            # Should find testing-related content
            assert len(rows) > 0, "Should find testing-related content"

            # Verify all results contain "test" in title or content
            for row in rows:
                title_content = f"{row[0]} {row[1]}".lower()
                assert "test" in title_content, f"Row should contain 'test': {row}"

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_multiple_operations(self, async_client_setup):
        """Test multiple async transaction simple_query operations in one transaction"""
        client, test_db = async_client_setup

        async with client.transaction() as tx:
            # Test multiple searches within the same transaction
            results = []

            # Search 1: Basic search
            result1 = await tx.query(
                "async_tx_docs.title", "async_tx_docs.content", boolean_match("title", "content").encourage("python")
            ).execute()
            results.append(result1)

            # Search 2: Boolean mode
            result2 = await tx.query(
                "async_tx_docs.title", "async_tx_docs.content", boolean_match("title", "content").must("javascript")
            ).execute()
            results.append(result2)

            # Search 3: With score
            result3 = await tx.query(
                "async_tx_docs.title",
                "async_tx_docs.content",
                boolean_match("title", "content").encourage("database").label("score"),
            ).execute()
            results.append(result3)

            # Verify all searches returned results
            for i, result in enumerate(results):
                assert result is not None
                rows = result.fetchall()
                assert len(rows) > 0, f"Search {i+1} should return results"

                # Verify content matches search terms
                search_terms = ["python", "javascript", "database"]
                term = search_terms[i]
                for row in rows:
                    title_content = f"{row[0]} {row[1]}".lower()
                    assert term in title_content, f"Row should contain '{term}': {row}"

    @pytest.mark.asyncio
    async def test_async_transaction_simple_query_with_database_prefix(self, async_client_setup):
        """Test async transaction simple_query with database prefix"""
        client, test_db = async_client_setup

        async with client.transaction() as tx:
            # Test search with database prefix within transaction
            result = await tx.query(
                f"{test_db}.async_tx_docs.title",
                f"{test_db}.async_tx_docs.content",
                boolean_match("title", "content").encourage("web"),
            ).execute()

            assert result is not None
            rows = result.fetchall()
            assert len(rows) > 0

            # Verify results contain "web"
            for row in rows:
                title_content = f"{row[0]} {row[1]}".lower()  # title and content
                assert "web" in title_content
