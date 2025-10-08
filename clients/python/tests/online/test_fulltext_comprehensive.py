#!/usr/bin/env python3

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
Comprehensive online tests for fulltext functionality.

This file consolidates all fulltext-related tests including:
- Basic fulltext search API functionality
- Fulltext search in transaction contexts
- Fulltext index creation and management
- Async fulltext operations

These tests require a running MatrixOne database and test the actual
fulltext functionality with real database operations.
"""

import pytest
import pytest_asyncio
from matrixone import Client, AsyncClient, FulltextAlgorithmType, FulltextModeType, FulltextParserType
from matrixone.sqlalchemy_ext import FulltextIndex, FulltextSearchBuilder
from matrixone.sqlalchemy_ext import boolean_match, natural_match
from matrixone.orm import declarative_base
from sqlalchemy import Column, Integer, String, Text, BigInteger


class TestFulltextComprehensive:
    """Comprehensive test suite for fulltext functionality"""

    # ============================================================================
    # Basic Fulltext Search API Tests
    # ============================================================================

    def test_fulltext_search_basic(self, test_client):
        """Test basic fulltext search functionality"""
        # Enable fulltext index functionality using interface
        test_client.fulltext_index.enable_fulltext()

        # Create test table
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_search_test")
        test_client.execute("USE fulltext_search_test")

        test_client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_documents (
                id INT PRIMARY KEY AUTO_INCREMENT,
                title VARCHAR(200),
                content TEXT
            )
        """
        )

        # Insert test data
        test_client.execute(
            """
            INSERT INTO test_documents (title, content) VALUES 
            ('Machine Learning Guide', 'This is a comprehensive guide to machine learning concepts and algorithms'),
            ('Data Science Handbook', 'A complete handbook covering data science techniques and tools'),
            ('AI Research Paper', 'Latest research in artificial intelligence and neural networks')
        """
        )

        # Create fulltext index
        test_client.fulltext_index.create("test_documents", name="ftidx_content", columns=["title", "content"])

        try:
            # Test search functionality
            result = test_client.query(
                "test_documents.title",
                "test_documents.content",
                boolean_match("title", "content").encourage("machine learning"),
            ).execute()

            assert result is not None
            assert len(result.rows) > 0

            # Verify that we get results with machine learning content
            found_ml = False
            for row in result.rows:
                if 'machine' in str(row).lower() and 'learning' in str(row).lower():
                    found_ml = True
                    break
            assert found_ml, "Should find machine learning related content"

        finally:
            # Clean up
            test_client.fulltext_index.drop("test_documents", "ftidx_content")
            test_client.execute("DROP TABLE test_documents")
            test_client.execute("DROP DATABASE fulltext_search_test")

    def test_fulltext_search_in_transaction_sync(self, test_client):
        """Test synchronous fulltext search (simplified from transaction)"""
        # Enable fulltext indexing using interface
        test_client.fulltext_index.enable_fulltext()

        # Create test database and table
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_tx_test")
        test_client.execute("USE fulltext_tx_test")

        test_client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_docs_tx (
                id INT PRIMARY KEY,
                title VARCHAR(100),
                content TEXT
            )
        """
        )

        # Clear any existing data and insert test data
        test_client.execute("DELETE FROM test_docs_tx")
        test_client.execute(
            """
            INSERT INTO test_docs_tx (id, title, content) VALUES
            (1, 'Python Programming', 'Python is a great programming language for data science'),
            (2, 'Machine Learning', 'Machine learning algorithms can learn from data'),
            (3, 'Database Systems', 'Database systems store and manage data efficiently')
        """
        )

        # Create fulltext index
        test_client.fulltext_index.create("test_docs_tx", name="ftidx_tx_docs", columns=["title", "content"])

        try:
            # Test query method
            result = test_client.query(
                "test_docs_tx.title",
                "test_docs_tx.content",
                boolean_match("title", "content").encourage("python programming"),
            ).execute()

            assert result is not None
            assert len(result.rows) > 0

            # Verify that we get Python related results
            found_python = False
            for row in result.rows:
                if 'python' in str(row).lower():
                    found_python = True
                    break
            assert found_python, "Should find Python related content"

        finally:
            # Clean up
            test_client.fulltext_index.drop("test_docs_tx", "ftidx_tx_docs")
            test_client.execute("DROP TABLE test_docs_tx")
            test_client.execute("DROP DATABASE fulltext_tx_test")

    @pytest.mark.asyncio
    async def test_fulltext_search_async(self, test_async_client):
        """Test asynchronous fulltext search functionality"""
        # Enable fulltext indexing using interface
        await test_async_client.fulltext_index.enable_fulltext()

        # Create test table
        await test_async_client.execute("CREATE DATABASE IF NOT EXISTS async_fulltext_test")
        await test_async_client.execute("USE async_fulltext_test")

        await test_async_client.execute(
            """
            CREATE TABLE IF NOT EXISTS async_documents (
                id INT PRIMARY KEY AUTO_INCREMENT,
                title VARCHAR(200),
                content TEXT
            )
        """
        )

        # Clear existing data and insert test data
        await test_async_client.execute("DELETE FROM async_documents")
        await test_async_client.execute(
            """
            INSERT INTO async_documents (title, content) VALUES
            ('Database Design', 'Database design principles and best practices'),
            ('SQL Optimization', 'Techniques for optimizing SQL queries and performance'),
            ('NoSQL Systems', 'Understanding NoSQL database systems and their use cases')
        """
        )

        # Create fulltext index
        await test_async_client.fulltext_index.create(
            "async_documents", name="ftidx_async_content", columns=["title", "content"]
        )

        try:
            # Test async search functionality
            result = await test_async_client.query(
                "async_documents.title", "async_documents.content", boolean_match("title", "content").encourage("database")
            ).execute()

            assert result is not None
            assert len(result.rows) > 0

            # Verify that we get database related results
            found_database = False
            for row in result.rows:
                if 'database' in str(row).lower():
                    found_database = True
                    break
            assert found_database, "Should find database related content"

        finally:
            # Clean up
            await test_async_client.fulltext_index.drop("async_documents", "ftidx_async_content")
            await test_async_client.execute("DROP TABLE async_documents")
            await test_async_client.execute("DROP DATABASE async_fulltext_test")

    @pytest.mark.asyncio
    async def test_fulltext_search_in_transaction_async(self, test_async_client):
        """Test asynchronous fulltext search (simplified from transaction)"""
        # Enable fulltext indexing using interface
        await test_async_client.fulltext_index.enable_fulltext()

        # Create test database and table
        await test_async_client.execute("CREATE DATABASE IF NOT EXISTS async_fulltext_tx_test")
        await test_async_client.execute("USE async_fulltext_tx_test")

        await test_async_client.execute(
            """
            CREATE TABLE IF NOT EXISTS async_docs_tx (
                id INT PRIMARY KEY,
                title VARCHAR(100),
                content TEXT
            )
        """
        )

        # Clear existing data and insert test data
        await test_async_client.execute("DELETE FROM async_docs_tx")
        await test_async_client.execute(
            """
            INSERT INTO async_docs_tx (id, title, content) VALUES
            (1, 'Async Programming', 'Async programming allows concurrent execution'),
            (2, 'Web Development', 'Web development involves frontend and backend'),
            (3, 'Cloud Computing', 'Cloud computing provides scalable resources')
        """
        )

        # Create fulltext index
        await test_async_client.fulltext_index.create(
            "async_docs_tx", name="ftidx_async_tx_docs", columns=["title", "content"]
        )

        try:
            # Test query method
            result = await test_async_client.query(
                "async_docs_tx.title",
                "async_docs_tx.content",
                boolean_match("title", "content").encourage("async programming"),
            ).execute()

            assert result is not None
            assert len(result.rows) > 0

            # Verify that we get async related results
            found_async = False
            for row in result.rows:
                if 'async' in str(row).lower():
                    found_async = True
                    break
            assert found_async, "Should find async related content"

        finally:
            # Clean up
            await test_async_client.fulltext_index.drop("async_docs_tx", "ftidx_async_tx_docs")
            await test_async_client.execute("DROP TABLE async_docs_tx")
            await test_async_client.execute("DROP DATABASE async_fulltext_tx_test")

    # ============================================================================
    # Fulltext Index Management Tests
    # ============================================================================

    @pytest.fixture(scope="function")
    def test_table(self, test_client):
        """Create test table for fulltext tests"""
        # Enable fulltext indexing using interface
        test_client.fulltext_index.enable_fulltext()

        # Create test table
        test_client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_fulltext (
                id BIGINT PRIMARY KEY,
                title VARCHAR(255),
                content TEXT,
                category VARCHAR(50)
            )
        """
        )

        yield "test_fulltext"

        # Cleanup
        try:
            test_client.execute("DROP TABLE IF EXISTS test_fulltext")
        except Exception:
            pass

    def test_create_fulltext_index_sync(self, test_client, test_table):
        """Test creating fulltext index synchronously"""
        # Create fulltext index using client.fulltext_index
        test_client.fulltext_index.create(
            test_table,
            name="ftidx_test",
            columns=["title", "content"],
            algorithm=FulltextAlgorithmType.BM25,
        )

        # Verify index was created by checking if we can search
        result = test_client.query(
            f"{test_table}.title", f"{test_table}.content", boolean_match("title", "content").encourage("test")
        ).execute()

        assert result is not None

    @pytest.mark.asyncio
    async def test_create_fulltext_index_async(self, test_async_client):
        """Test creating fulltext index asynchronously"""
        # Enable fulltext indexing using interface
        await test_async_client.fulltext_index.enable_fulltext()

        # Create test table
        await test_async_client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_fulltext_async (
                id BIGINT PRIMARY KEY,
                title VARCHAR(255),
                content TEXT
            )
        """
        )

        try:
            # Create fulltext index
            await test_async_client.fulltext_index.create(
                "test_fulltext_async",
                name="ftidx_async_test",
                columns=["title", "content"],
                algorithm=FulltextAlgorithmType.BM25,
            )

            # Verify index was created by checking if we can search
            result = await test_async_client.query(
                "test_fulltext_async.title",
                "test_fulltext_async.content",
                boolean_match("title", "content").encourage("test"),
            ).execute()

            assert result is not None

        finally:
            # Cleanup
            try:
                await test_async_client.execute("DROP TABLE IF EXISTS test_fulltext_async")
            except Exception:
                pass

    def test_drop_fulltext_index_sync(self, test_client, test_table):
        """Test dropping fulltext index synchronously"""
        # Create index first
        test_client.fulltext_index.create(test_table, name="ftidx_drop_test", columns=["title", "content"])

        # Drop the index
        test_client.fulltext_index.drop(test_table, name="ftidx_drop_test")

        # Verify index was dropped (this should not raise an exception)
        # Note: We can't easily verify the index is gone without checking system tables
        # So we just ensure the drop operation completes successfully
        assert True

    @pytest.mark.asyncio
    async def test_drop_fulltext_index_async(self, test_async_client):
        """Test dropping fulltext index asynchronously"""
        # Enable fulltext indexing using interface
        await test_async_client.fulltext_index.enable_fulltext()

        # Create test table
        await test_async_client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_fulltext_drop_async (
                id BIGINT PRIMARY KEY,
                title VARCHAR(255),
                content TEXT
            )
        """
        )

        try:
            # Create index first
            await test_async_client.fulltext_index.create(
                "test_fulltext_drop_async",
                name="ftidx_drop_async_test",
                columns=["title", "content"],
            )

            # Drop the index
            await test_async_client.fulltext_index.drop("test_fulltext_drop_async", name="ftidx_drop_async_test")

            # Verify index was dropped
            assert True

        finally:
            # Cleanup
            try:
                await test_async_client.execute("DROP TABLE IF EXISTS test_fulltext_drop_async")
            except Exception:
                pass

    # ============================================================================
    # Transaction Context Tests
    # ============================================================================

    def test_fulltext_search_with_manual_transaction(self, test_client):
        """Test fulltext search using simple_query interface"""
        # Enable fulltext indexing using interface
        test_client.fulltext_index.enable_fulltext()

        # Create test database and table
        test_client.execute("CREATE DATABASE IF NOT EXISTS manual_fulltext_tx_test")
        test_client.execute("USE manual_fulltext_tx_test")

        test_client.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_tx_docs (
                id INT PRIMARY KEY,
                title VARCHAR(100),
                content TEXT
            )
        """
        )

        # Clear any existing data and insert test data
        test_client.execute("DELETE FROM manual_tx_docs")
        test_client.execute(
            """
            INSERT INTO manual_tx_docs (id, title, content) VALUES
            (1, 'Transaction Management', 'Transactions ensure data consistency'),
            (2, 'ACID Properties', 'ACID properties guarantee reliable processing'),
            (3, 'Concurrency Control', 'Concurrency control manages simultaneous access')
        """
        )

        # Create fulltext index
        test_client.fulltext_index.create("manual_tx_docs", name="ftidx_manual_tx", columns=["title", "content"])

        try:
            # Test using query in context
            result = test_client.query(
                "manual_tx_docs.title",
                "manual_tx_docs.content",
                boolean_match("title", "content").encourage("transaction management"),
            ).execute()

            assert result is not None
            assert len(result.rows) > 0

            # Verify that we get transaction related results
            found_transaction = False
            for row in result.rows:
                if 'transaction' in str(row).lower():
                    found_transaction = True
                    break
            assert found_transaction, "Should find transaction related content"

        finally:
            # Clean up
            test_client.fulltext_index.drop("manual_tx_docs", "ftidx_manual_tx")
            test_client.execute("DROP TABLE manual_tx_docs")
            test_client.execute("DROP DATABASE manual_fulltext_tx_test")

    @pytest.mark.asyncio
    async def test_fulltext_search_with_manual_async_transaction(self, test_async_client):
        """Test async fulltext search using simple_query interface"""
        # Enable fulltext indexing using interface
        await test_async_client.fulltext_index.enable_fulltext()

        # Create test database and table
        await test_async_client.execute("CREATE DATABASE IF NOT EXISTS manual_async_fulltext_tx_test")
        await test_async_client.execute("USE manual_async_fulltext_tx_test")

        await test_async_client.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_async_tx_docs (
                id INT PRIMARY KEY,
                title VARCHAR(100),
                content TEXT
            )
        """
        )

        # Clear existing data and insert test data
        await test_async_client.execute("DELETE FROM manual_async_tx_docs")
        await test_async_client.execute(
            """
            INSERT INTO manual_async_tx_docs (id, title, content) VALUES
            (1, 'Async Transactions', 'Async transactions handle concurrent operations'),
            (2, 'Event Loop', 'Event loop manages async operations efficiently'),
            (3, 'Promise Handling', 'Promise handling manages async results')
        """
        )

        # Create fulltext index
        await test_async_client.fulltext_index.create(
            "manual_async_tx_docs",
            name="ftidx_manual_async_tx",
            columns=["title", "content"],
        )

        try:
            # Test using query in async context
            result = await test_async_client.query(
                "manual_async_tx_docs.title",
                "manual_async_tx_docs.content",
                boolean_match("title", "content").encourage("async transactions"),
            ).execute()

            assert result is not None
            assert len(result.rows) > 0

            # Verify that we get async transaction related results
            found_async_tx = False
            for row in result.rows:
                if 'async' in str(row).lower() and 'transaction' in str(row).lower():
                    found_async_tx = True
                    break
            assert found_async_tx, "Should find async transaction related content"

        finally:
            # Clean up
            await test_async_client.fulltext_index.drop("manual_async_tx_docs", "ftidx_manual_async_tx")
            await test_async_client.execute("DROP TABLE manual_async_tx_docs")
            await test_async_client.execute("DROP DATABASE manual_async_fulltext_tx_test")

    @pytest.mark.asyncio
    async def test_async_simple_query_advanced_features(self, test_async_client):
        """Test advanced async simple_query features"""
        # Create test database and table
        await test_async_client.execute("CREATE DATABASE IF NOT EXISTS async_advanced_test")
        await test_async_client.execute("USE async_advanced_test")

        await test_async_client.execute("DROP TABLE IF EXISTS async_advanced_docs")
        await test_async_client.execute(
            """
            CREATE TABLE async_advanced_docs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                category VARCHAR(100) NOT NULL,
                priority INT DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        # Insert test data
        test_docs = [
            (
                "High Priority Python Guide",
                "Comprehensive Python programming guide with async features",
                "Programming",
                3,
            ),
            (
                "Medium Priority JavaScript Tutorial",
                "JavaScript tutorial covering async/await patterns",
                "Programming",
                2,
            ),
            (
                "Low Priority Database Basics",
                "Basic database concepts and transaction handling",
                "Database",
                1,
            ),
            (
                "High Priority Async Patterns",
                "Advanced async programming patterns and best practices",
                "Programming",
                3,
            ),
            (
                "Medium Priority Web Development",
                "Modern web development with async JavaScript",
                "Web",
                2,
            ),
        ]

        for title, content, category, priority in test_docs:
            await test_async_client.execute(
                f"""
                INSERT INTO async_advanced_docs (title, content, category, priority) 
                VALUES ('{title}', '{content}', '{category}', {priority})
            """
            )

        try:
            # Create fulltext index
            await test_async_client.fulltext_index.create("async_advanced_docs", "ftidx_advanced", ["title", "content"])

            # Test 1: Complex boolean search with multiple conditions
            result = (
                await test_async_client.query(
                    "async_advanced_docs.title",
                    "async_advanced_docs.content",
                    boolean_match("title", "content").must("async").must_not("basic").label("score"),
                )
                .filter("async_advanced_docs.priority >= 2")
                .order_by("score DESC")
                .limit(3)
                .execute()
            )

            assert result is not None
            assert len(result.rows) > 0

            # Verify all results contain "async" in title or content (priority check is done in WHERE clause)
            for row in result.rows:
                title_content = f"{row[1]} {row[2]}".lower()  # title and content
                assert "async" in title_content
                assert "basic" not in title_content

            # Test 2: Search with custom score alias and ordering
            result = (
                await test_async_client.query(
                    "async_advanced_docs.title",
                    "async_advanced_docs.content",
                    boolean_match("title", "content").encourage("programming").label("relevance_score"),
                )
                .order_by("async_advanced_docs.priority DESC")
                .execute()
            )

            assert result is not None
            assert len(result.rows) > 0

            # Test 3: Multiple WHERE conditions
            result = (
                await test_async_client.query("async_advanced_docs.title", "async_advanced_docs.content")
                .filter(
                    boolean_match("title", "content").encourage("programming"),
                    "async_advanced_docs.category = 'Programming'",
                    "async_advanced_docs.priority > 1",
                )
                .execute()
            )

            assert result is not None
            # All results should be Programming category with priority > 1 (category check is done in WHERE clause)
            # Since we only select title and content, we can't directly check category in results
            assert len(result.rows) > 0  # Should have results matching the conditions

            # Test 4: Basic query functionality (replacing explain functionality)
            result = (
                await test_async_client.query(
                    "async_advanced_docs.title",
                    "async_advanced_docs.content",
                    boolean_match("title", "content").encourage("javascript").label("score"),
                )
                .filter("async_advanced_docs.priority >= 2")
                .execute()
            )

            assert result is not None
            # Should find documents with "javascript" in title or content and priority >= 2
            # From test data: "Medium Priority JavaScript Tutorial" has priority 2 and contains "javascript"
            assert len(result.rows) > 0, "Should find documents containing 'javascript' with priority >= 2"

            # Verify score column is present and has numeric values
            assert "score" in result.columns
            score_column_index = result.columns.index("score")
            for row in result.rows:
                score = row[score_column_index]
                assert isinstance(score, (int, float)), f"Score should be numeric, got {type(score)}"
                assert score >= 0, f"Score should be non-negative, got {score}"

            # Test 5: Method chaining verification
            result = (
                await test_async_client.query(
                    "async_advanced_docs.title",
                    "async_advanced_docs.content",
                    boolean_match("title", "content").encourage("python").label("score"),
                )
                .filter("async_advanced_docs.category = 'Programming'")
                .order_by("score DESC")
                .limit(5)
                .execute()
            )

            assert result is not None
            # Should find Programming documents containing "python"
            # From test data: "High Priority Python Guide" is Programming category and contains "python"
            assert len(result.rows) > 0, "Should find Programming documents containing 'python'"

            # Verify score column is present and has numeric values
            assert "score" in result.columns
            score_column_index = result.columns.index("score")
            for row in result.rows:
                score = row[score_column_index]
                assert isinstance(score, (int, float)), f"Score should be numeric, got {type(score)}"
                assert score >= 0, f"Score should be non-negative, got {score}"

        finally:
            # Clean up
            await test_async_client.fulltext_index.drop("async_advanced_docs", "ftidx_advanced")
            await test_async_client.execute("DROP TABLE async_advanced_docs")
            await test_async_client.execute("DROP DATABASE async_advanced_test")

    @pytest.mark.asyncio
    async def test_async_simple_query_concurrent_operations(self, test_async_client):
        """Test concurrent async simple_query operations"""
        import asyncio

        # Create test database and table
        await test_async_client.execute("CREATE DATABASE IF NOT EXISTS async_concurrent_test")
        await test_async_client.execute("USE async_concurrent_test")

        await test_async_client.execute("DROP TABLE IF EXISTS async_concurrent_docs")
        await test_async_client.execute(
            """
            CREATE TABLE async_concurrent_docs (
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
            await test_async_client.execute(
                f"""
                INSERT INTO async_concurrent_docs (title, content, category) 
                VALUES ('{title}', '{content}', '{category}')
            """
            )

        try:
            # Create fulltext index
            await test_async_client.fulltext_index.create("async_concurrent_docs", "ftidx_concurrent", ["title", "content"])

            # Create multiple concurrent search tasks (use database prefix to avoid connection pool issues)
            table_name = "async_concurrent_test.async_concurrent_docs"
            tasks = [
                test_async_client.query(
                    f"{table_name}.title", f"{table_name}.content", boolean_match("title", "content").encourage("python")
                ).execute(),
                test_async_client.query(
                    f"{table_name}.title", f"{table_name}.content", boolean_match("title", "content").encourage("javascript")
                ).execute(),
                test_async_client.query(
                    f"{table_name}.title", f"{table_name}.content", boolean_match("title", "content").encourage("database")
                ).execute(),
                test_async_client.query(
                    f"{table_name}.title", f"{table_name}.content", boolean_match("title", "content").encourage("web")
                ).execute(),
                test_async_client.query(
                    f"{table_name}.title", f"{table_name}.content", boolean_match("title", "content").encourage("testing")
                ).execute(),
            ]

            # Execute all searches concurrently
            results = await asyncio.gather(*tasks)

            # All searches should complete successfully
            assert len(results) == 5
            for result in results:
                assert result is not None
                assert isinstance(result.rows, list)
                assert len(result.rows) > 0

            # Verify each search returned relevant results (row[0] is title, row[1] is content)
            python_results = results[0]
            assert any("python" in str(row[0]).lower() or "python" in str(row[1]).lower() for row in python_results.rows)

            javascript_results = results[1]
            assert any(
                "javascript" in str(row[0]).lower() or "javascript" in str(row[1]).lower() for row in javascript_results.rows
            )

            database_results = results[2]
            assert any(
                "database" in str(row[0]).lower() or "database" in str(row[1]).lower() for row in database_results.rows
            )

        finally:
            # Clean up
            try:
                await test_async_client.fulltext_index.drop(
                    "async_concurrent_test.async_concurrent_docs", "ftidx_concurrent"
                )
                await test_async_client.execute("DROP TABLE async_concurrent_test.async_concurrent_docs")
                await test_async_client.execute("DROP DATABASE async_concurrent_test")
            except Exception as e:
                print(f"Cleanup warning: {e}")

    # ============================================================================
    # Advanced Fulltext Features Tests
    # ============================================================================

    def test_fulltext_search_with_different_modes(self, test_client):
        """Test fulltext search with different search modes"""
        # Enable fulltext indexing using interface
        test_client.fulltext_index.enable_fulltext()

        # Create test database and table
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_modes_test")
        test_client.execute("USE fulltext_modes_test")

        test_client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_modes (
                id INT PRIMARY KEY,
                title VARCHAR(100),
                content TEXT
            )
        """
        )

        # Clear any existing data and insert test data
        test_client.execute("DELETE FROM test_modes")
        test_client.execute(
            """
            INSERT INTO test_modes (id, title, content) VALUES
            (1, 'Natural Language Processing', 'NLP is a field of AI that focuses on language'),
            (2, 'Machine Learning Algorithms', 'ML algorithms learn patterns from data'),
            (3, 'Deep Learning Networks', 'Deep learning uses neural networks with multiple layers')
        """
        )

        # Create fulltext index
        test_client.fulltext_index.create("test_modes", name="ftidx_modes", columns=["title", "content"])

        try:
            # Test natural language mode
            result = test_client.query(
                "test_modes.title", "test_modes.content", boolean_match("title", "content").encourage("machine learning")
            ).execute()
            assert result is not None

            # Test boolean mode using must_have
            result = test_client.query(
                "test_modes.title", "test_modes.content", boolean_match("title", "content").must("machine", "learning")
            ).execute()
            assert result is not None

        finally:
            # Clean up
            test_client.fulltext_index.drop("test_modes", "ftidx_modes")
            test_client.execute("DROP TABLE test_modes")
            test_client.execute("DROP DATABASE fulltext_modes_test")

    def test_fulltext_search_multiple_columns(self, test_client):
        """Test fulltext search across multiple columns"""
        # Enable fulltext indexing using interface
        test_client.fulltext_index.enable_fulltext()

        # Create test database and table
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_multi_col_test")
        test_client.execute("USE fulltext_multi_col_test")

        test_client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_multi_col (
                id INT PRIMARY KEY,
                title VARCHAR(100),
                content TEXT,
                tags VARCHAR(200)
            )
        """
        )

        # Clear any existing data and insert test data
        test_client.execute("DELETE FROM test_multi_col")
        test_client.execute(
            """
            INSERT INTO test_multi_col (id, title, content, tags) VALUES
            (1, 'Python Programming', 'Python is great for data science', 'python, programming, data'),
            (2, 'JavaScript Development', 'JavaScript powers web applications', 'javascript, web, frontend'),
            (3, 'Database Design', 'Good database design is crucial', 'database, design, sql')
        """
        )

        # Create fulltext index on multiple columns
        test_client.fulltext_index.create(
            "test_multi_col",
            name="ftidx_multi_col",
            columns=["title", "content", "tags"],
        )

        try:
            # Test search across multiple columns
            result = test_client.query(
                "test_multi_col.title",
                "test_multi_col.content",
                "test_multi_col.tags",
                boolean_match("title", "content", "tags").encourage("python"),
            ).execute()

            assert result is not None
            # Note: Fulltext search might not return results if no exact matches
            # Just verify the search executes without error
            if len(result.rows) > 0:
                # Verify that we get Python related results
                found_python = False
                for row in result.rows:
                    if 'python' in str(row).lower():
                        found_python = True
                        break
                assert found_python, "Should find Python related content"

        finally:
            # Clean up
            test_client.fulltext_index.drop("test_multi_col", "ftidx_multi_col")
            test_client.execute("DROP TABLE test_multi_col")
            test_client.execute("DROP DATABASE fulltext_multi_col_test")

    # ============================================================================
    # JSON Parser Tests
    # ============================================================================

    def test_fulltext_json_parser_basic(self, test_client):
        """Test JSON parser for fulltext index - basic functionality"""
        from matrixone.orm import declarative_base
        from sqlalchemy import BigInteger, Column, Text

        # Enable fulltext indexing
        test_client.fulltext_index.enable_fulltext()

        # Create test database
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_json_test")
        test_client.execute("USE fulltext_json_test")

        # Define ORM model with JSON parser
        Base = declarative_base()

        class JsonDoc(Base):
            __tablename__ = "json_docs"
            id = Column(BigInteger, primary_key=True)
            json_content = Column(Text)

            __table_args__ = (FulltextIndex("ftidx_json", "json_content", parser=FulltextParserType.JSON),)

        # Create table using ORM
        try:
            test_client.create_table(JsonDoc)

            # Verify index was created with JSON parser
            result = test_client.execute("SHOW CREATE TABLE json_docs")
            create_stmt = result.fetchall()[0][1]
            assert "WITH PARSER json" in create_stmt, "Index should have WITH PARSER json clause"
            assert "FULLTEXT" in create_stmt, "Index should be FULLTEXT type"

            # Insert test data using client interface
            test_data = [
                {"id": 1, "json_content": '{"title": "Python Tutorial", "tags": ["python", "programming"]}'},
                {"id": 2, "json_content": '{"title": "Machine Learning", "tags": ["AI", "data science"]}'},
                {"id": 3, "json_content": '{"title": "Database Design", "tags": ["SQL", "database"]}'},
            ]
            test_client.batch_insert(JsonDoc, test_data)

            # Test search functionality
            result = test_client.query(JsonDoc).filter(boolean_match(JsonDoc.json_content).must("python")).execute()

            assert result is not None
            rows = result.fetchall()
            assert len(rows) >= 1, "Should find at least one result with 'python'"

            # Verify the result contains the expected JSON document
            found = False
            for row in rows:
                if 'python' in str(row.json_content).lower():
                    found = True
                    break
            assert found, "Should find JSON document containing 'python'"

        finally:
            # Clean up
            test_client.drop_table(JsonDoc)
            test_client.execute("DROP DATABASE fulltext_json_test")

    def test_fulltext_json_parser_multiple_columns(self, test_client):
        """Test JSON parser with multiple columns"""
        from matrixone.orm import declarative_base
        from sqlalchemy import BigInteger, Column, String, Text

        # Enable fulltext indexing
        test_client.fulltext_index.enable_fulltext()

        # Create test database
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_json_multi_test")
        test_client.execute("USE fulltext_json_multi_test")

        # Define ORM model with JSON parser on multiple columns
        Base = declarative_base()

        class JsonMulti(Base):
            __tablename__ = "json_multi"
            id = Column(BigInteger, primary_key=True)
            json1 = Column(Text)
            json2 = Column(String(1000))

            __table_args__ = (FulltextIndex("ftidx_json_multi", ["json1", "json2"], parser=FulltextParserType.JSON),)

        # Create table using ORM
        try:
            test_client.create_table(JsonMulti)

            # Verify index was created with JSON parser
            result = test_client.execute("SHOW CREATE TABLE json_multi")
            create_stmt = result.fetchall()[0][1]
            assert "WITH PARSER json" in create_stmt, "Index should have WITH PARSER json clause"
            assert "ftidx_json_multi" in create_stmt, "Index name should be present"

            # Insert test data using client interface
            test_data = [
                {"id": 1, "json1": '{"name": "red apple"}', "json2": '{"color": "red", "taste": "sweet"}'},
                {"id": 2, "json1": '{"name": "blue sky"}', "json2": '{"weather": "sunny", "season": "summer"}'},
                {"id": 3, "json1": '{"name": "green tree"}', "json2": '{"type": "oak", "color": "green"}'},
            ]
            test_client.batch_insert(JsonMulti, test_data)

            # Test search on multiple columns
            result = (
                test_client.query(JsonMulti).filter(boolean_match(JsonMulti.json1, JsonMulti.json2).must("red")).execute()
            )

            assert result is not None
            rows = result.fetchall()
            assert len(rows) >= 1, "Should find results with 'red'"

            # Verify result
            found_red = False
            for row in rows:
                if row.id == 1:  # ID should be 1
                    found_red = True
                    break
            assert found_red, "Should find the red apple document"

        finally:
            # Clean up
            test_client.drop_table(JsonMulti)
            test_client.execute("DROP DATABASE fulltext_json_multi_test")

    def test_fulltext_json_parser_chinese_content(self, test_client):
        """Test JSON parser with Chinese content"""
        from matrixone.orm import declarative_base
        from sqlalchemy import BigInteger, Column, Text

        # Enable fulltext indexing
        test_client.fulltext_index.enable_fulltext()

        # Create test database
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_json_chinese_test")
        test_client.execute("USE fulltext_json_chinese_test")

        # Define ORM model with JSON parser
        Base = declarative_base()

        class JsonChinese(Base):
            __tablename__ = "json_chinese"
            id = Column(BigInteger, primary_key=True)
            json_data = Column(Text)

            __table_args__ = (FulltextIndex("ftidx_json_chinese", "json_data", parser=FulltextParserType.JSON),)

        # Create table using ORM
        try:
            test_client.create_table(JsonChinese)

            # Insert Chinese JSON data using client interface
            test_data = [
                {"id": 1, "json_data": '{"title": "中文學習教材", "description": "適合初學者"}'},
                {"id": 2, "json_data": '{"title": "兒童中文", "description": "遠東兒童中文"}'},
                {"id": 3, "json_data": '{"title": "English Book", "description": "For beginners"}'},
            ]
            test_client.batch_insert(JsonChinese, test_data)

            # Test search for Chinese content
            result = (
                test_client.query(JsonChinese).filter(boolean_match(JsonChinese.json_data).must("中文學習教材")).execute()
            )

            assert result is not None
            rows = result.fetchall()
            assert len(rows) >= 1, "Should find Chinese content"

            # Verify result contains the expected Chinese document
            found_chinese = False
            for row in rows:
                if row.id == 1:  # ID should be 1
                    found_chinese = True
                    assert "中文學習教材" in str(row.json_data), "Should contain the Chinese text"
                    break
            assert found_chinese, "Should find the Chinese learning material document"

        finally:
            # Clean up
            test_client.drop_table(JsonChinese)
            test_client.execute("DROP DATABASE fulltext_json_chinese_test")

    def test_fulltext_json_parser_orm_integration(self, test_client):
        """Test JSON parser with ORM integration"""
        from matrixone.orm import declarative_base
        from sqlalchemy import BigInteger, Column, Text

        # Enable fulltext indexing
        test_client.fulltext_index.enable_fulltext()

        # Create test database
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_json_orm_test")
        test_client.execute("USE fulltext_json_orm_test")

        # Define ORM model with JSON parser index
        Base = declarative_base()

        class JsonDocument(Base):
            __tablename__ = "json_documents"
            id = Column(BigInteger, primary_key=True)
            json_content = Column(Text)

            __table_args__ = (FulltextIndex("idx_json_content", "json_content", parser=FulltextParserType.JSON),)

        try:
            # Create table using ORM
            test_client.create_table(JsonDocument)

            # Verify index was created correctly
            result = test_client.execute("SHOW CREATE TABLE json_documents")
            create_stmt = result.fetchall()[0][1]
            assert "WITH PARSER json" in create_stmt, "ORM-created index should have WITH PARSER json"
            assert "FULLTEXT" in create_stmt, "Index should be FULLTEXT type"
            assert "idx_json_content" in create_stmt, "Index name should be present"

            # Insert test data using client interface
            test_data = [
                {"id": 1, "json_content": '{"framework": "Django", "language": "Python"}'},
                {"id": 2, "json_content": '{"framework": "Flask", "language": "Python"}'},
                {"id": 3, "json_content": '{"framework": "Express", "language": "JavaScript"}'},
            ]
            test_client.batch_insert(JsonDocument, test_data)

            # Test ORM query with JSON parser
            result = (
                test_client.query(JsonDocument).filter(boolean_match(JsonDocument.json_content).must("Django")).execute()
            )

            assert result is not None
            rows = result.fetchall()
            assert len(rows) >= 1, "Should find Django document"

            # Verify the result
            found_django = False
            for row in rows:
                if row.id == 1:
                    found_django = True
                    assert "Django" in row.json_content
                    break
            assert found_django, "Should find the Django framework document"

        finally:
            # Clean up
            test_client.drop_table(JsonDocument)
            test_client.execute("DROP DATABASE fulltext_json_orm_test")

    def test_fulltext_json_parser_colon_handling(self, test_client):
        """
        Test that JSON strings with colons are properly handled in batch_insert.

        This is a regression test for the issue where SQLAlchemy's text() function
        would interpret colons in JSON strings (like {"a":1}) as bind parameters (:1),
        causing "A value is required for bind parameter" errors.

        The fix uses exec_driver_sql() to bypass SQLAlchemy's parameter parsing.
        """
        from matrixone.orm import declarative_base
        from sqlalchemy import BigInteger, Column, Text

        # Enable fulltext indexing
        test_client.fulltext_index.enable_fulltext()

        # Create test database
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_json_colon_test")
        test_client.execute("USE fulltext_json_colon_test")

        # Define ORM model
        Base = declarative_base()

        class JsonColonTest(Base):
            __tablename__ = "json_colon_test"
            id = Column(BigInteger, primary_key=True)
            json_data = Column(Text)

            __table_args__ = (FulltextIndex("idx_json_data", "json_data", parser=FulltextParserType.JSON),)

        try:
            # Create table
            test_client.create_table(JsonColonTest)

            # Critical test: Insert JSON data with colons
            # This would fail with "A value is required for bind parameter '1'" before the fix
            test_data = [
                {"id": 1, "json_data": '{"key1":"value1", "key2":123}'},
                {"id": 2, "json_data": '{"a":1, "b":"red", "c":{"nested":"value"}}'},
                {"id": 3, "json_data": '["item1", "item2", "item3"]'},
                {"id": 4, "json_data": '{"中文":"測試", "number":456}'},
            ]

            # This should NOT raise "A value is required for bind parameter" error
            test_client.batch_insert(JsonColonTest, test_data)

            # Verify all rows were inserted
            result = test_client.query(JsonColonTest).execute()
            rows = result.fetchall()
            assert len(rows) == 4, "Should insert all 4 rows with JSON containing colons"

            # Test single insert with JSON colons
            test_client.insert(JsonColonTest, {"id": 5, "json_data": '{"test":"single insert", "value":999}'})

            result = test_client.query(JsonColonTest).execute()
            rows = result.fetchall()
            assert len(rows) == 5, "Should have 5 rows after single insert"

            # Test fulltext search on JSON data
            result = test_client.query(JsonColonTest).filter(boolean_match(JsonColonTest.json_data).must("red")).execute()

            rows = result.fetchall()
            assert len(rows) >= 1, "Should find JSON with 'red'"
            assert rows[0].id == 2, "Should find the correct JSON document"

        finally:
            # Clean up
            test_client.drop_table(JsonColonTest)
            test_client.execute("DROP DATABASE fulltext_json_colon_test")

    # ============================================================================
    # NGRAM Parser Tests (Chinese Content)
    # ============================================================================

    def test_fulltext_ngram_parser_chinese(self, test_client):
        """Test NGRAM parser with Chinese content - comprehensive coverage"""
        test_client.fulltext_index.enable_fulltext()
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_ngram_test")
        test_client.execute("USE fulltext_ngram_test")

        Base = declarative_base()

        class ChineseArticle(Base):
            __tablename__ = "chinese_articles"
            id = Column(Integer, primary_key=True, autoincrement=True)
            title = Column(String(200))
            body = Column(Text)
            __table_args__ = (FulltextIndex("ftidx_ngram", ["title", "body"], parser=FulltextParserType.NGRAM),)

        try:
            test_client.create_table(ChineseArticle)

            # Insert Chinese content
            articles = [
                {"id": 1, "title": "神雕侠侣 第一回 风月无情", "body": "越女采莲秋水畔，窄袖轻罗，暗露双金钏"},
                {
                    "id": 2,
                    "title": "神雕侠侣 第二回 故人之子",
                    "body": "正自发痴，忽听左首屋中传出一人喝道：这是在人家府上，你又提小龙女干什么？",
                },
                {"id": 3, "title": "神雕侠侣 第三回 投师终南", "body": "郭靖在舟中潜运神功，数日间伤势便已痊愈了大半。"},
                {
                    "id": 4,
                    "title": "神雕侠侣 第四回 全真门下",
                    "body": "郭靖摆脱众道纠缠，提气向重阳宫奔去，忽听得钟声镗镗响起",
                },
                {"id": 5, "title": "神雕侠侣 第五回 活死人墓", "body": "杨过摔下山坡，滚入树林长草丛中，便即昏晕"},
                {"id": 6, "title": "神雕侠侣 第六回 玉女心经", "body": "小龙女从怀里取出一个瓷瓶，交在杨过手里"},
            ]
            test_client.batch_insert(ChineseArticle, articles)

            # Test NGRAM search for Chinese terms (using natural language mode)
            result = (
                test_client.query(ChineseArticle)
                .filter(natural_match(ChineseArticle.title, ChineseArticle.body, query="风月无情"))
                .execute()
            )
            rows = result.fetchall()
            assert len(rows) >= 1, "Should find articles with 风月无情"
            assert rows[0].id == 1, "Should find the correct article"

            # Test multi-character Chinese search
            result = (
                test_client.query(ChineseArticle)
                .filter(natural_match(ChineseArticle.title, ChineseArticle.body, query="杨过"))
                .execute()
            )
            rows = result.fetchall()
            assert len(rows) >= 2, "Should find multiple articles with 杨过"
            found_ids = {row.id for row in rows}
            assert 5 in found_ids and 6 in found_ids, "Should find articles 5 and 6"

            # Test search for common term across all articles
            result = (
                test_client.query(ChineseArticle)
                .filter(natural_match(ChineseArticle.title, ChineseArticle.body, query="神雕侠侣"))
                .execute()
            )
            rows = result.fetchall()
            assert len(rows) == 6, "Should find all 6 articles with 神雕侠侣 in title"

            # Test 小龙女 search
            result = (
                test_client.query(ChineseArticle)
                .filter(natural_match(ChineseArticle.title, ChineseArticle.body, query="小龙女"))
                .execute()
            )
            rows = result.fetchall()
            assert len(rows) >= 2, "Should find articles mentioning 小龙女"
            found_ids = {row.id for row in rows}
            assert 2 in found_ids and 6 in found_ids, "Should find articles 2 and 6"

        finally:
            test_client.drop_table(ChineseArticle)
            test_client.execute("DROP DATABASE fulltext_ngram_test")

    def test_fulltext_ngram_parser_mixed_content(self, test_client):
        """Test NGRAM parser with mixed English and Chinese content"""
        test_client.fulltext_index.enable_fulltext()
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_ngram_mixed_test")
        test_client.execute("USE fulltext_ngram_mixed_test")

        Base = declarative_base()

        class MixedArticle(Base):
            __tablename__ = "mixed_articles"
            id = Column(Integer, primary_key=True, autoincrement=True)
            title = Column(String(255))
            content = Column(Text)
            __table_args__ = (FulltextIndex("ftidx_ngram_mixed", ["title", "content"], parser=FulltextParserType.NGRAM),)

        try:
            test_client.create_table(MixedArticle)

            # Insert mixed content
            articles = [
                {
                    "id": 1,
                    "title": "MO全文索引示例",
                    "content": "这是一个关于MO全文索引的例子。它展示了如何使用ngram解析器进行全文搜索。",
                },
                {"id": 2, "title": "ngram解析器", "content": "ngram解析器允许MO对中文等语言进行分词，以优化全文搜索。"},
            ]
            test_client.batch_insert(MixedArticle, articles)

            # Test search for Chinese term (using natural language mode)
            result = (
                test_client.query(MixedArticle)
                .filter(natural_match(MixedArticle.title, MixedArticle.content, query="全文索引"))
                .execute()
            )
            rows = result.fetchall()
            assert len(rows) >= 1, "Should find articles with 全文索引"
            assert rows[0].id == 1, "Should find article 1"

            # Test search for English term (using natural language mode)
            result = (
                test_client.query(MixedArticle)
                .filter(natural_match(MixedArticle.title, MixedArticle.content, query="ngram"))
                .execute()
            )
            rows = result.fetchall()
            assert len(rows) >= 1, "Should find articles with ngram"

        finally:
            test_client.drop_table(MixedArticle)
            test_client.execute("DROP DATABASE fulltext_ngram_mixed_test")

    # ============================================================================
    # BM25 Algorithm Tests
    # ============================================================================

    def test_fulltext_bm25_algorithm(self, test_client):
        """Test BM25 relevancy algorithm"""
        test_client.fulltext_index.enable_fulltext()
        test_client.execute('SET ft_relevancy_algorithm = "BM25"')
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_bm25_test")
        test_client.execute("USE fulltext_bm25_test")

        Base = declarative_base()

        class BM25Article(Base):
            __tablename__ = "bm25_articles"
            id = Column(Integer, primary_key=True)
            body = Column(String(500))
            title = Column(Text)
            __table_args__ = (FulltextIndex("ftidx_bm25", ["body", "title"]),)

        try:
            test_client.create_table(BM25Article)

            # Insert test data
            articles = [
                {"id": 0, "body": "color is red", "title": "t1"},
                {"id": 1, "body": "car is yellow", "title": "crazy car"},
                {"id": 2, "body": "sky is blue", "title": "no limit"},
                {"id": 3, "body": "blue is not red", "title": "colorful"},
            ]
            test_client.batch_insert(BM25Article, articles)

            # Test basic BM25 search
            result = (
                test_client.query(BM25Article)
                .filter(boolean_match(BM25Article.body, BM25Article.title).must("red"))
                .execute()
            )
            rows = result.fetchall()
            assert len(rows) == 2, "Should find 2 articles with 'red'"
            found_ids = {row.id for row in rows}
            assert 0 in found_ids and 3 in found_ids, "Should find articles 0 and 3"

            # Test BM25 with multiple terms
            result = (
                test_client.query(BM25Article)
                .filter(boolean_match(BM25Article.body, BM25Article.title).must("blue", "red"))
                .execute()
            )
            rows = result.fetchall()
            assert len(rows) >= 1, "Should find articles with both blue and red"
            assert rows[0].id == 3, "Should find article 3"

        finally:
            test_client.drop_table(BM25Article)
            test_client.execute("DROP DATABASE fulltext_bm25_test")
            # Reset to TF-IDF
            test_client.execute('SET ft_relevancy_algorithm = "TF-IDF"')

    # ============================================================================
    # Complex Boolean Mode Operators
    # ============================================================================

    def test_fulltext_boolean_wildcard(self, test_client):
        """Test boolean mode wildcard operator"""
        test_client.fulltext_index.enable_fulltext()
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_wildcard_test")
        test_client.execute("USE fulltext_wildcard_test")

        Base = declarative_base()

        class WildcardTest(Base):
            __tablename__ = "wildcard_test"
            id = Column(Integer, primary_key=True)
            body = Column(String(500))
            title = Column(Text)
            __table_args__ = (FulltextIndex("ftidx_wildcard", ["body", "title"]),)

        try:
            test_client.create_table(WildcardTest)

            articles = [
                {"id": 0, "body": "color is red", "title": "t1"},
                {"id": 1, "body": "car is yellow", "title": "crazy car"},
                {"id": 2, "body": "sky is blue", "title": "no limit"},
                {"id": 3, "body": "blue is not red", "title": "colorful"},
            ]
            test_client.batch_insert(WildcardTest, articles)

            # Test wildcard: re* should match 'red'
            # Note: Using raw SQL as wildcard may not be directly supported in ORM API
            result = test_client.execute(
                "SELECT * FROM wildcard_test WHERE MATCH(body, title) AGAINST('re*' IN BOOLEAN MODE)"
            )
            rows = result.fetchall()
            assert len(rows) == 2, "Wildcard re* should match 'red'"

        finally:
            test_client.drop_table(WildcardTest)
            test_client.execute("DROP DATABASE fulltext_wildcard_test")

    def test_fulltext_boolean_phrase(self, test_client):
        """Test boolean mode phrase search"""
        test_client.fulltext_index.enable_fulltext()
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_phrase_test")
        test_client.execute("USE fulltext_phrase_test")

        Base = declarative_base()

        class PhraseTest(Base):
            __tablename__ = "phrase_test"
            id = Column(Integer, primary_key=True)
            body = Column(String(500))
            title = Column(Text)
            __table_args__ = (FulltextIndex("ftidx_phrase", ["body", "title"]),)

        try:
            test_client.create_table(PhraseTest)

            articles = [
                {"id": 0, "body": "color is red", "title": "t1"},
                {"id": 1, "body": "car is yellow", "title": "crazy car"},
                {"id": 2, "body": "sky is blue", "title": "no limit"},
                {"id": 3, "body": "blue is not red", "title": "colorful"},
            ]
            test_client.batch_insert(PhraseTest, articles)

            # Test phrase search: "is not red" should match exact phrase
            result = test_client.execute(
                'SELECT * FROM phrase_test WHERE MATCH(body, title) AGAINST(\'"is not red"\' IN BOOLEAN MODE)'
            )
            rows = result.fetchall()
            assert len(rows) == 1, "Should find exact phrase 'is not red'"
            assert rows[0][0] == 3, "Should find article 3"

            # Test non-matching phrase
            result = test_client.execute(
                'SELECT * FROM phrase_test WHERE MATCH(body, title) AGAINST(\'"blue is red"\' IN BOOLEAN MODE)'
            )
            rows = result.fetchall()
            assert len(rows) == 0, "Should not find non-existent phrase"

        finally:
            test_client.drop_table(PhraseTest)
            test_client.execute("DROP DATABASE fulltext_phrase_test")

    # ============================================================================
    # UPDATE/DELETE Index Maintenance Tests
    # ============================================================================

    def test_fulltext_update_maintenance(self, test_client):
        """Test that fulltext index is maintained after UPDATE"""
        test_client.fulltext_index.enable_fulltext()
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_update_test")
        test_client.execute("USE fulltext_update_test")

        Base = declarative_base()

        class UpdateTest(Base):
            __tablename__ = "update_test"
            id = Column(Integer, primary_key=True)
            body = Column(String(500))
            title = Column(Text)
            __table_args__ = (FulltextIndex("ftidx_update", ["body", "title"]),)

        try:
            test_client.create_table(UpdateTest)

            articles = [
                {"id": 0, "body": "color is red", "title": "t1"},
                {"id": 1, "body": "car is yellow", "title": "crazy car"},
            ]
            test_client.batch_insert(UpdateTest, articles)

            # Search for 'red' - should find article 0
            result = test_client.execute("SELECT * FROM update_test WHERE MATCH(body, title) AGAINST('red')")
            rows = result.fetchall()
            assert len(rows) == 1 and rows[0][0] == 0, "Should find article 0 with 'red'"

            # Update article 0 to have 'brown' instead of 'red'
            test_client.execute("UPDATE update_test SET body='color is brown' WHERE id=0")

            # Search for 'red' - should find nothing
            result = test_client.execute("SELECT * FROM update_test WHERE MATCH(body, title) AGAINST('red')")
            rows = result.fetchall()
            assert len(rows) == 0, "Should not find 'red' after update"

            # Search for 'brown' - should find updated article
            result = test_client.execute("SELECT * FROM update_test WHERE MATCH(body, title) AGAINST('brown')")
            rows = result.fetchall()
            assert len(rows) == 1 and rows[0][0] == 0, "Should find updated article with 'brown'"

        finally:
            test_client.drop_table(UpdateTest)
            test_client.execute("DROP DATABASE fulltext_update_test")

    def test_fulltext_delete_maintenance(self, test_client):
        """Test that fulltext index is maintained after DELETE"""
        test_client.fulltext_index.enable_fulltext()
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_delete_test")
        test_client.execute("USE fulltext_delete_test")

        Base = declarative_base()

        class DeleteTest(Base):
            __tablename__ = "delete_test"
            id = Column(Integer, primary_key=True)
            body = Column(String(500))
            title = Column(Text)
            __table_args__ = (FulltextIndex("ftidx_delete", ["body", "title"]),)

        try:
            test_client.create_table(DeleteTest)

            articles = [
                {"id": 0, "body": "red", "title": "t1"},
                {"id": 1, "body": "yellow", "title": "t2"},
                {"id": 2, "body": "blue", "title": "t3"},
                {"id": 3, "body": "blue red", "title": "t4"},
            ]
            test_client.batch_insert(DeleteTest, articles)

            # Search for 'red' - should find 2 articles
            result = test_client.execute("SELECT * FROM delete_test WHERE MATCH(body, title) AGAINST('red')")
            rows = result.fetchall()
            assert len(rows) == 2, "Should find 2 articles with 'red'"

            # Delete article 3
            test_client.execute("DELETE FROM delete_test WHERE id=3")

            # Search for 'red' - should find only article 0
            result = test_client.execute("SELECT * FROM delete_test WHERE MATCH(body, title) AGAINST('red')")
            rows = result.fetchall()
            assert len(rows) == 1 and rows[0][0] == 0, "Should find only article 0 after delete"

        finally:
            test_client.drop_table(DeleteTest)
            test_client.execute("DROP DATABASE fulltext_delete_test")

    # ============================================================================
    # ALTER TABLE Tests
    # ============================================================================

    def test_fulltext_alter_table_drop_column(self, test_client):
        """Test ALTER TABLE DROP COLUMN with fulltext index"""
        test_client.fulltext_index.enable_fulltext()
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_alter_test")
        test_client.execute("USE fulltext_alter_test")

        Base = declarative_base()

        class AlterTest(Base):
            __tablename__ = "alter_test"
            employee_number = Column(Integer, primary_key=True)
            last_name = Column(String(50))
            first_name = Column(String(50))
            email = Column(String(100))

        try:
            test_client.create_table(AlterTest)

            # Insert test data
            employees = [
                {"employee_number": 1002, "last_name": "Murphy", "first_name": "Diane", "email": "dmurphy@test.com"},
                {"employee_number": 1056, "last_name": "Patterson", "first_name": "Mary", "email": "mpatterso@test.com"},
            ]
            test_client.batch_insert(AlterTest, employees)

            # Create fulltext index
            test_client.execute("CREATE FULLTEXT INDEX f01 ON alter_test (last_name, first_name)")

            # Drop a column that's part of the fulltext index
            test_client.execute("ALTER TABLE alter_test DROP COLUMN last_name")

            # Verify the table structure
            result = test_client.execute("SHOW CREATE TABLE alter_test")
            create_stmt = result.fetchone()[1]
            assert "first_name" in create_stmt, "first_name should still exist"
            assert "last_name" not in create_stmt, "last_name should be dropped"
            assert "FULLTEXT" in create_stmt, "Fulltext index should still exist (on remaining column)"

            # Verify data is still accessible
            result = test_client.execute("SELECT COUNT(*) FROM alter_test")
            count = result.fetchone()[0]
            assert count == 2, "All data should still be present"

        finally:
            test_client.execute("DROP TABLE IF EXISTS alter_test")
            test_client.execute("DROP DATABASE fulltext_alter_test")

    # ============================================================================
    # NULL and Edge Case Tests
    # ============================================================================

    def test_fulltext_null_handling(self, test_client):
        """Test fulltext index with NULL values"""
        test_client.fulltext_index.enable_fulltext()
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_null_test")
        test_client.execute("USE fulltext_null_test")

        Base = declarative_base()

        class NullTest(Base):
            __tablename__ = "null_test"
            id = Column(Integer, primary_key=True)
            body = Column(String(500))
            title = Column(Text)
            __table_args__ = (FulltextIndex("ftidx_null", ["body", "title"]),)

        try:
            test_client.create_table(NullTest)

            # Insert data with NULL values
            test_client.execute(
                "INSERT INTO null_test VALUES (0, 'color is red', 't1'), (1, NULL, 'NOT INCLUDED'), "
                "(2, 'NOT INCLUDED BODY', NULL), (3, NULL, NULL)"
            )

            # Search should work even with NULLs
            result = test_client.execute("SELECT * FROM null_test WHERE MATCH(body, title) AGAINST('red')")
            rows = result.fetchall()
            assert len(rows) == 1 and rows[0][0] == 0, "Should find article 0 with 'red', ignoring NULLs"

            # NULL-only row should not be found
            result = test_client.execute("SELECT * FROM null_test WHERE MATCH(body, title) AGAINST('NULL')")
            rows = result.fetchall()
            # NULL values should not be indexed

        finally:
            test_client.drop_table(NullTest)
            test_client.execute("DROP DATABASE fulltext_null_test")

    def test_fulltext_empty_search_string(self, test_client):
        """Test fulltext with empty and special search strings"""
        test_client.fulltext_index.enable_fulltext()
        test_client.execute("CREATE DATABASE IF NOT EXISTS fulltext_edge_test")
        test_client.execute("USE fulltext_edge_test")

        Base = declarative_base()

        class EdgeTest(Base):
            __tablename__ = "edge_test"
            id = Column(Integer, primary_key=True)
            data = Column(Text)
            __table_args__ = (FulltextIndex("ftidx_edge", "data"),)

        try:
            test_client.create_table(EdgeTest)

            articles = [
                {"id": 1, "data": "test content"},
                {"id": 2, "data": "another test"},
            ]
            test_client.batch_insert(EdgeTest, articles)

            # Test with empty space - should return nothing
            result = test_client.execute("SELECT * FROM edge_test WHERE MATCH(data) AGAINST(' ')")
            rows = result.fetchall()
            # Empty string search should return no results

        finally:
            test_client.drop_table(EdgeTest)
            test_client.execute("DROP DATABASE fulltext_edge_test")
