#!/usr/bin/env python3
"""
Online tests for fulltext index functionality.

These tests require a running MatrixOne database and test the actual
fulltext index functionality with real database operations.
"""

import pytest
import pytest_asyncio
from matrixone import Client, AsyncClient, FulltextAlgorithmType, FulltextModeType
from matrixone.sqlalchemy_ext import FulltextIndex, FulltextSearchBuilder


class TestFulltextIndexOnline:
    """Test fulltext index functionality with real database"""

    @pytest.fixture(scope="function")
    def test_table(self, test_client):
        """Create test table for fulltext tests"""
        # Enable fulltext indexing
        test_client.execute("SET experimental_fulltext_index = 1")
        test_client.execute('SET ft_relevancy_algorithm = "BM25"')
        
        # Create test table
        test_client.execute("""
            CREATE TABLE IF NOT EXISTS test_fulltext (
                id BIGINT PRIMARY KEY,
                title VARCHAR(255),
                content TEXT,
                category VARCHAR(50)
            )
        """)
        
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
            table_name=test_table,
            name="ftidx_test",
            columns=["title", "content"],
            algorithm=FulltextAlgorithmType.BM25
        )
        
        # Verify index was created by checking if we can use it
        test_client.execute("INSERT INTO test_fulltext (id, title, content, category) VALUES (1, 'Test Title', 'Test content for fulltext search', 'test')")
        
        # Search using the index
        result = test_client.execute("""
            SELECT id, title, MATCH(title, content) AGAINST('test' IN NATURAL LANGUAGE MODE) as score
            FROM test_fulltext 
            WHERE MATCH(title, content) AGAINST('test' IN NATURAL LANGUAGE MODE)
        """)
        
        assert len(result.rows) > 0
        assert result.rows[0][0] == 1
        assert result.rows[0][1] == 'Test Title'
        assert result.rows[0][2] >= 0  # Score should be non-negative

    def test_create_fulltext_index_direct(self, test_client, test_table):
        """Test creating fulltext index using FulltextIndex class directly"""
        # Create fulltext index using FulltextIndex class
        success = FulltextIndex.create_index(
            engine=test_client.get_sqlalchemy_engine(),
            table_name=test_table,
            name="ftidx_direct",
            columns=["title", "content"],
            algorithm=FulltextAlgorithmType.TF_IDF
        )
        
        assert success is True
        
        # Test the index
        test_client.execute("INSERT INTO test_fulltext (id, title, content, category) VALUES (2, 'Direct Test', 'Direct test content', 'direct')")
        
        result = test_client.execute("""
            SELECT id, title
            FROM test_fulltext 
            WHERE MATCH(title, content) AGAINST('direct' IN NATURAL LANGUAGE MODE)
        """)
        
        assert len(result.rows) > 0
        assert result.rows[0][0] == 2

    def test_fulltext_search_modes(self, test_client, test_table):
        """Test different fulltext search modes"""
        # Create index
        test_client.fulltext_index.create(
            table_name=test_table,
            name="ftidx_modes",
            columns=["title", "content"]
        )
        
        # Insert test data
        test_client.execute("""
            INSERT INTO test_fulltext (id, title, content, category) VALUES 
            (1, 'Python Programming Guide', 'Learn Python programming from basics to advanced topics', 'programming'),
            (2, 'Web Development with Python', 'Build web applications using Python and Django framework', 'web'),
            (3, 'Machine Learning Python', 'Machine learning algorithms implemented in Python', 'ml')
        """)
        
        # Test natural language mode
        result = test_client.execute("""
            SELECT id, title, MATCH(title, content) AGAINST('Python programming' IN NATURAL LANGUAGE MODE) as score
            FROM test_fulltext 
            WHERE MATCH(title, content) AGAINST('Python programming' IN NATURAL LANGUAGE MODE)
            ORDER BY score DESC
        """)
        
        assert len(result.rows) >= 1  # Should find at least 1 record
        
        # Test boolean mode
        result = test_client.execute("""
            SELECT id, title
            FROM test_fulltext 
            WHERE MATCH(title, content) AGAINST('+Python +web' IN BOOLEAN MODE)
        """)
        
        assert len(result.rows) == 1
        assert result.rows[0][0] == 2  # Should find the web development record

    def test_fulltext_search_builder(self, test_client, test_table):
        """Test FulltextSearchBuilder functionality"""
        # Create index
        test_client.fulltext_index.create(
            table_name=test_table,
            name="ftidx_builder",
            columns=["title", "content"]
        )
        
        # Insert test data
        test_client.execute("""
            INSERT INTO test_fulltext (id, title, content, category) VALUES 
            (1, 'Database Design Principles', 'Learn how to design efficient database systems', 'database'),
            (2, 'SQL Query Optimization', 'Optimize your SQL queries for better performance', 'database'),
            (3, 'NoSQL Database Comparison', 'Compare different NoSQL database solutions', 'database')
        """)
        
        # Use search builder
        builder = FulltextSearchBuilder(test_table, ["title", "content"])
        builder.search("database").set_with_score(True).where("id > 0").set_order_by("score", "DESC").limit(2)
        
        sql = builder.build_sql()
        result = test_client.execute(sql)
        
        assert len(result.rows) <= 2  # Should be limited to 2 results
        assert len(result.rows[0]) >= 4  # Should include score column

    def test_drop_fulltext_index(self, test_client, test_table):
        """Test dropping fulltext index"""
        # Create index
        test_client.fulltext_index.create(
            table_name=test_table,
            name="ftidx_drop",
            columns=["title", "content"]
        )
        
        # Insert and search to verify index works
        test_client.execute("INSERT INTO test_fulltext (id, title, content, category) VALUES (1, 'Drop Test', 'Test for dropping index', 'test')")
        
        result = test_client.execute("""
            SELECT COUNT(*) FROM test_fulltext 
            WHERE MATCH(title, content) AGAINST('drop' IN NATURAL LANGUAGE MODE)
        """)
        assert result.rows[0][0] == 1
        
        # Drop the index
        test_client.fulltext_index.drop(test_table, "ftidx_drop")
        
        # Verify index is dropped (this might raise an error or return 0 results)
        try:
            result = test_client.execute("""
                SELECT COUNT(*) FROM test_fulltext 
                WHERE MATCH(title, content) AGAINST('drop' IN NATURAL LANGUAGE MODE)
            """)
            # If no error, results might be 0 or different
        except Exception:
            # Expected if index is properly dropped
            pass

    def test_fulltext_in_transaction(self, test_client, test_table):
        """Test fulltext index operations within transaction"""
        with test_client.transaction() as tx:
            # Create index within transaction
            tx.fulltext_index.create(
                table_name=test_table,
                name="ftidx_transaction",
                columns=["title", "content"]
            )
            
            # Insert data within transaction
            tx.execute("INSERT INTO test_fulltext (id, title, content, category) VALUES (1, 'Transaction Test', 'Test fulltext in transaction', 'transaction')")
            
            # Search within transaction
            result = tx.execute("""
                SELECT id, title FROM test_fulltext 
                WHERE MATCH(title, content) AGAINST('transaction' IN NATURAL LANGUAGE MODE)
            """)
            
            assert len(result.rows) == 1
            assert result.rows[0][0] == 1

    @pytest.mark.asyncio
    async def test_fulltext_index_async(self, test_async_client):
        """Test fulltext index functionality with async client"""
        # Enable fulltext indexing
        await test_async_client.execute("SET experimental_fulltext_index = 1")
        await test_async_client.execute('SET ft_relevancy_algorithm = "BM25"')
        
        # Create test table
        await test_async_client.execute("""
            CREATE TABLE IF NOT EXISTS test_fulltext_async (
                id BIGINT PRIMARY KEY,
                title VARCHAR(255),
                content TEXT
            )
        """)
        
        try:
            # Create fulltext index
            await test_async_client.fulltext_index.create(
                table_name="test_fulltext_async",
                name="ftidx_async",
                columns=["title", "content"],
                algorithm=FulltextAlgorithmType.BM25
            )
            
            # Insert data
            await test_async_client.execute("""
                INSERT INTO test_fulltext_async (id, title, content) VALUES 
                (1, 'Async Test Title', 'Async test content for fulltext search')
            """)
            
            # Search using fulltext
            result = await test_async_client.execute("""
                SELECT id, title, MATCH(title, content) AGAINST('async test' IN NATURAL LANGUAGE MODE) as score
                FROM test_fulltext_async 
                WHERE MATCH(title, content) AGAINST('async test' IN NATURAL LANGUAGE MODE)
            """)
            
            assert len(result.rows) > 0
            assert result.rows[0][0] == 1
            assert result.rows[0][2] >= 0  # Score should be non-negative
            
        finally:
            # Cleanup
            try:
                await test_async_client.execute("DROP TABLE IF EXISTS test_fulltext_async")
            except Exception:
                # Ignore cleanup errors - fixture will handle client disconnect
                pass

    def test_fulltext_algorithms(self, test_client, test_table):
        """Test different fulltext algorithms (TF-IDF vs BM25)"""
        # Insert test data first
        test_client.execute("""
            INSERT INTO test_fulltext (id, title, content, category) VALUES 
            (1, 'Algorithm Test', 'Testing different fulltext algorithms for search relevance', 'test')
        """)
        
        # Test TF-IDF algorithm
        test_client.execute('SET ft_relevancy_algorithm = "TF-IDF"')
        test_client.fulltext_index.create(
            table_name=test_table,
            name="ftidx_tfidf",
            columns=["title", "content"],
            algorithm=FulltextAlgorithmType.TF_IDF
        )
        
        result_tfidf = test_client.execute("""
            SELECT MATCH(title, content) AGAINST('algorithm test' IN NATURAL LANGUAGE MODE) as score
            FROM test_fulltext 
            WHERE MATCH(title, content) AGAINST('algorithm test' IN NATURAL LANGUAGE MODE)
        """)
        
        # Drop and recreate with BM25
        test_client.fulltext_index.drop(test_table, "ftidx_tfidf")
        test_client.execute('SET ft_relevancy_algorithm = "BM25"')
        test_client.fulltext_index.create(
            table_name=test_table,
            name="ftidx_bm25",
            columns=["title", "content"],
            algorithm=FulltextAlgorithmType.BM25
        )
        
        result_bm25 = test_client.execute("""
            SELECT MATCH(title, content) AGAINST('algorithm test' IN NATURAL LANGUAGE MODE) as score
            FROM test_fulltext 
            WHERE MATCH(title, content) AGAINST('algorithm test' IN NATURAL LANGUAGE MODE)
        """)
        
        # Both should return results, but scores might be different
        assert len(result_tfidf.rows) > 0
        assert len(result_bm25.rows) > 0
        assert result_tfidf.rows[0][0] >= 0
        assert result_bm25.rows[0][0] >= 0


if __name__ == "__main__":
    pytest.main([__file__])
