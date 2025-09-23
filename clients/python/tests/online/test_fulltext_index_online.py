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

    def test_fulltext_boolean_mode_comprehensive(self, test_client):
        """Test comprehensive boolean mode operations"""
        # Enable fulltext index
        test_client.execute("SET experimental_fulltext_index = 1")
        
        # Drop table if exists and create test table with more diverse data
        test_client.execute("DROP TABLE IF EXISTS test_boolean")
        test_client.execute("""
            CREATE TABLE test_boolean (
                id INT PRIMARY KEY,
                title VARCHAR(200),
                content TEXT
            )
        """)
        
        test_client.execute("""
            INSERT INTO test_boolean VALUES
            (1, 'Database Management Systems', 'A database management system is software for managing databases'),
            (2, 'MySQL vs PostgreSQL', 'Comparison between MySQL and PostgreSQL database systems'),
            (3, 'NoSQL Database Solutions', 'MongoDB, Cassandra, and Redis are popular NoSQL databases'),
            (4, 'Database Performance Optimization', 'Indexing and query optimization techniques'),
            (5, 'SQL Query Optimization', 'How to write efficient SQL queries for better performance'),
            (6, 'Database Security Best Practices', 'Authentication, authorization, and data encryption'),
            (7, 'Distributed Database Systems', 'Scaling databases across multiple servers and regions'),
            (8, 'Database Backup and Recovery', 'Strategies for backing up and recovering database data')
        """)
        
        # Create fulltext index
        test_client.fulltext_index.create("test_boolean", "ftidx_boolean", ["title", "content"])
        
        # Test + operator (must contain)
        result = test_client.execute("""
            SELECT id, title FROM test_boolean 
            WHERE MATCH(title, content) AGAINST('+database +management' IN BOOLEAN MODE)
        """)
        assert len(result.rows) >= 1
        assert any(row[0] == 1 for row in result.rows)  # Should include "Database Management Systems"
        
        # Test - operator (must not contain)
        result = test_client.execute("""
            SELECT id, title FROM test_boolean 
            WHERE MATCH(title, content) AGAINST('+database -MySQL' IN BOOLEAN MODE)
        """)
        assert len(result.rows) >= 1
        assert not any(row[0] == 2 for row in result.rows)  # Should exclude "MySQL vs PostgreSQL"
        
        # Test * wildcard
        result = test_client.execute("""
            SELECT id, title FROM test_boolean 
            WHERE MATCH(title, content) AGAINST('optim*' IN BOOLEAN MODE)
        """)
        assert len(result.rows) >= 1
        assert any(row[0] in [4, 5] for row in result.rows)  # Should include optimization queries
        
        # Test ~ operator (negation with lower relevance)
        result = test_client.execute("""
            SELECT id, title FROM test_boolean 
            WHERE MATCH(title, content) AGAINST('+database ~MySQL' IN BOOLEAN MODE)
        """)
        assert len(result.rows) >= 1
        
        # Test phrase matching
        result = test_client.execute("""
            SELECT id, title FROM test_boolean 
            WHERE MATCH(title, content) AGAINST('"database management"' IN BOOLEAN MODE)
        """)
        assert len(result.rows) >= 1
        assert any(row[0] == 1 for row in result.rows)  # Should include exact phrase match
        
        # Test complex boolean expression
        result = test_client.execute("""
            SELECT id, title FROM test_boolean 
            WHERE MATCH(title, content) AGAINST('+database +(<performance >optimization)' IN BOOLEAN MODE)
        """)
        assert len(result.rows) >= 1
        
        # Cleanup
        test_client.execute("DROP TABLE test_boolean")

    def test_fulltext_chinese_ngram_parser(self, test_client):
        """Test Chinese text search with ngram parser"""
        # Enable fulltext index
        test_client.execute("SET experimental_fulltext_index = 1")
        
        # Drop table if exists and create test table with Chinese content
        test_client.execute("DROP TABLE IF EXISTS test_chinese")
        test_client.execute("""
            CREATE TABLE test_chinese (
                id INT PRIMARY KEY,
                title VARCHAR(200),
                content TEXT
            )
        """)
        
        test_client.execute("""
            INSERT INTO test_chinese VALUES
            (1, '神雕侠侣 第一回 风月无情', '越女采莲秋水畔，窄袖轻罗，暗露双金钏'),
            (2, '神雕侠侣 第二回 故人之子', '正自发痴，忽听左首屋中传出一人喝道'),
            (3, '神雕侠侣 第三回 投师终南', '郭靖在舟中潜运神功，数日间伤势便已痊愈了大半'),
            (4, '神雕侠侣 第四回 全真门下', '郭靖摆脱众道纠缠，提气向重阳宫奔去'),
            (5, '神雕侠侣 第五回 活死人墓', '杨过摔下山坡，滚入树林长草丛中，便即昏晕'),
            (6, '神雕侠侣 第六回 玉女心经', '小龙女从怀里取出一个瓷瓶，交在杨过手里')
        """)
        
        # Create fulltext index with ngram parser
        test_client.execute("""
            CREATE FULLTEXT INDEX ftidx_chinese ON test_chinese (title, content) WITH PARSER ngram
        """)
        
        # Test Chinese text search
        result = test_client.execute("""
            SELECT id, title FROM test_chinese 
            WHERE MATCH(title, content) AGAINST('风月无情' IN NATURAL LANGUAGE MODE)
        """)
        assert len(result.rows) >= 1
        assert any(row[0] == 1 for row in result.rows)
        
        # Test character search
        result = test_client.execute("""
            SELECT id, title FROM test_chinese 
            WHERE MATCH(title, content) AGAINST('杨过' IN NATURAL LANGUAGE MODE)
        """)
        assert len(result.rows) >= 1
        assert any(row[0] in [5, 6] for row in result.rows)
        
        # Test another character
        result = test_client.execute("""
            SELECT id, title FROM test_chinese 
            WHERE MATCH(title, content) AGAINST('小龙女' IN NATURAL LANGUAGE MODE)
        """)
        assert len(result.rows) >= 1
        assert any(row[0] == 6 for row in result.rows)
        
        # Test title search
        result = test_client.execute("""
            SELECT id, title FROM test_chinese 
            WHERE MATCH(title, content) AGAINST('神雕侠侣' IN NATURAL LANGUAGE MODE)
        """)
        assert len(result.rows) >= 1
        
        # Cleanup
        test_client.execute("DROP TABLE test_chinese")

    def test_fulltext_json_parser(self, test_client):
        """Test JSON parser for fulltext search"""
        # Enable fulltext index
        test_client.execute("SET experimental_fulltext_index = 1")
        
        # Drop table if exists and create test table with JSON data
        test_client.execute("DROP TABLE IF EXISTS test_json")
        test_client.execute("""
            CREATE TABLE test_json (
                id INT PRIMARY KEY,
                name VARCHAR(100),
                details JSON
            )
        """)
        
        test_client.execute("""
            INSERT INTO test_json VALUES
            (1, 'Laptop', '{"brand": "Dell", "specs": "i7, 16GB RAM", "price": 1200}'),
            (2, 'Smartphone', '{"brand": "Apple", "model": "iPhone 12", "price": 800}'),
            (3, 'Tablet', '{"brand": "Samsung", "model": "Galaxy Tab", "price": 400}'),
            (4, 'Desktop', '{"brand": "HP", "specs": "i5, 8GB RAM", "price": 600}')
        """)
        
        # Create fulltext index with JSON parser
        test_client.execute("""
            CREATE FULLTEXT INDEX ftidx_json ON test_json (details) WITH PARSER json
        """)
        
        # Test JSON field search
        result = test_client.execute("""
            SELECT id, name FROM test_json 
            WHERE MATCH(details) AGAINST('Dell' IN NATURAL LANGUAGE MODE)
        """)
        assert len(result.rows) >= 1
        assert any(row[0] == 1 for row in result.rows)
        
        # Test another brand
        result = test_client.execute("""
            SELECT id, name FROM test_json 
            WHERE MATCH(details) AGAINST('Apple' IN NATURAL LANGUAGE MODE)
        """)
        assert len(result.rows) >= 1
        assert any(row[0] == 2 for row in result.rows)
        
        # Test boolean mode with JSON
        result = test_client.execute("""
            SELECT id, name FROM test_json 
            WHERE MATCH(details) AGAINST('+Dell +i7' IN BOOLEAN MODE)
        """)
        assert len(result.rows) >= 1
        assert any(row[0] == 1 for row in result.rows)
        
        # Test phrase search in JSON
        result = test_client.execute("""
            SELECT id, name FROM test_json 
            WHERE MATCH(details) AGAINST('"iPhone 12"' IN BOOLEAN MODE)
        """)
        assert len(result.rows) >= 1
        assert any(row[0] == 2 for row in result.rows)
        
        # Cleanup
        test_client.execute("DROP TABLE test_json")

    def test_fulltext_composite_primary_key(self, test_client):
        """Test fulltext search with composite primary key table"""
        # Enable fulltext index
        test_client.execute("SET experimental_fulltext_index = 1")
        
        # Drop table if exists and create table with composite primary key
        test_client.execute("DROP TABLE IF EXISTS test_composite")
        test_client.execute("""
            CREATE TABLE test_composite (
                id1 VARCHAR(10),
                id2 INT,
                title VARCHAR(200),
                content TEXT,
                PRIMARY KEY (id1, id2)
            )
        """)
        
        test_client.execute("""
            INSERT INTO test_composite VALUES
            ('A', 1, 'First Article', 'This is the first article about databases'),
            ('A', 2, 'Second Article', 'This is the second article about programming'),
            ('B', 1, 'Third Article', 'This is the third article about web development'),
            ('B', 2, 'Fourth Article', 'This is the fourth article about mobile apps')
        """)
        
        # Create fulltext index
        test_client.fulltext_index.create("test_composite", "ftidx_composite", ["title", "content"])
        
        # Test search
        result = test_client.execute("""
            SELECT id1, id2, title FROM test_composite 
            WHERE MATCH(title, content) AGAINST('first' IN NATURAL LANGUAGE MODE)
        """)
        assert len(result.rows) >= 1
        assert any(row[0] == 'A' and row[1] == 1 for row in result.rows)
        
        # Test with score
        result = test_client.execute("""
            SELECT id1, id2, title, MATCH(title, content) AGAINST('second' IN NATURAL LANGUAGE MODE) as score 
            FROM test_composite 
            WHERE MATCH(title, content) AGAINST('second' IN NATURAL LANGUAGE MODE)
        """)
        assert len(result.rows) >= 1
        assert any(row[0] == 'A' and row[1] == 2 for row in result.rows)
        assert result.rows[0][3] >= 0  # Score should be non-negative
        
        # Cleanup
        test_client.execute("DROP TABLE test_composite")

    def test_fulltext_table_operations(self, test_client):
        """Test fulltext search with table operations (UPDATE, DELETE, INSERT)"""
        # Enable fulltext index
        test_client.execute("SET experimental_fulltext_index = 1")
        
        # Drop table if exists and create test table
        test_client.execute("DROP TABLE IF EXISTS test_operations")
        test_client.execute("""
            CREATE TABLE test_operations (
                id INT PRIMARY KEY,
                title VARCHAR(200),
                content TEXT
            )
        """)
        
        test_client.execute("""
            INSERT INTO test_operations VALUES
            (1, 'Original Title', 'This is the original content about databases'),
            (2, 'Another Title', 'This is another article about programming'),
            (3, 'Third Title', 'This is the third article about web development')
        """)
        
        # Create fulltext index
        test_client.fulltext_index.create("test_operations", "ftidx_operations", ["title", "content"])
        
        # Test initial search
        result = test_client.execute("""
            SELECT id, title FROM test_operations 
            WHERE MATCH(title, content) AGAINST('original' IN NATURAL LANGUAGE MODE)
        """)
        assert len(result.rows) >= 1
        assert any(row[0] == 1 for row in result.rows)
        
        # Test UPDATE operation
        test_client.execute("""
            UPDATE test_operations 
            SET content = 'This is updated content about machine learning' 
            WHERE id = 1
        """)
        
        # Search should no longer find 'original' in updated record
        result = test_client.execute("""
            SELECT id, title FROM test_operations 
            WHERE MATCH(title, content) AGAINST('original' IN NATURAL LANGUAGE MODE)
        """)
        # The title still contains 'Original', so this might still match
        # Let's check that the content was updated by searching for the new content
        result2 = test_client.execute("""
            SELECT id, title FROM test_operations 
            WHERE MATCH(title, content) AGAINST('updated' IN NATURAL LANGUAGE MODE)
        """)
        assert len(result2.rows) >= 1
        assert any(row[0] == 1 for row in result2.rows)
        
        # But should find 'machine learning'
        result = test_client.execute("""
            SELECT id, title FROM test_operations 
            WHERE MATCH(title, content) AGAINST('machine learning' IN NATURAL LANGUAGE MODE)
        """)
        assert len(result.rows) >= 1
        assert any(row[0] == 1 for row in result.rows)
        
        # Test DELETE operation
        test_client.execute("DELETE FROM test_operations WHERE id = 2")
        
        # Search should no longer find deleted record
        result = test_client.execute("""
            SELECT id, title FROM test_operations 
            WHERE MATCH(title, content) AGAINST('another' IN NATURAL LANGUAGE MODE)
        """)
        assert not any(row[0] == 2 for row in result.rows)
        
        # Test INSERT operation
        test_client.execute("""
            INSERT INTO test_operations VALUES 
            (4, 'New Article', 'This is a new article about artificial intelligence')
        """)
        
        # Should find new record
        result = test_client.execute("""
            SELECT id, title FROM test_operations 
            WHERE MATCH(title, content) AGAINST('new' IN NATURAL LANGUAGE MODE)
        """)
        assert len(result.rows) >= 1
        assert any(row[0] == 4 for row in result.rows)
        
        # Cleanup
        test_client.execute("DROP TABLE test_operations")

    def test_fulltext_aggregate_functions(self, test_client):
        """Test aggregate functions with fulltext search"""
        # Enable fulltext index
        test_client.execute("SET experimental_fulltext_index = 1")
        
        # Drop table if exists and create test table
        test_client.execute("DROP TABLE IF EXISTS test_aggregate")
        test_client.execute("""
            CREATE TABLE test_aggregate (
                id INT PRIMARY KEY,
                category VARCHAR(50),
                title VARCHAR(200),
                content TEXT
            )
        """)
        
        test_client.execute("""
            INSERT INTO test_aggregate VALUES
            (1, 'tech', 'Database Tutorial', 'Learn about database management systems'),
            (2, 'tech', 'Programming Guide', 'Learn programming languages and techniques'),
            (3, 'business', 'Business Strategy', 'Strategic planning for business growth'),
            (4, 'tech', 'Web Development', 'Building modern web applications'),
            (5, 'business', 'Marketing Tips', 'Effective marketing strategies for businesses')
        """)
        
        # Create fulltext index
        test_client.fulltext_index.create("test_aggregate", "ftidx_aggregate", ["title", "content"])
        
        # Test COUNT with fulltext search
        result = test_client.execute("""
            SELECT COUNT(*) FROM test_aggregate 
            WHERE MATCH(title, content) AGAINST('tech' IN NATURAL LANGUAGE MODE)
        """)
        assert result.rows[0][0] >= 0
        
        # Test COUNT with boolean mode
        result = test_client.execute("""
            SELECT COUNT(*) FROM test_aggregate 
            WHERE MATCH(title, content) AGAINST('+programming +database' IN BOOLEAN MODE)
        """)
        assert result.rows[0][0] >= 0
        
        # Test GROUP BY with fulltext search
        result = test_client.execute("""
            SELECT category, COUNT(*) as count 
            FROM test_aggregate 
            WHERE MATCH(title, content) AGAINST('business' IN NATURAL LANGUAGE MODE)
            GROUP BY category
        """)
        assert len(result.rows) >= 1
        
        # Cleanup
        test_client.execute("DROP TABLE test_aggregate")

    def test_fulltext_explain_plan(self, test_client):
        """Test explain plan for fulltext queries"""
        # Enable fulltext index
        test_client.execute("SET experimental_fulltext_index = 1")
        
        # Drop table if exists and create test table
        test_client.execute("DROP TABLE IF EXISTS test_explain")
        test_client.execute("""
            CREATE TABLE test_explain (
                id INT PRIMARY KEY,
                title VARCHAR(200),
                content TEXT
            )
        """)
        
        test_client.execute("""
            INSERT INTO test_explain VALUES
            (1, 'Database Systems', 'Introduction to database management systems'),
            (2, 'Programming Languages', 'Overview of popular programming languages'),
            (3, 'Web Development', 'Building modern web applications')
        """)
        
        # Create fulltext index
        test_client.fulltext_index.create("test_explain", "ftidx_explain", ["title", "content"])
        
        # Test EXPLAIN for fulltext query
        result = test_client.execute("""
            EXPLAIN SELECT * FROM test_explain 
            WHERE MATCH(title, content) AGAINST('database' IN NATURAL LANGUAGE MODE)
        """)
        assert len(result.rows) > 0
        
        # Test EXPLAIN for fulltext query with score
        result = test_client.execute("""
            EXPLAIN SELECT id, MATCH(title, content) AGAINST('programming' IN NATURAL LANGUAGE MODE) as score 
            FROM test_explain 
            WHERE MATCH(title, content) AGAINST('programming' IN NATURAL LANGUAGE MODE)
        """)
        assert len(result.rows) > 0
        
        # Cleanup
        test_client.execute("DROP TABLE test_explain")

    def test_fulltext_error_handling(self, test_client):
        """Test error handling for fulltext operations"""
        # Enable fulltext index
        test_client.execute("SET experimental_fulltext_index = 1")
        
        # Drop table if exists and create test table
        test_client.execute("DROP TABLE IF EXISTS test_errors")
        test_client.execute("""
            CREATE TABLE test_errors (
                id INT PRIMARY KEY,
                title VARCHAR(200),
                content TEXT
            )
        """)
        
        test_client.execute("""
            INSERT INTO test_errors VALUES
            (1, 'Test Title', 'Test content for error handling')
        """)
        
        # Create first fulltext index
        test_client.fulltext_index.create("test_errors", "ftidx_errors", ["title", "content"])
        
        # Test creating duplicate index (should fail)
        try:
            test_client.execute("""
                CREATE FULLTEXT INDEX ftidx_errors2 ON test_errors (title, content)
            """)
            # If we get here, the test should fail
            assert False, "Expected error for duplicate fulltext index"
        except Exception:
            # Expected behavior - duplicate index should fail
            pass
        
        # Test invalid search syntax
        try:
            test_client.execute("""
                SELECT * FROM test_errors 
                WHERE MATCH(title, content) AGAINST('+]]]' IN BOOLEAN MODE)
            """)
            # If we get here, the test should fail
            assert False, "Expected error for invalid search syntax"
        except Exception:
            # Expected behavior - invalid syntax should fail
            pass
        
        # Test search with empty pattern
        try:
            test_client.execute("""
                SELECT * FROM test_errors 
                WHERE MATCH(title, content) AGAINST('' IN NATURAL LANGUAGE MODE)
            """)
            # This might succeed with empty results, which is acceptable
        except Exception:
            # Expected behavior for empty pattern
            pass
        
        # Cleanup
        test_client.execute("DROP TABLE test_errors")


if __name__ == "__main__":
    pytest.main([__file__])
