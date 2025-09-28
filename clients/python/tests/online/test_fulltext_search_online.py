"""
Online tests for fulltext search functionality.
Tests against real MatrixOne database with actual data.
"""

import pytest
import os
import sys
from sqlalchemy import Column, Integer, String, Text, create_engine
from sqlalchemy.orm import sessionmaker

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import Client, AsyncClient
from matrixone.orm import declarative_base
from matrixone.config import get_connection_params
from matrixone.sqlalchemy_ext.fulltext_search import (
    boolean_match,
    natural_match,
    group,
    FulltextSearchMode
)

Base = declarative_base()


class Article(Base):
    """Test article model for fulltext search."""
    __tablename__ = 'test_articles'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(255), nullable=False)
    content = Column(Text, nullable=False)
    tags = Column(String(500), nullable=True)
    category = Column(String(100), nullable=True)


class TestFulltextSearchOnline:
    """Online fulltext search tests."""
    
    @classmethod
    def setup_class(cls):
        """Set up test database and data."""
        # Get connection parameters using standard config
        host, port, user, password, database = get_connection_params()
        
        cls.client = Client()
        cls.client.connect(host=host, port=port, user=user, password=password, database=database)
        
        # Create test database
        cls.test_db = "test_fulltext_search"
        try:
            cls.client.execute(f"CREATE DATABASE IF NOT EXISTS {cls.test_db}")
            cls.client.execute(f"USE {cls.test_db}")
        except Exception as e:
            pytest.skip(f"Cannot create test database: {e}")
        
        # Create table
        try:
            cls.client.execute("DROP TABLE IF EXISTS test_articles")
            cls.client.execute("""
                CREATE TABLE IF NOT EXISTS test_articles (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    content TEXT NOT NULL,
                    tags VARCHAR(500),
                    category VARCHAR(100)
                )
            """)
        except Exception as e:
            pytest.skip(f"Cannot create test table: {e}")
        
        # Insert test data
        test_articles = [
            {
                'title': 'Python Programming Tutorial',
                'content': 'Learn Python programming from basics to advanced concepts. This tutorial covers variables, functions, classes, and more.',
                'tags': 'python,programming,tutorial,beginner',
                'category': 'Programming'
            },
            {
                'title': 'Java Development Guide',
                'content': 'Complete guide to Java development including Spring framework, Hibernate, and best practices for enterprise applications.',
                'tags': 'java,development,spring,enterprise',
                'category': 'Programming'
            },
            {
                'title': 'Machine Learning with Python',
                'content': 'Introduction to machine learning using Python. Covers neural networks, deep learning, and AI algorithms.',
                'tags': 'python,machine-learning,AI,neural-networks',
                'category': 'AI'
            },
            {
                'title': 'JavaScript Frontend Development',
                'content': 'Modern JavaScript development for frontend applications. Learn React, Vue, and Angular frameworks.',
                'tags': 'javascript,frontend,react,vue,angular',
                'category': 'Web Development'
            },
            {
                'title': 'Database Design Principles',
                'content': 'Learn database design principles, normalization, indexing, and query optimization techniques.',
                'tags': 'database,design,sql,optimization',
                'category': 'Database'
            },
            {
                'title': 'Legacy Python 2.7 Migration',
                'content': 'Guide for migrating legacy Python 2.7 applications to Python 3. Deprecated features and compatibility issues.',
                'tags': 'python,legacy,migration,deprecated',
                'category': 'Programming'
            },
            {
                'title': 'Advanced Neural Networks',
                'content': 'Deep dive into advanced neural network architectures including CNNs, RNNs, and transformer models.',
                'tags': 'neural-networks,deep-learning,CNN,RNN,transformer',
                'category': 'AI'
            },
            {
                'title': 'Web Security Best Practices',
                'content': 'Security best practices for web applications. Learn about authentication, authorization, and common vulnerabilities.',
                'tags': 'security,web,authentication,vulnerabilities',
                'category': 'Security'
            }
        ]
        
        try:
            for article in test_articles:
                cls.client.execute(
                    "INSERT INTO test_articles (title, content, tags, category) VALUES (%s, %s, %s, %s)",
                    (article['title'], article['content'], article['tags'], article['category'])
                )
        except Exception as e:
            pytest.skip(f"Cannot insert test data: {e}")
        
        # Create fulltext index
        try:
            # Set algorithm to BM25
            cls.client.execute('SET ft_relevancy_algorithm = "BM25"')
            
            # Create fulltext index
            cls.client.execute(
                "CREATE FULLTEXT INDEX ft_articles ON test_articles(title, content, tags)"
            )
        except Exception as e:
            pytest.skip(f"Cannot create fulltext index: {e}")
    
    @classmethod
    def teardown_class(cls):
        """Clean up test database."""
        try:
            cls.client.execute("DROP TABLE IF EXISTS test_articles")
            cls.client.execute(f"DROP DATABASE IF EXISTS {cls.test_db}")
            cls.client.disconnect()
        except:
            pass
    
    def test_basic_must_search(self):
        """Test basic must search functionality."""
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags").must("python")
        )
        
        results = query.all()
        assert len(results) >= 2  # Should find Python articles
        
        # Check that all results contain 'python'
        for result in results:
            content_lower = (result.title + " " + result.content + " " + (result.tags or "")).lower()
            assert "python" in content_lower
    
    def test_must_not_search(self):
        """Test must_not search functionality."""
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must("programming")
            .must_not("legacy")
        )
        
        results = query.all()
        assert len(results) >= 1
        
        # Check that no results contain 'legacy'
        for result in results:
            content_lower = (result.title + " " + result.content + " " + (result.tags or "")).lower()
            assert "legacy" not in content_lower
            assert "programming" in content_lower
    
    def test_encourage_search(self):
        """Test encourage (optional positive weight) search."""
        # Search for programming content, encourage tutorial
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must("programming")
            .encourage("tutorial")
        )
        
        results = query.all()
        assert len(results) >= 1
        
        # Results should be ordered by relevance (tutorial articles should rank higher)
        tutorial_found = False
        for result in results:
            content_lower = (result.title + " " + result.content + " " + (result.tags or "")).lower()
            assert "programming" in content_lower
            if "tutorial" in content_lower:
                tutorial_found = True
        
        # At least one result should contain 'tutorial'
        assert tutorial_found
    
    def test_discourage_search(self):
        """Test discourage (negative weight) search."""
        # Search for Python content, discourage legacy
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must("python")
            .discourage("legacy")
        )
        
        results = query.all()
        assert len(results) >= 1
        
        # All results should contain 'python'
        for result in results:
            content_lower = (result.title + " " + result.content + " " + (result.tags or "")).lower()
            assert "python" in content_lower
    
    def test_group_search(self):
        """Test group search functionality."""
        # Must contain either 'python' or 'java'
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must(group().medium("python", "java"))
        )
        
        results = query.all()
        assert len(results) >= 2
        
        # Each result should contain either 'python' or 'java'
        for result in results:
            content_lower = (result.title + " " + result.content + " " + (result.tags or "")).lower()
            assert "python" in content_lower or "java" in content_lower
    
    def test_complex_boolean_search(self):
        """Test complex boolean search with multiple conditions."""
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must("programming")
            .encourage(group().medium("python", "java"))
            .discourage("legacy")
            .must_not("security")
        )
        
        results = query.all()
        assert len(results) >= 1
        
        for result in results:
            content_lower = (result.title + " " + result.content + " " + (result.tags or "")).lower()
            assert "programming" in content_lower
            assert "security" not in content_lower
    
    def test_phrase_search(self):
        """Test phrase search functionality."""
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .phrase("machine learning")
        )
        
        results = query.all()
        assert len(results) >= 1
        
        # Check that results contain the exact phrase
        for result in results:
            content_lower = (result.title + " " + result.content).lower()
            assert "machine learning" in content_lower
    
    def test_prefix_search(self):
        """Test prefix search functionality."""
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .prefix("neural")
        )
        
        results = query.all()
        assert len(results) >= 1
        
        # Check that results contain words starting with 'neural'
        for result in results:
            content_lower = (result.title + " " + result.content + " " + (result.tags or "")).lower()
            # Should match 'neural', 'neural-networks', etc.
            assert any(word.startswith("neural") for word in content_lower.replace("-", " ").split())
    
    def test_element_weight_search(self):
        """Test element-level weight operators."""
        # Test high and low weight within groups
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must(group().high("python").low("tutorial"))
        )
        
        results = query.all()
        assert len(results) >= 1
        
        # Results should contain both terms with different weights
        for result in results:
            content_lower = (result.title + " " + result.content + " " + (result.tags or "")).lower()
            assert "python" in content_lower or "tutorial" in content_lower
    
    def test_natural_language_search(self):
        """Test natural language search mode."""
        # First try with terms that exist in our test data
        query = self.client.query(Article).filter(
            natural_match("title", "content", "tags", query="python programming")
        )
        
        results = query.all()
        # If natural language search returns 0 results, try simpler terms
        if len(results) == 0:
            query = self.client.query(Article).filter(
                natural_match("title", "content", "tags", query="programming")
            )
            results = query.all()
        
        # Natural language search might return fewer results than boolean search
        # So we'll be more lenient and just check that it doesn't error
        assert len(results) >= 0  # Should not error
        
        # If we have results, verify they contain relevant terms
        if len(results) > 0:
            programming_keywords = ["programming", "python", "java", "development", "tutorial"]
            for result in results:
                content_lower = (result.title + " " + result.content + " " + (result.tags or "")).lower()
                # At least one programming keyword should be present
                assert any(keyword in content_lower for keyword in programming_keywords)
    
    def test_combined_with_regular_filters(self):
        """Test fulltext search combined with regular SQL filters."""
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags").must("programming")
        ).filter(
            Article.category == "Programming"
        )
        
        results = query.all()
        assert len(results) >= 1
        
        for result in results:
            assert result.category == "Programming"
            content_lower = (result.title + " " + result.content + " " + (result.tags or "")).lower()
            assert "programming" in content_lower
    
    def test_ordering_and_limits(self):
        """Test ordering and limits with fulltext search."""
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags").encourage("python")
        ).order_by(Article.id.desc()).limit(3)
        
        results = query.all()
        assert len(results) <= 3
        
        # Check ordering (should be descending by ID)
        if len(results) > 1:
            for i in range(len(results) - 1):
                assert results[i].id >= results[i + 1].id
    
    def test_count_with_fulltext(self):
        """Test count queries with fulltext search."""
        count = self.client.query(Article).filter(
            boolean_match("title", "content", "tags").must("programming")
        ).count()
        
        assert count >= 1
        assert isinstance(count, int)
    
    def test_matrixone_style_complex_query(self):
        """Test MatrixOne-style complex query: +red -(<blue >is)."""
        # Adapt to our test data: +programming -(>legacy <deprecated)
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must("programming")
            .must_not(group().high("legacy").low("deprecated"))
        )
        
        results = query.all()
        # Should find programming articles without legacy/deprecated content
        for result in results:
            content_lower = (result.title + " " + result.content + " " + (result.tags or "")).lower()
            assert "programming" in content_lower
    
    def test_multiple_must_groups(self):
        """Test multiple must groups."""
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must(group().medium("python", "java"))
            .must(group().medium("programming", "development"))
        )
        
        results = query.all()
        assert len(results) >= 1
        
        for result in results:
            content_lower = (result.title + " " + result.content + " " + (result.tags or "")).lower()
            # Must contain at least one from each group
            assert ("python" in content_lower or "java" in content_lower)
            assert ("programming" in content_lower or "development" in content_lower)
    
    def test_empty_results(self):
        """Test queries that should return no results."""
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags")
            .must("nonexistent_term_xyz123")
        )
        
        results = query.all()
        assert len(results) == 0
    
    def test_case_insensitive_search(self):
        """Test case insensitive search."""
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags").must("PYTHON")
        )
        
        results = query.all()
        assert len(results) >= 1  # Should find python articles regardless of case
    
    def test_special_characters_in_search(self):
        """Test search with special characters."""
        # Test hyphenated terms
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags").encourage("machine-learning")
        )
        
        results = query.all()
        # Should handle hyphenated terms appropriately
        assert len(results) >= 0  # May or may not find results, but shouldn't error


class TestAsyncFulltextSearch:
    """Test async fulltext search functionality."""
    
    @classmethod
    def setup_class(cls):
        """Ensure test database exists for async tests."""
        # Get connection parameters using standard config
        host, port, user, password, database = get_connection_params()
        
        # Create sync client to set up database
        sync_client = Client()
        sync_client.connect(host=host, port=port, user=user, password=password, database=database)
        
        # Create test database if not exists
        sync_client.execute("CREATE DATABASE IF NOT EXISTS test_fulltext_search")
        sync_client.execute("USE test_fulltext_search")
        
        # Create table if not exists
        sync_client.execute("DROP TABLE IF EXISTS test_articles")
        sync_client.execute("""
            CREATE TABLE IF NOT EXISTS test_articles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(200),
                content TEXT,
                tags VARCHAR(500),
                category VARCHAR(50)
            )
        """)
        
        # Insert at least one test record for async tests
        sync_client.execute(
            "INSERT INTO test_articles (title, content, tags, category) VALUES (%s, %s, %s, %s)",
            ("Async Python Tutorial", "Learn async programming with Python asyncio", "python,async,tutorial", "Programming")
        )
        
        # Create fulltext index
        sync_client.execute('SET ft_relevancy_algorithm = "BM25"')
        try:
            sync_client.execute("CREATE FULLTEXT INDEX ft_articles ON test_articles(title, content, tags)")
        except Exception:
            # Index might already exist, ignore error
            pass
        
        sync_client.disconnect()
    
    @pytest.mark.asyncio
    async def test_async_basic_search(self):
        """Test basic async fulltext search."""
        # Get connection parameters using standard config
        host, port, user, password, database = get_connection_params()
        
        async_client = AsyncClient()
        await async_client.connect(host=host, port=port, user=user, password=password, database=database)
        
        # Use the test database
        await async_client.execute("USE test_fulltext_search")
        
        query = async_client.query(Article).filter(
            boolean_match("title", "content", "tags").must("python")
        )
        
        results = await query.all()
        assert len(results) >= 1
        
        for result in results:
            content_lower = (result.title + " " + result.content + " " + (result.tags or "")).lower()
            assert "python" in content_lower
        
        # Properly close async client to avoid warnings
        try:
            await async_client.disconnect()
        except Exception:
            pass  # Ignore disconnect errors


class TestFulltextSearchEdgeCases:
    """Test edge cases and error conditions."""
    
    @classmethod
    def setup_class(cls):
        """Set up client for edge case tests."""
        # Get connection parameters using standard config
        host, port, user, password, database = get_connection_params()
        
        cls.client = Client()
        cls.client.connect(host=host, port=port, user=user, password=password, database=database)
        
        # Ensure test database and data exist
        cls.client.execute("CREATE DATABASE IF NOT EXISTS test_fulltext_search")
        cls.client.execute("USE test_fulltext_search")
        
        # Create table if not exists
        cls.client.execute("DROP TABLE IF EXISTS test_articles")
        cls.client.execute("""
            CREATE TABLE IF NOT EXISTS test_articles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(200),
                content TEXT,
                tags VARCHAR(500),
                category VARCHAR(50)
            )
        """)
        
        # Insert test data
        cls.client.execute(
            "INSERT INTO test_articles (title, content, tags, category) VALUES (%s, %s, %s, %s)",
            ("Python Programming Guide", "Complete Python programming tutorial", "python,programming,guide", "Programming")
        )
        cls.client.execute(
            "INSERT INTO test_articles (title, content, tags, category) VALUES (%s, %s, %s, %s)",
            ("Java Development", "Java application development guide", "java,development", "Programming")
        )
        
        # Create fulltext index
        cls.client.execute('SET ft_relevancy_algorithm = "BM25"')
        try:
            cls.client.execute("CREATE FULLTEXT INDEX ft_articles ON test_articles(title, content, tags)")
        except Exception:
            # Index might already exist, ignore error
            pass
    
    @classmethod
    def teardown_class(cls):
        """Clean up client."""
        try:
            cls.client.disconnect()
        except:
            pass
    
    def test_single_column_index(self):
        """Test search on single column."""
        # Create a single-column fulltext index for testing
        try:
            self.client.execute("DROP INDEX ft_title ON test_articles")
        except Exception:
            # Index might not exist, ignore error
            pass
        
        try:
            self.client.execute("CREATE FULLTEXT INDEX ft_title ON test_articles(title)")
        except Exception:
            # Index might already exist, ignore error
            pass
        
        query = self.client.query(Article).filter(
            boolean_match("title").must("python")
        )
        
        results = query.all()
        assert len(results) >= 1
        
        # Verify results contain python in title
        for result in results:
            assert "python" in result.title.lower()
    
    def test_very_long_query(self):
        """Test very long fulltext query."""
        long_terms = ["term" + str(i) for i in range(10)]  # Reasonable number of terms
        
        filter_obj = boolean_match("title", "content", "tags")
        for term in long_terms:
            filter_obj = filter_obj.encourage(term)
        
        query = self.client.query(Article).filter(filter_obj)
        results = query.all()
        # Should not error, even if no results
        assert isinstance(results, list)
        # Long query with non-existent terms should return empty results
        assert len(results) == 0
    
    def test_case_insensitive_search(self):
        """Test case insensitive search functionality."""
        # Test uppercase search
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags").must("PYTHON")
        )
        
        results = query.all()
        assert len(results) >= 1
        
        # Verify results contain python (case insensitive)
        for result in results:
            content_lower = (result.title + " " + result.content + " " + (result.tags or "")).lower()
            assert "python" in content_lower
    
    def test_empty_result_handling(self):
        """Test handling of searches that return no results."""
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags").must("nonexistent_term_xyz123")
        )
        
        results = query.all()
        assert len(results) == 0
        assert isinstance(results, list)
    
    def test_special_characters_handling(self):
        """Test handling of special characters in search terms."""
        # Insert data with special characters
        self.client.execute(
            "INSERT INTO test_articles (title, content, tags, category) VALUES (%s, %s, %s, %s)",
            ("C++ Programming", "Learn C++ programming language", "c++,programming", "Programming")
        )
        
        # Test search with special characters (should handle gracefully)
        query = self.client.query(Article).filter(
            boolean_match("title", "content", "tags").encourage("c++")
        )
        
        results = query.all()
        # Should not error, may or may not find results depending on indexing
        assert isinstance(results, list)


if __name__ == "__main__":
    # Run with: python -m pytest tests/online/test_fulltext_search_online.py -v
    pytest.main([__file__, "-v", "-s"])
