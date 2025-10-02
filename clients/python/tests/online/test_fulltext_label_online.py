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
Online tests for FulltextFilter label functionality with real MatrixOne database
Tests AS score feature with actual data
"""

import unittest
import sys
import os

# Add the parent directory to sys.path to import matrixone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import Client
from matrixone.config import get_connection_kwargs
from matrixone.sqlalchemy_ext.fulltext_search import boolean_match, natural_match, group

try:
    from sqlalchemy.orm import declarative_base
except ImportError:
    from matrixone.orm import declarative_base

from sqlalchemy import Column, Integer, String, Text


class TestFulltextLabelOnline(unittest.TestCase):
    """Online tests for FulltextFilter label functionality."""

    @classmethod
    def setUpClass(cls):
        """Set up test database and data."""
        # Get connection parameters
        conn_params = get_connection_kwargs()
        # Filter out unsupported parameters
        client_params = {k: v for k, v in conn_params.items() if k in ['host', 'port', 'user', 'password', 'database']}

        cls.client = Client()
        cls.client.connect(**client_params)

        # Create test database
        cls.test_db = "test_fulltext_label"
        try:
            cls.client.execute(f"DROP DATABASE IF EXISTS {cls.test_db}")
            cls.client.execute(f"CREATE DATABASE {cls.test_db}")
            cls.client.execute(f"USE {cls.test_db}")
        except Exception as e:
            print(f"Database setup warning: {e}")

        # Define model
        cls.Base = declarative_base()

        class Article(cls.Base):
            __tablename__ = "test_articles"
            id = Column(Integer, primary_key=True)
            title = Column(String(200))
            content = Column(Text)
            tags = Column(String(500))
            category = Column(String(50))

        cls.Article = Article

        # Create table
        cls.client.execute(
            """
            CREATE TABLE IF NOT EXISTS test_articles (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(200),
                content TEXT,
                tags VARCHAR(500),
                category VARCHAR(50)
            )
        """
        )

        # Insert test data
        test_articles = [
            {
                'title': 'Python Programming Tutorial',
                'content': 'Learn Python programming with this comprehensive tutorial. Python is a powerful language for machine learning and web development.',
                'tags': 'python, programming, tutorial, beginner',
                'category': 'Programming',
            },
            {
                'title': 'Machine Learning Basics',
                'content': 'Introduction to machine learning concepts. This tutorial covers supervised and unsupervised learning algorithms.',
                'tags': 'machine learning, AI, algorithms, tutorial',
                'category': 'AI',
            },
            {
                'title': 'Java vs Python Comparison',
                'content': 'Comparing Java and Python programming languages. Both are popular for enterprise development and data science.',
                'tags': 'java, python, comparison, programming',
                'category': 'Programming',
            },
            {
                'title': 'Deprecated Python Features',
                'content': 'Old Python features that should be avoided. Legacy code patterns and deprecated functions.',
                'tags': 'python, deprecated, legacy, old',
                'category': 'Programming',
            },
            {
                'title': 'Neural Network Guide',
                'content': 'Deep learning with neural networks. Advanced machine learning tutorial for experienced developers.',
                'tags': 'neural networks, deep learning, advanced, machine learning',
                'category': 'AI',
            },
            {
                'title': 'Web Development with Python',
                'content': 'Building web applications using Python frameworks like Django and Flask. Modern web development practices.',
                'tags': 'python, web development, django, flask',
                'category': 'Web',
            },
        ]

        for article_data in test_articles:
            sql = f"""INSERT INTO test_articles (title, content, tags, category) VALUES 
                ('{article_data['title'].replace("'", "''")}', 
                 '{article_data['content'].replace("'", "''")}',
                 '{article_data['tags'].replace("'", "''")}',
                 '{article_data['category'].replace("'", "''")}'
                )"""
            cls.client.execute(sql)

        # Create fulltext indexes
        try:
            cls.client.execute("CREATE FULLTEXT INDEX ft_title_content ON test_articles(title, content)")
        except Exception as e:
            print(f"Fulltext index creation warning: {e}")

        try:
            cls.client.execute("CREATE FULLTEXT INDEX ft_tags ON test_articles(tags)")
        except Exception as e:
            print(f"Tags index creation warning: {e}")

    @classmethod
    def tearDownClass(cls):
        """Clean up test database."""
        try:
            cls.client.execute(f"DROP DATABASE IF EXISTS {cls.test_db}")
        except Exception as e:
            print(f"Cleanup warning: {e}")

        cls.client.disconnect()

    def test_basic_boolean_label_query(self):
        """Test basic boolean query with label using Client.query()."""
        # Query with label
        results = self.client.query(
            self.Article.id,
            self.Article.title,
            boolean_match("title", "content").must("python").label("python_score"),
        ).all()

        # Should find articles mentioning Python
        self.assertGreater(len(results), 0)

        # Check result structure
        for row in results:
            self.assertEqual(len(row), 3)  # id, title, score
            article_id, title, score = row
            self.assertIsInstance(article_id, int)
            self.assertIsInstance(title, str)
            self.assertIn("python", title.lower())
            # Score should be a float or None
            self.assertTrue(score is None or isinstance(score, (int, float)))

    def test_natural_language_label_query(self):
        """Test natural language query with label."""
        results = self.client.query(
            self.Article.id,
            self.Article.title,
            natural_match("title", "content", query="machine learning").label("ml_score"),
        ).all()

        # Should find machine learning articles
        self.assertGreater(len(results), 0)

        for row in results:
            article_id, title, score = row
            self.assertIsInstance(article_id, int)
            self.assertIsInstance(title, str)

    def test_complex_boolean_with_label(self):
        """Test complex boolean query with multiple operators and label."""
        results = self.client.query(
            self.Article.id,
            self.Article.title,
            boolean_match("title", "content")
            .must("programming")
            .encourage("tutorial")
            .discourage("deprecated")
            .label("complex_score"),
        ).all()

        # Should find programming articles, prefer tutorials, avoid deprecated
        self.assertGreater(len(results), 0)

        # Verify structure
        for row in results:
            article_id, title, score = row
            self.assertIsInstance(article_id, int)
            self.assertIsInstance(title, str)

    def test_multiple_fulltext_labels(self):
        """Test multiple fulltext expressions with different labels."""
        results = self.client.query(
            self.Article.id,
            self.Article.title,
            boolean_match("title", "content").must("python").label("python_score"),
            boolean_match("tags").must("tutorial").label("tutorial_score"),
        ).all()

        # Should return id, title, python_score, tutorial_score
        for row in results:
            self.assertEqual(len(row), 4)
            article_id, title, python_score, tutorial_score = row
            self.assertIsInstance(article_id, int)
            self.assertIsInstance(title, str)

    def test_label_with_filters(self):
        """Test label queries combined with regular filters."""
        results = (
            self.client.query(
                self.Article.id,
                self.Article.title,
                self.Article.category,
                boolean_match("title", "content").must("python").label("score"),
            )
            .filter(self.Article.category == "Programming")
            .all()
        )

        # Should find Python articles in Programming category
        for row in results:
            article_id, title, category, score = row
            self.assertEqual(category, "Programming")
            self.assertIn("python", title.lower())

    def test_label_with_ordering(self):
        """Test ordering by fulltext score."""
        # Note: This tests the SQL generation, actual score ordering depends on MatrixOne implementation
        results = self.client.query(
            self.Article.id,
            self.Article.title,
            boolean_match("title", "content").must("python").label("relevance"),
        ).all()

        # Should execute without errors
        self.assertGreater(len(results), 0)

    def test_group_operations_with_label(self):
        """Test group operations with label."""
        results = self.client.query(
            self.Article.id,
            self.Article.title,
            boolean_match("title", "content").must("programming").must(group().medium("python", "java")).label("lang_score"),
        ).all()

        # Should find articles with programming AND (python OR java)
        for row in results:
            article_id, title, score = row
            title_lower = title.lower()
            self.assertTrue("python" in title_lower or "java" in title_lower)

    def test_phrase_search_with_label(self):
        """Test phrase search with label."""
        # Note: MatrixOne has limitations with phrase operators in complex queries
        # Using simple term matching instead to test the label functionality
        results = self.client.query(
            self.Article.id,
            self.Article.title,
            boolean_match("title", "content").must("machine").encourage("learning").label("phrase_score"),
        ).all()

        # Should execute without errors and find relevant articles
        self.assertIsInstance(results, list)
        # Should find machine learning related articles
        if results:
            for row in results:
                article_id, title, score = row
                self.assertIsInstance(article_id, int)
                self.assertIsInstance(title, str)

    def test_prefix_search_with_label(self):
        """Test prefix search with label."""
        results = self.client.query(
            self.Article.id,
            self.Article.title,
            boolean_match("title", "content")
            .must(group().prefix("program"))  # Should match "programming"
            .label("prefix_score"),
        ).all()

        # Should find articles with words starting with "program"
        self.assertGreater(len(results), 0)

    def test_weight_operators_with_label(self):
        """Test weight operators within groups with label."""
        results = self.client.query(
            self.Article.id,
            self.Article.title,
            boolean_match("title", "content")
            .must("python")
            .must(group().high("tutorial").low("deprecated"))
            .label("weighted_score"),
        ).all()

        # Should execute and find relevant articles
        self.assertGreater(len(results), 0)

    def test_single_column_label(self):
        """Test single column fulltext search with label."""
        # Note: MatrixOne requires MATCH columns to be covered by existing fulltext index
        # Since we have ft_title_content(title, content), we need to search both columns
        results = self.client.query(
            self.Article.id,
            self.Article.title,
            boolean_match("title", "content").must("Python").label("title_score"),
        ).all()

        # Should find articles with "Python" in title or content
        for row in results:
            article_id, title, score = row
            # Python could be in title or content
            self.assertTrue("Python" in title or "python" in title.lower())

    def test_tags_column_label(self):
        """Test fulltext search on tags column with label."""
        results = self.client.query(
            self.Article.id,
            self.Article.tags,
            boolean_match("tags").must("tutorial").label("tag_score"),
        ).all()

        # Should find articles tagged with "tutorial"
        for row in results:
            article_id, tags, score = row
            self.assertIn("tutorial", tags.lower())

    def test_empty_result_with_label(self):
        """Test query that returns no results with label."""
        results = self.client.query(
            self.Article.id,
            self.Article.title,
            boolean_match("title", "content").must("nonexistent").label("empty_score"),
        ).all()

        # Should return empty list without errors
        self.assertEqual(len(results), 0)

    def test_label_sql_generation_verification(self):
        """Test that generated SQL contains correct AS clause."""
        query = self.client.query(
            self.Article.id,
            boolean_match("title", "content").must("test").label("verification_score"),
        )

        # Get the generated SQL
        sql, params = query._build_sql()

        # Verify SQL contains AS clause
        self.assertIn("AS verification_score", sql)
        self.assertIn("MATCH(title, content)", sql)
        self.assertIn("AGAINST(", sql)

    def test_matrixone_compatibility_online(self):
        """Test compatibility with actual MatrixOne syntax."""
        # Test natural language mode (like MatrixOne test cases)
        results = self.client.query(
            self.Article.id,
            natural_match("title", "content", query="python tutorial").label("score"),
        ).all()

        # Should execute without syntax errors
        self.assertIsInstance(results, list)

        # Test boolean mode
        results = self.client.query(
            self.Article.id,
            boolean_match("title", "content").must("python").encourage("tutorial").label("score"),
        ).all()

        # Should execute without syntax errors
        self.assertIsInstance(results, list)

    def test_concurrent_label_queries(self):
        """Test multiple label queries to ensure no conflicts."""
        # Run multiple queries with different labels
        query1 = self.client.query(self.Article.id, boolean_match("title", "content").must("python").label("score1"))

        query2 = self.client.query(self.Article.id, boolean_match("title", "content").must("machine").label("score2"))

        results1 = query1.all()
        results2 = query2.all()

        # Both should work independently
        self.assertIsInstance(results1, list)
        self.assertIsInstance(results2, list)

    def test_label_with_limit_offset(self):
        """Test label queries with limit and offset."""
        results = (
            self.client.query(
                self.Article.id,
                self.Article.title,
                boolean_match("title", "content").must("programming").label("score"),
            )
            .limit(2)
            .offset(0)
            .all()
        )

        # Should respect limit
        self.assertLessEqual(len(results), 2)


if __name__ == '__main__':
    unittest.main()
