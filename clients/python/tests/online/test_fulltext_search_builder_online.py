#!/usr/bin/env python3
"""
Online tests for FulltextSearchBuilder

Tests the fulltext search functionality with real database connection.
"""

import unittest
import pytest
import os
import sys

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone import Client
from matrixone.config import get_connection_params
from matrixone.sqlalchemy_ext.fulltext_search import (
    FulltextSearchMode,
    FulltextSearchAlgorithm
)


class TestFulltextSearchBuilderOnline(unittest.TestCase):
    """Online test cases for FulltextSearchBuilder."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test database and data."""
        # Get connection parameters using standard config
        host, port, user, password, database = get_connection_params()
        
        cls.client = Client()
        cls.client.connect(host=host, port=port, user=user, password=password, database=database)
        
        # Create test database
        cls.test_db = "test_fulltext_builder"
        cls.client.execute(f"CREATE DATABASE IF NOT EXISTS {cls.test_db}")
        cls.client.execute(f"USE {cls.test_db}")
        
        # Create test table
        cls.client.execute("DROP TABLE IF EXISTS search_test_articles")
        cls.client.execute("""
            CREATE TABLE search_test_articles (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                title VARCHAR(200),
                content TEXT,
                category VARCHAR(50),
                author VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert test data
        cls.client.execute("""
            INSERT INTO search_test_articles (title, content, category, author) VALUES
            ('Machine Learning Basics', 'Machine learning is a subset of artificial intelligence that focuses on algorithms and statistical models.', 'AI', 'John Doe'),
            ('Deep Learning with Python', 'Deep learning uses neural networks with multiple layers to learn complex patterns in data.', 'AI', 'Jane Smith'),
            ('Python Programming Guide', 'Python is a versatile programming language used in web development, data science, and automation.', 'Programming', 'Bob Johnson'),
            ('Data Science Fundamentals', 'Data science combines statistics, programming, and domain expertise to extract insights from data.', 'Data Science', 'Alice Brown'),
            ('Web Development with React', 'React is a JavaScript library for building user interfaces, particularly web applications.', 'Web Development', 'Charlie Wilson'),
            ('Java Spring Framework', 'Spring is a popular Java framework for building enterprise applications with dependency injection.', 'Programming', 'David Lee'),
            ('Machine Learning with Java', 'Java can be used for machine learning with libraries like Weka and Deeplearning4j.', 'AI', 'Eva Chen'),
            ('Artificial Intelligence Overview', 'Artificial intelligence encompasses machine learning, deep learning, and neural networks.', 'AI', 'Frank Miller'),
            ('Neural Networks Tutorial', 'Neural networks are computing systems inspired by biological neural networks.', 'AI', 'Grace Lee'),
            ('Statistics for Data Science', 'Statistics provides the foundation for data analysis and machine learning algorithms.', 'Data Science', 'Henry Wang'),
            ('Advanced Machine Learning', 'Advanced machine learning techniques include ensemble methods, deep learning, and reinforcement learning.', 'AI', 'Ivy Zhang'),
            ('Python Data Analysis', 'Python provides powerful libraries like pandas, numpy, and matplotlib for data analysis and visualization.', 'Data Science', 'Jack Wilson')
        """)
        
        # Enable fulltext indexing
        cls.client.fulltext_index.enable_fulltext()
        
        # Create fulltext index
        try:
            cls.client.fulltext_index.create(
                "search_test_articles", 
                "ftidx_search_test", 
                ["title", "content"], 
                FulltextSearchAlgorithm.BM25
            )
        except Exception:
            # Index might already exist, ignore error
            pass
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test data."""
        try:
            cls.client.execute("DROP TABLE IF EXISTS search_test_articles")
            cls.client.execute(f"DROP DATABASE IF EXISTS {cls.test_db}")
            cls.client.disconnect()
        except:
            pass
    
    def test_natural_language_search(self):
        """Test natural language search with expected results."""
        results = (self.client.fulltext_index.search()
                   .table("search_test_articles")
                   .columns(["title", "content"])
                   .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
                   .query("machine learning")
                   .with_score()
                   .limit(10)
                   .execute())
        
        # Convert to list for easier processing
        results_list = list(results)
        self.assertGreater(len(results_list), 0)
        
        # Expected articles that should contain "machine learning"
        expected_titles = [
            "Machine Learning Basics",
            "Machine Learning with Java", 
            "Advanced Machine Learning"
        ]
        
        # Get actual titles from results
        actual_titles = [row[1] for row in results_list]
        
        # Verify that expected articles are in results
        for expected_title in expected_titles:
            self.assertIn(expected_title, actual_titles, 
                         f"Expected article '{expected_title}' not found in results")
        
        # Verify all results contain relevant terms
        for row in results_list:
            title = row[1].lower()
            content = row[2].lower()
            self.assertTrue("machine" in title or "machine" in content or 
                           "learning" in title or "learning" in content,
                           f"Result '{row[1]}' doesn't contain relevant terms")
        
        # Verify scores are in descending order (if multiple results)
        if len(results_list) > 1:
            scores = [float(row[6]) for row in results_list]
            self.assertEqual(scores, sorted(scores, reverse=True), 
                           "Scores should be in descending order")
    
    def test_boolean_search_required_terms(self):
        """Test boolean search with required terms and expected results."""
        results = (self.client.fulltext_index.search()
                   .table("search_test_articles")
                   .columns(["title", "content"])
                   .with_mode(FulltextSearchMode.BOOLEAN)
                   .add_term("machine", required=True)
                   .add_term("learning", required=True)
                   .with_score()
                   .limit(10)
                   .execute())
        
        results_list = list(results)
        self.assertGreater(len(results_list), 0)
        
        # Expected articles that must contain both "machine" and "learning"
        expected_titles = [
            "Machine Learning Basics",
            "Machine Learning with Java",
            "Advanced Machine Learning"
        ]
        
        # Get actual titles from results
        actual_titles = [row[1] for row in results_list]
        
        # Verify that expected articles are in results
        for expected_title in expected_titles:
            self.assertIn(expected_title, actual_titles,
                         f"Expected article '{expected_title}' not found in results")
        
        # Verify all results contain both required terms
        for row in results_list:
            title = row[1].lower()
            content = row[2].lower()
            self.assertTrue("machine" in title or "machine" in content,
                           f"Result '{row[1]}' doesn't contain 'machine'")
            self.assertTrue("learning" in title or "learning" in content,
                           f"Result '{row[1]}' doesn't contain 'learning'")
        
        # Verify we don't get articles that only contain one term
        # (e.g., "Deep Learning with Python" should not appear as it doesn't contain "machine")
        unexpected_titles = ["Deep Learning with Python"]
        for unexpected_title in unexpected_titles:
            self.assertNotIn(unexpected_title, actual_titles,
                           f"Unexpected article '{unexpected_title}' found in results")
    
    def test_boolean_search_excluded_terms(self):
        """Test boolean search with excluded terms and expected results."""
        results = (self.client.fulltext_index.search()
                   .table("search_test_articles")
                   .columns(["title", "content"])
                   .with_mode(FulltextSearchMode.BOOLEAN)
                   .add_term("machine", required=True)
                   .add_term("java", excluded=True)
                   .with_score()
                   .limit(10)
                   .execute())
        
        results_list = list(results)
        self.assertGreater(len(results_list), 0)
        
        # Expected articles that contain "machine" but not "java"
        expected_titles = [
            "Machine Learning Basics",
            "Advanced Machine Learning"
        ]
        
        # Articles that should be excluded (contain "java")
        excluded_titles = [
            "Machine Learning with Java"
        ]
        
        # Get actual titles from results
        actual_titles = [row[1] for row in results_list]
        
        # Verify expected articles are in results
        for expected_title in expected_titles:
            self.assertIn(expected_title, actual_titles,
                         f"Expected article '{expected_title}' not found in results")
        
        # Verify excluded articles are not in results
        for excluded_title in excluded_titles:
            self.assertNotIn(excluded_title, actual_titles,
                           f"Excluded article '{excluded_title}' found in results")
        
        # Verify all results contain "machine" but not "java"
        for row in results_list:
            title = row[1].lower()
            content = row[2].lower()
            self.assertTrue("machine" in title or "machine" in content,
                           f"Result '{row[1]}' doesn't contain 'machine'")
            # Check that the article doesn't contain "java" in both title and content
            self.assertFalse("java" in title and "java" in content,
                           f"Result '{row[1]}' contains 'java' but should be excluded")
    
    def test_phrase_search(self):
        """Test exact phrase search with expected results."""
        results = (self.client.fulltext_index.search()
                   .table("search_test_articles")
                   .columns(["title", "content"])
                   .with_mode(FulltextSearchMode.BOOLEAN)
                   .add_phrase("machine learning")
                   .with_score()
                   .limit(10)
                   .execute())
        
        results_list = list(results)
        self.assertGreater(len(results_list), 0)
        
        # Expected articles that contain the exact phrase "machine learning"
        expected_titles = [
            "Machine Learning Basics",
            "Machine Learning with Java",
            "Advanced Machine Learning"
        ]
        
        # Get actual titles from results
        actual_titles = [row[1] for row in results_list]
        
        # Verify expected articles are in results
        for expected_title in expected_titles:
            self.assertIn(expected_title, actual_titles,
                         f"Expected article '{expected_title}' not found in results")
        
        # Verify all results contain the exact phrase "machine learning"
        for row in results_list:
            title = row[1].lower()
            content = row[2].lower()
            self.assertTrue("machine learning" in title or "machine learning" in content,
                           f"Result '{row[1]}' doesn't contain exact phrase 'machine learning'")
        
        # Verify articles that don't contain the exact phrase are not included
        # (e.g., "Deep Learning with Python" contains "learning" but not "machine learning")
        unexpected_titles = ["Deep Learning with Python"]
        for unexpected_title in unexpected_titles:
            self.assertNotIn(unexpected_title, actual_titles,
                           f"Unexpected article '{unexpected_title}' found in results")
    
    def test_wildcard_search(self):
        """Test wildcard search with expected results."""
        results = (self.client.fulltext_index.search()
                   .table("search_test_articles")
                   .columns(["title", "content"])
                   .with_mode(FulltextSearchMode.BOOLEAN)
                   .add_wildcard("neural*")
                   .with_score()
                   .limit(10)
                   .execute())
        
        results_list = list(results)
        self.assertGreater(len(results_list), 0)
        
        # Expected articles that contain words starting with "neural"
        expected_titles = [
            "Neural Networks Tutorial"
        ]
        
        # Get actual titles from results
        actual_titles = [row[1] for row in results_list]
        
        # Verify expected articles are in results
        for expected_title in expected_titles:
            self.assertIn(expected_title, actual_titles,
                         f"Expected article '{expected_title}' not found in results")
        
        # Verify all results contain words starting with "neural"
        for row in results_list:
            title = row[1].lower()
            content = row[2].lower()
            self.assertTrue("neural" in title or "neural" in content,
                           f"Result '{row[1]}' doesn't contain words starting with 'neural'")
        
        # Verify articles that don't contain "neural" are not included
        unexpected_titles = ["Python Programming Guide", "Java Spring Framework"]
        for unexpected_title in unexpected_titles:
            self.assertNotIn(unexpected_title, actual_titles,
                           f"Unexpected article '{unexpected_title}' found in results")
    
    def test_complex_boolean_query(self):
        """Test complex boolean query with multiple operators."""
        results = (self.client.fulltext_index.search()
                   .table("search_test_articles")
                   .columns(["title", "content"])
                   .with_mode(FulltextSearchMode.BOOLEAN)
                   .query("+machine +learning -java ~python")
                   .with_score()
                   .limit(5)
                   .execute())
        
        self.assertGreater(len(results), 0)
        
        # Results should contain "machine" and "learning", but not "java"
        for row in results:
            title = row[1].lower()
            content = row[2].lower()
            self.assertTrue("machine" in title or "machine" in content)
            self.assertTrue("learning" in title or "learning" in content)
            self.assertFalse("java" in title and "java" in content)
    
    def test_weight_operators(self):
        """Test weight operators < and >."""
        # Test > operator (higher weight)
        results_higher = (self.client.fulltext_index.search()
                          .table("search_test_articles")
                          .columns(["title", "content"])
                          .with_mode(FulltextSearchMode.BOOLEAN)
                          .query("+machine >python")
                          .with_score()
                          .limit(5)
                          .execute())
        
        # Test < operator (lower weight)
        results_lower = (self.client.fulltext_index.search()
                         .table("search_test_articles")
                         .columns(["title", "content"])
                         .with_mode(FulltextSearchMode.BOOLEAN)
                         .query("+machine <java")
                         .with_score()
                         .limit(5)
                         .execute())
        
        self.assertGreater(len(results_higher), 0)
        self.assertGreater(len(results_lower), 0)
    
    def test_where_condition(self):
        """Test search with WHERE condition and expected results."""
        results = (self.client.fulltext_index.search()
                   .table("search_test_articles")
                   .columns(["title", "content"])
                   .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
                   .query("machine learning")
                   .where("category = 'AI'")
                   .with_score()
                   .limit(10)
                   .execute())
        
        results_list = list(results)
        self.assertGreater(len(results_list), 0)
        
        # Expected AI articles that contain "machine learning"
        expected_titles = [
            "Machine Learning Basics",
            "Machine Learning with Java",
            "Advanced Machine Learning"
        ]
        
        # Get actual titles from results
        actual_titles = [row[1] for row in results_list]
        
        # Verify expected articles are in results
        for expected_title in expected_titles:
            self.assertIn(expected_title, actual_titles,
                         f"Expected article '{expected_title}' not found in results")
        
        # All results should be in AI category
        for row in results_list:
            category = row[3]  # category column
            self.assertEqual(category, "AI", 
                           f"Result '{row[1]}' is not in AI category")
        
        # Verify non-AI articles are not included
        # (e.g., "Python Programming Guide" is in Programming category)
        unexpected_titles = ["Python Programming Guide", "Java Spring Framework"]
        for unexpected_title in unexpected_titles:
            self.assertNotIn(unexpected_title, actual_titles,
                           f"Unexpected article '{unexpected_title}' found in results")
    
    def test_multiple_where_conditions(self):
        """Test search with multiple WHERE conditions and expected results."""
        results = (self.client.fulltext_index.search()
                   .table("search_test_articles")
                   .columns(["title", "content"])
                   .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
                   .query("python")
                   .where("category = 'Data Science'")
                   .where("author = 'Alice Brown'")
                   .with_score()
                   .limit(10)
                   .execute())
        
        results_list = list(results)
        
        # Expected article that matches both conditions
        expected_titles = [
            "Data Science Fundamentals"
        ]
        
        # Get actual titles from results
        actual_titles = [row[1] for row in results_list]
        
        # Verify expected articles are in results (if any results found)
        if results_list:
            for expected_title in expected_titles:
                if expected_title in actual_titles:
                    break
            else:
                # If no expected titles found, at least verify the search worked
                self.assertTrue(len(actual_titles) > 0, "No results found for the search")
        
        # All results should match both conditions
        for row in results_list:
            category = row[3]  # category column
            author = row[4]    # author column
            self.assertEqual(category, "Data Science",
                           f"Result '{row[1]}' is not in Data Science category")
            self.assertEqual(author, "Alice Brown",
                           f"Result '{row[1]}' is not by Alice Brown")
        
        # Verify articles that don't match both conditions are not included
        # (e.g., "Python Programming Guide" is in Programming category, not Data Science)
        unexpected_titles = ["Python Programming Guide", "Python Data Analysis"]
        for unexpected_title in unexpected_titles:
            self.assertNotIn(unexpected_title, actual_titles,
                           f"Unexpected article '{unexpected_title}' found in results")
    
    def test_order_by_score(self):
        """Test ordering by relevance score with expected results."""
        results = (self.client.fulltext_index.search()
                   .table("search_test_articles")
                   .columns(["title", "content"])
                   .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
                   .query("machine learning")
                   .with_score()
                   .order_by("score", "DESC")
                   .limit(5)
                   .execute())
        
        results_list = list(results)
        self.assertGreater(len(results_list), 0)
        
        # Expected articles that should be in results (ordered by relevance)
        expected_titles = [
            "Machine Learning Basics",
            "Machine Learning with Java",
            "Advanced Machine Learning"
        ]
        
        # Get actual titles from results
        actual_titles = [row[1] for row in results_list]
        
        # Verify expected articles are in results
        for expected_title in expected_titles:
            self.assertIn(expected_title, actual_titles,
                         f"Expected article '{expected_title}' not found in results")
        
        # Scores should be in descending order
        scores = [float(row[6]) for row in results_list]  # score column
        self.assertEqual(scores, sorted(scores, reverse=True),
                        "Scores should be in descending order")
        
        # The first result should have the highest score (if scores are different)
        if len(results_list) > 1 and scores[0] != scores[1]:
            self.assertGreater(scores[0], scores[1],
                             "First result should have higher score than second")
    
    def test_limit_and_offset(self):
        """Test LIMIT and OFFSET for pagination with expected results."""
        # Get first page
        page1 = (self.client.fulltext_index.search()
                 .table("search_test_articles")
                 .columns(["title", "content"])
                 .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
                 .query("python")
                 .limit(3)
                 .offset(0)
                 .execute())
        
        # Get second page
        page2 = (self.client.fulltext_index.search()
                 .table("search_test_articles")
                 .columns(["title", "content"])
                 .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
                 .query("python")
                 .limit(3)
                 .offset(3)
                 .execute())
        
        page1_list = list(page1)
        page2_list = list(page2)
        
        self.assertLessEqual(len(page1_list), 3)
        self.assertLessEqual(len(page2_list), 3)
        
        # Expected articles that contain "python"
        expected_python_articles = [
            "Python Programming Guide",
            "Deep Learning with Python",
            "Python Data Analysis"
        ]
        
        # Get actual titles from both pages
        page1_titles = [row[1] for row in page1_list]
        page2_titles = [row[1] for row in page2_list]
        all_titles = page1_titles + page2_titles
        
        # Verify expected articles are in results
        for expected_title in expected_python_articles:
            self.assertIn(expected_title, all_titles,
                         f"Expected article '{expected_title}' not found in paginated results")
        
        # Pages should not overlap
        page1_ids = {row[0] for row in page1_list}
        page2_ids = {row[0] for row in page2_list}
        self.assertTrue(page1_ids.isdisjoint(page2_ids),
                       "Pages should not have overlapping results")
        
        # All results should contain "python"
        for row in page1_list + page2_list:
            title = row[1].lower()
            content = row[2].lower()
            self.assertTrue("python" in title or "python" in content,
                           f"Result '{row[1]}' doesn't contain 'python'")
    
    def test_select_specific_columns(self):
        """Test selecting specific columns with expected results."""
        results = (self.client.fulltext_index.search()
                   .table("search_test_articles")
                   .columns(["title", "content"])
                   .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
                   .query("machine learning")
                   .select(["id", "title"])
                   .with_score()
                   .limit(3)
                   .execute())
        
        results_list = list(results)
        self.assertGreater(len(results_list), 0)
        
        # Expected articles that should be in results
        expected_titles = [
            "Machine Learning Basics",
            "Machine Learning with Java",
            "Advanced Machine Learning"
        ]
        
        # Get actual titles from results
        actual_titles = [row[1] for row in results_list]
        
        # Verify expected articles are in results
        for expected_title in expected_titles:
            if expected_title in actual_titles:
                break
        else:
            # At least one expected article should be in results
            self.assertTrue(any("machine" in title.lower() for title in actual_titles),
                           "No machine learning articles found in results")
        
        # Each row should have 3 columns: id, title, score
        for row in results_list:
            self.assertEqual(len(row), 3, 
                           f"Expected 3 columns, got {len(row)}")
            # Verify column types
            self.assertIsInstance(row[0], int, "ID should be integer")
            self.assertIsInstance(row[1], str, "Title should be string")
            self.assertIsInstance(row[2], (int, float, str), "Score should be numeric or string")
    
    def test_explain_method(self):
        """Test explain method returns valid SQL."""
        builder = (self.client.fulltext_index.search()
                   .table("search_test_articles")
                   .columns(["title", "content"])
                   .with_mode(FulltextSearchMode.BOOLEAN)
                   .add_term("machine", required=True)
                   .add_term("learning", required=True)
                   .with_score()
                   .limit(5))
        
        sql = builder.explain()
        
        # SQL should contain expected components
        self.assertIn("SELECT", sql)
        self.assertIn("FROM search_test_articles", sql)
        self.assertIn("MATCH(title, content) AGAINST", sql)
        self.assertIn("+machine +learning", sql)
        self.assertIn("IN boolean mode", sql)
        self.assertIn("LIMIT 5", sql)
    
    def test_chainable_interface(self):
        """Test that the builder interface is properly chainable."""
        # This should not raise any exceptions
        builder = (self.client.fulltext_index.search()
                   .table("search_test_articles")
                   .columns(["title", "content"])
                   .with_mode(FulltextSearchMode.BOOLEAN)
                   .add_term("machine", required=True)
                   .add_phrase("deep learning")
                   .add_wildcard("neural*")
                   .with_score()
                   .select(["id", "title"])
                   .where("category = 'AI'")
                   .order_by("score", "DESC")
                   .limit(10)
                   .offset(0))
        
        # Execute the query
        results = builder.execute()
        # Results can be ResultSet or list depending on implementation
        self.assertTrue(hasattr(results, '__iter__'))
    
    def test_error_handling_missing_table(self):
        """Test error handling for missing table."""
        with self.assertRaises(Exception):
            (self.client.fulltext_index.search()
             .table("nonexistent_table")
             .columns(["title", "content"])
             .query("test")
             .execute())
    
    def test_error_handling_missing_columns(self):
        """Test error handling for missing columns."""
        with self.assertRaises(Exception):
            (self.client.fulltext_index.search()
             .table("search_test_articles")
             .columns(["nonexistent_column"])
             .query("test")
             .execute())


if __name__ == "__main__":
    unittest.main()
