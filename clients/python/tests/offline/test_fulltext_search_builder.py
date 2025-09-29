#!/usr/bin/env python3
"""
Offline tests for FulltextSearchBuilder

Tests the SQL generation and query building logic without database connection.
"""

import unittest
from unittest.mock import Mock, MagicMock

from matrixone.sqlalchemy_ext.fulltext_search import (
    FulltextSearchBuilder,
    FulltextSearchMode,
    FulltextSearchAlgorithm,
)


class TestFulltextSearchBuilder(unittest.TestCase):
    """Test cases for FulltextSearchBuilder offline functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_client = Mock()
        self.builder = FulltextSearchBuilder(self.mock_client)

    def test_basic_sql_generation(self):
        """Test basic SQL generation."""
        sql = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
            .query("machine learning")
            ._build_sql()
        )

        expected = (
            "SELECT * FROM articles " "WHERE MATCH(title, content) AGAINST('machine learning' IN natural language mode)"
        )
        self.assertEqual(sql, expected)

    def test_boolean_mode_sql_generation(self):
        """Test boolean mode SQL generation."""
        sql = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .query("+machine +learning -java")
            ._build_sql()
        )

        expected = (
            "SELECT * FROM articles " "WHERE MATCH(title, content) AGAINST('+machine +learning -java' IN boolean mode)"
        )
        self.assertEqual(sql, expected)

    def test_add_term_required(self):
        """Test add_term with required=True."""
        sql = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_term("machine", required=True)
            .add_term("learning", required=True)
            ._build_sql()
        )

        expected = "SELECT * FROM articles " "WHERE MATCH(title, content) AGAINST('+machine +learning' IN boolean mode)"
        self.assertEqual(sql, expected)

    def test_add_term_excluded(self):
        """Test add_term with excluded=True."""
        sql = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_term("machine", required=True)
            .add_term("java", excluded=True)
            ._build_sql()
        )

        expected = "SELECT * FROM articles " "WHERE MATCH(title, content) AGAINST('+machine -java' IN boolean mode)"
        self.assertEqual(sql, expected)

    def test_add_phrase(self):
        """Test add_phrase method."""
        sql = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_phrase("machine learning")
            ._build_sql()
        )

        expected = "SELECT * FROM articles " "WHERE MATCH(title, content) AGAINST('\"machine learning\"' IN boolean mode)"
        self.assertEqual(sql, expected)

    def test_add_wildcard(self):
        """Test add_wildcard method."""
        sql = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_wildcard("machine*")
            .add_wildcard("learn*")
            ._build_sql()
        )

        expected = "SELECT * FROM articles " "WHERE MATCH(title, content) AGAINST('machine* learn*' IN boolean mode)"
        self.assertEqual(sql, expected)

    def test_with_score(self):
        """Test with_score method."""
        sql = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
            .query("machine learning")
            .with_score()
            ._build_sql()
        )

        expected = (
            "SELECT *, MATCH(title, content) AGAINST('machine learning' IN natural language mode) AS score "
            "FROM articles "
            "WHERE MATCH(title, content) AGAINST('machine learning' IN natural language mode)"
        )
        self.assertEqual(sql, expected)

    def test_select_columns(self):
        """Test select method."""
        sql = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
            .query("machine learning")
            .select(["id", "title"])
            ._build_sql()
        )

        expected = (
            "SELECT id, title FROM articles "
            "WHERE MATCH(title, content) AGAINST('machine learning' IN natural language mode)"
        )
        self.assertEqual(sql, expected)

    def test_where_condition(self):
        """Test where method."""
        sql = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
            .query("machine learning")
            .where("category = 'AI'")
            ._build_sql()
        )

        expected = (
            "SELECT * FROM articles "
            "WHERE MATCH(title, content) AGAINST('machine learning' IN natural language mode) "
            "AND category = 'AI'"
        )
        self.assertEqual(sql, expected)

    def test_multiple_where_conditions(self):
        """Test multiple where conditions."""
        sql = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
            .query("machine learning")
            .where("category = 'AI'")
            .where("author = 'John Doe'")
            ._build_sql()
        )

        expected = (
            "SELECT * FROM articles "
            "WHERE MATCH(title, content) AGAINST('machine learning' IN natural language mode) "
            "AND category = 'AI' AND author = 'John Doe'"
        )
        self.assertEqual(sql, expected)

    def test_order_by(self):
        """Test order_by method."""
        sql = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
            .query("machine learning")
            .with_score()
            .order_by("score", "DESC")
            ._build_sql()
        )

        expected = (
            "SELECT *, MATCH(title, content) AGAINST('machine learning' IN natural language mode) AS score "
            "FROM articles "
            "WHERE MATCH(title, content) AGAINST('machine learning' IN natural language mode) "
            "ORDER BY score DESC"
        )
        self.assertEqual(sql, expected)

    def test_limit(self):
        """Test limit method."""
        sql = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
            .query("machine learning")
            .limit(10)
            ._build_sql()
        )

        expected = (
            "SELECT * FROM articles "
            "WHERE MATCH(title, content) AGAINST('machine learning' IN natural language mode) "
            "LIMIT 10"
        )
        self.assertEqual(sql, expected)

    def test_offset(self):
        """Test offset method."""
        sql = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
            .query("machine learning")
            .limit(10)
            .offset(20)
            ._build_sql()
        )

        expected = (
            "SELECT * FROM articles "
            "WHERE MATCH(title, content) AGAINST('machine learning' IN natural language mode) "
            "LIMIT 10 OFFSET 20"
        )
        self.assertEqual(sql, expected)

    def test_complex_boolean_query(self):
        """Test complex boolean query with all operators."""
        sql = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .query("+machine +learning -java ~python <neural >deep")
            .with_score()
            .select(["id", "title"])
            .where("category = 'AI'")
            .order_by("score", "DESC")
            .limit(20)
            ._build_sql()
        )

        expected = (
            "SELECT id, title, MATCH(title, content) AGAINST('+machine +learning -java ~python <neural >deep' IN boolean mode) AS score "
            "FROM articles "
            "WHERE MATCH(title, content) AGAINST('+machine +learning -java ~python <neural >deep' IN boolean mode) "
            "AND category = 'AI' "
            "ORDER BY score DESC "
            "LIMIT 20"
        )
        self.assertEqual(sql, expected)

    def test_phrase_and_wildcard_combination(self):
        """Test combination of phrases and wildcards."""
        sql = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_phrase("machine learning")
            .add_wildcard("neural*")
            .add_term("python", excluded=True)
            ._build_sql()
        )

        # Check that the SQL contains all the required parts
        self.assertIn("SELECT * FROM articles", sql)
        self.assertIn("WHERE MATCH(title, content) AGAINST(", sql)
        self.assertIn("IN boolean mode", sql)
        # Check individual terms are present
        self.assertIn("-python", sql)
        self.assertIn('"machine learning"', sql)
        self.assertIn("neural*", sql)

    def test_query_resets_previous_terms(self):
        """Test that query() method resets previous terms."""
        sql = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.BOOLEAN)
            .add_term("machine", required=True)
            .add_phrase("deep learning")
            .query("artificial intelligence")
            ._build_sql()
        )

        expected = "SELECT * FROM articles " "WHERE MATCH(title, content) AGAINST('artificial intelligence' IN boolean mode)"
        self.assertEqual(sql, expected)

    def test_missing_table_name_raises_error(self):
        """Test that missing table name raises ValueError."""
        with self.assertRaises(ValueError) as context:
            (self.builder.columns(["title", "content"]).query("test")._build_sql())

        self.assertIn("Table name is required", str(context.exception))

    def test_missing_columns_raises_error(self):
        """Test that missing columns raises ValueError."""
        with self.assertRaises(ValueError) as context:
            (self.builder.table("articles").query("test")._build_sql())

        self.assertIn("Search columns are required", str(context.exception))

    def test_missing_query_raises_error(self):
        """Test that missing query raises ValueError."""
        with self.assertRaises(ValueError) as context:
            (self.builder.table("articles").columns(["title", "content"])._build_sql())

        self.assertIn("Query is required", str(context.exception))

    def test_explain_method(self):
        """Test explain method returns the same SQL as _build_sql."""
        builder = (
            self.builder.table("articles")
            .columns(["title", "content"])
            .with_mode(FulltextSearchMode.NATURAL_LANGUAGE)
            .query("machine learning")
            .with_score()
            .limit(10)
        )

        sql1 = builder._build_sql()
        sql2 = builder.explain()

        self.assertEqual(sql1, sql2)

    def test_chainable_interface(self):
        """Test that all methods return self for chaining."""
        builder = self.builder

        # Test that each method returns the builder instance
        self.assertIs(builder.table("test"), builder)
        self.assertIs(builder.columns(["col1"]), builder)
        self.assertIs(builder.with_mode(FulltextSearchMode.NATURAL_LANGUAGE), builder)
        self.assertIs(builder.with_algorithm(FulltextSearchAlgorithm.BM25), builder)
        self.assertIs(builder.query("test"), builder)
        self.assertIs(builder.add_term("test"), builder)
        self.assertIs(builder.add_phrase("test"), builder)
        self.assertIs(builder.add_wildcard("test*"), builder)
        self.assertIs(builder.with_score(), builder)
        self.assertIs(builder.select(["col1"]), builder)
        self.assertIs(builder.where("col1 = 'value'"), builder)
        self.assertIs(builder.order_by("col1"), builder)
        self.assertIs(builder.limit(10), builder)
        self.assertIs(builder.offset(20), builder)


if __name__ == "__main__":
    unittest.main()
