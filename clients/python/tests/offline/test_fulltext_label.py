#!/usr/bin/env python3
"""
Offline tests for FulltextFilter label functionality
Tests SQL generation for AS score feature
"""

import unittest
import sys
import os

# Add the parent directory to sys.path to import matrixone
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone.sqlalchemy_ext.fulltext_search import boolean_match, natural_match, group


class TestFulltextLabel(unittest.TestCase):
    """Test FulltextFilter label functionality for AS score."""

    def test_basic_boolean_label(self):
        """Test basic boolean match with label."""
        expr = boolean_match("title", "content").must("python").label("score")
        sql = expr.compile()

        expected = "MATCH(title, content) AGAINST('+python' IN BOOLEAN MODE) AS score"
        self.assertEqual(sql, expected)

    def test_basic_natural_label(self):
        """Test basic natural language match with label."""
        expr = natural_match("title", "content", query="machine learning").label("relevance")
        sql = expr.compile()

        expected = "MATCH(title, content) AGAINST('machine learning') AS relevance"
        self.assertEqual(sql, expected)

    def test_complex_boolean_label(self):
        """Test complex boolean query with label."""
        expr = (
            boolean_match("title", "content", "tags")
            .must("python", "programming")
            .encourage("tutorial")
            .discourage("deprecated")
            .label("python_score")
        )
        sql = expr.compile()

        # Should contain all required elements
        self.assertIn("MATCH(title, content, tags)", sql)
        self.assertIn("AGAINST(", sql)
        self.assertIn("+python", sql)
        self.assertIn("+programming", sql)
        self.assertIn("tutorial", sql)  # encourage (no prefix)
        self.assertIn("~deprecated", sql)  # discourage
        self.assertIn("IN BOOLEAN MODE", sql)
        self.assertIn("AS python_score", sql)

    def test_boolean_with_groups_label(self):
        """Test boolean query with groups and label."""
        expr = (
            boolean_match("title", "content")
            .must("programming")
            .must(group().medium("python", "java"))
            .discourage(group().medium("legacy", "outdated"))
            .label("programming_score")
        )
        sql = expr.compile()

        # Should contain group syntax
        self.assertIn("MATCH(title, content)", sql)
        self.assertIn("+programming", sql)
        self.assertIn("+(python java)", sql)
        self.assertIn("~(legacy outdated)", sql)
        self.assertIn("AS programming_score", sql)

    def test_natural_language_modes_label(self):
        """Test different natural language modes with label."""
        # Basic natural language
        expr1 = natural_match("body", query="artificial intelligence").label("ai_score")
        sql1 = expr1.compile()
        self.assertEqual(sql1, "MATCH(body) AGAINST('artificial intelligence') AS ai_score")

    def test_boolean_operators_with_label(self):
        """Test all boolean operators with label."""
        expr = (
            boolean_match("title", "content")
            .must("required")
            .must_not("excluded")
            .encourage("optional")
            .discourage("reduced")
            .label("complex_score")
        )
        sql = expr.compile()

        expected_parts = [
            "MATCH(title, content)",
            "AGAINST(",
            "+required",
            "-excluded",
            "optional",  # encourage (no prefix)
            "~reduced",  # discourage
            "IN BOOLEAN MODE",
            "AS complex_score",
        ]

        for part in expected_parts:
            self.assertIn(part, sql)

    def test_group_weight_operators_with_label(self):
        """Test group weight operators with label."""
        expr = (
            boolean_match("title", "content")
            .must("main")
            .must(group().high("important").medium("normal").low("minor"))
            .label("weighted_score")
        )
        sql = expr.compile()

        # Check for weight operators within groups
        self.assertIn("+main", sql)
        self.assertIn("+(>important normal <minor)", sql)
        self.assertIn("AS weighted_score", sql)

    def test_phrase_and_prefix_with_label(self):
        """Test phrase and prefix matching with label."""
        expr = boolean_match("title", "content").must(group().phrase("machine learning").prefix("neural")).label("ml_score")
        sql = expr.compile()

        # Should contain phrase and prefix syntax
        self.assertIn('+("machine learning" neural*)', sql)
        self.assertIn("AS ml_score", sql)

    def test_multiple_columns_label(self):
        """Test multiple column matching with label."""
        expr = boolean_match("title", "content", "tags", "description").must("search").label("multi_score")
        sql = expr.compile()

        expected = "MATCH(title, content, tags, description) AGAINST('+search' IN BOOLEAN MODE) AS multi_score"
        self.assertEqual(sql, expected)

    def test_single_column_label(self):
        """Test single column matching with label."""
        expr = boolean_match("title").must("important").label("title_score")
        sql = expr.compile()

        expected = "MATCH(title) AGAINST('+important' IN BOOLEAN MODE) AS title_score"
        self.assertEqual(sql, expected)

    def test_label_with_special_characters(self):
        """Test label with special characters in alias name."""
        expr = boolean_match("title").must("test").label("test_score_v2")
        sql = expr.compile()

        expected = "MATCH(title) AGAINST('+test' IN BOOLEAN MODE) AS test_score_v2"
        self.assertEqual(sql, expected)

    def test_nested_groups_with_label(self):
        """Test nested group functionality with label."""
        # Create nested group structure
        inner_group = group().medium("python", "java")
        outer_group = group().medium("programming").medium("tutorial")

        expr = (
            boolean_match("title", "content").must("coding").must(inner_group).encourage(outer_group).label("nested_score")
        )
        sql = expr.compile()

        # Should contain nested structure
        self.assertIn("+coding", sql)
        self.assertIn("+(python java)", sql)
        self.assertIn("(programming tutorial)", sql)
        self.assertIn("AS nested_score", sql)

    def test_empty_query_with_label(self):
        """Test behavior with empty query and label."""
        # Empty query should raise ValueError
        with self.assertRaises(ValueError):
            expr = boolean_match("title", "content").label("empty_score")
            sql = expr.compile()

    def test_matrixone_syntax_compatibility(self):
        """Test compatibility with MatrixOne test case syntax."""
        # Based on test/distributed/cases/fulltext/*.result

        # Test case 1: Basic natural language with score
        expr1 = natural_match("body", "title", query="is red").label("score")
        sql1 = expr1.compile()
        expected1 = "MATCH(body, title) AGAINST('is red') AS score"
        self.assertEqual(sql1, expected1)

        # Test case 2: Boolean mode with score
        expr2 = boolean_match("body", "title").must("fast").encourage("red").label("score")
        sql2 = expr2.compile()
        self.assertIn("MATCH(body, title)", sql2)
        self.assertIn("+fast", sql2)
        self.assertIn("red", sql2)
        self.assertIn("AS score", sql2)

    def test_label_sql_injection_protection(self):
        """Test that label names are properly escaped/validated."""
        # Test normal label
        expr1 = boolean_match("title").must("test").label("normal_score")
        sql1 = expr1.compile()
        self.assertIn("AS normal_score", sql1)

        # Test label with numbers
        expr2 = boolean_match("title").must("test").label("score123")
        sql2 = expr2.compile()
        self.assertIn("AS score123", sql2)

        # Test label with underscores
        expr3 = boolean_match("title").must("test").label("my_test_score")
        sql3 = expr3.compile()
        self.assertIn("AS my_test_score", sql3)

    def test_chaining_after_label(self):
        """Test that label returns the correct object type."""
        expr = boolean_match("title", "content").must("python")
        labeled_expr = expr.label("score")

        # Check that labeled expression has compile method
        self.assertTrue(hasattr(labeled_expr, 'compile'))

        # Check that it generates correct SQL
        sql = labeled_expr.compile()
        expected = "MATCH(title, content) AGAINST('+python' IN BOOLEAN MODE) AS score"
        self.assertEqual(sql, expected)

    def test_multiple_labels_different_expressions(self):
        """Test multiple different expressions with different labels."""
        expr1 = boolean_match("title").must("python").label("python_score")
        expr2 = natural_match("content", query="tutorial guide").label("tutorial_score")
        expr3 = boolean_match("tags").encourage("programming").label("tag_score")

        sql1 = expr1.compile()
        sql2 = expr2.compile()
        sql3 = expr3.compile()

        # Each should have unique label
        self.assertIn("AS python_score", sql1)
        self.assertIn("AS tutorial_score", sql2)
        self.assertIn("AS tag_score", sql3)

        # Each should have correct syntax
        self.assertIn("BOOLEAN MODE", sql1)
        self.assertNotIn("BOOLEAN MODE", sql2)  # Natural language mode
        self.assertIn("BOOLEAN MODE", sql3)


if __name__ == '__main__':
    unittest.main()
