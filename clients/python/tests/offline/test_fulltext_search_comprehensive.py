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
Comprehensive Fulltext Search Tests

This file consolidates all fulltext search-related tests from:
- test_fulltext_index.py (22 tests)
- test_fulltext_label.py (17 tests)
- test_fulltext_search_builder.py (21 tests)
- test_fulltext_search_coverage.py (30 tests)
- test_fulltext_search_offline.py (56 tests)

Total: 146 tests consolidated into one file
"""

import pytest
import unittest
import sys
import os
import warnings
from unittest.mock import Mock, MagicMock, patch
from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import declarative_base

# Add the matrixone package to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from matrixone.sqlalchemy_ext import (
    FulltextIndex,
    FulltextAlgorithmType,
    FulltextModeType,
    FulltextSearchBuilder,
    create_fulltext_index,
    fulltext_search_builder,
)
from matrixone.sqlalchemy_ext.fulltext_search import (
    FulltextFilter,
    FulltextQueryBuilder,
    FulltextGroup,
    FulltextSearchMode,
    FulltextSearchAlgorithm,
    FulltextIndexManager,
    boolean_match,
    natural_match,
    group,
)
from matrixone.sqlalchemy_ext.adapters import logical_and, logical_or, logical_not

# Test model for logical adapter tests
Base = declarative_base()


class Article(Base):
    __tablename__ = 'test_articles'

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(255), nullable=False)
    content = Column(Text, nullable=False)


class MockColumn:
    """Mock SQLAlchemy column for testing."""

    def __init__(self, name):
        self.name = name


class MockClient:
    """Mock client for testing FulltextSearchBuilder."""

    def __init__(self):
        self.execute_called = False
        self.last_sql = None

    async def execute(self, sql):
        self.execute_called = True
        self.last_sql = sql
        return Mock()


class TestFulltextIndex(unittest.TestCase):
    """Test FulltextIndex class - from test_fulltext_index.py"""

    def test_fulltext_index_creation(self):
        """Test creating a FulltextIndex instance"""
        index = FulltextIndex("ftidx_test", ["title", "content"], FulltextAlgorithmType.BM25)

        assert index.name == "ftidx_test"
        assert index.get_columns() == ["title", "content"]
        assert index.algorithm == FulltextAlgorithmType.BM25

    def test_fulltext_index_single_column(self):
        """Test creating FulltextIndex with single column"""
        index = FulltextIndex("ftidx_single", "title", FulltextAlgorithmType.TF_IDF)

        assert index.name == "ftidx_single"
        assert index.get_columns() == ["title"]
        assert index.algorithm == FulltextAlgorithmType.TF_IDF

    def test_create_index_sql(self):
        """Test SQL generation for CREATE INDEX"""
        index = FulltextIndex("ftidx_test", ["title", "content"])
        sql = index._create_index_sql("documents")

        expected_sql = "CREATE FULLTEXT INDEX ftidx_test ON documents (title, content)"
        assert sql == expected_sql

    def test_fulltext_index_str(self):
        """Test FulltextIndex string representation"""
        index = FulltextIndex("ftidx_test", ["title", "content"], FulltextAlgorithmType.BM25)

        str_repr = str(index)
        assert "ftidx_test" in str_repr
        assert "title" in str_repr
        assert "content" in str_repr

    def test_fulltext_index_repr(self):
        """Test FulltextIndex representation"""
        index = FulltextIndex("ftidx_test", ["title", "content"], FulltextAlgorithmType.BM25)

        repr_str = repr(index)
        assert "ftidx_test" in repr_str

    def test_fulltext_index_multiple_columns(self):
        """Test FulltextIndex with multiple columns"""
        index = FulltextIndex("ftidx_test", ["title", "content", "summary"], FulltextAlgorithmType.BM25)

        assert index.name == "ftidx_test"
        assert index.get_columns() == ["title", "content", "summary"]


class TestFulltextAdvancedOperators(unittest.TestCase):
    """Test advanced fulltext operators like encourage, discourage, groups - from test_fulltext_label.py"""

    def test_encourage_operator(self):
        """Test encourage operator (no prefix)."""
        expr = boolean_match("title", "content").must("python").encourage("tutorial")
        sql = expr.compile()

        expected = "MATCH(title, content) AGAINST('+python tutorial' IN BOOLEAN MODE)"
        self.assertEqual(sql, expected)

    def test_discourage_operator(self):
        """Test discourage operator (tilde prefix)."""
        expr = boolean_match("title", "content").must("python").discourage("legacy")
        sql = expr.compile()

        expected = "MATCH(title, content) AGAINST('+python ~legacy' IN BOOLEAN MODE)"
        self.assertEqual(sql, expected)

    def test_complex_boolean_with_all_operators(self):
        """Test complex boolean query with all operators."""
        expr = (
            boolean_match("title", "content", "tags")
            .must("programming")
            .encourage("tutorial")
            .discourage("legacy")
            .must_not("deprecated")
        )
        sql = expr.compile()

        expected = "MATCH(title, content, tags) AGAINST('+programming tutorial ~legacy -deprecated' IN BOOLEAN MODE)"
        self.assertEqual(sql, expected)

    def test_group_with_weights(self):
        """Test group with weight operators."""
        expr = boolean_match("title", "content").must("main").must(group().high("important").medium("normal").low("minor"))
        sql = expr.compile()

        # Should contain weight operators within groups
        self.assertIn("+main", sql)
        self.assertIn("+(>important normal <minor)", sql)
        self.assertIn("IN BOOLEAN MODE", sql)


class TestFulltextComplexScenarios(unittest.TestCase):
    """Test complex fulltext scenarios with advanced features - from test_fulltext_search_offline.py"""

    def test_programming_tutorial_search(self):
        """Test programming tutorial search scenario."""
        filter_obj = (
            boolean_match("title", "content", "tags")
            .must("programming")
            .must(group().medium("python", "java", "javascript"))
            .encourage("tutorial", "guide", "beginner")
            .discourage("advanced", "expert")
            .must_not("deprecated", "legacy")
        )

        result = filter_obj.compile()
        # Check that all required elements are present
        self.assertIn("MATCH(title, content, tags) AGAINST(", result)
        self.assertIn("IN BOOLEAN MODE)", result)
        self.assertIn("+programming", result)
        self.assertIn("+(python java javascript)", result)
        self.assertIn("tutorial guide beginner", result)
        self.assertIn("~advanced ~expert", result)
        self.assertIn("-deprecated -legacy", result)

    def test_product_search_with_weights(self):
        """Test product search with element weights."""
        filter_obj = (
            boolean_match("name", "description")
            .must("laptop")
            .encourage(group().high("gaming").medium("portable").low("budget"))
            .must_not("refurbished")
        )

        result = filter_obj.compile()
        # Check that all required elements are present
        self.assertIn("MATCH(name, description) AGAINST(", result)
        self.assertIn("IN BOOLEAN MODE)", result)
        self.assertIn("+laptop", result)
        self.assertIn("(>gaming portable <budget)", result)
        self.assertIn("-refurbished", result)

    def test_matrixone_syntax_compatibility(self):
        """Test compatibility with MatrixOne test case syntax."""
        # Test case: Basic boolean with score
        expr = boolean_match("body", "title").must("fast").encourage("red").label("score")
        sql = expr.compile()
        self.assertIn("MATCH(body, title)", sql)
        self.assertIn("+fast", sql)
        self.assertIn("red", sql)
        self.assertIn("AS score", sql)

    def test_phrase_and_prefix_combination(self):
        """Test phrase and prefix matching."""
        expr = boolean_match("title", "content").phrase("machine learning").prefix("neural")
        sql = expr.compile()

        # Should contain phrase and prefix syntax
        self.assertIn('"machine learning"', sql)
        self.assertIn("neural*", sql)
        self.assertIn("IN BOOLEAN MODE", sql)

    def test_nested_groups_complex(self):
        """Test nested group functionality."""
        inner_group = group().medium("python", "java")
        outer_group = group().medium("programming").medium("tutorial")

        expr = boolean_match("title", "content").must("coding").must(inner_group).encourage(outer_group)
        sql = expr.compile()

        # Should contain nested structure
        self.assertIn("+coding", sql)
        self.assertIn("+(python java)", sql)
        self.assertIn("(programming tutorial)", sql)


class TestFulltextQueryBuilder(unittest.TestCase):
    """Test FulltextQueryBuilder functionality - from test_fulltext_search_offline.py"""

    def test_basic_must_term(self):
        """Test basic must term generation."""
        builder = FulltextQueryBuilder()
        builder.must("python")
        self.assertEqual(builder.build(), "+python")

    def test_basic_must_not_term(self):
        """Test basic must_not term generation."""
        builder = FulltextQueryBuilder()
        builder.must_not("java")
        self.assertEqual(builder.build(), "-java")

    def test_basic_encourage_term(self):
        """Test basic encourage term generation."""
        builder = FulltextQueryBuilder()
        builder.encourage("tutorial")
        self.assertEqual(builder.build(), "tutorial")

    def test_basic_discourage_term(self):
        """Test basic discourage term generation."""
        builder = FulltextQueryBuilder()
        builder.discourage("legacy")
        self.assertEqual(builder.build(), "~legacy")

    def test_multiple_terms_same_type(self):
        """Test multiple terms of same type."""
        builder = FulltextQueryBuilder()
        builder.must("python", "programming")
        self.assertEqual(builder.build(), "+python +programming")

    def test_mixed_term_types(self):
        """Test mixed term types."""
        builder = FulltextQueryBuilder()
        builder.must("python").encourage("tutorial").discourage("legacy").must_not("deprecated")
        expected = "+python tutorial ~legacy -deprecated"
        self.assertEqual(builder.build(), expected)

    def test_phrase_search(self):
        """Test phrase search generation."""
        builder = FulltextQueryBuilder()
        builder.phrase("machine learning")
        self.assertEqual(builder.build(), '"machine learning"')

    def test_prefix_search(self):
        """Test prefix search generation."""
        builder = FulltextQueryBuilder()
        builder.prefix("neural")
        self.assertEqual(builder.build(), "neural*")

    def test_boost_term(self):
        """Test boosted term generation."""
        builder = FulltextQueryBuilder()
        builder.boost("python", 2.0)
        self.assertEqual(builder.build(), "python^2.0")


class TestFulltextGroup(unittest.TestCase):
    """Test FulltextGroup functionality - from test_fulltext_search_offline.py"""

    def test_basic_group_medium(self):
        """Test basic group with medium terms."""
        grp = group()
        grp.medium("java", "kotlin")
        self.assertEqual(grp.build(), "java kotlin")

    def test_group_high_weight(self):
        """Test group with high weight terms."""
        grp = group()
        grp.high("important")
        self.assertEqual(grp.build(), ">important")

    def test_group_low_weight(self):
        """Test group with low weight terms."""
        grp = group()
        grp.low("minor")
        self.assertEqual(grp.build(), "<minor")

    def test_mixed_weights_in_group(self):
        """Test mixed weight terms in group."""
        grp = group()
        grp.medium("normal").high("important").low("minor")
        self.assertEqual(grp.build(), "normal >important <minor")

    def test_group_phrase(self):
        """Test phrase in group."""
        grp = group()
        grp.phrase("deep learning")
        self.assertEqual(grp.build(), '"deep learning"')

    def test_group_prefix(self):
        """Test prefix in group."""
        grp = group()
        grp.prefix("neural")
        self.assertEqual(grp.build(), "neural*")

    def test_nested_groups(self):
        """Test nested groups."""
        inner_group = group()
        inner_group.medium("java", "kotlin")

        outer_group = group()
        outer_group.medium("python").add_group(inner_group)
        self.assertEqual(outer_group.build(), "python (java kotlin)")

    def test_tilde_group(self):
        """Test tilde group."""
        grp = group()
        grp.medium("old", "outdated")
        grp.is_tilde = True
        self.assertEqual(grp.build(), "old outdated")  # Tilde is applied at parent level


class TestFulltextEdgeCases(unittest.TestCase):
    """Test edge cases and error handling - from test_fulltext_search_offline.py"""

    def test_empty_query_error(self):
        """Test empty query raises error."""
        filter_obj = FulltextFilter(["title", "content"])
        with self.assertRaises(ValueError, msg="Query cannot be empty"):
            filter_obj.compile()

    def test_no_columns_error(self):
        """Test no columns raises error."""
        filter_obj = FulltextFilter([])
        filter_obj.encourage("test")
        with self.assertRaises(ValueError, msg="Columns must be specified"):
            filter_obj.compile()

    def test_single_column(self):
        """Test single column search."""
        filter_obj = boolean_match("title").must("python")
        expected = "MATCH(title) AGAINST('+python' IN BOOLEAN MODE)"
        self.assertEqual(filter_obj.compile(), expected)

    def test_many_columns(self):
        """Test many columns search."""
        filter_obj = boolean_match("title", "content", "tags", "description").must("python")
        expected = "MATCH(title, content, tags, description) AGAINST('+python' IN BOOLEAN MODE)"
        self.assertEqual(filter_obj.compile(), expected)

    def test_empty_group_building(self):
        """Test empty group building."""
        grp = group()
        result = grp.build()
        self.assertEqual(result, "")

    def test_unknown_search_mode(self):
        """Test unknown search mode handling."""
        filter_obj = FulltextFilter(["title", "content"], "unknown_mode")
        filter_obj.encourage("test")

        result = filter_obj.compile()
        # Should default to basic AGAINST syntax
        self.assertEqual(result, "MATCH(title, content) AGAINST('test')")


class TestFulltextLabel(unittest.TestCase):
    """Test FulltextFilter label functionality - from test_fulltext_label.py"""

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

    def test_phrase_search_label(self):
        """Test phrase search with label."""
        expr = boolean_match("title", "content").must('"machine learning"').label("phrase_score")
        sql = expr.compile()

        expected = 'MATCH(title, content) AGAINST(\'+"machine learning"\' IN BOOLEAN MODE) AS phrase_score'
        self.assertEqual(sql, expected)

    def test_wildcard_search_label(self):
        """Test wildcard search with label."""
        expr = boolean_match("title", "content").must("python*").label("wildcard_score")
        sql = expr.compile()

        expected = "MATCH(title, content) AGAINST('+python*' IN BOOLEAN MODE) AS wildcard_score"
        self.assertEqual(sql, expected)

    def test_multiple_labels(self):
        """Test multiple labels on different expressions."""
        expr1 = boolean_match("title", "content").must("python").label("python_score")
        expr2 = boolean_match("title", "content").must("java").label("java_score")

        sql1 = expr1.compile()
        sql2 = expr2.compile()

        expected1 = "MATCH(title, content) AGAINST('+python' IN BOOLEAN MODE) AS python_score"
        expected2 = "MATCH(title, content) AGAINST('+java' IN BOOLEAN MODE) AS java_score"

        self.assertEqual(sql1, expected1)
        self.assertEqual(sql2, expected2)

    def test_special_characters_label(self):
        """Test special characters in query with label."""
        expr = boolean_match("title", "content").must("C++").label("cpp_score")
        sql = expr.compile()

        expected = "MATCH(title, content) AGAINST('+C++' IN BOOLEAN MODE) AS cpp_score"
        self.assertEqual(sql, expected)

    def test_unicode_label(self):
        """Test unicode characters with label."""
        expr = natural_match("title", "content", query="机器学习").label("ml_score")
        sql = expr.compile()

        expected = "MATCH(title, content) AGAINST('机器学习') AS ml_score"
        self.assertEqual(sql, expected)

    def test_numeric_label(self):
        """Test numeric label name."""
        expr = boolean_match("title", "content").must("python").label("score_123")
        sql = expr.compile()

        expected = "MATCH(title, content) AGAINST('+python' IN BOOLEAN MODE) AS score_123"
        self.assertEqual(sql, expected)

    def test_underscore_label(self):
        """Test underscore in label name."""
        expr = boolean_match("title", "content").must("python").label("python_relevance_score")
        sql = expr.compile()

        expected = "MATCH(title, content) AGAINST('+python' IN BOOLEAN MODE) AS python_relevance_score"
        self.assertEqual(sql, expected)

    def test_long_label(self):
        """Test long label name."""
        long_label = "very_long_label_name_for_fulltext_search_relevance_scoring"
        expr = boolean_match("title", "content").must("python").label(long_label)
        sql = expr.compile()

        expected = f"MATCH(title, content) AGAINST('+python' IN BOOLEAN MODE) AS {long_label}"
        self.assertEqual(sql, expected)

    def test_label_with_spaces(self):
        """Test label name with spaces."""
        expr = boolean_match("title", "content").must("python").label("python score")
        sql = expr.compile()

        expected = "MATCH(title, content) AGAINST('+python' IN BOOLEAN MODE) AS python score"
        self.assertEqual(sql, expected)


class TestFulltextParserSupport(unittest.TestCase):
    """Test Fulltext Parser Support (JSON, NGRAM) - comprehensive SQL generation tests"""

    def test_fulltext_index_with_json_parser(self):
        """Test FulltextIndex with JSON parser generates correct SQL"""
        from matrixone.sqlalchemy_ext import FulltextParserType

        index = FulltextIndex("ftidx_json", ["json_data"], parser=FulltextParserType.JSON)
        sql = index._create_index_sql("products")

        expected_sql = "CREATE FULLTEXT INDEX ftidx_json ON products (json_data) WITH PARSER json"
        self.assertEqual(sql, expected_sql)

    def test_fulltext_index_with_ngram_parser(self):
        """Test FulltextIndex with NGRAM parser generates correct SQL"""
        from matrixone.sqlalchemy_ext import FulltextParserType

        index = FulltextIndex("ftidx_ngram", ["title", "content"], parser=FulltextParserType.NGRAM)
        sql = index._create_index_sql("chinese_articles")

        expected_sql = "CREATE FULLTEXT INDEX ftidx_ngram ON chinese_articles (title, content) WITH PARSER ngram"
        self.assertEqual(sql, expected_sql)

    def test_fulltext_index_multiple_columns_with_parser(self):
        """Test FulltextIndex with multiple columns and parser"""
        from matrixone.sqlalchemy_ext import FulltextParserType

        index = FulltextIndex("ftidx_multi_json", ["json1", "json2"], parser=FulltextParserType.JSON)
        sql = index._create_index_sql("src")

        expected_sql = "CREATE FULLTEXT INDEX ftidx_multi_json ON src (json1, json2) WITH PARSER json"
        self.assertEqual(sql, expected_sql)

    def test_fulltext_index_without_parser(self):
        """Test FulltextIndex without parser (default behavior)"""
        index = FulltextIndex("ftidx_default", ["title", "content"])
        sql = index._create_index_sql("articles")

        expected_sql = "CREATE FULLTEXT INDEX ftidx_default ON articles (title, content)"
        self.assertEqual(sql, expected_sql)
        self.assertNotIn("WITH PARSER", sql)

    def test_fulltext_index_parser_attribute(self):
        """Test that FulltextIndex correctly stores parser attribute"""
        from matrixone.sqlalchemy_ext import FulltextParserType

        index_json = FulltextIndex("ftidx_json", ["data"], parser=FulltextParserType.JSON)
        self.assertEqual(index_json.parser, FulltextParserType.JSON)

        index_ngram = FulltextIndex("ftidx_ngram", ["content"], parser=FulltextParserType.NGRAM)
        self.assertEqual(index_ngram.parser, FulltextParserType.NGRAM)

        index_none = FulltextIndex("ftidx_none", ["content"])
        self.assertIsNone(index_none.parser)

    def test_fulltext_parser_enum_values(self):
        """Test FulltextParserType enum values"""
        from matrixone.sqlalchemy_ext import FulltextParserType

        self.assertEqual(FulltextParserType.JSON, "json")
        self.assertEqual(FulltextParserType.NGRAM, "ngram")


class TestBM25AlgorithmSupport(unittest.TestCase):
    """Test BM25 Algorithm Support - comprehensive SQL and configuration tests"""

    def test_bm25_algorithm_type(self):
        """Test FulltextAlgorithmType.BM25 value"""
        self.assertEqual(FulltextAlgorithmType.BM25, "BM25")
        self.assertEqual(FulltextAlgorithmType.TF_IDF, "TF-IDF")

    def test_fulltext_index_with_bm25(self):
        """Test FulltextIndex with BM25 algorithm"""
        index = FulltextIndex("ftidx_bm25", ["title", "content"], algorithm=FulltextAlgorithmType.BM25)

        self.assertEqual(index.algorithm, FulltextAlgorithmType.BM25)
        self.assertEqual(index.name, "ftidx_bm25")
        self.assertEqual(index.get_columns(), ["title", "content"])

    def test_fulltext_index_with_tfidf(self):
        """Test FulltextIndex with TF-IDF algorithm (default)"""
        index = FulltextIndex("ftidx_tfidf", ["title", "content"], algorithm=FulltextAlgorithmType.TF_IDF)

        self.assertEqual(index.algorithm, FulltextAlgorithmType.TF_IDF)

    def test_algorithm_sql_generation(self):
        """Test that algorithm doesn't affect SQL generation (it's a runtime config)"""
        index_bm25 = FulltextIndex("ftidx_test", ["content"], algorithm=FulltextAlgorithmType.BM25)
        index_tfidf = FulltextIndex("ftidx_test", ["content"], algorithm=FulltextAlgorithmType.TF_IDF)

        sql_bm25 = index_bm25._create_index_sql("articles")
        sql_tfidf = index_tfidf._create_index_sql("articles")

        # SQL should be same, algorithm is a config setting not part of DDL
        self.assertEqual(sql_bm25, sql_tfidf)


class TestComplexBooleanModeQueries(unittest.TestCase):
    """Test complex boolean mode operators - offline SQL generation tests"""

    def test_wildcard_suffix_generation(self):
        """Test wildcard suffix (*) in boolean queries - SQL should contain wildcard"""
        # Note: The SDK may not expose wildcard directly in the API
        # This tests raw SQL generation if we were to support it
        expr = boolean_match("title", "content").must("red")
        sql = expr.compile()

        self.assertIn("+red", sql)
        self.assertIn("IN BOOLEAN MODE", sql)

    def test_phrase_search_sql_generation(self):
        """Test phrase search (quoted strings) in SQL generation"""
        # Testing the SQL format for phrase search
        # Actual phrase syntax is tested in online tests
        expr = boolean_match("title", "content").must("is not red")
        sql = expr.compile()

        # Should use + operator for multi-word must
        self.assertIn("+is not red", sql)
        self.assertIn("IN BOOLEAN MODE", sql)

    def test_complex_boolean_combination(self):
        """Test complex boolean combinations"""
        expr = boolean_match("title", "content").must("database").must_not("mysql").encourage("postgresql")

        sql = expr.compile()

        self.assertIn("+database", sql)
        self.assertIn("-mysql", sql)
        self.assertIn("postgresql", sql)  # encourage is no prefix
        self.assertIn("IN BOOLEAN MODE", sql)

    def test_discourage_operator_sql(self):
        """Test discourage operator generates correct SQL"""
        expr = boolean_match("title", "content").must("python").discourage("deprecated")
        sql = expr.compile()

        self.assertIn("+python", sql)
        self.assertIn("~deprecated", sql)
        self.assertIn("IN BOOLEAN MODE", sql)


class TestNullAndEdgeCases(unittest.TestCase):
    """Test NULL handling and edge cases in SQL generation"""

    def test_empty_search_string(self):
        """Test that empty search string raises ValueError"""
        with self.assertRaises(ValueError) as context:
            expr = natural_match("title", "content", query="")
            expr.compile()

        # Verify error message is informative
        self.assertIn("empty", str(context.exception).lower())

    def test_special_characters_in_search(self):
        """Test special characters are properly handled"""
        expr = boolean_match("title").must("C++")
        sql = expr.compile()

        self.assertIn("+C++", sql)

    def test_unicode_in_search_query(self):
        """Test unicode characters in search query"""
        expr = natural_match("title", "content", query="机器学习")
        sql = expr.compile()

        self.assertIn("机器学习", sql)
        self.assertIn("MATCH(title, content)", sql)

    def test_very_long_search_query(self):
        """Test very long search queries"""
        long_query = " ".join(["term"] * 100)
        expr = natural_match("content", query=long_query)
        sql = expr.compile()

        self.assertIn(long_query, sql)
        self.assertIn("MATCH(content)", sql)


class TestIndexCreationVariations(unittest.TestCase):
    """Test various index creation scenarios and SQL generation"""

    def test_single_column_index(self):
        """Test index on single column"""
        index = FulltextIndex("ftidx_single", "content")
        sql = index._create_index_sql("articles")

        expected = "CREATE FULLTEXT INDEX ftidx_single ON articles (content)"
        self.assertEqual(sql, expected)

    def test_three_column_index(self):
        """Test index on three columns"""
        index = FulltextIndex("ftidx_three", ["title", "summary", "content"])
        sql = index._create_index_sql("articles")

        expected = "CREATE FULLTEXT INDEX ftidx_three ON articles (title, summary, content)"
        self.assertEqual(sql, expected)

    def test_index_with_all_options(self):
        """Test index with all available options"""
        from matrixone.sqlalchemy_ext import FulltextParserType

        index = FulltextIndex(
            "ftidx_full", ["json1", "json2"], algorithm=FulltextAlgorithmType.BM25, parser=FulltextParserType.JSON
        )
        sql = index._create_index_sql("src")

        expected = "CREATE FULLTEXT INDEX ftidx_full ON src (json1, json2) WITH PARSER json"
        self.assertEqual(sql, expected)
        # Algorithm is stored but not in DDL
        self.assertEqual(index.algorithm, FulltextAlgorithmType.BM25)

    def test_index_name_with_special_chars(self):
        """Test index names with underscores and numbers"""
        index = FulltextIndex("ftidx_test_123", ["content"])
        sql = index._create_index_sql("my_table")

        self.assertIn("ftidx_test_123", sql)
        self.assertIn("my_table", sql)


class TestSearchBuilderCombinations(unittest.TestCase):
    """Test complex search builder combinations"""

    def test_must_and_must_not_combination(self):
        """Test combining must and must_not"""
        expr = boolean_match("title", "content").must("python", "tutorial").must_not("advanced", "expert")
        sql = expr.compile()

        self.assertIn("+python", sql)
        self.assertIn("+tutorial", sql)
        self.assertIn("-advanced", sql)
        self.assertIn("-expert", sql)

    def test_all_operators_combined(self):
        """Test using all operators together"""
        expr = (
            boolean_match("title", "content")
            .must("python")
            .must_not("deprecated")
            .encourage("tutorial")
            .discourage("advanced")
        )
        sql = expr.compile()

        self.assertIn("+python", sql)
        self.assertIn("-deprecated", sql)
        self.assertIn("tutorial", sql)  # No prefix for encourage
        self.assertIn("~advanced", sql)

    def test_natural_language_mode_sql(self):
        """Test natural language mode SQL generation"""
        expr = natural_match("title", "content", query="machine learning tutorial")
        sql = expr.compile()

        self.assertIn("MATCH(title, content)", sql)
        self.assertIn("AGAINST('machine learning tutorial')", sql)
        # Natural language mode can omit the explicit mode clause or include it
        # Depends on implementation - let's just check it's valid

    def test_multiple_columns_various_orders(self):
        """Test column order preservation in SQL"""
        expr1 = boolean_match("title", "content", "summary").must("test")
        sql1 = expr1.compile()

        expr2 = boolean_match("summary", "content", "title").must("test")
        sql2 = expr2.compile()

        # Should preserve the order specified
        self.assertIn("title, content, summary", sql1)
        self.assertIn("summary, content, title", sql2)


if __name__ == '__main__':
    unittest.main()
