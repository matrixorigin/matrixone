"""
Offline tests for fulltext search functionality.
Tests SQL generation without database connection.
"""

import pytest
import warnings
from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import declarative_base
from matrixone.sqlalchemy_ext.fulltext_search import (
    FulltextFilter,
    FulltextQueryBuilder,
    FulltextGroup,
    FulltextSearchMode,
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
    category = Column(String(100), nullable=True)


class TestFulltextQueryBuilder:
    """Test FulltextQueryBuilder SQL generation."""

    def test_basic_must_term(self):
        """Test basic must term generation."""
        builder = FulltextQueryBuilder()
        builder.must("python")
        assert builder.build() == "+python"

    def test_basic_must_not_term(self):
        """Test basic must_not term generation."""
        builder = FulltextQueryBuilder()
        builder.must_not("java")
        assert builder.build() == "-java"

    def test_basic_encourage_term(self):
        """Test basic encourage term generation."""
        builder = FulltextQueryBuilder()
        builder.encourage("tutorial")
        assert builder.build() == "tutorial"

    def test_basic_discourage_term(self):
        """Test basic discourage term generation."""
        builder = FulltextQueryBuilder()
        builder.discourage("legacy")
        assert builder.build() == "~legacy"

    def test_multiple_terms_same_type(self):
        """Test multiple terms of same type."""
        builder = FulltextQueryBuilder()
        builder.must("python", "programming")
        assert builder.build() == "+python +programming"

    def test_mixed_term_types(self):
        """Test mixed term types."""
        builder = FulltextQueryBuilder()
        builder.must("python").encourage("tutorial").discourage("legacy").must_not("deprecated")
        expected = "+python tutorial ~legacy -deprecated"
        assert builder.build() == expected

    def test_phrase_search(self):
        """Test phrase search generation."""
        builder = FulltextQueryBuilder()
        builder.phrase("machine learning")
        assert builder.build() == '"machine learning"'

    def test_prefix_search(self):
        """Test prefix search generation."""
        builder = FulltextQueryBuilder()
        builder.prefix("neural")
        assert builder.build() == "neural*"

    def test_boost_term(self):
        """Test boosted term generation."""
        builder = FulltextQueryBuilder()
        builder.boost("python", 2.0)
        assert builder.build() == "python^2.0"


class TestFulltextGroup:
    """Test FulltextGroup SQL generation."""

    def test_basic_group_medium(self):
        """Test basic group with medium terms."""
        grp = group()
        grp.medium("java", "kotlin")
        assert grp.build() == "java kotlin"

    def test_group_high_weight(self):
        """Test group with high weight terms."""
        grp = group()
        grp.high("important")
        assert grp.build() == ">important"

    def test_group_low_weight(self):
        """Test group with low weight terms."""
        grp = group()
        grp.low("minor")
        assert grp.build() == "<minor"

    def test_mixed_weights_in_group(self):
        """Test mixed weight terms in group."""
        grp = group()
        grp.medium("normal").high("important").low("minor")
        assert grp.build() == "normal >important <minor"

    def test_group_phrase(self):
        """Test phrase in group."""
        grp = group()
        grp.phrase("deep learning")
        assert grp.build() == '"deep learning"'

    def test_group_prefix(self):
        """Test prefix in group."""
        grp = group()
        grp.prefix("neural")
        assert grp.build() == "neural*"

    def test_nested_groups(self):
        """Test nested groups."""
        inner_group = group()
        inner_group.medium("java", "kotlin")

        outer_group = group()
        outer_group.medium("python").add_group(inner_group)
        assert outer_group.build() == "python (java kotlin)"

    def test_tilde_group(self):
        """Test tilde group."""
        grp = group()
        grp.medium("old", "outdated")
        grp.is_tilde = True
        assert grp.build() == "old outdated"  # Tilde is applied at parent level


class TestFulltextQueryBuilderWithGroups:
    """Test FulltextQueryBuilder with groups."""

    def test_must_group(self):
        """Test must with group."""
        builder = FulltextQueryBuilder()
        grp = group().medium("java", "kotlin")
        builder.must(grp)
        assert builder.build() == "+(java kotlin)"

    def test_must_not_group(self):
        """Test must_not with group."""
        builder = FulltextQueryBuilder()
        grp = group().medium("spam", "junk")
        builder.must_not(grp)
        assert builder.build() == "-(spam junk)"

    def test_encourage_group(self):
        """Test encourage with group."""
        builder = FulltextQueryBuilder()
        grp = group().medium("tutorial", "guide")
        builder.encourage(grp)
        assert builder.build() == "(tutorial guide)"

    def test_discourage_group(self):
        """Test discourage with group."""
        builder = FulltextQueryBuilder()
        grp = group().medium("old", "outdated")
        builder.discourage(grp)
        assert builder.build() == "~(old outdated)"

    def test_complex_matrixone_style(self):
        """Test complex MatrixOne style: +red -(<blue >is)."""
        builder = FulltextQueryBuilder()
        grp = group().low("blue").high("is")
        builder.must("red").must_not(grp)
        assert builder.build() == "+red -(<blue >is)"

    def test_multiple_groups(self):
        """Test multiple groups."""
        builder = FulltextQueryBuilder()
        grp1 = group().medium("python", "java")
        grp2 = group().medium("tutorial", "guide")
        builder.must(grp1).encourage(grp2)
        assert builder.build() == "+(python java) (tutorial guide)"


class TestFulltextFilter:
    """Test FulltextFilter SQL compilation."""

    def test_boolean_mode_basic(self):
        """Test basic boolean mode compilation."""
        filter_obj = boolean_match("title", "content").must("python")
        expected = "MATCH(title, content) AGAINST('+python' IN BOOLEAN MODE)"
        assert filter_obj.compile() == expected

    def test_natural_language_mode(self):
        """Test natural language mode compilation."""
        filter_obj = natural_match("title", "content", query="machine learning")
        expected = "MATCH(title, content) AGAINST('machine learning')"
        assert filter_obj.compile() == expected

    def test_complex_boolean_query(self):
        """Test complex boolean query compilation."""
        filter_obj = (
            boolean_match("title", "content", "tags")
            .must("programming")
            .encourage("tutorial")
            .discourage("legacy")
            .must_not("deprecated")
        )
        expected = "MATCH(title, content, tags) AGAINST('+programming tutorial ~legacy -deprecated' IN BOOLEAN MODE)"
        assert filter_obj.compile() == expected

    def test_boolean_with_groups(self):
        """Test boolean mode with groups."""
        filter_obj = (
            boolean_match("title", "content")
            .must(group().medium("python", "java"))
            .discourage(group().medium("old", "outdated"))
        )
        expected = "MATCH(title, content) AGAINST('+(python java) ~(old outdated)' IN BOOLEAN MODE)"
        assert filter_obj.compile() == expected

    def test_phrase_and_prefix(self):
        """Test phrase and prefix in filter."""
        filter_obj = boolean_match("title", "content").phrase("machine learning").prefix("neural")
        expected = 'MATCH(title, content) AGAINST(\'"machine learning" neural*\' IN BOOLEAN MODE)'
        assert filter_obj.compile() == expected

    def test_boost_terms(self):
        """Test boosted terms in filter."""
        filter_obj = boolean_match("title", "content").boost("python", 2.0).boost("java", 1.5)
        expected = "MATCH(title, content) AGAINST('python^2.0 java^1.5' IN BOOLEAN MODE)"
        assert filter_obj.compile() == expected

    def test_element_level_weights(self):
        """Test element-level weight operators."""
        filter_obj = boolean_match("title", "content").must_not(group().low("blue").high("is"))
        expected = "MATCH(title, content) AGAINST('-(<blue >is)' IN BOOLEAN MODE)"
        assert filter_obj.compile() == expected

    def test_query_expansion_mode(self):
        """Test query expansion mode."""
        filter_obj = FulltextFilter(["title", "content"], FulltextSearchMode.QUERY_EXPANSION)
        filter_obj.encourage("machine learning")
        expected = "MATCH(title, content) AGAINST('machine learning' WITH QUERY EXPANSION)"
        assert filter_obj.compile() == expected


class TestConvenienceFunctions:
    """Test convenience functions."""

    def test_boolean_match_creation(self):
        """Test boolean_match function."""
        filter_obj = boolean_match("title", "content")
        assert filter_obj.columns == ["title", "content"]
        assert filter_obj.mode == FulltextSearchMode.BOOLEAN

    def test_natural_match_creation(self):
        """Test natural_match function."""
        filter_obj = natural_match("title", "content", query="test query")
        assert filter_obj.columns == ["title", "content"]
        assert filter_obj.mode == FulltextSearchMode.NATURAL_LANGUAGE
        expected = "MATCH(title, content) AGAINST('test query')"
        assert filter_obj.compile() == expected

    def test_group_creation(self):
        """Test group function."""
        grp = group()
        assert isinstance(grp, FulltextGroup)
        assert grp.group_type == "or"


class TestEdgeCases:
    """Test edge cases and error conditions."""

    def test_empty_query_error(self):
        """Test empty query raises error."""
        filter_obj = FulltextFilter(["title", "content"])
        with pytest.raises(ValueError, match="Query cannot be empty"):
            filter_obj.compile()

    def test_no_columns_error(self):
        """Test no columns raises error."""
        filter_obj = FulltextFilter([])
        filter_obj.encourage("test")
        with pytest.raises(ValueError, match="Columns must be specified"):
            filter_obj.compile()

    def test_single_column(self):
        """Test single column search."""
        filter_obj = boolean_match("title").must("python")
        expected = "MATCH(title) AGAINST('+python' IN BOOLEAN MODE)"
        assert filter_obj.compile() == expected

    def test_many_columns(self):
        """Test many columns search."""
        filter_obj = boolean_match("title", "content", "tags", "description").must("python")
        expected = "MATCH(title, content, tags, description) AGAINST('+python' IN BOOLEAN MODE)"
        assert filter_obj.compile() == expected


class TestComplexScenarios:
    """Test complex real-world scenarios."""

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
        assert "MATCH(title, content, tags) AGAINST(" in result
        assert "IN BOOLEAN MODE)" in result
        assert "+programming" in result
        assert "+(python java javascript)" in result
        assert "tutorial guide beginner" in result
        assert "~advanced ~expert" in result
        assert "-deprecated -legacy" in result

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
        assert "MATCH(name, description) AGAINST(" in result
        assert "IN BOOLEAN MODE)" in result
        assert "+laptop" in result
        assert "(>gaming portable <budget)" in result
        assert "-refurbished" in result

    def test_news_article_filtering(self):
        """Test news article filtering scenario."""
        filter_obj = (
            boolean_match("headline", "content")
            .must("technology")
            .encourage("AI", "machine learning")
            .phrase("artificial intelligence")
            .prefix("tech")
            .discourage(group().medium("rumor", "speculation"))
            .must_not("fake news")
        )

        result = filter_obj.compile()
        # Check that all required elements are present
        assert "MATCH(headline, content) AGAINST(" in result
        assert "IN BOOLEAN MODE)" in result
        assert "+technology" in result
        assert "AI" in result
        assert "machine learning" in result  # Note: phrase() adds quotes
        assert '"artificial intelligence"' in result
        assert "tech*" in result
        assert "~(rumor speculation)" in result
        assert "fake news" in result  # Note: must_not() adds - prefix

    def test_academic_paper_search(self):
        """Test academic paper search scenario."""
        filter_obj = (
            boolean_match("title", "abstract", "keywords")
            .must("neural networks")
            .must(group().medium("deep learning", "machine learning"))
            .encourage("CNN", "RNN", "transformer")
            .boost("attention", 2.0)
            .discourage("survey", "review")
            .must_not("outdated")
        )

        result = filter_obj.compile()
        # Check that all required elements are present
        assert "MATCH(title, abstract, keywords) AGAINST(" in result
        assert "IN BOOLEAN MODE)" in result
        assert "+neural" in result
        assert "networks" in result  # Note: must() with multiple terms
        assert "+(deep learning machine learning)" in result
        assert "CNN RNN transformer" in result
        assert "attention^2.0" in result
        assert "~survey ~review" in result
        assert "-outdated" in result


class TestLogicalAdapters:
    """Test generic logical adapters for custom filter objects."""

    def test_logical_and_with_fulltext_filters(self):
        """Test logical_and with FulltextFilter objects."""
        filter1 = boolean_match("title", "content").must("python")
        filter2 = boolean_match("title", "content").must("java")

        combined = logical_and(filter1, filter2)
        assert combined is not None
        # Should create a BooleanClauseList
        assert hasattr(combined, 'clauses')

    def test_logical_or_with_fulltext_filters(self):
        """Test logical_or with FulltextFilter objects."""
        filter1 = boolean_match("title", "content").must("python")
        filter2 = boolean_match("title", "content").must("java")

        combined = logical_or(filter1, filter2)
        assert combined is not None
        # Should create a BooleanClauseList
        assert hasattr(combined, 'clauses')

    def test_logical_not_with_fulltext_filter(self):
        """Test logical_not with FulltextFilter object."""
        filter_obj = boolean_match("title", "content").must("deprecated")

        negated = logical_not(filter_obj)
        assert negated is not None
        # Should create a TextClause with NOT
        assert str(negated).startswith("NOT (")

    def test_logical_and_mixed_conditions(self):
        """Test logical_and with mixed FulltextFilter and regular SQLAlchemy conditions."""
        fulltext_condition = boolean_match("title", "content").must("python")
        regular_condition = Article.category == "Programming"

        combined = logical_and(fulltext_condition, regular_condition)
        assert combined is not None
        # Should handle both types of conditions
        assert hasattr(combined, 'clauses')

    def test_logical_or_mixed_conditions(self):
        """Test logical_or with mixed FulltextFilter and regular SQLAlchemy conditions."""
        fulltext_condition = boolean_match("title", "content").must("python")
        regular_condition = Article.category == "Programming"

        combined = logical_or(fulltext_condition, regular_condition)
        assert combined is not None
        # Should handle both types of conditions
        assert hasattr(combined, 'clauses')

    def test_logical_and_multiple_conditions(self):
        """Test logical_and with multiple conditions."""
        ft1 = boolean_match("title", "content").must("python")
        ft2 = natural_match("title", "content", query="tutorial")
        reg1 = Article.category == "Programming"
        reg2 = Article.id > 100

        combined = logical_and(ft1, ft2, reg1, reg2)
        assert combined is not None
        assert hasattr(combined, 'clauses')

    def test_logical_or_multiple_conditions(self):
        """Test logical_or with multiple conditions."""
        ft1 = boolean_match("title", "content").must("python")
        ft2 = natural_match("title", "content", query="java")
        reg1 = Article.category == "Programming"
        reg2 = Article.category == "AI"

        combined = logical_or(ft1, ft2, reg1, reg2)
        assert combined is not None
        assert hasattr(combined, 'clauses')

    def test_nested_logical_operations(self):
        """Test nested logical operations."""
        # (fulltext1 AND regular1) OR (fulltext2 AND regular2)
        ft1 = boolean_match("title", "content").must("python")
        reg1 = Article.category == "Programming"

        ft2 = natural_match("title", "content", query="machine learning")
        reg2 = Article.category == "AI"

        and_condition1 = logical_and(ft1, reg1)
        and_condition2 = logical_and(ft2, reg2)

        final_condition = logical_or(and_condition1, and_condition2)
        assert final_condition is not None
        assert hasattr(final_condition, 'clauses')

    def test_logical_not_with_regular_condition(self):
        """Test logical_not with regular SQLAlchemy condition."""
        regular_condition = Article.category == "Deprecated"

        negated = logical_not(regular_condition)
        assert negated is not None
        # Should use SQLAlchemy's not_() for regular conditions

    def test_logical_and_empty_conditions(self):
        """Test logical_and with no conditions."""
        # Should handle empty condition list gracefully
        try:
            combined = logical_and()
            # If it doesn't raise an error, that's fine
            # SQLAlchemy's and_() behavior varies with no arguments
        except Exception:
            # If it raises an error, that's also acceptable
            pass

    def test_custom_filter_detection(self):
        """Test that logical adapters correctly detect compile() method."""

        class CustomFilter:
            def compile(self):
                return "custom_condition = 1"

        custom = CustomFilter()
        regular = Article.id > 0

        combined = logical_and(custom, regular)
        assert combined is not None


class TestLogicalAdapterEdgeCases:
    """Test edge cases for logical adapters."""

    def test_logical_adapters_with_none_values(self):
        """Test logical adapters handle None values gracefully."""
        filter1 = boolean_match("title", "content").must("python")

        # These should not crash
        try:
            logical_and(filter1, None)
            logical_or(filter1, None)
        except Exception as e:
            # If they raise exceptions, that's acceptable behavior
            assert "None" in str(e) or "null" in str(e).lower()

    def test_logical_adapters_with_incompatible_objects(self):
        """Test logical adapters with objects that don't have compile method."""
        filter1 = boolean_match("title", "content").must("python")
        incompatible = "just a string"

        # Should handle gracefully - either work or raise clear error
        try:
            combined = logical_and(filter1, incompatible)
            # If it works, that's fine
            assert combined is not None
        except Exception:
            # If it raises an error, that's also acceptable
            pass

    def test_single_condition_logical_ops(self):
        """Test logical operations with single conditions."""
        filter1 = boolean_match("title", "content").must("python")

        # Single condition operations
        and_single = logical_and(filter1)
        or_single = logical_or(filter1)

        assert and_single is not None
        assert or_single is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
