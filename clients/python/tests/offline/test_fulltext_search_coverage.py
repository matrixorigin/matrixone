"""
Additional tests to improve fulltext search coverage.
"""

import pytest
from sqlalchemy import Column, Integer, String
from matrixone.sqlalchemy_ext.fulltext_search import (
    FulltextFilter,
    FulltextQueryBuilder,
    FulltextGroup,
    FulltextSearchMode,
    FulltextSearchAlgorithm,
    FulltextSearchBuilder,
    FulltextIndexManager,
    boolean_match,
    natural_match,
    group,
    fulltext_and,
    fulltext_or
)


class MockColumn:
    """Mock SQLAlchemy column for testing."""
    def __init__(self, name):
        self.name = name


class MockClient:
    """Mock client for testing FulltextSearchBuilder."""
    def __init__(self):
        self.executed_sql = []
    
    def execute(self, sql):
        self.executed_sql.append(sql)
        return []
    
    def get_sqlalchemy_engine(self):
        return None


class TestFulltextSearchModes:
    """Test different search modes and algorithms."""
    
    def test_search_mode_constants(self):
        """Test search mode constants."""
        assert FulltextSearchMode.NATURAL_LANGUAGE == "natural language mode"
        assert FulltextSearchMode.BOOLEAN == "boolean mode"
        assert FulltextSearchMode.QUERY_EXPANSION == "query expansion mode"
    
    def test_search_algorithm_constants(self):
        """Test search algorithm constants."""
        assert FulltextSearchAlgorithm.TF_IDF == "TF-IDF"
        assert FulltextSearchAlgorithm.BM25 == "BM25"


class TestFulltextFilterModes:
    """Test FulltextFilter mode switching."""
    
    def test_natural_language_mode_switch(self):
        """Test switching to natural language mode."""
        filter_obj = boolean_match("title", "content")
        filter_obj.natural_language()
        filter_obj.encourage("test")
        
        result = filter_obj.compile()
        assert "MATCH(title, content) AGAINST('test')" == result
    
    def test_boolean_mode_switch(self):
        """Test switching to boolean mode."""
        filter_obj = FulltextFilter(["title", "content"], FulltextSearchMode.NATURAL_LANGUAGE)
        filter_obj.boolean_mode()
        filter_obj.must("test")
        
        result = filter_obj.compile()
        assert "MATCH(title, content) AGAINST('+test' IN BOOLEAN MODE)" == result
    
    def test_query_expansion_mode_switch(self):
        """Test switching to query expansion mode."""
        filter_obj = boolean_match("title", "content")
        filter_obj.query_expansion()
        filter_obj.encourage("test")
        
        result = filter_obj.compile()
        assert "MATCH(title, content) AGAINST('test' WITH QUERY EXPANSION)" == result


class TestFulltextFilterColumnHandling:
    """Test column handling in FulltextFilter."""
    
    def test_columns_attribute(self):
        """Test columns attribute."""
        filter_obj = FulltextFilter(["title"])
        filter_obj.columns = ["title", "content", "tags"]
        filter_obj.must("test")
        
        result = filter_obj.compile()
        assert "MATCH(title, content, tags)" in result
    
    def test_sqlalchemy_column_objects(self):
        """Test with SQLAlchemy column objects."""
        col1 = MockColumn("title")
        col2 = MockColumn("content")
        
        filter_obj = boolean_match(col1, col2)
        filter_obj.must("test")
        
        result = filter_obj.compile()
        assert "MATCH(title, content)" in result


class TestFulltextGroupAdvanced:
    """Test advanced FulltextGroup functionality."""
    
    def test_group_types(self):
        """Test different group types."""
        grp_or = FulltextGroup("or")
        grp_and = FulltextGroup("and")
        grp_not = FulltextGroup("not")
        
        assert grp_or.group_type == "or"
        assert grp_and.group_type == "and"
        assert grp_not.group_type == "not"
    
    def test_group_must_must_not_behavior(self):
        """Test must/must_not behavior in different group types."""
        # Test in OR group (should not add +/- operators)
        grp_or = FulltextGroup("or")
        grp_or.must("test")
        assert grp_or.build() == "test"
        
        # Test in main group (should add + operator)
        grp_main = FulltextGroup("and")
        grp_main.must("test")
        assert grp_main.build() == "+test"
    
    def test_nested_group_building(self):
        """Test nested group building with different types."""
        inner = group()
        inner.medium("java", "kotlin")
        inner.group_type = "not"
        
        outer = group()
        outer.medium("python").add_group(inner)
        
        result = outer.build()
        assert "python" in result
        assert "-(java kotlin)" in result
    
    def test_tilde_group_building(self):
        """Test tilde group building."""
        inner = group()
        inner.medium("old", "outdated")
        
        outer = group()
        outer.medium("python").add_tilde_group(inner)
        
        result = outer.build()
        assert "python" in result
        assert "~(old outdated)" in result


class TestFulltextSearchBuilder:
    """Test FulltextSearchBuilder functionality."""
    
    def test_basic_builder_setup(self):
        """Test basic builder setup."""
        client = MockClient()
        builder = FulltextSearchBuilder(client)
        
        assert builder.client == client
        assert builder._table_name is None
        assert builder._columns == []
        assert builder._search_mode == FulltextSearchMode.NATURAL_LANGUAGE
    
    def test_builder_chaining(self):
        """Test builder method chaining."""
        client = MockClient()
        builder = (FulltextSearchBuilder(client)
                  .table("articles")
                  .columns(["title", "content"])
                  .with_mode(FulltextSearchMode.BOOLEAN)
                  .with_algorithm(FulltextSearchAlgorithm.BM25)
                  .query("test query")
                  .with_score()
                  .select(["id", "title"])
                  .where("category = 'tech'")
                  .order_by("score", "DESC")
                  .limit(10)
                  .offset(5))
        
        assert builder._table_name == "articles"
        assert builder._columns == ["title", "content"]
        assert builder._search_mode == FulltextSearchMode.BOOLEAN
        assert builder._algorithm == FulltextSearchAlgorithm.BM25
        assert builder._include_score == True
        assert builder._select_columns == ["id", "title"]
        assert builder._where_conditions == ["category = 'tech'"]
        assert builder._order_by == "score DESC"
        assert builder._limit_value == 10
        assert builder._offset_value == 5
    
    def test_add_term_variations(self):
        """Test add_term with different parameters."""
        client = MockClient()
        builder = FulltextSearchBuilder(client)
        
        # Required term
        builder.add_term("python", required=True)
        # Excluded term
        builder.add_term("java", excluded=True)
        # Optional term
        builder.add_term("tutorial")
        
        query_string = builder._query_obj.build()
        assert "+python" in query_string
        assert "-java" in query_string
        assert "tutorial" in query_string
    
    def test_add_phrase_and_wildcard(self):
        """Test add_phrase and add_wildcard methods."""
        client = MockClient()
        builder = FulltextSearchBuilder(client)
        
        builder.add_phrase("machine learning")
        builder.add_wildcard("neural*")
        
        query_string = builder._query_obj.build()
        assert '"machine learning"' in query_string
        assert "neural*" in query_string
    
    def test_sql_building(self):
        """Test SQL building."""
        client = MockClient()
        builder = (FulltextSearchBuilder(client)
                  .table("articles")
                  .columns(["title", "content"])
                  .query("test")
                  .with_score()
                  .where("category = 'tech'")
                  .order_by("score", "DESC")
                  .limit(5))
        
        sql = builder._build_sql()
        assert "SELECT" in sql
        assert "FROM articles" in sql
        assert "MATCH(title, content)" in sql
        assert "WHERE" in sql
        assert "ORDER BY score DESC" in sql
        assert "LIMIT 5" in sql
    
    def test_explain_method(self):
        """Test explain method."""
        client = MockClient()
        builder = (FulltextSearchBuilder(client)
                  .table("articles")
                  .columns(["title", "content"])
                  .query("test"))
        
        sql = builder.explain()
        assert isinstance(sql, str)
        assert "MATCH" in sql
    
    def test_execute_method(self):
        """Test execute method."""
        client = MockClient()
        builder = (FulltextSearchBuilder(client)
                  .table("articles")
                  .columns(["title", "content"])
                  .query("test"))
        
        result = builder.execute()
        assert len(client.executed_sql) == 1
        assert "MATCH" in client.executed_sql[0]
    
    def test_builder_error_conditions(self):
        """Test builder error conditions."""
        client = MockClient()
        builder = FulltextSearchBuilder(client)
        
        # No table name
        with pytest.raises(ValueError, match="Table name is required"):
            builder._build_sql()
        
        # No columns
        builder.table("articles")
        with pytest.raises(ValueError, match="Search columns are required"):
            builder._build_sql()
        
        # No query
        builder.columns(["title", "content"])
        with pytest.raises(ValueError, match="Query is required"):
            builder._build_sql()


class TestFulltextIndexManager:
    """Test FulltextIndexManager functionality."""
    
    def test_index_manager_creation(self):
        """Test index manager creation."""
        client = MockClient()
        manager = FulltextIndexManager(client)
        
        assert manager.client == client
    
    def test_search_builder_creation(self):
        """Test search builder creation from manager."""
        client = MockClient()
        manager = FulltextIndexManager(client)
        
        builder = manager.search()
        assert isinstance(builder, FulltextSearchBuilder)
        assert builder.client == client


class TestSQLAlchemyCompatibilityAdvanced:
    """Test advanced SQLAlchemy compatibility features."""
    
    def test_fulltext_filter_as_text(self):
        """Test as_text method."""
        filter_obj = boolean_match("title", "content").must("python")
        text_obj = filter_obj.as_text()
        
        # Should return a SQLAlchemy text object
        assert hasattr(text_obj, 'text')
    
    def test_fulltext_filter_string_representations(self):
        """Test string representations."""
        filter_obj = boolean_match("title", "content").must("python")
        
        str_repr = str(filter_obj)
        assert "FulltextFilter" in str_repr
        
        repr_str = repr(filter_obj)
        assert "FulltextFilter" in repr_str
        assert "columns=" in repr_str
        assert "mode=" in repr_str
    
    def test_compiler_dispatch(self):
        """Test _compiler_dispatch method."""
        filter_obj = boolean_match("title", "content").must("python")
        
        # Mock visitor
        class MockVisitor:
            def process(self, text_obj, **kw):
                return f"PROCESSED: {text_obj.text}"
        
        visitor = MockVisitor()
        result = filter_obj._compiler_dispatch(visitor)
        assert "PROCESSED:" in result
        assert "MATCH" in result


class TestErrorHandlingAndEdgeCases:
    """Test error handling and edge cases."""
    
    def test_empty_group_building(self):
        """Test empty group building."""
        grp = group()
        result = grp.build()
        assert result == ""
    
    def test_filter_with_empty_columns(self):
        """Test filter with empty columns list."""
        filter_obj = FulltextFilter([])
        filter_obj.encourage("test")
        
        with pytest.raises(ValueError, match="Columns must be specified"):
            filter_obj.compile()
    
    def test_filter_with_no_query(self):
        """Test filter with no query terms."""
        filter_obj = FulltextFilter(["title", "content"])
        
        with pytest.raises(ValueError, match="Query cannot be empty"):
            filter_obj.compile()
    
    def test_unknown_search_mode(self):
        """Test unknown search mode handling."""
        filter_obj = FulltextFilter(["title", "content"], "unknown_mode")
        filter_obj.encourage("test")
        
        result = filter_obj.compile()
        # Should default to basic AGAINST syntax
        assert "MATCH(title, content) AGAINST('test')" == result


class TestComplexIntegrationScenarios:
    """Test complex integration scenarios."""
    
    def test_deeply_nested_groups(self):
        """Test deeply nested groups."""
        level3 = group().medium("deep", "nested")
        level2 = group().medium("middle").add_group(level3)
        level1 = group().medium("top").add_group(level2)
        
        builder = FulltextQueryBuilder()
        builder.must(level1)
        
        result = builder.build()
        assert "+(top (middle (deep nested)))" == result
    
    def test_mixed_operations_complex(self):
        """Test complex mixed operations."""
        filter_obj = (boolean_match("title", "content", "tags")
                     .must("programming")
                     .encourage(group().high("python").medium("tutorial"))
                     .discourage(group().low("legacy").medium("deprecated"))
                     .must_not("spam")
                     .phrase("best practices")
                     .prefix("modern")
                     .boost("framework", 1.5))
        
        result = filter_obj.compile()
        # Verify all components are present
        assert "MATCH(title, content, tags)" in result
        assert "IN BOOLEAN MODE" in result
        assert "+programming" in result
        assert "(>python tutorial)" in result
        assert "~(<legacy deprecated)" in result
        assert "-spam" in result
        assert '"best practices"' in result
        assert "modern*" in result
        assert "framework^1.5" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
