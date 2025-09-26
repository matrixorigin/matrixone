#!/usr/bin/env python3
"""
Offline tests for fulltext index functionality.

These tests use mocked database connections to verify the fulltext index
functionality without requiring a real MatrixOne database.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from matrixone.sqlalchemy_ext import (
    FulltextIndex, FulltextAlgorithmType, FulltextModeType,
    FulltextSearchBuilder, create_fulltext_index, fulltext_search_builder
)


class TestFulltextIndex:
    """Test FulltextIndex class"""

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

    @patch('matrixone.sqlalchemy_ext.fulltext_index.text')
    def test_create_index_class_method(self, mock_text):
        """Test FulltextIndex.create_index class method"""
        # Mock engine and connection
        mock_engine = Mock()
        mock_connection = Mock()
        mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.begin.return_value.__exit__ = Mock(return_value=None)
        
        # Mock text function
        mock_text.return_value = "CREATE FULLTEXT INDEX ftidx_test ON documents (title, content)"
        
        # Test successful creation
        result = FulltextIndex.create_index(
            engine=mock_engine,
            table_name="documents",
            name="ftidx_test",
            columns=["title", "content"],
            algorithm=FulltextAlgorithmType.BM25
        )
        
        assert result is True
        mock_connection.execute.assert_called_once()

    @patch('matrixone.sqlalchemy_ext.fulltext_index.text')
    def test_create_index_class_method_failure(self, mock_text):
        """Test FulltextIndex.create_index class method with failure"""
        # Mock engine that raises exception
        mock_engine = Mock()
        mock_engine.begin.side_effect = Exception("Connection failed")
        
        # Test failed creation
        result = FulltextIndex.create_index(
            engine=mock_engine,
            table_name="documents",
            name="ftidx_test",
            columns=["title", "content"]
        )
        
        assert result is False

    @patch('matrixone.sqlalchemy_ext.fulltext_index.text')
    def test_create_index_in_transaction(self, mock_text):
        """Test FulltextIndex.create_index_in_transaction class method"""
        # Mock connection
        mock_connection = Mock()
        
        # Mock text function
        mock_text.return_value = "CREATE FULLTEXT INDEX ftidx_test ON documents (title, content)"
        
        # Test successful creation in transaction
        result = FulltextIndex.create_index_in_transaction(
            connection=mock_connection,
            table_name="documents",
            name="ftidx_test",
            columns=["title", "content"],
            algorithm=FulltextAlgorithmType.BM25
        )
        
        assert result is True
        mock_connection.execute.assert_called_once()

    @patch('matrixone.sqlalchemy_ext.fulltext_index.text')
    def test_drop_index_class_method(self, mock_text):
        """Test FulltextIndex.drop_index class method"""
        # Mock engine and connection
        mock_engine = Mock()
        mock_connection = Mock()
        mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.begin.return_value.__exit__ = Mock(return_value=None)
        
        # Mock text function
        mock_text.return_value = "DROP INDEX ftidx_test ON documents"
        
        # Test successful drop
        result = FulltextIndex.drop_index(
            engine=mock_engine,
            table_name="documents",
            name="ftidx_test"
        )
        
        assert result is True
        mock_connection.execute.assert_called_once()

    @patch('matrixone.sqlalchemy_ext.fulltext_index.text')
    def test_instance_create_method(self, mock_text):
        """Test FulltextIndex instance create method"""
        # Mock engine and connection
        mock_engine = Mock()
        mock_connection = Mock()
        mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.begin.return_value.__exit__ = Mock(return_value=None)
        
        # Mock text function
        mock_text.return_value = "CREATE FULLTEXT INDEX ftidx_test ON documents (title, content)"
        
        # Create index instance and test
        index = FulltextIndex("ftidx_test", ["title", "content"])
        result = index.create(mock_engine, "documents")
        
        assert result is True
        mock_connection.execute.assert_called_once()

    @patch('matrixone.sqlalchemy_ext.fulltext_index.text')
    def test_instance_drop_method(self, mock_text):
        """Test FulltextIndex instance drop method"""
        # Mock engine and connection
        mock_engine = Mock()
        mock_connection = Mock()
        mock_engine.begin.return_value.__enter__ = Mock(return_value=mock_connection)
        mock_engine.begin.return_value.__exit__ = Mock(return_value=None)
        
        # Mock text function
        mock_text.return_value = "DROP INDEX ftidx_test ON documents"
        
        # Create index instance and test
        index = FulltextIndex("ftidx_test", ["title", "content"])
        result = index.drop(mock_engine, "documents")
        
        assert result is True
        mock_connection.execute.assert_called_once()


class TestFulltextSearchBuilder:
    """Test FulltextSearchBuilder class"""

    def test_search_builder_creation(self):
        """Test creating FulltextSearchBuilder instance"""
        builder = FulltextSearchBuilder("documents", ["title", "content"])
        
        assert builder.table_name == "documents"
        assert builder.columns == ["title", "content"]
        assert builder.search_term is None
        assert builder.mode == FulltextModeType.NATURAL_LANGUAGE
        assert builder.with_score is False

    def test_search_builder_single_column(self):
        """Test FulltextSearchBuilder with single column"""
        builder = FulltextSearchBuilder("documents", "title")
        
        assert builder.table_name == "documents"
        assert builder.columns == ["title"]

    def test_search_builder_method_chaining(self):
        """Test FulltextSearchBuilder method chaining"""
        builder = (FulltextSearchBuilder("documents", ["title", "content"])
                  .search("Python programming")
                  .set_mode(FulltextModeType.BOOLEAN)
                  .set_with_score(True)
                  .where("id > 0")
                  .set_order_by("score", "DESC")
                  .limit(10)
                  .offset(5))
        
        assert builder.search_term == "Python programming"
        assert builder.mode == FulltextModeType.BOOLEAN
        assert builder.with_score is True
        assert builder.where_conditions == ["id > 0"]
        assert builder.order_by == "score DESC"
        assert builder.limit_value == 10
        assert builder.offset_value == 5

    def test_build_sql_natural_language(self):
        """Test SQL generation for natural language mode"""
        builder = (FulltextSearchBuilder("documents", ["title", "content"])
                  .search("machine learning")
                  .set_with_score(True))
        
        sql = builder.build_sql()
        
        expected_sql = (
            "SELECT *, MATCH(title, content) AGAINST('machine learning') AS score "
            "FROM documents "
            "WHERE MATCH(title, content) AGAINST('machine learning') "
            "ORDER BY score DESC"
        )
        assert sql == expected_sql

    def test_build_sql_boolean_mode(self):
        """Test SQL generation for boolean mode"""
        builder = (FulltextSearchBuilder("documents", ["title", "content"])
                  .search("+Python +web")
                  .set_mode(FulltextModeType.BOOLEAN)
                  .where("author = 'Alice'")
                  .limit(5))
        
        sql = builder.build_sql()
        
        expected_sql = (
            "SELECT * FROM documents "
            "WHERE MATCH(title, content) AGAINST('+Python +web' IN BOOLEAN MODE) "
            "AND author = 'Alice' LIMIT 5"
        )
        assert sql == expected_sql

    def test_build_sql_with_order_and_offset(self):
        """Test SQL generation with custom order and offset"""
        builder = (FulltextSearchBuilder("documents", ["title", "content"])
                  .search("database")
                  .set_order_by("id", "ASC")
                  .limit(10)
                  .offset(20))
        
        sql = builder.build_sql()
        
        expected_sql = (
            "SELECT * FROM documents "
            "WHERE MATCH(title, content) AGAINST('database') "
            "ORDER BY id ASC LIMIT 10 OFFSET 20"
        )
        assert sql == expected_sql

    def test_build_sql_no_search_term(self):
        """Test build_sql raises error when no search term is set"""
        builder = FulltextSearchBuilder("documents", ["title", "content"])
        
        with pytest.raises(ValueError, match="Search term is required"):
            builder.build_sql()

    def test_execute_method(self):
        """Test execute method"""
        # Mock connection and result
        mock_connection = Mock()
        mock_result = Mock()
        mock_connection.execute.return_value = mock_result
        
        builder = (FulltextSearchBuilder("documents", ["title", "content"])
                  .search("test"))
        
        result = builder.execute(mock_connection)
        
        assert result == mock_result
        mock_connection.execute.assert_called_once()


class TestConvenienceFunctions:
    """Test convenience functions"""

    @patch('matrixone.sqlalchemy_ext.fulltext_index.FulltextIndex')
    def test_create_fulltext_index_function(self, mock_fulltext_index):
        """Test create_fulltext_index convenience function"""
        mock_engine = Mock()
        mock_fulltext_index.create_index.return_value = True
        
        result = create_fulltext_index(
            engine=mock_engine,
            table_name="documents",
            name="ftidx_test",
            columns=["title", "content"],
            algorithm=FulltextAlgorithmType.BM25
        )
        
        assert result is True
        mock_fulltext_index.create_index.assert_called_once_with(
            mock_engine, "documents", "ftidx_test", ["title", "content"], FulltextAlgorithmType.BM25
        )

    def test_fulltext_search_builder_function(self):
        """Test fulltext_search_builder convenience function"""
        builder = fulltext_search_builder("documents", ["title", "content"])
        
        assert isinstance(builder, FulltextSearchBuilder)
        assert builder.table_name == "documents"
        assert builder.columns == ["title", "content"]

    def test_fulltext_search_builder_function_single_column(self):
        """Test fulltext_search_builder with single column"""
        builder = fulltext_search_builder("documents", "title")
        
        assert isinstance(builder, FulltextSearchBuilder)
        assert builder.table_name == "documents"
        assert builder.columns == ["title"]


class TestFulltextAlgorithmType:
    """Test FulltextAlgorithmType enum"""

    def test_algorithm_types(self):
        """Test algorithm type constants"""
        assert FulltextAlgorithmType.TF_IDF == "TF-IDF"
        assert FulltextAlgorithmType.BM25 == "BM25"


class TestFulltextModeType:
    """Test FulltextModeType enum"""

    def test_mode_types(self):
        """Test mode type constants"""
        assert FulltextModeType.NATURAL_LANGUAGE == "natural language mode"
        assert FulltextModeType.BOOLEAN == "boolean mode"
        assert FulltextModeType.QUERY_EXPANSION == "query expansion mode"


if __name__ == "__main__":
    pytest.main([__file__])
