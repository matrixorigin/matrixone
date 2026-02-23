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
Offline tests for FulltextIndex validation.
Tests the validation logic added to fix TODO issues.
"""

import pytest
from unittest.mock import Mock, patch
from sqlalchemy.schema import CreateIndex

from matrixone.sqlalchemy_ext.fulltext_index import FulltextIndex, compile_create_index


class TestFulltextIndexValidation:
    """Test FulltextIndex validation functionality."""

    def test_empty_columns_raises_error(self):
        """Test that empty columns list raises ValueError."""
        with pytest.raises(ValueError, match="requires at least one column"):
            FulltextIndex("test_idx", [])

    def test_non_string_columns_raises_error(self):
        """Test that non-string columns raise TypeError."""
        with pytest.raises(TypeError, match="All columns must be strings"):
            FulltextIndex("test_idx", [123, 456])

    def test_mixed_type_columns_raises_error(self):
        """Test that mixed type columns raise TypeError."""
        with pytest.raises(TypeError, match="All columns must be strings"):
            FulltextIndex("test_idx", ["valid", 123])

    def test_single_string_column_valid(self):
        """Test that single string column is valid."""
        index = FulltextIndex("test_idx", "content")
        assert index._column_names == ["content"]

    def test_multiple_string_columns_valid(self):
        """Test that multiple string columns are valid."""
        index = FulltextIndex("test_idx", ["title", "content"])
        assert index._column_names == ["title", "content"]

    def test_unbound_table_raises_error(self):
        """Test that unbound table raises ValueError during compilation."""
        # Create index without binding to table
        index = FulltextIndex("test_idx", "content")

        # Mock CreateIndex element
        mock_element = Mock()
        mock_element.element = index

        # Mock compiler
        mock_compiler = Mock()

        # Test that compilation raises ValueError
        with pytest.raises(ValueError, match="not bound to a table"):
            compile_create_index(mock_element, mock_compiler)

    def test_bound_table_compiles_successfully(self):
        """Test that bound table compiles successfully."""
        from sqlalchemy import Table, Column, Integer, String, MetaData

        # Create table and bind index
        metadata = MetaData()
        table = Table('test_table', metadata, Column('id', Integer, primary_key=True), Column('content', String(255)))

        # Create index and bind to table
        index = FulltextIndex("test_idx", "content")
        index._set_parent(table)

        # Mock CreateIndex element
        mock_element = Mock()
        mock_element.element = index

        # Mock compiler
        mock_compiler = Mock()

        # Test that compilation succeeds
        result = compile_create_index(mock_element, mock_compiler)
        assert "CREATE FULLTEXT INDEX test_idx ON test_table (content)" in result

    def test_parser_included_in_ddl(self):
        """Test that parser is included in DDL when specified."""
        from sqlalchemy import Table, Column, Integer, String, MetaData
        from matrixone.sqlalchemy_ext.fulltext_index import FulltextParserType

        # Create table and bind index with parser
        metadata = MetaData()
        table = Table('test_table', metadata, Column('id', Integer, primary_key=True), Column('content', String(255)))

        # Create index with parser
        index = FulltextIndex("test_idx", "content", parser=FulltextParserType.JSON)
        index._set_parent(table)

        # Mock CreateIndex element
        mock_element = Mock()
        mock_element.element = index

        # Mock compiler
        mock_compiler = Mock()

        # Test that compilation includes parser
        result = compile_create_index(mock_element, mock_compiler)
        assert "WITH PARSER json" in result

    def test_multiple_columns_in_ddl(self):
        """Test that multiple columns are properly formatted in DDL."""
        from sqlalchemy import Table, Column, Integer, String, Text, MetaData

        # Create table with multiple text columns
        metadata = MetaData()
        table = Table(
            'test_table',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('title', String(255)),
            Column('content', Text),
        )

        # Create index with multiple columns
        index = FulltextIndex("test_idx", ["title", "content"])
        index._set_parent(table)

        # Mock CreateIndex element
        mock_element = Mock()
        mock_element.element = index

        # Mock compiler
        mock_compiler = Mock()

        # Test that compilation includes both columns
        result = compile_create_index(mock_element, mock_compiler)
        assert "CREATE FULLTEXT INDEX test_idx ON test_table (title, content)" in result
