"""
Offline tests for all JSON functionality - verify SQL generation.

Tests cover:
- JSON functions (json_extract, json_set, json_insert, json_replace)
- SQLAlchemy standard syntax (column['key'], .astext, .cast())
- Boolean handling
"""

import pytest
from sqlalchemy import Column, Integer, String, select, update, Numeric
from sqlalchemy.dialects.mysql import JSON

from matrixone.orm import declarative_base
from matrixone.sqlalchemy_ext import (
    JSON,
    json_extract,
    json_extract_float64,
    json_extract_string,
    json_insert,
    json_replace,
    json_set,
)

Base = declarative_base()


class JsonModel(Base):
    """Unified test model with JSON column."""

    __tablename__ = 'test_json_all'
    id = Column(Integer, primary_key=True)
    name = Column(String(100))
    data = Column(JSON)  # MatrixOne JSON type with standard syntax support


class TestJSONExtract:
    """Test json_extract function SQL generation."""

    def test_single_path(self):
        """Test single path extraction."""
        stmt = select(JsonModel.name, json_extract(JsonModel.data, '$.field').label('value'))
        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "json_extract(test_json_all.data, '$.field')" in sql
        assert ":json_extract" not in sql  # Path should be literal

    def test_multiple_paths(self):
        """Test multiple paths extraction."""
        stmt = select(json_extract(JsonModel.data, '$.field1', '$.field2').label('result'))
        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "'$.field1', '$.field2'" in sql

    def test_nested_path(self):
        """Test nested path extraction."""
        stmt = select(json_extract(JsonModel.data, '$.user.address.city'))
        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "'$.user.address.city'" in sql

    def test_array_index(self):
        """Test array index extraction."""
        stmt = select(json_extract(JsonModel.data, '$.items[0]'))
        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "'$.items[0]'" in sql

    def test_wildcard_paths(self):
        """Test wildcard path extraction."""
        # Just verify statements can be created
        stmt1 = select(json_extract(JsonModel.data, '$[*]'))
        stmt2 = select(json_extract(JsonModel.data, '$.*'))
        stmt3 = select(json_extract(JsonModel.data, '$**.name'))
        assert stmt1 is not None
        assert stmt2 is not None
        assert stmt3 is not None


class TestJSONExtractTyped:
    """Test typed extraction functions."""

    def test_extract_string(self):
        """Test json_extract_string SQL generation."""
        stmt = select(json_extract_string(JsonModel.data, '$.name'))
        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "json_extract_string" in sql
        assert "'$.name'" in sql

    def test_extract_float64(self):
        """Test json_extract_float64 SQL generation."""
        stmt = select(json_extract_float64(JsonModel.data, '$.price'))
        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "json_extract_float64" in sql
        assert "'$.price'" in sql


class TestJSONModification:
    """Test json_set/insert/replace SQL generation."""

    def test_json_set(self):
        """Test json_set SQL generation."""
        stmt = update(JsonModel).values(data=json_set(JsonModel.data, '$.field', 'value'))
        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "json_set(test_json_all.data, '$.field'" in sql
        assert "value" in sql

    def test_json_insert(self):
        """Test json_insert SQL generation."""
        stmt = update(JsonModel).values(data=json_insert(JsonModel.data, '$.new_field', 'value'))
        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "json_insert(test_json_all.data, '$.new_field'" in sql

    def test_json_replace(self):
        """Test json_replace SQL generation."""
        stmt = update(JsonModel).values(data=json_replace(JsonModel.data, '$.field', 'new_value'))
        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "json_replace(test_json_all.data, '$.field'" in sql

    def test_multiple_fields(self):
        """Test multiple field updates."""
        stmt = update(JsonModel).values(data=json_set(JsonModel.data, '$.field1', 'value1', '$.field2', 'value2'))
        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "'$.field1'" in sql
        assert "'$.field2'" in sql


class TestStandardSyntax:
    """Test SQLAlchemy standard JSON syntax."""

    def test_dict_access_syntax(self):
        """Test column['key'] syntax exists."""
        expr = JsonModel.data['brand']
        assert expr is not None
        assert hasattr(expr, 'astext')
        assert hasattr(expr, 'cast')
        assert hasattr(expr, 'asbool')

    def test_nested_access(self):
        """Test nested access column['key1']['key2']."""
        expr = JsonModel.data['hardware']['processor']
        assert expr is not None
        assert hasattr(expr, 'astext')

    def test_astext_property(self):
        """Test .astext property."""
        expr = JsonModel.data['brand'].astext
        assert expr is not None

    def test_asbool_property(self):
        """Test .asbool property for booleans."""
        expr = JsonModel.data['active'].asbool
        assert expr is not None

    def test_cast_method(self):
        """Test .cast() method."""
        expr = JsonModel.data['price'].cast(Numeric)
        assert expr is not None


class TestComplexQueries:
    """Test complex query SQL generation."""

    def test_multiple_json_functions(self):
        """Test multiple JSON functions in single query."""
        stmt = select(
            JsonModel.name,
            json_extract(JsonModel.data, '$.field1').label('f1'),
            json_extract_float64(JsonModel.data, '$.field2').label('f2'),
            json_extract_string(JsonModel.data, '$.field3').label('f3'),
        )
        sql = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "json_extract(test_json_all.data, '$.field1')" in sql
        assert "json_extract_float64(test_json_all.data, '$.field2')" in sql
        assert "json_extract_string(test_json_all.data, '$.field3')" in sql


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
