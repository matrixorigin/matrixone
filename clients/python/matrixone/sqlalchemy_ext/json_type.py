"""
Custom JSON type for MatrixOne with SQLAlchemy standard syntax support.

This module provides enhanced JSON type that supports:
- column['key'] object access and column[index] array access
- .astext property for automatic quote removal
- Better Python type mapping
"""

import json
from typing import Union

from sqlalchemy import cast, String
from sqlalchemy.types import TypeDecorator
from sqlalchemy.dialects.mysql import JSON as MySQLJSON
from sqlalchemy.sql.expression import ColumnElement
from sqlalchemy.sql.visitors import InternalTraversal

from .json_functions import json_extract, json_extract_string


def _path_component(key: Union[str, int]) -> str:
    if isinstance(key, int) and not isinstance(key, bool):
        if key < 0:
            raise ValueError('JSON array indices must be non-negative')
        return f'[{key}]'
    return '.' + json.dumps(str(key), ensure_ascii=False)


class JSONPathElement(ColumnElement):
    """
    Represents a JSON path element accessed via column['key'].

    This class provides SQLAlchemy-compatible JSON path access with
    MatrixOne-specific implementations.
    """

    __visit_name__ = 'json_path_element'
    # Cloning and alias adaptation must visit the source expression. The full
    # member path also distinguishes compiled statements for sibling keys.
    _traverse_internals = [
        ('column', InternalTraversal.dp_clauseelement),
        ('_current_path', InternalTraversal.dp_string),
    ]
    # SQLAlchemy uses expression identity for anonymous labels, even when
    # equality builds a SQL predicate.
    __hash__ = ColumnElement.__hash__

    def __init__(self, column: ColumnElement, key: Union[str, int]):
        self.column = column
        self.key = key
        self._current_path = '$' + _path_component(key)
        # Call parent init
        super().__init__()

    def __getitem__(self, key: Union[str, int]) -> 'JSONPathElement':
        """
        Support nested access: column['key1']['key2']

        Example:
            Product.specs['hardware']['processor']
            # Generates path: $."hardware"."processor"
        """
        new_element = JSONPathElement.__new__(JSONPathElement)
        new_element.column = self.column
        new_element.key = key
        new_element._current_path = self._current_path + _path_component(key)
        return new_element

    @property
    def astext(self) -> ColumnElement:
        """
        Convert JSON value to text, removing quotes from string values.

        This is equivalent to PostgreSQL's ->> operator or MySQL's JSON_UNQUOTE.

        Example:
            # Returns: Dell (without quotes)
            Product.specs['brand'].astext

            # SQL: json_extract_string(specs, '$."brand"')
        """
        return json_extract_string(self.column, self._current_path)

    @property
    def asbool(self) -> ColumnElement:
        """
        Convert JSON boolean value to SQL boolean.

        True only for a JSON boolean true, including nullable paths. A JSON
        string containing "true" is a different value.

        Example:
            # Returns: True/False as boolean comparison
            Product.specs['active'].asbool

            # JSON string and boolean domains are checked separately.
        """
        from sqlalchemy import and_, func

        # A JSON string "true" is not a JSON boolean. Test its string domain
        # separately rather than relying on JSON/string comparison coercion.
        string = json_extract_string(self.column, self._current_path)
        serialized = cast(json_extract(self.column, self._current_path), String)
        return and_(string.is_(None), func.nullif(serialized, 'null') == 'true')

    def cast(self, type_):
        """
        Cast extracted JSON value to specific SQL type.

        Extract exact scalar text before applying the requested SQL type.
        Numeric values never pass through float64, preserving large integers
        and decimal digits. JSON null and missing paths become SQL NULL.

        Example:
            Product.specs['price'].cast(Numeric(30, 6))
            Product.specs['count'].cast(Integer)
            Product.specs['name'].cast(String(20))
        """
        return cast(self._scalar_text(), type_)

    def _scalar_text(self):
        # JSON-to-numeric casts are not generally supported by MatrixOne.
        # Serialize first, preserving exact integer/decimal digits; JSON null
        # becomes SQL NULL while the JSON string "null" retains its quotes.
        from sqlalchemy import func

        string = json_extract_string(self.column, self._current_path)
        serialized = cast(json_extract(self.column, self._current_path), String)
        return func.coalesce(string, func.nullif(serialized, 'null'))

    def __eq__(self, other):
        """Support equality comparison with smart quote handling."""
        # Extract value and compare
        extracted = json_extract(self.column, self._current_path)

        # Smart quote handling
        if isinstance(other, str):
            # Don't add quotes for boolean strings or if already quoted
            if other in ('true', 'false', 'null') or other.startswith('"'):
                other_value = other
            else:
                # Add quotes for regular strings
                other_value = json.dumps(other, ensure_ascii=False)
        elif isinstance(other, bool):
            # Convert Python bool to JSON boolean string
            other_value = 'true' if other else 'false'
        else:
            # Numbers and other types: use as-is
            other_value = other

        return extracted == other_value

    def __ne__(self, other):
        """Support inequality comparison with smart quote handling."""
        extracted = json_extract(self.column, self._current_path)

        # Smart quote handling (same as __eq__)
        if isinstance(other, str):
            if other in ('true', 'false', 'null') or other.startswith('"'):
                other_value = other
            else:
                other_value = json.dumps(other, ensure_ascii=False)
        elif isinstance(other, bool):
            other_value = 'true' if other else 'false'
        else:
            other_value = other

        return extracted != other_value

    def __gt__(self, other):
        """Support greater than comparison."""
        extracted = json_extract(self.column, self._current_path)
        return extracted > other

    def __lt__(self, other):
        """Support less than comparison."""
        extracted = json_extract(self.column, self._current_path)
        return extracted < other

    def __ge__(self, other):
        """Support greater than or equal comparison."""
        extracted = json_extract(self.column, self._current_path)
        return extracted >= other

    def __le__(self, other):
        """Support less than or equal comparison."""
        extracted = json_extract(self.column, self._current_path)
        return extracted <= other

    @property
    def _from_objects(self):
        """Required for SQLAlchemy to understand this as a column element."""
        return self.column._from_objects

    def _compiler_dispatch(self, visitor, **kwargs):
        """
        Custom compilation - delegate to json_extract.

        This ensures that column['key'] compiles to json_extract(column, '$."key"')
        """
        # Create json_extract function call
        extract_func = json_extract(self.column, self._current_path)
        # Delegate to json_extract's compiler
        return extract_func._compiler_dispatch(visitor, **kwargs)


class JSON(TypeDecorator):
    """
    Custom JSON type for MatrixOne with enhanced accessor support.

    This type provides SQLAlchemy-standard JSON access patterns:
    - column['key'] - Dictionary-style access
    - column['key'].astext - Text extraction without quotes
    - column['key'].cast(type) - Type conversion

    Usage:
        from matrixone.sqlalchemy_ext import JSON
        from sqlalchemy import Column, Integer, String

        class Product(Base):
            __tablename__ = 'products'
            id = Column(Integer, primary_key=True)
            name = Column(String(200))
            specifications = Column(JSON)

        # SQLAlchemy standard syntax
        query = select(
            Product.specifications['brand'].astext,
            Product.specifications['price'].cast(Numeric)
        ).where(
            Product.specifications['category'].astext == 'Electronics'
        )
    """

    impl = MySQLJSON
    cache_ok = True

    class Comparator(MySQLJSON.Comparator):
        """Custom comparator for JSON column operations."""

        def __getitem__(self, key: Union[str, int]) -> JSONPathElement:
            """
            Enable object or array access: column['key'] or column[index]

            Args:
                key: JSON object key or non-negative integer array index

            Returns:
                JSONPathElement that can be further accessed or compared
            """
            return JSONPathElement(self.expr, key)

        @property
        def astext(self) -> ColumnElement:
            """
            Convert entire JSON column to text.

            Note: For extracting specific keys, use column['key'].astext
            """
            # For the entire column, just cast to String
            return cast(self.expr, String)

    comparator_factory = Comparator


# Convenience alias
MATRIXONE_JSON = JSON  # Legacy alias


__all__ = [
    'JSON',
    'JSONPathElement',
    'MATRIXONE_JSON',
]
