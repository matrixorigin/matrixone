"""
Custom JSON type for MatrixOne with SQLAlchemy standard syntax support.

This module provides enhanced JSON type that supports:
- column['key'] dictionary-style access
- .astext property for automatic quote removal
- Better Python type mapping
"""

from sqlalchemy import cast, String, Integer, Float, Numeric
from sqlalchemy.types import TypeDecorator
from sqlalchemy.dialects.mysql import JSON as MySQLJSON
from sqlalchemy.sql.expression import ColumnElement

from .json_functions import json_extract, json_extract_string, json_extract_float64


class JSONPathElement(ColumnElement):
    """
    Represents a JSON path element accessed via column['key'].

    This class provides SQLAlchemy-compatible JSON path access with
    MatrixOne-specific implementations.
    """

    __visit_name__ = 'json_path_element'

    def __init__(self, column: ColumnElement, key: str):
        self.column = column
        self.key = key
        self._current_path = f'$.{key}'
        # Call parent init
        super().__init__()

    def __getitem__(self, key: str) -> 'JSONPathElement':
        """
        Support nested access: column['key1']['key2']

        Example:
            Product.specs['hardware']['processor']
            # Generates path: $.hardware.processor
        """
        new_element = JSONPathElement.__new__(JSONPathElement)
        new_element.column = self.column
        new_element.key = key
        new_element._current_path = f'{self._current_path}.{key}'
        return new_element

    @property
    def astext(self) -> ColumnElement:
        """
        Convert JSON value to text, removing quotes from string values.

        This is equivalent to PostgreSQL's ->> operator or MySQL's JSON_UNQUOTE.

        Example:
            # Returns: Dell (without quotes)
            Product.specs['brand'].astext

            # SQL: json_extract_string(specs, '$.brand')
        """
        return json_extract_string(self.column, self._current_path)

    @property
    def asbool(self) -> ColumnElement:
        """
        Convert JSON boolean value to SQL boolean.

        Handles both JSON true/false representations.

        Example:
            # Returns: True/False as boolean comparison
            Product.specs['active'].asbool

            # SQL: json_extract(specs, '$.active') = 'true'
        """
        # Simply check if value equals 'true'
        extracted = json_extract(self.column, self._current_path)
        return extracted == 'true'

    def cast(self, type_):
        """
        Cast extracted JSON value to specific SQL type.

        MatrixOne Note: Direct CAST on JSON values fails, so we use:
        - json_extract_float64 for numeric types
        - json_extract_string for string types
        - Regular cast for other types (may fail)

        Example:
            Product.specs['price'].cast(Numeric)   # Uses json_extract_float64
            Product.specs['count'].cast(Integer)   # Uses json_extract_float64
            Product.specs['name'].cast(String)     # Uses json_extract_string
        """
        # Check if type is numeric
        if isinstance(type_, type) and issubclass(type_, (Integer, Float, Numeric)):
            # Use json_extract_float64 for numeric types
            return json_extract_float64(self.column, self._current_path)
        elif type_.__class__.__name__ in ('Integer', 'Float', 'Numeric', 'DECIMAL'):
            # Handle type instances
            return json_extract_float64(self.column, self._current_path)
        elif isinstance(type_, type) and issubclass(type_, String):
            # Use json_extract_string for string types
            return json_extract_string(self.column, self._current_path)
        elif type_.__class__.__name__ in ('String', 'VARCHAR', 'TEXT'):
            # Handle type instances
            return json_extract_string(self.column, self._current_path)
        else:
            # For other types, try regular cast (may fail in MatrixOne)
            extracted = json_extract(self.column, self._current_path)
            return cast(extracted, type_)

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
                other_value = f'"{other}"'
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
                other_value = f'"{other}"'
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
        return [self.column]

    @property
    def _is_subquery(self):
        """Required attribute for SQLAlchemy 2.0."""
        return False

    def _compiler_dispatch(self, visitor, **kwargs):
        """
        Custom compilation - delegate to json_extract.

        This ensures that column['key'] compiles to json_extract(column, '$.key')
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

        def __getitem__(self, key: str) -> JSONPathElement:
            """
            Enable dictionary-style access: column['key']

            Args:
                key: JSON object key

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
