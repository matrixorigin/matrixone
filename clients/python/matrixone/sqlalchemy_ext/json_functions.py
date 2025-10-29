"""
JSON functions for MatrixOne SQLAlchemy integration.

This module provides JSON manipulation functions compatible with MatrixOne's
JSON support. These functions handle path literals correctly to avoid parameter
binding issues.
"""

from sqlalchemy import literal_column
from sqlalchemy.sql.expression import ColumnElement
from sqlalchemy.sql import functions


class json_extract(functions.GenericFunction):
    """
    Extract values from JSON documents using JSON path expressions.

    This function wraps MatrixOne's JSON_EXTRACT function and ensures
    that path arguments are treated as literals rather than bound parameters.

    Args:
        json_column: The JSON column or expression
        *paths: One or more JSON path expressions (e.g., '$.name', '$.age')

    Returns:
        SQLAlchemy function expression

    Examples:
        >>> from matrixone.sqlalchemy_ext.json_functions import json_extract
        >>> from sqlalchemy import select
        >>>
        >>> # Extract single path
        >>> stmt = select(json_extract(Product.specs, '$.brand'))
        >>>
        >>> # Extract multiple paths
        >>> stmt = select(json_extract(Product.specs, '$.brand', '$.model'))
        >>>
        >>> # Use in WHERE clause
        >>> stmt = select(Product).where(
        ...     json_extract(Product.specs, '$.category') == 'Electronics'
        ... )
    """

    type = None
    name = 'json_extract'
    inherit_cache = True

    def __init__(self, json_column: ColumnElement, *paths: str, **kwargs):
        # Convert paths to literal columns to avoid parameter binding
        literal_paths = [literal_column(f"'{path}'") for path in paths]
        super().__init__(json_column, *literal_paths, **kwargs)


class json_extract_string(functions.GenericFunction):
    """
    Extract JSON value as string type.

    Args:
        json_column: The JSON column or expression
        path: JSON path expression

    Returns:
        SQLAlchemy function expression returning string

    Example:
        >>> stmt = select(json_extract_string(Product.specs, '$.name'))
    """

    type = None
    name = 'json_extract_string'
    inherit_cache = True

    def __init__(self, json_column: ColumnElement, path: str, **kwargs):
        literal_path = literal_column(f"'{path}'")
        super().__init__(json_column, literal_path, **kwargs)


class json_extract_float64(functions.GenericFunction):
    """
    Extract JSON numeric value as float.

    Args:
        json_column: The JSON column or expression
        path: JSON path expression

    Returns:
        SQLAlchemy function expression returning float

    Example:
        >>> stmt = select(json_extract_float64(Product.specs, '$.price'))
    """

    type = None
    name = 'json_extract_float64'
    inherit_cache = True

    def __init__(self, json_column: ColumnElement, path: str, **kwargs):
        literal_path = literal_column(f"'{path}'")
        super().__init__(json_column, literal_path, **kwargs)


class json_set(functions.GenericFunction):
    """
    Insert or update JSON values (upsert behavior).

    Args:
        json_column: The JSON column or expression
        *path_value_pairs: Alternating path and value arguments

    Returns:
        SQLAlchemy function expression

    Example:
        >>> from sqlalchemy import update
        >>>
        >>> # Update single key
        >>> stmt = update(Product).values(
        ...     specs=json_set(Product.specs, '$.ram', 32)
        ... )
        >>>
        >>> # Update multiple keys
        >>> stmt = update(Product).values(
        ...     specs=json_set(Product.specs, '$.ram', 32, '$.storage', '1TB')
        ... )
    """

    type = None
    name = 'json_set'
    inherit_cache = True

    def __init__(self, json_column: ColumnElement, *path_value_pairs, **kwargs):
        # Process path-value pairs
        # Paths should be literals, values can be bound parameters
        processed_args = []
        for i, arg in enumerate(path_value_pairs):
            if i % 2 == 0:  # Path (even index)
                processed_args.append(literal_column(f"'{arg}'"))
            else:  # Value (odd index)
                processed_args.append(arg)

        super().__init__(json_column, *processed_args, **kwargs)


class json_insert(functions.GenericFunction):
    """
    Insert JSON values only if path doesn't exist.

    Args:
        json_column: The JSON column or expression
        *path_value_pairs: Alternating path and value arguments

    Returns:
        SQLAlchemy function expression

    Example:
        >>> stmt = update(Product).values(
        ...     specs=json_insert(Product.specs, '$.warranty', '2 years')
        ... )
    """

    type = None
    name = 'json_insert'
    inherit_cache = True

    def __init__(self, json_column: ColumnElement, *path_value_pairs, **kwargs):
        processed_args = []
        for i, arg in enumerate(path_value_pairs):
            if i % 2 == 0:  # Path
                processed_args.append(literal_column(f"'{arg}'"))
            else:  # Value
                processed_args.append(arg)

        super().__init__(json_column, *processed_args, **kwargs)


class json_replace(functions.GenericFunction):
    """
    Update JSON values only if path exists.

    Args:
        json_column: The JSON column or expression
        *path_value_pairs: Alternating path and value arguments

    Returns:
        SQLAlchemy function expression

    Example:
        >>> stmt = update(Product).values(
        ...     specs=json_replace(Product.specs, '$.price', 999.99)
        ... )
    """

    type = None
    name = 'json_replace'
    inherit_cache = True

    def __init__(self, json_column: ColumnElement, *path_value_pairs, **kwargs):
        processed_args = []
        for i, arg in enumerate(path_value_pairs):
            if i % 2 == 0:  # Path
                processed_args.append(literal_column(f"'{arg}'"))
            else:  # Value
                processed_args.append(arg)

        super().__init__(json_column, *processed_args, **kwargs)


class json_quote(functions.GenericFunction):
    """
    Convert string to JSON string (add quotes and escape).

    Args:
        string_value: String to quote

    Returns:
        SQLAlchemy function expression

    Example:
        >>> stmt = select(json_quote('Hello "World"'))
    """

    type = None
    name = 'json_quote'
    inherit_cache = True


class json_unquote(functions.GenericFunction):
    """
    Remove JSON string quotes and unescape.

    Args:
        json_string: JSON string to unquote

    Returns:
        SQLAlchemy function expression

    Example:
        >>> stmt = select(json_unquote('"Hello"'))
    """

    type = None
    name = 'json_unquote'
    inherit_cache = True


class json_row(functions.GenericFunction):
    """
    Construct JSON object from key-value pairs.

    Args:
        *key_value_pairs: Alternating key and value arguments

    Returns:
        SQLAlchemy function expression

    Example:
        >>> stmt = select(json_row('name', 'John', 'age', 30))
    """

    type = None
    name = 'json_row'
    inherit_cache = True


# Convenience aliases (lowercase versions)
JSON_EXTRACT = json_extract
JSON_EXTRACT_STRING = json_extract_string
JSON_EXTRACT_FLOAT64 = json_extract_float64
JSON_SET = json_set
JSON_INSERT = json_insert
JSON_REPLACE = json_replace
JSON_QUOTE = json_quote
JSON_UNQUOTE = json_unquote
JSON_ROW = json_row


__all__ = [
    'json_extract',
    'json_extract_string',
    'json_extract_float64',
    'json_set',
    'json_insert',
    'json_replace',
    'json_quote',
    'json_unquote',
    'json_row',
    'JSON_EXTRACT',
    'JSON_EXTRACT_STRING',
    'JSON_EXTRACT_FLOAT64',
    'JSON_SET',
    'JSON_INSERT',
    'JSON_REPLACE',
    'JSON_QUOTE',
    'JSON_UNQUOTE',
    'JSON_ROW',
]
