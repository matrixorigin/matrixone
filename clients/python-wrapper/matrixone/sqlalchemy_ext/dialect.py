"""
MatrixOne SQLAlchemy dialect support.
"""

from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.dialects.mysql.base import MySQLDialect


class MatrixOneDialect(MySQLDialect):
    """
    MatrixOne dialect for SQLAlchemy.

    Extends MySQL dialect to support MatrixOne-specific features like vecf32/vecf64 types.
    """

    name = "matrixone"

    def _extract_error_code(self, exception):
        """Extract error code from MatrixOne exceptions."""
        if hasattr(exception, "args") and len(exception.args) > 0:
            if isinstance(exception.args[0], int):
                return exception.args[0]
        return None

    # MatrixOne supports MySQL-compatible syntax
    supports_statement_cache = True

    def get_table_names(self, connection, schema=None, **kw):
        """Get table names from MatrixOne database."""
        return super().get_table_names(connection, schema, **kw)

    def get_columns(self, connection, table_name, schema=None, **kw):
        """Get column information including vector types."""
        columns = super().get_columns(connection, table_name, schema, **kw)

        # Process vector columns (case-insensitive)
        for column in columns:
            if isinstance(column["type"], str):
                type_str_lower = column["type"].lower()
                if type_str_lower.startswith("vecf32"):
                    column["type"] = self._create_vector_type("f32", column["type"])
                elif type_str_lower.startswith("vecf64"):
                    column["type"] = self._create_vector_type("f64", column["type"])

        return columns

    def _create_vector_type(self, precision: str, type_str: str):
        """Create appropriate vector type from string."""
        # Extract dimension from vecf32(128), VECF32(128), vecf64(256), etc.
        # Handle case-insensitive matching
        import re

        from .vector_type import VectorType

        # Clean the input string and make it lowercase for pattern matching
        clean_type_str = type_str.strip().lower()

        # Build pattern dynamically to avoid f-string issues
        if precision == "f32":
            pattern = r"vecf32\((\d+)\)"
        elif precision == "f64":
            pattern = r"vecf64\((\d+)\)"
        else:
            pattern = rf"vecf{precision}\((\d+)\)"

        match = re.search(pattern, clean_type_str)

        if match:
            try:
                dimension = int(match.group(1))
                return VectorType(dimension=dimension, precision=precision)
            except ValueError:
                pass

        return VectorType(precision=precision)

    def type_descriptor(self, type_):
        """Handle MatrixOne specific types."""
        if hasattr(type_, "__visit_name__") and type_.__visit_name__ == "VECTOR":
            return VARCHAR(65535)  # Use VARCHAR for vector storage
        return super().type_descriptor(type_)


class MatrixOneCompiler(MySQLDialect.statement_compiler):
    """MatrixOne SQL compiler for handling vector types."""

    def visit_user_defined_type(self, type_, **kw):
        """Handle user-defined vector types."""
        if hasattr(type_, "get_col_spec"):
            return type_.get_col_spec()
        return super().visit_user_defined_type(type_, **kw)


class MatrixOneTypeCompiler(MySQLDialect.type_compiler):
    """MatrixOne type compiler for handling vector types."""

    def visit_VECTOR(self, type_, **kw):
        """Handle VECTOR type compilation."""
        if hasattr(type_, "get_col_spec"):
            return type_.get_col_spec()
        return "VECTOR"


# Register the dialect
MatrixOneDialect.statement_compiler = MatrixOneCompiler
MatrixOneDialect.type_compiler = MatrixOneTypeCompiler


# MatrixOneVectorType removed - use VectorType from vector_type.py instead
