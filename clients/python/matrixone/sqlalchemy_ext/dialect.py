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
MatrixOne SQLAlchemy dialect support.
"""

import sqlalchemy
from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.dialects.mysql.base import MySQLDialect


class MatrixOneDialect(MySQLDialect):
    """
    MatrixOne dialect for SQLAlchemy.

    This dialect extends MySQL dialect to support MatrixOne-specific features
    including vector types (vecf32/vecf64), fulltext search, and other
    MatrixOne-specific SQL constructs.

    Key Features:

    - Full MySQL compatibility for standard operations
    - Support for MatrixOne vector types (vecf32, vecf64)
    - Fulltext search syntax support
    - MatrixOne-specific error handling
    - Connection charset configuration
    - Statement caching support

    Supported MatrixOne Features:
    - Vector data types and operations
    - Fulltext indexing and search
    - Snapshot and PITR operations
    - Account and user management
    - Vector similarity search functions

    Usage:

        # Create engine with MatrixOne dialect
        engine = create_engine(
            'matrixone://user:password@host:port/database',
            dialect=MatrixOneDialect()
        )

        # Or use the dialect name in connection string
        engine = create_engine('matrixone+mysql://user:password@host:port/database')

    Note: This dialect is automatically used when connecting to MatrixOne
    databases through the MatrixOne Python client.
    """

    name = "matrixone"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Initialize missing MySQL dialect attributes
        self._connection_charset = "utf8mb4"

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

    def has_table(self, connection, table_name, schema=None, **kw):
        """Check if table exists in MatrixOne database."""
        # MatrixOne doesn't use schemas, but MySQL dialect requires schema to be not None
        # Use current database name as schema to satisfy MySQL dialect requirements
        if schema is None:
            # Get current database name from connection URL or connection info
            try:
                # Try to get database name from connection URL
                if hasattr(connection, 'connection') and hasattr(connection.connection, 'get_dsn_parameters'):
                    dsn_params = connection.connection.get_dsn_parameters()
                    schema = dsn_params.get('dbname') or dsn_params.get('database')

                # Fallback: try to get from connection string
                if not schema and hasattr(connection, 'connection') and hasattr(connection.connection, 'dsn'):
                    dsn = connection.connection.dsn
                    if 'dbname=' in dsn:
                        schema = dsn.split('dbname=')[1].split()[0]
                    elif 'database=' in dsn:
                        schema = dsn.split('database=')[1].split()[0]

                # Final fallback: use default database name
                if not schema:
                    schema = "test"

            except Exception:
                # Ultimate fallback
                schema = "test"

        return super().has_table(connection, table_name, schema, **kw)

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


# SQLAlchemy version compatibility
SA_VERSION = tuple(map(int, sqlalchemy.__version__.split(".")[:2]))

if SA_VERSION >= (2, 0):
    # SQLAlchemy 2.0+
    from sqlalchemy.sql.compiler import TypeCompiler

    class MatrixOneTypeCompiler(TypeCompiler):
        """MatrixOne type compiler for handling vector types."""

        def visit_VECTOR(self, type_, **kw):
            """Handle VECTOR type compilation."""
            if hasattr(type_, "get_col_spec"):
                return type_.get_col_spec()
            return "VECTOR"

        # Delegate all other types to MySQL type compiler
        def visit_integer(self, type_, **kw):
            """Handle INTEGER type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_integer(type_, **kw)

        def visit_string(self, type_, **kw):
            """Handle STRING type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_string(type_, **kw)

        def visit_text(self, type_, **kw):
            """Handle TEXT type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_text(type_, **kw)

        def visit_boolean(self, type_, **kw):
            """Handle BOOLEAN type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_boolean(type_, **kw)

        def visit_float(self, type_, **kw):
            """Handle FLOAT type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_float(type_, **kw)

        def visit_numeric(self, type_, **kw):
            """Handle NUMERIC type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_numeric(type_, **kw)

        def visit_DECIMAL(self, type_, **kw):
            """Handle DECIMAL type compilation for SQLAlchemy 2.0.x."""
            if (
                hasattr(type_, 'precision')
                and hasattr(type_, 'scale')
                and type_.precision is not None
                and type_.scale is not None
            ):
                return f"DECIMAL({type_.precision}, {type_.scale})"
            return "DECIMAL"

        def visit_NUMERIC(self, type_, **kw):
            """Handle NUMERIC type compilation for SQLAlchemy 2.0.x."""
            if (
                hasattr(type_, 'precision')
                and hasattr(type_, 'scale')
                and type_.precision is not None
                and type_.scale is not None
            ):
                return f"DECIMAL({type_.precision}, {type_.scale})"
            return "DECIMAL"

        def visit_BIGINT(self, type_, **kw):
            """Handle BIGINT type compilation for SQLAlchemy 2.0.x."""
            return "BIGINT"

        def visit_big_integer(self, type_, **kw):
            """Handle BigInteger type compilation for SQLAlchemy 2.0.x."""
            return "BIGINT"

        def visit_TIMESTAMP(self, type_, **kw):
            """Handle TIMESTAMP type compilation for SQLAlchemy 2.0.x."""
            return "TIMESTAMP"

        def visit_date(self, type_, **kw):
            """Handle DATE type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_date(type_, **kw)

        def visit_datetime(self, type_, **kw):
            """Handle DATETIME type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_datetime(type_, **kw)

        def visit_time(self, type_, **kw):
            """Handle TIME type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_time(type_, **kw)

        def visit_timestamp(self, type_, **kw):
            """Handle TIMESTAMP type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_TIMESTAMP(type_, **kw)

        def visit_type_decorator(self, type_, **kw):
            """Handle TypeDecorator type compilation."""
            # For MatrixOne vector types, handle them directly
            if hasattr(type_, 'type_engine'):
                actual_type = type_.type_engine(self.dialect)
                if hasattr(actual_type, '__visit_name__') and actual_type.__visit_name__ == 'VECTOR':
                    return self.visit_VECTOR(actual_type, **kw)

            # For other types, delegate to MySQL compiler
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_type_decorator(type_, **kw)

        # Additional type methods for comprehensive SQLAlchemy 2.0.x support
        def visit_small_integer(self, type_, **kw):
            """Handle SMALLINT type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_small_integer(type_, **kw)

        def visit_CHAR(self, type_, **kw):
            """Handle CHAR type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_CHAR(type_, **kw)

        def visit_VARCHAR(self, type_, **kw):
            """Handle VARCHAR type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_VARCHAR(type_, **kw)

        def visit_unicode(self, type_, **kw):
            """Handle Unicode type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_unicode(type_, **kw)

        def visit_unicode_text(self, type_, **kw):
            """Handle UnicodeText type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_unicode_text(type_, **kw)

        def visit_large_binary(self, type_, **kw):
            """Handle LargeBinary type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_large_binary(type_, **kw)

        def visit_binary(self, type_, **kw):
            """Handle Binary type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_BINARY(type_, **kw)

        def visit_BLOB(self, type_, **kw):
            """Handle BLOB type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_BLOB(type_, **kw)

        def visit_enum(self, type_, **kw):
            """Handle Enum type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_enum(type_, **kw)

        def visit_pickle(self, type_, **kw):
            """Handle PickleType compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_pickle(type_, **kw)

        def visit_interval(self, type_, **kw):
            """Handle Interval type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_interval(type_, **kw)

        def visit_uuid(self, type_, **kw):
            """Handle UUID type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_uuid(type_, **kw)

        def visit_JSON(self, type_, **kw):
            """Handle JSON type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_JSON(type_, **kw)

        def visit_arbitrary_type(self, type_, **kw):
            """Handle arbitrary type compilation."""
            from sqlalchemy.dialects.mysql.base import MySQLDialect

            mysql_compiler = MySQLDialect.type_compiler_cls(self.dialect)
            return mysql_compiler.visit_arbitrary_type(type_, **kw)

else:
    # SQLAlchemy 1.4.x
    class MatrixOneTypeCompiler(MySQLDialect.type_compiler):
        """MatrixOne type compiler for handling vector types."""

        def visit_VECTOR(self, type_, **kw):
            """Handle VECTOR type compilation."""
            if hasattr(type_, "get_col_spec"):
                return type_.get_col_spec()
            return "VECTOR"

        def visit_timestamp(self, type_, **kw):
            """Handle TIMESTAMP type compilation for SQLAlchemy 1.4.x."""
            # In SQLAlchemy 1.4.x, we need to handle TIMESTAMP differently
            return "TIMESTAMP"

        def visit_DECIMAL(self, type_, **kw):
            """Handle DECIMAL type compilation for SQLAlchemy 2.0.x."""
            if (
                hasattr(type_, 'precision')
                and hasattr(type_, 'scale')
                and type_.precision is not None
                and type_.scale is not None
            ):
                return f"DECIMAL({type_.precision}, {type_.scale})"
            return "DECIMAL"

        def visit_NUMERIC(self, type_, **kw):
            """Handle NUMERIC type compilation for SQLAlchemy 2.0.x."""
            if (
                hasattr(type_, 'precision')
                and hasattr(type_, 'scale')
                and type_.precision is not None
                and type_.scale is not None
            ):
                return f"DECIMAL({type_.precision}, {type_.scale})"
            return "DECIMAL"

        def visit_BIGINT(self, type_, **kw):
            """Handle BIGINT type compilation for SQLAlchemy 1.4.x."""
            return "BIGINT"

        def visit_big_integer(self, type_, **kw):
            """Handle BigInteger type compilation for SQLAlchemy 1.4.x."""
            return "BIGINT"

        def visit_TIMESTAMP(self, type_, **kw):
            """Handle TIMESTAMP type compilation for SQLAlchemy 1.4.x."""
            return "TIMESTAMP"

        # Additional type methods for comprehensive SQLAlchemy 1.4.x support
        def visit_small_integer(self, type_, **kw):
            """Handle SMALLINT type compilation."""
            return "SMALLINT"

        def visit_CHAR(self, type_, **kw):
            """Handle CHAR type compilation."""
            if hasattr(type_, 'length') and type_.length:
                return f"CHAR({type_.length})"
            return "CHAR"

        def visit_VARCHAR(self, type_, **kw):
            """Handle VARCHAR type compilation."""
            if hasattr(type_, 'length') and type_.length:
                return f"VARCHAR({type_.length})"
            return "VARCHAR"

        def visit_unicode(self, type_, **kw):
            """Handle Unicode type compilation."""
            if hasattr(type_, 'length') and type_.length:
                return f"VARCHAR({type_.length})"
            return "VARCHAR"

        def visit_unicode_text(self, type_, **kw):
            """Handle UnicodeText type compilation."""
            return "TEXT"

        def visit_large_binary(self, type_, **kw):
            """Handle LargeBinary type compilation."""
            return "BLOB"

        def visit_binary(self, type_, **kw):
            """Handle Binary type compilation."""
            if hasattr(type_, 'length') and type_.length:
                return f"BINARY({type_.length})"
            return "BINARY"

        def visit_BLOB(self, type_, **kw):
            """Handle BLOB type compilation."""
            return "BLOB"

        def visit_enum(self, type_, **kw):
            """Handle Enum type compilation."""
            if hasattr(type_, 'enums') and type_.enums:
                enum_values = "','".join(type_.enums)
                return f"ENUM('{enum_values}')"
            return "ENUM"

        def visit_pickle(self, type_, **kw):
            """Handle PickleType compilation."""
            return "BLOB"

        def visit_interval(self, type_, **kw):
            """Handle Interval type compilation."""
            return "DATETIME"

        def visit_uuid(self, type_, **kw):
            """Handle UUID type compilation."""
            return "CHAR(32)"

        def visit_JSON(self, type_, **kw):
            """Handle JSON type compilation."""
            return "JSON"

        def visit_arbitrary_type(self, type_, **kw):
            """Handle arbitrary type compilation."""
            return "VARCHAR"


# Register the dialect
MatrixOneDialect.statement_compiler = MatrixOneCompiler
MatrixOneDialect.type_compiler = MatrixOneTypeCompiler


# MatrixOneVectorType removed - use VectorType from vector_type.py instead
