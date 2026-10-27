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
from sqlalchemy import event
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.dialects.mysql.base import MySQLTypeCompiler, MySQLDialect, ischema_names as mysql_ischema_names

from .vector_type import VectorType, VectorPrecision


class _Vecf32Type(VectorType):
    """VectorType with f32 precision for ischema_names registration."""

    def __init__(self, dimension=None):
        super().__init__(dimension=dimension, precision=VectorPrecision.F32)


class _Vecf64Type(VectorType):
    """VectorType with f64 precision for ischema_names registration."""

    def __init__(self, dimension=None):
        super().__init__(dimension=dimension, precision=VectorPrecision.F64)


# Extend MySQL's ischema_names with MatrixOne-specific types
_mo_ischema_names = mysql_ischema_names.copy()
_mo_ischema_names["vecf32"] = _Vecf32Type
_mo_ischema_names["vecf64"] = _Vecf64Type
_mo_ischema_names["bool"] = mysql_ischema_names["boolean"]
# Older servers emit uppercase types when describing views. Keep MySQL's
# global registry untouched while accepting both server spellings.
_mo_ischema_names.update({name.upper(): value for name, value in tuple(_mo_ischema_names.items())})


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
    driver = "pymysql"
    # Standalone compilation must preserve Decimal as PyMySQL/aiomysql do.
    supports_native_decimal = True

    ischema_names = _mo_ischema_names

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Initialize missing MySQL dialect attributes
        self._connection_charset = "utf8mb4"
        # Set MySQL dialect compatibility flags
        # This flag is used by MySQL dialect to handle foreign key reflection quirks
        # For MatrixOne, we set it to False as we don't have those specific MySQL bugs
        self._needs_correct_for_88718_96365 = False

    @classmethod
    def engine_created(cls, engine):
        _install_compiler_contract(engine)

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
        # MatrixOne doesn't use schemas, use current database if schema is None
        if schema is None:
            try:
                result = connection.execute(sqlalchemy.text("SELECT DATABASE()"))
                schema = result.scalar()
            except Exception:
                raise ValueError("Failed to get current database. Use 'USE database_name' or specify schema parameter.")
            if not schema:
                raise ValueError("No database selected. Use 'USE database_name' or specify schema parameter.")

        return super().get_table_names(connection, schema, **kw)

    def has_table(self, connection, table_name, schema=None, **kw):
        """Check if table exists in MatrixOne database."""
        # MatrixOne doesn't use schemas, but MySQL dialect requires schema to be not None
        # Use current database name as schema to satisfy MySQL dialect requirements
        if schema is None:
            try:
                result = connection.execute(sqlalchemy.text("SELECT DATABASE()"))
                schema = result.scalar()
            except Exception:
                raise ValueError("Failed to get current database. Use 'USE database_name' or specify schema parameter.")
            if not schema:
                raise ValueError("No database selected. Use 'USE database_name' or specify schema parameter.")

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
        # Don't convert VectorType - let it pass through to the type compiler
        if hasattr(type_, "__visit_name__") and type_.__visit_name__ == "VECTOR":
            return type_  # Return the VectorType itself, don't convert to VARCHAR
        return super().type_descriptor(type_)


class MatrixOneCompiler(MySQLDialect.statement_compiler):
    """MatrixOne SQL compiler for handling vector types."""

    def visit_select(self, select_stmt, asfrom=False, **kwargs):
        snapshot = select_stmt._annotations.get('matrixone_snapshot')
        if snapshot is not None and (asfrom or self.stack or kwargs.get('visiting_cte') is not None):
            raise ValueError('Snapshot annotations on nested SELECTs or CTEs are unsupported')
        sql = super().visit_select(select_stmt, asfrom=asfrom, **kwargs)
        if snapshot is not None:
            from .._snapshot_sql import rewrite_snapshot

            # Defer the grammar value until positional/postcompile bind scanning
            # finishes, so bind-looking snapshot names remain data.
            occupied = set(self.binds).union(self.bind_names.values(), self.escaped_bind_names.values())
            index = 0
            name = '_matrixone_snapshot_0_value'
            while name in occupied:
                index += 1
                name = '_matrixone_snapshot_' + str(index) + '_value'
            # The fixed suffix cannot collide with expanding binds' numeric suffixes.
            self._matrixone_snapshot_binding = (name, snapshot)
            token = self.process(
                sqlalchemy.bindparam(name, snapshot, type_=sqlalchemy.String(), literal_execute=True),
                literal_binds=False,
                render_postcompile=bool(kwargs.get('render_postcompile') or kwargs.get('literal_binds')),
            )
            sql = rewrite_snapshot(sql, snapshot, snapshot_token=token)
        return sql

    def construct_params(self, *args, **kwargs):
        parameters = super().construct_params(*args, **kwargs)
        binding = getattr(self, '_matrixone_snapshot_binding', None)
        if binding is not None and binding[0] in parameters:
            # Execution parameters cannot replace immutable snapshot annotations.
            # Never re-add a value already consumed by eager postcompilation.
            parameters[binding[0]] = binding[1]
        return parameters

    def visit_user_defined_type(self, type_, **kw):
        """Handle user-defined vector types."""
        if hasattr(type_, "get_col_spec"):
            return type_.get_col_spec()
        return super().visit_user_defined_type(type_, **kw)


class MatrixOneTypeCompiler(MySQLTypeCompiler):
    """Inherit scalar fidelity; specialize only MatrixOne vector types."""

    def visit_VECTOR(self, type_, **kw):
        return type_.get_col_spec() if hasattr(type_, 'get_col_spec') else 'VECTOR'


# Register the dialect
MatrixOneDialect.statement_compiler = MatrixOneCompiler
MatrixOneDialect.type_compiler = MatrixOneTypeCompiler
MatrixOneDialect.type_compiler_cls = MatrixOneTypeCompiler


# MatrixOneVectorType removed - use VectorType from vector_type.py instead


def adapt_engine_dialect(engine):
    """Add MatrixOne traits without replacing the driver's initialization owner.

    SQLAlchemy's first-connect closure retains this object, even for a lazy
    engine. Keep its DBAPI, ping, server state and driver-specific overrides.
    """
    _install_compiler_contract(engine)
    dialect = engine.dialect
    if isinstance(dialect, MatrixOneDialect):
        return
    driver_type = type(dialect)
    dialect.__class__ = type(
        'MatrixOne_' + driver_type.__name__,
        (driver_type, MatrixOneDialect),
        {'supports_statement_cache': True, 'statement_compiler': MatrixOneCompiler},
    )
    dialect.ischema_names = dict(dialect.ischema_names, **_mo_ischema_names)
    compiler = MatrixOneTypeCompiler(dialect)
    dialect.type_compiler = compiler
    dialect.type_compiler_instance = compiler
    engine.clear_compiled_cache()


_COMPILER_CONTRACT = 'matrixone-snapshot-v1'
_PREPARED_ATTRIBUTE = '_matrixone_compiler_prepared'


def _prepare_compiler_statement(connection, statement, multiparams, params, execution_options):
    """Partition external caches without changing the caller's cache or dialect owner."""
    if not isinstance(statement, ClauseElement):
        return statement, multiparams, params
    if statement._annotations.get('matrixone_compiler_contract') == _COMPILER_CONTRACT:
        return statement, multiparams, params
    memo = getattr(statement, _PREPARED_ATTRIBUTE, None)
    # Generative SQLAlchemy copies may inherit __dict__; never reuse a parent's SQL.
    if memo is not None and memo[0] is statement and memo[1] == _COMPILER_CONTRACT:
        prepared = memo[2]
    else:
        prepared = statement._annotate({'matrixone_compiler_contract': _COMPILER_CONTRACT})
        prepared.__dict__.pop(_PREPARED_ATTRIBUTE, None)
        setattr(statement, _PREPARED_ATTRIBUTE, (statement, _COMPILER_CONTRACT, prepared))
    return prepared, multiparams, params


def _install_compiler_contract(engine):
    # OptionEngine inherits its base engine's listeners. contains() avoids duplicate
    # registration on an exact engine; duplicate inherited calls are harmless.
    if not event.contains(engine, 'before_execute', _prepare_compiler_statement):
        event.listen(engine, 'before_execute', _prepare_compiler_statement, retval=True)
