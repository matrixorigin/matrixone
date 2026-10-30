# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""File relations and bounded Arrow interoperability for the synchronous SDK.

Input uses MatrixOne's native Arrow IPC LOAD, not row INSERTs. Output uses a
PyMySQL server-side cursor and converts each batch; it is not Arrow Flight or
zero-copy transport. PyArrow is an optional dependency, imported on first use.
"""

import glob
import io
import math
import os
import re
import sys
import tempfile
import uuid
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import NamedTuple
from urllib.parse import quote, unquote, urlsplit, urlunsplit

from ._telemetry import observe, phase, pool_wait
from .exceptions import MatrixOneError, annotate_execution_error
from ._sql_literals import sql_identifier


class AnalyticsCleanupError(MatrixOneError):
    """Cleanup failed after an acknowledged transaction. Do not retry the write."""

    def __init__(self, message, committed=False, rows=None):
        super().__init__(message, committed=committed, rows=rows, phase='cleanup')


class AnalyticsCommitUnknownError(MatrixOneError):
    """The connection failed during COMMIT; reconcile before retrying."""

    committed = None

    def __init__(self, message, rows=None):
        super().__init__(message, committed=None, rows=rows, phase='commit')


@contextmanager
def _write_transaction_impl(engine, rows=None):
    """Own a real autocommit-off transaction through its terminal outcome."""
    from ._utils import discard_unrestored_connection

    pool_wait()
    with engine.connect() as conn:
        with conn.begin():
            state = conn.exec_driver_sql('SELECT @@time_zone, @@mo_pk_check_by_dn, @@sql_mode, @@autocommit')
            try:
                old_tz, old_pk, old_mode, old_autocommit = state.one()
            finally:
                state.close()
        auto = str(old_autocommit).strip().lower()
        if auto not in ('0', '1', 'false', 'true', 'off', 'on'):
            raise ValueError('Cannot verify server autocommit mode')
        changed_autocommit = auto in ('1', 'true', 'on')
        committed = False
        execution_phase = 'setup'
        commit_started = False
        terminal = True
        tx = None
        try:
            tx = conn.begin()
            terminal = False
            if changed_autocommit:
                conn.exec_driver_sql('SET autocommit = 0').close()
                state = conn.exec_driver_sql('SELECT @@autocommit')
                try:
                    if str(state.scalar()).strip().lower() not in ('0', 'false', 'off'):
                        raise ValueError('Server did not establish an autocommit-off transaction')
                finally:
                    state.close()
            conn.exec_driver_sql("SET time_zone = '+00:00'").close()
            conn.exec_driver_sql('SET mo_pk_check_by_dn = 1').close()
            modes = [mode for mode in old_mode.split(',') if mode]
            if 'STRICT_ALL_TABLES' not in modes:
                modes.append('STRICT_ALL_TABLES')
            conn.exec_driver_sql('SET sql_mode = %s', (','.join(modes),)).close()
            execution_phase = 'execute'
            yield conn
            commit_started = True
            try:
                with phase('commit'):
                    tx.commit()
            except Exception as error:
                committed = None
                raise AnalyticsCommitUnknownError(
                    'COMMIT did not return acknowledgement; reconcile before retrying', rows=rows
                ) from error
            committed = True
            terminal = True
        except BaseException as primary:
            annotate_execution_error(
                primary,
                phase='commit' if commit_started else execution_phase,
                rows=rows,
                committed=None if commit_started else False,
            )
            if not commit_started and tx is not None and tx.is_active:
                try:
                    # An adopted AUTOCOMMIT engine may skip DBAPI rollback.
                    # Require a server acknowledgement before restoring mode.
                    if not conn.invalidated:
                        conn.exec_driver_sql('ROLLBACK').close()
                    tx.rollback()
                    terminal = True
                except BaseException as rollback_error:
                    primary.rollback_error = rollback_error
            if not terminal:
                try:
                    discard_unrestored_connection(conn)
                except BaseException as invalidation_error:
                    primary.invalidation_error = invalidation_error
            raise
        finally:
            primary = sys.exc_info()[1]
            if terminal and not conn.invalidated:
                try:
                    with conn.begin():
                        conn.exec_driver_sql('SET time_zone = %s', (old_tz,)).close()
                        conn.exec_driver_sql('SET mo_pk_check_by_dn = %s', (old_pk,)).close()
                        conn.exec_driver_sql('SET sql_mode = %s', (old_mode,)).close()
                        # SET autocommit=1 commits an active transaction. Only
                        # restore it after an acknowledged COMMIT or ROLLBACK.
                        if changed_autocommit:
                            conn.exec_driver_sql('SET autocommit = 1').close()
                except BaseException as cleanup_error:
                    try:
                        discard_unrestored_connection(conn)
                    except BaseException as invalidation_error:
                        cleanup_error.invalidation_error = invalidation_error
                    if primary is not None:
                        primary.cleanup_error = cleanup_error
                    else:
                        raise AnalyticsCleanupError(
                            'Session cleanup failed after transaction; committed=%s' % committed,
                            committed=committed,
                            rows=rows,
                        ) from cleanup_error


@contextmanager
def _write_transaction(engine, rows=None):
    try:
        with _write_transaction_impl(engine, rows) as connection:
            yield connection
    except BaseException as error:
        annotate_execution_error(error, phase='setup', rows=rows, committed=getattr(error, 'committed', False))
        raise


def _arrow():
    if sys.version_info < (3, 9):
        raise ImportError('Arrow analytics requires Python >=3.9; core SDK remains available on Python 3.8')
    try:
        import pyarrow as pa
    except ImportError as exc:
        raise ImportError("Install matrixone-python-sdk[analytics] to use Arrow analytics") from exc
    return pa


def _identifier(name):
    try:
        return sql_identifier(name, force=True)
    except (TypeError, ValueError):
        raise ValueError("SQL identifiers must contain 1..64 characters and no NUL") from None


def _table_name(name):
    parts = name.split(".")
    if len(parts) not in (1, 2):
        raise ValueError("Expected table or database.table")
    return ".".join(_identifier(p) for p in parts)


def _qualified_table(database, table):
    return _identifier(database) + "." + _identifier(table)


def _driver_identifier(name):
    # exec_driver_sql supplies an empty parameter tuple even for DDL. Literal
    # percent signs in identifiers must survive PyMySQL's printf interpolation.
    return _identifier(name).replace("%", "%%")


def _validate_schema(schema):
    names = [name.casefold() for name in schema.names]
    if not names or len(names) != len(set(names)):
        raise ValueError("Schema needs columns with unique, case-insensitive names")
    for field in schema:
        _identifier(field.name)
        if field.name.casefold().startswith("__mo_"):
            raise ValueError("Column names starting with __mo_ are reserved")
        _sql_type(field.type)


def _sql_type(dtype):
    pa = _arrow()
    t = pa.types
    if t.is_dictionary(dtype):
        return _sql_type(dtype.value_type)
    if t.is_boolean(dtype):
        return "BOOL"
    if t.is_integer(dtype):
        name = {8: "TINYINT", 16: "SMALLINT", 32: "INT", 64: "BIGINT"}[dtype.bit_width]
        return name + (" UNSIGNED" if t.is_unsigned_integer(dtype) else "")
    if t.is_float32(dtype):
        return "FLOAT"
    if t.is_float64(dtype):
        return "DOUBLE"
    if t.is_string(dtype) or t.is_large_string(dtype):
        return "TEXT"
    if t.is_binary(dtype) or t.is_large_binary(dtype) or t.is_fixed_size_binary(dtype):
        return "BLOB"
    if t.is_decimal128(dtype) and 0 <= dtype.scale <= dtype.precision:
        # The native Arrow bridge requires Decimal128 storage, i.e. precision > 18.
        return "DECIMAL(%d,%d)" % (max(19, dtype.precision), dtype.scale)
    if t.is_date(dtype):
        return "DATE"
    if t.is_timestamp(dtype):
        return "TIMESTAMP(6)" if dtype.tz else "DATETIME(6)"
    if t.is_time(dtype):
        return "TIME(6)"
    raise TypeError("Unsupported Arrow type %s; supply a supported explicit schema" % dtype)


def _ddl_columns(schema):
    return ", ".join(
        "%s %s%s" % (_driver_identifier(f.name), _sql_type(f.type), "" if f.nullable else " NOT NULL") for f in schema
    )


def _validate_scan_precision(schema):
    pa = _arrow()
    for field in schema:
        dtype = field.type.value_type if pa.types.is_dictionary(field.type) else field.type
        if (pa.types.is_timestamp(dtype) or pa.types.is_time(dtype)) and dtype.unit == "ns":
            raise TypeError(
                "Nanosecond file column %s cannot be scanned losslessly by MatrixOne; "
                "normalize the source to microseconds with a checked Arrow cast first" % field.name
            )


def _wire_schema(schema):
    pa = _arrow()
    fields = []
    for f in schema:
        dtype = f.type.value_type if pa.types.is_dictionary(f.type) else f.type
        if pa.types.is_decimal128(dtype):
            dtype = pa.decimal128(max(19, dtype.precision), dtype.scale)
        elif pa.types.is_timestamp(dtype):
            dtype = pa.timestamp("us", tz="UTC" if dtype.tz else None)
        elif pa.types.is_time(dtype):
            dtype = pa.time64("us")
        fields.append(pa.field(f.name, dtype, nullable=f.nullable))
    return pa.schema(fields)


def _merge_schemas(schemas):
    """Reject common-type promotions that can silently lose integer precision."""
    pa = _arrow()
    merged = pa.unify_schemas(schemas, promote_options="permissive")
    for schema in schemas:
        for field in schema:
            src, dst = field.type, merged.field(field.name).type
            if pa.types.is_decimal(src) and not pa.types.is_decimal(dst):
                raise ValueError("Lossy decimal promotion for %s; use an explicit schema" % field.name)
            if pa.types.is_integer(src):
                if not pa.types.is_integer(dst):
                    raise ValueError("Lossy integer promotion for %s; use an explicit schema" % field.name)
                if pa.types.is_signed_integer(src) != pa.types.is_signed_integer(dst):
                    if not (pa.types.is_signed_integer(dst) and dst.bit_width > src.bit_width):
                        raise ValueError("Unsafe signed/unsigned promotion for %s" % field.name)
    return pa.schema(
        [
            pa.field(
                f.name, f.type, nullable=f.nullable or any(f.name not in s.names for s in schemas), metadata=f.metadata
            )
            for f in merged
        ],
        metadata=merged.metadata,
    )


def _as_reader(data, schema=None):
    pa = _arrow()
    if isinstance(data, pa.RecordBatchReader):
        if schema is not None and not data.schema.equals(schema):
            raise ValueError("Reader schema differs from explicit schema")
        return data
    if isinstance(data, pa.RecordBatch):
        data = pa.Table.from_batches([data])
    elif not isinstance(data, pa.Table):
        if hasattr(data, "to_arrow"):
            data = data.to_arrow()  # Polars
        else:
            data = pa.Table.from_pandas(data, schema=schema, preserve_index=False)
    if schema is not None:
        data = data.cast(schema, safe=True)
    return data.to_reader()


def _protocol_schema(cursor):
    """Use protocol types, not the first batch, so empty/all-NULL results work."""
    pa = _arrow()
    from pymysql.constants import FIELD_TYPE as ft
    from pymysql.constants import FLAG

    fields = []
    for f in cursor._result.fields:
        code = f.type_code
        integers = {ft.TINY: 8, ft.SHORT: 16, ft.LONG: 32, ft.INT24: 32, ft.LONGLONG: 64}
        if code in integers:
            if code == ft.TINY and f.length == 1:
                dtype = pa.bool_()
            else:
                factory = getattr(pa, ("uint" if f.flags & FLAG.UNSIGNED else "int") + str(integers[code]))
                dtype = factory()
        elif code in (ft.DECIMAL, ft.NEWDECIMAL):
            precision = f.length - (0 if f.flags & FLAG.UNSIGNED else 1) - (1 if f.scale else 0)
            if not 1 <= precision <= 38:
                raise TypeError("Decimal protocol precision is outside Arrow Decimal128")
            dtype = pa.decimal128(precision, f.scale)
        elif code == ft.FLOAT:
            dtype = pa.float32()
        elif code == ft.DOUBLE:
            dtype = pa.float64()
        elif code in (ft.DATE, ft.NEWDATE):
            dtype = pa.date32()
        elif code == ft.TIMESTAMP:
            dtype = pa.timestamp("us", tz="UTC")
        elif code == ft.DATETIME:
            dtype = pa.timestamp("us")
        elif code == ft.TIME:
            # SQL TIME also supports negative and >24h durations.
            dtype = pa.duration("us")
        elif code in (ft.STRING, ft.VAR_STRING, ft.VARCHAR, ft.BLOB, ft.TINY_BLOB, ft.MEDIUM_BLOB, ft.LONG_BLOB):
            dtype = pa.binary() if f.charsetnr == 63 else pa.string()
        elif code in (ft.JSON, ft.ENUM, ft.SET):
            dtype = pa.string()
        elif code == ft.NULL:
            dtype = pa.null()
        else:
            raise TypeError("Unsupported result protocol type %s for %s" % (code, f.name))
        fields.append(pa.field(f.name, dtype, nullable=not bool(f.flags & FLAG.NOT_NULL)))
    schema = pa.schema(fields)
    if len(set(schema.names)) != len(schema.names):
        raise ValueError("Duplicate result column names; use SQL aliases")
    return schema


def _stage_uri_parts(parts):
    if re.search(r'%(?![0-9a-fA-F]{2})', parts.path):
        raise ValueError('Malformed percent escape in Stage URI')
    if parts.netloc:
        if any(c in parts.netloc for c in "@:%\\"):
            raise ValueError('Ambiguous Stage URI authority')
        return 'stage://' + parts.netloc, unquote(parts.path, errors='strict')
    if not parts.path.startswith('/'):
        raise ValueError('Stage path authority requires stage:///name/path')
    encoded, separator, remainder = parts.path[1:].partition('/')
    name = unquote(encoded, errors='strict')
    _identifier(name)
    if name in ('.', '..'):
        raise ValueError('Dot-only Stage names require host-form URLs')
    return 'stage:///' + quote(name, safe=''), '/' + unquote(remainder, errors='strict') if separator else ''


def _server_file_source(path):
    """Return matching external-scan and bounded DATALINK identities."""
    path = os.fspath(path)
    if not isinstance(path, str) or not path or "\x00" in path:
        raise ValueError("Server file paths must be nonempty strings without NUL")
    if path.startswith("/"):
        scan_path, file_path = path, path
        uri = "file://" + quote(file_path, safe="/")
    else:
        parts = urlsplit(path)
        if parts.query or parts.fragment or "?" in path or "#" in path:
            raise ValueError("Server file URIs must not contain query or fragment components")
        file_path = unquote(parts.path)
        if parts.scheme == "file" and not parts.netloc and file_path.startswith("/"):
            scan_path = file_path
            uri = "file://" + quote(file_path, safe="/")
        elif parts.scheme == "stage":
            if not path.lower().startswith("stage://"):
                raise ValueError("Stage URI requires stage://")
            root, file_path = _stage_uri_parts(parts)
            scan_path = uri = root + quote(file_path, safe="/")
        else:
            raise ValueError("Server files require an absolute local path, file:/// URI or stage:// URI")
    if not file_path or "\x00" in file_path or any(c in file_path for c in "*?[]\\"):
        raise ValueError("Explicit server file paths cannot contain glob metacharacters or backslash")
    return scan_path, uri + "?offset=0&size=1"


def _rows_to_batch(rows, source_schema, schema):
    """Convert detached rows; this helper never owns a driver or connection."""
    with phase('conversion'):
        pa = _arrow()
        arrays = []
        for i, f in enumerate(schema):
            values = [r[i] for r in rows]
            if pa.types.is_boolean(f.type):
                if any(v is not None and v not in (0, 1) for v in values):
                    raise ValueError("Non-boolean value in result column %s" % f.name)
                values = [None if v is None else bool(v) for v in values]
            if pa.types.is_time(f.type):
                values = [None if v is None else _time_microseconds(v) for v in values]
            if not f.nullable and any(v is None for v in values):
                raise ValueError("NULL in non-nullable result column %s" % f.name)
            if pa.types.is_time(f.type):
                # The driver conversion above always yields microseconds.
                # Arrow must rescale and reject fractional narrowing.
                array = pa.array(values, type=pa.time64("us"), safe=True).cast(f.type, safe=True)
            elif pa.types.is_boolean(f.type):
                array = pa.array(values, type=f.type, safe=True)
            else:
                # pa.array([1.5], type=int64, safe=True) still truncates. Cast
                # from the actual protocol type to enforce fractional checks.
                source_type = source_schema.field(i).type
                if pa.types.is_boolean(source_type):
                    if any(v is not None and v not in (0, 1) for v in values):
                        raise ValueError("Invalid boolean protocol value in column %s" % f.name)
                    values = [None if v is None else bool(v) for v in values]
                source = pa.array(values, type=source_type, safe=True)
                array = source.cast(f.type, safe=True)
                if pa.types.is_timestamp(source.type) and pa.types.is_date(f.type):
                    if not array.cast(source.type, safe=True).equals(source):
                        raise ValueError("Lossy timestamp-to-date conversion in result column %s" % f.name)
                if (
                    pa.types.is_floating(source.type)
                    and pa.types.is_floating(f.type)
                    and source.type.bit_width > f.type.bit_width
                ):
                    import pyarrow.compute as pc

                    equal = pc.or_(pc.equal(source, array.cast(source.type)), pc.is_nan(source))
                    if not pc.all(pc.fill_null(equal, True)).as_py():
                        raise ValueError("Lossy floating-point narrowing in result column %s" % f.name)
            arrays.append(array)
        return pa.RecordBatch.from_arrays(arrays, schema=schema)


class ArrowStream:
    """Context-managed batch iterator. Early close discards its pool connection.

    batch_size bounds row count, not bytes: a single large value can exceed the
    usual batch memory footprint. The caller must close a partially read stream.
    """

    def __init__(
        self,
        engine,
        sql,
        params=None,
        schema=None,
        batch_size=65536,
        network_timeout=None,
        _source_check=None,
        _client=None,
    ):
        self._telemetry_client = _client
        if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        if engine.dialect.driver != "pymysql":
            raise TypeError("Arrow streaming currently requires the PyMySQL driver")
        if network_timeout is not None and (
            isinstance(network_timeout, bool)
            or not isinstance(network_timeout, (int, float))
            or not math.isfinite(network_timeout)
            or network_timeout <= 0
        ):
            raise ValueError("network_timeout must be finite and positive")
        self._raw = engine.raw_connection()
        self._driver = self._raw.driver_connection
        self._old_timeouts = (self._driver._read_timeout, self._driver._write_timeout)
        if network_timeout is not None:
            self._driver._read_timeout = self._driver._write_timeout = network_timeout
        self._cursor = None
        self._complete = False
        self._closed = False
        self.batch_size = batch_size
        self._source_check = _source_check
        try:
            from pymysql.cursors import SSCursor

            with self._raw.cursor() as session_cursor:
                session_cursor.execute("SELECT @@time_zone, @@sql_mode")
                self._old_timezone, self._old_sql_mode = session_cursor.fetchone()
                session_cursor.execute("SET time_zone = '+00:00'")
                modes = [m for m in self._old_sql_mode.split(",") if m]
                if "STRICT_ALL_TABLES" not in modes:
                    modes.append("STRICT_ALL_TABLES")
                session_cursor.execute("SET sql_mode = %s", (",".join(modes),))
            self._check_sources()
            self._cursor = self._raw.cursor(SSCursor)
            self._cursor.execute(sql, params)
            if self._cursor.description is None:
                raise ValueError("Arrow query must return rows")
            self._source_schema = _protocol_schema(self._cursor)
            self.schema = schema if schema is not None else self._source_schema
            if [n.casefold() for n in self.schema.names] != [d[0].casefold() for d in self._cursor.description]:
                raise ValueError("Explicit schema names/order must match query columns")
        except BaseException as primary:
            self._close_after_error(primary)
            raise

    def _check_sources(self):
        if self._source_check is not None:
            cursor = self._raw.cursor()
            try:
                self._source_check(cursor.execute)
            except BaseException as primary:
                try:
                    cursor.close()
                except BaseException as cleanup_error:
                    primary.cursor_cleanup_error = cleanup_error
                raise
            else:
                cursor.close()

    def _close_after_error(self, primary):
        try:
            self.close()
        except BaseException as cleanup_error:
            primary.cleanup_error = cleanup_error

    def __iter__(self):
        return self

    @observe('arrow_fetch')
    def __next__(self):
        if self._closed:
            raise StopIteration
        try:
            with phase('fetch'):
                rows = self._cursor.fetchmany(self.batch_size)
            if not rows:
                self._complete = True
                self._check_sources()
                self.close()
                raise StopIteration
            return _rows_to_batch(rows, self._source_schema, self.schema)
        except StopIteration:
            raise
        except BaseException as primary:
            self._close_after_error(primary)
            raise

    def to_arrow(self):
        """Materialize remaining batches. Use iteration for bounded memory."""
        return _arrow().Table.from_batches(list(self), schema=self.schema)

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            if self._complete:
                self._cursor.close()
                self._raw.rollback()
                with self._raw.cursor() as session_cursor:
                    session_cursor.execute("SET time_zone = %s", (self._old_timezone,))
                    session_cursor.execute("SET sql_mode = %s", (self._old_sql_mode,))
            else:
                # Avoid SSCursor.close() draining millions of unread rows.
                if self._cursor is not None and self._cursor._result is not None:
                    self._cursor._result.unbuffered_active = False
                try:
                    self._raw.invalidate()
                finally:
                    if self._cursor is not None:
                        result = self._cursor._result
                        if result is not None:
                            # PyMySQL retains its connection in an unfinished
                            # MySQLResult. The connection also holds the result,
                            # so sever this cycle after invalidating the socket.
                            result.connection = None
                        self._cursor.connection = None
        except BaseException:
            self._raw.invalidate()
            raise
        finally:
            self._driver._read_timeout, self._driver._write_timeout = self._old_timeouts
            self._raw.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc is None:
            self.close()
        else:
            self._close_after_error(exc)


def _time_microseconds(value):
    if not isinstance(value, timedelta):
        return value
    micros = (value.days * 86400 + value.seconds) * 1000000 + value.microseconds
    if not 0 <= micros < 86400 * 1000000:
        raise ValueError("SQL TIME outside a clock day; use Arrow duration('us')")
    return micros


class FileRelation:
    """An external scan whose catalog tables are dropped on context exit.

    These are ordinary uniquely named external tables, NOT SQL temporary tables.
    The caller needs CREATE/DROP rights. A process crash can leave catalog entries.
    """

    def __init__(self, manager, schema, sql, tables, source_checks=None, server_sources=(), sources=()):
        self._manager = manager
        self._engine = manager.engine
        self.schema = schema
        self._sql = sql
        self._tables = tables
        self._closed = False
        self._sources = tuple(sources)
        self._source_checks = source_checks or []
        self._server_sources = tuple(server_sources)

    @property
    def sources(self):
        """Stable, expanded client-side paths; no rediscovery or database I/O."""
        return self._sources

    def _validate_sources(self):
        for check in self._source_checks:
            check()

    def _validate_server_sources(self, execute):
        for source in self._server_sources:
            # NULL is an existing empty file. Missing/permission errors must propagate.
            result = execute("SELECT length(load_file(CAST(%s AS DATALINK)))", (source,))
            # SQLAlchemy owns a CursorResult; the raw DBAPI adapter returns a row count.
            close = getattr(result, "close", None)
            if close is not None:
                close()

    @property
    def sql(self):
        if self._closed:
            raise RuntimeError("File relation is closed")
        self._validate_sources()
        if self._server_sources:
            with self._engine.connect() as conn:
                self._validate_server_sources(conn.exec_driver_sql)
        return self._sql

    def query(self, sql=None, params=None, schema=None, batch_size=65536, network_timeout=None):
        """Custom SQL references the relation through its quoted .sql expression."""
        if self._closed:
            raise RuntimeError("File relation is closed")
        self._validate_sources()
        if sql is None and schema is None:
            schema = self.schema
        if network_timeout is None:
            network_timeout = getattr(self._manager.client, "query_timeout", 300)
        return ArrowStream(
            self._engine,
            sql or "SELECT * FROM " + self._sql + " AS files",
            params,
            schema,
            batch_size,
            network_timeout,
            _source_check=self._validate_server_sources,
            _client=self._manager.client,
        )

    @observe('file_import')
    def import_into(self, table):
        """Append the relation to an existing table in one transaction."""
        if self._closed:
            raise RuntimeError("File relation is closed")
        self._validate_sources()
        with _write_transaction(self._engine) as conn:
            self._validate_server_sources(conn.exec_driver_sql)
            result = conn.exec_driver_sql(
                (
                    "INSERT INTO %s (%s) SELECT * FROM %s AS files"
                    % (_table_name(table), ", ".join(map(_identifier, self.schema.names)), self._sql)
                ).replace("%", "%%")
            )
            self._validate_server_sources(conn.exec_driver_sql)
            return result.rowcount

    def close(self):
        if self._closed:
            return
        # Remove only owned objects. Retain failed drops for a retry.
        first_error = None
        for identity in reversed(self._tables[:]):
            try:
                with self._engine.begin() as conn:
                    conn.exec_driver_sql("DROP TABLE IF EXISTS " + _qualified_table(*identity).replace("%", "%%"))
                self._tables.remove(identity)
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        self._closed = not self._tables
        if first_error is not None:
            raise first_error

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc is None:
            self.close()
        else:
            self._close_after_error(exc)

    def _close_after_error(self, primary):
        try:
            self.close()
        except BaseException as cleanup_error:
            primary.cleanup_error = cleanup_error
            try:
                self._manager.client.logger.warning("External table cleanup failed: %s" % cleanup_error)
            except BaseException:
                pass  # Diagnostics cannot replace the selected primary failure.


class _FileScan(NamedTuple):
    columns: str
    options: str
    params: tuple
    suffix: str
    projection: str

    def ddl(self, qualified):
        return "CREATE EXTERNAL TABLE %s (%s) INFILE {%s}%s" % (
            qualified.replace("%", "%%"),
            self.columns,
            self.options,
            self.suffix,
        )


class _FilePlan(NamedTuple):
    schema: object
    scans: tuple
    checks: tuple
    server_sources: tuple
    sources: tuple = ()


def _check_cancelled(cancelled):
    if cancelled is not None and cancelled.is_set():
        from concurrent.futures import CancelledError

        raise CancelledError("Analytics worker cancelled")


def _plan_files(
    paths,
    format,
    schema=None,
    union_by_name=False,
    delimiter=",",
    hive_partitioning=False,
    server_paths=None,
    filesystem=None,
    max_files=256,
    header=True,
    cancelled=None,
):
    """Discover and validate sources without acquiring a database connection."""
    pa = _arrow()
    if format not in ("parquet", "csv", "jsonline"):
        raise ValueError("format must be parquet, csv or jsonline")
    if not isinstance(header, bool) or (format != 'csv' and not header):
        raise ValueError('header must be a boolean and applies only to CSV')
    column_names = schema.names if schema is not None and not header else None
    if len(delimiter) != 1 or delimiter in ('"', "\n", "\r", "\x00"):
        raise ValueError("CSV delimiter must be a single non-quote character")
    if isinstance(max_files, bool) or not isinstance(max_files, int) or max_files <= 0:
        raise ValueError("max_files must be a positive integer")
    patterns = [paths] if isinstance(paths, (str, os.PathLike)) else paths
    files, seen = [], set()
    for pattern in patterns:
        _check_cancelled(cancelled)
        pattern = os.fspath(pattern)
        if filesystem is not None and glob.has_magic(pattern):
            raise ValueError("Remote globs require an explicit expanded file list")
        matches = glob.iglob(pattern) if filesystem is None and glob.has_magic(pattern) else (pattern,)
        expanded = []
        matched = False
        for match in matches:
            _check_cancelled(cancelled)
            matched = True
            path = str(Path(match).resolve(strict=True)) if filesystem is None else match
            if path not in seen:
                seen.add(path)
                expanded.append(path)
                if len(seen) > max_files:
                    raise ValueError("Expected 1..%d source files" % max_files)
        if not matched:
            raise FileNotFoundError("No files match %s" % pattern)
        files.extend(sorted(expanded))
    if not files or len(files) > max_files:
        raise ValueError("Expected 1..%d source files" % max_files)
    if server_paths is None:
        server_paths = files
    else:
        bounded_paths = []
        for path in server_paths:
            _check_cancelled(cancelled)
            bounded_paths.append(path)
            if len(bounded_paths) > len(files):
                raise ValueError("server_paths must match the expanded file list")
        server_paths = bounded_paths
    if len(server_paths) != len(files):
        raise ValueError("server_paths must match the expanded file list")
    server_sources = tuple(_server_file_source(path) for path in server_paths)
    server_paths = [source[0] for source in server_sources]

    schemas, partitions = [], []
    for path in files:
        _check_cancelled(cancelled)
        try:
            source_schema = (
                schema
                if schema is not None
                else _infer_schema(path, format, delimiter, filesystem, header, column_names)
            )
            _validate_schema(source_schema)
            _validate_scan_precision(source_schema)
            if schema is not None or format == "csv":
                _validate_source_columns(path, format, delimiter, filesystem, source_schema, header)
            part = _hive_values(path) if hive_partitioning else {}
            if set(k.casefold() for k in part) & set(n.casefold() for n in source_schema.names):
                raise ValueError("Hive key overlaps a physical column")
            logical_schema = pa.schema(list(source_schema) + [pa.field(k, pa.string()) for k in part])
            _validate_schema(logical_schema)
            schemas.append((source_schema, logical_schema))
            partitions.append(part)
        except Exception as exc:
            raise ValueError("Schema inference for %s failed: %s" % (path, exc)) from exc
    if not union_by_name and any(not s[1].equals(schemas[0][1], check_metadata=False) for s in schemas):
        raise ValueError("File schemas differ; pass union_by_name=True or an explicit schema")
    merged = _merge_schemas([s[1] for s in schemas])
    _validate_schema(merged)
    checks = []
    for path, (physical, _) in zip(files, schemas):
        expected = _infer_schema(path, format, delimiter, filesystem, header, column_names)

        def check(path=path, expected=expected):
            actual = _infer_schema(path, format, delimiter, filesystem, header, column_names)
            if not actual.equals(expected, check_metadata=False):
                raise ValueError("Source schema changed after relation creation: %s" % path)
            if format == "csv":
                _validate_source_columns(path, format, delimiter, filesystem, expected, header)

        checks.append(check)
    scans = []
    for server_path, (physical, logical), part in zip(server_paths, schemas, partitions):
        options = "'filepath'=%s, 'format'=%s"
        params = [os.fspath(server_path), format]
        if format == "jsonline":
            options += ", 'jsondata'='object'"
        columns = _ddl_columns(physical)
        suffix = ""
        if format == "csv":
            suffix = " FIELDS TERMINATED BY %s ENCLOSED BY %s"
            if header:
                suffix += " IGNORE 1 LINES"
            params.extend([delimiter, '"'])
        projections = []
        for f in merged:
            if f.name in part:
                # Hex literal avoids SQL-mode-dependent quote escaping.
                value = (
                    "NULL"
                    if part[f.name] is None
                    else "CONVERT(X'%s' USING utf8mb4)" % part[f.name].encode("utf-8").hex()
                )
            elif f.name in physical.names:
                value = _identifier(f.name)
            else:
                value = "NULL"
            if value == "NULL":
                dtype = f.type
                if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
                    cast_type = "CHAR"
                elif pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
                    cast_type = "BINARY"
                else:
                    cast_type = _sql_type(dtype)
                value = "CAST(NULL AS %s)" % cast_type
            # UNION's common-type rules own promotion; missing columns are
            # typed by the other branches. No lossy client-side conversion.
            projections.append(value + " AS " + _identifier(f.name))
        scans.append(_FileScan(columns, options, tuple(params), suffix, ", ".join(projections)))
    return _FilePlan(merged, tuple(scans), tuple(checks), tuple(source[1] for source in server_sources), tuple(files))


class _StagedArrow(NamedTuple):
    path: str
    schema: object
    rows: int


class _StagingSink(io.RawIOBase):
    """Enforce the disk budget before every IPC write, including the footer."""

    def __init__(self, fd, limit):
        super().__init__()
        self._file = os.fdopen(fd, "wb", buffering=0)
        self._limit = limit

    def writable(self):
        return True

    def tell(self):
        return self._file.tell()

    def write(self, data):
        if len(data) > self._limit - self.tell():
            raise ValueError("Arrow staging byte limit exceeded")
        return self._file.write(data)

    def flush(self):
        if not self._file.closed:
            self._file.flush()

    def close(self):
        try:
            super().close()
        finally:
            self._file.close()


def _validate_upload(batch_size, max_staging_bytes):
    if not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    if isinstance(max_staging_bytes, bool) or not isinstance(max_staging_bytes, int) or max_staging_bytes <= 0:
        raise ValueError("max_staging_bytes must be a positive integer")


def _stage_arrow(data, staging_dir, schema, batch_size, max_staging_bytes, on_path, cancelled=None):
    """Produce one IPC file. The caller owns unlink after this function exits.

    on_path registers the exact reserved identity before any IPC write. No DB
    connection is involved. A supplied reader remains owned by its caller.
    """
    pa = _arrow()
    import pyarrow.ipc as ipc

    _validate_upload(batch_size, max_staging_bytes)
    _check_cancelled(cancelled)
    owned_reader = not isinstance(data, pa.RecordBatchReader)
    reader = _as_reader(data, schema)
    sink = writer = None
    try:
        _validate_schema(reader.schema)
        wire = _wire_schema(reader.schema)
        directory = Path(staging_dir).resolve(strict=True)
        _check_cancelled(cancelled)
        fd, path = tempfile.mkstemp(prefix="mo_py_arrow_", suffix=".arrow", dir=str(directory))
        try:
            on_path(path)
            sink = _StagingSink(fd, max_staging_bytes)
        except BaseException:
            os.close(fd)
            raise
        writer = ipc.new_file(sink, wire)
        rows = 0
        for batch in reader:
            _check_cancelled(cancelled)
            if not batch.schema.equals(reader.schema, check_metadata=False):
                raise ValueError("Arrow reader schema drift")
            for start in range(0, batch.num_rows, batch_size):
                _check_cancelled(cancelled)
                sliced = batch.slice(start, batch_size)
                for i, field in enumerate(wire):
                    if not field.nullable and sliced.column(i).null_count:
                        raise ValueError("NULL in non-nullable input column %s" % field.name)
                converted = sliced.cast(wire, safe=True)
                writer.write_batch(converted)
                rows += converted.num_rows
        _check_cancelled(cancelled)
        return _StagedArrow(path, wire, rows)
    finally:
        primary = sys.exc_info()[1]
        cleanup_errors = []
        for resource in (writer, sink, reader if owned_reader else None):
            if resource is not None:
                try:
                    resource.close()
                except BaseException as error:
                    cleanup_errors.append(error)
        if cleanup_errors:
            if primary is not None:
                primary.arrow_cleanup_errors = tuple(cleanup_errors)
            else:
                error = cleanup_errors[0]
                error.arrow_cleanup_errors = tuple(cleanup_errors[1:])
                raise error


class AnalyticsManager:
    """DuckDB-style conveniences over existing MatrixOne SQL execution paths."""

    def __init__(self, client):
        self.client = client

    def _require_arrow_load(self):
        from .capabilities import require_arrow_override

        require_arrow_override(self.client)

    @property
    def engine(self):
        if self.client._engine is None:
            raise RuntimeError("Connect the MatrixOne client first")
        return self.client._engine

    @observe('arrow_open')
    def query(self, sql, params=None, schema=None, batch_size=65536, network_timeout=None):
        """Stream query results as Arrow batches. Parameters use DBAPI %s binding."""
        _arrow()
        if network_timeout is None:
            network_timeout = getattr(self.client, "query_timeout", 300)
        return ArrowStream(self.engine, sql, params, schema, batch_size, network_timeout, _client=self.client)

    def read_parquet(self, paths, **kwargs):
        return self.read_files(paths, format="parquet", **kwargs)

    def read_csv(self, paths, **kwargs):
        return self.read_files(paths, format="csv", **kwargs)

    def read_json(self, paths, lines=True, orient='records', **kwargs):
        """Read newline-delimited JSON objects, not a top-level JSON array."""
        if lines is not True or orient != 'records':
            raise ValueError('File relations support JSON Lines objects: lines=True, orient=records')
        return self.read_files(paths, format="jsonline", **kwargs)

    def read_files(
        self,
        paths,
        format,
        schema=None,
        union_by_name=False,
        delimiter=",",
        hive_partitioning=False,
        server_paths=None,
        filesystem=None,
        max_files=256,
        header=True,
    ):
        """Infer client-side schemas and scan server-visible files without import.

        Local globs are expanded into a fixed file list. Remote inputs need an
        explicit list and a PyArrow filesystem; server_paths maps that list to
        server file/stage paths. CSV inference reads a bounded first block, so
        use an explicit schema when later values can change inferred types.
        Hive partition values are strings, preserving zero-padding.
        """
        plan = _plan_files(
            paths,
            format,
            schema,
            union_by_name,
            delimiter,
            hive_partitioning,
            server_paths,
            filesystem,
            max_files,
            header,
        )
        tables, selects = [], []
        relation = FileRelation(self, plan.schema, "", tables, plan.checks, plan.server_sources, plan.sources)
        sources_checked = False
        try:
            for scan in plan.scans:
                table = "mo_py_files_" + uuid.uuid4().hex
                # Reserve the cleanup owner before CREATE/COMMIT can have an
                # uncertain outcome. A failed construction returns no relation.
                identity = None
                try:
                    with relation._engine.begin() as conn:
                        if not sources_checked:
                            relation._validate_server_sources(conn.exec_driver_sql)
                            sources_checked = True
                        identity = (conn.exec_driver_sql("SELECT DATABASE()").scalar(), table)
                        qualified = _qualified_table(*identity)
                        tables.append(identity)
                        conn.exec_driver_sql(scan.ddl(qualified), scan.params)
                except Exception as error:
                    from sqlalchemy.exc import DBAPIError

                    # A definite collision proves this name was not ours,
                    # whether CREATE or its transaction reports the rejection.
                    if (
                        isinstance(error, DBAPIError)
                        and not error.connection_invalidated
                        and getattr(error.orig, 'args', ())
                        and error.orig.args[0] == 1050
                        and identity in tables
                    ):
                        tables.remove(identity)
                    raise
                selects.append("SELECT %s FROM %s" % (scan.projection, qualified))
            relation._sql = "(" + " UNION ALL ".join(selects) + ")"
            return relation
        except BaseException as primary:
            primary.attempted_tables = tuple(_qualified_table(*identity) for identity in tables)
            primary.file_relation = relation
            relation._close_after_error(primary)
            primary.owned_tables = tuple(_qualified_table(*identity) for identity in tables)
            raise

    @observe('arrow_upload')
    def upload_arrow(
        self,
        table,
        data,
        staging_dir,
        server_staging_dir=None,
        schema=None,
        create=True,
        batch_size=65536,
        max_staging_bytes=1024 * 1024 * 1024,
    ):
        """Create/append via one atomic native Arrow LOAD from shared staging.

        staging_dir must already exist and be visible to the server, optionally
        under server_staging_dir. No LOCAL/Flight transport is implied. Nested
        types and sub-microsecond timestamps fail rather than silently truncate.
        With create=True the target must not exist; existing tables are untouched.
        """
        target = _table_name(table).replace("%", "%%")
        _validate_upload(batch_size, max_staging_bytes)
        self._require_arrow_load()
        paths = []
        rows = None
        committed = False
        operation_phase = 'staging'
        try:
            staged = _stage_arrow(data, staging_dir, schema, batch_size, max_staging_bytes, paths.append)
            rows = staged.rows
            wire = staged.schema
            server_path = _server_staging_path(server_staging_dir or Path(staged.path).parent, Path(staged.path).name)
            try:
                with _write_transaction(self.engine, rows=rows) as conn:
                    from .capabilities import check_arrow_load

                    check_arrow_load(conn)
                    operation_phase = 'ddl_table'
                    if create:
                        conn.exec_driver_sql("CREATE TABLE %s (%s)" % (target, _ddl_columns(wire)))
                    operation_phase = 'load'
                    conn.exec_driver_sql(
                        "LOAD DATA INFILE {'filepath'=%s, 'format'='arrow'} INTO TABLE " + target,
                        (server_path,),
                    )
                committed = True
            except (AnalyticsCleanupError, AnalyticsCommitUnknownError) as error:
                committed = error.committed
                raise
            return rows
        except BaseException as error:
            if getattr(error, 'phase', None) == 'execute':
                error.phase = operation_phase
            annotate_execution_error(
                error, phase=operation_phase, rows=rows, committed=getattr(error, 'committed', False)
            )
            raise
        finally:
            primary = sys.exc_info()[1]
            try:
                if paths:
                    os.unlink(paths[0])
            except FileNotFoundError:
                pass
            except OSError as cleanup_error:
                if primary is not None:
                    primary.staging_cleanup_error = cleanup_error
                else:
                    raise AnalyticsCleanupError(
                        "Staging cleanup failed after transaction; committed=%s" % committed,
                        committed=committed,
                        rows=rows,
                    ) from cleanup_error

    def upload_dataframe(self, table, dataframe, staging_dir, **kwargs):
        return self.upload_arrow(table, dataframe, staging_dir, **kwargs)


def _infer_schema(path, format, delimiter, filesystem, header=True, column_names=None):
    pa = _arrow()
    if filesystem is None:
        source = pa.input_stream(path)
    else:
        source = filesystem.open_input_file(path)
    with source:
        if format == "parquet":
            import pyarrow.parquet as pq

            return pq.ParquetFile(source).schema_arrow
        if format == "csv":
            import pyarrow.csv as csv

            with csv.open_csv(
                source,
                read_options=csv.ReadOptions(
                    column_names=column_names, autogenerate_column_names=not header and column_names is None
                ),
                parse_options=csv.ParseOptions(delimiter=delimiter, newlines_in_values=True),
                convert_options=csv.ConvertOptions(
                    null_values=["\\N"], strings_can_be_null=True, quoted_strings_can_be_null=True
                ),
            ) as reader:
                return reader.schema
        import pyarrow.json as json

        if hasattr(json, "open_json"):
            with json.open_json(source) as reader:
                return reader.schema
        # PyArrow 18 has no streaming JSON reader. Infer from a bounded
        # first block; read_json(source) would materialize the entire file.
        block_size = 1 << 20
        sample = source.read(block_size)
        if len(sample) == block_size and not sample.endswith(b"\n"):
            boundary = sample.rfind(b"\n")
            if boundary < 0:
                raise ValueError("JSON record exceeds the 1 MiB schema inference block")
            sample = sample[: boundary + 1]
        return json.read_json(pa.BufferReader(sample)).schema


def _validate_source_columns(path, format, delimiter, filesystem, schema, header=True):
    """Explicit schema changes types, not positional CSV header meaning."""
    if format == "csv":
        import pyarrow.csv as csv

        source = _arrow().input_stream(path) if filesystem is None else filesystem.open_input_file(path)
        with source:
            with csv.open_csv(
                source,
                read_options=csv.ReadOptions(column_names=schema.names if not header else None),
                parse_options=csv.ParseOptions(delimiter=delimiter, newlines_in_values=True),
                convert_options=csv.ConvertOptions(column_types={n: _arrow().string() for n in schema.names}),
            ) as reader:
                if reader.schema.names != schema.names:
                    raise ValueError("Explicit schema names/order differ from CSV header")
                # The native scanner silently ignores extra CSV fields. Validate
                # all records, in bounded Arrow blocks, before exposing the scan.
                for _ in reader:
                    pass
    elif format == "parquet":
        actual = _infer_schema(path, format, delimiter, filesystem)
        _validate_scan_precision(actual)
        if actual.names != schema.names:
            raise ValueError("Explicit schema names/order differ from Parquet fields")


def _hive_values(path):
    values = {}
    for component in Path(path).parts[:-1]:
        if "=" in component:
            name, value = component.split("=", 1)
            name, value = unquote(name), unquote(value)
            _identifier(name)
            if name in values:
                raise ValueError("Duplicate Hive key %s" % name)
            values[name] = None if value == "__HIVE_DEFAULT_PARTITION__" else value
    return values


def _server_staging_path(directory, filename):
    """Join a server path without applying local filesystem URI normalization."""
    directory = os.fspath(directory)
    parsed = urlsplit(directory)
    if parsed.scheme:
        if parsed.query or parsed.fragment or parsed.scheme not in ('file', 'stage', 's3', 'oss'):
            raise ValueError('Unsupported or ambiguous server staging URI')
        if parsed.scheme == 'stage':
            if not directory.lower().startswith('stage://'):
                raise ValueError('Stage URI requires stage://')
            root, path = _stage_uri_parts(parsed)
            return root + quote(path.rstrip('/') + '/' + filename, safe='/')
        if parsed.scheme != 'file' and not parsed.netloc:
            raise ValueError('Server staging URI requires an authority')
        return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip('/') + '/' + filename, '', ''))
    return str(Path(directory) / filename)
