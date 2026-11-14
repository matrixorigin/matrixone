# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import QueuePool

from matrixone._release_compat import _before_cursor_execute, _handle_error, _qualified_release
from matrixone.exceptions import QueryError


def connection(banner='8.0.30-MatrixOne-v4.2.8'):
    return SimpleNamespace(
        closed=False,
        invalidated=False,
        connection=SimpleNamespace(driver_connection=SimpleNamespace(server_version=banner)),
    )


@pytest.mark.parametrize(
    'banner',
    [
        None,
        '',
        '8.0.30-MatrixOne-v4.2.80',
        '8.0.30-MatrixOne-v4.2.8+custom',
        '8.0.30-MatrixOne-v4.2',
        '8.0.30-MatrixOne-v1.3.0',
    ],
)
def test_only_qualified_physical_identity_selects_profile(banner):
    c = connection(banner)
    sql = 'CREATE TABLE t(id INT, v VECF8(4))'
    assert _before_cursor_execute(c, None, sql, (), None, False) == (sql, ())
    c.connection.driver_connection.server_version = '8.0.30-MatrixOne-v4.2.8'
    assert _qualified_release(c)


@pytest.mark.parametrize('version', ['4.2.8-hotfix-20261114', '4.2.8-hotfix'])
def test_qualified_hotfix_keeps_its_exact_identity_in_diagnostic(version):
    c = connection('8.0.30-MatrixOne-v' + version)
    with pytest.raises(QueryError) as rejected:
        _before_cursor_execute(c, None, 'CREATE TABLE t(v VECF4(4))', (), None, False)
    assert rejected.value.backend_version == version
    assert 'MatrixOne v' + version + ' compatibility:' in str(rejected.value)


@pytest.mark.parametrize(
    'sql,feature',
    [
        ('CREATE TABLE t(id INT, v VECF8(4))', 'vecf8'),
        ('CREATE TABLE t(id INT, v VECF4(4))', 'vecf4'),
        ('CREATE FULLTEXT INDEX ft ON t (`tick``col`)', 'fulltext_identifiers'),
        ('CREATE FULLTEXT INDEX ft ON t (`order`)', 'fulltext_identifiers'),
        ('CREATE FULLTEXT INDEX ft ON t (`fulltext1`)', 'fulltext_identifiers'),
        ('CREATE TABLE t(id INT, body TEXT, FULLTEXT ft(body) WITH PARSER gojieba ASYNC)', 'async_fulltext_parser'),
        ('CREATE TABLE t(id INT, body TEXT, FULLTEXT ft(body) WITH PARSER `gojieba` ASYNC)', 'async_fulltext_parser'),
        ('CREATE TABLE t(id INT, body TEXT, FULLTEXT ft(body) ASYNC WITH PARSER gojieba)', 'async_fulltext_parser'),
        (
            'CREATE TABLE t(id INT, body TEXT, FULLTEXT ft(body) WITH PARSER gojieba COMMENT \'kept\' ASYNC)',
            'async_fulltext_parser',
        ),
        ("CREATE STAGE s URL='file:///tmp/' ENABLE=FALSE", 'stage_create_disabled'),
        ("CREATE STAGE s URL='file:///tmp/' COMMENT='one''two'", 'stage_literals'),
        ("ALTER STAGE s SET COMMENT='one''two'", 'stage_literals'),
        ("/*!50000 CREATE STAGE s URL='file:///tmp/' ENABLE=FALSE */", 'stage_create_disabled'),
    ],
)
def test_unsafe_release_statement_is_actionable_before_dispatch(sql, feature):
    with pytest.raises(QueryError, match='MatrixOne v4.2.8 compatibility:') as caught:
        _before_cursor_execute(connection(), None, sql, (), None, False)
    e = caught.value
    assert e.phase == 'compatibility' and e.committed is False and e.statement_acknowledged is False
    assert e.compatibility_feature == feature and e.compatibility_hint
    assert e.orig is None and e.errno is None
    assert sql not in str(e)


@pytest.mark.parametrize(
    'sql',
    [
        "SELECT 'CREATE STAGE s ENABLE=FALSE'",
        "CREATE TABLE t(v TEXT DEFAULT 'FULLTEXT WITH PARSER gojieba ASYNC')",
        "CREATE TABLE t(v TEXT COMMENT 'VECF8(4)')",
        "CREATE TABLE t(id INT, body TEXT, FULLTEXT ft(body) WITH PARSER gojieba)",
        "CREATE TABLE t(id INT, body TEXT, FULLTEXT ft(body) ASYNC)",
        "CREATE TABLE t(id INT, async INT, body TEXT, FULLTEXT ft(body) WITH PARSER gojieba)",
        "CREATE TABLE t(id INT, body TEXT, FULLTEXT ft(body) WITH PARSER gojieba, async INT)",
        "CREATE TABLE t(id INT, body TEXT, FULLTEXT async(body) WITH PARSER gojieba)",
        "CREATE FULLTEXT INDEX async ON t(body) WITH PARSER gojieba",
        "ALTER TABLE t ADD FULLTEXT ft(body) WITH PARSER gojieba, ADD COLUMN async INT",
        "CREATE TABLE t(a TEXT, b TEXT, FULLTEXT fa(a) ASYNC, FULLTEXT fb(b) WITH PARSER gojieba)",
        "CREATE TABLE t(a TEXT, b TEXT, FULLTEXT fa(a) WITH PARSER gojieba, FULLTEXT fb(b) ASYNC)",
        "CREATE FULLTEXT INDEX ft ON t(body) WITH PARSER async WITH PARSER gojieba",
        "CREATE FULLTEXT INDEX ft ON t (`body`)",
        'CREATE TABLE t(id INT PRIMARY KEY, body TEXT, `other``col` INT, FULLTEXT ft(body))',
        "CREATE STAGE s URL='file:///tmp/' ENABLE=TRUE",
        "ALTER STAGE s SET ENABLE=FALSE",
        "/* FULLTEXT ASYNC */ CREATE TABLE t(id INT)",
        "CREATE TABLE t(id INT) // VECF4(4)",
        "CREATE TABLE t(id INT) # VECF8(4)",
        "CREATE TABLE t(v TEXT DEFAULT $$FULLTEXT WITH PARSER gojieba ASYNC$$)",
    ],
)
def test_supported_and_literal_comment_controls_are_unchanged(sql):
    assert _before_cursor_execute(connection(), None, sql, (), None, False) == (sql, ())


def test_bound_stage_value_is_checked_without_rendering_or_disclosing_it():
    with pytest.raises(QueryError) as caught:
        _before_cursor_execute(connection(), None, 'CREATE STAGE s URL=%s', ("file:///private'path/",), None, False)
    assert caught.value.compatibility_feature == 'stage_literals'
    assert 'private' not in str(caught.value)


def test_preflight_has_no_fabricated_native_cause_from_an_outer_exception():
    try:
        raise Exception(2013, 'unrelated private error')
    except Exception:
        with pytest.raises(QueryError) as rejected:
            _before_cursor_execute(connection(), None, 'CREATE TABLE t(v VECF8(4))', (), None, False)
    assert rejected.value.errno is None and rejected.value.orig is None
    assert 'private' not in str(rejected.value)


def test_explicit_driver_sql_keeps_its_exact_bytes():
    context = SimpleNamespace(execution_options={'_matrixone_preserve_driver_sql': True})
    sql = 'SELECT 1.0$$alias$$, %s'
    assert _before_cursor_execute(connection(), None, sql, (7,), context, False) == (sql, (7,))


def test_double_quoted_ddl_does_not_probe_sql_mode_or_change_transaction_state():
    c = connection()

    def unexpected(*args, **kwargs):
        raise AssertionError('compatibility inspection must issue zero SQL')

    c.exec_driver_sql = unexpected
    sql = 'CREATE STAGE IF NOT EXISTS s URL="file:///tmp/data/"'
    assert _before_cursor_execute(c, None, sql, (), None, False) == (sql, ())


def test_ambiguous_quote_modes_do_not_authorize_sql_rewriting():
    sql = r"SELECT 'a\'1.0$$alias$$"
    assert _before_cursor_execute(connection(), None, sql, (), None, False) == (sql, ())


def test_release_date_display_width_preserves_all_other_metadata():
    from matrixone._release_compat import normalize_described_columns

    rows = [
        ('day', 'DATE(0)', 'NO', '', None, '', 'comment'),
        ('amount', 'DECIMAL(24,6)', 'YES', '', None, '', ''),
        ('n', 'BIGINT UNSIGNED(0)', 'YES', '', None, '', ''),
    ]
    assert normalize_described_columns(connection(), rows) == [
        ('day', 'date', 'NO', '', None, '', 'comment'),
        ('amount', 'decimal(24,6)', 'YES', '', None, '', ''),
        ('n', 'bigint(0) unsigned', 'YES', '', None, '', ''),
    ]
    assert normalize_described_columns(connection('8.0.30-MatrixOne-v1.3.0'), rows) is rows


def test_unrecoverable_view_domain_has_an_actionable_metadata_error():
    from matrixone._release_compat import normalize_described_columns

    rows = [('value', 'ENUM(0)', 'YES', '', None, '', '')]
    with pytest.raises(QueryError) as rejected:
        normalize_described_columns(connection(), rows)
    error = rejected.value
    assert error.compatibility_feature == 'view_enum_set_provenance'
    assert error.phase == 'metadata_reflection' and error.committed is False
    assert error.statement_acknowledged is None and error.orig is None and error.errno is None
    assert normalize_described_columns(connection('8.0.30-MatrixOne-v4.2'), rows) is rows


@pytest.mark.parametrize(
    'sql,expected',
    [
        ('SELECT 1.0$$alias$$, %s', 'SELECT 1.0 $$alias$$, %s'),
        ('SELECT .1$tag$? :ignored$tag$, :v', 'SELECT .1 $tag$? :ignored$tag$, :v'),
        ('SELECT 1e+2$$alias$$, ?', 'SELECT 1e+2 $$alias$$, ?'),
        ("SELECT '1.0$$alias$$', 7", "SELECT '1.0$$alias$$', 7"),
        ('SELECT name1$tag$, 7', 'SELECT name1$tag$, 7'),
        ('SELECT 1.0$$unterminated', 'SELECT 1.0$$unterminated'),
        ('SELECT 1 // 1.0$$alias$$', 'SELECT 1 // 1.0$$alias$$'),
    ],
)
def test_numeric_portability_preserves_data_parameters_and_noncode(sql, expected):
    params = {'v': "private ' value"}
    actual, bound = _before_cursor_execute(connection(), None, sql, params, None, False)
    assert actual == expected and bound is params


@pytest.mark.parametrize(
    'sql,code,message,feature',
    [
        (
            'DATA BRANCH DELETE DATABASE b',
            20101,
            'internal error: DATA BRANCH DELETE target b is not an active branch database',
            'branch_database_identity',
        ),
        (
            'SHOW CREATE TABLE t',
            1105,
            'You have an error in your SQL syntax; syntax error near invalid enum',
            'schema_serialization',
        ),
        ('LOAD DATA INFILE', 20301, 'invalid input: parquet value +Inf overflows FLOAT', 'parquet_float32_infinity'),
        (
            'INSERT INTO t VALUES(1)',
            20101,
            'internal error: jieba dictionary not available: jieba.dict.utf8: stat jieba.dict.utf8: no such file or directory',
            'jieba_dictionary',
        ),
    ],
)
def test_native_rejection_keeps_real_source_and_actionable_hint(sql, code, message, feature):
    native = Exception(code, message)
    original = OperationalError(sql, {}, native)
    context = SimpleNamespace(
        connection=connection(), original_exception=native, sqlalchemy_exception=original, statement=sql
    )
    e = _handle_error(context)
    assert isinstance(e, QueryError) and e.orig is original and e.errno == code
    assert e.phase == 'execute' and e.committed is None and e.statement_acknowledged is None
    assert e.compatibility_feature == feature and e.compatibility_hint
    assert message not in str(e)


@pytest.mark.parametrize(
    'code,message',
    [
        (20101, 'permission denied'),
        (2013, 'connection lost'),
        (1105, 'unrelated syntax error'),
        (20301, 'invalid input: parquet value 1e99 overflows FLOAT'),
        (20101, 'internal error: unrelated dictionary failure'),
        (2013, 'internal error: jieba dictionary not available: jieba.dict.utf8'),
    ],
)
def test_other_native_failures_are_never_qualified_as_release_limitations(code, message):
    context = SimpleNamespace(
        connection=connection(),
        original_exception=Exception(code, message),
        sqlalchemy_exception=None,
        statement='SHOW CREATE TABLE t',
    )
    assert _handle_error(context) is None
    context.connection.invalidated = True
    assert _handle_error(context) is None


def test_engine_local_rejection_has_no_catalog_effect_and_pool_is_reusable(monkeypatch):
    from matrixone import _release_compat

    engine = create_engine('sqlite://', poolclass=QueuePool, pool_size=1, max_overflow=0)
    _release_compat.install_release_compatibility(engine)
    _release_compat.install_release_compatibility(engine)
    monkeypatch.setattr(_release_compat, '_release_version', lambda conn: '4.2.8')
    try:
        with pytest.raises(QueryError, match='VECF8'):
            with engine.connect() as conn:
                conn.exec_driver_sql('CREATE TABLE release_guard(v VECF8(4))')
        with engine.connect() as conn:
            assert conn.exec_driver_sql("SELECT COUNT(*) FROM sqlite_master WHERE name='release_guard'").scalar() == 0
            assert conn.exec_driver_sql('SELECT 42').scalar() == 42
        assert engine.pool.checkedout() == 0
    finally:
        engine.dispose()


@pytest.mark.parametrize('bitwise', [False, True])
def test_set_domain_case_and_processors_come_from_bound_tenant_catalog(bitwise):
    from sqlalchemy.dialects.mysql import SET
    from matrixone._release_compat import restore_set_members

    c = connection()
    captured = []

    class Rows:
        closed = False

        def all(self):
            return [('flags', "UP,O'Brien,Down word")]

        def close(self):
            self.closed = True

    result = Rows()

    def execute(sql, parameters):
        captured.append((str(sql), parameters))
        return result

    c.execute = execute
    columns = [
        {
            'name': 'flags',
            'type': SET(
                'up', "o'brien", 'down word', retrieve_as_bitwise=bitwise, charset='utf8mb4', collation='utf8mb4_bin'
            ),
        }
    ]
    updated = restore_set_members(c, columns, "private'table", 'private_database')
    dtype = updated[0]['type']
    assert dtype.values == ('UP', "O'Brien", 'Down word')
    assert dtype.charset == 'utf8mb4' and dtype.collation == 'utf8mb4_bin' and dtype.retrieve_as_bitwise is bitwise
    assert result.closed and len(captured) == 1
    sql, parameters = captured[0]
    assert 'account_id=current_account_id()' in sql
    assert 'BINARY att_relname=BINARY' in sql and '@@lower_case_table_names=0' in sql
    assert "private'table" not in sql and 'private_database' not in sql
    assert parameters == {'table': "private'table", 'schema': 'private_database'}
    if bitwise:
        from sqlalchemy.dialects.mysql import dialect

        assert dtype.bind_processor(dialect())({'UP'}) == 1
        assert dtype.result_processor(dialect(), None)(1) == {'UP'}


@pytest.mark.parametrize('rows', [[], [('flags', None)], [('flags', 'UP'), ('flags', 'OTHER')]])
def test_missing_or_ambiguous_set_domain_fails_after_closing_metadata_result(rows):
    from sqlalchemy.dialects.mysql import SET
    from matrixone._release_compat import restore_set_members

    class Result:
        closed = False

        def all(self):
            return rows

        def close(self):
            self.closed = True

    result = Result()
    c = connection()
    c.execute = lambda *args: result
    original = SET('original')
    columns = [{'name': 'flags', 'type': original}]
    with pytest.raises(QueryError, match='native column domain') as rejected:
        restore_set_members(c, columns, 't', 'db')
    assert rejected.value.phase == 'metadata_reflection' and result.closed
    assert columns[0]['type'] is original


def test_set_metadata_fetch_failure_keeps_primary_when_close_also_fails():
    from sqlalchemy.dialects.mysql import SET
    from matrixone._release_compat import restore_set_members

    primary, cleanup_error = RuntimeError('fetch failed'), RuntimeError('close failed')

    class Result:
        def all(self):
            raise primary

        def close(self):
            raise cleanup_error

    c = connection()
    c.execute = lambda *args: Result()
    with pytest.raises(RuntimeError) as rejected:
        restore_set_members(c, [{'name': 's', 'type': SET('UP')}], 't', 'db')
    assert rejected.value is primary and primary.metadata_cleanup_error is cleanup_error
