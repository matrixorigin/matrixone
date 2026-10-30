"""Shared public SQL/compiler/error contracts; native release proof is separate."""

import asyncio
import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from matrixone import with_snapshot

import pytest
import pymysql
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError

from matrixone import Client, AsyncClient, select, compile_select_to_sql
from matrixone._query_compilation import compile_expression
from matrixone._sql_literals import identifier_parts, sql_object_name
from matrixone.cdc import BaseCDCManager
from matrixone.account import BaseAccountManager
from matrixone.client import _classify_db_error
from matrixone.clone import CloneManager, AsyncCloneManager
from matrixone.exceptions import CloneError, QueryError
from matrixone.export import _query_to_sql
from matrixone.metadata import MetadataRow
from matrixone.session import Session
from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect
from matrixone.sqlalchemy_ext.fulltext_index import FulltextSearchBuilder as IndexSearch
from matrixone.sqlalchemy_ext.fulltext_search import FulltextQueryBuilder, FulltextSearchBuilder
from scripts.run_contracts import release_target, validate_server_identity


@pytest.fixture
def engine():
    engine = sa.create_engine('sqlite://')
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(params=['pyformat', 'format'])
def mysql_transport(request, monkeypatch):
    """Real compilation/event/driver formatting with a deterministic DBAPI transport."""
    wire = []
    formatter_connection = pymysql.Connection(defer_connect=True)
    formatter_connection.server_status = 0
    formatter = formatter_connection.cursor()

    class Cursor:
        description = (('value', None, None, None, None, None, None),)
        rowcount = 1

        def execute(self, sql, params=None):
            wire.append(formatter.mogrify(sql, params))
            self.rows = [(1,)]

        def fetchone(self):
            return self.rows.pop(0) if self.rows else None

        def fetchall(self):
            result, self.rows = self.rows, []
            return result

        def close(self):
            pass

    class Connection:
        def character_set_name(self):
            return 'utf8mb4'

        def cursor(self):
            return Cursor()

        def rollback(self):
            pass

        def commit(self):
            pass

        def close(self):
            pass

    engine = sa.create_engine('mysql+pymysql://unused@localhost/db', creator=Connection, paramstyle=request.param)
    # Only server introspection is unavailable; execution dispatch remains real.
    engine.dialect.initialize = lambda connection: None
    monkeypatch.setattr(Client, '_detect_backend_version', lambda client: None)
    try:
        yield engine, wire, formatter
    finally:
        engine.dispose()
        formatter_connection.close()


def test_adopted_engine_preserves_legacy_string_execution(mysql_transport):
    engine, wire, _ = mysql_transport

    def check():
        with engine.connect() as connection:
            if sa.__version__.startswith('1.4.'):
                assert connection.execute('SELECT 1').scalar() == 1
                assert wire[-1] == 'SELECT 1'
            else:
                with pytest.raises(sa.exc.ObjectNotExecutableError):
                    connection.execute('SELECT 1')
            assert connection.execute(sa.text('SELECT :value'), {'value': 1}).scalar() == 1
            assert connection.exec_driver_sql('SELECT 1').scalar() == 1

    check()
    Client.from_engine(engine, logger=Mock(), sql_log_mode='off')
    check()


@pytest.mark.parametrize(
    'snapshot',
    [
        'plain',
        'snapshot%name',
        'snapshot%%name',
        'snapshot%s',
        'snapshot%(id)s',
        'snapshot__[POSTCOMPILE_id]',
        'snapshot__[POSTCOMPILE_ids]',
        "quo'te%漢",
    ],
)
@pytest.mark.parametrize('bound', [False, True])
@pytest.mark.parametrize('owner', ['client', 'session'])
def test_snapshot_driver_formatting_preserves_identity(mysql_transport, snapshot, bound, owner):
    engine, wire, _ = mysql_transport
    table = sa.table('t', sa.column('id'))
    other = sa.table('other', sa.column('id'))
    columns = [sa.literal(42).label('projection'), table.c.id] if bound else [table.c.id]
    statement = with_snapshot(select(*columns).select_from(table.join(other, table.c.id == other.c.id)), snapshot)
    if bound:
        statement = statement.where(table.c.id == sa.bindparam('id', 7)).where(
            table.c.id.in_(sa.bindparam('ids', [7, 8], expanding=True))
        )
    cache = {}
    adopted = engine.execution_options(compiled_cache=cache)
    with adopted.connect() as connection:
        assert connection.execute(statement).scalar() == 1
        assert 'snapshot =' not in wire[-1]
    warmed = dict(cache)
    client = Client.from_engine(adopted, logger=Mock(), sql_log_mode='off')
    hint = "{snapshot = '" + snapshot.replace("'", "''") + "'}"

    def check(executor):
        for _ in range(2):
            assert executor.execute(statement).scalar() == 1
            assert wire[-1].count(hint) == 2
            if bound:
                assert 'SELECT 42 AS projection' in wire[-1]
                assert 't.id = 7' in wire[-1]
                assert 't.id IN (7, 8)' in wire[-1]

    if owner == 'client':
        check(client)
    else:
        with client.session() as session:
            check(session)
    assert all(cache[key] is value for key, value in warmed.items())
    assert statement._annotations['matrixone_snapshot'] == snapshot


@pytest.mark.parametrize(
    'snapshot',
    ['snapshot%name', 'snapshot%%name', 'snapshot%s', 'snapshot%(id)s', 'snapshot__[POSTCOMPILE_id]', "quo'te%漢"],
)
def test_snapshot_raw_export_sql_keeps_original_percents(mysql_transport, snapshot):
    engine, wire, formatter = mysql_transport
    client = Client.from_engine(engine, logger=Mock(), sql_log_mode='off')
    table = sa.table('t', sa.column('id'))
    statement = with_snapshot(select(table.c.id).where(table.c.id == 7), snapshot)
    hint = "{snapshot = '" + snapshot.replace("'", "''") + "'}"
    for sql in (
        compile_select_to_sql(statement),
        compile_select_to_sql(statement, engine),
        compile_expression(statement),
        _query_to_sql(statement),
    ):
        assert hint in sql
        assert client.execute(sql).scalar() == 1
        assert wire[-1] == sql
    literal = statement.compile(dialect=engine.dialect, compile_kwargs={'literal_binds': True})
    arguments = literal.params if engine.dialect.paramstyle == 'pyformat' else ()
    assert formatter.mogrify(str(literal), arguments) == compile_select_to_sql(statement)


@pytest.mark.parametrize(
    'parameter',
    [
        'matrixone_snapshot_1',
        '_matrixone_snapshot_0_value',
        '_matrixone_snapshot.0.value',
        '_matrixone_snapshot 0 value',
    ],
)
@pytest.mark.parametrize('expanding', [False, True])
def test_snapshot_annotation_isolated_from_caller_bind_namespace(mysql_transport, parameter, expanding):
    engine, wire, _ = mysql_transport
    client = Client.from_engine(engine.execution_options(compiled_cache={}), logger=Mock(), sql_log_mode='off')
    table = sa.table('t', sa.column('id'))
    value = [7, 8] if expanding else 7
    bind = sa.bindparam(parameter, [5, 6] if expanding else 5, expanding=expanding)
    predicate = table.c.id.in_(bind) if expanding else table.c.id == bind
    live = select(table.c.id).where(predicate)
    assert client.execute(live).scalar() == 1
    assert ('t.id IN (5, 6)' if expanding else 't.id = 5') in wire[-1]
    snapshot = with_snapshot(live, 'expected_history')
    supplied = {parameter: value}
    supplied.update(
        {
            f'_matrixone_snapshot_{index}_value': 'wrong_history'
            for index in range(4)
            if f'_matrixone_snapshot_{index}_value' != parameter
        }
    )
    supplied['matrixone_snapshot_1'] = value if parameter == 'matrixone_snapshot_1' else 'wrong_history'
    original = dict(supplied)
    expected_predicate = 't.id IN (7, 8)' if expanding else 't.id = 7'
    for _ in range(2):
        assert client.execute(snapshot, supplied).scalar() == 1
        assert "{snapshot = 'expected_history'}" in wire[-1]
        assert expected_predicate in wire[-1]
    # SQLAlchemy 2.1 also collects .params() separately from execution arguments.
    derived = snapshot.params(supplied)
    assert client.execute(derived).scalar() == 1
    assert "{snapshot = 'expected_history'}" in wire[-1]
    raw = compile_select_to_sql(derived)
    assert "{snapshot = 'expected_history'}" in raw and expected_predicate in raw
    assert supplied == original


def test_snapshot_binding_avoids_multiple_occupied_caller_names(mysql_transport):
    engine, wire, _ = mysql_transport
    client = Client.from_engine(engine.execution_options(compiled_cache={}), logger=Mock(), sql_log_mode='off')
    table = sa.table('t', sa.column('id'))
    statement = with_snapshot(
        select(table.c.id)
        .where(table.c.id >= sa.bindparam('_matrixone_snapshot_0_value', 3))
        .where(table.c.id <= sa.bindparam('_matrixone_snapshot_1_value', 5)),
        'expected_history',
    )
    for lower, upper in ((7, 9), (8, 10)):
        parameters = {
            '_matrixone_snapshot_0_value': lower,
            '_matrixone_snapshot_1_value': upper,
            '_matrixone_snapshot_2_value': 'wrong_history',
        }
        original = dict(parameters)
        assert client.execute(statement, parameters).scalar() == 1
        assert "{snapshot = 'expected_history'}" in wire[-1]
        assert 't.id >= ' + str(lower) in wire[-1]
        assert 't.id <= ' + str(upper) in wire[-1]
        assert parameters == original


@pytest.mark.parametrize('snapshot', [None, 'historical'])
@pytest.mark.parametrize('expanding', [False, True])
def test_raw_compilation_preserves_escaped_bind_processors(mysql_transport, snapshot, expanding):
    engine, wire, _ = mysql_transport
    client = Client.from_engine(engine, logger=Mock(), sql_log_mode='off')

    class OffsetInteger(sa.TypeDecorator):
        impl = sa.Integer
        cache_ok = True

        def process_bind_param(self, value, dialect):
            return value + 1

    table = sa.table('t', sa.column('id'))
    bind = sa.bindparam('ordinary name', [7, 8] if expanding else 7, type_=OffsetInteger(), expanding=expanding)
    statement = select(table.c.id).where(table.c.id.in_(bind) if expanding else table.c.id == bind)
    if snapshot is not None:
        statement = with_snapshot(statement, snapshot)
    assert client.execute(statement).scalar() == 1
    expected = 't.id IN (8, 9)' if expanding else 't.id = 8'
    assert expected in wire[-1]
    assert expected in compile_select_to_sql(statement)


@pytest.mark.parametrize(
    'value,expected',
    [
        (True, True),
        (False, False),
        (1, True),
        (0, False),
        ('TRUE', True),
        ('false', False),
        ('1', True),
        ('0', False),
        (None, False),
    ],
)
def test_metadata_hidden_preserves_driver_value(engine, value, expected):
    with engine.connect() as connection:
        row = connection.execute(
            sa.text(
                "SELECT 'v' col_name,'object' object_name,:hidden is_hidden,'' obj_loc,"
                "'' create_ts,'' delete_ts,1 rows_cnt,0 null_cnt,1 compress_size,2 origin_size"
            ),
            {'hidden': value},
        ).first()
        assert MetadataRow.from_sqlalchemy_row(row).is_hidden is expected


@pytest.mark.parametrize('value', [2, -1, 'unknown', b'1'])
def test_metadata_hidden_rejects_unknown_protocol_values(engine, value):
    with engine.connect() as connection:
        row = connection.execute(
            sa.text(
                "SELECT 'v' col_name,'object' object_name,:hidden is_hidden,'' obj_loc,"
                "'' create_ts,'' delete_ts,1 rows_cnt,0 null_cnt,1 compress_size,2 origin_size"
            ),
            {'hidden': value},
        ).first()
        with pytest.raises(ValueError, match='boolean representation'):
            MetadataRow.from_sqlalchemy_row(row)


@pytest.mark.parametrize(
    'name,qualified,parts',
    [
        ('plain', False, ('plain',)),
        ('dash-name', False, ('dash-name',)),
        ('`a``b`', False, ('a`b',)),
        ('"Mixed"', False, ('Mixed',)),
        ('db.`a.b`', True, ('db', 'a.b')),
        ('`d.b`.`a``b`', True, ('d.b', 'a`b')),
    ],
)
def test_manager_names_round_trip_complete_components(name, qualified, parts):
    assert identifier_parts(name, qualified) == parts
    assert identifier_parts(sql_object_name(name, qualified), qualified) == parts


@pytest.mark.parametrize('name', ['`a` trailing', '`a`; DROP DATABASE other', '`unterminated', '', 'x' * 65])
def test_manager_names_reject_incomplete_or_trailing_sql(name):
    with pytest.raises(ValueError):
        sql_object_name(name)


def test_admin_string_authority_and_consumers(engine):
    client = Client(logger=Mock(), sql_log_mode='off')
    value = "O'Reilly 漢字"
    with engine.connect() as connection:
        assert connection.exec_driver_sql('SELECT ' + client._escape_string(value)).scalar() == value
    assert client._escape_identifier('plain') == '`plain`'
    stage = client.stage._build_create_stage_sql("quoted 'name' {space}", 'file:///tmp/data', comment=value)
    assert "CREATE STAGE `quoted 'name' {space}`" in stage
    assert "COMMENT='O''Reilly 漢字'" in stage
    user = BaseAccountManager(client)._build_create_user_sql('someone', value)
    assert "IDENTIFIED BY 'O''Reilly 漢字'" in user
    cdc = BaseCDCManager(client)
    sql = cdc._build_create_sql('cdc-name', 'mysql://source', 'mysql', "mysql://O'Reilly", 'db.t:db.t')
    assert "CREATE CDC `cdc-name` 'mysql://source' 'mysql' 'mysql://O''Reilly'" in sql
    assert "task_name = 'O''Reilly'" in cdc._build_select_tasks_sql("O'Reilly")


@pytest.mark.parametrize('value', ['a\\b', 'a\0b'])
def test_admin_grammar_rejects_mode_dependent_values_before_io(value):
    client = Client(logger=Mock(), sql_log_mode='off')
    with pytest.raises(ValueError, match='Administrative SQL string tokens'):
        client._escape_string(value)
    with pytest.raises(ValueError, match='Stage string tokens'):
        client.stage._build_create_stage_sql('plain', 'file:///tmp/data', comment=value)


def snapshot_tables():
    metadata = sa.MetaData()
    a = sa.Table('a', metadata, sa.Column('id', sa.Integer), sa.Column('amount', sa.Numeric), schema='db')
    b = sa.Table('b', metadata, sa.Column('ref', sa.Integer), schema='db')
    return a, b


def test_snapshot_select_is_generative_and_cache_keyed_across_consumers():
    a, b = snapshot_tables()
    live = select(a.c.id).select_from(a.join(b, a.c.id == b.c.ref))
    first = with_snapshot(live, 'first')
    second = with_snapshot(live, 'second')
    assert first is not live
    assert 'matrixone_snapshot' not in live._annotations
    assert live._generate_cache_key() != first._generate_cache_key() != second._generate_cache_key()
    statement = first.where(a.c.amount == sa.bindparam('amount')).params(amount=Decimal('1.000000000000000001'))
    for compile_sql in (compile_select_to_sql, compile_expression, _query_to_sql):
        sql = compile_sql(statement)
        assert sql.count("{snapshot = 'first'}") == 2
        assert '1.000000000000000001' in sql
    assert 'snapshot' not in compile_select_to_sql(live)
    compiled = statement.compile(dialect=MatrixOneDialect(), compile_kwargs={'render_postcompile': True})
    expanded = getattr(compiled, '_post_compile_expanded_state', None)
    parameters = expanded.parameters if expanded is not None else compiled.params
    assert parameters['amount'] == Decimal('1.000000000000000001')
    # SQLAlchemy 1.4 retains the consumed literal-execute value in introspection;
    # real driver assertions above verify the actual remaining wire parameters.
    assert all(value == 'first' for key, value in parameters.items() if key != 'amount')


@pytest.mark.parametrize('shape', ['subquery', 'cte', 'outer_cte'])
def test_nested_snapshot_statement_rejects_complete_query(shape):
    a, _ = snapshot_tables()
    inner = with_snapshot(select(a.c.id), 'historical')
    if shape == 'subquery':
        statement = select(inner.subquery())
    elif shape == 'cte':
        statement = select(inner.cte())
    else:
        statement = with_snapshot(select(select(a.c.id).cte()), 'historical')
    with pytest.raises(ValueError, match='Snapshot|snapshot'):
        compile_select_to_sql(statement)


def test_snapshot_foreign_engine_and_invalid_name_are_explicit(engine):
    a, _ = snapshot_tables()
    assert 'db.a' in compile_select_to_sql(select(a), engine)
    with pytest.raises(ValueError, match='MatrixOne engine'):
        compile_select_to_sql(select(a, snapshot='historical'), engine)
    for invalid in ('', 'a\\b', 'a\0b', 0):
        with pytest.raises(ValueError):
            select(a, snapshot=invalid)


@pytest.mark.parametrize('entry', ['query', 'client', 'index'])
@pytest.mark.parametrize('limit', [None, 0, 2])
def test_fulltext_pagination_preserves_explicit_zero(entry, limit):
    if entry == 'query':
        sql = FulltextQueryBuilder().must('hello').as_sql('docs', ['body'], limit=limit)
    elif entry == 'client':
        builder = FulltextSearchBuilder(None).table('docs').columns(['body']).query('hello')
        if limit is not None:
            builder.limit(limit)
        sql = builder.explain()
    else:
        builder = IndexSearch('docs', ['body']).search('hello')
        if limit is not None:
            builder.limit(limit)
        sql = builder.build_sql()
    assert ('LIMIT' not in sql) if limit is None else sql.endswith('LIMIT ' + str(limit))


def test_classified_query_error_retains_exact_source_without_double_wrapping():
    source = DBAPIError('SELECT missing', (), Exception(1146, 'missing table'))
    error = _classify_db_error(source)
    assert error.orig is source
    assert error.errno == 1146
    assert _classify_db_error(error) is error


@pytest.mark.parametrize('entry', ['client', 'client_session', 'exported_session'])
def test_public_query_error_chain_and_reuse(engine, entry):
    client = Client(logger=Mock(), sql_log_mode='off')
    client._engine = engine
    if entry == 'client':
        with pytest.raises(QueryError) as raised:
            client.execute('SELECT * FROM absent_boundary_table')
        assert client.execute('SELECT 42').scalar() == 42
    else:
        session = client.session() if entry == 'client_session' else Session(bind=engine, client=client)
        with session as executor:
            with pytest.raises(QueryError) as raised:
                executor.execute('SELECT * FROM absent_boundary_table')
            assert executor.execute('SELECT 42').scalar() == 42
    assert isinstance(raised.value.orig, sa.exc.SQLAlchemyError)
    assert raised.value.__cause__ is raised.value.orig


@pytest.mark.parametrize('async_mode', [False, True])
@pytest.mark.asyncio
async def test_clone_acknowledgement_requires_target_readback(async_mode):
    client = Client(logger=Mock(), sql_log_mode='off')
    client._engine = object()
    commands = []
    failure = QueryError('native target does not exist')

    def execute(sql):
        commands.append(sql)
        if sql.startswith('SHOW CREATE'):
            raise failure
        return SimpleNamespace(rows=[])

    async def execute_async(sql):
        return execute(sql)

    executor = SimpleNamespace(execute=execute_async if async_mode else execute)
    manager = (AsyncCloneManager if async_mode else CloneManager)(client, executor=executor)
    with pytest.raises(CloneError, match='acknowledged') as raised:
        if async_mode:
            await manager.clone_table('db.target', 'db.source')
        else:
            manager.clone_table('db.target', 'db.source')
    assert commands == ['CREATE TABLE db.target CLONE db.source', 'SHOW CREATE TABLE db.target']
    assert raised.value.__cause__ is failure


@pytest.mark.asyncio
async def test_clone_readback_cancellation_remains_cancellation():
    client = Client(logger=Mock(), sql_log_mode='off')
    client._engine = object()
    calls = []

    async def execute(sql):
        calls.append(sql)
        if sql.startswith('SHOW CREATE'):
            raise asyncio.CancelledError()
        return SimpleNamespace(rows=[])

    manager = AsyncCloneManager(client, executor=SimpleNamespace(execute=execute))
    with pytest.raises(asyncio.CancelledError):
        await manager.clone_table('target', 'source')
    assert calls == ['CREATE TABLE target CLONE source', 'SHOW CREATE TABLE target']


def test_buffered_client_distinguishes_orm_rows_from_dml(engine):
    from sqlalchemy.orm import declarative_base

    base = declarative_base()

    class Item(base):
        __tablename__ = 'buffered_item'
        id = sa.Column(sa.Integer, primary_key=True)
        value = sa.Column(sa.String(20))

    base.metadata.create_all(engine)
    client = Client(logger=Mock(), sql_log_mode='off')
    client._engine = engine
    client.execute(sa.insert(Item).values(id=1, value='first'))
    result = client.execute(select(Item.id, Item.value))
    assert result.columns == ['id', 'value']
    assert [tuple(row) for row in result.rows] == [(1, 'first')]
    assert client.execute(sa.update(Item).values(value='changed')).affected_rows == 1
    entity = client.execute(select(Item)).rows[0][0]
    assert isinstance(entity, Item) and entity.value == 'changed'
    assert client.execute(sa.delete(Item)).affected_rows == 1
    assert client.execute(select(Item.id)).rows == []


@pytest.mark.asyncio
async def test_async_driver_adoption_keeps_owner_and_compiles_snapshots():
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine('mysql+aiomysql://unused@127.0.0.1:1/unused')
    dialect, pool, dbapi = engine.dialect, engine.pool, engine.dialect.dbapi
    try:
        client = AsyncClient.from_engine(engine, logger=Mock(), sql_log_mode='off')
        assert client._engine is engine
        assert engine.dialect is dialect and engine.pool is pool and engine.dialect.dbapi is dbapi
        assert isinstance(dialect, MatrixOneDialect)
        assert dialect.supports_native_decimal
        a, _ = snapshot_tables()
        assert "{snapshot = 'historical'}" in str(
            select(a, snapshot='historical').compile(dialect=dialect, compile_kwargs={'render_postcompile': True})
        )
    finally:
        await engine.dispose()


@pytest.mark.parametrize('version', ['4.1.0', '4.1.6', '4.2.0', '4.2.6'])
def test_release_target_requires_exact_observed_identity(version):
    assert release_target(version) == version
    validate_server_identity('8.0.30-MatrixOne-v' + version, 'gitsha', version)
    with pytest.raises(ValueError, match='differs'):
        validate_server_identity('8.0.30-MatrixOne-v' + version + '0', 'gitsha', version)
    with pytest.raises(ValueError, match='revision'):
        validate_server_identity('8.0.30-MatrixOne-v' + version, 'wrong', version, 'expected')


def test_source_identity_requires_revision_and_banner():
    validate_server_identity('8.0.30-MatrixOne-v20261026', 'abcdef', 'v20261026', 'abc')
    with pytest.raises(ValueError, match='Source identity'):
        validate_server_identity('8.0.30-MatrixOne-v20261026', 'abcdef', 'v20261026')


def test_full_server_contracts_require_explicit_disposable_opt_in(tmp_path):
    env = dict(os.environ)
    env.pop('MO_SDK_DISPOSABLE_SERVER', None)
    result = subprocess.run(
        [sys.executable, 'scripts/run_contracts.py', '--server', '4.1.0', '--output', str(tmp_path)],
        cwd=str(Path(__file__).resolve().parents[3]),
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 2
    assert 'MO_SDK_DISPOSABLE_SERVER=1' in result.stderr


@pytest.mark.parametrize('async_mode', [False, True])
@pytest.mark.parametrize('existing', [False, True])
@pytest.mark.parametrize('object_type,target', [('TABLE', '`Db`.`Mixed-Target`'), ('DATABASE', '`Mixed-Db`')])
@pytest.mark.asyncio
async def test_clone_noop_or_plain_write_uses_same_executor(async_mode, existing, object_type, target):
    client = Client(logger=Mock(), sql_log_mode='off')
    client._engine = object()
    calls = []

    def execute(sql):
        calls.append(sql)
        return SimpleNamespace(rows=[(1,)] if existing or sql.startswith('SHOW') else [])

    async def execute_async(sql):
        return execute(sql)

    manager = (AsyncCloneManager if async_mode else CloneManager)(
        client, executor=SimpleNamespace(execute=execute_async if async_mode else execute)
    )
    result = manager._clone(object_type, target, 'missing_source', None, True)
    if async_mode:
        await result
    assert len(calls) == (1 if existing else 3)
    assert calls[0].startswith('SELECT 1 FROM information_schema.')
    assert "@@lower_case_table_names=0 THEN 'Mixed-" in calls[0]
    if object_type == 'TABLE':
        assert "@@lower_case_table_names=0 THEN 'Db' ELSE LOWER('Db')" in calls[0]
    if not existing:
        assert calls[1].startswith('CREATE ' + object_type + ' ')
        assert calls[2].startswith('SHOW CREATE ' + object_type + ' ')
    assert not any('IF NOT EXISTS' in sql for sql in calls)


@pytest.mark.parametrize('phase', ['observation', 'write'])
@pytest.mark.parametrize('async_mode', [False, True])
@pytest.mark.asyncio
async def test_clone_observation_and_race_errors_preserve_owner(phase, async_mode):
    client = Client(logger=Mock(), sql_log_mode='off')
    client._engine = object()
    calls = []
    error = QueryError('catalog unavailable' if phase == 'observation' else 'duplicate target')

    def execute(sql):
        calls.append(sql)
        if phase == 'observation' or sql.startswith('CREATE'):
            raise error
        return SimpleNamespace(rows=[])

    async def execute_async(sql):
        return execute(sql)

    manager = (AsyncCloneManager if async_mode else CloneManager)(
        client, executor=SimpleNamespace(execute=execute_async if async_mode else execute)
    )
    with pytest.raises(CloneError) as raised:
        result = manager.clone_table('target', 'source', if_not_exists=True)
        if async_mode:
            await result
    assert raised.value.__cause__ is error
    assert len(calls) == (1 if phase == 'observation' else 2)
    assert not any('IF NOT EXISTS' in sql or 'DROP' in sql or 'ROLLBACK' in sql for sql in calls)


@pytest.mark.asyncio
async def test_clone_one_deadline_drains_cancelled_verification(monkeypatch):
    import matrixone.clone as module

    client = Client(logger=Mock(), sql_log_mode='off', query_timeout=10)
    client._engine = object()
    entered, released = asyncio.Event(), asyncio.Event()
    calls, budgets = [], []

    async def execute(sql):
        calls.append(sql)
        if sql.startswith('SHOW'):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                released.set()
        return SimpleNamespace(rows=[])

    real_wait_for = asyncio.wait_for

    async def controlled_budget(operation, timeout):
        assert calls == []  # The budget starts before even the catalog observation.
        budgets.append(timeout)
        task = asyncio.create_task(operation)
        await real_wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        raise asyncio.TimeoutError()

    monkeypatch.setattr(module.asyncio, 'wait_for', controlled_budget)
    manager = AsyncCloneManager(client, executor=SimpleNamespace(execute=execute))
    with pytest.raises(asyncio.TimeoutError):
        await manager.clone_table('target', 'source', if_not_exists=True)
    assert budgets == [10] and released.is_set() and len(calls) == 3


def test_compiler_memo_preserves_generative_parent_sql():
    from matrixone.sqlalchemy_ext.dialect import _prepare_compiler_statement

    statement = sa.select(sa.literal(1).label('value'))
    prepared = _prepare_compiler_statement(None, statement, (), {}, {})[0]
    assert _prepare_compiler_statement(None, statement, (), {}, {})[0] is prepared
    assert _prepare_compiler_statement(None, prepared, (), {}, {})[0] is prepared
    derived = statement.where(sa.literal(2) == 3)
    changed = _prepare_compiler_statement(None, derived, (), {}, {})[0]
    assert changed is not prepared
    assert 'WHERE' in str(changed) and 'WHERE' not in str(prepared)
    assert prepared._generate_cache_key() != statement._generate_cache_key()
    assert getattr(derived, '_matrixone_compiler_prepared')[0] is derived
    assert '_matrixone_compiler_prepared' not in changed.__dict__


@pytest.mark.parametrize('async_mode', [False, True])
@pytest.mark.parametrize('object_type', ['table', 'database'])
@pytest.mark.asyncio
async def test_clone_snapshot_convenience_stays_on_executor(async_mode, object_type):
    client = Client(logger=Mock(), sql_log_mode='off')
    client._engine = object()
    client._snapshots = SimpleNamespace(
        exists=Mock(side_effect=AssertionError('must not acquire another pool connection'))
    )
    calls = []

    def execute(sql):
        calls.append(sql)
        return SimpleNamespace(rows=[(1,)])

    async def execute_async(sql):
        return execute(sql)

    manager = (AsyncCloneManager if async_mode else CloneManager)(
        client, executor=SimpleNamespace(execute=execute_async if async_mode else execute)
    )
    # Preserve database public version gating while isolating the execution owner.
    client.set_backend_version('4.1.0')
    method = getattr(manager, 'clone_' + object_type + '_with_snapshot')
    result = method('target', 'missing_source', 'missing_snapshot', if_not_exists=True)
    if async_mode:
        await result
    assert len(calls) == 1 and calls[0].startswith('SELECT 1 FROM information_schema.')
    client.snapshots.exists.assert_not_called()


@pytest.mark.parametrize('async_mode', [False, True])
@pytest.mark.asyncio
async def test_moctl_public_facades_share_literal_and_source(async_mode):
    from matrixone.async_client import AsyncMoCtlManager
    from matrixone.moctl import MoCtlManager, MoCtlError
    from matrixone.exceptions import MoCtlError as AsyncMoCtlError

    client = SimpleNamespace()
    calls = []

    def execute(sql):
        calls.append(sql)
        return SimpleNamespace(rows=[('{"result":[{"returnStr":"OK"}]}',)])

    async def execute_async(sql):
        return execute(sql)

    client.execute = execute_async if async_mode else execute
    manager = (AsyncMoCtlManager if async_mode else MoCtlManager)(client)
    result = manager._execute_moctl('CN', 'FLUSH', "db.o'reilly")
    if async_mode:
        result = await result
    assert result['result'][0]['returnStr'] == 'OK'
    assert calls == ["SELECT mo_ctl('CN', 'FLUSH', 'db.o''reilly')"]

    source = QueryError('native failure')

    def fail(sql):
        raise source

    async def fail_async(sql):
        return fail(sql)

    client.execute = fail_async if async_mode else fail
    with pytest.raises(AsyncMoCtlError if async_mode else MoCtlError) as raised:
        result = manager._execute_moctl('CN', 'FLUSH', 'db.t')
        if async_mode:
            await result
    assert raised.value.__cause__ is source


@pytest.mark.parametrize('owner', ['client', 'session', 'legacy_session'])
def test_explicit_driver_binding_uses_existing_owner_without_sdk_rewriting(mysql_transport, owner):
    engine, wire, formatter = mysql_transport
    client = Client.from_engine(engine, logger=Mock(), sql_log_mode='off')
    sql = 'SELECT %s AS value'
    params = ("literal ? :name %(other)s 100% O'Reilly",)
    expected = formatter.mogrify(sql, params)
    if owner == 'client':
        assert client.execute_driver(sql, params).scalar() == 1
        assert wire[-1] == expected
        assert client.execute_driver("SELECT '100%' AS value").scalar() == 1
        assert wire[-1] == "SELECT '100%' AS value"
    else:
        from matrixone.client import Session as LegacySession

        session_type = Session if owner == 'session' else LegacySession
        with session_type(bind=engine, client=client) as session:
            assert session.execute_driver(sql, params).scalar() == 1
            assert wire[-1] == expected
            assert session.execute_driver("SELECT '100%' AS value").scalar() == 1
            assert wire[-1] == "SELECT '100%' AS value"
    # Native-driver opt-in must not change existing SDK parameter semantics.
    assert client.execute('SELECT ? AS value', ("marker %s :name",)).scalar() == 1
    assert wire[-1] == "SELECT 'marker %s :name' AS value"


@pytest.mark.parametrize('kind', ['ivfflat', 'hnsw', 'generic'])
@pytest.mark.parametrize('ddl_kind', ['standard', 'vector'])
def test_vector_ddl_uses_driver_and_schema_owners(mysql_transport, kind, ddl_kind):
    from matrixone.sqlalchemy_ext.vector_index import IVFVectorIndex, HnswVectorIndex, VectorIndex, CreateVectorIndex

    engine, wire, _ = mysql_transport
    Client.from_engine(engine, logger=Mock(), sql_log_mode='off')
    table = sa.Table('vectors%', sa.MetaData(), sa.Column('fulltext2', sa.Integer), schema='logical')
    index = {'ivfflat': IVFVectorIndex, 'hnsw': HnswVectorIndex, 'generic': VectorIndex}[kind](
        'ix`%(id)s', table.c.fulltext2
    )
    original = (table.name, table.schema, index.name, engine.dialect.identifier_preparer)
    schema_formatter = engine.dialect.identifier_preparer.quote_schema
    ddl = (
        sa.schema.CreateIndex(index, if_not_exists=True)
        if ddl_kind == 'standard'
        else CreateVectorIndex(index, if_not_exists=True)
    )
    with engine.connect().execution_options(schema_translate_map={'logical': 'tenant%s'}) as connection:
        connection.execute(ddl)
    algorithm = 'hnsw' if kind == 'hnsw' else 'ivfflat'
    assert wire[-1].startswith(
        'CREATE INDEX IF NOT EXISTS `ix``%(id)s` USING ' + algorithm + ' ON `tenant%s`.`vectors%`(`fulltext2`)'
    )
    assert table.name is original[0] and table.schema is original[1] and index.name is original[2]
    assert engine.dialect.identifier_preparer is original[3]
    assert engine.dialect.identifier_preparer.quote_schema == schema_formatter


@pytest.mark.parametrize(
    'name,token',
    [
        ('ordinary', '`ordinary`'),
        ('ix%name', '`ix%name`'),
        ('ix%s', '`ix%s`'),
        ('ix%%name', '`ix%%name`'),
        ('ix%(id)s', '`ix%(id)s`'),
        ('ix`name', '`ix``name`'),
        ('`ix%s`', '`ix%s`'),
        (sa.sql.quoted_name('`ix%s`', True), '```ix%s```'),
        ('中文.name%', '`中文.name%`'),
    ],
)
def test_vector_ddl_percent_names_match_standalone_wire_identity(mysql_transport, name, token):
    from matrixone.sqlalchemy_ext.vector_index import IVFVectorIndex

    engine, wire, _ = mysql_transport
    table = sa.Table('vectors', sa.MetaData(), sa.Column('v', sa.Integer), schema='physical')
    index = IVFVectorIndex(name, table.c.v, lists=1)
    expected = 'CREATE INDEX ' + token + " USING ivfflat ON `physical`.`vectors`(`v`) lists = 1 op_type 'vector_l2_ops'"
    with engine.connect() as connection:
        connection.execute(sa.schema.CreateIndex(index))
    assert wire[-1] == expected
    assert index.create_sql('physical.vectors') == expected
    assert index.create(engine, 'physical.vectors') is True
    assert wire[-1] == expected
    assert index.drop(engine, 'physical.vectors') is True
    assert wire[-1] == 'DROP INDEX ' + token + ' ON `physical`.`vectors`'


@pytest.mark.parametrize(
    'schema,mapping,prefix',
    [
        ('logical', {'logical': 'tenant_actual'}, '`tenant_actual`.'),
        ('fulltext2', None, '`fulltext2`.'),
        ('logical', {'logical': 'fulltext2'}, '`fulltext2`.'),
        ('logical', {'logical': None}, '`default%s`.'),
        (None, {None: 'tenant_actual'}, '`tenant_actual`.'),
        (None, {None: None}, '`default%s`.'),
        (None, None, ''),
        ('plain', None, '`plain`.'),
        ('logical%src', {'logical%src': 'tenant%%actual'}, '`tenant%%actual`.'),
        (sa.sql.quoted_name('schema%quoted', True), None, '`schema%quoted`.'),
        (sa.sql.quoted_name('literal', False), None, 'literal.'),
        ('logical', {'logical': sa.sql.quoted_name('tenant_actual', False)}, 'tenant_actual.'),
    ],
)
def test_vector_ddl_preserves_schema_translation_semantics(mysql_transport, schema, mapping, prefix):
    from matrixone.sqlalchemy_ext.vector_index import VectorIndex, CreateVectorIndex

    engine, wire, _ = mysql_transport
    engine.dialect.default_schema_name = 'default%s'
    table = sa.Table('vectors%', sa.MetaData(), sa.Column('v%', sa.Integer), schema=schema)
    index = VectorIndex('ix%s', table.c['v%'])
    original = (table.schema, table.name, index.name)
    options = {'schema_translate_map': dict(mapping)} if mapping is not None else {}
    with engine.connect().execution_options(**options) as connection:
        connection.execute(CreateVectorIndex(index))
    assert ' ON ' + prefix + '`vectors%`(`v%`)' in wire[-1]
    assert 'CREATE INDEX `ix%s` USING ivfflat' in wire[-1]
    assert table.schema is original[0] and table.name is original[1] and index.name is original[2]


def test_vector_ddl_schema_mapping_reuse_and_other_index_dispatch(mysql_transport):
    from matrixone.sqlalchemy_ext.vector_index import IVFVectorIndex

    engine, wire, _ = mysql_transport
    table = sa.Table('vectors', sa.MetaData(), sa.Column('v', sa.Integer), schema='logical')
    index = IVFVectorIndex('ix%s', table.c.v)
    statement = sa.schema.CreateIndex(index)
    for physical in ('tenant%s', 'fulltext2'):
        with engine.connect().execution_options(schema_translate_map={'logical': physical}) as connection:
            connection.execute(statement)
        assert ' ON `' + physical + '`.`vectors`(`v`)' in wire[-1]
    ordinary = sa.Index('plain%index', table.c.v)
    with engine.connect().execution_options(schema_translate_map={'logical': 'tenant_actual'}) as connection:
        connection.execute(sa.schema.CreateIndex(ordinary))
    assert 'plain%index' in wire[-1] and 'tenant_actual' in wire[-1] and 'USING ivfflat' not in wire[-1]


def test_vector_ddl_respects_dialect_omit_schema(mysql_transport):
    from matrixone.sqlalchemy_ext.vector_index import IVFVectorIndex

    engine, wire, _ = mysql_transport
    table = sa.Table('vectors', sa.MetaData(), sa.Column('v', sa.Integer), schema='logical')
    index = IVFVectorIndex('ix', table.c.v)
    engine.dialect.identifier_preparer.omit_schema = True
    with engine.connect().execution_options(schema_translate_map={'logical': 'tenant_actual'}) as connection:
        connection.execute(sa.schema.CreateIndex(index))
    assert ' ON `vectors`(`v`)' in wire[-1] and 'tenant_actual' not in wire[-1]


def test_vector_ddl_missing_default_schema_fails_before_transport(mysql_transport):
    from matrixone.sqlalchemy_ext.vector_index import IVFVectorIndex

    engine, wire, _ = mysql_transport
    engine.dialect.default_schema_name = None
    table = sa.Table('vectors', sa.MetaData(), sa.Column('v', sa.Integer), schema='logical')
    index = IVFVectorIndex('ix', table.c.v)
    with engine.connect().execution_options(schema_translate_map={'logical': None}) as connection:
        wire.clear()
        with pytest.raises((sa.exc.CompileError, sa.exc.StatementError), match='default schema name') as caught:
            connection.execute(sa.schema.CreateIndex(index))
    error = caught.value.orig if isinstance(caught.value, sa.exc.StatementError) else caught.value
    assert isinstance(error, sa.exc.CompileError)
    assert wire == []
