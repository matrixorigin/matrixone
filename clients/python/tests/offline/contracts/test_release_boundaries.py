"""Shared public SQL/compiler/error contracts; native release proof is separate."""

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
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
    stage = client.stage._build_create_stage_sql('dash-name', 'file:///tmp/data', comment=value)
    assert "CREATE STAGE `dash-name` URL='file:///tmp/data' COMMENT='O''Reilly 漢字'" == stage
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
    first = live.with_snapshot('first')
    second = live.with_snapshot('second')
    assert first is not live
    assert 'matrixone_snapshot' not in live._annotations
    assert live._generate_cache_key() != first._generate_cache_key() != second._generate_cache_key()
    statement = first.where(a.c.amount == sa.bindparam('amount')).params(amount=Decimal('1.000000000000000001'))
    for compile_sql in (compile_select_to_sql, compile_expression, _query_to_sql):
        sql = compile_sql(statement)
        assert sql.count("{snapshot = 'first'}") == 2
        assert '1.000000000000000001' in sql
    assert 'snapshot' not in compile_select_to_sql(live)
    compiled = statement.compile(dialect=MatrixOneDialect())
    assert compiled.params == {'amount': Decimal('1.000000000000000001')}


@pytest.mark.parametrize('shape', ['subquery', 'cte', 'outer_cte'])
def test_nested_snapshot_statement_rejects_complete_query(shape):
    a, _ = snapshot_tables()
    inner = select(a.c.id).with_snapshot('historical')
    if shape == 'subquery':
        statement = select(inner.subquery())
    elif shape == 'cte':
        statement = select(inner.cte())
    else:
        statement = select(select(a.c.id).cte()).with_snapshot('historical')
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
        assert "{snapshot = 'historical'}" in str(select(a, snapshot='historical').compile(dialect=dialect))
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
    assert "LOWER('Mixed-" in calls[0]
    if object_type == 'TABLE':
        assert "table_schema=LOWER('Db')" in calls[0]
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
