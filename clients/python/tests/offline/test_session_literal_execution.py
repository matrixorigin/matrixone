# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import asyncio
from contextlib import contextmanager, nullcontext
from unittest.mock import Mock

import aiomysql
import pymysql
import pytest
import pytest_asyncio
import sqlalchemy
from sqlalchemy import Column, Integer, String, bindparam, create_engine, event, literal, select, text
from sqlalchemy.exc import IntegrityError, StatementError
from sqlalchemy.ext.asyncio import AsyncSession as ParentAsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import Session as ParentSession
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import QueuePool
from sqlalchemy.types import TypeDecorator

from matrixone import Client
from matrixone._session_execution import _LiteralSQL
from matrixone.async_client import AsyncClient
from matrixone.client import Session as ClientSession
from matrixone.exceptions import QueryError
from matrixone.session import Session as ExportedSession
from matrixone.vector_manager import AsyncVectorManager, VectorManager

Base = declarative_base()


class Row(Base):
    __tablename__ = 'rows'
    id = Column(Integer, primary_key=True)
    embedding = Column(String)
    label = Column(String)


LABELS = ['tag', ':tag', 'prefix :tag', '{"a":1,"b":":tag"}', '% %% %s %(tag)s', "it's 雪", r'\:tag']


def data():
    return [{'id': i, 'embedding': [i, i + 1], 'label': label} for i, label in enumerate(LABELS)]


@pytest.fixture
def client(tmp_path):
    c = Client(logger=Mock())
    c._engine = create_engine('sqlite:///' + str(tmp_path / 'sync.sqlite'))
    Base.metadata.create_all(c._engine)
    try:
        yield c
    finally:
        c.disconnect()


@pytest_asyncio.fixture
async def async_client(tmp_path):
    c = AsyncClient(logger=Mock())
    c._engine = create_async_engine('sqlite+aiosqlite:///' + str(tmp_path / 'async.sqlite'))
    async with c._engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        yield c
    finally:
        await c.disconnect()


@contextmanager
def session_for(client, cls):
    if cls is ClientSession:
        with client.session() as session:
            yield session
        return
    with cls(bind=client._engine, client=client) as session:
        with session.begin():
            yield session


def pending_sql(row_id, operation):
    if operation == 'select':
        return f'SELECT count(*) FROM rows WHERE id={row_id}'
    return f"UPDATE rows SET label='new' WHERE id={row_id}"


def pending_observation(result, operation):
    return result.scalar() if operation == 'select' else result.rowcount


def expected_pending(operation, mode):
    flushes = tuple(map(int, sqlalchemy.__version__.split('.')[:2])) >= (2, 1) and mode == 'enabled'
    return (int(flushes), 'new' if flushes and operation == 'update' else 'old')


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
@pytest.mark.parametrize('operation', ['select', 'update'])
@pytest.mark.parametrize('mode', ['enabled', 'disabled', 'context'])
def test_pending_orm_string_matches_parent(client, cls, operation, mode):
    outcomes = []
    for row_id, raw in enumerate([False, True], start=1):
        with session_for(client, cls) as session:
            session.autoflush = mode != 'disabled'
            session.add(Row(id=row_id, label='old'))
            with session.no_autoflush if mode == 'context' else nullcontext():
                sql = pending_sql(row_id, operation)
                observed = pending_observation(session.execute(sql if raw else text(sql)), operation)
        label = client.execute(f'SELECT label FROM rows WHERE id={row_id}').scalar()
        outcomes.append((observed, label))
    assert outcomes[0] == outcomes[1] == expected_pending(operation, mode)


@pytest.mark.asyncio
@pytest.mark.parametrize('operation', ['select', 'update'])
@pytest.mark.parametrize('mode', ['enabled', 'disabled', 'context'])
async def test_async_pending_orm_string_matches_parent(async_client, operation, mode):
    c = async_client
    outcomes = []
    for row_id, raw in enumerate([False, True], start=1):
        async with c.session() as session:
            session.autoflush = mode != 'disabled'
            session.add(Row(id=row_id, label='old'))
            with session.no_autoflush if mode == 'context' else nullcontext():
                sql = pending_sql(row_id, operation)
                observed = pending_observation(await session.execute(sql if raw else text(sql)), operation)
        label = (await c.execute(f'SELECT label FROM rows WHERE id={row_id}')).scalar()
        outcomes.append((observed, label))
    assert outcomes[0] == outcomes[1] == expected_pending(operation, mode)


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
def test_no_autoflush_restores_string_execution(client, cls):
    with session_for(client, cls) as session:
        session.add(Row(id=1, label='old'))
        with session.no_autoflush:
            assert session.execute(pending_sql(1, 'select')).scalar() == 0
        assert session.autoflush is True
        assert session.execute(pending_sql(1, 'select')).scalar() == expected_pending('select', 'enabled')[0]


@pytest.mark.asyncio
async def test_async_no_autoflush_restores_string_execution(async_client):
    async with async_client.session() as session:
        session.add(Row(id=1, label='old'))
        with session.no_autoflush:
            assert (await session.execute(pending_sql(1, 'select'))).scalar() == 0
        assert session.autoflush is True
        assert (await session.execute(pending_sql(1, 'select'))).scalar() == expected_pending('select', 'enabled')[0]


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
def test_failed_pending_flush_matches_parent_and_reuses_client(client, cls):
    client.execute("INSERT INTO rows (id,label) VALUES (1,'original')")
    observed = []
    outcomes = []
    checkouts, checkins = [], []
    event.listen(client._engine, 'checkout', lambda *args: checkouts.append(1))
    event.listen(client._engine, 'checkin', lambda *args: checkins.append(1))
    event.listen(client._engine, 'before_cursor_execute', lambda c, cur, sql, *args: observed.append(sql))
    for raw in [False, True]:
        observed.clear()
        with pytest.raises((QueryError, IntegrityError)) as error:
            with session_for(client, cls) as session:
                session.add(Row(id=1, label='duplicate'))
                sql = pending_sql(1, 'select')
                session.execute(sql if raw else text(sql))
        outcomes.append((type(error.value), any(sql.startswith('SELECT') for sql in observed)))
        assert client.execute('SELECT label FROM rows').rows == [('original',)]
        assert len(checkouts) == len(checkins)
    flushes = bool(expected_pending('select', 'enabled')[0])
    assert outcomes[0] == outcomes[1] == (QueryError if flushes else IntegrityError, not flushes)
    VectorManager(client).batch_insert('rows', [{'id': 2, 'embedding': [1, 2], 'label': 'reuse'}])
    assert client.execute('SELECT count(*) FROM rows').rows == [(2,)]


@pytest.mark.asyncio
async def test_async_failed_pending_flush_matches_parent_and_reuses_client(async_client):
    c = async_client
    await c.execute("INSERT INTO rows (id,label) VALUES (1,'original')")
    observed = []
    outcomes = []
    checkouts, checkins = [], []
    event.listen(c._engine.sync_engine, 'checkout', lambda *args: checkouts.append(1))
    event.listen(c._engine.sync_engine, 'checkin', lambda *args: checkins.append(1))
    event.listen(c._engine.sync_engine, 'before_cursor_execute', lambda conn, cur, sql, *args: observed.append(sql))
    for raw in [False, True]:
        observed.clear()
        with pytest.raises((QueryError, IntegrityError)) as error:
            async with c.session() as session:
                session.add(Row(id=1, label='duplicate'))
                sql = pending_sql(1, 'select')
                await session.execute(sql if raw else text(sql))
        outcomes.append((type(error.value), any(sql.startswith('SELECT') for sql in observed)))
        assert (await c.execute('SELECT label FROM rows')).rows == [('original',)]
        assert len(checkouts) == len(checkins)
    flushes = bool(expected_pending('select', 'enabled')[0])
    assert outcomes[0] == outcomes[1] == (QueryError if flushes else IntegrityError, not flushes)
    await AsyncVectorManager(c).batch_insert('rows', [{'id': 2, 'embedding': [1, 2], 'label': 'reuse'}])
    assert (await c.execute('SELECT count(*) FROM rows')).rows == [(2,)]


def test_public_batch_literals_and_statement_budget(client):
    statements, commits = [], []
    event.listen(client._engine, 'before_cursor_execute', lambda c, cur, sql, *args: statements.append(sql))
    event.listen(client._engine, 'commit', lambda c: commits.append(c))
    manager = VectorManager(client)
    assert manager.batch_insert('rows', data(), batch_size=3) is manager
    assert len(statements) == 3
    assert all(sql.startswith('INSERT INTO rows') for sql in statements)
    assert len(commits) == 1
    assert client.execute('SELECT label FROM rows ORDER BY id').rows == [(label,) for label in LABELS]


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
def test_session_substitution_orm_options_and_logging(client, cls):
    observed = []
    event.listen(
        client._engine,
        'before_cursor_execute',
        lambda c, cur, sql, p, ctx, many: observed.append(ctx.execution_options),
    )
    with session_for(client, cls) as session:
        connection = session.connection()
        client.logger.reset_mock()
        options = {'test_marker': 'first'}
        session.execute(
            'INSERT INTO rows VALUES (?, ?, ?)', (20, None, ':tag %'), execution_options=options, log_mode='off'
        )
        assert options == {'test_marker': 'first'}
        assert session.connection() is connection
        assert observed[-1]['test_marker'] == 'first'
        client.logger.log_query.assert_called_once()
        assert client.logger.log_query.call_args.kwargs['log_mode'] == 'off'
        assert (
            session.execute('SELECT label FROM rows', execution_options={'test_marker': 'second'}).scalar() == ':tag %'
        )
        assert observed[-1]['test_marker'] == 'second'
        session.execute('INSERT INTO rows VALUES (?, ?, ?)', (21, None, ''))
        assert session.execute(text('SELECT label FROM rows WHERE id=:id'), {'id': 21}).scalar() == ''
        session.add(Row(id=22, label='ORM'))
        assert session.execute(select(Row).where(Row.id == 22)).scalar_one().label == 'ORM'
        client.logger.reset_mock()
        with pytest.raises(QueryError):
            session.execute('SELECT * FROM missing_table')
        client.logger.log_query.assert_called_once()
        assert client.logger.log_query.call_args.kwargs['success'] is False


ROUTING_MODES = ['dictionary', 'bind', 'mapper', 'clause', 'mixed', 'unknown', 'event', 'invoke']


def routing_options(mode, alternate):
    if mode == 'dictionary':
        return {'bind_arguments': {'bind': alternate}}
    if mode == 'mixed':
        return {'bind_arguments': {'mapper': Row}, 'bind': alternate}
    if mode in ('bind', 'mapper', 'clause'):
        return {mode: {'bind': alternate, 'mapper': Row, 'clause': Row.__table__}[mode]}
    if mode == 'unknown':
        return {'unknown_option': True}

    def route(state):
        if mode == 'invoke':
            return state.invoke_statement(bind_arguments={'bind': alternate})
        state.bind_arguments['bind'] = alternate
        state.update_execution_options(routing_hook=True)

    return {'_add_event': route}


def route_observation(execute, statement, options):
    try:
        return ('value', execute(statement, **options).scalar())
    except QueryError as error:
        return ('error', str(error))


def assert_routing_outcomes(outcomes, mode, *, asynchronous=False):
    assert outcomes[0] == outcomes[1]
    legacy = sqlalchemy.__version__.startswith('1.4.')
    if mode == 'unknown' or (not legacy and mode in ('bind', 'mapper', 'clause', 'mixed')):
        assert outcomes[0][0] == 'error'
        assert 'unexpected keyword argument' in outcomes[0][1]
    elif legacy and mode == 'invoke' and not asynchronous:
        # Existing sync wrappers don't accept 1.4 invoke_statement's positional
        # option arguments. The unchanged text path is the same control.
        assert outcomes[0][0] == 'error'
        assert 'positional arguments' in outcomes[0][1]
    else:
        assert outcomes[0] == ('value', 'alternate')


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
@pytest.mark.parametrize('mode', ROUTING_MODES)
def test_execution_keywords_match_parent_and_route(client, cls, mode, tmp_path):
    client.execute("INSERT INTO rows (id,label) VALUES (1,'primary')")
    alternate = create_engine('sqlite:///' + str(tmp_path / 'routing.sqlite'))
    try:
        Base.metadata.create_all(alternate)
        with alternate.begin() as connection:
            connection.execute(text("INSERT INTO rows (id,label) VALUES (1,'alternate')"))
        outcomes = []
        for raw in [False, True]:
            with session_for(client, cls) as session:
                session.bind_mapper(Row, alternate)
                options = routing_options(mode, alternate)
                original = dict(options.get('bind_arguments', {}))
                sql = 'SELECT label FROM rows WHERE id=1'
                outcomes.append(route_observation(session.execute, sql if raw else text(sql), options))
                if raw and 'bind_arguments' in options:
                    assert options['bind_arguments'] == original
        assert_routing_outcomes(outcomes, mode)
    finally:
        alternate.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ROUTING_MODES)
async def test_async_execution_keywords_match_parent_and_route(async_client, mode, tmp_path):
    c = async_client
    await c.execute("INSERT INTO rows (id,label) VALUES (1,'primary')")
    alternate = create_async_engine('sqlite+aiosqlite:///' + str(tmp_path / 'routing.sqlite'))
    try:
        async with alternate.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
            await connection.execute(text("INSERT INTO rows (id,label) VALUES (1,'alternate')"))
        outcomes = []
        for raw in [False, True]:
            async with c.session() as session:
                session.sync_session.bind_mapper(Row, alternate.sync_engine)
                options = routing_options(mode, alternate.sync_engine)
                original = dict(options.get('bind_arguments', {}))
                sql = 'SELECT label FROM rows WHERE id=1'
                try:
                    result = await session.execute(sql if raw else text(sql), **options)
                    outcomes.append(('value', result.scalar()))
                except QueryError as error:
                    outcomes.append(('error', str(error)))
                if raw and 'bind_arguments' in options:
                    assert options['bind_arguments'] == original
        assert_routing_outcomes(outcomes, mode, asynchronous=True)
    finally:
        await alternate.dispose()


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
def test_session_explicit_bind_and_caller_rollback(client, cls, tmp_path):
    alternate = create_engine('sqlite:///' + str(tmp_path / 'alternate.sqlite'))
    Base.metadata.create_all(alternate)
    try:
        with cls(bind=client._engine, client=client) as session:
            connection = session.connection()
            VectorManager(client, executor=session).batch_insert('rows', data(), batch_size=3)
            assert session.connection() is connection
            assert session.in_transaction()
            session.execute('INSERT INTO rows VALUES (99, NULL, "caller")')
            routing = {'bind': alternate}
            session.execute('INSERT INTO rows VALUES (1, NULL, ":other")', bind_arguments=routing)
            assert routing == {'bind': alternate}
            assert session.execute('SELECT label FROM rows', bind_arguments=routing).scalar() == ':other'
            session.rollback()
        assert client.execute('SELECT count(*) FROM rows').rows == [(0,)]
        with alternate.connect() as conn:
            assert conn.exec_driver_sql('SELECT count(*) FROM rows').scalar() == 0
    finally:
        alternate.dispose()


@pytest.mark.parametrize('failure', ['duplicate', 'serialization'])
def test_late_failure_rolls_back_and_reuses_client(client, failure):
    rows = data()[:3]
    if failure == 'duplicate':
        rows[-1]['id'] = 0
    else:
        rows[-1]['label'] = {'bad': object()}
    commits = []
    event.listen(client._engine, 'commit', lambda c: commits.append(c))
    with pytest.raises(QueryError):
        VectorManager(client).batch_insert('rows', rows, batch_size=2)
    assert not commits
    assert client.execute('SELECT count(*) FROM rows').rows == [(0,)]
    VectorManager(client).batch_insert('rows', data(), batch_size=3)
    assert client.execute('SELECT count(*) FROM rows').rows == [(len(LABELS),)]


@pytest.mark.parametrize('no_parameters', [False, True])
@pytest.mark.parametrize('paramstyle', ['format', 'pyformat'])
@pytest.mark.parametrize(
    'defaults', ['connection', 'session'] if sqlalchemy.__version__.startswith('2.1.') else ['connection']
)
@pytest.mark.parametrize('cursor_cls', [pymysql.cursors.Cursor, aiomysql.cursors.Cursor])
def test_percent_execution_at_real_mysql_driver(client, cursor_cls, no_parameters, defaults, paramstyle):
    from sqlalchemy.dialects.mysql.pymysql import dialect

    connection = pymysql.connections.Connection(defer_connect=True)
    try:
        if cursor_cls is aiomysql.cursors.Cursor:
            connection.loop = None
        cursor = cursor_cls(connection)
        raw = "SELECT ':tag', '% %% %s %(tag)s'"
        mysql = dialect(paramstyle=paramstyle)
        # Real Session/Connection execution uses actual MySQL parameter
        # metadata; real driver formatting feeds the SQLite transport.
        client._engine.dialect.paramstyle = mysql.paramstyle
        client._engine.dialect.positional = mysql.positional
        client._engine.dialect.identifier_preparer._double_percents = mysql.identifier_preparer._double_percents
        client._engine = client._engine.execution_options(no_parameters=no_parameters)
        observed = []

        def format_for_transport(conn, cur, sql, params, context, many):
            observed.append(context.no_parameters)
            formatted = cursor.mogrify(sql, None if context.no_parameters else params)
            assert formatted == raw
            return formatted, ()

        event.listen(client._engine, 'before_cursor_execute', format_for_transport, retval=True)
        kwargs = {'execution_options': {'no_parameters': not no_parameters}} if defaults == 'session' else {}
        with ClientSession(bind=client._engine, client=client, **kwargs) as session:
            original = session.connection()
            default = not no_parameters if defaults == 'session' else no_parameters
            assert tuple(session.execute(raw).one()) == (':tag', '% %% %s %(tag)s')
            options = {'no_parameters': not default}
            assert tuple(session.execute(raw, execution_options=options).one()) == (':tag', '% %% %s %(tag)s')
            assert options == {'no_parameters': not default}
            assert observed == [default, not default]
            assert session.connection() is original
    finally:
        connection.close()


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
@pytest.mark.parametrize('paramstyle', ['qmark', 'numeric'])
def test_positional_literals_do_not_disable_normal_binding(client, cls, paramstyle):
    client._engine.dialect.paramstyle = paramstyle
    client._engine.dialect.positional = True
    literals = ['%s', '%(tag)s', '?', ':1', '$1', '[_POSITION]', '__[POSTCOMPILE_tag]']
    sql = 'SELECT ' + ', '.join("'" + value + "'" for value in literals)
    with session_for(client, cls) as session:
        assert tuple(session.execute(sql).one()) == tuple(literals)
        assert session.execute(text('SELECT :value'), {'value': 11}).scalar() == 11


@pytest.mark.parametrize(
    'paramstyle', ['numeric_dollar'] if not sqlalchemy.__version__.startswith('1.4.') else ['numeric']
)
def test_bindless_numeric_compilation(paramstyle):
    from sqlalchemy.engine.default import DefaultDialect

    sql = "SELECT '%(tag)s', '[_POSITION]', '__[POSTCOMPILE_tag]'"
    compiled = _LiteralSQL(sql).compile(dialect=DefaultDialect(paramstyle=paramstyle))
    assert str(compiled) == sql
    assert compiled.construct_params() == {}


class TypedLabel(TypeDecorator):
    impl = String
    cache_ok = True

    def process_result_value(self, value, dialect):
        return value + '|typed'


def transform_literal(state, composed):
    typed = state.statement.columns(label=TypedLabel())
    if not composed:
        state.statement = typed
        return
    source = typed.subquery()
    state.statement = (
        select(source.c.label, literal(7).label('bound_value'))
        .where(literal(2).in_(bindparam('allowed', [1, 2], expanding=True)))
        .where(literal(3) == bindparam('guard', 3, type_=Integer, literal_execute=True))
    )


def transformed_query(value):
    return "SELECT '" + value + "' AS label"


def transformed_expected(value, composed):
    return (value + '|typed', 7) if composed else (value + '|typed',)


@contextmanager
def mysql_formatting_transport(engine, cursor_cls, paramstyle):
    from sqlalchemy.dialects.mysql.pymysql import dialect

    connection = pymysql.connections.Connection(defer_connect=True)
    if cursor_cls is aiomysql.cursors.Cursor:
        connection.loop = None
    cursor = cursor_cls(connection)
    mysql = dialect(paramstyle=paramstyle)
    original = (
        engine.dialect.paramstyle,
        engine.dialect.positional,
        engine.dialect.identifier_preparer._double_percents,
    )
    engine.dialect.paramstyle = mysql.paramstyle
    engine.dialect.positional = mysql.positional
    engine.dialect.identifier_preparer._double_percents = mysql.identifier_preparer._double_percents

    def format_for_transport(conn, cur, sql, params, context, many):
        # Match SQLAlchemy's dispatch: no_parameters omits an empty argument
        # container, but a composed query must still send its real binds.
        arguments = None if context.no_parameters and not params else params
        return cursor.mogrify(sql, arguments), ()

    event.listen(engine, 'before_cursor_execute', format_for_transport, retval=True)
    try:
        yield
    finally:
        event.remove(engine, 'before_cursor_execute', format_for_transport)
        (
            engine.dialect.paramstyle,
            engine.dialect.positional,
            engine.dialect.identifier_preparer._double_percents,
        ) = original
        # Formatting-only cursors have no live connection or result to drain.
        connection.close()


def mysql_transformed_expected(value, composed, no_parameters):
    # Preserve the parent's documented bind-free no_parameters behavior.
    if no_parameters and not composed:
        value = value.replace('%', '%%')
    return transformed_expected(value, composed)


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
@pytest.mark.parametrize('paramstyle', ['format', 'pyformat'])
@pytest.mark.parametrize('composed', [False, True])
@pytest.mark.parametrize('no_parameters', [False, True])
def test_transformed_literals_at_real_mysql_driver(client, cls, paramstyle, composed, no_parameters):
    with mysql_formatting_transport(client._engine, pymysql.cursors.Cursor, paramstyle):
        with session_for(client, cls) as session:
            event.listen(session, 'do_orm_execute', lambda state: transform_literal(state, composed))
            for value in ['ordinary', '100%', '%s', '%%']:
                sql = transformed_query(value)
                expected = mysql_transformed_expected(value, composed, no_parameters)
                for statement in [text(sql), sql]:
                    result = session.execute(statement, execution_options={'no_parameters': no_parameters})
                    assert tuple(result.one()) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize('paramstyle', ['format', 'pyformat'])
@pytest.mark.parametrize('composed', [False, True])
@pytest.mark.parametrize('no_parameters', [False, True])
async def test_async_transformed_literals_at_real_mysql_driver(async_client, paramstyle, composed, no_parameters):
    engine = async_client._engine.sync_engine
    with mysql_formatting_transport(engine, aiomysql.cursors.Cursor, paramstyle):
        async with async_client.session() as session:
            event.listen(session.sync_session, 'do_orm_execute', lambda state: transform_literal(state, composed))
            for value in ['ordinary', '100%', '%s', '%%']:
                sql = transformed_query(value)
                expected = mysql_transformed_expected(value, composed, no_parameters)
                for statement in [text(sql), sql]:
                    result = await session.execute(statement, execution_options={'no_parameters': no_parameters})
                    assert tuple(result.one()) == expected


def assert_parent_named_percent_error(error):
    # Some SQLAlchemy releases mistake quoted named-percent syntax for a
    # positional bind. Qualify this exact inherited failure with a real parent
    # Session, rather than assuming it affects only one patch release.
    assert isinstance(error.orig, KeyError)
    assert error.orig.args == ('tag',)


def parent_named_percent_error(engine, composed, no_parameters):
    with ParentSession(bind=engine) as session:
        event.listen(session, 'do_orm_execute', lambda state: transform_literal(state, composed))
        try:
            result = session.execute(
                text(transformed_query('%(tag)s')), execution_options={'no_parameters': no_parameters}
            )
        except StatementError as error:
            assert_parent_named_percent_error(error)
            return True
        assert tuple(result.one()) == transformed_expected('%(tag)s', composed)
        return False


async def async_parent_named_percent_error(engine, composed, no_parameters):
    async with ParentAsyncSession(bind=engine) as session:
        event.listen(session.sync_session, 'do_orm_execute', lambda state: transform_literal(state, composed))
        try:
            result = await session.execute(
                text(transformed_query('%(tag)s')), execution_options={'no_parameters': no_parameters}
            )
        except StatementError as error:
            assert_parent_named_percent_error(error)
            return True
        assert tuple(result.one()) == transformed_expected('%(tag)s', composed)
        return False


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
@pytest.mark.parametrize('composed', [False, True])
@pytest.mark.parametrize('no_parameters', [False, True])
def test_transformed_literals_and_outer_binds(client, cls, composed, no_parameters):
    parent_error = parent_named_percent_error(client._engine, composed, no_parameters)
    with session_for(client, cls) as session:
        event.listen(session, 'do_orm_execute', lambda state: transform_literal(state, composed))
        for value in ['ordinary', '100%', '%s']:
            sql = transformed_query(value)
            for statement in [text(sql), sql]:
                result = session.execute(statement, execution_options={'no_parameters': no_parameters})
                assert tuple(result.one()) == transformed_expected(value, composed)
        assert tuple(session.execute(transformed_query(':tag')).one()) == transformed_expected(':tag', composed)
        for statement in [text(transformed_query('%(tag)s')), transformed_query('%(tag)s')]:
            if parent_error:
                with pytest.raises(QueryError, match=r"\(builtins\.KeyError\) 'tag'"):
                    session.execute(statement, execution_options={'no_parameters': no_parameters})
            else:
                assert tuple(
                    session.execute(statement, execution_options={'no_parameters': no_parameters}).one()
                ) == transformed_expected('%(tag)s', composed)


@pytest.mark.asyncio
@pytest.mark.parametrize('composed', [False, True])
@pytest.mark.parametrize('no_parameters', [False, True])
async def test_async_transformed_literals_and_outer_binds(async_client, composed, no_parameters):
    parent_error = await async_parent_named_percent_error(async_client._engine, composed, no_parameters)
    async with async_client.session() as session:
        event.listen(session.sync_session, 'do_orm_execute', lambda state: transform_literal(state, composed))
        for value in ['ordinary', '100%', '%s']:
            sql = transformed_query(value)
            for statement in [text(sql), sql]:
                result = await session.execute(statement, execution_options={'no_parameters': no_parameters})
                assert tuple(result.one()) == transformed_expected(value, composed)
        assert tuple((await session.execute(transformed_query(':tag'))).one()) == transformed_expected(':tag', composed)
        for statement in [text(transformed_query('%(tag)s')), transformed_query('%(tag)s')]:
            if parent_error:
                with pytest.raises(QueryError, match=r"\(builtins\.KeyError\) 'tag'"):
                    await session.execute(statement, execution_options={'no_parameters': no_parameters})
            else:
                assert tuple(
                    (await session.execute(statement, execution_options={'no_parameters': no_parameters})).one()
                ) == transformed_expected('%(tag)s', composed)


def record_literal_hooks(session, engine):
    statements, markers = [], []

    def inspect_and_mark(state):
        statements.append(str(state.statement))
        state.update_execution_options(literal_hook=True)

    event.listen(session, 'do_orm_execute', inspect_and_mark)
    event.listen(
        engine,
        'before_cursor_execute',
        lambda c, cur, sql, p, context, many: markers.append(context.execution_options.get('literal_hook')),
    )
    return statements, markers


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
def test_literal_representation_and_session_hooks(client, cls):
    sql = r"SELECT ':tag', '\:tag', '% %(tag)s'"
    with session_for(client, cls) as session:
        statements, markers = record_literal_hooks(session, client._engine)
        assert tuple(session.execute(sql).one()) == (':tag', r'\:tag', '% %(tag)s')
    assert statements == [sql]
    assert markers == [True]


@pytest.mark.asyncio
async def test_async_literal_representation_and_session_hooks(async_client):
    sql = r"SELECT ':tag', '\:tag', '% %(tag)s'"
    async with async_client.session() as session:
        statements, markers = record_literal_hooks(session.sync_session, async_client._engine.sync_engine)
        assert tuple((await session.execute(sql)).one()) == (':tag', r'\:tag', '% %(tag)s')
    assert statements == [sql]
    assert markers == [True]


@pytest.mark.asyncio
async def test_async_public_literals_and_caller_rollback(async_client):
    c = async_client
    statements, commits = [], []
    event.listen(c._engine.sync_engine, 'before_cursor_execute', lambda conn, cur, sql, *args: statements.append(sql))
    event.listen(c._engine.sync_engine, 'commit', lambda conn: commits.append(conn))
    manager = AsyncVectorManager(c)
    assert await manager.batch_insert('rows', data(), batch_size=3) is manager
    assert len(statements) == 3
    assert len(commits) == 1
    async with c.session() as session:
        assert (await session.execute('SELECT label FROM rows ORDER BY id')).all() == [(label,) for label in LABELS]
        await session.execute('DELETE FROM rows')
    with pytest.raises(RuntimeError, match='caller rollback'):
        async with c.session() as session:
            connection = await session.connection()
            await session.vector_ops.batch_insert('rows', data(), batch_size=3)
            assert await session.connection() is connection
            await session.execute('INSERT INTO rows VALUES (?, ?, ?)', (99, None, ':caller %'))
            assert (
                await session.execute(text('SELECT label FROM rows WHERE id=:id'), {'id': 99})
            ).scalar() == ':caller %'
            raise RuntimeError('caller rollback')
    assert (await c.execute('SELECT count(*) FROM rows')).rows == [(0,)]


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['duplicate', 'serialization', 'cancel', 'orm-cancel'])
async def test_async_failure_pool_return_and_reuse(async_client, failure):
    c = async_client
    counters = {'checkout': 0, 'checkin': 0, 'inserts': 0, 'commit': 0}
    task = None

    def checkout(*args):
        counters['checkout'] += 1

    def checkin(*args):
        counters['checkin'] += 1

    def commit(*args):
        counters['commit'] += 1

    def capture(conn, cursor, sql, args, context, many):
        if sql.startswith('INSERT INTO rows'):
            counters['inserts'] += 1
            if failure in ('cancel', 'orm-cancel') and counters['inserts'] == 1:
                task.cancel()

    listeners = [('checkout', checkout), ('checkin', checkin), ('commit', commit), ('after_cursor_execute', capture)]
    for name, listener in listeners:
        event.listen(c._engine.sync_engine, name, listener)
    try:
        rows = data()[:3]
        if failure == 'duplicate':
            rows[-1]['id'] = 0
        elif failure == 'serialization':
            rows[-1]['label'] = {'bad': object()}
        if failure == 'orm-cancel':

            async def pending_write():
                async with c.session() as session:
                    session.add(Row(id=1, label='pending'))
                    # Older Core statements do not autoflush. Their unchanged
                    # ORM query is the control for cancelling at this phase.
                    statement = pending_sql(1, 'select') if expected_pending('select', 'enabled')[0] else select(Row)
                    await session.execute(statement)

            operation = pending_write()
        else:
            operation = AsyncVectorManager(c).batch_insert('rows', rows, batch_size=2)
        task = asyncio.create_task(operation)
        with pytest.raises(asyncio.CancelledError if failure in ('cancel', 'orm-cancel') else QueryError):
            await task
        assert counters['inserts'] == 1
        assert counters['commit'] == 0
        assert counters['checkout'] == counters['checkin']
        assert (await c.execute('SELECT count(*) FROM rows')).rows == [(0,)]
    finally:
        for name, listener in listeners:
            event.remove(c._engine.sync_engine, name, listener)
    await AsyncVectorManager(c).batch_insert('rows', data(), batch_size=3)
    assert (await c.execute('SELECT count(*) FROM rows')).rows == [(len(LABELS),)]


@pytest.mark.asyncio
async def test_async_streaming_rejected_and_buffered_execution_reusable(async_client):
    async with async_client.session() as session:
        with pytest.raises(QueryError, match='stream'):
            await session.execute('SELECT 7', execution_options={'stream_results': True})
        assert (await session.execute('SELECT 8')).one()[0] == 8
    assert (await async_client.execute('SELECT 9')).rows == [(9,)]


@pytest.mark.asyncio
async def test_async_options_routing_and_orm_delegation(async_client, tmp_path):
    c = async_client
    alternate = create_async_engine('sqlite+aiosqlite:///' + str(tmp_path / 'alternate.sqlite'))
    async with alternate.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    observed = []
    event.listen(
        c._engine.sync_engine,
        'before_cursor_execute',
        lambda conn, cur, sql, p, ctx, many: observed.append(ctx.execution_options),
    )
    try:
        async with c.session() as session:
            await session.connection()
            options = {'test_marker': 'first'}
            c.logger.reset_mock()
            await session.execute('SELECT 1', execution_options=options, log_mode='off')
            assert observed[-1]['test_marker'] == 'first'
            assert options == {'test_marker': 'first'}
            c.logger.log_query.assert_called_once()
            assert c.logger.log_query.call_args.kwargs['log_mode'] == 'off'
            await session.execute('SELECT 1', execution_options={'test_marker': 'second'})
            assert observed[-1]['test_marker'] == 'second'
            session.add(Row(id=55, label='ORM'))
            assert (await session.execute(select(Row).where(Row.id == 55))).scalar_one().label == 'ORM'
            routing = {'bind': alternate.sync_engine}
            await session.execute('INSERT INTO rows VALUES (?, ?, ?)', (77, None, ''), bind_arguments=routing)
            assert routing == {'bind': alternate.sync_engine}
            assert (await session.execute('SELECT label FROM rows', bind_arguments=routing)).scalar() == ''
            await session.rollback()
        async with alternate.connect() as conn:
            assert (await conn.exec_driver_sql('SELECT count(*) FROM rows')).scalar() == 0
    finally:
        await alternate.dispose()


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
@pytest.mark.parametrize('transformed', [False, True])
@pytest.mark.parametrize('autocommit', [False, True] if sqlalchemy.__version__.startswith('1.4.') else [False])
def test_row_semantics_and_single_connection_reuse(cls, autocommit, transformed, tmp_path):
    c = Client(logger=Mock())
    c._engine = create_engine(
        'sqlite:///' + str(tmp_path / 'single.sqlite'),
        poolclass=QueuePool,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.1,
    )
    kwargs = {'autocommit': True} if autocommit else {}

    def result_types(state):
        if transformed:
            state.statement = state.statement.columns(id=Integer())

    def new_session():
        session = cls(bind=c._engine, client=c, **kwargs)
        event.listen(session, 'do_orm_execute', result_types)
        return session

    try:
        # Compare the SDK string path to its unchanged SQLAlchemy statement path.
        for statement in [text('SELECT 7 AS id'), 'SELECT 7 AS id']:
            with new_session() as session:
                for _ in range(2):
                    result = session.execute(statement, execution_options={'future_result': False})
                    row = result.one()
                    result.close()
                    assert (7 in row) is not autocommit
                    assert ('id' in row) is autocommit
                    assert row._mapping['id'] == 7
                    if autocommit:
                        assert c._engine.pool.checkedout() == 0
            assert c._engine.pool.checkedout() == 0
        with new_session() as session:
            with pytest.raises(QueryError):
                session.execute('SELECT * FROM missing_table')
            if autocommit:
                assert c._engine.pool.checkedout() == 0
            # Invalid option containers are rejected without leaking a connection.
            with pytest.raises(QueryError):
                session.execute('SELECT 7', execution_options=object())
            if autocommit:
                assert c._engine.pool.checkedout() == 0

        if autocommit and transformed:
            legacy_transformed_rejection_matches_parent(c, cls)
        else:
            # Fail after checkout but before cursor/driver execution. Session now
            # rejects invalid option containers before it acquires a connection.
            def reject_execution(conn, *args):
                assert c._engine.pool.checkedout() == 1
                raise RuntimeError('pre-driver hook failure')

            completed = []

            def record_completed(conn, cur, sql, *args):
                completed.append(sql)

            event.listen(c._engine, 'before_cursor_execute', reject_execution)
            event.listen(c._engine, 'after_cursor_execute', record_completed)
            try:
                with new_session() as session:
                    with pytest.raises(QueryError, match='pre-driver hook failure'):
                        session.execute('SELECT 7')
                    if autocommit:
                        assert c._engine.pool.checkedout() == 0
            finally:
                event.remove(c._engine, 'before_cursor_execute', reject_execution)
                event.remove(c._engine, 'after_cursor_execute', record_completed)
            assert completed == []
            assert c._engine.pool.checkedout() == 0
        with new_session() as session:
            assert session.execute('SELECT 8').scalar() == 8
    finally:
        c.disconnect()


def legacy_transformed_rejection_matches_parent(client, cls):
    # 1.4 autocommit + compiled before_cursor_execute rejection leaks in the
    # unmodified parent too. Fixture cleanup below is not product cleanup.
    engine = client._engine
    for owner in [ParentSession, cls]:
        captured, completed = [], []

        def reject(conn, cursor, *args):
            captured.append((conn, cursor))
            raise RuntimeError('pre-driver hook failure')

        def record_completed(*args):
            completed.append(1)

        event.listen(engine, 'before_cursor_execute', reject)
        event.listen(engine, 'after_cursor_execute', record_completed)
        try:
            options = {} if owner is ParentSession else {'client': client}
            with owner(bind=engine, autocommit=True, **options) as session:
                event.listen(
                    session,
                    'do_orm_execute',
                    lambda state: setattr(state, 'statement', state.statement.columns(id=Integer())),
                )
                statement = text('SELECT 7 AS id') if owner is ParentSession else 'SELECT 7 AS id'
                error = RuntimeError if owner is ParentSession else QueryError
                with pytest.raises(error, match='pre-driver hook failure'):
                    session.execute(statement)
                assert len(captured) == 1
                assert completed == []
                assert engine.pool.checkedout() == 1
            assert engine.pool.checkedout() == 1
        finally:
            event.remove(engine, 'before_cursor_execute', reject)
            event.remove(engine, 'after_cursor_execute', record_completed)
            for connection, cursor in captured:
                try:
                    cursor.close()
                finally:
                    connection.close()
            assert engine.pool.checkedout() == 0
        with engine.connect() as connection:
            assert connection.exec_driver_sql('SELECT 8').scalar() == 8
        assert engine.pool.checkedout() == 0
