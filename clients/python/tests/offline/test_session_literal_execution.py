# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import asyncio
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

import aiomysql
import pymysql
import pytest
import pytest_asyncio
import sqlalchemy
from sqlalchemy import Column, Integer, String, create_engine, event, select, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.pool import QueuePool

from matrixone import Client
from matrixone._session_execution import _prepare_driver_sql
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
    with cls(bind=client._engine, client=client) as session:
        with session.begin():
            yield session


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
        with pytest.raises(QueryError, match='unexpected keyword'):
            session.execute('SELECT 1', unknown_option=True)


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
@pytest.mark.parametrize('cursor_cls', [pymysql.cursors.Cursor, aiomysql.cursors.Cursor])
def test_percent_preparation_at_real_mysql_driver(cursor_cls, no_parameters):
    connection = pymysql.connections.Connection(defer_connect=True)
    try:
        if cursor_cls is aiomysql.cursors.Cursor:
            connection.loop = None
        cursor = cursor_cls(connection)
        raw = "SELECT ':tag', '% %% %s %(tag)s'"
        dialect_connection = SimpleNamespace(
            dialect=SimpleNamespace(paramstyle='pyformat'),
            get_execution_options=lambda: {'no_parameters': no_parameters},
        )
        rendered, options = _prepare_driver_sql(SimpleNamespace(), dialect_connection, raw, {})
        assert cursor.mogrify(rendered, None if no_parameters else ()) == raw
        assert options == {}
        rendered, _ = _prepare_driver_sql(
            SimpleNamespace(execution_options={'no_parameters': no_parameters}),
            dialect_connection,
            raw,
            {'no_parameters': not no_parameters},
        )
        assert cursor.mogrify(rendered, () if no_parameters else None) == raw
    finally:
        connection.close()


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
@pytest.mark.parametrize('failure', ['duplicate', 'serialization', 'cancel'])
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
            if failure == 'cancel' and counters['inserts'] == 1:
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
        task = asyncio.create_task(AsyncVectorManager(c).batch_insert('rows', rows, batch_size=2))
        with pytest.raises(asyncio.CancelledError if failure == 'cancel' else QueryError):
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
@pytest.mark.parametrize('autocommit', [False, True] if sqlalchemy.__version__.startswith('1.4.') else [False])
def test_row_semantics_and_single_connection_reuse(cls, autocommit, tmp_path):
    c = Client(logger=Mock())
    c._engine = create_engine(
        'sqlite:///' + str(tmp_path / 'single.sqlite'),
        poolclass=QueuePool,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.1,
    )
    kwargs = {'autocommit': True} if autocommit else {}
    try:
        # Compare the SDK string path to its unchanged SQLAlchemy statement path.
        for statement in [text('SELECT 7 AS id'), 'SELECT 7 AS id']:
            with cls(bind=c._engine, client=c, **kwargs) as session:
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
        with cls(bind=c._engine, client=c, **kwargs) as session:
            with pytest.raises(QueryError):
                session.execute('SELECT * FROM missing_table')
            if autocommit:
                assert c._engine.pool.checkedout() == 0
            # Fail before driver execution, after a connection has been acquired.
            with pytest.raises(QueryError):
                session.execute('SELECT 7', execution_options=object())
            if autocommit:
                assert c._engine.pool.checkedout() == 0
        assert c._engine.pool.checkedout() == 0
        with cls(bind=c._engine, client=c, **kwargs) as session:
            assert session.execute('SELECT 8').scalar() == 8
    finally:
        c.disconnect()
