# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import json
from unittest.mock import Mock

import numpy as np
import pytest
import sqlalchemy
from sqlalchemy import Integer, String, bindparam, create_engine, event, select
from sqlalchemy.pool import QueuePool

from matrixone import Client
from matrixone._sql_literals import substitute_parameters
from matrixone.client import Session as ClientSession
from matrixone.exceptions import QueryError
from matrixone.session import Session as ExportedSession
from matrixone.vector_manager import AsyncVectorManager, VectorManager

from .test_session_literal_execution import async_client, client, session_for  # noqa: F401


@pytest.mark.parametrize('dtype', [np.float32, np.int64, np.float64])
def test_public_vector_numpy_elements(client, dtype):
    values = list(np.array([1, 2], dtype=dtype))
    VectorManager(client).insert('rows', {'id': 1, 'embedding': values})
    assert json.loads(client.execute('SELECT embedding FROM rows').rows[0][0]) == [1, 2]


@pytest.mark.asyncio
@pytest.mark.parametrize('dtype', [np.float32, np.int64, np.float64])
async def test_async_public_vector_numpy_elements(async_client, dtype):
    values = list(np.array([1, 2], dtype=dtype))
    await AsyncVectorManager(async_client).insert('rows', {'id': 1, 'embedding': values})
    assert json.loads((await async_client.execute('SELECT embedding FROM rows')).rows[0][0]) == [1, 2]


@pytest.mark.parametrize(
    'sql,params,mode,expected',
    [
        ("SELECT 1 AS `tail\\`, ?", (7,), '', "SELECT 1 AS `tail\\`, 7"),
        ("SELECT '\\', ?", (7,), 'NO_BACKSLASH_ESCAPES', "SELECT '\\', 7"),
        ('SELECT 1 AS "tail\\", :id', {'id': 7}, 'ANSI_QUOTES', 'SELECT 1 AS "tail\\", 7'),
        ('SELECT /*!50000 ? */', (7,), '', 'SELECT /*!50000 7 */'),
        ('SELECT /*! :id */ /* ? :ignored */', {'id': 7}, '', 'SELECT /*! 7 */ /* ? :ignored */'),
        ("SELECT /*!50000 '?' AS marker, ? */", (7,), '', "SELECT /*!50000 '?' AS marker, 7 */"),
        ('SELECT /* ? */ ? -- :ignored', (7,), '', 'SELECT /* ? */ 7 -- :ignored'),
    ],
)
def test_server_lexical_boundaries(sql, params, mode, expected):
    assert substitute_parameters(sql, params, sql_mode=mode) == expected


def mode_transport(engine, observed, mode):
    engine.dialect.name = 'matrixone'

    def transport(connection, cursor, sql, params, context, many):
        if sql == 'SELECT @@SESSION.sql_mode':
            observed.append(connection)
            return "SELECT '" + mode['value'] + "'", ()
        return sql, params

    event.listen(engine, 'before_cursor_execute', transport, retval=True)


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
def test_live_mode_uses_current_session_connection(client, cls):
    observed, mode = [], {'value': 'NO_BACKSLASH_ESCAPES'}
    mode_transport(client._engine, observed, mode)
    with session_for(client, cls) as session:
        connection = session.connection()
        assert tuple(session.execute("SELECT '\\', ?", (7,)).one()) == ('\\', 7)
        mode['value'] = ''
        assert tuple(session.execute('SELECT 1 AS `tail\\`, ?', (8,)).one()) == (1, 8)
        assert observed == [connection, connection]


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
def test_mode_probe_preserves_routing_and_call_options(client, cls, tmp_path):
    alternate = create_engine('sqlite:///' + str(tmp_path / 'alternate.sqlite'))
    observed, mode = [], {'value': 'NO_BACKSLASH_ESCAPES'}
    mode_transport(alternate, observed, mode)
    with alternate.begin() as connection:
        connection.exec_driver_sql('CREATE TABLE route(label TEXT)')
        connection.exec_driver_sql("INSERT INTO route VALUES ('alternate')")
    markers = []
    event.listen(
        alternate,
        'before_cursor_execute',
        lambda c, cur, sql, p, ctx, many: markers.append(ctx.execution_options.get('marker')),
    )
    route, options = {'bind': alternate}, {'marker': 'once'}
    try:
        with session_for(client, cls) as session:
            assert tuple(
                session.execute(
                    "SELECT '\\', label FROM route WHERE 1=?", (1,), bind_arguments=route, execution_options=options
                ).one()
            ) == ('\\', 'alternate')
            assert session.execute('SELECT label FROM route', bind_arguments=route).scalar() == 'alternate'
        assert len(observed) == 1
        assert markers == [None, 'once', None]
        assert route == {'bind': alternate} and options == {'marker': 'once'}
    finally:
        alternate.dispose()


def test_ordinary_parameter_query_has_no_mode_probe(client):
    observed, mode = [], {'value': 'NO_BACKSLASH_ESCAPES'}
    mode_transport(client._engine, observed, mode)
    with client.session() as session:
        assert session.execute('SELECT ?', (7,)).scalar() == 7
    assert observed == []


@pytest.mark.asyncio
async def test_async_live_mode_uses_current_session_connection(async_client):
    observed, mode = [], {'value': 'NO_BACKSLASH_ESCAPES'}
    mode_transport(async_client._engine.sync_engine, observed, mode)
    async with async_client.session() as session:
        connection = await session.connection()
        assert tuple((await session.execute("SELECT '\\', ?", (7,))).one()) == ('\\', 7)
        mode['value'] = ''
        assert tuple((await session.execute('SELECT 1 AS `tail\\`, ?', (8,))).one()) == (1, 8)
        assert observed == [connection.sync_connection, connection.sync_connection]


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
@pytest.mark.parametrize('transformed', [False, True])
def test_mode_probe_retains_result_connection_ownership(tmp_path, cls, transformed):
    legacy = sqlalchemy.__version__.startswith('1.4.')
    client = Client(logger=Mock())
    client._engine = create_engine(
        'sqlite:///' + str(tmp_path / 'legacy.sqlite'),
        poolclass=QueuePool,
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.1,
    )
    observed, mode = [], {'value': 'NO_BACKSLASH_ESCAPES'}
    mode_transport(client._engine, observed, mode)
    try:
        with cls(bind=client._engine, client=client, **({'autocommit': True} if legacy else {})) as session:
            if transformed:
                event.listen(
                    session, 'do_orm_execute', lambda state: setattr(state, 'statement', state.statement.columns())
                )
            assert tuple(session.execute("SELECT '\\', ?", (7,)).one()) == ('\\', 7)
            assert client._engine.pool.checkedout() == (0 if legacy else 1)
            with pytest.raises(QueryError):
                session.execute("SELECT '\\', ?", ())
            assert client._engine.pool.checkedout() == (0 if legacy else 1)
            assert session.execute('SELECT 8').scalar() == 8
        assert client._engine.pool.checkedout() == 0
    finally:
        client.disconnect()


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
@pytest.mark.parametrize('bound', [False, True])
@pytest.mark.parametrize('transformed', [False, 'columns', 'subquery'])
def test_mode_discovery_follows_execution_hook(client, cls, tmp_path, bound, transformed):
    alternate = create_engine('sqlite:///' + str(tmp_path / 'hook.sqlite'))
    default_probes, routed_probes = [], []
    mode_transport(client._engine, default_probes, {'value': ''})
    mode_transport(alternate, routed_probes, {'value': 'NO_BACKSLASH_ESCAPES'})

    def route(state):
        state.bind_arguments['bind'] = alternate
        if transformed:
            state.statement = state.statement.columns(marker=String(), value=Integer())
            if transformed == 'subquery':
                leaf = state.statement.subquery()
                state.statement = select(leaf.c.marker, leaf.c.value, bindparam('outer', 9))

    try:
        with cls(bind=client._engine if bound else None, client=client) as session:
            event.listen(session, 'do_orm_execute', route)
            assert tuple(session.execute("SELECT '\\' AS marker, ? AS value", (7,)).one()) == (
                ('\\', 7, 9) if transformed == 'subquery' else ('\\', 7)
            )
        assert default_probes == []
        assert len(routed_probes) == 1
        assert routed_probes[0].engine is alternate
    finally:
        alternate.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize('bound', [False, True])
@pytest.mark.parametrize('transformed', [False, 'columns', 'subquery'])
async def test_async_mode_discovery_follows_execution_hook(async_client, tmp_path, bound, transformed):
    from sqlalchemy.ext.asyncio import create_async_engine

    from matrixone.session import AsyncSession

    alternate = create_async_engine('sqlite+aiosqlite:///' + str(tmp_path / 'async-hook.sqlite'))
    default_probes, routed_probes = [], []
    mode_transport(async_client._engine.sync_engine, default_probes, {'value': ''})
    mode_transport(alternate.sync_engine, routed_probes, {'value': 'NO_BACKSLASH_ESCAPES'})

    def route(state):
        state.bind_arguments['bind'] = alternate.sync_engine
        if transformed:
            state.statement = state.statement.columns(marker=String(), value=Integer())
            if transformed == 'subquery':
                leaf = state.statement.subquery()
                state.statement = select(leaf.c.marker, leaf.c.value, bindparam('outer', 9))

    try:
        async with AsyncSession(bind=async_client._engine if bound else None, client=async_client) as session:
            event.listen(session.sync_session, 'do_orm_execute', route)
            assert tuple((await session.execute("SELECT '\\' AS marker, ? AS value", (7,))).one()) == (
                ('\\', 7, 9) if transformed == 'subquery' else ('\\', 7)
            )
        assert default_probes == []
        assert len(routed_probes) == 1
        assert routed_probes[0].engine is alternate.sync_engine
    finally:
        await alternate.dispose()


def test_snapshot_defers_parameters_to_execution_connection(client):
    from matrixone.client import SnapshotClient

    probes = []
    mode_transport(client._engine, probes, {'value': 'NO_BACKSLASH_ESCAPES'})
    snapshot = SnapshotClient(client, 'existing_snapshot')
    assert snapshot.execute("SELECT '\\', ?", (7,)).rows == [('\\', 7)]
    assert len(probes) == 1
