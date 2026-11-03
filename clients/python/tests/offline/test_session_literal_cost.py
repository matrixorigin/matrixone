# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Bound SDK preparation work independently of SQLAlchemy's query execution."""

from collections import Counter

import pytest
from sqlalchemy import Integer, String, event, exists, literal, select
from sqlalchemy.orm import Session as ParentSession

from matrixone import _session_execution as execution
from matrixone.client import Session as ClientSession
from matrixone.session import Session as ExportedSession

from .test_session_literal_execution import Row, async_client, client, session_for  # noqa: F401
from .test_sql_review_boundaries import mode_transport


def shared_cte(seed, depth=11):
    previous = seed.cte('level_0')
    for level in range(1, depth + 1):
        previous = (
            select(previous.c.marker, previous.c.value)
            .where(exists(select(literal(1)).select_from(previous)))
            .cte('level_' + str(level))
        )
    return select(previous.c.marker, previous.c.value)


@pytest.mark.parametrize('kind', ['core', 'shared_cte', 'orm'])
def test_cached_unrelated_queries_do_not_walk_sdk_expressions(client, monkeypatch, kind):
    statement = (
        select(Row.id) if kind == 'orm' else select(literal('ordinary').label('marker'), literal(7).label('value'))
    )
    if kind == 'shared_cte':
        statement = shared_cte(statement)
    # Both a direct Connection and a plain SQLAlchemy Session are independent
    # consumers of the Engine-wide listener, including a prewarmed cache.
    with client._engine.connect() as connection, ParentSession(bind=connection) as session:
        execute = session.execute if kind == 'orm' else connection.execute
        expected = execute(statement).all()
        calls = []
        original = statement.get_children

        def children(**kwargs):
            calls.append(1)
            return original(**kwargs)

        monkeypatch.setattr(statement, 'get_children', children)
        assert execute(statement).all() == expected
        assert calls == [], 'cached ordinary execution must not enter the SDK graph walk'


def test_shared_deferred_nodes_are_discovered_once(client, monkeypatch):
    statement = shared_cte(
        execution._LiteralSQL("SELECT '\\' AS marker, ? AS value", client, (7,)).columns(
            marker=String(), value=Integer()
        )
    )
    pending, unique = [statement], {}
    while pending:
        node = pending.pop()
        if id(node) not in unique:
            unique[id(node)] = node
            pending.extend(node.get_children())
    visits = Counter()
    with monkeypatch.context() as scoped:
        for node in unique.values():
            original = node.get_children

            def children(original=original, node_id=id(node), **kwargs):
                visits[node_id] += 1
                return original(**kwargs)

            scoped.setattr(node, 'get_children', children)
        with client._engine.connect() as connection:
            rendered, multiparams, params = execution._render_wrapped_literals(
                connection, statement, (), {}, {'_matrixone_deferred_literals': True}
            )
    assert max(visits.values()) == 1, 'shared CTE references must not multiply discovery work'
    assert len(visits) <= len(unique)
    assert rendered is not statement and multiparams == () and params == {}


@pytest.mark.parametrize('cls', [ClientSession, ExportedSession])
def test_shared_cte_keeps_mode_options_and_next_execution_is_unmarked(client, cls):
    probes, markers = [], []
    mode_transport(client._engine, probes, {'value': 'NO_BACKSLASH_ESCAPES'})
    event.listen(
        client._engine,
        'before_cursor_execute',
        lambda c, cur, sql, p, ctx, many: markers.append(
            bool(ctx.execution_options.get('_matrixone_deferred_literals'))
        ),
    )

    def wrap(state):
        if isinstance(state.statement, execution._LiteralSQL) and state.statement._literal_client is not None:
            state.statement = shared_cte(state.statement.columns(marker=String(), value=Integer()))

    options = {'test_marker': 'unchanged'}
    with session_for(client, cls) as session:
        event.listen(session, 'do_orm_execute', wrap)
        assert tuple(session.execute("SELECT '\\' AS marker, ? AS value", (7,), execution_options=options).one()) == (
            '\\',
            7,
        )
        assert session.execute('SELECT ?', (8,)).scalar() == 8
    assert len(probes) == 1
    assert markers == ([False] if cls is ClientSession else []) + [False, True, False]
    assert options == {'test_marker': 'unchanged'}


@pytest.mark.asyncio
async def test_async_shared_cte_keeps_actual_mode_and_recovers(async_client):
    probes = []
    mode_transport(async_client._engine.sync_engine, probes, {'value': 'NO_BACKSLASH_ESCAPES'})

    def wrap(state):
        if isinstance(state.statement, execution._LiteralSQL) and state.statement._literal_client is not None:
            state.statement = shared_cte(state.statement.columns(marker=String(), value=Integer()))

    async with async_client.session() as session:
        event.listen(session.sync_session, 'do_orm_execute', wrap)
        assert tuple((await session.execute("SELECT '\\' AS marker, ? AS value", (7,))).one()) == ('\\', 7)
        assert (await session.execute('SELECT ?', (8,))).scalar() == 8
    assert len(probes) == 1
