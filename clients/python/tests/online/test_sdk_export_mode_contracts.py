# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Raw export quote boundaries belong to the executing connection's mode."""

import csv
import asyncio
import inspect
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import event
import sqlalchemy as sa
from sqlalchemy.util.concurrency import await_only

from matrixone import Client, AsyncClient
from matrixone.client import ResultSet
from matrixone.async_client import AsyncResultSet
from matrixone.exceptions import QueryError
from .test_sdk_quality_contracts import server, client, async_client  # noqa: F401

pytestmark = pytest.mark.skipif(os.environ.get('MO_SDK_ONLINE') != '1', reason='isolated server required')


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
@pytest.mark.parametrize('owner', ['client', 'session'])
@pytest.mark.parametrize('format', ['csv', 'jsonl'])
@pytest.mark.parametrize('mode', ['', 'NO_BACKSLASH_ESCAPES'])
async def test_export_mode_and_statement_share_one_checkout(server, kind, owner, format, mode):
    asynchronous = kind == 'async'
    client = (AsyncClient if asynchronous else Client)(
        pool_size=1, max_overflow=0, pool_timeout=0.5, query_timeout=3, sql_log_mode='off'
    )
    filename = 'export_mode_' + uuid4().hex + '.' + format
    local = Path(os.environ['MO_SDK_EXPORT_LOCAL_DIR']) / filename
    remote = os.environ['MO_SDK_EXPORT_DIR'].rstrip('/') + '/' + filename
    query = "SELECT LENGTH('\\') AS value" if mode else "SELECT LENGTH('quote\\' ; // 50%s ? :ignored') AS value"
    expected = 1 if mode else len("quote' ; // 50%s ? :ignored")
    calls = []
    engine = None

    def capture(connection, cursor, sql, *args):
        if sql == 'SELECT @@SESSION.sql_mode' or 'INTO OUTFILE' in sql:
            calls.append((sql, id(connection.connection)))

    def wrap_statement(state):
        # Real routing hooks may wrap a textual statement before selecting a bind.
        state.statement = state.statement.columns()

    async def resolve(value):
        return await value if inspect.isawaitable(value) else value

    async def export(executor):
        event.listen(engine, 'before_cursor_execute', capture)
        try:
            result = await resolve(getattr(executor.export, 'to_' + format)(remote, query + '; -- trailing'))
            if owner == 'client':
                assert isinstance(result, AsyncResultSet if asynchronous else ResultSet)
            else:
                result.close()
        finally:
            event.remove(engine, 'before_cursor_execute', capture)

    try:
        await resolve(client.connect(**server))
        engine = client._engine.sync_engine if asynchronous else client._engine
        if owner == 'client':
            await resolve(client.execute('SET sql_mode = ?', (mode,)))
            assert (await resolve(client.execute(query))).rows == [(expected,)]
            await export(client)
        elif asynchronous:
            async with client.session() as session:
                await session.execute('SET sql_mode = ?', (mode,))
                assert (await session.execute(query)).scalar() == expected
                event.listen(session.sync_session, 'do_orm_execute', wrap_statement)
                await export(session)
        else:
            with client.session() as session:
                session.execute('SET sql_mode = ?', (mode,))
                assert session.execute(query).scalar() == expected
                event.listen(session, 'do_orm_execute', wrap_statement)
                await export(session)
        assert len(calls) == 2 and calls[0][0] == 'SELECT @@SESSION.sql_mode'
        assert calls[0][1] == calls[1][1]
        assert engine.pool.checkedout() == 0
        if format == 'csv':
            with local.open(newline='') as output:
                assert list(csv.reader(output)) == [['value'], [str(expected)]]
        else:
            assert [json.loads(line) for line in local.read_text().splitlines()] == [{'value': expected}]
    finally:
        local.unlink(missing_ok=True)
        await resolve(client.disconnect())


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_mode_probe_failure_preserves_source_and_pool(client, async_client, kind):
    c = client if kind == 'sync' else async_client
    engine = c._engine if kind == 'sync' else c._engine.sync_engine
    filename = 'mode_failure_' + uuid4().hex + '.jsonl'
    local = Path(os.environ['MO_SDK_EXPORT_LOCAL_DIR']) / filename
    remote = os.environ['MO_SDK_EXPORT_DIR'].rstrip('/') + '/' + filename
    failure = sa.exc.OperationalError('SELECT @@SESSION.sql_mode', {}, RuntimeError('mode lookup failed'))
    probes = []
    prepared = c.execute("SET sql_mode='NO_BACKSLASH_ESCAPES'")
    if inspect.isawaitable(prepared):
        await prepared

    def fail_mode_probe(connection, cursor, sql, *args):
        assert 'INTO OUTFILE' not in sql
        if sql == 'SELECT @@SESSION.sql_mode':
            probes.append(1)
            raise failure

    event.listen(engine, 'before_cursor_execute', fail_mode_probe)
    try:
        with pytest.raises(QueryError) as error:
            value = c.export.to_jsonl(remote, "SELECT LENGTH('\\') AS value; -- trailing")
            if inspect.isawaitable(value):
                await value
        assert error.value.orig is failure
        assert probes == [1]
        assert not local.exists()
        assert engine.pool.checkedout() == 0
    finally:
        event.remove(engine, 'before_cursor_execute', fail_mode_probe)
        local.unlink(missing_ok=True)
    result = c.execute('SELECT 42')
    if inspect.isawaitable(result):
        result = await result
    assert result.rows == [(42,)]


@pytest.mark.asyncio
@pytest.mark.parametrize('terminal', ['cancel', 'timeout'])
async def test_cancellation_during_mode_probe_releases_checkout(async_client, terminal):
    c = async_client
    engine = c._engine.sync_engine
    entered, release = asyncio.Event(), asyncio.Event()
    filename = 'mode_cancel_' + uuid4().hex + '.jsonl'
    local = Path(os.environ['MO_SDK_EXPORT_LOCAL_DIR']) / filename
    remote = os.environ['MO_SDK_EXPORT_DIR'].rstrip('/') + '/' + filename
    await c.execute("SET sql_mode='NO_BACKSLASH_ESCAPES'")
    c.query_timeout = 0.1 if terminal == 'timeout' else 3

    async def blocked():
        entered.set()
        await release.wait()

    def block_mode_probe(connection, cursor, sql, *args):
        if sql == 'SELECT @@SESSION.sql_mode':
            await_only(blocked())

    event.listen(engine, 'before_cursor_execute', block_mode_probe)
    task = asyncio.create_task(c.export.to_jsonl(remote, "SELECT LENGTH('\\') AS value; -- trailing"))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        if terminal == 'cancel':
            task.cancel()
        with pytest.raises(asyncio.CancelledError if terminal == 'cancel' else asyncio.TimeoutError):
            await task
        assert engine.pool.checkedout() == 0
        assert not local.exists()
    finally:
        release.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        event.remove(engine, 'before_cursor_execute', block_mode_probe)
        local.unlink(missing_ok=True)
    assert (await c.execute('SELECT 42')).rows == [(42,)]


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_compiled_export_callable_is_evaluated_once(client, async_client, kind):
    c = client if kind == 'sync' else async_client
    calls = []

    def value():
        calls.append(1)
        return str(len(calls)) + '\\50%'

    statement = sa.select(sa.bindparam('value', callable_=value, type_=sa.String).label('value'))
    filename = 'mode_callable_' + uuid4().hex + '.jsonl'
    local = Path(os.environ['MO_SDK_EXPORT_LOCAL_DIR']) / filename
    remote = os.environ['MO_SDK_EXPORT_DIR'].rstrip('/') + '/' + filename
    try:
        result = c.export.to_jsonl(remote, statement)
        if inspect.isawaitable(result):
            await result
        assert calls == [1]
        assert [json.loads(line) for line in local.read_text().splitlines()] == [{'value': '1\\50%'}]
    finally:
        local.unlink(missing_ok=True)
