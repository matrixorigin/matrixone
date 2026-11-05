# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Public cleanup deadlines retain one owner until real completion."""

import asyncio
import threading
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from matrixone import AsyncClient
from matrixone.async_analytics import AsyncFileRelation
from test_async_workers import settled


def relation_for(engine, query_timeout=1):
    client = AsyncClient(query_timeout=query_timeout, sql_log_mode='off')
    client._engine = engine
    plan = SimpleNamespace(schema=None, checks=(), server_sources=(), sources=('source',))
    relation = AsyncFileRelation(client.analytics, plan)
    relation._tables = [('main', 'owned')]
    return relation


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", ["timeout", "cancel"])
async def test_direct_close_deadline_retains_one_drop_and_joins_retry(tmp_path, interruption):
    from sqlalchemy import event
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import AsyncAdaptedQueuePool

    engine = create_async_engine(
        'sqlite+aiosqlite:///' + str(tmp_path / 'cleanup.db'),
        poolclass=AsyncAdaptedQueuePool,
        pool_size=1,
        max_overflow=0,
        pool_timeout=30,
    )
    drops = []

    @event.listens_for(engine.sync_engine, 'before_cursor_execute')
    def record(connection, cursor, statement, parameters, context, many):
        if statement.startswith('DROP TABLE'):
            drops.append(statement)

    held = None
    closing = None
    relation = relation_for(engine, query_timeout=0.03)
    try:
        async with engine.begin() as connection:
            await connection.exec_driver_sql('CREATE TABLE owned (id INT)')
            await connection.exec_driver_sql('CREATE TABLE unrelated (id INT)')
            await connection.exec_driver_sql('INSERT INTO unrelated VALUES (42)')
        held = await engine.connect()
        closing = asyncio.create_task(relation.close())
        if interruption == "timeout":
            done, _ = await asyncio.wait({closing}, timeout=1)
            assert closing in done, "SDK close did not enforce its operation deadline"
            with pytest.raises(asyncio.TimeoutError):
                closing.result()
        else:

            async def admitted():
                while not relation._manager._finalizers:
                    await settled()

            await asyncio.wait_for(admitted(), 1)
            closing.cancel()
            with pytest.raises(asyncio.CancelledError):
                await closing
        assert len(relation._manager._finalizers) == 1
        cleanup = next(iter(relation._manager._finalizers))
        assert not cleanup.done() and relation._tables == [('main', 'owned')]
        with pytest.raises(RuntimeError, match='clos'):
            relation.query()
        relation._manager.client.query_timeout = 1
        retry = asyncio.create_task(relation.close())
        await settled()
        assert relation._manager._finalizers == {cleanup}
        await held.close()
        held = None
        await asyncio.wait_for(retry, 1)
        assert relation._closed and not relation._tables
        assert len(drops) == 1
        async with engine.connect() as connection:
            assert (await connection.exec_driver_sql('SELECT id FROM unrelated')).scalar() == 42
            assert (
                await connection.exec_driver_sql("SELECT count(*) FROM sqlite_master WHERE name='owned'")
            ).scalar() == 0
        assert engine.pool.checkedout() == 0
    finally:
        if closing is not None and not closing.done():
            closing.cancel()
            await asyncio.gather(closing, return_exceptions=True)
        if held is not None:
            await held.close()
        await relation.close()
        await engine.dispose()


@pytest.mark.asyncio
async def test_context_cleanup_bound_preserves_primary_and_blocks_reconnect(monkeypatch):
    import matrixone.async_analytics as module

    monkeypatch.setattr(module, '_CLEANUP_TIMEOUT', 0.02, raising=False)
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def execute(sql):
        calls.append(sql)
        entered.set()
        await release.wait()
        return SimpleNamespace(close=lambda: None)

    @asynccontextmanager
    async def begin():
        yield SimpleNamespace(exec_driver_sql=execute)

    relation = relation_for(SimpleNamespace(begin=begin))
    primary = ValueError('caller failure')
    try:
        with pytest.raises(ValueError) as caught:
            async with relation:
                raise primary
        assert caught.value is primary and entered.is_set()
        assert isinstance(primary.cleanup_error, asyncio.TimeoutError) and primary.cleanup_pending
        assert len(relation._manager._finalizers) == 1
        with pytest.raises(RuntimeError, match='still closing'):
            relation._manager.reconnect()
        disconnect = asyncio.create_task(relation._manager.disconnect())
        await settled()
        assert not disconnect.done()
        release.set()
        await asyncio.wait_for(disconnect, 1)
        await relation.close()
        assert len(calls) == 1 and relation._closed
        relation._manager.reconnect()
    finally:
        release.set()
        await relation.close()


@pytest.mark.asyncio
async def test_failed_drop_retries_only_remaining_identity():
    failing = True
    calls = []

    async def execute(sql):
        calls.append(sql)
        if failing and '`remaining`' in sql:
            raise PermissionError('DROP denied')
        return SimpleNamespace(close=lambda: None)

    @asynccontextmanager
    async def begin():
        yield SimpleNamespace(exec_driver_sql=execute)

    relation = relation_for(SimpleNamespace(begin=begin))
    relation._tables = [('main', 'removed'), ('main', 'remaining')]
    with pytest.raises(PermissionError):
        await relation.close()
    assert relation._tables == [('main', 'remaining')]
    with pytest.raises(RuntimeError, match='clos'):
        await relation.sql()
    failing = False
    await relation.close()
    assert relation._closed and not relation._tables
    assert sum('`removed`' in sql for sql in calls) == 1
    assert sum('`remaining`' in sql for sql in calls) == 2


@pytest.mark.asyncio
async def test_retry_unlink_deadline_keeps_file_and_staging_admission(tmp_path, monkeypatch):
    import matrixone._analytics_workers as module

    client = AsyncClient(query_timeout=0.03, sql_log_mode='off')
    manager = client.analytics
    operation = await manager._workers.upload()
    path = tmp_path / 'owned.arrow'
    path.write_bytes(b'owned')
    operation.reserve_path(str(path))
    original = module.os.unlink

    def denied(path):
        raise PermissionError('read-only mount')

    monkeypatch.setattr(module.os, 'unlink', denied)
    operation.finish()
    with pytest.raises(PermissionError):
        await operation.wait_cleanup()
    entered, release = asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()

    def blocked(path):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5), 'unlink barrier not released'
        original(path)

    monkeypatch.setattr(module.os, 'unlink', blocked)
    retry = asyncio.create_task(manager.retry_cleanup(operation.id))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        done, _ = await asyncio.wait({retry}, timeout=1)
        assert retry in done, "SDK cleanup retry did not enforce its operation deadline"
        with pytest.raises(asyncio.TimeoutError):
            retry.result()
        assert path.exists() and len(manager.pending_operations()) == 1
        second = await manager._workers.upload()
        third = asyncio.create_task(manager._workers.upload())
        await settled()
        assert not third.done()
        release.set()
        await asyncio.wait_for(operation.wait_cleanup(), 1)
        replacement = await asyncio.wait_for(third, 1)
        for owned in (second, replacement):
            owned.finish()
            await asyncio.wait_for(owned.wait_cleanup(), 1)
        assert not path.exists() and manager.pending_operations() == ()
    finally:
        release.set()
        if not retry.done():
            retry.cancel()
        await asyncio.gather(retry, return_exceptions=True)
        await operation.wait_cleanup()
