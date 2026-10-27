# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
import asyncio
from contextlib import contextmanager, asynccontextmanager
from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from matrixone import Client, AsyncClient
from matrixone.capabilities import PROBES
from matrixone.version import requires_version, VersionError


class Result:
    def __init__(self, rows):
        self.rows = rows
        self.closed = False

    def fetchone(self):
        return self.rows[0]

    def fetchall(self):
        return self.rows

    def close(self):
        self.closed = True


class ProbeEngine:
    def __init__(self):
        self.identity = ('8.0.30-MatrixOne-v4.2.5', 'release-sha')
        self.calls = []
        self.results = []
        self.checkedout = 0
        self.failure = None

    @contextmanager
    def connect(self):
        self.checkedout += 1
        try:
            yield self
        finally:
            self.checkedout -= 1

    def exec_driver_sql(self, sql):
        self.calls.append(sql)
        if sql.startswith('SELECT VERSION'):
            rows = [self.identity]
        elif self.failure:
            raise self.failure
        elif sql == PROBES['right_join']:
            rows = [(None, 2)]
        else:
            rows = [('setting', 'off')]
        result = Result(rows)
        self.results.append(result)
        return result


def test_versions_and_decorators_belong_to_client():
    a, b = Client(), Client()
    a.set_backend_version('1.0.0')
    b.set_backend_version('4.2.5')
    assert a.get_backend_version() == '1.0.0'
    assert b.get_backend_version() == '4.2.5'

    class Manager:
        def __init__(self, client):
            self.client = client

        @requires_version(min_version='4.0.0', feature_name='contract_test')
        def call(self):
            return 42

    with pytest.raises(VersionError):
        Manager(a).call()
    assert Manager(b).call() == 42


def test_capabilities_observe_identity_settings_and_cache():
    client = Client()
    engine = client._engine = ProbeEngine()
    client.set_backend_version('999.0.0')
    first = client.get_capabilities()
    assert first['right_join'].state == 'supported'
    assert first['hnsw_index'].state == 'disabled'
    assert first['arrow_load'].state == 'unknown'
    assert first['snapshot'].state == 'unknown'
    with pytest.raises(FrozenInstanceError):
        first['right_join'].state = 'unknown'
    first.clear()
    count = len(engine.calls)
    assert client.get_capabilities()['right_join'].state == 'supported'
    assert len(engine.calls) == count + 1  # identity is rechecked; data probes cached
    engine.identity = ('8.0.30-MatrixOne-v', 'new-sha')
    assert client.get_capabilities()['right_join'].server_identity == engine.identity
    assert len(engine.calls) == count + 6
    assert engine.checkedout == 0
    assert all(result.closed for result in engine.results)
    assert not any(sql.startswith(('SET', 'CREATE', 'LOAD')) for sql in engine.calls)


@pytest.mark.parametrize('errno,state', [(1064, 'unsupported'), (1142, 'unknown'), (2006, 'unknown')])
def test_probe_error_classification_does_not_leak_server_text(errno, state):
    client = Client()
    engine = client._engine = ProbeEngine()
    engine.failure = OperationalError('private sql', {}, Exception(errno, 'secret credential'))
    result = client.get_capabilities()
    assert result['right_join'].state == state
    assert 'secret' not in result['right_join'].reason
    assert engine.checkedout == 0


def test_unexpected_probe_error_propagates_without_cache():
    client = Client()
    engine = client._engine = ProbeEngine()
    engine.failure = TypeError('implementation error')
    with pytest.raises(TypeError):
        client.get_capabilities()
    assert client._capability_cache is None
    assert engine.checkedout == 0


def test_overrides_are_explicit_and_generation_scoped():
    client = Client()
    client._engine = ProbeEngine()
    client._reset_compatibility({'arrow_load': 'supported'})
    assert client.get_capabilities()['arrow_load'].source == 'override'
    client._reset_compatibility()
    assert client.get_capabilities()['arrow_load'].state == 'unknown'
    with pytest.raises(ValueError):
        client._reset_compatibility({'typo': 'supported'})


@pytest.mark.asyncio
async def test_async_cancel_and_reconnect_never_publish_stale_observations():
    entered, resume = asyncio.Event(), asyncio.Event()
    engine = ProbeEngine()

    @asynccontextmanager
    async def connect():
        engine.checkedout += 1

        async def execute(sql):
            if sql == PROBES['right_join']:
                entered.set()
                await resume.wait()
            return engine.exec_driver_sql(sql)

        try:
            yield SimpleNamespace(exec_driver_sql=execute)
        finally:
            engine.checkedout -= 1

    client = AsyncClient()
    client._engine = SimpleNamespace(connect=connect)
    task = asyncio.create_task(client.get_capabilities())
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert engine.checkedout == 0
    assert client._capability_cache is None
    entered.clear()
    task = asyncio.create_task(client.get_capabilities())
    await asyncio.wait_for(entered.wait(), 5)
    client._reset_compatibility()
    resume.set()
    with pytest.raises(RuntimeError, match='Connection changed'):
        await task
    assert client._capability_cache is None
    assert engine.checkedout == 0
    assert (await client.get_capabilities())['right_join'].state == 'supported'


@pytest.mark.asyncio
async def test_capability_timeout_releases_connection_without_cache():
    entered = asyncio.Event()
    held = []

    @asynccontextmanager
    async def connect():
        held.append(True)

        async def execute(sql):
            entered.set()
            await asyncio.Event().wait()

        try:
            yield SimpleNamespace(exec_driver_sql=execute)
        finally:
            held.clear()

    client = AsyncClient(query_timeout=0.05)
    client._engine = SimpleNamespace(connect=connect)
    task = asyncio.create_task(client.get_capabilities())
    await asyncio.wait_for(entered.wait(), 1)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(task, 1)
    assert not held
    assert client._capability_cache is None
