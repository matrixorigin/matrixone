# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
import asyncio
from contextlib import contextmanager, asynccontextmanager
from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import OperationalError

from matrixone import Client, AsyncClient
from matrixone.capabilities import PROBES, failed_observation, _CACHE_SLOT
from matrixone.connection_hooks import ConnectionHook
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
        self.info = {}
        self.server_id = "cn-one"
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
            rows = [self.identity + (self.server_id,)]
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
    assert len(engine.calls) == count + len(PROBES) + 2
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
    assert _CACHE_SLOT not in engine.info
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


@pytest.mark.parametrize(
    'message,state',
    [
        ("format 'arrow' is not supported", 'unsupported'),
        ('FORMAT "arrow" IS NOT SUPPORTED', 'unsupported'),
        ("format 'csv' is not supported", 'unknown'),
        ('permission denied: private stage token', 'unknown'),
    ],
)
def test_arrow_unsupported_requires_exact_server_evidence(message, state):
    error = OperationalError('private sql', {}, Exception(20300, message))
    value = failed_observation(error, ('release', 'sha'), 'arrow_load')
    assert value.state == state
    assert 'private' not in value.reason
    assert 'token' not in value.reason


def test_setting_policy_change_invalidates_cached_observations():
    client = Client()
    engine = client._engine = ProbeEngine()
    client.get_capabilities()
    count = len(engine.calls)
    hook = engine._matrixone_pool_settings = ConnectionHook()
    client.get_capabilities()
    assert len(engine.calls) == count + 1 + len(PROBES)
    count = len(engine.calls)
    client.get_capabilities()
    assert len(engine.calls) == count + 1
    with hook._settings_lock:
        hook._settings_revision += 1
    count = len(engine.calls)
    client.get_capabilities()
    assert len(engine.calls) == count + 1 + len(PROBES)


def test_setting_change_during_probe_does_not_publish_cache():
    client = Client()
    engine = client._engine = ProbeEngine()
    hook = engine._matrixone_pool_settings = ConnectionHook()
    execute = engine.exec_driver_sql

    def mutate(sql):
        result = execute(sql)
        if sql == PROBES['right_join']:
            with hook._settings_lock:
                hook._settings_revision += 1
        return result

    engine.exec_driver_sql = mutate
    with pytest.raises(RuntimeError, match='Connection changed'):
        client.get_capabilities()
    assert _CACHE_SLOT not in engine.info
    assert engine.checkedout == 0


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
    assert _CACHE_SLOT not in engine.info
    entered.clear()
    task = asyncio.create_task(client.get_capabilities())
    await asyncio.wait_for(entered.wait(), 5)
    client._reset_compatibility()
    resume.set()
    with pytest.raises(RuntimeError, match='Connection changed'):
        await task
    assert _CACHE_SLOT not in engine.info
    assert engine.checkedout == 0
    assert (await client.get_capabilities())['right_join'].state == 'supported'


@pytest.mark.asyncio
async def test_capability_timeout_releases_connection_without_cache():
    entered = asyncio.Event()
    held = []
    info = {}

    @asynccontextmanager
    async def connect():
        held.append(True)

        async def execute(sql):
            entered.set()
            await asyncio.Event().wait()

        try:
            yield SimpleNamespace(exec_driver_sql=execute, info=info)
        finally:
            held.clear()

    client = AsyncClient(query_timeout=0.05)
    client._engine = SimpleNamespace(connect=connect)
    task = asyncio.create_task(client.get_capabilities())
    await asyncio.wait_for(entered.wait(), 1)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(task, 1)
    assert not held
    assert _CACHE_SLOT not in info


def test_upload_does_not_apply_a_cached_negative_to_a_replaced_checkout():
    from matrixone.capabilities import check_arrow_load

    client = Client()
    engine = client._engine = ProbeEngine()
    engine.failure = OperationalError('probe', {}, Exception(20300, "format 'arrow' is not supported"))
    assert client.get_capabilities()['arrow_load'].state == 'unsupported'
    engine.identity = ('current-server', 'replacement-sha')
    engine.failure = None
    before = len(engine.calls)
    client.analytics._require_arrow_load()  # A cached negative is not authority.
    assert len(engine.calls) == before
    with engine.connect() as selected:
        check_arrow_load(selected)
    assert len(engine.calls) == before + 1
    assert engine.calls[-1].startswith('EXPLAIN LOAD DATA')
    assert engine.checkedout == 0


@pytest.mark.asyncio
async def test_actual_async_checkout_rejection_and_explicit_override_are_distinct():
    from matrixone.capabilities import check_arrow_load_async, require_arrow_override
    from matrixone.exceptions import QueryError

    client = AsyncClient()
    client._reset_compatibility({'arrow_load': 'disabled'})
    with pytest.raises(QueryError) as caught:
        require_arrow_override(client)
    assert caught.value.phase == 'capability' and caught.value.committed is False

    original = OperationalError('probe', {}, Exception(20300, "format 'arrow' is not supported"))

    async def execute(sql):
        raise original

    with pytest.raises(QueryError) as caught:
        await check_arrow_load_async(SimpleNamespace(exec_driver_sql=execute))
    assert caught.value.orig is original
    assert caught.value.phase == 'capability' and caught.value.committed is False


@pytest.mark.parametrize('code', [1064, 1193, 1235])
def test_arrow_explain_syntax_failure_does_not_certify_load_unsupported(code):
    error = OperationalError('probe', {}, Exception(code, 'EXPLAIN could not run'))
    assert failed_observation(error, ('server', 'sha'), 'arrow_load').state == 'unknown'


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
async def test_cache_tracks_physical_session_node_and_refresh_without_extra_roundtrips(asynchronous):
    engine = ProbeEngine()
    original = engine.exec_driver_sql
    setting = ['off']

    def execute(sql):
        if sql == PROBES['hnsw_index']:
            engine.calls.append(sql)
            return Result([('setting', setting[0])])
        return original(sql)

    engine.exec_driver_sql = execute
    if asynchronous:

        @asynccontextmanager
        async def connect():
            async def run(sql):
                return execute(sql)

            yield SimpleNamespace(exec_driver_sql=run, info=engine.info)

        client = AsyncClient()
        client._engine = SimpleNamespace(connect=connect)
    else:
        client = Client()
        client._engine = engine

    async def discover(**kwargs):
        value = client.get_capabilities(**kwargs)
        return await value if asynchronous else value

    assert (await discover())['hnsw_index'].state == 'disabled'
    first_info = engine.info
    count = len(engine.calls)
    assert (await discover())['hnsw_index'].state == 'disabled'
    assert len(engine.calls) == count + 1  # Existing one identity query, no added round trip.
    setting[0] = 'on'
    engine.info = {}  # Another pooled physical session, same node/build.
    assert (await discover())['hnsw_index'].state == 'supported'
    engine.info = first_info
    assert (await discover())['hnsw_index'].state == 'disabled'
    engine.server_id = 'cn-two'  # Transparent backend migration, same client socket/build.
    assert (await discover())['hnsw_index'].state == 'supported'
    engine.server_id = ''
    await discover()
    assert not engine.info
    engine.server_id = 'cn-two'
    setting[0] = 'off'
    assert (await discover(refresh=True))['hnsw_index'].state == 'disabled'
    assert len(engine.info) == 1  # One bounded slot, not a history of nodes/generations.


def test_unknown_node_does_not_cache_and_physical_replacement_clears_observation():
    from sqlalchemy import create_engine
    from sqlalchemy.pool import QueuePool
    from matrixone.capabilities import _connection_cache

    client = Client()
    engine = client._engine = ProbeEngine()
    engine.server_id = ''
    client.get_capabilities()
    count = len(engine.calls)
    client.get_capabilities()
    assert len(engine.calls) == count + 1 + len(PROBES)
    assert not engine.info
    pool = create_engine('sqlite://', poolclass=QueuePool, pool_size=1)
    try:
        with pool.connect() as connection:
            info, _ = _connection_cache(connection, client, None, ('version', 'build', 'cn'))
            info[_CACHE_SLOT] = 'owned observation'
        with pool.connect() as connection:
            assert _CACHE_SLOT in connection.info
            connection.invalidate()
            assert _CACHE_SLOT not in connection.info
    finally:
        pool.dispose()


def test_old_server_fallback_is_uncached_and_refresh_failure_discards_old_result():
    client = Client()
    engine = client._engine = ProbeEngine()
    original = engine.exec_driver_sql

    def old_server(sql):
        if '@@server_id' in sql:
            engine.calls.append(sql)
            raise OperationalError(sql, {}, Exception(1193, 'Unknown variable'))
        if sql.startswith('SELECT VERSION'):
            engine.calls.append(sql)
            return Result([engine.identity])
        return original(sql)

    engine.exec_driver_sql = old_server
    assert client.get_capabilities()['hnsw_index'].state == 'disabled'
    assert not engine.info
    engine.exec_driver_sql = original
    client.get_capabilities()
    assert _CACHE_SLOT in engine.info

    def broken(sql):
        if sql == PROBES['right_join']:
            raise RuntimeError('probe failed')
        return original(sql)

    engine.exec_driver_sql = broken
    with pytest.raises(RuntimeError, match='probe failed'):
        client.get_capabilities(refresh=True)
    assert _CACHE_SLOT not in engine.info
    engine.exec_driver_sql = original
    client.get_capabilities()
    engine.exec_driver_sql = lambda sql: (_ for _ in ()).throw(RuntimeError('metadata failed'))
    with pytest.raises(RuntimeError, match='metadata failed'):
        client.get_capabilities(refresh=True)
    assert _CACHE_SLOT not in engine.info


def test_ordinary_sql_does_not_discover_capabilities():
    from sqlalchemy import create_engine, event

    engine = create_engine('sqlite://')
    client = Client()
    client._engine = engine
    statements = []
    event.listen(
        engine, 'before_cursor_execute', lambda conn, cursor, sql, params, context, many: statements.append(sql)
    )
    try:
        assert client.execute('SELECT 42').rows == [(42,)]
        assert statements == ['SELECT 42']
    finally:
        engine.dispose()


def test_invalidated_metadata_is_not_old_server_fallback_and_policy_is_detached():
    client = Client()
    engine = client._engine = ProbeEngine()
    hook = engine._matrixone_pool_settings = ConnectionHook()
    hook.set_client(client)
    client.get_capabilities()
    key = engine.info[_CACHE_SLOT][0]
    assert key[1] == (hook._settings_token, hook._settings_revision)

    def invalid(sql):
        engine.info.clear()  # SQLAlchemy invalidation drops the physical-record information.
        raise OperationalError(sql, {}, Exception(1193, 'lost connection'), connection_invalidated=True)

    engine.exec_driver_sql = invalid
    with pytest.raises(OperationalError):
        client.get_capabilities()
    assert not engine.info


def test_real_invalidated_connection_preserves_original_metadata_error(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.engine import Connection

    engine = create_engine('sqlite://')
    client = Client()
    client._engine = engine
    execute = Connection.exec_driver_sql
    expected = OperationalError('metadata', {}, Exception(1193, 'lost'), connection_invalidated=True)

    def invalidate(connection, sql, *args, **kwargs):
        execute(connection, 'SELECT 1')
        connection.invalidate()
        raise expected

    monkeypatch.setattr(Connection, 'exec_driver_sql', invalidate)
    try:
        with pytest.raises(OperationalError) as caught:
            client.get_capabilities()
        assert caught.value is expected
    finally:
        engine.dispose()


def test_metadata_error_without_driver_code_preserves_original_error():
    client = Client()
    engine = client._engine = ProbeEngine()
    expected = OperationalError('metadata', {}, Exception())

    def fail(sql):
        raise expected

    engine.exec_driver_sql = fail
    with pytest.raises(OperationalError) as caught:
        client.get_capabilities()
    assert caught.value is expected


def test_real_two_record_pool_keeps_same_node_session_observations_separate(monkeypatch):
    from sqlalchemy import create_engine
    from sqlalchemy.engine import Connection
    from sqlalchemy.pool import QueuePool

    engine = create_engine('sqlite://', poolclass=QueuePool, pool_size=2, max_overflow=0)
    first, second = engine.connect(), engine.connect()
    first.info['setting'], second.info['setting'] = 'off', 'on'
    first.close()
    second.close()
    calls = []

    def execute(connection, sql, *args, **kwargs):
        calls.append(sql)
        if sql.startswith('SELECT VERSION'):
            return Result([('same-version', 'same-build', 'same-cn')])
        if sql == PROBES['right_join']:
            return Result([(None, 2)])
        return Result([('setting', connection.info['setting'])])

    monkeypatch.setattr(Connection, 'exec_driver_sql', execute)
    client = Client()
    client._engine = engine
    try:
        assert client.get_capabilities()['hnsw_index'].state == 'disabled'
        assert client.get_capabilities()['hnsw_index'].state == 'supported'
        count = len(calls)
        assert client.get_capabilities()['hnsw_index'].state == 'disabled'
        assert client.get_capabilities()['hnsw_index'].state == 'supported'
        assert len(calls) == count + 2
    finally:
        engine.dispose()
