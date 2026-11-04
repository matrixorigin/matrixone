# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Exercise the real driver result readers without a socket or file access."""

import os
import struct
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pymysql.protocol import MysqlPacket
from pymysql.err import OperationalError

from matrixone._local_infile import install_guard, local_file_authority


@pytest.fixture(params=['pymysql', 'aiomysql'])
def driver_kind(request):
    return request.param


@pytest.mark.asyncio
async def test_real_driver_local_packets_require_exact_single_use_authority(driver_kind, monkeypatch):
    import pymysql.connections as sync
    import aiomysql.connection as native_async

    driver = sync.Connection(defer_connect=True) if driver_kind == 'pymysql' else native_async.Connection()
    module = sync if driver_kind == 'pymysql' else native_async
    opened, packets = [], []
    driver.write_packet = Mock()
    if driver_kind == 'pymysql':

        def send(filename, connection):
            opened.append(filename)

        if hasattr(module, '_send_local_file'):
            monkeypatch.setattr(module, '_send_local_file', send)
        else:

            class Sender:
                def __init__(self, filename, connection):
                    self.filename = filename

                def send_data(self):
                    opened.append(self.filename)

            monkeypatch.setattr(module, 'LoadLocalFile', Sender)
    else:

        class Sender:
            def __init__(self, filename, connection):
                self.filename = filename

            async def send_data(self):
                opened.append(self.filename)

        monkeypatch.setattr(module, 'LoadLocalFile', Sender)

    def packet(*args, **kwargs):
        packet_type = args[0] if args else kwargs.get('packet_type', MysqlPacket)
        return packet_type(packets.pop(0), 'utf8')

    async def async_packet(*args, **kwargs):
        return packet(*args, **kwargs)

    driver._read_packet = packet if driver_kind == 'pymysql' else async_packet
    install_guard(driver)
    connection = SimpleNamespace(connection=SimpleNamespace(driver_connection=driver))
    requested = os.fsencode('/owned/界.csv')
    ok = b'\x00\x00\x00\x02\x00\x00\x00'

    async def read():
        result = driver._read_query_result()
        if driver_kind == 'aiomysql':
            await result

    # An ordinary query and a wrong server filename must send no file bytes.
    for authorized in (None, '/different.csv'):
        packets[:] = [b'\xfb' + requested, ok]
        with pytest.raises(OperationalError, match='Unapproved LOCAL'):
            with local_file_authority(connection, authorized):
                await read()
        assert opened == [] and not driver._local_infile
        driver.write_packet.assert_not_called()
        assert driver._matrixone_local_guard.path is None

    packets[:] = [b'\xfb' + requested, ok]
    with local_file_authority(connection, os.fsdecode(requested)):
        await read()
        assert opened == [requested] and not driver._local_infile
        # A second result/nextset cannot replay the authority.
        driver.write_packet.reset_mock()
        packets[:] = [b'\xfb' + requested, ok]
        with pytest.raises(OperationalError):
            await read()
        driver.write_packet.assert_not_called()
    assert driver._matrixone_local_guard.path is None

    # 0xfb is also a legitimate NULL value after the result's first packet.
    fields = b''.join(bytes([len(value)]) + value for value in (b'def', b'', b'', b'', b'value', b''))
    fields += b'\x0c' + struct.pack('<HIBHBxx', 45, 256, 253, 0, 0)
    eof = b'\xfe\x00\x00\x02\x00'
    packets[:] = [b'\x01', fields, eof, b'\xfb', eof]
    await read()
    assert tuple(driver._result.rows) == ((None,),)
    assert not driver._local_infile and not driver._matrixone_local_guard.reading


@pytest.mark.parametrize('blocked', ['_read_packet', '_read_query_result'])
def test_unsupported_driver_mutation_aborts_setup_preserving_original_failure(blocked):
    class Restricted:
        _local_infile = False

        def __init__(self):
            self.closed = False

        def __setattr__(self, name, value):
            if name == blocked:
                raise AttributeError('unsupported driver shape')
            object.__setattr__(self, name, value)

        def _read_query_result(self):
            pass

        def _read_packet(self):
            pass

        def close(self):
            self.closed = True

    driver = Restricted()
    with pytest.raises(AttributeError, match='unsupported driver shape'):
        install_guard(driver)
    assert driver.closed


@pytest.mark.parametrize('skip_error_events', [False, True])
def test_selected_execution_owner_revokes_authority_when_later_hook_raises(skip_error_events):
    import sqlite3
    import sqlalchemy as sa
    from matrixone._session_execution import _LiteralSQL

    class Physical(sqlite3.Connection):
        pass

    engine = sa.create_engine('sqlite://', creator=lambda: sqlite3.connect(':memory:', factory=Physical))
    try:
        with engine.connect() as connection:
            raw = connection.connection.driver_connection
            raw._local_infile = False
            raw._matrixone_local_guard = SimpleNamespace(path=None, consumed=False, reading=False)

            def fail(conn, cursor, statement, parameters, context, many):
                assert raw._matrixone_local_guard.path == b'/owned/file.csv'
                raise RuntimeError('later hook rejected statement')

            sa.event.listen(engine, 'before_cursor_execute', fail)
            statement = _LiteralSQL('SELECT 1').execution_options(
                _matrixone_local_path='/owned/file.csv',
                skip_user_error_events=skip_error_events,
            )
            with pytest.raises(RuntimeError, match='later hook'):
                connection.execute(statement)
            assert raw._matrixone_local_guard.path is None and not raw._local_infile
            sa.event.remove(engine, 'before_cursor_execute', fail)
            assert connection.exec_driver_sql('SELECT 7').scalar() == 7
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize('option', [None, False])
async def test_async_guard_precedes_driver_startup_queries(option, monkeypatch):
    import aiomysql.connection as module
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.exc import OperationalError as SQLAlchemyError
    from matrixone._local_infile import configure_engine, connection_options

    created, opened = [], []

    async def startup(driver):
        created.append(driver)
        await driver._read_query_result()

    async def packet(driver, *args, **kwargs):
        return MysqlPacket(b'\xfb/not-the-requested-file', 'utf8')

    class Sender:
        def __init__(self, filename, connection):
            opened.append(filename)

    monkeypatch.setattr(module.Connection, '_connect', startup)
    monkeypatch.setattr(module.Connection, '_read_packet', packet)
    monkeypatch.setattr(module, 'LoadLocalFile', Sender)
    engine = create_async_engine('mysql+aiomysql://unused@localhost/test', connect_args=connection_options(option))
    configure_engine(engine, option)
    try:
        with pytest.raises(SQLAlchemyError) as caught:
            async with engine.connect():
                pytest.fail('unapproved startup file request must not connect')
        assert caught.value.orig.args[0] == 2013
        assert opened == [] and len(created) == 1
        assert created[0]._writer is None and not created[0]._local_infile
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_canceling_async_connection_startup_closes_the_constructed_driver(monkeypatch):
    import asyncio
    import aiomysql.connection as module
    from sqlalchemy.ext.asyncio import create_async_engine
    from matrixone._local_infile import configure_engine, connection_options

    entered = asyncio.Event()
    created = []

    async def startup(driver):
        created.append(driver)
        assert driver._matrixone_local_guard is not None
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(module.Connection, '_connect', startup)
    engine = create_async_engine('mysql+aiomysql://unused@localhost/test', connect_args=connection_options(None))
    configure_engine(engine, None)

    async def checkout():
        async with engine.connect():
            pytest.fail('stalled startup must not connect')

    task = asyncio.create_task(checkout())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(created) == 1 and created[0]._writer is None
        assert not created[0]._local_infile
    finally:
        if not task.done():
            task.cancel()
        await engine.dispose()


@pytest.fixture
def disabled_cyclic_gc():
    import gc

    enabled = gc.isenabled()
    gc.collect()
    gc.disable()
    try:
        yield
    finally:
        if enabled:
            gc.enable()


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['install', 'read', 'unwind'])
async def test_guard_releases_physical_driver_without_cyclic_gc(driver_kind, phase, disabled_cyclic_gc, monkeypatch):
    import asyncio
    import weakref
    from pymysql.connections import Connection
    from aiomysql.connection import Connection as AsyncConnection

    connection_type = Connection if driver_kind == 'pymysql' else AsyncConnection
    failure = RuntimeError if driver_kind == 'pymysql' else asyncio.CancelledError

    def packet(driver, *args, **kwargs):
        if phase == 'unwind':
            raise failure('interrupted result')
        return MysqlPacket(b'\x00\x00\x00\x02\x00\x00\x00', 'utf8')

    async def async_packet(driver, *args, **kwargs):
        return packet(driver, *args, **kwargs)

    # Patch the inherited method, not the instance: monkeypatch's undo record
    # must not keep a bound method or the physical connection alive.
    monkeypatch.setattr(connection_type, '_read_packet', packet if driver_kind == 'pymysql' else async_packet)

    async def exercise():
        driver = Connection(defer_connect=True) if driver_kind == 'pymysql' else AsyncConnection()
        reference = weakref.ref(driver)
        install_guard(driver)
        if phase != 'install':

            async def read():
                result = driver._read_query_result()
                if driver_kind == 'aiomysql':
                    await result

            if phase == 'unwind':
                with pytest.raises(failure, match='interrupted result'):
                    await read()
            else:
                await read()
            assert '_read_packet' not in vars(driver)
            assert not driver._matrixone_local_guard.reading and not driver._local_infile
        return reference

    reference = await exercise()
    assert reference() is None, 'guard must not retain a physical connection until cyclic GC'


@pytest.mark.asyncio
async def test_unfinished_arrow_stream_releases_real_driver_without_draining(
    driver_kind, disabled_cyclic_gc, monkeypatch
):
    import weakref
    import pymysql.connections as sync
    import aiomysql.connection as native_async
    from matrixone.analytics import ArrowStream
    from matrixone.async_analytics import AsyncArrowStream

    module = sync if driver_kind == 'pymysql' else native_async
    events = []

    def forbid_drain(*args, **kwargs):
        pytest.fail('early stream cleanup must not consume unread rows')

    monkeypatch.setattr(module.MySQLResult, '_finish_unbuffered_query', forbid_drain)

    class SyncLease:
        def __init__(self, driver):
            self.driver = driver

        def invalidate(self):
            events.append('invalidate')
            self.driver._force_close()

        def close(self):
            events.append('close')
            self.driver = None

    class AsyncLease:
        async def invalidate(self):
            events.append('invalidate')

        async def close(self):
            events.append('close')

    async def exercise():
        driver = sync.Connection(defer_connect=True) if driver_kind == 'pymysql' else native_async.Connection()
        reference = weakref.ref(driver)
        install_guard(driver)
        result = module.MySQLResult(driver)
        result.unbuffered_active = True
        driver._result = result
        cursor_type = __import__(
            'pymysql.cursors' if driver_kind == 'pymysql' else 'aiomysql.cursors', fromlist=['SSCursor']
        ).SSCursor
        cursor = cursor_type(driver)
        cursor._result = result
        if driver_kind == 'pymysql':
            stream = ArrowStream.__new__(ArrowStream)
            stream._closed = stream._complete = False
            stream._driver, stream._cursor, stream._raw = driver, cursor, SyncLease(driver)
            stream._old_timeouts = (driver._read_timeout, driver._write_timeout)
            stream.close()
            stream.close()
        else:

            async def cleanup(awaitable, primary):
                await awaitable

            stream = AsyncArrowStream.__new__(AsyncArrowStream)
            stream._closed = stream._complete = False
            stream._driver, stream._cursor, stream._connection = driver, cursor, AsyncLease()
            stream._lease = SimpleNamespace(release=lambda: events.append('release'))
            stream._manager = SimpleNamespace(_streams={stream}, _cleanup=cleanup)
            await stream.close()
            await stream.close()
            assert not stream._manager._streams
            assert driver._writer is None
        assert not result.unbuffered_active
        return reference

    reference = await exercise()
    assert events == (['invalidate', 'close'] if driver_kind == 'pymysql' else ['release', 'invalidate', 'close'])
    assert reference() is None, 'returning a pool lease must also release the physical driver'
