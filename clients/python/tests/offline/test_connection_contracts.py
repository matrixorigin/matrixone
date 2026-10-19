# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import asyncio
import ssl
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from matrixone import Client
from matrixone.async_client import AsyncClient
from matrixone._connection_options import TLSRequiredConnection, tls_options
from matrixone.exceptions import ConnectionError


def test_failed_probe_disposes_engine_and_clears_state(monkeypatch):
    client = Client(logger=Mock())
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.exec_driver_sql.side_effect = RuntimeError('unreachable')
    monkeypatch.setattr(client, '_create_engine', lambda: engine)
    with pytest.raises(ConnectionError, match='unreachable'):
        client.connect(database='test', on_connect=[])
    assert not client.connected()
    engine.dispose.assert_called_once()
    assert not any(call.kwargs.get('success') is True for call in client.logger.log_connection.call_args_list)


def test_success_requires_authenticated_query(monkeypatch):
    client = Client(logger=Mock(), connection_timeout=17)
    engine = MagicMock()
    monkeypatch.setattr(client, '_create_engine', lambda: engine)
    monkeypatch.setattr(client, '_detect_backend_version', lambda: None)
    client.connect(database='test', on_connect=[])
    engine.connect.return_value.__enter__.return_value.exec_driver_sql.assert_called_once_with('SELECT 1')
    assert client._connection_params['connect_timeout'] == 17
    assert client.connected()
    client.disconnect()


@pytest.mark.parametrize('mode', ['disabled', 'preferred', 'required'])
def test_security_policy_context(mode):
    options = tls_options({'ssl_mode': mode})
    if mode == 'required':
        context = options['ssl']
        assert context.verify_mode == ssl.CERT_REQUIRED
        assert context.check_hostname
    elif mode == 'disabled':
        assert options == {'ssl_disabled': True}
    else:
        assert options == {}


def test_required_tls_refused_before_authentication():
    import pymysql

    connection = TLSRequiredConnection.__new__(TLSRequiredConnection)
    connection.server_capabilities = 0
    with pytest.raises(pymysql.err.OperationalError, match='does not advertise TLS'):
        connection._request_authentication()


def test_connection_error_redacts_password_and_suppresses_traceback_chain(monkeypatch):
    client = Client(logger=Mock())
    secret = 'synthetic-password-marker'
    monkeypatch.setattr(client, '_create_engine', Mock(side_effect=RuntimeError('1049 ' + secret)))
    with pytest.raises(ConnectionError) as result:
        client.connect(database='test', password=secret, on_connect=[])
    assert secret not in str(result.value)
    assert result.value.__suppress_context__
    assert secret not in str(client.logger.log_error.call_args)
    assert '-p ' in str(result.value)


@pytest.mark.parametrize('value', [0, -1, True, float('nan'), float('inf')])
def test_invalid_timeouts_rejected_without_engine(value, monkeypatch):
    client = Client(logger=Mock(), query_timeout=value)
    factory = Mock()
    monkeypatch.setattr(client, '_create_engine', factory)
    with pytest.raises(ValueError):
        client.connect(database='test', on_connect=[])
    factory.assert_not_called()


@pytest.mark.asyncio
async def test_async_deadline_cancels_the_operation():
    client = AsyncClient(logger=Mock(), query_timeout=0.02)
    finished = asyncio.Event()

    async def stalled(*args):
        try:
            await asyncio.Event().wait()
        finally:
            finished.set()

    client._execute = stalled
    with pytest.raises(asyncio.TimeoutError):
        await client.execute('SELECT 1')
    assert finished.is_set()


@pytest.mark.asyncio
async def test_async_explicit_cancellation_propagates():
    client = AsyncClient(logger=Mock(), query_timeout=10)
    started = asyncio.Event()

    async def stalled(*args):
        started.set()
        await asyncio.Event().wait()

    client._execute = stalled
    task = asyncio.create_task(client.execute('SELECT 1'))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_async_failed_probe_clears_engine(monkeypatch):
    client = AsyncClient(logger=Mock(), connection_timeout=0.02)
    engine = MagicMock()
    engine.dispose = AsyncMock()

    class Context:
        async def __aenter__(self):
            await asyncio.Event().wait()

        async def __aexit__(self, *args):
            pass

    engine.begin.return_value = Context()
    monkeypatch.setattr(client, '_create_async_engine', lambda: engine)
    with pytest.raises(ConnectionError):
        await client.connect(database='test', on_connect=[])
    assert not client.connected()
    engine.dispose.assert_awaited_once()


def test_driver_receives_sync_timeout_and_charset(monkeypatch):
    import matrixone.client as module

    client = Client(logger=Mock(), connection_timeout=12, query_timeout=7)
    client._connection_params = {
        'host': 'localhost',
        'port': 6001,
        'user': 'test',
        'password': 'synthetic',
        'database': 'test',
        'connect_timeout': 12,
        'charset': 'utf8mb4',
        'ssl_mode': 'disabled',
    }
    factory = Mock(return_value=MagicMock())
    monkeypatch.setattr(module, 'create_engine', factory)
    client._create_engine()
    assert factory.call_args.kwargs['connect_args'] == {
        'connect_timeout': 12,
        'read_timeout': 7,
        'write_timeout': 7,
        'charset': 'utf8mb4',
        'ssl_disabled': True,
    }


def test_async_driver_receives_connect_timeout(monkeypatch):
    import matrixone.async_client as module

    client = AsyncClient(logger=Mock(), connection_timeout=13)
    client._connection_params = {
        'host': 'localhost',
        'port': 6001,
        'user': 'test',
        'password': 'synthetic',
        'database': 'test',
        'connect_timeout': 13,
        'charset': 'utf8mb4',
    }
    factory = Mock()
    monkeypatch.setattr(module, 'create_async_engine', factory)
    client._create_async_engine()
    assert factory.call_args.kwargs['connect_args']['connect_timeout'] == 13


def test_failed_disconnect_does_not_leave_connected_state():
    client = Client(logger=Mock())
    client._engine = Mock()
    client._engine.dispose.side_effect = RuntimeError('dispose failure')
    with pytest.raises(RuntimeError, match='dispose failure'):
        client.disconnect()
    assert not client.connected()
