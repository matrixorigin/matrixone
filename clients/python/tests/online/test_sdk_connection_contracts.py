# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import asyncio
import os
import ssl
from unittest.mock import Mock

import pytest

from matrixone import Client
from matrixone.async_client import AsyncClient
from matrixone.exceptions import ConnectionError

pytestmark = pytest.mark.skipif(os.environ.get('MO_SDK_ONLINE') != '1', reason='isolated transport deployment required')


def options(port=None):
    return {
        'host': os.environ.get('MO_HOST', '127.0.0.1'),
        'port': port or int(os.environ.get('MO_PORT', '6001')),
        'user': os.environ.get('MO_USER', 'root'),
        'password': os.environ.get('MO_PASSWORD', '111'),
        'database': os.environ.get('MO_DATABASE', 'mo_sdk_regression'),
        'on_connect': [],
    }


def test_required_tls_real_certificate_and_peer():
    ca = os.environ.get('MO_TLS_CA')
    assert ca, 'MO_TLS_CA must identify the isolated transport test CA'
    client = Client(logger=Mock(), connection_timeout=3, query_timeout=3)
    client.connect(**options(int(os.environ.get('MO_TLS_PORT', '6001'))), ssl_mode='required', ssl_ca=ca)
    try:
        assert client.execute('SELECT 1').rows == [(1,)]
        with client._engine.connect() as conn:
            raw = conn.connection
            driver = getattr(raw, 'driver_connection', None) or raw.connection
            assert isinstance(driver._sock, ssl.SSLSocket)
    finally:
        client.disconnect()


def test_required_tls_rejects_hostname_mismatch():
    params = options(int(os.environ.get('MO_TLS_PORT', '6001')))
    params['host'] = 'localhost'  # Local cert only has IP:127.0.0.1.
    client = Client(logger=Mock(), connection_timeout=3, query_timeout=3)
    with pytest.raises(ConnectionError, match='(?i)certificate verify failed|CERTIFICATE_VERIFY_FAILED'):
        client.connect(**params, ssl_mode='required', ssl_ca=os.environ['MO_TLS_CA'])
    assert not client.connected()


def test_required_tls_rejects_untrusted_certificate():
    client = Client(logger=Mock(), connection_timeout=3, query_timeout=3)
    with pytest.raises(ConnectionError, match='(?i)certificate verify failed|CERTIFICATE_VERIFY_FAILED'):
        client.connect(**options(int(os.environ.get('MO_TLS_PORT', '6001'))), ssl_mode='required')
    assert not client.connected()


def test_required_tls_rejects_non_tls_peer():
    client = Client(logger=Mock(), connection_timeout=3, query_timeout=3)
    with pytest.raises(ConnectionError, match="(TLS|SSL) is required.*(does not advertise|doesn't support)"):
        client.connect(
            **options(int(os.environ.get('MO_PLAIN_PORT', '6002'))), ssl_mode='required', ssl_ca=os.environ['MO_TLS_CA']
        )
    assert not client.connected()


def test_disabled_tls_stays_plaintext():
    client = Client(logger=Mock(), connection_timeout=3, query_timeout=3)
    client.connect(**options(), ssl_mode='disabled')
    try:
        with client._engine.connect() as conn:
            raw = conn.connection
            driver = getattr(raw, 'driver_connection', None) or raw.connection
            assert not isinstance(driver._sock, ssl.SSLSocket)
    finally:
        client.disconnect()


def test_authentication_failure_clears_state():
    client = Client(logger=Mock(), connection_timeout=3, query_timeout=3)
    params = options()
    params['password'] = 'synthetic-invalid-password-marker'
    with pytest.raises(ConnectionError) as result:
        client.connect(**params, ssl_mode='disabled')
    assert params['password'] not in str(result.value)
    assert not client.connected()


@pytest.mark.asyncio
async def test_async_timeout_releases_pool_and_reuses_connection_capacity():
    client = AsyncClient(logger=Mock(), query_timeout=3, connection_timeout=3)
    await client.connect(**options())
    try:
        client.query_timeout = 0.05
        with pytest.raises(asyncio.TimeoutError):
            await client.execute('SELECT SLEEP(1)')
        assert client._engine.sync_engine.pool.checkedout() == 0
        client.query_timeout = 3
        assert (await client.execute('SELECT 1')).rows == [(1,)]
    finally:
        await client.disconnect()


@pytest.mark.asyncio
async def test_async_cancellation_releases_pool():
    client = AsyncClient(logger=Mock(), query_timeout=3, connection_timeout=3)
    await client.connect(**options())
    try:
        task = asyncio.create_task(client.execute('SELECT SLEEP(1)'))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client._engine.sync_engine.pool.checkedout() == 0
        assert (await client.execute('SELECT 1')).rows == [(1,)]
    finally:
        await client.disconnect()
