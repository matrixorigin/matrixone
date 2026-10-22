# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Real drivers receiving a MySQL ERR greeting, without replacing engine factories."""

import socket
import threading
from contextlib import contextmanager
from unittest.mock import Mock

import pytest

from matrixone import Client
from matrixone.async_client import AsyncClient
from matrixone.exceptions import ConnectionError


@contextmanager
def error_peer(password):
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    listener.settimeout(5)
    failures = []

    def serve():
        try:
            with listener.accept()[0] as connection:
                payload = b'\xff\x15\x04#28000' + ('Access denied; echoed password: ' + password).encode('utf8')
                connection.sendall(len(payload).to_bytes(3, 'little') + b'\x00' + payload)
        except BaseException as error:
            failures.append(error)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        yield listener.getsockname()[1]
    finally:
        thread.join(6)
        listener.close()
        assert not thread.is_alive()
        assert not failures


def assert_redacted(client, error, password):
    # Inspect the actual logged exception, before Mock's own repr escaping.
    logged = client.logger.log_error.call_args.args[0]
    for value in {password, repr(password)[1:-1], password.encode('unicode_escape').decode('ascii')}:
        assert value not in str(error)
        assert value not in str(logged)
    assert 'Access denied' in str(error)
    assert error.__suppress_context__
    assert not client.connected()


PASSWORDS = [
    'synthetic-plain-marker',
    r'synthetic\backslash-marker',
    'synthetic\\quote\'"-marker',
    'synthetic\ncontrol\\-marker',
]


@pytest.mark.parametrize('password', PASSWORDS)
def test_public_sync_connection_redacts_driver_representation(password):
    client = Client(logger=Mock(), connection_timeout=2)
    with error_peer(password) as port:
        with pytest.raises(ConnectionError) as error:
            client.connect(
                host='127.0.0.1', port=port, user='probe', password=password, database='probe', on_connect=[]
            )
    assert_redacted(client, error.value, password)


@pytest.mark.asyncio
@pytest.mark.parametrize('password', PASSWORDS)
async def test_public_async_connection_redacts_driver_representation(password):
    client = AsyncClient(logger=Mock(), connection_timeout=2)
    with error_peer(password) as port:
        with pytest.raises(ConnectionError) as error:
            await client.connect(
                host='127.0.0.1', port=port, user='probe', password=password, database='probe', on_connect=[]
            )
    assert_redacted(client, error.value, password)


@pytest.mark.parametrize('password', ["雪\\quote'", '雪\n\\quote"', "both\\'\"\t雪"])
@pytest.mark.parametrize('message', ["driver's error: %s", 'driver "error": %s'])
def test_redaction_handles_quote_choice_and_printable_unicode(password, message):
    from pymysql.err import OperationalError
    from sqlalchemy.exc import OperationalError as WrappedError

    from matrixone._connection_options import redact_error

    error = WrappedError(None, None, OperationalError(1045, message % password))
    redacted = redact_error(error, password)
    assert '***' in redacted
    assert '雪' not in redacted
    assert 'driver' in redacted
