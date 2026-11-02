"""Real driver encoding without transport; no process-global monkey patches."""

import gc
import weakref
from types import SimpleNamespace

import aiomysql
import pytest
from pymysql.constants import SERVER_STATUS
from sqlalchemy import event
from sqlalchemy.ext.asyncio import create_async_engine

from matrixone.sqlalchemy_ext.dialect import adapt_engine_dialect, _prepare_async_binary


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', [0, SERVER_STATUS.SERVER_STATUS_NO_BACKSLASH_ESCAPES])
@pytest.mark.parametrize('charset', ['utf8mb4', 'gbk'])
async def test_actual_async_driver_binary_encoding_is_local_and_mode_independent(mode, charset):
    original_type_method = aiomysql.Connection.escape
    original_module_alias = aiomysql.connection.escape_bytes_prefixed
    raw = aiomysql.Connection(charset=charset)
    try:
        raw.server_status = mode
        expected = [raw.escape(value) for value in ('ordinary', 7, None)]
        _prepare_async_binary(SimpleNamespace(_connection=raw), None, None)
        installed = raw.escape
        _prepare_async_binary(SimpleNamespace(driver_connection=raw), None, None)
        assert raw.escape is installed
        assert [raw.escape(value) for value in ('ordinary', 7, None)] == expected
        for value in (b'\x00\xff', bytearray(b'\x00\xff'), memoryview(b'\x00\xff')):
            assert raw.escape(value) == "X'00ff'"
        assert aiomysql.Connection.escape is original_type_method
        assert aiomysql.connection.escape_bytes_prefixed is original_module_alias
    finally:
        raw.close()


@pytest.mark.asyncio
async def test_async_binary_adapter_is_per_connection_and_collectible():
    first = aiomysql.Connection()
    second = aiomysql.Connection()
    try:
        _prepare_async_binary(first, None, None)
        assert first.escape is not second.escape
        assert getattr(second.escape, '__func__', None) is aiomysql.Connection.escape
        reference = weakref.ref(first)
    finally:
        first.close()
        second.close()
    del first
    gc.collect()
    assert reference() is None


@pytest.mark.asyncio
async def test_async_dialect_adoption_installs_one_first_pool_checkout_listener():
    engine = create_async_engine('mysql+aiomysql://user:password@localhost/db')
    try:
        adapt_engine_dialect(engine.sync_engine)
        adapt_engine_dialect(engine.sync_engine.execution_options(compiled_cache={}))
        adapt_engine_dialect(engine.sync_engine)
        listeners = list(engine.sync_engine.pool.dispatch.checkout)
        assert listeners[0] is _prepare_async_binary
        assert listeners.count(_prepare_async_binary) == 1
        assert event.contains(engine.sync_engine.pool, 'checkout', _prepare_async_binary)
        assert engine.dialect._send_false_to_ping is True
        await engine.dispose()
        adapt_engine_dialect(engine.sync_engine)
        assert list(engine.sync_engine.pool.dispatch.checkout).count(_prepare_async_binary) == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_separate_async_engines_share_the_actual_pool_ping_owner():
    first = create_async_engine('mysql+aiomysql://user:password@localhost/db', pool_pre_ping=True)
    second = create_async_engine('mysql+aiomysql://user:password@localhost/db', pool=first.pool)
    try:
        assert first.dialect is not second.dialect
        assert first.pool._dialect is second.dialect
        second.dialect._send_false_to_ping = False
        adapt_engine_dialect(first.sync_engine)
        assert first.dialect._send_false_to_ping is True
        assert first.pool._dialect._send_false_to_ping is True
        assert list(first.pool.dispatch.checkout).count(_prepare_async_binary) == 1
    finally:
        await first.dispose()
        await second.dispose()


@pytest.mark.asyncio
async def test_async_binary_adapter_preserves_custom_nonbinary_values_and_errors():
    raw = aiomysql.Connection()
    original_error = RuntimeError('custom encoder failed')
    seen = []

    def custom(value):
        seen.append(value)
        if value == 'error':
            raise original_error
        return ('custom', value)

    raw.escape = custom
    try:
        _prepare_async_binary(raw, None, None)
        assert raw.escape('value') == ('custom', 'value')
        with pytest.raises(RuntimeError) as caught:
            raw.escape('error')
        assert caught.value is original_error
        assert raw.escape(b'\x00\xff') == "X'00ff'"
        assert seen == ['value', 'error']
    finally:
        raw.close()


def test_async_binary_adapter_does_not_inspect_foreign_driver_encoders():
    class Foreign:
        @property
        def escape(self):
            raise AssertionError('foreign encoder must remain untouched')

    _prepare_async_binary(Foreign(), None, None)


@pytest.mark.asyncio
async def test_async_binary_adapter_installation_failure_is_not_hidden():
    class ReadOnly(aiomysql.Connection):
        @property
        def escape(self):
            return super().escape

    raw = ReadOnly()
    try:
        with pytest.raises(AttributeError):
            _prepare_async_binary(raw, None, None)
        assert raw.escape.__func__ is aiomysql.Connection.escape
    finally:
        raw.close()
