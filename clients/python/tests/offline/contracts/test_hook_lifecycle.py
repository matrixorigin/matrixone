"""Physical connection ownership across supported SQLAlchemy pool versions."""

import asyncio
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import QueuePool

from matrixone.connection_hooks import ConnectionHook


class Physical:
    def __init__(self):
        self.closed = 0

    def close(self):
        self.closed += 1

    def rollback(self):
        pass


@pytest.mark.parametrize('adopt', [False, True])
def test_cancelled_hook_closes_physical_connection(adopt):
    created = []

    def connect():
        connection = Physical()
        created.append(connection)
        return connection

    engine = create_engine('sqlite://', poolclass=QueuePool)
    engine.pool = QueuePool(connect)
    if adopt:
        engine.pool.connect().close()
    client = Mock()
    cancellation = asyncio.CancelledError('stop admission')
    hook = ConnectionHook(custom_hook=Mock(side_effect=cancellation))
    hook.set_client(client)
    hook.attach_to_engine(engine)
    with pytest.raises(asyncio.CancelledError) as caught:
        engine.pool.connect()
    assert caught.value is cancellation
    assert len(created) == 1
    assert created[0].closed == 1
    assert engine.pool.checkedout() == 0
    engine.dispose()
    assert created[0].closed == 1


def test_hook_failure_runs_once_per_physical_connection():
    engine = create_engine('sqlite://', poolclass=QueuePool)
    callback = Mock(side_effect=ValueError('ordinary callback failure'))
    hook = ConnectionHook(custom_hook=callback)
    hook.set_client(Mock())
    hook.attach_to_engine(engine)
    try:
        for _ in range(3):
            with engine.connect():
                pass
        assert callback.call_count == 1
        with engine.connect() as connection:
            connection.invalidate()
        with engine.connect():
            pass
        assert callback.call_count == 2
    finally:
        engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'returns_awaitable', 'async'])
@pytest.mark.parametrize('message', ['custom hook failed', "object NoneType can't be used in 'await' expression"])
async def test_immediate_async_hook_reports_type_error(kind, message):
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    client = Mock()
    client._engine = engine

    async def raise_error(client):
        raise TypeError(message)

    def fail_sync(client):
        raise TypeError(message)

    def return_awaitable(client):
        return raise_error(client)

    callbacks = {'sync': fail_sync, 'returns_awaitable': return_awaitable, 'async': raise_error}
    hook = ConnectionHook(custom_hook=callbacks[kind])
    try:
        await hook.execute_async(client)
        client.logger.warning.assert_called_once_with(f'Connection hook execution failed: {message}')
        async with engine.connect() as connection:
            assert (await connection.exec_driver_sql('SELECT 1')).scalar() == 1
    finally:
        await engine.dispose()
