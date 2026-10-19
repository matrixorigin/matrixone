# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from matrixone.exceptions import QueryError
from matrixone.vector_manager import AsyncVectorManager, VectorManager

ROWS = [{'id': i, 'embedding': [i, i + 1]} for i in range(7)]


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
async def test_public_chunks_preserve_scalar_literals(asynchronous):
    executor = SimpleNamespace(execute=AsyncMock() if asynchronous else Mock())
    manager = (AsyncVectorManager if asynchronous else VectorManager)(Mock(), executor=executor)
    rows = [{'id': i, 'embedding': [1, 2], 'value': value} for i, value in enumerate([True, 1.0, False])]
    if asynchronous:
        await manager.batch_insert('t', rows, batch_size=2)
        calls = executor.execute.await_args_list
    else:
        manager.batch_insert('t', rows, batch_size=2)
        calls = executor.execute.call_args_list
    assert [call.args[0] for call in calls] == [
        "INSERT INTO t (id, embedding, value) VALUES (0, '[1,2]', True), (1, '[1,2]', 1.0)",
        "INSERT INTO t (id, embedding, value) VALUES (2, '[1,2]', False)",
    ]


def test_client_batch_owns_one_transaction():
    executor = Mock()
    entries = []

    @contextmanager
    def session():
        entries.append('begin')
        yield executor
        entries.append('commit')

    client = SimpleNamespace(session=session)
    manager = VectorManager(client)
    assert manager.batch_insert('t', ROWS, batch_size=3) is manager
    assert executor.execute.call_count == 3
    assert entries == ['begin', 'commit']
    assert all('INSERT INTO t' in call.args[0] for call in executor.execute.call_args_list)


def test_caller_session_is_not_committed():
    client, executor = Mock(), Mock()
    VectorManager(client, executor=executor).batch_insert('t', ROWS, batch_size=2)
    assert executor.execute.call_count == 4
    client.session.assert_not_called()
    executor.commit.assert_not_called()


def test_failure_exits_owned_transaction():
    executor = Mock()
    executor.execute.side_effect = [None, RuntimeError('late failure')]
    exited = []

    @contextmanager
    def session():
        try:
            yield executor
        except Exception:
            exited.append('rollback')
            raise

    with pytest.raises(QueryError, match='late failure'):
        VectorManager(SimpleNamespace(session=session)).batch_insert('t', ROWS, batch_size=3)
    assert exited == ['rollback']


@pytest.mark.parametrize('size', [0, -1, True, 1.5])
def test_invalid_batch_size_does_not_start_write(size):
    client = Mock()
    with pytest.raises(ValueError):
        VectorManager(client).batch_insert('t', ROWS, batch_size=size)
    client.session.assert_not_called()


def test_bad_schema_rejected_before_write():
    client = Mock()
    with pytest.raises(ValueError):
        VectorManager(client).batch_insert('t', [{'a': 1}, {'b': 2}])
    client.session.assert_not_called()


def test_empty_batch_does_not_acquire_connection():
    client = Mock()
    manager = VectorManager(client)
    assert manager.batch_insert('t', []) is manager
    client.session.assert_not_called()


@pytest.mark.asyncio
async def test_async_transaction_and_chunk_parity():
    executor = SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def session():
        yield executor

    manager = AsyncVectorManager(SimpleNamespace(session=session))
    assert await manager.batch_insert('t', ROWS, batch_size=3) is manager
    assert executor.execute.await_count == 3


@pytest.mark.asyncio
async def test_async_caller_session_ownership():
    client = Mock()
    executor = SimpleNamespace(execute=AsyncMock(), commit=AsyncMock())
    await AsyncVectorManager(client, executor=executor).batch_insert('t', ROWS, batch_size=3)
    assert executor.execute.await_count == 3
    executor.commit.assert_not_awaited()
    client.session.assert_not_called()
