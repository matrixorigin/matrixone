# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Snapshot restore uses server grammar and cannot enlist in a session."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from matrixone.async_client import AsyncRestoreManager as ClientAsyncRestoreManager
from matrixone.exceptions import RestoreError
from matrixone.restore import AsyncRestoreManager, RestoreManager

CASES = [
    ('restore_cluster', ('cluster_sp',), "RESTORE CLUSTER{snapshot='cluster_sp'}"),
    ('restore_tenant', ('sp', 'acc'), "RESTORE ACCOUNT acc{snapshot='sp'}"),
    ('restore_tenant', ('sp', 'acc', 'dest'), "RESTORE ACCOUNT acc{snapshot='sp'} TO ACCOUNT dest"),
    ('restore_database', ('sp', 'acc', 'db'), "RESTORE DATABASE acc.db{snapshot='sp'}"),
    ('restore_database', ('sp', 'acc', 'db', 'dest'), "RESTORE DATABASE acc.db{snapshot='sp'} TO ACCOUNT dest"),
    ('restore_table', ('sp', 'acc', 'db', 't'), "RESTORE TABLE acc.db.t{snapshot='sp'}"),
    ('restore_table', ('sp', 'acc', 'db', 't', 'dest'), "RESTORE TABLE acc.db.t{snapshot='sp'} TO ACCOUNT dest"),
]


@pytest.mark.parametrize('method,args,sql', CASES)
def test_sync_restore_and_session_rejection(method, args, sql):
    client = Mock()
    client.execute.return_value = object()
    assert getattr(RestoreManager(client), method)(*args)
    client.execute.assert_called_once_with(sql)
    executor = Mock()
    with pytest.raises(RestoreError, match='session'):
        getattr(RestoreManager(client, executor), method)(*args)
    executor.execute.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('method,args,sql', CASES)
async def test_async_restore_and_session_rejection(method, args, sql):
    assert ClientAsyncRestoreManager is AsyncRestoreManager
    client = Mock()
    client.execute = AsyncMock(return_value=object())
    assert await getattr(AsyncRestoreManager(client), method)(*args)
    client.execute.assert_awaited_once_with(sql)
    executor = Mock()
    executor.execute = AsyncMock()
    with pytest.raises(RestoreError, match='session'):
        await getattr(AsyncRestoreManager(client, executor), method)(*args)
    executor.execute.assert_not_awaited()


def test_restore_quotes_atomic_names_and_preserves_failure_cause():
    client = Mock()
    client.execute.return_value = None
    assert not RestoreManager(client).restore_table("s'p", 'account', 'db.name', 'tick`col', 'order')
    client.execute.assert_called_once_with(
        "RESTORE TABLE account.`db.name`.`tick``col`{snapshot='s''p'} TO ACCOUNT `order`"
    )
    failure = RuntimeError('server rejected')
    client.execute.side_effect = failure
    with pytest.raises(RestoreError) as exc:
        RestoreManager(client).restore_cluster('sp')
    assert exc.value.__cause__ is failure
    client.execute.reset_mock()
    with pytest.raises(RestoreError):
        RestoreManager(client).restore_cluster('')
    client.execute.assert_not_called()


@pytest.mark.asyncio
async def test_async_restore_preserves_cancellation():
    client = Mock()
    client.execute = AsyncMock(side_effect=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await AsyncRestoreManager(client).restore_cluster('sp')
