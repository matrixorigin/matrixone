# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Version checks preserve the coroutine contract of decorated methods."""

import asyncio
import inspect
from types import SimpleNamespace

import pytest

from matrixone.clone import AsyncCloneManager
from matrixone.pitr import AsyncPitrManager
from matrixone.snapshot import AsyncSnapshotManager
from matrixone.version import VersionError, VersionManager, requires_version


def owner(version):
    manager = VersionManager()
    manager.set_backend_version(version)
    return SimpleNamespace(_version_manager=manager)


@pytest.mark.parametrize(
    'method', [AsyncCloneManager.clone_database, AsyncPitrManager.create_cluster_pitr, AsyncSnapshotManager.create]
)
def test_public_versioned_methods_remain_coroutine_functions(method):
    assert inspect.iscoroutinefunction(method)


@pytest.mark.asyncio
@pytest.mark.parametrize('nested_owner', [False, True])
async def test_async_version_failures_are_collected_by_gather(nested_owner):
    calls = []

    @requires_version(min_version='2.0.0')
    async def operation(self, value):
        calls.append(value)
        return value

    old, supported = owner('1.0.0'), owner('2.0.0')
    if nested_owner:
        old, supported = SimpleNamespace(client=old), SimpleNamespace(client=supported)
    results = await asyncio.gather(operation(old, 'old'), operation(supported, 'new'), return_exceptions=True)
    assert isinstance(results[0], VersionError)
    assert results[1] == 'new'
    assert calls == ['new']


@pytest.mark.asyncio
async def test_async_warning_mode_returns_awaitable_none(capsys):
    calls = []

    @requires_version(min_version='2.0.0', raise_error=False)
    async def operation(self):
        calls.append(True)

    assert await operation(owner('1.0.0')) is None
    assert calls == []
    assert 'not available' in capsys.readouterr().out


@pytest.mark.asyncio
async def test_async_metadata_and_body_exception_are_preserved():
    @requires_version(min_version='2.0.0')
    async def operation(self, *, value):
        """An operation with a keyword argument."""
        raise LookupError(value)

    assert operation.__name__ == 'operation'
    assert operation.__doc__ == operation.__wrapped__.__doc__
    assert operation._version_requirement['min_version'] == '2.0.0'
    with pytest.raises(LookupError, match='body failure'):
        await operation(owner('2.0.0'), value='body failure')


@pytest.mark.parametrize('raise_error', [False, True])
def test_sync_version_checks_remain_synchronous(raise_error, capsys):
    @requires_version(min_version='2.0.0', raise_error=raise_error)
    def operation(self):
        return 'ok'

    assert not inspect.iscoroutinefunction(operation)
    assert operation(owner('2.0.0')) == 'ok'
    if raise_error:
        with pytest.raises(VersionError):
            operation(owner('1.0.0'))
    else:
        assert operation(owner('1.0.0')) is None
        assert 'not available' in capsys.readouterr().out
