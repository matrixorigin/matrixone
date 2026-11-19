# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Deterministic ownership tests: no database or wall-clock performance oracle."""

import asyncio
import threading

import pytest

from matrixone._analytics_workers import AnalyticsWorkers


async def settled():
    future = asyncio.get_running_loop().create_future()
    asyncio.get_running_loop().call_soon(future.set_result, None)
    await future


@pytest.mark.asyncio
async def test_canceled_producers_keep_file_and_admission_until_real_exit(tmp_path):
    workers = AnalyticsWorkers()
    loop = asyncio.get_running_loop()
    operations, tasks, releases = [], [], []
    for index in range(2):
        operation = await workers.upload()
        entered = asyncio.Event()
        release = threading.Event()
        path = tmp_path / str(index)

        def producer(path=path, entered=entered, release=release, on_path=None, cancelled=None):
            on_path(str(path))
            path.write_bytes(b'owned')
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(10), 'test barrier was not released'
            return path

        task = asyncio.create_task(operation.prepare(producer))
        await asyncio.wait_for(entered.wait(), 5)
        operations.append(operation)
        tasks.append(task)
        releases.append(release)
    third = asyncio.create_task(workers.upload())
    try:
        for task in tasks:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        await settled()
        assert not third.done()
        assert len(workers.pending()) == 2
        assert all(status['worker_running'] for status in workers.pending())
        assert sorted(path.name for path in tmp_path.iterdir()) == ['0', '1']
        releases[0].set()
        await asyncio.wait_for(operations[0].wait_cleanup(), 5)
        replacement = await asyncio.wait_for(third, 5)
        replacement.finish()
        await asyncio.wait_for(replacement.wait_cleanup(), 5)
        assert not (tmp_path / '0').exists()
        assert (tmp_path / '1').exists()
    finally:
        for release in releases:
            release.set()
        for operation in operations:
            operation.finish()
            await asyncio.wait_for(operation.wait_cleanup(), 5)
        if not third.done():
            third.cancel()
    assert not list(tmp_path.iterdir())
    assert workers.pending() == ()


@pytest.mark.asyncio
async def test_conversion_ticket_stays_charged_but_unlink_has_its_own_lane(tmp_path):
    workers = AnalyticsWorkers()
    upload = await workers.upload()
    path = tmp_path / 'complete.arrow'
    path.write_bytes(b'owned')
    upload.reserve_path(str(path))
    releases, futures = [], []
    loop = asyncio.get_running_loop()
    for _ in range(2):
        lease = await workers.workers.acquire()
        entered = asyncio.Event()
        release = threading.Event()

        def convert(entered=entered, release=release):
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(10), 'test barrier was not released'

        future = lease.submit(convert)
        lease.release()
        await asyncio.wait_for(entered.wait(), 5)
        releases.append(release)
        futures.append(future)
    waiting = asyncio.create_task(workers.workers.acquire())
    try:
        await settled()
        assert not waiting.done()
        upload.finish()
        await asyncio.wait_for(upload.wait_cleanup(), 5)
        assert not path.exists()
        assert not waiting.done()
        releases[0].set()
        lease = await asyncio.wait_for(waiting, 5)
        lease.release()
    finally:
        for release in releases:
            release.set()
        for future in futures:
            await asyncio.wait_for(asyncio.wrap_future(future), 5)
        if not waiting.done():
            waiting.cancel()


@pytest.mark.asyncio
async def test_failed_unlink_is_bounded_retryable_and_preserves_exact_identity(tmp_path, monkeypatch):
    import matrixone._analytics_workers as module

    workers = AnalyticsWorkers()
    operations = []
    original_unlink = module.os.unlink

    def fail(path):
        raise PermissionError('read-only staging mount')

    monkeypatch.setattr(module.os, 'unlink', fail)
    for index in range(2):
        operation = await workers.upload()
        path = tmp_path / str(index)
        path.write_bytes(b'owned')
        operation.reserve_path(str(path))
        operation.finish()
        with pytest.raises(PermissionError):
            await asyncio.wait_for(operation.wait_cleanup(), 5)
        operations.append(operation)
    waiting = asyncio.create_task(workers.upload())
    await settled()
    assert not waiting.done()
    assert len(workers.pending()) == 2
    assert all(status['cleanup_failed'] for status in workers.pending())
    monkeypatch.setattr(module.os, 'unlink', original_unlink)
    for operation in operations:
        await workers.retry_cleanup(operation.id)
    replacement = await asyncio.wait_for(waiting, 5)
    replacement.finish()
    await asyncio.wait_for(replacement.wait_cleanup(), 5)
    assert workers.pending() == ()
    assert not list(tmp_path.iterdir())


def test_loop_closure_does_not_drop_worker_finalizer_or_reset_admission(tmp_path):
    workers = AnalyticsWorkers()
    release = threading.Event()
    saved = []

    async def old_loop():
        operation = await workers.upload()
        saved.append(operation)
        entered = asyncio.Event()
        loop = asyncio.get_running_loop()
        path = tmp_path / 'old-loop.arrow'

        def producer(on_path, cancelled):
            on_path(str(path))
            path.write_bytes(b'owned')
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(10), 'test barrier was not released'

        task = asyncio.create_task(operation.prepare(producer))
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        workers.set_open(False)

    try:
        asyncio.run(old_loop())
        assert len(workers.pending()) == 1
        release.set()
        saved[0]._cleanup.result(timeout=5)
        assert workers.pending() == ()
        assert not list(tmp_path.iterdir())

        async def new_loop():
            workers.set_open(True)
            first, second = await workers.upload(), await workers.upload()
            for operation in (first, second):
                operation.finish()
                await asyncio.wait_for(operation.wait_cleanup(), 5)

        asyncio.run(new_loop())
    finally:
        release.set()


@pytest.mark.asyncio
async def test_missing_arrow_dependency_fails_before_database_checkout(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import Mock
    from matrixone import AsyncClient
    import matrixone.async_analytics as module

    client = AsyncClient()
    checkout = Mock(side_effect=AssertionError('database checkout must not occur'))
    client._engine = SimpleNamespace(dialect=SimpleNamespace(driver='aiomysql'), connect=checkout)

    def missing():
        raise ImportError('Install analytics extra')

    monkeypatch.setattr(module, '_arrow', missing)
    with pytest.raises(ImportError, match='analytics extra'):
        async with client.analytics.query('SELECT 1'):
            pass
    checkout.assert_not_called()
    first = await asyncio.wait_for(client.analytics._workers.workers.acquire(), 5)
    second = await asyncio.wait_for(client.analytics._workers.workers.acquire(), 5)
    first.release()
    second.release()
    assert not client.analytics._streams


@pytest.mark.asyncio
async def test_cleanup_retry_cannot_replace_an_inflight_attempts_notification(tmp_path, monkeypatch):
    import matrixone._analytics_workers as module

    workers = AnalyticsWorkers()
    operation = await workers.upload()
    path = tmp_path / 'retry.arrow'
    path.write_bytes(b'owned')
    operation.reserve_path(str(path))
    original = operation._cleanup
    unlink_entered = asyncio.Event()
    notification_entered = asyncio.Event()
    allow_failure, allow_notification = threading.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    unlink = module.os.unlink
    set_exception = module.Future.set_exception
    attempts = []

    def controlled_unlink(name):
        attempts.append(name)
        if len(attempts) == 1:
            loop.call_soon_threadsafe(unlink_entered.set)
            assert allow_failure.wait(5), 'unlink barrier was not released'
            raise PermissionError('first cleanup failed')
        unlink(name)

    def controlled_notification(future, error):
        if future is original:
            loop.call_soon_threadsafe(notification_entered.set)
            assert allow_notification.wait(5), 'notification barrier was not released'
        return set_exception(future, error)

    monkeypatch.setattr(module.os, 'unlink', controlled_unlink)
    monkeypatch.setattr(module.Future, 'set_exception', controlled_notification)
    retry = None
    try:
        operation.finish()
        await asyncio.wait_for(unlink_entered.wait(), 5)
        original_waiter = asyncio.wrap_future(original)
        allow_failure.set()
        await asyncio.wait_for(notification_entered.wait(), 5)
        assert operation.status()['cleanup_failed']
        retry = asyncio.create_task(workers.retry_cleanup(operation.id))
        await settled()
        assert operation._cleanup is not original
        assert not original_waiter.done()
        allow_notification.set()
        with pytest.raises(PermissionError, match='first cleanup failed'):
            await asyncio.wait_for(original_waiter, 5)
        await asyncio.wait_for(retry, 5)
        assert attempts == [str(path), str(path)]
        assert not path.exists()
        following = await asyncio.wait_for(workers.upload(), 5)
        following.finish()
        await asyncio.wait_for(following.wait_cleanup(), 5)
        assert workers.pending() == ()
    finally:
        allow_failure.set()
        allow_notification.set()
        if retry is not None and not retry.done():
            retry.cancel()


@pytest.mark.asyncio
async def test_disconnect_cancels_stream_checkout_before_reconnect(monkeypatch):
    from types import SimpleNamespace
    from matrixone import AsyncClient
    import matrixone.async_analytics as module

    client = AsyncClient()
    entered = asyncio.Event()
    released = asyncio.Event()

    async def connect():
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            released.set()

    client._engine = SimpleNamespace(dialect=SimpleNamespace(driver='aiomysql'), connect=connect)
    monkeypatch.setattr(module, '_arrow', lambda: None)
    manager = client.analytics
    stream = manager.query('SELECT 1')
    entering = asyncio.create_task(stream.__aenter__())
    await asyncio.wait_for(entered.wait(), 5)
    await asyncio.wait_for(manager.disconnect(), 5)
    with pytest.raises(asyncio.CancelledError):
        await entering
    assert released.is_set()
    assert not manager._tasks and not manager._streams and not manager._finalizers
    manager.reconnect()
    first = await asyncio.wait_for(manager._workers.workers.acquire(), 5)
    second = await asyncio.wait_for(manager._workers.workers.acquire(), 5)
    first.release()
    second.release()


@pytest.mark.asyncio
async def test_closed_stream_releases_late_checkout_without_running_a_query(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from matrixone import AsyncClient
    import matrixone.async_analytics as module

    client = AsyncClient()
    entered, release = asyncio.Event(), asyncio.Event()
    connection = SimpleNamespace(invalidate=AsyncMock(), close=AsyncMock(), get_raw_connection=AsyncMock())

    async def connect():
        entered.set()
        await release.wait()
        return connection

    client._engine = SimpleNamespace(dialect=SimpleNamespace(driver='aiomysql'), connect=connect)
    monkeypatch.setattr(module, '_arrow', lambda: None)
    stream = client.analytics.query('SELECT 1')
    entering = asyncio.create_task(stream.__aenter__())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await stream.close()
        release.set()
        with pytest.raises(RuntimeError, match='closed during connection checkout'):
            await asyncio.wait_for(entering, 5)
        connection.invalidate.assert_awaited_once()
        connection.close.assert_awaited_once()
        connection.get_raw_connection.assert_not_called()
        assert not client.analytics._tasks and not client.analytics._streams
    finally:
        release.set()
        if not entering.done():
            entering.cancel()


@pytest.mark.asyncio
async def test_file_source_deadline_closes_checkout_and_releases_tracking():
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from matrixone import AsyncClient
    from matrixone.async_analytics import AsyncFileRelation

    closed, entered = asyncio.Event(), asyncio.Event()

    async def execute(*args):
        entered.set()
        await asyncio.Event().wait()

    @asynccontextmanager
    async def connect():
        try:
            yield SimpleNamespace(exec_driver_sql=execute)
        finally:
            closed.set()

    client = AsyncClient(query_timeout=0.02)
    client._engine = SimpleNamespace(connect=connect)
    relation = AsyncFileRelation(client.analytics, SimpleNamespace(schema=None, checks=(), server_sources=('owned',)))
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(relation.sql(), 5)
    assert entered.is_set() and closed.is_set()
    assert not client.analytics._tasks


@pytest.mark.asyncio
async def test_network_timeout_excludes_local_source_validation(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from matrixone import AsyncClient
    from matrixone.async_analytics import AsyncFileRelation

    client = AsyncClient(query_timeout=5)
    client._engine = SimpleNamespace(dialect=SimpleNamespace(driver='aiomysql'))
    relation = AsyncFileRelation(client.analytics, SimpleNamespace(schema=None, checks=(), server_sources=('owned',)))
    stream = relation.query(network_timeout=0.01)
    local_started, local_release = asyncio.Event(), asyncio.Event()

    async def local(lease):
        local_started.set()
        await local_release.wait()

    cursor = SimpleNamespace(execute=AsyncMock(), close=AsyncMock())
    stream._driver = SimpleNamespace(cursor=AsyncMock(return_value=cursor))
    monkeypatch.setattr(relation, '_sources', local)
    checking = asyncio.create_task(stream._check_sources())
    await asyncio.wait_for(local_started.wait(), 5)
    # This wait is deliberately longer than network_timeout. No network call
    # exists yet, so only the operation budget applies to local validation.
    await asyncio.sleep(0.03)
    assert not checking.done()
    local_release.set()
    await asyncio.wait_for(checking, 5)
    cursor.execute.assert_awaited_once()

    async def stalled(*args):
        await asyncio.Event().wait()

    cursor.execute.side_effect = stalled
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(stream._check_sources(), 5)
    assert cursor.close.await_count == 2


@pytest.mark.asyncio
async def test_cancel_during_inflight_grant_never_blocks_loop_or_leaks_permit(monkeypatch):
    from matrixone._analytics_workers import _Admission, Future

    admission = _Admission(1)
    held = await admission.acquire()
    waiting = asyncio.create_task(admission.acquire())
    await settled()
    grant = admission._waiters[0]
    publishing = asyncio.Event()
    release_publication, canceled = threading.Event(), threading.Event()
    watchdog_fired = []
    loop = asyncio.get_running_loop()
    original_set_result = Future.set_result

    def publish(future, value):
        if future is grant:
            loop.call_soon_threadsafe(publishing.set)
            assert release_publication.wait(5), 'publication barrier was not released'
        return original_set_result(future, value)

    def watchdog():
        if not canceled.wait(1):
            watchdog_fired.append(True)
        release_publication.set()

    monkeypatch.setattr(Future, 'set_result', publish)
    releaser = threading.Thread(target=held.release)
    rescue = threading.Thread(target=watchdog)
    releaser.start()
    try:
        await asyncio.wait_for(publishing.wait(), 5)
        rescue.start()
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        canceled.set()
        release_publication.set()
        await asyncio.wait_for(loop.run_in_executor(None, releaser.join, 5), 5)
        assert not watchdog_fired, 'cancellation blocked the event loop awaiting grant publication'
        replacement = await asyncio.wait_for(admission.acquire(), 5)
        replacement.release()
        assert admission._available == 1
    finally:
        canceled.set()
        release_publication.set()
        releaser.join(5)
        if rescue.ident is not None:
            rescue.join(5)


@pytest.mark.asyncio
async def test_old_file_relation_cannot_checkout_disposed_engine_after_reconnect():
    from types import SimpleNamespace
    from unittest.mock import Mock
    from matrixone import AsyncClient
    from matrixone.async_analytics import AsyncFileRelation

    client = AsyncClient()
    old_connect = Mock(side_effect=AssertionError('stale checkout must not occur'))
    client._engine = SimpleNamespace(connect=old_connect)
    manager = client.analytics
    relation = AsyncFileRelation(manager, SimpleNamespace(schema=None, checks=(), server_sources=()))
    await manager.disconnect()
    client._engine = SimpleNamespace()
    manager.reconnect()
    with pytest.raises(RuntimeError, match='Client connection changed'):
        await relation.sql()
    old_connect.assert_not_called()
    assert not manager._tasks


@pytest.mark.asyncio
async def test_cleanup_submission_failure_remains_retryable_without_stalling_lane(tmp_path, monkeypatch):
    workers = AnalyticsWorkers()
    operation = await workers.upload()
    path = tmp_path / 'submit.arrow'
    path.write_bytes(b'owned')
    operation.reserve_path(str(path))
    original = workers._unlink_executor.submit

    def fail(*args):
        raise RuntimeError('cannot start cleanup worker')

    with monkeypatch.context() as patch:
        patch.setattr(workers._unlink_executor, 'submit', fail)
        operation.finish()
        with pytest.raises(RuntimeError, match='cannot start cleanup worker'):
            await operation.wait_cleanup()
        assert workers.pending()[0]['cleanup_failed']
        assert workers._unlink_active is None and path.exists()
    following = await workers.upload()
    following.finish()
    await asyncio.wait_for(following.wait_cleanup(), 5)
    await asyncio.wait_for(workers.retry_cleanup(operation.id), 5)
    assert not path.exists() and workers.pending() == ()


@pytest.mark.asyncio
async def test_queued_cpu_job_cannot_run_after_submit_raises_or_escape_old_admission(monkeypatch):
    workers = AnalyticsWorkers()
    entered, release = asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    unintended = threading.Event()

    def running():
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5), 'running job barrier was not released'

    first = await workers.workers.acquire()
    future = first.submit(running)
    first.release()
    await asyncio.wait_for(entered.wait(), 5)
    second = await workers.workers.acquire()
    generation = workers._executor

    def fail_thread_creation():
        raise OSError('cannot start a second worker')

    try:
        with monkeypatch.context() as patch:
            patch.setattr(generation._executor, '_adjust_thread_count', fail_thread_creation)
            with pytest.raises(OSError):
                second.submit(unintended.set)
        second.release()
        with pytest.raises(RuntimeError, match='closed'):
            await workers.workers.acquire()
        # Explicit reconnect changes the executor, never the shared permit count.
        workers.set_open(True)
        replacement = await asyncio.wait_for(workers.workers.acquire(), 5)
        waiting = asyncio.create_task(workers.workers.acquire())
        await settled()
        assert not waiting.done()
        release.set()
        await asyncio.wait_for(asyncio.wrap_future(future), 5)
        granted = await asyncio.wait_for(waiting, 5)
        granted.release()
        replacement.release()
        # The old worker drains the rejected wrapper before terminating.
        await loop.run_in_executor(None, generation._executor.shutdown, True)
        assert not unintended.is_set()
        assert workers.workers._available == 2
    finally:
        release.set()
        second.release()


@pytest.mark.asyncio
async def test_queued_unlink_after_submit_failure_does_not_delete_a_later_file(tmp_path, monkeypatch):
    workers = AnalyticsWorkers()
    operation = await workers.upload()
    path = tmp_path / 'exact.arrow'
    path.write_bytes(b'owned')
    operation.reserve_path(str(path))
    generation = workers._unlink_executor

    def fail_thread_creation():
        raise OSError('cannot start cleanup worker')

    with monkeypatch.context() as patch:
        patch.setattr(generation._executor, '_adjust_thread_count', fail_thread_creation)
        operation.finish()
    with pytest.raises(OSError):
        await operation.wait_cleanup()
    assert path.read_bytes() == b'owned' and generation.failed
    await asyncio.wait_for(workers.retry_cleanup(operation.id), 5)
    assert workers.pending() == () and not path.exists()
    # Simulate a surviving old worker eventually dequeuing the rejected job.
    # It must not unlink a subsequently recreated pathname after ownership ended.
    path.write_bytes(b'later-owner')
    rejected = generation._executor._work_queue.get_nowait()
    assert rejected is not None
    rejected.run()
    assert path.read_bytes() == b'later-owner'
