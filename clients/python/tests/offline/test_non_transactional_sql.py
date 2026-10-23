# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""#30966: DATA BRANCH MERGE/PICK must execute in autocommit mode.

The server rejects these statements with "DATA BRANCH MERGE/PICK is not
supported in transactions" (pkg/frontend/data_branch.go). Pooled driver
connections run with autocommit=False, and SQLAlchemy's do_begin is a no-op,
so a plain engine.begin() puts them in an implicit, never-committed
transaction. Client.execute / AsyncClient._execute must run them on a
checked-out connection flipped to autocommit and restored afterwards, while
ordinary statements keep the begin() transaction path, without corrupting the
baseline of caller-supplied engines (Client.from_engine).
"""

import time
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
import sqlalchemy

from matrixone import Client
from matrixone.async_client import AsyncClient
from matrixone._utils import is_non_transactional_statement


@pytest.mark.parametrize(
    "sql",
    [
        # the exact shape _build_merge_table_sql produces
        "data branch merge src into dst when conflict skip",
        "data branch merge src into dst when conflict accept",
        "DATA BRANCH MERGE db.src INTO db.dst ON CONFLICT SKIP",
        "  data branch merge src into dst",
        # valid server PICK forms (see test/distributed/cases/git4data/branch/pick)
        "data branch pick t2 into t1 keys(4)",
        "data branch pick t2 into t1 keys(2) when conflict accept",
        "data branch pick t2 into t1 keys(select a from t2 where a > 3)",
        "data branch pick t1 into t0 between snapshot sp1 and sp2 keys(4)",
        "\n\tDATA BRANCH PICK t2 INTO t1 KEYS(4)",
        # leading comments
        "/* rebase */ data branch merge src into dst",
        "-- rebase note\ndata branch merge src into dst",
        "# rebase note\ndata branch pick t2 into t1 keys(4)",
        # MySQL comments are also accepted between the keywords by the server
        "data /* trace */ branch merge src into dst",
        "data branch /* trace */ merge src into dst",
        "data -- trace\nbranch pick t2 into t1 keys(4)",
        # MatrixOne's scanner skips // line comments unconditionally
        "// trace\ndata branch merge src into dst",
        "//x\ndata branch pick t2 into t1 keys(1)",
        # executable MySQL comments: the server runs their contents
        "/*! data branch merge src into dst */",
        "/*!30101 data branch merge src into dst */",
        "/*! data branch pick t2 into t1 keys(2) */",
        # repeated leading comments stay classified (and fast)
        ("/*c*/" * 32) + " data branch merge src into dst",
    ],
)
def test_merge_pick_statements_are_non_transactional(sql):
    assert is_non_transactional_statement(sql)


@pytest.mark.parametrize(
    "sql",
    [
        None,
        "",
        "   ",
        "select * from merge_t1",
        "select 'data branch merge a into b' as note",
        "insert into log_t values ('data branch merge a into b')",
        "update t set note = 'data branch pick' where id = 1",
        "explain data branch merge src into dst",
        "explain data branch pick t2 into t1 keys(4)",
        "data branch create dst from src",
        "-- only a merge-shaped comment, no statement",
        "-- data branch merge src into dst\nSELECT 1",
        "/* data branch merge src into dst */ SELECT 1",
        "/* data branch pick t2 into t1 keys(4) */",
        "# data branch merge src into dst",
        "// data branch merge src into dst\nSELECT 1",
        "// only a merge-shaped // comment",
        "--x data branch merge src into dst",  # bare -- is not a MySQL comment
        # executable-comment contents participate: 'data /*! trace */ ...'
        # executes as 'data trace branch merge', which the server rejects
        "data /*! trace */ branch merge src into dst",
        "set autocommit = 1; data branch merge src into dst",
        "data branch mergeability",  # not the MERGE verb
        "data branch pickerel",  # not the PICK verb
        "create table dst as select * from src",
        # repeated leading comments followed by an ordinary statement
        ("/*c*/" * 32) + " SELECT 1",
    ],
)
def test_ordinary_statements_stay_in_transactions(sql):
    assert not is_non_transactional_statement(sql)


def test_classification_is_linear_in_comment_count():
    # The first implementation used a regex whose comment group backtracked
    # exponentially; classification must stay effectively linear because every
    # user query passes through it.
    def timed(n):
        sql = ("/*c*/" * n) + " SELECT 1"
        start = time.perf_counter()
        assert not is_non_transactional_statement(sql)
        return time.perf_counter() - start

    small = timed(128)
    large = timed(1024)
    # 8x the comments must finish in well under 100x the time; the previous
    # regex needed ~3.4s for 24 repetitions and grew by ~16x per +4 comments.
    assert large < 1.0
    assert large < max(0.05, small * 100)


# ─── routing: mocked engines ─────────────────────────────────────────────────


def _result(commands, autocommit="0"):
    """exec_driver_sql side effect: answers the autocommit snapshot query."""

    def run(sql):
        commands.append(sql)
        r = MagicMock()
        if sql == "SELECT @@autocommit":
            r.scalar.return_value = autocommit
        else:
            r.returns_rows = False
            r.rowcount = 0
        return r

    return run


def _sync_engine():
    """Engine mock whose begin()/connect() contexts record which path ran."""
    engine = MagicMock()

    begin_conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = begin_conn

    connect_conn = MagicMock()
    connect_cm = Mock()
    connect_cm.__enter__ = Mock(return_value=connect_conn)
    connect_cm.__exit__ = Mock(return_value=False)
    engine.connect.return_value = connect_cm
    return engine, begin_conn, connect_conn, connect_cm


def test_sync_merge_routes_flips_and_restores_autocommit():
    engine, begin_conn, connect_conn, _ = _sync_engine()
    commands = []
    connect_conn.exec_driver_sql.side_effect = _result(commands)
    client = Client(logger=Mock())
    client._engine = engine

    client.execute("data branch merge src into dst when conflict skip")

    engine.connect.assert_called_once_with()
    engine.begin.assert_not_called()
    begin_conn.exec_driver_sql.assert_not_called()
    assert commands == [
        "SELECT @@autocommit",
        "SET autocommit = 1",
        "data branch merge src into dst when conflict skip",
        "SET autocommit = 0",
    ]
    connect_conn.invalidate.assert_not_called()


def test_sync_pick_with_keys_routes_to_autocommit():
    engine, _, connect_conn, _ = _sync_engine()
    commands = []
    connect_conn.exec_driver_sql.side_effect = _result(commands)
    client = Client(logger=Mock())
    client._engine = engine

    client.execute("data branch pick t2 into t1 keys(4)")

    engine.connect.assert_called_once_with()
    assert commands[-2] == "data branch pick t2 into t1 keys(4)"
    assert commands[-1] == "SET autocommit = 0"


def test_sync_merge_preserves_existing_autocommit_baseline():
    # from_engine callers may pool autocommit=True connections; no flip and no
    # restore may happen then, or their baseline would be corrupted to False.
    engine, _, connect_conn, _ = _sync_engine()
    commands = []
    connect_conn.exec_driver_sql.side_effect = _result(commands, autocommit="1")
    client = Client(logger=Mock())
    client._engine = engine

    client.execute("data branch merge src into dst")

    assert "SET autocommit = 1" not in commands
    assert "SET autocommit = 0" not in commands
    assert commands == ["SELECT @@autocommit", "data branch merge src into dst"]


def test_sync_ordinary_sql_keeps_begin_transaction_path():
    engine, begin_conn, connect_conn, _ = _sync_engine()
    client = Client(logger=Mock())
    client._engine = engine

    client.execute("select 1")

    engine.begin.assert_called_once_with()
    engine.connect.assert_not_called()
    connect_conn.exec_driver_sql.assert_not_called()
    begin_conn.exec_driver_sql.assert_called_once_with("select 1")


def test_sync_flip_failure_releases_checkout_without_executing():
    # If SET autocommit itself fails, the failure happens inside the connection
    # context, so __exit__ still runs and the pooled checkout is released; the
    # protected section also attempts the restore before giving up the
    # connection.
    engine, begin_conn, connect_conn, connect_cm = _sync_engine()
    commands = []

    def failing(sql):
        commands.append(sql)
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        raise Exception("server gone")

    connect_conn.exec_driver_sql.side_effect = failing
    client = Client(logger=Mock())
    client._engine = engine

    with pytest.raises(Exception):
        client.execute("data branch merge src into dst")

    connect_cm.__exit__.assert_called_once()  # context exited -> checkout released
    assert "data branch merge src into dst" not in commands
    assert commands == ["SELECT @@autocommit", "SET autocommit = 1", "SET autocommit = 0"]
    connect_conn.invalidate.assert_called_once_with()
    engine.begin.assert_not_called()


def test_sync_flip_failure_after_server_applied_it_is_restored():
    # The SET can succeed server-side yet raise locally (dead result read,
    # raising driver hook). Restoration responsibility was recorded BEFORE the
    # flip, so the baseline is still restored and the original error surfaces.
    engine, _, connect_conn, _ = _sync_engine()
    applied = []

    def post_apply_failure(sql):
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        if sql == "SET autocommit = 1":
            applied.append(sql)  # server applied the change...
            raise Exception("read died after apply")  # ...then the read died
        if sql == "SET autocommit = 0":
            applied.append(sql)
            return Mock(returns_rows=False, rowcount=0)
        raise AssertionError("merge must not run after a failed flip")

    connect_conn.exec_driver_sql.side_effect = post_apply_failure
    client = Client(logger=Mock())
    client._engine = engine

    with pytest.raises(Exception) as err:
        client.execute("data branch merge src into dst")

    assert "read died after apply" in str(err.value)
    assert applied == ["SET autocommit = 1", "SET autocommit = 0"]
    connect_conn.invalidate.assert_not_called()  # restore succeeded; conn healthy


def test_sync_restore_failure_invalidates_without_masking_success():
    # A failed restore drops the mutated connection instead of returning it to
    # the pool, but must not replace the statement's own outcome.
    engine, _, connect_conn, _ = _sync_engine()

    def failing_restore(sql):
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        if sql == "SET autocommit = 0":
            raise Exception("restore failed")
        return Mock(returns_rows=False, rowcount=0)

    connect_conn.exec_driver_sql.side_effect = failing_restore
    client = Client(logger=Mock())
    client._engine = engine

    result = client.execute("data branch merge src into dst")

    assert result is not None  # merge outcome preserved
    connect_conn.invalidate.assert_called_once_with()


def test_sync_restore_keyboard_interrupt_discards_connection():
    # KeyboardInterrupt delivered during the restore is a BaseException: it
    # escapes `except Exception`, leaving the server-side autocommit state
    # unknown. The connection must still be discarded instead of returned to
    # the pool, and the interrupt must propagate unreplaced.
    engine, _, connect_conn, _ = _sync_engine()
    applied = []

    def interrupt_on_restore(sql):
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        applied.append(sql)
        if sql == "SET autocommit = 0":
            raise KeyboardInterrupt()
        return Mock(returns_rows=False, rowcount=0)

    connect_conn.exec_driver_sql.side_effect = interrupt_on_restore
    client = Client(logger=Mock())
    client._engine = engine

    with pytest.raises(KeyboardInterrupt):
        client.execute("data branch merge src into dst")

    connect_conn.invalidate.assert_called_once_with()
    assert applied == ["SET autocommit = 1", "data branch merge src into dst", "SET autocommit = 0"]


def test_sync_invalidate_failure_after_failed_restore_keeps_merge_success():
    # invalidate() itself can raise (ResourceClosedError on an already-closed
    # connection, a pool finalization error). That failure must not escape
    # the finally and replace the statement's successful outcome.
    engine, _, connect_conn, _ = _sync_engine()

    def failing_restore(sql):
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        if sql == "SET autocommit = 0":
            raise Exception("restore failed")
        return Mock(returns_rows=False, rowcount=0)

    connect_conn.exec_driver_sql.side_effect = failing_restore
    connect_conn.invalidate.side_effect = RuntimeError("discard failed")
    client = Client(logger=Mock())
    client._engine = engine

    result = client.execute("data branch merge src into dst")

    assert result is not None  # merge outcome preserved despite both failures
    connect_conn.invalidate.assert_called_once_with()  # discard still attempted


def test_sync_invalidate_failure_keeps_original_statement_error():
    # Error-path twin: the original statement error must survive even when
    # both the restore and the invalidation fail.
    engine, _, connect_conn, _ = _sync_engine()

    def failing_everything(sql):
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        if sql == "SET autocommit = 1":
            return Mock(returns_rows=False, rowcount=0)
        if sql == "SET autocommit = 0":
            raise Exception("restore failed")
        raise Exception("merge blew up")

    connect_conn.exec_driver_sql.side_effect = failing_everything
    connect_conn.invalidate.side_effect = RuntimeError("discard failed")
    client = Client(logger=Mock())
    client._engine = engine

    with pytest.raises(Exception) as err:
        client.execute("data branch merge src into dst")

    assert "merge blew up" in str(err.value)
    assert "restore failed" not in str(err.value)
    assert "discard failed" not in str(err.value)
    connect_conn.invalidate.assert_called_once_with()


def test_sync_keyboard_interrupt_survives_invalidate_failure():
    # The BaseException branch guards invalidate() so a discard failure can
    # never swallow an in-flight interrupt; pin that guarantee so the two
    # branches cannot drift apart.
    engine, _, connect_conn, _ = _sync_engine()

    def interrupt_on_restore(sql):
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        if sql == "SET autocommit = 0":
            raise KeyboardInterrupt()
        return Mock(returns_rows=False, rowcount=0)

    connect_conn.exec_driver_sql.side_effect = interrupt_on_restore
    connect_conn.invalidate.side_effect = RuntimeError("discard failed")
    client = Client(logger=Mock())
    client._engine = engine

    with pytest.raises(KeyboardInterrupt):
        client.execute("data branch merge src into dst")

    connect_conn.invalidate.assert_called_once_with()
    # the BaseException branch runs the same pool-safety detach fallback
    connect_conn.detach.assert_called_once_with()


def test_sync_invalidate_failure_detaches_connection():
    # SQLAlchemy 1.4 fires PoolEvents.invalidate listeners BEFORE the
    # record's hard close, so a raising listener leaves the live, mutated
    # connection checked out. When the discard raises, the fallback must
    # still keep the connection out of the pool: detach() marks it for a
    # literal close instead of a pool return.
    engine, _, connect_conn, _ = _sync_engine()

    def failing_restore(sql):
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        if sql == "SET autocommit = 0":
            raise Exception("restore failed")
        return Mock(returns_rows=False, rowcount=0)

    connect_conn.exec_driver_sql.side_effect = failing_restore
    connect_conn.invalidate.side_effect = RuntimeError("listener raised before close")
    client = Client(logger=Mock())
    client._engine = engine

    result = client.execute("data branch merge src into dst")

    assert result is not None  # merge outcome preserved despite both failures
    connect_conn.invalidate.assert_called_once_with()
    connect_conn.detach.assert_called_once_with()


def _async_engine():
    engine = MagicMock()

    begin_conn = MagicMock()
    begin_cm = AsyncMock()
    begin_cm.__aenter__.return_value = begin_conn
    engine.begin.return_value = begin_cm

    connect_conn = MagicMock()
    connect_cm = AsyncMock()
    connect_cm.__aenter__.return_value = connect_conn
    engine.connect.return_value = connect_cm
    return engine, begin_conn, connect_conn, connect_cm


def _async_result(commands):
    async def run(sql):
        commands.append(sql)
        r = MagicMock()
        if sql == "SELECT @@autocommit":
            r.scalar.return_value = "0"
        else:
            r.returns_rows = False
            r.rowcount = 0
        return r

    return run


@pytest.mark.asyncio
async def test_async_merge_routes_flips_and_restores_autocommit():
    engine, begin_conn, connect_conn, _ = _async_engine()
    commands = []
    connect_conn.exec_driver_sql = AsyncMock(side_effect=_async_result(commands))
    client = AsyncClient(logger=Mock())
    client._engine = engine

    await client._execute("data branch merge src into dst when conflict accept")

    engine.connect.assert_called_once_with()
    engine.begin.assert_not_called()
    begin_conn.exec_driver_sql.assert_not_called()
    assert commands == [
        "SELECT @@autocommit",
        "SET autocommit = 1",
        "data branch merge src into dst when conflict accept",
        "SET autocommit = 0",
    ]


@pytest.mark.asyncio
async def test_async_ordinary_sql_keeps_begin_transaction_path():
    engine, begin_conn, connect_conn, _ = _async_engine()
    begin_conn.exec_driver_sql = AsyncMock(return_value=MagicMock(returns_rows=False, rowcount=0))
    client = AsyncClient(logger=Mock())
    client._engine = engine

    await client._execute("select 1")

    engine.begin.assert_called_once_with()
    begin_conn.exec_driver_sql.assert_awaited_once_with("select 1")
    engine.connect.assert_not_called()


@pytest.mark.asyncio
async def test_async_flip_failure_releases_checkout_without_executing():
    engine, _, connect_conn, connect_cm = _async_engine()
    commands = []

    async def failing(sql):
        commands.append(sql)
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        raise Exception("server gone")

    connect_conn.exec_driver_sql = AsyncMock(side_effect=failing)
    client = AsyncClient(logger=Mock())
    client._engine = engine

    with pytest.raises(Exception):
        await client._execute("data branch merge src into dst")

    connect_cm.__aexit__.assert_awaited_once()  # checkout released
    assert "data branch merge src into dst" not in commands
    assert commands == ["SELECT @@autocommit", "SET autocommit = 1", "SET autocommit = 0"]


@pytest.mark.asyncio
async def test_async_cancellation_is_not_masked_by_restore():
    # SQLAlchemy invalidates the async connection on cancellation, so issuing
    # restore SQL raises (e.g. PendingRollbackError); that error must never
    # replace the original CancelledError.
    import asyncio as _asyncio

    from sqlalchemy.exc import PendingRollbackError

    engine, _, connect_conn, _ = _async_engine()

    async def cancel_on_merge(sql):
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        if sql == "SET autocommit = 1":
            return Mock(returns_rows=False, rowcount=0)
        if sql == "SET autocommit = 0":
            raise PendingRollbackError("statements pending")
        raise _asyncio.CancelledError()

    connect_conn.exec_driver_sql = AsyncMock(side_effect=cancel_on_merge)
    connect_conn.invalidate = AsyncMock()
    client = AsyncClient(logger=Mock())
    client._engine = engine

    with pytest.raises(_asyncio.CancelledError):
        await client._execute("data branch merge src into dst")

    connect_conn.invalidate.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_wait_for_timeout_stays_timeout():
    # The reviewer's probe shape: a wrap in asyncio.wait_for must still surface
    # TimeoutError when the driver is blocked and restore SQL raises on the
    # invalidated connection.
    import asyncio as _asyncio

    from sqlalchemy.exc import PendingRollbackError

    engine, _, connect_conn, _ = _async_engine()

    async def hang_on_merge(sql):
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        if sql == "SET autocommit = 1":
            return Mock(returns_rows=False, rowcount=0)
        if sql == "SET autocommit = 0":
            raise PendingRollbackError("statements pending")
        await _asyncio.sleep(30)

    connect_conn.exec_driver_sql = AsyncMock(side_effect=hang_on_merge)
    connect_conn.invalidate = AsyncMock()
    client = AsyncClient(logger=Mock())
    client._engine = engine

    # asyncio.wait_for raises asyncio.TimeoutError, which is only an alias of
    # the builtin TimeoutError on Python 3.11+; the SDK supports >=3.8, so
    # spell it via the asyncio module (as test_connection_contracts.py does).
    with pytest.raises(_asyncio.TimeoutError):
        await _asyncio.wait_for(client._execute("data branch merge src into dst"), timeout=0.2)


@pytest.mark.asyncio
async def test_async_disconnect_during_merge_keeps_original_error():
    from sqlalchemy.exc import PendingRollbackError

    engine, _, connect_conn, _ = _async_engine()

    async def disconnect_on_merge(sql):
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        if sql == "SET autocommit = 1":
            return Mock(returns_rows=False, rowcount=0)
        if sql == "SET autocommit = 0":
            raise PendingRollbackError("statements pending")
        raise Exception("server connection gone away")

    connect_conn.exec_driver_sql = AsyncMock(side_effect=disconnect_on_merge)
    connect_conn.invalidate = AsyncMock()
    client = AsyncClient(logger=Mock())
    client._engine = engine

    with pytest.raises(Exception) as err:
        await client._execute("data branch merge src into dst")

    assert "server connection gone away" in str(err.value)
    assert "statements pending" not in str(err.value)


@pytest.mark.asyncio
async def test_async_cancellation_during_restore_discards_connection():
    # The restore can land server-side while its response is lost to a task
    # cancellation: the restore await then raises CancelledError, which is a
    # BaseException and escapes `except Exception`. Unlike the test above —
    # which cancels during the merge and sees the *restore* fail on the
    # already-invalidated connection — this cancels during the restore itself.
    # The connection in an unknown autocommit state must be discarded rather
    # than returned to the pool, and the cancellation must propagate
    # unreplaced.
    import asyncio as _asyncio

    engine, _, connect_conn, _ = _async_engine()
    applied = []

    async def cancel_on_restore(sql):
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        applied.append(sql)  # the server applies the statement...
        if sql == "SET autocommit = 0":
            raise _asyncio.CancelledError()  # ...then the response is lost to cancellation
        return Mock(returns_rows=False, rowcount=0)

    connect_conn.exec_driver_sql = AsyncMock(side_effect=cancel_on_restore)
    connect_conn.invalidate = AsyncMock()
    client = AsyncClient(logger=Mock())
    client._engine = engine

    with pytest.raises(_asyncio.CancelledError):
        await client._execute("data branch merge src into dst")

    connect_conn.invalidate.assert_awaited_once()
    assert applied == ["SET autocommit = 1", "data branch merge src into dst", "SET autocommit = 0"]


@pytest.mark.asyncio
async def test_async_invalidate_failure_after_failed_restore_keeps_merge_success():
    # Async twin of the sync guard: an invalidate() that raises must not
    # escape the finally and replace the statement's successful outcome.
    from sqlalchemy.exc import PendingRollbackError

    engine, _, connect_conn, _ = _async_engine()

    async def failing_restore(sql):
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        if sql == "SET autocommit = 0":
            raise PendingRollbackError("statements pending")
        return Mock(returns_rows=False, rowcount=0)

    connect_conn.exec_driver_sql = AsyncMock(side_effect=failing_restore)
    connect_conn.invalidate = AsyncMock(side_effect=RuntimeError("discard failed"))
    client = AsyncClient(logger=Mock())
    client._engine = engine

    result = await client._execute("data branch merge src into dst")

    assert result is not None  # merge outcome preserved despite both failures
    connect_conn.invalidate.assert_awaited_once()  # discard still attempted


@pytest.mark.asyncio
async def test_async_invalidate_failure_keeps_original_statement_error():
    # Error-path twin: the original statement error must survive even when
    # both the restore and the invalidation fail.
    from sqlalchemy.exc import PendingRollbackError

    engine, _, connect_conn, _ = _async_engine()

    async def disconnect_on_merge(sql):
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        if sql == "SET autocommit = 1":
            return Mock(returns_rows=False, rowcount=0)
        if sql == "SET autocommit = 0":
            raise PendingRollbackError("statements pending")
        raise Exception("server connection gone away")

    connect_conn.exec_driver_sql = AsyncMock(side_effect=disconnect_on_merge)
    connect_conn.invalidate = AsyncMock(side_effect=RuntimeError("discard failed"))
    client = AsyncClient(logger=Mock())
    client._engine = engine

    with pytest.raises(Exception) as err:
        await client._execute("data branch merge src into dst")

    assert "server connection gone away" in str(err.value)
    assert "statements pending" not in str(err.value)
    assert "discard failed" not in str(err.value)
    connect_conn.invalidate.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_cancellation_survives_invalidate_failure():
    # Cancellation during the merge, restore failing on the invalidated
    # connection, and invalidate() itself raising: the CancelledError must
    # still propagate unreplaced.
    import asyncio as _asyncio

    from sqlalchemy.exc import PendingRollbackError

    engine, _, connect_conn, _ = _async_engine()

    async def cancel_on_merge(sql):
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        if sql == "SET autocommit = 1":
            return Mock(returns_rows=False, rowcount=0)
        if sql == "SET autocommit = 0":
            raise PendingRollbackError("statements pending")
        raise _asyncio.CancelledError()

    connect_conn.exec_driver_sql = AsyncMock(side_effect=cancel_on_merge)
    connect_conn.invalidate = AsyncMock(side_effect=RuntimeError("discard failed"))
    client = AsyncClient(logger=Mock())
    client._engine = engine

    with pytest.raises(_asyncio.CancelledError):
        await client._execute("data branch merge src into dst")

    connect_conn.invalidate.assert_awaited_once()


@pytest.mark.asyncio
async def test_async_cancellation_during_restore_survives_invalidate_failure():
    # BaseException branch pin: cancellation delivered during the restore
    # await, then invalidate() itself failing — the cancellation still
    # propagates unreplaced.
    import asyncio as _asyncio

    engine, _, connect_conn, _ = _async_engine()

    async def cancel_on_restore(sql):
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        if sql == "SET autocommit = 0":
            raise _asyncio.CancelledError()
        return Mock(returns_rows=False, rowcount=0)

    connect_conn.exec_driver_sql = AsyncMock(side_effect=cancel_on_restore)
    connect_conn.invalidate = AsyncMock(side_effect=RuntimeError("discard failed"))
    client = AsyncClient(logger=Mock())
    client._engine = engine

    with pytest.raises(_asyncio.CancelledError):
        await client._execute("data branch merge src into dst")

    connect_conn.invalidate.assert_awaited_once()
    # the same pool-safety fallback runs on the BaseException branch
    connect_conn.sync_connection.detach.assert_called_once_with()


@pytest.mark.asyncio
async def test_async_invalidate_failure_detaches_connection():
    # Async twin of the sync pool-safety fallback. AsyncConnection has no
    # detach of its own, so the proxied sync Connection is detached; on
    # SQLAlchemy 1.4 (invalidate listeners fire before the record's hard
    # close) that is what keeps a mutated connection out of the pool when
    # the discard itself raises.
    engine, _, connect_conn, _ = _async_engine()

    async def failing_restore(sql):
        if sql == "SELECT @@autocommit":
            return Mock(scalar=Mock(return_value="0"))
        if sql == "SET autocommit = 0":
            raise Exception("restore failed")
        return Mock(returns_rows=False, rowcount=0)

    connect_conn.exec_driver_sql = AsyncMock(side_effect=failing_restore)
    connect_conn.invalidate = AsyncMock(side_effect=RuntimeError("listener raised before close"))
    client = AsyncClient(logger=Mock())
    client._engine = engine

    result = await client._execute("data branch merge src into dst")

    assert result is not None  # merge outcome preserved despite both failures
    connect_conn.invalidate.assert_awaited_once()
    connect_conn.sync_connection.detach.assert_called_once_with()


@pytest.mark.parametrize(
    "sql",
    [
        "// trace\ndata branch merge src into dst",
        "/*! data branch merge src into dst */",
        "/*!30101 data branch merge src into dst */",
    ],
)
def test_sync_matrixone_comment_form_merges_route_to_autocommit(sql):
    engine, begin_conn, connect_conn, _ = _sync_engine()
    commands = []
    connect_conn.exec_driver_sql.side_effect = _result(commands)
    client = Client(logger=Mock())
    client._engine = engine

    client.execute(sql)

    engine.connect.assert_called_once_with()
    engine.begin.assert_not_called()
    begin_conn.exec_driver_sql.assert_not_called()
    assert commands[0] == "SELECT @@autocommit"
    assert commands[1] == "SET autocommit = 1"
    assert commands[2] == sql  # the whole statement reaches the server
    assert commands[3] == "SET autocommit = 0"


@pytest.mark.parametrize(
    "sql",
    [
        "// trace\ndata branch merge src into dst",
        "/*! data branch pick t2 into t1 keys(2) */",
    ],
)
@pytest.mark.asyncio
async def test_async_matrixone_comment_forms_route_to_autocommit(sql):
    engine, begin_conn, connect_conn, _ = _async_engine()
    commands = []
    connect_conn.exec_driver_sql = AsyncMock(side_effect=_async_result(commands))
    client = AsyncClient(logger=Mock())
    client._engine = engine

    await client._execute(sql)

    engine.connect.assert_called_once_with()
    engine.begin.assert_not_called()
    begin_conn.exec_driver_sql.assert_not_called()
    assert commands[0] == "SELECT @@autocommit"
    assert commands[1] == "SET autocommit = 1"
    assert commands[2] == sql
    assert commands[3] == "SET autocommit = 0"


@pytest.mark.parametrize(
    "sql",
    [
        "/* trace */ -- note\nselect 1",
        "// trace\nselect 1",
        "/* data branch merge src into dst */ select 1",
    ],
)
def test_inert_comment_ordinary_sql_keeps_begin_path(sql):
    engine, begin_conn, connect_conn, _ = _sync_engine()
    begin_conn.exec_driver_sql.return_value = MagicMock(returns_rows=False, rowcount=0)
    client = Client(logger=Mock())
    client._engine = engine

    client.execute(sql)

    engine.begin.assert_called_once_with()
    engine.connect.assert_not_called()


# ─── dialect isolation contract ──────────────────────────────────────────────
# The custom MatrixOneDialect replaces the stock pymysql dialect on the sync
# engine after create_engine(), so engine.initialize() never ran on it: it
# must recover and restore the engine-default isolation level itself.


def _dbapi_conn(default_isolation="REPEATABLE READ", autocommit="0"):
    """Mock DBAPI connection answering the isolation-state queries."""
    queries = []

    def execute(sql):
        queries.append(sql)
        cursor = mock_cursor
        if sql == "SELECT @@transaction_isolation":
            cursor._fetchone = (default_isolation,)
        elif sql == "SELECT @@autocommit":
            cursor._fetchone = (autocommit,)
        return 0

    mock_cursor = Mock()
    mock_cursor.execute.side_effect = execute
    mock_cursor.fetchone.side_effect = lambda: getattr(mock_cursor, "_fetchone", None)

    conn = Mock()
    conn.cursor.return_value = mock_cursor
    conn.recorded = queries
    return conn


def test_dialect_offers_autocommit_isolation_level():
    from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect

    dialect = MatrixOneDialect()
    assert "AUTOCOMMIT" in dialect.get_isolation_level_values(None)


def test_dialect_autocommit_set_restores_full_baseline():
    from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect

    dialect = MatrixOneDialect()
    conn = _dbapi_conn()
    dialect.set_isolation_level(conn, "AUTOCOMMIT")
    conn.autocommit.assert_called_once_with(True)
    # default isolation captured from the untouched connection
    assert conn.recorded[0] == "SELECT @@transaction_isolation"
    assert conn.recorded[1] == "SELECT @@autocommit"
    assert dialect.default_isolation_level == "REPEATABLE READ"
    assert dialect.default_autocommit is False

    conn.autocommit.reset_mock()
    dialect.reset_isolation_level(conn)
    conn.autocommit.assert_called_once_with(False)
    assert "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ" in conn.recorded


def test_dialect_default_isolation_captured_once():
    from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect

    dialect = MatrixOneDialect()
    conn = _dbapi_conn(default_isolation="READ COMMITTED")
    dialect.set_isolation_level(conn, "AUTOCOMMIT")
    dialect.reset_isolation_level(conn)
    dialect.set_isolation_level(conn, "REPEATABLE READ")
    # snapshot query ran exactly once; reset restored the captured READ COMMITTED
    assert conn.recorded.count("SELECT @@transaction_isolation") == 1


def test_dialect_reset_restores_autocommit_baseline():
    # Engine baseline with autocommit on (e.g. a caller's engine configured
    # with isolation_level='AUTOCOMMIT'): a temporary non-autocommit
    # isolation selection must reset back to autocommit on, not off, or the
    # engine's commit behavior changes for every later checkout.
    from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect

    dialect = MatrixOneDialect()
    conn = _dbapi_conn(autocommit="1")

    dialect.set_isolation_level(conn, "READ COMMITTED")
    # during the temporary selection, autocommit is off
    conn.autocommit.assert_called_once_with(False)
    conn.autocommit.reset_mock()

    assert dialect.default_autocommit is True
    before = len(conn.recorded)
    dialect.reset_isolation_level(conn)
    # baseline restored: autocommit back on, with no isolation-level SQL
    conn.autocommit.assert_called_once_with(True)
    assert not any("ISOLATION LEVEL" in q for q in conn.recorded[before:])


class _MethodAutocommitConn:
    """pymysql-style connection: autocommit is a callable method."""

    def __init__(self):
        self.calls = []

    def autocommit(self, value):
        self.calls.append(value)


class _PropertyAutocommitConn:
    """mysql-connector-style connection: autocommit is a property."""

    def __init__(self):
        self.assigned = []

    @property
    def autocommit(self):
        return getattr(self, "_ac", False)

    @autocommit.setter
    def autocommit(self, value):
        self.assigned.append(value)
        self._ac = value


def test_dialect_autocommit_uses_method_for_pymysql_style():
    from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect

    conn = _MethodAutocommitConn()
    MatrixOneDialect._apply_autocommit(conn, True)
    MatrixOneDialect._apply_autocommit(conn, False)
    assert conn.calls == [True, False]


def test_dialect_autocommit_uses_assignment_for_property_style():
    from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect

    conn = _PropertyAutocommitConn()
    MatrixOneDialect._apply_autocommit(conn, True)
    MatrixOneDialect._apply_autocommit(conn, False)
    assert conn.assigned == [True, False]
    assert conn.autocommit is False


class _FullFakeCursor:
    """Minimal cursor answering the dialect's isolation snapshot/reset SQL."""

    def __init__(self, default_isolation="REPEATABLE READ"):
        self._row = (default_isolation,)
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)

    def fetchone(self):
        return self._row

    def close(self):
        pass


def test_dialect_set_reset_autocommit_handles_property_drivers():
    # from_engine() accepts mysql+mysqlconnector engines; the isolation
    # overrides must not call a property as if it were a method.
    from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect

    dialect = MatrixOneDialect()

    class PropertyConnWithCursor(_PropertyAutocommitConn):
        def cursor(self):
            return _FullFakeCursor(default_isolation="REPEATABLE READ")

    conn = PropertyConnWithCursor()
    dialect.set_isolation_level(conn, "AUTOCOMMIT")
    dialect.reset_isolation_level(conn)
    assert conn.assigned == [True, False]


def test_dialect_decodes_bytes_isolation_level():
    from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect

    dialect = MatrixOneDialect()
    queries = []

    class ByteCursor:
        def execute(self, sql):
            queries.append(sql)

        def fetchone(self):
            return (b"REPEATABLE-READ",)

        def close(self):
            pass

    conn = Mock()
    conn.cursor.return_value = ByteCursor()
    dialect._ensure_default_isolation_level(conn)
    assert dialect.default_isolation_level == "REPEATABLE READ"

    conn.autocommit = Mock()
    dialect.reset_isolation_level(conn)
    assert "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ" in queries
    assert "b'" not in queries[-1]


# ─── real QueuePool integration with a fake driver ───────────────────────────
# Exercises the actual SQLAlchemy pooling machinery the SDK relies on, proving
# that failed flips release the checkout and that pool baselines are preserved
# over forced reuse (pool_size=1 => the same physical connection must serve
# every statement).


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self._rows = []
        self.description = None
        self.rowcount = 0

    def execute(self, sql, args=None):
        from pymysql.err import OperationalError

        normalized = " ".join(str(sql).split()).lower()
        self._conn.executed.append(str(sql))
        self._rows = []
        self.description = None
        self.rowcount = 0
        if "set autocommit" in normalized and self._conn.fail_set_autocommit:
            raise OperationalError(2006, "injected SET AUTOCOMMIT failure")
        if "set autocommit = 1" in normalized:
            self._conn.autocommit_state = "1"
            if self._conn.fail_flip_after_apply:
                # server applied the change, then the read died with an error
                # that must NOT disconnect-invalidate the connection, so the
                # restore path itself has to prove it can fix the baseline
                raise ValueError("read died after apply")
        elif "set autocommit = 0" in normalized:
            if self._conn.fail_restore:
                raise OperationalError(2006, "injected restore failure")
            if self._conn.fail_restore_soft:
                # not a disconnect code: the engine must NOT auto-invalidate,
                # so the SDK's own discard path is the one under test
                raise OperationalError(1105, "injected non-disconnect restore refusal")
            self._conn.autocommit_state = "0"
        elif "data branch merge" in normalized and self._conn.fail_merge:
            raise OperationalError(2006, "injected merge failure")
        elif "select @@autocommit" in normalized:
            self.description = (("@@autocommit", None, None, None, None, None, None),)
            self._rows = [(self._conn.autocommit_state,)]
        elif "version()" in normalized:
            self.description = (("VERSION()", None, None, None, None, None, None),)
            self._rows = [("8.0.30-MatrixOne-v1.3.0",)]
        elif "database()" in normalized:
            self.description = (("DATABASE()", None, None, None, None, None, None),)
            self._rows = [("test",)]
        elif "@@transaction_isolation" in normalized or "@@tx_isolation" in normalized:
            self.description = (("@@transaction_isolation", None, None, None, None, None, None),)
            self._rows = [("REPEATABLE READ",)]
        elif normalized.startswith("select") or normalized.startswith("show"):
            # generic single-cell answer for dialect initialize probes
            # (@@sql_mode, @@lower_case_table_names, ...)
            self.description = (("v", None, None, None, None, None, None),)
            self._rows = [("0",)]
        elif "select 1" in normalized:
            self.description = (("1", None, None, None, None, None, None),)
            self._rows = [(1,)]
            self.rowcount = 1
        return self.rowcount

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def fetchmany(self, size=None):
        return self.fetchall()

    def close(self):
        pass


class _FakeDbapiConnection:
    def __init__(
        self,
        autocommit="0",
        fail_set_autocommit=False,
        fail_flip_after_apply=False,
        fail_restore=False,
        fail_restore_soft=False,
        fail_merge=False,
    ):
        self.autocommit_state = autocommit
        self.fail_set_autocommit = fail_set_autocommit
        self.fail_flip_after_apply = fail_flip_after_apply
        self.fail_restore = fail_restore
        self.fail_restore_soft = fail_restore_soft
        self.fail_merge = fail_merge
        self.executed = []
        self.rollback_calls = 0
        self.close_calls = 0

    def cursor(self):
        return _FakeCursor(self)

    def autocommit(self, value):
        # driver-level autocommit setter used by the isolation-level API
        # (dialect set_isolation_level / reset_isolation_level, engine
        # first-connect hooks)
        self.autocommit_state = "1" if value else "0"

    def character_set_name(self):
        # required by the pymysql/mysqldb dialect's connect hook
        return "utf8mb4"

    def commit(self):
        pass

    def rollback(self):
        self.rollback_calls += 1

    def close(self):
        self.close_calls += 1


def _pool_engine(created, _engine_kwargs=None, **conn_kwargs):
    def creator():
        conn = _FakeDbapiConnection(**conn_kwargs)
        created.append(conn)
        return conn

    engine = sqlalchemy.create_engine(
        "mysql+pymysql://root:pw@fake-host:6001/test",
        creator=creator,
        pool_size=1,
        max_overflow=0,
        **(_engine_kwargs or {}),
    )
    return engine


def test_real_pool_reuse_preserves_autocommit_baseline():
    created = []
    engine = _pool_engine(created, autocommit="0")
    client = Client(logger=Mock())
    client._engine = engine

    client.execute("data branch merge src into dst")
    client.execute("data branch merge src into dst")
    client.execute("select 1")

    assert len(created) == 1  # same physical connection served everything
    conn = created[0]
    assert conn.autocommit_state == "0"  # restored after every merge
    executed = conn.executed
    assert executed.count("SELECT @@autocommit") == 2
    assert executed.count("SET autocommit = 1") == 2
    assert executed.count("SET autocommit = 0") == 2
    # merge path never touches transaction isolation
    assert not any("ISOLATION LEVEL" in s for s in executed)
    engine.dispose()


def test_real_pool_autocommit_true_baseline_not_corrupted():
    # Client.from_engine() engines may pool autocommit=True connections (the
    # review's reported corruption). Nothing may flip them.
    created = []
    engine = _pool_engine(created, autocommit="1")
    client = Client(logger=Mock())
    client._engine = engine

    client.execute("data branch merge src into dst")
    client.execute("data branch merge src into dst")

    assert len(created) == 1
    conn = created[0]
    assert conn.autocommit_state == "1"
    assert "SET autocommit = 1" not in conn.executed
    assert "SET autocommit = 0" not in conn.executed
    engine.dispose()


def test_real_pool_flip_failure_releases_checkout():
    created = []
    engine = _pool_engine(created, autocommit="0", fail_set_autocommit=True)
    client = Client(logger=Mock())
    client._engine = engine

    with pytest.raises(Exception):
        client.execute("data branch merge src into dst")

    # The pool must not be exhausted by the failure: the next checkout either
    # reuses the connection or recreates it, without blocking.
    assert engine.pool.checkedout() == 0
    before = len(created)
    with engine.connect() as conn:
        conn.exec_driver_sql("SELECT 1")
    assert len(created) <= before + 1
    engine.dispose()


def test_real_pool_flip_applied_then_failed_restores_next_checkout_baseline():
    # The reviewer's gap: the SET succeeds server-side but raises locally.
    # Restoration responsibility was recorded before the flip, so the same
    # physical connection must come back with its original baseline.
    created = []
    engine = _pool_engine(created, autocommit="0", fail_flip_after_apply=True)
    client = Client(logger=Mock())
    client._engine = engine

    with pytest.raises(Exception):
        client.execute("data branch merge src into dst")

    assert len(created) == 1  # restore worked; connection stayed usable
    conn = created[0]
    assert conn.autocommit_state == "0"  # baseline restored after the failed flip
    assert engine.pool.checkedout() == 0
    with engine.connect() as checked:
        assert checked.exec_driver_sql("SELECT @@autocommit").scalar() == "0"
    engine.dispose()


def test_real_pool_restore_failure_replaces_connection_without_masking_success():
    created = []
    engine = _pool_engine(created, autocommit="0", fail_restore=True)
    client = Client(logger=Mock())
    client._engine = engine

    # merge itself succeeded; the failed restore must not change that outcome
    result = client.execute("data branch merge src into dst")
    assert result is not None

    # the mutated connection was invalidated, so the next checkout is recreated
    with engine.connect() as checked:
        assert checked.exec_driver_sql("SELECT 1") is not None
    assert len(created) == 2
    engine.dispose()


def test_real_pool_invalidate_failure_preserves_success_and_replaces_connection(monkeypatch):
    # Direct probe of the review's remaining edge with the real pool
    # machinery: every Connection.invalidate() call runs the genuine discard
    # and then raises. The code must (a) preserve the merge's successful
    # outcome despite the invalidate failure escaping from the engine, and
    # (b) still dispose of the mutated physical connection, which the next
    # checkout proves by being a fresh one.
    from sqlalchemy.engine.base import Connection as SAConnection

    created = []
    engine = _pool_engine(created, autocommit="0", fail_restore=True)
    client = Client(logger=Mock())
    client._engine = engine

    real_invalidate = SAConnection.invalidate
    discard_calls = []

    def raising_invalidate(self, exception=None):
        real_invalidate(self, exception)  # the genuine pool disposal still runs
        discard_calls.append(exception)
        raise RuntimeError("injected post-disposal invalidate failure")

    monkeypatch.setattr(SAConnection, "invalidate", raising_invalidate)

    result = client.execute("data branch merge src into dst")
    assert result is not None  # outcome preserved even though every invalidate raised
    assert discard_calls  # the engine's auto-invalidation ran through the wrapper

    # the mutated connection was discarded: the next checkout is recreated
    with engine.connect() as checked:
        assert checked.exec_driver_sql("SELECT 1") is not None
    assert len(created) == 2
    engine.dispose()


def test_real_pool_restore_failure_with_raising_invalidate_listener_not_repooled():
    # The review's reproduced leak: SQLAlchemy 1.4 fires PoolEvents.invalidate
    # listeners BEFORE the record's hard close, so a raising listener leaves
    # the live connection - still flipped to autocommit=1 - checked out, and
    # the pool would hand it to the next checkout. The restore failure here
    # uses a non-disconnect error so the engine does not auto-invalidate and
    # the SDK's own discard path is the one under test. On 2.x the record is
    # marked for recycle instead; the observable contract is the same: the
    # mutated connection is never reused.
    from sqlalchemy import event

    created = []
    engine = _pool_engine(created, autocommit="0", fail_restore_soft=True)

    @event.listens_for(engine, "invalidate")
    def raising_listener(dbapi_connection, connection_record, exception):
        raise RuntimeError("injected invalidate listener failure")

    client = Client(logger=Mock())
    client._engine = engine

    result = client.execute("data branch merge src into dst")
    assert result is not None  # merge outcome preserved despite the listener

    # next checkout is a fresh physical connection at the engine baseline;
    # the fresh checkout proves the mutated connection was never reused
    with engine.connect() as checked:
        assert checked.exec_driver_sql("SELECT @@autocommit").scalar() == "0"
    assert len(created) == 2
    # the mutated physical connection was closed (1.4: literal close after
    # detach at checkout release; 2.x: recycle-close of the record the
    # interrupted invalidate marked at the same release)
    assert created[0].close_calls == 1
    assert engine.pool.checkedout() == 0
    engine.dispose()


def test_real_pool_autocommit_engine_isolation_baseline_restored():
    # The review's other pool-state finding: a caller's engine configured
    # with isolation_level='AUTOCOMMIT' pools connections whose baseline is
    # autocommit on (the driver dialect's connect hook applies it to every
    # fresh physical connection). Resetting a temporary per-connection
    # isolation selection must restore that baseline - autocommit flag
    # included - instead of forcing autocommit off for every later checkout.
    created = []
    engine = _pool_engine(
        created,
        _engine_kwargs={"isolation_level": "AUTOCOMMIT"},
        autocommit="0",
    )
    client = Client.from_engine(engine, logger=Mock())

    with client._engine.connect() as conn:
        # the engine baseline was applied to the fresh physical connection
        assert created[0].autocommit_state == "1"
        opt = conn.execution_options(isolation_level="READ COMMITTED")
        opt.exec_driver_sql("SELECT 1")
        # during the temporary selection, autocommit is off
        assert created[0].autocommit_state == "0"
        opt.close()

    # after checkin, the pool reset restored the engine baseline - and the
    # same physical connection keeps serving at that baseline
    assert created[0].autocommit_state == "1"
    with client._engine.connect() as conn:
        assert conn.exec_driver_sql("SELECT @@autocommit").scalar() == "1"
    assert len(created) == 1
    engine.dispose()
