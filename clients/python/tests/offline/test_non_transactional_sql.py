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


def _dbapi_conn(default_isolation="REPEATABLE READ"):
    """Mock DBAPI connection answering the isolation-state queries."""
    queries = []

    def execute(sql):
        queries.append(sql)
        cursor = mock_cursor
        if sql == "SELECT @@transaction_isolation":
            cursor._fetchone = (default_isolation,)
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
    assert dialect.default_isolation_level == "REPEATABLE READ"

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
    assert "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED" in conn.recorded


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
        fail_merge=False,
    ):
        self.autocommit_state = autocommit
        self.fail_set_autocommit = fail_set_autocommit
        self.fail_flip_after_apply = fail_flip_after_apply
        self.fail_restore = fail_restore
        self.fail_merge = fail_merge
        self.executed = []
        self.rollback_calls = 0
        self.close_calls = 0

    def cursor(self):
        return _FakeCursor(self)

    def character_set_name(self):
        # required by the pymysql/mysqldb dialect's connect hook
        return "utf8mb4"

    def commit(self):
        pass

    def rollback(self):
        self.rollback_calls += 1

    def close(self):
        self.close_calls += 1


def _pool_engine(created, **conn_kwargs):
    def creator():
        conn = _FakeDbapiConnection(**conn_kwargs)
        created.append(conn)
        return conn

    engine = sqlalchemy.create_engine(
        "mysql+pymysql://root:pw@fake-host:6001/test",
        creator=creator,
        pool_size=1,
        max_overflow=0,
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
