#!/usr/bin/env python3

import importlib.util
import socket
import threading
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).with_name("issue_25599_proxy_disconnect_cancel.py")
SPEC = importlib.util.spec_from_file_location("issue_25599", SCRIPT)
ISSUE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ISSUE)


class FakeSocket:
    def __init__(self, disconnected):
        self.disconnected = disconnected
        self.shutdown_how = None
        self.closed = False

    def shutdown(self, how):
        self.shutdown_how = how
        self.disconnected.set()

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, name, disconnected=None):
        self.name = name
        self.closed = False
        self._sock = FakeSocket(disconnected) if disconnected else None

    def close(self):
        self.closed = True


class FakeCursor:
    def __init__(self):
        self.sql = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql):
        self.sql = sql

    def fetchall(self):
        return ((1,),)


class CursorConnection:
    def __init__(self):
        self.cursor_value = FakeCursor()

    def cursor(self):
        return self.cursor_value


class ProxyDisconnectCancelTest(unittest.TestCase):
    def make_connections(self):
        disconnected = threading.Event()
        connections = [
            FakeConnection("admin"),
            FakeConnection("setup"),
            FakeConnection("session_a"),
            FakeConnection("session_b", disconnected),
            FakeConnection("verifier"),
        ]
        return connections, disconnected

    def run_main(self, *, backend_succeeds=False, rows=((100,),)):
        connections, disconnected = self.make_connections()

        def execute(conn, sql):
            if conn.name == "session_b":
                disconnected.wait(2)
                if backend_succeeds:
                    return ()
                raise OSError("client socket closed")
            if conn.name == "verifier":
                return rows
            return ()

        with (
            mock.patch.object(ISSUE, "connect", side_effect=connections),
            mock.patch.object(ISSUE, "execute", side_effect=execute),
            mock.patch.object(ISSUE.time, "sleep", return_value=None),
            mock.patch.object(ISSUE.time, "monotonic", side_effect=[0, 1, 6]),
        ):
            result = ISSUE.main()
        return result, connections

    def test_connect_passes_network_and_session_options(self):
        with mock.patch.object(ISSUE.pymysql, "connect", return_value="connection") as connect:
            result = ISSUE.connect(database="db", autocommit=False)

        self.assertEqual("connection", result)
        connect.assert_called_once_with(
            host=ISSUE.HOST,
            port=ISSUE.PORT,
            user=ISSUE.USER,
            password=ISSUE.PASSWORD,
            database="db",
            autocommit=False,
            connect_timeout=10,
            read_timeout=30,
            write_timeout=10,
        )

    def test_execute_returns_all_rows(self):
        conn = CursorConnection()
        self.assertEqual(((1,),), ISSUE.execute(conn, "select 1"))
        self.assertEqual("select 1", conn.cursor_value.sql)

    def test_main_cancels_disconnected_update(self):
        result, connections = self.run_main()

        self.assertEqual(0, result)
        self.assertEqual(socket.SHUT_RDWR, connections[3]._sock.shutdown_how)
        self.assertTrue(connections[3]._sock.closed)
        self.assertTrue(all(conn.closed for conn in connections if conn.name != "session_b"))

    def test_main_detects_backend_update_success(self):
        result, _ = self.run_main(backend_succeeds=True)
        self.assertEqual(1, result)

    def test_main_detects_committed_value(self):
        result, _ = self.run_main(rows=((400,),))
        self.assertEqual(1, result)

    def test_main_requires_blocked_update_to_start(self):
        started = mock.Mock()
        started.wait.return_value = False
        finished = mock.Mock()
        with (
            mock.patch.object(ISSUE, "connect", side_effect=self.make_connections()[0]),
            mock.patch.object(ISSUE, "execute", return_value=()),
            mock.patch.object(ISSUE.threading, "Event", side_effect=[started, finished]),
            mock.patch.object(ISSUE.threading, "Thread"),
        ):
            with self.assertRaisesRegex(AssertionError, "did not start"):
                ISSUE.main()

    def test_main_requires_update_to_be_blocked(self):
        started = mock.Mock()
        started.wait.return_value = True
        finished = mock.Mock()
        finished.is_set.return_value = True
        with (
            mock.patch.object(ISSUE, "connect", side_effect=self.make_connections()[0]),
            mock.patch.object(ISSUE, "execute", return_value=()),
            mock.patch.object(ISSUE.threading, "Event", side_effect=[started, finished]),
            mock.patch.object(ISSUE.threading, "Thread"),
            mock.patch.object(ISSUE.time, "sleep", return_value=None),
        ):
            with self.assertRaisesRegex(AssertionError, "did not block"):
                ISSUE.main()

    def test_main_detects_backend_that_does_not_terminate(self):
        started = mock.Mock()
        started.wait.return_value = True
        finished = mock.Mock()
        finished.is_set.return_value = False
        finished.wait.return_value = False
        connections, _ = self.make_connections()
        with (
            mock.patch.object(ISSUE, "connect", side_effect=connections),
            mock.patch.object(ISSUE, "execute", return_value=()),
            mock.patch.object(ISSUE.threading, "Event", side_effect=[started, finished]),
            mock.patch.object(ISSUE.threading, "Thread"),
            mock.patch.object(ISSUE.time, "sleep", return_value=None),
        ):
            self.assertEqual(1, ISSUE.main())


if __name__ == "__main__":
    unittest.main()
