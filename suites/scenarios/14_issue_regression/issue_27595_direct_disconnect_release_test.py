#!/usr/bin/env python3

import importlib.util
import socket
import threading
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).with_name("issue_27595_direct_disconnect_release.py")
SPEC = importlib.util.spec_from_file_location("issue_27595", SCRIPT)
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


class DirectDisconnectReleaseTest(unittest.TestCase):
    def make_connections(self):
        disconnected = threading.Event()
        connections = [
            FakeConnection("admin"),
            FakeConnection("setup"),
            FakeConnection("holder", disconnected),
            FakeConnection("observer"),
            FakeConnection("waiter"),
        ]
        return connections, disconnected

    def run_main(self, *, waiter_rows=(("running",),), holder_succeeds=False):
        connections, disconnected = self.make_connections()

        def execute(conn, sql):
            normalized = " ".join(sql.lower().split())
            if conn.name == "holder" and normalized == "select connection_id()":
                return ((7,),)
            if conn.name == "holder" and "for update" in normalized:
                return (("running",),)
            if conn.name == "holder" and normalized == "select sleep(60)":
                disconnected.wait(2)
                if holder_succeeds:
                    return ((0,),)
                raise OSError("client socket closed")
            if conn.name == "waiter" and "for update" in normalized:
                return waiter_rows
            return ()

        with (
            mock.patch.object(ISSUE, "connect", side_effect=connections),
            mock.patch.object(ISSUE, "execute", side_effect=execute),
            mock.patch.object(ISSUE, "wait_for_statement", return_value=True),
            mock.patch.object(ISSUE.time, "monotonic", side_effect=[10, 10.5]),
        ):
            result = ISSUE.main()
        return result, connections

    def test_connect_passes_network_and_timeout_options(self):
        with mock.patch.object(ISSUE.pymysql, "connect", return_value="connection") as connect:
            result = ISSUE.connect(database="db", autocommit=False, read_timeout=5)

        self.assertEqual("connection", result)
        connect.assert_called_once_with(
            host=ISSUE.HOST,
            port=ISSUE.PORT,
            user=ISSUE.USER,
            password=ISSUE.PASSWORD,
            database="db",
            autocommit=False,
            connect_timeout=10,
            read_timeout=5,
            write_timeout=10,
        )

    def test_wait_for_statement_matches_connection_and_sql(self):
        observer = FakeConnection("observer")
        processlist = (
            ("node", 6, "other", "select sleep(60)"),
            ("node", 7, "holder", "SELECT  SLEEP(60)"),
        )
        with (
            mock.patch.object(ISSUE, "execute", return_value=processlist),
            mock.patch.object(ISSUE.time, "monotonic", side_effect=[0, 0.1]),
        ):
            self.assertTrue(ISSUE.wait_for_statement(observer, 7, "select sleep(60)"))

    def test_main_releases_lock_after_direct_disconnect(self):
        result, connections = self.run_main()

        self.assertEqual(0, result)
        self.assertEqual(socket.SHUT_RDWR, connections[2]._sock.shutdown_how)
        self.assertTrue(connections[2]._sock.closed)
        self.assertTrue(all(conn.closed for conn in connections))

    def test_main_rejects_unexpected_waiter_rows(self):
        result, _ = self.run_main(waiter_rows=())
        self.assertEqual(1, result)

    def test_main_rejects_successful_disconnected_statement(self):
        result, _ = self.run_main(holder_succeeds=True)
        self.assertEqual(1, result)

    def test_main_requires_observed_sleep_statement(self):
        connections, _ = self.make_connections()
        with (
            mock.patch.object(ISSUE, "connect", side_effect=connections),
            mock.patch.object(ISSUE, "execute", return_value=(("running",),)),
            mock.patch.object(ISSUE, "wait_for_statement", return_value=False),
        ):
            with self.assertRaisesRegex(AssertionError, "did not enter"):
                ISSUE.main()


if __name__ == "__main__":
    unittest.main()
