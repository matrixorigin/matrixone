#!/usr/bin/env python3

import os
import socket
import sys
import threading
import time

import pymysql


HOST = os.getenv("MO_HOST", "127.0.0.1")
PORT = int(os.getenv("MO_PORT", "6001"))
USER = os.getenv("MO_USER", "root")
PASSWORD = os.getenv("MO_PASSWORD", "111")
DATABASE = os.getenv("MO_DATABASE", "issue_25599")


def connect(*, database=None, autocommit=True):
    return pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=database,
        autocommit=autocommit,
        connect_timeout=10,
        read_timeout=30,
        write_timeout=10,
    )


def execute(conn, sql):
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def main():
    admin = connect()
    execute(admin, f"drop database if exists `{DATABASE}`")
    execute(admin, f"create database `{DATABASE}`")
    admin.close()

    setup = connect(database=DATABASE)
    execute(setup, "create table t (id int primary key, v int)")
    execute(setup, "insert into t values (1, 100)")
    setup.close()

    session_a = connect(database=DATABASE, autocommit=False)
    session_b = connect(database=DATABASE, autocommit=True)
    execute(session_a, "update t set v = 200 where id = 1")

    update_started = threading.Event()
    update_finished = threading.Event()
    update_result = {}

    def blocked_update():
        update_started.set()
        try:
            execute(session_b, "update t set v = 400 where id = 1")
            update_result["success"] = True
        except Exception as exc:
            update_result["error"] = repr(exc)
        finally:
            update_finished.set()

    worker = threading.Thread(target=blocked_update, daemon=True)
    worker.start()
    if not update_started.wait(5):
        raise AssertionError("Session B did not start its UPDATE")
    time.sleep(1)
    if update_finished.is_set():
        raise AssertionError(f"Session B UPDATE did not block: {update_result}")

    client_socket = session_b._sock
    client_socket.shutdown(socket.SHUT_RDWR)
    client_socket.close()

    time.sleep(1)
    execute(session_a, "rollback")
    session_a.close()
    if not update_finished.wait(10):
        print("FAIL: disconnected client execution did not terminate")
        return 1
    worker.join()

    verifier = connect(database=DATABASE)
    deadline = time.monotonic() + 5
    rows = None
    while time.monotonic() < deadline:
        rows = execute(verifier, "select v from t where id = 1")
        if rows != ((100,),):
            break
        time.sleep(0.1)
    verifier.close()

    if update_result.get("success"):
        print("FAIL: disconnected client execution returned success")
        return 1
    print("PASS: disconnected client execution terminates without success")

    if rows != ((100,),):
        print(
            "FAIL: disconnected blocked UPDATE does not commit\n"
            f"expected v=100, actual rows={list(rows or ())}"
        )
        return 1
    print("PASS: disconnected blocked UPDATE was canceled")
    return 0


if __name__ == "__main__":
    sys.exit(main())
