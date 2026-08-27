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
DATABASE = os.getenv("MO_DATABASE", "issue_27595")


def connect(*, database=None, autocommit=True, read_timeout=10):
    return pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=database,
        autocommit=autocommit,
        connect_timeout=10,
        read_timeout=read_timeout,
        write_timeout=10,
    )


def execute(conn, sql):
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def wait_for_statement(observer, connection_id, sql_fragment, timeout=5):
    deadline = time.monotonic() + timeout
    normalized_fragment = " ".join(sql_fragment.lower().split())
    while time.monotonic() < deadline:
        for row in execute(observer, "show processlist"):
            normalized_cells = [" ".join(str(cell).lower().split()) for cell in row]
            if str(connection_id) in normalized_cells and any(
                normalized_fragment in cell for cell in normalized_cells
            ):
                return True
        time.sleep(0.05)
    return False


def disconnect(conn):
    conn._sock.shutdown(socket.SHUT_RDWR)
    conn._sock.close()


def close_quietly(conn):
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def main():
    admin = setup = holder = observer = waiter = None
    holder_finished = threading.Event()
    holder_result = {}
    try:
        admin = connect()
        execute(admin, f"drop database if exists `{DATABASE}`")
        execute(admin, f"create database `{DATABASE}`")

        setup = connect(database=DATABASE)
        execute(
            setup,
            "create table lock_probe ("
            "user_id varchar(128) not null, "
            "session_id varchar(64) not null, "
            "status varchar(20) not null, "
            "primary key (user_id, session_id))",
        )
        execute(setup, "insert into lock_probe values ('u', 's', 'running')")

        holder = connect(database=DATABASE, autocommit=False, read_timeout=70)
        observer = connect(database=DATABASE)
        waiter = connect(database=DATABASE, autocommit=False, read_timeout=5)
        connection_id = execute(holder, "select connection_id()")[0][0]
        locked = execute(
            holder,
            "select status from lock_probe "
            "where user_id = 'u' and session_id = 's' for update",
        )
        if locked != (("running",),):
            raise AssertionError(f"holder did not acquire the expected row: {locked}")

        def run_sleep():
            try:
                execute(holder, "select sleep(60)")
                holder_result["success"] = True
            except Exception as exc:
                holder_result["error"] = repr(exc)
            finally:
                holder_finished.set()

        worker = threading.Thread(target=run_sleep, daemon=True)
        worker.start()
        if not wait_for_statement(observer, connection_id, "select sleep(60)"):
            raise AssertionError("holder did not enter SELECT SLEEP(60)")

        disconnect(holder)
        started = time.monotonic()
        rows = execute(
            waiter,
            "select status from lock_probe "
            "where user_id = 'u' and session_id = 's' for update",
        )
        elapsed = time.monotonic() - started
        execute(waiter, "commit")

        if rows != (("running",),):
            print(f"FAIL: waiter returned unexpected rows: {rows}")
            return 1
        if elapsed >= 5:
            print(f"FAIL: disconnected holder retained its lock for {elapsed:.3f}s")
            return 1
        if not holder_finished.wait(5):
            print("FAIL: disconnected holder statement did not terminate")
            return 1
        if holder_result.get("success"):
            print("FAIL: disconnected holder statement returned success")
            return 1

        worker.join()
        print(f"PASS: disconnected holder released its lock in {elapsed:.3f}s")
        return 0
    finally:
        close_quietly(waiter)
        close_quietly(observer)
        close_quietly(holder)
        close_quietly(setup)
        if admin is not None:
            try:
                execute(admin, f"drop database if exists `{DATABASE}`")
            except Exception:
                pass
        close_quietly(admin)


if __name__ == "__main__":
    sys.exit(main())
