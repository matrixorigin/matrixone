# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Local MySQL packet proxy for lost-ack/stalled-read fault injection.

Only connects to the explicitly configured loopback test service. It does not
change network policy or touch any remote database. Capture SQL command names,
never authentication packets or credentials.
"""

import os
import socket
import socketserver
import threading


def _read_exact(sock, length):
    data = bytearray()
    while len(data) < length:
        block = sock.recv(length - len(data))
        if not block:
            raise EOFError
        data.extend(block)
    return bytes(data)


def _packet(sock):
    header = _read_exact(sock, 4)
    length = int.from_bytes(header[:3], "little")
    return header + _read_exact(sock, length)


class FaultProxy:
    def __init__(self, mode, target_port=None, stall_sql=None):
        self.mode, self.target_port, self.stall_sql = (
            mode,
            int(os.environ.get('MO_PORT', '6001')) if target_port is None else target_port,
            stall_sql,
        )
        self.triggered = threading.Event()
        self.stop_event = threading.Event()
        self.commit_ack_seen = False
        self.create_requests = 0
        self._sockets = set()
        self._lock = threading.Lock()
        self._used = False
        proxy = self

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                downstream = self.request
                upstream = socket.create_connection(("127.0.0.1", proxy.target_port), timeout=5)
                upstream.settimeout(None)
                with proxy._lock:
                    proxy._sockets.update((upstream, downstream))
                state = {"loaded": False, "created": False, "query": ""}
                terminal = threading.Event()

                def pump_requests():
                    try:
                        while not terminal.is_set():
                            pkt = _packet(downstream)
                            if pkt[4:5] == b"\x03":
                                sql = pkt[5:].decode("utf-8", errors="replace").strip().upper()
                                state["query"] = sql
                                if sql.startswith("LOAD DATA"):
                                    state["loaded"] = True
                                if sql.startswith("CREATE EXTERNAL TABLE"):
                                    state["created"] = True
                                    with proxy._lock:
                                        proxy.create_requests += 1
                                if proxy.mode == "disconnect_before_commit" and state["loaded"] and sql == "COMMIT":
                                    proxy.triggered.set()
                                    raise EOFError
                            upstream.sendall(pkt)
                    except (EOFError, OSError):
                        terminal.set()
                        for s in (upstream, downstream):
                            try:
                                s.shutdown(socket.SHUT_RDWR)
                            except OSError:
                                pass

                requests = threading.Thread(target=pump_requests, daemon=True)
                requests.start()
                try:
                    while not terminal.is_set():
                        pkt = _packet(upstream)
                        drop = state["query"] == "COMMIT" and (
                            (state["loaded"] and proxy.mode == "drop_commit_ack")
                            or (state["created"] and proxy.mode == "drop_create_commit_ack")
                        )
                        stall = (
                            proxy.mode == "stall_result"
                            and proxy.stall_sql
                            and state["query"] == proxy.stall_sql.upper()
                        )
                        with proxy._lock:
                            fault = (drop or stall) and not proxy._used
                            if fault:
                                proxy._used = True
                        if fault:
                            if drop:
                                proxy.commit_ack_seen = pkt[4:5] == b"\x00"
                                proxy.triggered.set()
                                raise EOFError
                            proxy.triggered.set()
                            proxy.stop_event.wait(10)
                            raise EOFError
                        downstream.sendall(pkt)
                except (EOFError, OSError):
                    pass
                finally:
                    terminal.set()
                    for s in (upstream, downstream):
                        try:
                            s.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
                        s.close()
                    requests.join(timeout=1)
                    with proxy._lock:
                        proxy._sockets.difference_update((upstream, downstream))

        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True

        self.server = Server(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *args):
        self.stop_event.set()
        with self._lock:
            for sock in list(self._sockets):
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
