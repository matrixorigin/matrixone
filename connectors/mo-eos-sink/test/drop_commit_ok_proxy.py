#!/usr/bin/env python3
"""Drop the first MySQL COMMIT response after forwarding the request to MO."""

import argparse
import socket
import threading


def packet(sock):
    header = exact(sock, 4)
    size = int.from_bytes(header[:3], "little")
    return header + exact(sock, size)


def exact(sock, size):
    chunks = []
    while size:
        chunk = sock.recv(size)
        if not chunk:
            raise EOFError
        chunks.append(chunk)
        size -= len(chunk)
    return b"".join(chunks)


def serve(client, host, port, dropped):
    server = socket.create_connection((host, port), timeout=10)
    server.settimeout(None)
    pending = threading.Event()

    def upstream():
        try:
            while True:
                data = packet(client)
                if not dropped.is_set() and data[4:] == b"\x03COMMIT":
                    pending.set()
                server.sendall(data)
        except (EOFError, OSError):
            pass
        finally:
            try:
                server.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    sender = threading.Thread(target=upstream, daemon=True)
    sender.start()
    try:
        while True:
            data = packet(server)
            if pending.is_set() and not dropped.is_set():
                dropped.set()
                print("DROPPED COMMIT RESPONSE", flush=True)
                break
            client.sendall(data)
    except (EOFError, OSError):
        pass
    finally:
        client.close()
        server.close()
        sender.join(timeout=1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", type=int, default=16058)
    parser.add_argument("--server", type=int, default=16057)
    args = parser.parse_args()
    dropped = threading.Event()
    with socket.create_server(("127.0.0.1", args.listen)) as listener:
        print("PROXY READY", flush=True)
        while True:
            client, _ = listener.accept()
            threading.Thread(target=serve, args=(client, "127.0.0.1", args.server, dropped), daemon=True).start()


if __name__ == "__main__":
    main()
