# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Minimal process-group liveness watchdog for a Python UDF handler."""

import os
import signal
import sys


def _watch(read_fd: int, process_group_id: int) -> None:
    try:
        while True:
            if os.read(read_fd, 1) == b"":
                try:
                    os.killpg(process_group_id, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                return
    except (OSError, ValueError):
        return
    finally:
        try:
            os.close(read_fd)
        except OSError:
            pass


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: watchdog.py READ_FD PROCESS_GROUP_ID")
    try:
        read_fd = int(sys.argv[1])
        process_group_id = int(sys.argv[2])
    except ValueError as exc:
        raise SystemExit("watchdog arguments must be integers") from exc
    if read_fd < 0 or process_group_id <= 0:
        raise SystemExit("watchdog arguments are out of range")
    _watch(read_fd, process_group_id)


if __name__ == "__main__":
    main()
