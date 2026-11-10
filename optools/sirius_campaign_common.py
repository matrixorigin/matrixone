#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Shared bounded process ownership and identities for the Sirius campaign."""

from __future__ import annotations

import ctypes
import json
import math
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import threading
import time
from typing import Any, Mapping


RUNNER_CLEANUP_TIMEOUT_SECONDS = 5.0

_PR_SET_CHILD_SUBREAPER = 36

_PR_GET_CHILD_SUBREAPER = 37

_SUBPROCESS_EXECUTOR_LOCK = threading.Lock()


class CampaignError(ValueError):
    pass


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    )


def _run_spec(
    sequence: int,
    phase: str,
    route: Mapping[str, str],
    query: int,
    streams: int,
    suite: int,
    repetition: int,
    measured: bool,
    scale_factor: int,
    dataset_sha256: str,
    query_sha256: str,
) -> dict[str, Any]:
    return {
        "sequence": sequence,
        "phase": phase,
        "route": route["id"],
        "backend": route["backend"],
        "scan_mode": route["scan_mode"],
        "query": query,
        "query_sha256": query_sha256,
        "scale_factor": scale_factor,
        "dataset_sha256": dataset_sha256,
        "streams": streams,
        "suite": suite,
        "repetition": repetition,
        "measured": measured,
        "fallback_allowed": False,
    }


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _oracle_cell(spec: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        spec["scale_factor"], spec["phase"], spec["streams"], spec["suite"],
        spec["repetition"], spec["query"],
    )


def _libc_prctl() -> Any:
    libc = ctypes.CDLL(None, use_errno=True)
    if not hasattr(libc, "prctl"):
        raise CampaignError("campaign runner requires Linux prctl")
    return libc.prctl


def _get_child_subreaper() -> bool:
    value = ctypes.c_int()
    if _libc_prctl()(_PR_GET_CHILD_SUBREAPER, ctypes.byref(value), 0, 0, 0) != 0:
        error = ctypes.get_errno()
        raise CampaignError(f"read child-subreaper state: {os.strerror(error)}")
    return bool(value.value)


def _set_child_subreaper(enabled: bool) -> None:
    if _libc_prctl()(_PR_SET_CHILD_SUBREAPER, int(enabled), 0, 0, 0) != 0:
        error = ctypes.get_errno()
        raise CampaignError(f"set child-subreaper state: {os.strerror(error)}")


def _direct_child_pids() -> set[int]:
    children: set[int] = set()
    task_root = Path(f"/proc/{os.getpid()}/task")
    try:
        tasks = tuple(task_root.iterdir())
    except OSError as error:
        raise CampaignError(f"enumerate campaign tasks: {error}") from error
    for task in tasks:
        try:
            raw = (task / "children").read_text(encoding="ascii").strip()
        except FileNotFoundError:
            continue
        except OSError as error:
            raise CampaignError(f"enumerate campaign children: {error}") from error
        if raw:
            children.update(int(value) for value in raw.split())
    return children


def _enter_exclusive_subreaper() -> bool:
    if sys.platform != "linux" or not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
        raise CampaignError("campaign runner ownership requires Linux prctl, procfs, and pidfds")
    existing = _direct_child_pids()
    if existing:
        raise CampaignError(
            f"campaign runner requires exclusive subprocess ownership; existing children={sorted(existing)}"
        )
    previous = _get_child_subreaper()
    _set_child_subreaper(True)
    return previous


def _remaining_cleanup_seconds(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise CampaignError("campaign runner cleanup deadline exceeded")
    return remaining


def _kill_original_process_group(process: subprocess.Popen[str]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _close_process_pipes(process: subprocess.Popen[str]) -> None:
    for pipe in (process.stdout, process.stderr):
        if pipe is not None:
            pipe.close()


def _kill_and_reap_adopted_children(deadline: float) -> None:
    while True:
        children = _direct_child_pids()
        if not children:
            return
        descriptors: dict[int, int] = {}
        try:
            for pid in children:
                try:
                    descriptor = os.pidfd_open(pid)
                except ProcessLookupError:
                    continue
                descriptors[pid] = descriptor
                try:
                    signal.pidfd_send_signal(descriptor, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            poller = select.poll()
            for descriptor in descriptors.values():
                poller.register(descriptor, select.POLLIN)
            pending = set(descriptors.values())
            while pending:
                timeout_ms = max(1, math.ceil(_remaining_cleanup_seconds(deadline) * 1000))
                events = poller.poll(timeout_ms)
                if not events:
                    raise CampaignError("campaign runner cleanup deadline exceeded")
                pending.difference_update(descriptor for descriptor, _ in events)
        finally:
            for descriptor in descriptors.values():
                os.close(descriptor)
        for pid in children:
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass


def _cleanup_owned_processes(process: subprocess.Popen[str], close_pipes: bool) -> None:
    deadline = time.monotonic() + RUNNER_CLEANUP_TIMEOUT_SECONDS
    _kill_original_process_group(process)
    if close_pipes:
        _close_process_pipes(process)
    if process.poll() is None:
        try:
            process.wait(timeout=_remaining_cleanup_seconds(deadline))
        except subprocess.TimeoutExpired as error:
            raise CampaignError("direct campaign runner did not terminate during cleanup") from error
    _kill_and_reap_adopted_children(deadline)
