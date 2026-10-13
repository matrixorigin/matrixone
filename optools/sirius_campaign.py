#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Own the real services, trusted evidence collector, and v2 campaign worker."""
from __future__ import annotations

import argparse
import contextlib
import ctypes
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
import itertools
import shutil
import re
import tomllib
import csv
import io
import stat

import sirius_campaign_v2 as v2

PROFILE_WORKSPACE_BYTES = 48 << 30
PROFILE_FINALIZE_SECONDS = 10800


def profile_workspace_bytes(root):
    size = files = 0
    for path in root.rglob("*"):
        try:
            info = path.stat()
        except FileNotFoundError:
            # The comparator owns and retires temporary runs concurrently.
            # A removed file retains no bytes; other stat failures stay fatal.
            continue
        if not stat.S_ISREG(info.st_mode):
            continue
        files += 1
        size += info.st_size
        if files > 65536 or size > PROFILE_WORKSPACE_BYTES:
            raise v2.CampaignError("profile/log workspace quota exceeded")
    return size


def write_private(path, data):
    descriptor = os.open(
        path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
    )
    with os.fdopen(descriptor, "w") as file:
        file.write(data)


def failure_location(error):
    """Report only a repository source line; exception bodies may hold secrets."""
    known = {
        "sirius_campaign.py",
        "sirius_campaign_v2.py",
        "sirius_parity.py",
        "sirius_resource.py",
    }
    location = None
    trace = error.__traceback__
    while trace is not None:
        name = Path(trace.tb_frame.f_code.co_filename).name
        if name in known:
            location = {"file": name, "line": trace.tb_lineno}
        trace = trace.tb_next
    return location


def sha256(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as file:
        for chunk in iter(lambda: file.read(1 << 20), b""):
            value.update(chunk)
    return value.hexdigest()


def controller_sha256():
    digest = hashlib.sha256()
    for name in (
        "sirius_campaign.py",
        "sirius_campaign_v2.py",
        "sirius_resource.py",
        "sirius_profile_exit.gdb",
    ):
        digest.update(name.encode() + b"\0")
        digest.update((Path(__file__).parent / name).read_bytes())
    return digest.hexdigest()


def pid_identity(pid):
    text = Path(f"/proc/{pid}/stat").read_text()
    fields = text[text.rfind(")") + 2 :].split()
    return int(fields[19])


def launch_target(arguments):
    """Record our launch identity before exec; profiler process trees can differ."""
    private_json(
        arguments.identity,
        {"pid": os.getpid(), "generation": pid_identity(os.getpid())},
    )
    os.execve(
        arguments.service,
        [str(arguments.service), "-cfg", str(arguments.configuration)],
        os.environ.copy(),
    )


def wait_target(service, path, timeout=120, monitor=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if monitor is not None:
            monitor()
        if path.exists():
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(descriptor, "r") as file:
                info = os.fstat(file.fileno())
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_mode & 0o777 != 0o600
                    or info.st_nlink != 1
                    or info.st_size > 4096
                ):
                    raise v2.CampaignError(
                        "profile launch identity is not private and bounded"
                    )
                try:
                    identity = json.load(file)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    # Creation precedes the small launch record's flush. Retry
                    # a partial publication within the same bounded deadline.
                    time.sleep(0.05)
                    continue
            if (
                not isinstance(identity, dict)
                or set(identity) != {"pid", "generation"}
                or type(identity["pid"]) is not int
                or type(identity["generation"]) is not int
                or identity["pid"] <= 0
                or pid_identity(identity["pid"]) != identity["generation"]
            ):
                raise v2.CampaignError("profile launch generation changed")
            service.targets[identity["pid"]] = identity["generation"]
            return identity["pid"]
        if service.process.poll() is not None:
            raise v2.CampaignError("profiler exited before target launch")
        time.sleep(0.05)
    raise v2.CampaignError("profile target launch deadline exceeded")


def process_observation(pid):
    text = Path(f"/proc/{pid}/stat").read_text()
    fields = text[text.rfind(")") + 2 :].split()
    cpu = (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    rss = int(fields[21]) * os.sysconf("SC_PAGE_SIZE")
    return cpu, rss


class Sampler:
    """Bounded PID observation; RSS and allocator credit remain distinct."""

    def __init__(self, pid, metric_url, gpu=False):
        self.pid, self.generation = pid, pid_identity(pid)
        self.metric_url = metric_url
        self.gpu = gpu
        self.gpu_peak = 0
        self.stop = threading.Event()
        self.samples = 0
        self.rss_peak = self.go_peak = 0
        self.error = None
        self.start_cpu, self.baseline = process_observation(pid)
        self.rss_peak = self.baseline
        self.thread = threading.Thread(
            target=self._run, name="campaign-pid-observation", daemon=False
        )

    def _sample(self):
        if pid_identity(self.pid) != self.generation:
            raise v2.CampaignError("service PID generation changed")
        _, rss = process_observation(self.pid)
        self.rss_peak = max(self.rss_peak, rss)
        # Existing Go allocator metrics; never infer native allocation by subtraction.
        found = False
        with urllib.request.urlopen(self.metric_url, timeout=2) as response:
            total = 0
            while True:
                raw = response.readline((64 << 10) + 1)
                if not raw:
                    break
                total += len(raw)
                if len(raw) > 64 << 10 or total > 16 << 20:
                    raise v2.CampaignError("metrics response exceeds streaming bound")
                if raw.startswith(b"go_memstats_heap_alloc_bytes "):
                    value = int(float(raw.split()[1]))
                    self.go_peak = max(self.go_peak, value)
                    found = True
                    break
        if not found:
            raise v2.CampaignError("Go allocator observation unavailable")
        self.samples += 1
        if self.gpu:
            self.gpu_peak = max(self.gpu_peak, self.nvml.used(self.pid))

    def _run(self):
        try:
            self.nvml = NVML() if self.gpu else None
            try:
                self._sample()
                while not self.stop.wait(0.1):
                    self._sample()
                self._sample()
            finally:
                if self.nvml:
                    self.nvml.close()
        except BaseException as error:
            self.error = error

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, *_):
        self.stop.set()
        self.thread.join(5)
        if self.thread.is_alive():
            self.error = v2.CampaignError("sampler did not quiesce")
            if exc_type is None:
                raise self.error

    def result(self):
        if self.error is not None:
            raise v2.CampaignError(
                "required PID/allocator observation failed"
            ) from self.error
        cpu, final = process_observation(self.pid)
        if (
            pid_identity(self.pid) != self.generation
            or self.samples == 0
            or self.gpu
            and self.gpu_peak == 0
        ):
            raise v2.CampaignError("PID observation has no valid samples")
        return {
            "host_process": {
                "source": "proc-stat",
                "scope": "cn-process",
                "pid": self.pid,
                "generation": self.generation,
                "units": "bytes",
                "interval_seconds": 0.1,
                "samples": self.samples,
                "peak_bytes": self.rss_peak,
                "baseline_bytes": self.baseline,
                "final_bytes": final,
            },
            "go_heap": {
                "source": "go-prometheus",
                "scope": "cn-process",
                "units": "bytes",
                "interval_seconds": 0.1,
                "samples": self.samples,
                "peak_bytes": self.go_peak,
            },
            "cpu": {
                "source": "proc-stat",
                "scope": "cn-process",
                "units": "seconds",
                "value": cpu - self.start_cpu,
            },
            "gpu_residency": (
                {
                    "source": "nvml-process-v1",
                    "scope": "cn-process",
                    "units": "bytes",
                    "interval_seconds": 0.1,
                    "samples": self.samples,
                    "peak_bytes": self.gpu_peak,
                }
                if self.gpu
                else {"status": "N/A"}
            ),
        }


class NVML:
    class Process(ctypes.Structure):
        _fields_ = [("pid", ctypes.c_uint), ("used_gpu_memory", ctypes.c_ulonglong)]

    def __init__(self):
        self.library = ctypes.CDLL("libnvidia-ml.so.1")
        if self.library.nvmlInit_v2() != 0:
            raise v2.CampaignError("NVML initialization failed")
        self.handle = ctypes.c_void_p()
        if (
            self.library.nvmlDeviceGetHandleByIndex_v2(0, ctypes.byref(self.handle))
            != 0
        ):
            self.close()
            raise v2.CampaignError("NVML GPU unavailable")

    def used(self, pid):
        count = ctypes.c_uint(0)
        code = self.library.nvmlDeviceGetComputeRunningProcesses(
            self.handle, ctypes.byref(count), None
        )
        if code not in (0, 7) or count.value > 1024:
            raise v2.CampaignError("NVML process enumeration failed")
        entries = (self.Process * (count.value + 8))()
        count.value = len(entries)
        if (
            self.library.nvmlDeviceGetComputeRunningProcesses(
                self.handle, ctypes.byref(count), entries
            )
            != 0
        ):
            raise v2.CampaignError("NVML process observation failed")
        matches = [
            entry.used_gpu_memory
            for entry in entries[: count.value]
            if entry.pid == pid
        ]
        # Before the first GPU query a live CN can have no CUDA context yet.
        # This is an observed zero, followed by a required post-drain sample.
        if not matches:
            return 0
        if len(matches) != 1 or matches[0] == (1 << 64) - 1:
            raise v2.CampaignError("CN GPU residency unavailable")
        return matches[0]

    def close(self):
        self.library.nvmlShutdown()


class EventCollector:
    def __init__(self, path, account_id=0):
        self.path = Path(path)
        self.account_id = account_id

    def checkpoint(self):
        return self.path.stat().st_size

    def collect(self, offset, query_id):
        identity = v2.query_uuid(query_id)
        schema, terminal = [], []
        with self.path.open("rb") as file:
            file.seek(offset)
            end = self.path.stat().st_size
            if end - offset > 64 << 20:
                raise v2.CampaignError("per-query log evidence exceeds bound")
            while file.tell() < end:
                raw = file.readline((1 << 20) + 1)
                if len(raw) > 1 << 20:
                    raise v2.CampaignError("log event exceeds bound")
                line = raw.decode("utf-8", errors="replace")
                structured = json.loads(line) if line.startswith("{") else None
                for marker, target in (
                    ("MO campaign result schema ", schema),
                    ("Sirius embedded execution ", terminal),
                ):
                    if structured is not None:
                        if structured.get("msg") != marker.rstrip():
                            continue
                        event = structured
                    else:
                        if marker not in line:
                            continue
                        event = json.loads(line.split(marker, 1)[1])
                    if v2.query_uuid(event.get("query_id")) != identity:
                        continue
                    if event.get("account_id") != self.account_id:
                        raise v2.CampaignError("query account attribution mismatch")
                    target.append(event)
        if len(schema) != 1 or len(terminal) > 1:
            raise v2.CampaignError(
                "missing or duplicate executed-schema/terminal event"
            )
        return schema[0], terminal[0] if terminal else None


class PoolCollector:
    """Existing window tags, attributed by exclusive owned-CN admission order."""

    pattern = re.compile(
        r"\[(host|gpu)_pool\] (?:HOST|GPU):(-?\d+) (QueryBegin|QueryEnd) instance=(0x[0-9a-f]+) connection=(\d+) window=(\d+) query=(\d+) allocated=(\d+) bytes peak=(\d+) bytes (?:free_blocks=(\d+)|reserved=(\d+) bytes)"
    )

    def __init__(self, root):
        self.root = Path(root)

    def checkpoint(self):
        files = list(self.root.glob("*.log"))
        if len(files) > 4:
            raise v2.CampaignError("native log file count exceeds bound")
        return {path.name: path.stat().st_size for path in files}

    def validate_windows(self, expected):
        windows = []
        files = list(self.root.glob("*.log"))
        if len(files) > 4:
            raise v2.CampaignError("native log file count exceeds bound")
        for path in files:
            with path.open("rb") as file:
                while raw := file.readline((1 << 20) + 1):
                    if len(raw) > 1 << 20:
                        raise v2.CampaignError("native log line exceeds bound")
                    match = re.search(
                        rb" query (\d+) telemetry_query=[0-9a-f]{32}", raw
                    )
                    if match:
                        if len(windows) >= 1024:
                            raise v2.CampaignError("native window ledger exceeds bound")
                        windows.append(int(match[1]))
        if windows != list(range(1, expected + 1)):
            raise v2.CampaignError("exclusive CN admission order is ambiguous")

    def collect(self, offsets, query_id, *, pending=False, window_id=None):
        windows = []
        entries = []
        files = list(self.root.glob("*.log"))
        if len(files) > 4:
            raise v2.CampaignError("native log file count exceeds bound")
        for path in files:
            with path.open("rb") as file:
                file.seek(offsets.get(path.name, 0))
                start = file.tell()
                end = path.stat().st_size
                while file.tell() < end:
                    if file.tell() - start > 64 << 20:
                        raise v2.CampaignError("native log segment exceeds bound")
                    raw = file.readline((1 << 20) + 1)
                    if len(raw) > 1 << 20:
                        raise v2.CampaignError("native log line exceeds bound")
                    line = raw.decode("utf-8", errors="replace")
                    match = re.search(
                        r" query (\d+) telemetry_query=([0-9a-f]{32})", line
                    )
                    if match and (
                        int(match[1]) == window_id
                        if window_id is not None
                        else match[2] == v2.query_uuid(query_id)
                    ):
                        windows.append(int(match[1]))
                    match = self.pattern.search(line)
                    if match:
                        if len(entries) >= 128:
                            raise v2.CampaignError(
                                "native pool event count exceeds bound"
                            )
                        entries.append(match.groups())
                        if (
                            windows
                            and int(match[6]) == windows[0]
                            and match[1] == "gpu"
                            and match[3] == "QueryEnd"
                        ):
                            break
        if len(windows) != 1:
            if pending and not windows:
                return None
            raise v2.CampaignError("native pool query attribution unavailable")
        window = windows[0]
        pools = {}
        owners = set()
        for (
            tier,
            space,
            phase,
            instance,
            connection,
            observed,
            ordinal,
            allocated,
            peak,
            free,
            reserved,
        ) in entries:
            if int(observed) != window:
                continue
            owners.add((instance, connection, ordinal))
            pool = pools.setdefault((tier, int(space)), {})
            if phase in pool:
                raise v2.CampaignError("duplicate native pool phase")
            pool[phase] = {
                "allocated_bytes": int(allocated),
                "lifetime_peak_bytes": int(peak),
                **(
                    {"free_blocks": int(free)}
                    if free is not None
                    else {"reserved_bytes": int(reserved)}
                ),
            }
        if (
            len(owners) != 1
            or {tier for tier, space in pools} != {"host", "gpu"}
            or any(
                set(phases) != {"QueryBegin", "QueryEnd"} for phases in pools.values()
            )
        ):
            if pending and len(owners) <= 1:
                return None
            raise v2.CampaignError("incomplete native logical-pool evidence")
        return {
            "source": "sirius-tagged-pools",
            "scope": "query-window",
            "window": window,
            "pools": [
                {
                    "tier": tier,
                    "space": space,
                    "begin": phases["QueryBegin"],
                    "end": phases["QueryEnd"],
                }
                for (tier, space), phases in sorted(pools.items())
            ],
        }


class PublicExecutor:
    """Evidence comes from owned service logs, never runner-supplied claims."""

    def __init__(self, command, config, service):
        if pid_identity(service["pid"]) != service["generation"]:
            raise v2.CampaignError(
                "owned CN generation changed before worker admission"
            )
        self.executor = v2.BoundedExecutor(command, config["timeout_seconds"])
        self.collector = EventCollector(service["log"], service.get("account_id", 0))
        self.service = service
        self.scratch = Path(config["scratch"])
        self.current_sequence = 0
        self.pool_offsets = {}
        self.native_ordinal = 0
        self.pools = (
            PoolCollector(service["native_log_directory"])
            if service.get("native_log_directory")
            else None
        )

    def __call__(self, spec):
        self.current_sequence = spec["sequence"]
        offset = self.collector.checkpoint()
        pool_offsets = self.pools.checkpoint() if self.pools else None
        started_ns = time.time_ns()
        monotonic_start = time.monotonic_ns()
        try:
            with Sampler(
                self.service["pid"],
                self.service["metric_url"],
                spec["backend"] == "embedded",
            ) as sampler:
                output = self.executor(spec)
            ended_ns = time.time_ns()
            if (
                abs((ended_ns - started_ns) - (time.monotonic_ns() - monotonic_start))
                > 10_000_000
            ):
                raise v2.CampaignError(
                    "resource clock correlation drift exceeded bound"
                )
            schema, terminal = self.collector.collect(offset, output["query_id"])
            output["output_schema"] = schema["output_schema"]
            output["output_schema_sha256"] = schema["output_schema_sha256"]
            if spec["backend"] == "embedded" and terminal is None:
                raise v2.CampaignError(
                    "embedded query did not produce terminal GPU evidence"
                )
            if spec["backend"] == "native" and terminal is not None:
                raise v2.CampaignError("native oracle entered embedded execution")
            output["terminal_event"] = terminal
            output["observations"] = sampler.result()
            if self.pools and spec["backend"] == "embedded":
                if len(self.pool_offsets) >= 1024:
                    raise v2.CampaignError("pool attribution ledger exceeds bound")
                self.native_ordinal += 1
                self.pool_offsets[v2.query_uuid(output["query_id"])] = (
                    pool_offsets,
                    self.native_ordinal,
                )
            if self.service.get("resource"):
                output["resource_span"] = {
                    "time_domain": "unix-epoch-ns",
                    "scope": "runner-lifetime",
                    "start_ns": started_ns,
                    "end_ns": ended_ns,
                }
            return output
        except BaseException:
            # Runner descendants have joined before control reaches this owner.
            # A successful descriptor followed by collector failure still owns a
            # spool which has not yet transferred to the comparator.
            if not self.executor.poisoned:
                name = f"run-{spec['sequence']:08d}-{spec['route']}.ndjson"
                (self.scratch / name).unlink(missing_ok=True)
            raise

    def attach_pools(self, records):
        if not self.pools:
            return
        embedded = [record for record in records if record["backend"] == "embedded"]
        if not embedded:
            return
        last = embedded[-1]["query_id"]
        deadline = time.monotonic() + 5
        offsets, window = self.pool_offsets[last]
        while self.pools.collect(offsets, last, pending=True, window_id=window) is None:
            if time.monotonic() >= deadline:
                raise v2.CampaignError("native pool flush deadline exceeded")
            time.sleep(0.05)
        for record in embedded:
            offsets, window = self.pool_offsets[record["query_id"]]
            record["native_pools"] = self.pools.collect(
                offsets, record["query_id"], window_id=window
            )
            record["native_pools"]["attribution"] = "exclusive-cn-admission-order"
            record["native_pools"]["public_query_id"] = record["query_id"]
        self.pools.validate_windows(self.native_ordinal)
        # QueryEnd precedes EmbeddedPrepared::finish's engine/plan destruction.
        # Preserve that snapshot. A following window's begin is the existing
        # observation after public drain/close and before any new native work.
        for previous, following in zip(embedded, embedded[1:]):
            before = previous["native_pools"]
            after = following["native_pools"]
            if after["window"] != before["window"] + 1:
                raise v2.CampaignError("native cleanup successor is ambiguous")
            for pool in before["pools"]:
                if pool["tier"] != "gpu" or not (
                    pool["end"]["allocated_bytes"] or pool["end"]["reserved_bytes"]
                ):
                    continue
                matches = [
                    item
                    for item in after["pools"]
                    if (item["tier"], item["space"]) == (pool["tier"], pool["space"])
                ]
                if len(matches) != 1:
                    raise v2.CampaignError("native cleanup pool successor missing")
                pool["after_close"] = {
                    "source": "next-query-window-begin",
                    "window": after["window"],
                    "query_id": following["query_id"],
                    **matches[0]["begin"],
                }
        self.pool_offsets.clear()


class Service:
    """A process group plus start generation is the controller's ownership key."""

    def __init__(self, command, cwd, log, environment):
        self.log = Path(log)
        descriptor = os.open(
            self.log, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
        )
        self.file = os.fdopen(descriptor, "wb")
        self.process = None
        try:
            self.process = subprocess.Popen(
                command,
                cwd=cwd,
                env=environment,
                stdout=self.file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            self.generation = pid_identity(self.process.pid)
            self.targets = {}
        except BaseException:
            try:
                if self.process is not None:
                    # Popen's unreaped child owns this fresh process group even
                    # if publishing its start generation failed.
                    try:
                        os.killpg(self.process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    self.process.wait(timeout=5)
            finally:
                self.file.close()
            raise

    def track_target(self, pid):
        self.targets[pid] = pid_identity(pid)

    def wait(self, timeout, monitor=None):
        if monitor is None:
            return self.process.wait(timeout=timeout)
        deadline = time.monotonic() + timeout
        while True:
            monitor()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired("owned service", timeout)
            try:
                status = self.process.wait(timeout=min(0.5, remaining))
                monitor()
                return status
            except subprocess.TimeoutExpired:
                continue

    def kill_and_wait(self):
        for pid, generation in self.targets.items():
            try:
                if pid_identity(pid) == generation:
                    os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, FileNotFoundError):
                pass
        if self.process.poll() is None:
            if pid_identity(self.process.pid) != self.generation:
                raise v2.CampaignError("service identity changed before kill")
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        self.process.wait(timeout=5)

    def close(self, monitor=None):
        try:
            if monitor is not None:
                monitor()
            for pid, generation in self.targets.items():
                try:
                    if pid_identity(pid) != generation:
                        # The owned generation has exited; a reused PID belongs
                        # to someone else and must never receive our signal.
                        continue
                    os.kill(pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                except FileNotFoundError:
                    pass
            if self.process.poll() is None:
                if pid_identity(self.process.pid) != self.generation:
                    raise v2.CampaignError("service identity changed before close")
                if not self.targets:
                    os.killpg(self.process.pid, signal.SIGTERM)
                try:
                    self.wait(
                        PROFILE_FINALIZE_SECONDS if self.targets else 30,
                        monitor,
                    )
                except subprocess.TimeoutExpired:
                    raise v2.CampaignError("owned service shutdown deadline exceeded")
            if self.targets and self.process.returncode != 0:
                raise v2.CampaignError("profile launcher exited unsuccessfully")
            for pid, generation in self.targets.items():
                try:
                    if pid_identity(pid) == generation:
                        raise v2.CampaignError("profile target did not quiesce")
                except (ProcessLookupError, FileNotFoundError):
                    pass
        except BaseException:
            # Quota, timeout and cancellation all retain this process owner
            # until it is killed and joined. Descendants join at the controller.
            self.kill_and_wait()
            raise
        finally:
            self.file.close()


def close_services(services, previous_subreaper, monitor=None):
    failures = []
    for service in reversed(services):
        try:
            service.close(monitor=monitor)
        except BaseException as error:
            failures.append(error)
    # The controller entered exclusive ownership before launching any role.
    # Its worker's closer still runs first. If that worker died, its escaped
    # runner descendants are adopted here and joined after the sibling roles.
    try:
        v2.legacy._kill_and_reap_adopted_children(
            time.monotonic() + v2.legacy.RUNNER_CLEANUP_TIMEOUT_SECONDS
        )
    except BaseException as error:
        failures.append(error)
    if failures:
        raise v2.CampaignError("owned service cleanup failed") from failures[0]
    v2.legacy._set_child_subreaper(previous_subreaper)


def private_json(path, data):
    write_private(path, json.dumps(data, sort_keys=True, indent=2) + "\n")


def write_timing_tables(output, campaign, records, summary):
    buffer = io.StringIO()
    fields = (
        "sequence",
        "phase",
        "scale_factor",
        "streams",
        "route",
        "query",
        "suite",
        "repetition",
        "measured",
        "wall_seconds",
        "first_row_seconds",
        "row_count",
    )
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for record in records:
        writer.writerow({key: record[key] for key in fields})
    write_private(output / "timings.csv", buffer.getvalue())
    if campaign["resource_campaign"]:
        return
    lines = [
        "Timing measurements (seconds)",
        "",
        "Warmups and stream controls are excluded from performance medians.",
        "",
        "| Scale | Query | Native MO median | Embedded GPU median |",
        "| --- | --- | ---: | ---: |",
    ]
    for scale in ("SF1", "SF10"):
        routes = summary["scales"][scale]
        for query in range(1, 23):
            key = f"Q{query}"
            lines.append(
                f"| {scale} | {key} | {routes['mo-native']['query_medians'][key]:.6f} | {routes['embedded-mo']['query_medians'][key]:.6f} |"
            )
        for metric, label in (
            ("sum_query_medians", "Sum of query medians"),
            ("median_full_suite", "Median full suite"),
            ("q9_stress_median", "Q9 repetition median"),
        ):
            lines.append(
                f"| {scale} | {label} | {routes['mo-native'][metric]:.6f} | {routes['embedded-mo'][metric]:.6f} |"
            )
    write_private(output / "timings.md", "\n".join(lines) + "\n")


def native_configuration(workspace):
    return f"""sirius:
  space:
    gpu:
      - device_id: 0
        memory_capacity: {6 << 30}
    host:
      - numa_id: -1
        memory_capacity: {16 << 30}
  telemetry:
    output_directory: {json.dumps(str(workspace/'native-telemetry'))}
"""


def make_configs(workspace, native_config, stream, port_base):
    """Separate roles permit CN profile restarts without a dataset reload."""
    data = workspace / "data"
    common = f"""data-dir = {json.dumps(str(data))}
[log]
level = "info"
[[fileservice]]
name = "LOCAL"
backend = "DISK-V2"
data-dir = {json.dumps(str(data/'local'))}
[[fileservice]]
name = "SHARED"
backend = "DISK-V2"
data-dir = {json.dumps(str(data/'shared'))}
[hakeeper-client]
service-addresses = ["127.0.0.1:{port_base}"]
"""
    log_id = str(uuid.uuid4())
    log = f"""service-type = "LOG"
{common}
[logservice]
uuid = "{log_id}"
logservice-address = "127.0.0.1:{port_base}"
logservice-listen-address = "127.0.0.1:{port_base}"
raft-address = "127.0.0.1:{port_base+1}"
raft-listen-address = "127.0.0.1:{port_base+1}"
gossip-address = "127.0.0.1:{port_base+2}"
gossip-listen-address = "127.0.0.1:{port_base+2}"
gossip-seed-addresses = ["127.0.0.1:{port_base+2}"]
[logservice.BootstrapConfig]
bootstrap-cluster = true
num-of-log-shards = 1
num-of-tn-shards = 1
num-of-log-shard-replicas = 1
init-hakeeper-members = ["131072:{log_id}"]
"""
    tn = f"""service-type = "TN"
{common}
[tn]
uuid = "{uuid.uuid4()}"
port-base = {port_base+100}
[observability]
disableMetric = true
disableTrace = true
"""
    cn = f"""service-type = "CN"
{common}
[cn]
uuid = "{uuid.uuid4()}"
port-base = {port_base+200}
[cn.frontend]
host = "127.0.0.1"
port = {port_base+300}
processLimitationSize = 34359738368
[cn.sirius]
enabled = true
backend = "embedded"
input-mode = "mo"
native-config-path = {json.dumps(str(native_config))}
gpu-streams = {stream}
max-waiting-queries = 16
cleanup-timeout = "30s"
request-timeout = "1h"
[observability]
disableMetric = false
enable-metric-to-prom = true
status-host = "127.0.0.1"
status-port = {port_base+301}
disableTrace = true
"""
    output = {}
    for role, contents in (("log", log), ("tn", tn), ("cn", cn)):
        path = workspace / f"{role}-{stream}.toml"
        write_private(path, contents)
        output[role] = path
    return output


def wait_ready(service, port, timeout=120, monitor=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if monitor is not None:
            monitor()
        if service.process.poll() is not None:
            raise v2.CampaignError("service exited before readiness")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            # Bounded readiness polling; this does not order a test phase.
            time.sleep(0.1)
    raise v2.CampaignError("service readiness deadline exceeded")


def run_worker(config_path):
    config = json.loads(Path(config_path).read_text())
    runner_cfg = config["runner_configuration"]
    os.environ["MO_SIRIUS_CAMPAIGN_CONFIG"] = runner_cfg
    campaign = config["campaign"]
    scratch = v2.Scratch(config["scratch"])
    executor = PublicExecutor(
        [config["runner"]], json.loads(Path(runner_cfg).read_text()), config["service"]
    )
    try:
        warmups = []
        if config.get("warmup_campaign"):
            warmups = v2.execute_campaign(config["warmup_campaign"], executor, scratch)
            scratch = v2.Scratch(config["scratch"])
        records = v2.execute_campaign(campaign, executor, scratch)
        probes = []
        if config.get("probe_campaign"):
            scratch = v2.Scratch(config["scratch"])
            probes = v2.execute_campaign(config["probe_campaign"], executor, scratch)
        executor.attach_pools(warmups + records + probes)
        private_json(
            Path(config["records"]),
            {
                "partial": True,
                "schedule_sha256": hashlib.sha256(
                    v2.canonical(campaign["schedule"])
                ).hexdigest(),
                "records": records,
                "excluded_warmups": warmups,
                "cleanup_probes": probes,
            },
        )
    except BaseException as error:
        private_json(
            Path(config["records"]),
            {
                "partial": True,
                "complete": False,
                "category": "worker-failure",
                "sequence": executor.current_sequence,
                "location": failure_location(error),
            },
        )
        raise
    finally:
        scratch.close()


def resource_phase_selection(resource, phases):
    if phases is None:
        return ()
    if (
        not resource
        or not phases
        or len(phases) > 6
        or any(type(phase) is not int or not 1 <= phase <= 6 for phase in phases)
        or len(set(phases)) != len(phases)
    ):
        raise v2.CampaignError("diagnostic phases require unique resource phases 1..6")
    return tuple(sorted(phases))


def resource_diagnostic_summary(campaign, records, selected):
    """Validate selected cells without creating a full-campaign verdict."""
    selected = resource_phase_selection(campaign["resource_campaign"], selected)
    expected = [
        spec
        for number, (_, specs) in enumerate(
            itertools.groupby(
                campaign["schedule"],
                lambda spec: (spec["scale_factor"], spec["streams"]),
            ),
            1,
        )
        for spec in specs
        if number in selected
    ]
    if len(records) != len(expected):
        raise v2.CampaignError("diagnostic requires all selected query records")
    identities = set()
    for spec, record in zip(expected, records):
        v2.validate_record(spec, record)
        identity = v2.query_uuid(record["query_id"])
        if identity in identities:
            raise v2.CampaignError("diagnostic contains duplicate query identity")
        identities.add(identity)
    return {
        "schema_version": 2,
        "diagnostic": True,
        "partial": True,
        "complete": False,
        "resource_phases": list(selected),
        "validated": len(records),
        "schedule_sha256": hashlib.sha256(v2.canonical(expected)).hexdigest(),
        "provenance": campaign["provenance"],
    }


def run_complete(arguments, *, provenance=None):
    """Only the controller can assemble the immutable complete verdict."""
    selected = resource_phase_selection(
        arguments.resource, getattr(arguments, "resource_phases", None)
    )
    workspace = arguments.workspace.resolve()
    output = arguments.output.resolve()
    if output.exists() and any(output.iterdir()):
        raise v2.CampaignError("output directory must be empty")
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    provenance = (
        json.loads(arguments.provenance.read_text())
        if provenance is None
        else provenance
    )
    if provenance.get("service_binary_sha256") != sha256(
        arguments.service
    ) or provenance.get("runner_binary_sha256") != sha256(arguments.runner):
        raise v2.CampaignError("campaign executable identity mismatch")
    if provenance.get("controller_sha256") != controller_sha256():
        raise v2.CampaignError("campaign collector/comparator identity mismatch")
    base_runner = json.loads((workspace / "runner.json").read_text())
    expected_session = [
        "set session mo_sirius_campaign_evidence=1",
        "set session div_precision_increment=4",
        "set time_zone='+00:00'",
    ]
    if base_runner.get("session") != expected_session:
        raise v2.CampaignError("live campaign session differs from frozen provenance")
    sirius = tomllib.loads((workspace / "cn-2.toml").read_text())["cn"]["sirius"]
    if any(
        sirius.get(key) != value
        for key, value in {
            "enabled": True,
            "backend": "embedded",
            "input-mode": "mo",
            "max-waiting-queries": 16,
            "cleanup-timeout": "30s",
        }.items()
    ) or Path(sirius["native-config-path"]).read_text() != native_configuration(
        workspace
    ):
        raise v2.CampaignError(
            "live campaign configuration differs from frozen profile"
        )
    campaign = v2.build_campaign(
        provenance,
        resource=arguments.resource,
        flight_profiles=[
            {"id": route, "coverage": [], "status": "N/A", "reason": "not-provisioned"}
            for route in ("flight-mo", "flight-tae")
        ],
    )
    environment = os.environ.copy()
    environment["GOMAXPROCS"] = "4"
    run_workspace = workspace / f"campaign-{uuid.uuid4().hex}"
    run_workspace.mkdir(mode=0o700)
    (run_workspace / "scratch").mkdir(mode=0o700)
    services = []
    cn = None
    records = []
    attempted = 0
    failed_sequence = None
    worker_location = None
    stage = "service-startup"
    profile_workspace_peak = 0

    def check_workspace():
        nonlocal profile_workspace_peak
        profile_workspace_peak = max(
            profile_workspace_peak, profile_workspace_bytes(run_workspace)
        )

    previous_subreaper = v2.legacy._enter_exclusive_subreaper()
    try:
        for role in ("log", "tn"):
            configs = workspace / f"{role}-2.toml"
            service = Service(
                [str(arguments.service.resolve()), "-cfg", str(configs)],
                workspace,
                run_workspace / f"{role}.log",
                environment,
            )
            services.append(service)
        # Group only contiguous configurations; preserve the original schedule.
        for phase_number, ((scale, stream), specs) in enumerate(
            itertools.groupby(
                campaign["schedule"],
                lambda spec: (spec["scale_factor"], spec["streams"]),
            ),
            1,
        ):
            if selected and phase_number not in selected:
                continue
            if cn is not None:
                cn.close(monitor=check_workspace)
                services.remove(cn)
                cn = None
            cn_template = (workspace / "cn-2.toml").read_text()
            cn_template = cn_template.replace(
                "gpu-streams = 2", f"gpu-streams = {stream}"
            )
            timeout = 10800 if arguments.resource else 3600
            if re.search(r"(?m)^request-timeout\s*=", cn_template):
                cn_template = re.sub(
                    r"(?m)^request-timeout\s*=.*$",
                    f'request-timeout = "{timeout}s"',
                    cn_template,
                )
            else:
                cn_template = cn_template.replace(
                    "[cn.sirius]", f'[cn.sirius]\nrequest-timeout = "{timeout}s"'
                )
            phase_workspace = run_workspace / f"phase-{phase_number}"
            phase_workspace.mkdir(mode=0o700)
            (phase_workspace / "nsys-temp").mkdir(mode=0o700)
            path = phase_workspace / "cn.toml"
            write_private(path, cn_template)
            log = phase_workspace / "cn.log"
            command = [str(arguments.service.resolve()), "-cfg", str(path)]
            cn_environment = environment.copy()
            cn_environment.update(
                SIRIUS_LOG_BACKEND="spdlog",
                SIRIUS_LOG_LEVEL="info",
                SIRIUS_LOG_DIR=str(phase_workspace / "native"),
                NSYS_TMPDIR=str(phase_workspace / "nsys-temp"),
            )
            if arguments.resource:
                prefix = phase_workspace / "resource"
                identity_path = phase_workspace / "target.json"
                command = [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "target",
                    "--service",
                    str(arguments.service.resolve()),
                    "--configuration",
                    str(path),
                    "--identity",
                    str(identity_path),
                ]
                command = [
                    "gdb",
                    "--batch",
                    "-x",
                    str(Path(__file__).with_name("sirius_profile_exit.gdb")),
                    "--args",
                    *command,
                ]
                command = [
                    "nsys",
                    "profile",
                    "--trace=cuda,nvtx",
                    "--sample=none",
                    "--cpuctxsw=none",
                    "--cuda-memory-usage=true",
                    "--cuda-trace-all-apis=false",
                    "--cuda-event-trace=false",
                    "--cuda-flush-interval=100",
                    "--discard-environment=true",
                    "--duration=10800",
                    "--kill=none",
                    "--wait=primary",
                    "--force-overwrite=false",
                    "--output",
                    str(prefix),
                    *command,
                ]
            cn = Service(command, workspace, log, cn_environment)
            services.append(cn)
            stage = "profile-launch" if arguments.resource else "cn-readiness"
            pid = (
                wait_target(cn, identity_path, monitor=check_workspace)
                if arguments.resource
                else cn.process.pid
            )
            stage = "cn-readiness"
            wait_ready(
                cn,
                arguments.port_base + 300,
                timeout=300 if arguments.resource else 120,
                monitor=check_workspace,
            )
            phase = list(specs)
            runner_cfg = json.loads((workspace / "runner.json").read_text())
            runner_cfg["scratch"] = str(run_workspace / "scratch")
            runner_cfg["timeout_seconds"] = 10800 if arguments.resource else 3600
            runner_path = phase_workspace / "runner.json"
            private_json(runner_path, runner_cfg)
            partial = {**campaign, "schedule": phase}
            records_path = phase_workspace / "records.json"
            worker_cfg = {
                "runner_configuration": str(runner_path),
                "runner": str(arguments.runner.resolve()),
                "scratch": runner_cfg["scratch"],
                "service": {
                    "pid": pid,
                    "generation": pid_identity(pid),
                    "log": str(log),
                    "metric_url": f"http://127.0.0.1:{arguments.port_base+301}/metrics",
                },
                "campaign": partial,
                "records": str(records_path),
            }
            worker_cfg["service"]["native_log_directory"] = str(
                phase_workspace / "native"
            )
            if arguments.resource:
                worker_cfg["service"]["resource"] = True

                def excluded(specs, phase_name, base):
                    return {
                        **campaign,
                        "schedule": [
                            {
                                **spec,
                                "sequence": base + i,
                                "phase": phase_name,
                                "measured": False,
                            }
                            for i, spec in enumerate(specs)
                        ],
                    }

                worker_cfg["warmup_campaign"] = excluded(
                    phase, "resource-warmup", 1_000_000 + phase_number * 1000
                )
                worker_cfg["probe_campaign"] = excluded(
                    phase[:2], "cleanup-probe", 2_000_000 + phase_number * 1000
                )
            worker_path = phase_workspace / "worker.json"
            private_json(worker_path, worker_cfg)
            worker_environment = environment.copy()
            stage = "public-worker"
            worker = Service(
                [
                    sys.executable,
                    str(Path(__file__).resolve()),
                    "worker",
                    str(worker_path),
                ],
                workspace,
                phase_workspace / "worker.log",
                worker_environment,
            )
            try:
                deadline = (
                    time.monotonic()
                    + (2 * len(phase) + 2) * runner_cfg["timeout_seconds"]
                    + 30
                )
                while worker.process.poll() is None:
                    # Bound all private trace/log material independently of row scratch.
                    check_workspace()
                    if any(service.process.poll() is not None for service in services):
                        raise v2.CampaignError("owned CN exited during campaign")
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise v2.CampaignError("worker phase deadline exceeded")
                    try:
                        worker.process.wait(timeout=min(0.5, remaining))
                    except subprocess.TimeoutExpired:
                        continue
                status = worker.process.returncode
                if status != 0:
                    if records_path.exists() and records_path.stat().st_size <= 65536:
                        failure = json.loads(records_path.read_text())
                        sequence = failure.get("sequence")
                        if type(sequence) is int and any(
                            spec["sequence"] == sequence
                            for spec in phase
                            + worker_cfg.get("warmup_campaign", {}).get("schedule", [])
                            + worker_cfg.get("probe_campaign", {}).get("schedule", [])
                        ):
                            failed_sequence = sequence
                        location = failure.get("location")
                        if (
                            isinstance(location, dict)
                            and set(location) == {"file", "line"}
                            and location["file"]
                            in {
                                "sirius_campaign.py",
                                "sirius_campaign_v2.py",
                                "sirius_parity.py",
                                "sirius_resource.py",
                            }
                            and type(location["line"]) is int
                            and 0 < location["line"] < 10000
                        ):
                            worker_location = location
                    raise v2.CampaignError("public campaign worker failed")
            finally:
                worker.close(monitor=check_workspace)
            envelope = json.loads(records_path.read_text())
            if (
                envelope.get("partial") is not True
                or envelope.get("schedule_sha256")
                != hashlib.sha256(v2.canonical(phase)).hexdigest()
            ):
                raise v2.CampaignError("worker phase identity mismatch")
            records.extend(envelope["records"])
            attempted += len(phase)
            if arguments.resource:
                stage = "profile-export"
                cn.close(monitor=check_workspace)
                services.remove(cn)
                cn = None
                check_workspace()
                exported = phase_workspace / "resource.sqlite"
                exporter = Service(
                    [
                        "nsys",
                        "export",
                        "--type=sqlite",
                        "--ts-normalize=true",
                        "--output",
                        str(exported),
                        str(prefix) + ".nsys-rep",
                    ],
                    workspace,
                    phase_workspace / "export.log",
                    environment,
                )
                services.append(exporter)
                try:
                    if exporter.wait(PROFILE_FINALIZE_SECONDS, check_workspace) != 0:
                        raise v2.CampaignError("owned profile export failed")
                finally:
                    exporter.close(monitor=check_workspace)
                services.remove(exporter)
                from sirius_resource import (
                    allocation_summary,
                    MAX_LEDGER_BYTES,
                    MAX_SORT_FILE_BYTES,
                )

                check_workspace()
                if profile_workspace_bytes(run_workspace) > (
                    PROFILE_WORKSPACE_BYTES - MAX_LEDGER_BYTES - 2 * MAX_SORT_FILE_BYTES
                ):
                    raise v2.CampaignError(
                        "profile workspace lacks allocation ledger capacity"
                    )

                all_runs = (
                    envelope["excluded_warmups"]
                    + envelope["records"]
                    + envelope["cleanup_probes"]
                )
                stage = "allocation-parse"
                allocation = allocation_summary(exported, pid, all_runs)
                measured = [
                    span
                    for span in allocation["runs"]
                    if span["phase"] == "resource-validation"
                ]
                final = [
                    span
                    for span in allocation["runs"]
                    if span["phase"] == "cleanup-probe"
                ]
                if len(measured) != 22 or len(final) != 1:
                    raise v2.CampaignError("resource profile phase coverage differs")
                baseline = measured[0]["before_bytes"]
                after = final[0]["before_bytes"]
                growth = sum(max(0, after[kind] - baseline[kind]) for kind in baseline)
                tagged = [
                    run["native_pools"]
                    for run in envelope["records"]
                    if run["backend"] == "embedded"
                ]
                logical_gpu_clean = all(
                    pool.get("after_close", pool["end"])["allocated_bytes"] == 0
                    and pool.get("after_close", pool["end"])["reserved_bytes"] == 0
                    for entry in tagged
                    for pool in entry["pools"]
                    if pool["tier"] == "gpu"
                )
                allocation.update(
                    scale_factor=scale,
                    streams=stream,
                    provenance=provenance,
                    warmed_baseline_bytes=baseline,
                    post_cleanup_bytes=after,
                    unexplained_growth_bytes=growth,
                    pool_baseline_reconciled=growth == 0 and logical_gpu_clean,
                    logical_pool_scope="query-window",
                    physical_baseline_scope="idle-cn-process",
                )
                private_json(output / f"resource-phase-{phase_number}.json", allocation)
                private_json(
                    output / f"excluded-phase-{phase_number}.json",
                    {
                        "warmups": envelope["excluded_warmups"],
                        "cleanup_probes": envelope["cleanup_probes"],
                    },
                )
                private_json(
                    output / f"query-records-phase-{phase_number}.json", envelope
                )
                # Worker, CN, profiler, export and ledger owners have joined.
                # Safe evidence survives; old raw phases cannot accumulate.
                shutil.rmtree(phase_workspace)
        stage = "summary"
        summary = (
            resource_diagnostic_summary(campaign, records, selected)
            if selected
            else v2.summarize(campaign, records)
        )
    except BaseException as error:
        diagnostic = {
            "schema_version": 2,
            "category": "campaign-failure",
            "stage": stage,
            "attempted": attempted,
            "validated": len(records),
            "complete": False,
            "location": worker_location or failure_location(error),
            "profile_workspace_peak_bytes": profile_workspace_peak,
        }
        if failed_sequence is not None:
            diagnostic["sequence"] = failed_sequence
        target = output / "failure.json"
        if not target.exists():
            private_json(target, diagnostic)
        raise
    finally:
        try:
            close_services(services, previous_subreaper, monitor=check_workspace)
        except BaseException as error:
            target = output / "failure.json"
            if not target.exists():
                private_json(
                    target,
                    {
                        "schema_version": 2,
                        "category": "cleanup-failure",
                        "stage": "service-shutdown",
                        "complete": False,
                        "location": failure_location(error),
                    },
                )
            raise
        shutil.rmtree(run_workspace)
    # Full evidence is publishable only after all owned processes quiesce.
    if selected:
        private_json(output / "resource-diagnostic.json", summary)
    else:
        private_json(output / "campaign.json", campaign)
        private_json(output / "summary.json", summary)
        write_timing_tables(output, campaign, records, summary)
    path = output / ("diagnostic-runs.jsonl" if selected else "runs.jsonl")
    descriptor = os.open(
        path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
    )
    with os.fdopen(descriptor, "w") as file:
        for record in records:
            file.write(v2.canonical(record).decode() + "\n")


def start_cluster(arguments):
    workspace = arguments.workspace.resolve()
    workspace.mkdir(mode=0o700, parents=True)
    native = workspace / "sirius.yaml"
    write_private(native, native_configuration(workspace))
    configs = make_configs(workspace, native, 2, arguments.port_base)
    environment = os.environ.copy()
    environment["GOMAXPROCS"] = "4"
    services = []
    previous_subreaper = v2.legacy._enter_exclusive_subreaper()
    try:
        for role in ("log", "tn", "cn"):
            service = Service(
                [str(arguments.service.resolve()), "-cfg", str(configs[role])],
                workspace,
                workspace / f"{role}.log",
                environment,
            )
            services.append(service)
        wait_ready(services[-1], arguments.port_base + 300)
        cfg = {
            "dsn": f"dump:111@tcp(127.0.0.1:{arguments.port_base+300})/",
            "query_directory": str(
                arguments.source_root.resolve() / "pkg/sql/plan/tpch"
            ),
            "scratch": str(workspace / "scratch"),
            "databases": {"1": "tpch_sf1", "10": "tpch_sf10"},
            "session": [
                "set session mo_sirius_campaign_evidence=1",
                "set session div_precision_increment=4",
                "set time_zone='+00:00'",
            ],
            "timeout_seconds": 3600,
        }
        (workspace / "scratch").mkdir(mode=0o700)
        private_json(workspace / "runner.json", cfg)
        private_json(
            workspace / "services.json",
            {
                "pid": services[-1].process.pid,
                "generation": services[-1].generation,
                "log": str(services[-1].log),
                "metric_url": f"http://127.0.0.1:{arguments.port_base+301}/metrics",
            },
        )
        while True:
            for service in services:
                if service.process.poll() is not None:
                    raise v2.CampaignError("owned service exited")
            time.sleep(0.5)
    finally:
        close_services(services, previous_subreaper)


def freeze_provenance(arguments):
    """Capture actual binaries, guarded SDK, source data and local toolchain."""
    import sirius_sdk

    root = arguments.source_root.resolve()
    packaged = json.loads((root / "lib/sirius-provenance.json").read_text())
    if packaged["binary_sha256"] != sha256(arguments.service):
        raise v2.CampaignError("service binary differs from its packaged SDK proof")
    sdk = Path(packaged["sdk"])
    sirius_sdk.validate(sdk, packaged["mode"], packaged["merged_ref"], root)
    if sha256(sdk / "link.json") != packaged["sdk_manifest_sha256"]:
        raise v2.CampaignError("packaged SDK manifest differs from the live closure")

    def command(*args):
        value = subprocess.check_output(
            args, timeout=30, stderr=subprocess.DEVNULL, text=True
        ).strip()
        if len(value) > 65536:
            raise v2.CampaignError("toolchain observation exceeds bound")
        return value

    build = command("go", "version", "-m", str(arguments.service.resolve()))
    revision = re.search(r"vcs.revision=([0-9a-f]{40})", build)
    if revision is None or revision[1] != command(
        "git", "-C", str(root), "rev-parse", "HEAD"
    ):
        raise v2.CampaignError(
            "service binary does not match the current MO source revision"
        )
    gpu = command(
        "nvidia-smi",
        "--query-gpu=name,uuid,memory.total,driver_version",
        "--format=csv,noheader,nounits",
    ).splitlines()
    if len(gpu) != 1:
        raise v2.CampaignError("campaign requires one explicitly selected GPU")
    name, gpu_uuid, memory, driver = [value.strip() for value in gpu[0].split(",")]
    cpu = next(
        line.split(":", 1)[1].strip()
        for line in Path("/proc/cpuinfo").read_text().splitlines()
        if line.startswith("model name")
    )
    datasets = {}
    for scale, loaded_path in zip((1, 10), arguments.loaded):
        manifest = json.loads(
            (arguments.datasets / f"SF{scale}" / "manifest.json").read_text()
        )
        loaded = json.loads(loaded_path.read_text())
        if (
            loaded.get("scale_factor") != scale
            or loaded.get("dataset_sha256") != manifest["dataset_sha256"]
        ):
            raise v2.CampaignError("loaded dataset differs from its source manifest")
        for table, source in manifest["tables"].items():
            actual = loaded.get("tables", {}).get(table, {})
            if (
                actual.get("row_count") != source["row_count"]
                or actual.get("source_sha256") != source["sha256"]
                or not v2.legacy._is_sha256(actual.get("loaded_data_sha256"))
            ):
                raise v2.CampaignError("loaded table verification is incomplete")
        datasets[f"SF{scale}"] = manifest["dataset_sha256"]
    prefix = Path(packaged["pixi_provider"]["prefix"])
    provenance = {
        "matrixone_revision": revision[1],
        "sirius_revision": packaged["source_revision"],
        "sdk_sha256": packaged["sdk_manifest_sha256"],
        "sdk_build_mode": packaged["mode"],
        "service_binary_sha256": sha256(arguments.service),
        "runner_binary_sha256": sha256(arguments.runner),
        "controller_sha256": controller_sha256(),
        "datasets": datasets,
        "query_sha256": {
            f"Q{q}": sha256(root / f"pkg/sql/plan/tpch/q{q}.sql") for q in range(1, 23)
        },
        "hardware": {
            "gpu_name": name,
            "gpu_uuid": gpu_uuid,
            "gpu_total_bytes": int(memory) << 20,
            "cpu_model": cpu,
            "cpu_count": os.cpu_count(),
            "host_total_bytes": os.sysconf("SC_PHYS_PAGES")
            * os.sysconf("SC_PAGE_SIZE"),
        },
        "toolchain": {
            "go": build.splitlines()[0].rsplit(": ", 1)[-1],
            "cxx": command(packaged["compiler"], "-dumpfullversion"),
            "cuda": command(str(prefix / "bin/nvcc"), "--version").splitlines()[-1],
            "driver": driver,
            "pixi_lock_sha256": packaged["pixi_provider"]["lock_sha256"],
            "nsight": command("nsys", "--version"),
            "gdb": command("gdb", "--version").splitlines()[0],
        },
        "config": {
            "gpu_memory_bytes": 6 << 30,
            "native_host_memory_bytes": 16 << 30,
            "input_window_bytes": 64 << 20,
            "result_window_bytes": 64 << 20,
            "active_queries": 1,
            "waiting_queries": 16,
            "cleanup_timeout_seconds": 30,
            "query_timeout_seconds": 3600,
            "resource_timeout_seconds": 10800,
            "session": {
                "campaign_evidence": True,
                "div_precision_increment": 4,
                "time_zone": "+00:00",
            },
        },
    }
    v2.build_campaign(provenance)
    private_json(arguments.output, provenance)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    worker = sub.add_parser("worker")
    worker.add_argument("config", type=Path)
    target = sub.add_parser("target")
    target.add_argument("--service", type=Path, required=True)
    target.add_argument("--configuration", type=Path, required=True)
    target.add_argument("--identity", type=Path, required=True)
    serve = sub.add_parser("serve")
    serve.add_argument("--workspace", type=Path, required=True)
    serve.add_argument("--service", type=Path, required=True)
    serve.add_argument("--source-root", type=Path, required=True)
    serve.add_argument("--port-base", type=int, default=36001)
    run = sub.add_parser("run")
    run.add_argument("--workspace", type=Path, required=True)
    run.add_argument("--service", type=Path, required=True)
    run.add_argument("--runner", type=Path, required=True)
    run.add_argument("--provenance", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--resource", action="store_true")
    run.add_argument(
        "--resource-phases",
        type=int,
        choices=range(1, 7),
        nargs="+",
        help="diagnostic only: selected resource phases; never publishes acceptance",
    )
    run.add_argument("--port-base", type=int, default=36001)
    freeze = sub.add_parser("freeze")
    freeze.add_argument("--source-root", type=Path, required=True)
    freeze.add_argument("--service", type=Path, required=True)
    freeze.add_argument("--runner", type=Path, required=True)
    freeze.add_argument("--datasets", type=Path, required=True)
    freeze.add_argument("--loaded", type=Path, nargs=2, required=True)
    freeze.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    def interrupted(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    try:
        if args.command == "worker":
            run_worker(args.config)
        elif args.command == "target":
            launch_target(args)
        elif args.command == "serve":
            start_cluster(args)
        elif args.command == "freeze":
            freeze_provenance(args)
        else:
            run_complete(args)
    except (KeyboardInterrupt, v2.CampaignError):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
