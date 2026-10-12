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

"""Bounded public-MO/Sirius campaign protocol v2; v1 remains independently valid."""
from __future__ import annotations

import contextlib
import copy
from decimal import Decimal, InvalidOperation
import hashlib
import heapq
import io
import json
import math
import os
from pathlib import Path
import selectors
import signal
import stat
import statistics
import struct
import subprocess
import sys
import tempfile
import time

import sirius_parity as legacy

MEMORY_BYTES = 32 << 20
CHUNK_BYTES = 8 << 20
SCRATCH_BYTES = 4 << 30
ROW_BYTES = 1 << 20
OUTPUT_BYTES = 1 << 20
STDERR_BYTES = 64 << 10
ROUTES = (
    {"id": "mo-native", "backend": "native", "scan_mode": "mo"},
    {"id": "embedded-mo", "backend": "embedded", "scan_mode": "mo"},
)
# Zero-based output positions of the unchanged canonical SQL ORDER BY keys.
# Direction is irrelevant for identifying peers; the ordered stream is retained.
ORDER_KEYS = {
    1: (0, 1),
    2: (0, 2, 1, 3),
    3: (1, 2),
    4: (0,),
    5: (1,),
    7: (0, 1, 2),
    8: (0,),
    9: (0, 1),
    10: (2,),
    11: (1,),
    12: (0,),
    13: (1, 0),
    15: (0,),
    16: (3, 0, 1, 2),
    18: (4, 3),
    20: (0,),
    21: (1, 0),
    22: (0,),
}

CampaignError = legacy.CampaignError


def canonical(value):
    return legacy.canonical_json(value).encode("utf-8")


def query_uuid(value):
    if not isinstance(value, str):
        raise CampaignError("missing query identity")
    value = value.replace("-", "").lower()
    if (
        len(value) != 32
        or value == "0" * 32
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise CampaignError("invalid query identity")
    return value


def build_campaign(provenance, *, resource=False, flight_profiles=()):
    required = (
        "matrixone_revision",
        "sirius_revision",
        "sdk_sha256",
        "query_sha256",
        "datasets",
        "hardware",
        "config",
        "toolchain",
        "service_binary_sha256",
        "runner_binary_sha256",
        "sdk_build_mode",
        "controller_sha256",
    )
    if not isinstance(provenance, dict) or set(provenance) != set(required):
        raise CampaignError("incomplete v2 provenance")
    for field in ("matrixone_revision", "sirius_revision"):
        value = provenance[field]
        if (
            not isinstance(value, str)
            or len(value) != 40
            or any(x not in "0123456789abcdef" for x in value)
        ):
            raise CampaignError("invalid v2 revision")
    if any(
        not legacy._is_sha256(provenance[key])
        for key in (
            "sdk_sha256",
            "service_binary_sha256",
            "runner_binary_sha256",
            "controller_sha256",
        )
    ):
        raise CampaignError("invalid SDK identity")
    if provenance["sdk_build_mode"] not in ("release", "development"):
        raise CampaignError("invalid SDK build mode")
    for field, allowed in (
        (
            "hardware",
            {
                "gpu_name",
                "gpu_uuid",
                "gpu_total_bytes",
                "cpu_model",
                "cpu_count",
                "host_total_bytes",
            },
        ),
        (
            "toolchain",
            {"go", "cxx", "cuda", "driver", "pixi_lock_sha256", "nsight", "gdb"},
        ),
        (
            "config",
            {
                "gpu_memory_bytes",
                "native_host_memory_bytes",
                "input_window_bytes",
                "result_window_bytes",
                "active_queries",
                "waiting_queries",
                "cleanup_timeout_seconds",
                "query_timeout_seconds",
                "resource_timeout_seconds",
                "session",
            },
        ),
    ):
        value = provenance[field]
        if not isinstance(value, dict) or set(value) != allowed:
            raise CampaignError("invalid bounded provenance fields")
        if len(canonical(value)) > 4096:
            raise CampaignError("provenance section exceeds bound")
    if not legacy._is_sha256(provenance["toolchain"]["pixi_lock_sha256"]):
        raise CampaignError("invalid frozen toolchain lock identity")
    if provenance["config"]["session"] != {
        "campaign_evidence": True,
        "div_precision_increment": 4,
        "time_zone": "+00:00",
    }:
        raise CampaignError("campaign session semantics differ")
    expected = {
        "gpu_memory_bytes": 6 << 30,
        "native_host_memory_bytes": 16 << 30,
        "input_window_bytes": 64 << 20,
        "result_window_bytes": 64 << 20,
        "active_queries": 1,
        "waiting_queries": 16,
        "cleanup_timeout_seconds": 30,
        "query_timeout_seconds": 3600,
        "resource_timeout_seconds": 10800,
    }
    if any(
        type(provenance["config"][key]) is not int or provenance["config"][key] != value
        for key, value in expected.items()
    ):
        raise CampaignError("campaign limits differ from the approved profile")
    for key, value in provenance["hardware"].items():
        if key in ("gpu_total_bytes", "cpu_count", "host_total_bytes"):
            if type(value) is not int or not 0 < value < 1 << 60:
                raise CampaignError("invalid hardware capacity")
        elif (
            not isinstance(value, str)
            or not 0 < len(value) <= 256
            or any(ord(c) < 32 for c in value)
        ):
            raise CampaignError("invalid hardware identity")
    if any(
        not isinstance(value, str)
        or not 0 < len(value) <= 512
        or any(ord(c) < 32 for c in value)
        for value in provenance["toolchain"].values()
    ):
        raise CampaignError("invalid toolchain identity")
    hashes = provenance["query_sha256"]
    if set(hashes) != {f"Q{x}" for x in range(1, 23)} or not all(
        legacy._is_sha256(x) for x in hashes.values()
    ):
        raise CampaignError("invalid canonical query inventory")
    datasets = provenance["datasets"]
    if set(datasets) != {"SF1", "SF10"} or not all(
        legacy._is_sha256(x) for x in datasets.values()
    ):
        raise CampaignError("invalid immutable dataset identity")
    profiles = copy.deepcopy(list(flight_profiles))
    if len(profiles) > 2:
        raise CampaignError("Flight profile count exceeds bound")
    identities = set()
    for profile in profiles:
        if (
            not isinstance(profile, dict)
            or not set(profile) <= {"id", "coverage", "status", "reason", "provenance"}
            or profile.get("id") not in ("flight-mo", "flight-tae")
            or profile["id"] in identities
            or not isinstance(profile.get("coverage"), list)
            or any(
                type(q) is not int or q not in range(1, 23) for q in profile["coverage"]
            )
        ):
            raise CampaignError("invalid declared Flight coverage")
        identities.add(profile["id"])
        if len(set(profile["coverage"])) != len(profile["coverage"]):
            raise CampaignError("duplicate Flight coverage")
        if not profile["coverage"]:
            if (
                profile.get("status", "N/A") != "N/A"
                or profile.get("reason", "not-provisioned")
                not in {"not-provisioned", "unavailable", "unsupported"}
                or "provenance" in profile
            ):
                raise CampaignError("invalid preflight Flight availability")
            continue
        proof = profile.get("provenance")
        required = {
            "matrixone_revision",
            "sirius_revision",
            "launcher_sha256",
            "configuration_sha256",
            "server_binary_sha256",
            "tn_no_gc_verified",
        }
        if not isinstance(proof, dict) or set(proof) != required:
            raise CampaignError("missing declared Flight provenance")
        if (
            any(
                not isinstance(proof[key], str)
                or len(proof[key]) != 40
                or any(char not in "0123456789abcdef" for char in proof[key])
                for key in ("matrixone_revision", "sirius_revision")
            )
            or any(
                not legacy._is_sha256(proof[key])
                for key in (
                    "launcher_sha256",
                    "configuration_sha256",
                    "server_binary_sha256",
                )
            )
            or type(proof["tn_no_gc_verified"]) is not bool
            or profile["id"] == "flight-tae"
            and not proof["tn_no_gc_verified"]
        ):
            raise CampaignError("invalid declared Flight provenance")
    schedule = []

    def append(scale, phase, stream, suite, repetition, query, route):
        spec = legacy._run_spec(
            len(schedule) + 1,
            phase,
            route,
            query,
            stream,
            suite,
            repetition,
            phase == "performance" and suite > 0,
            scale,
            datasets[f"SF{scale}"],
            hashes[f"Q{query}"],
        )
        spec["schema_version"] = 2
        spec["order_keys"] = list(ORDER_KEYS.get(query, ()))
        schedule.append(spec)

    def pair(scale, phase, stream, suite, repetition, query):
        offset = (scale + query + stream + suite + repetition) % 2
        for route in ROUTES[offset:] + ROUTES[:offset]:
            append(scale, phase, stream, suite, repetition, query, route)
        for profile in profiles:
            if query in profile["coverage"]:
                append(
                    scale,
                    phase,
                    stream,
                    suite,
                    repetition,
                    query,
                    {
                        "id": profile["id"],
                        "backend": "flight",
                        "scan_mode": profile["id"].split("-")[1],
                    },
                )

    for scale in (1, 10):
        if resource:
            for stream in (1, 4, 2):
                for query in range(1, 23):
                    pair(scale, "resource-validation", stream, 0, 1, query)
        else:
            for stream in (1, 4):
                for query in range(1, 23):
                    pair(scale, "stream-validation", stream, 0, 1, query)
            for suite in range(6):
                for query in range(1, 23):
                    pair(scale, "performance", 2, suite, 1, query)
            for repetition in range(1, 11):
                pair(scale, "q9-concurrency", 2, 0, repetition, 9)
    return {
        "schema_version": 2,
        "resource_campaign": resource,
        "provenance": copy.deepcopy(provenance),
        "routes": list(ROUTES),
        "flight_profiles": profiles,
        "schedule": schedule,
        "gpu_concurrency": 1,
        "fallback_allowed": False,
        "memory_bytes": MEMORY_BYTES,
        "scratch_bytes": SCRATCH_BYTES,
    }


class BoundedExecutor(legacy.SubprocessExecutor):
    """Reuse v1's exclusive ownership; bound both pipes before JSON decoding."""

    def _call_exclusive(self, spec):
        if self.poisoned:
            raise CampaignError("runner cleanup owner is poisoned")
        previous = legacy._enter_exclusive_subreaper()
        process = None
        try:
            environment = os.environ.copy()
            environment["MO_SIRIUS_CAMPAIGN_RUN"] = canonical(spec).decode()
            process = subprocess.Popen(
                self.command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=environment,
                start_new_session=True,
            )
            buffers = {"stdout": bytearray(), "stderr": bytearray()}
            deadline = time.monotonic() + self.timeout_seconds
            with selectors.DefaultSelector() as selector:
                for name in buffers:
                    pipe = getattr(process, name)
                    os.set_blocking(pipe.fileno(), False)
                    selector.register(pipe, selectors.EVENT_READ, name)
                while selector.get_map():
                    if time.monotonic() >= deadline:
                        raise CampaignError("runner deadline exceeded")
                    for key, _ in selector.select(
                        min(0.1, max(0, deadline - time.monotonic()))
                    ):
                        chunk = os.read(key.fileobj.fileno(), 64 << 10)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        buffer = buffers[key.data]
                        limit = OUTPUT_BYTES if key.data == "stdout" else STDERR_BYTES
                        if len(chunk) > limit - len(buffer):
                            raise CampaignError("runner pipe bound exceeded")
                        buffer.extend(chunk)
                    if process.poll() is not None:
                        deadline = min(
                            deadline,
                            time.monotonic() + legacy.RUNNER_CLEANUP_TIMEOUT_SECONDS,
                        )
                process.wait(timeout=max(0.001, deadline - time.monotonic()))
            status = process.returncode
            legacy._cleanup_owned_processes(process, close_pipes=True)
            process = None
            if status:
                raise CampaignError("runner exited unsuccessfully")
            output = json.loads(buffers["stdout"])
            if not isinstance(output, dict):
                raise CampaignError("runner descriptor is not an object")
            return output
        finally:
            if process is not None:
                try:
                    legacy._cleanup_owned_processes(process, close_pipes=True)
                except BaseException:
                    self.poisoned = True
                    raise
            if legacy._direct_child_pids():
                self.poisoned = True
                raise CampaignError("runner descendants did not quiesce")
            legacy._set_child_subreaper(previous)


class Scratch:
    """Exactly one owner for admitted spools and comparator runs."""

    def __init__(self, root, limit=SCRATCH_BYTES):
        self.root = Path(root)
        info = self.root.lstat()
        if not stat.S_ISDIR(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o700:
            raise CampaignError("scratch root is not private")
        self.limit = limit
        self.owned = {}
        self.used = 0

    def admit(self, result):
        name = result.get("spool")
        if (
            not isinstance(name, str)
            or Path(name).name != name
            or name in ("", ".", "..")
            or "\0" in name
            or name in self.owned
        ):
            raise CampaignError("invalid or duplicated spool ownership")
        size = result.get("bytes")
        if type(size) is not int or size < 0 or size > self.limit - self.used:
            raise CampaignError("scratch quota exceeded")
        descriptor = os.open(
            self.root / name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC
        )
        try:
            info = os.fstat(descriptor)
            if (
                not stat.S_ISREG(info.st_mode)
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_nlink != 1
                or info.st_size != size
            ):
                raise CampaignError("spool is not a private complete regular file")
            if (
                not legacy._is_sha256(result.get("sha256"))
                or type(result.get("row_count")) is not int
                or result["row_count"] < 0
            ):
                raise CampaignError("invalid spool digest or count")
            self.owned[name] = size
            self.used += size
            return os.fdopen(descriptor, "rb", buffering=64 << 10)
        except BaseException:
            os.close(descriptor)
            raise

    def create(self):
        descriptor, name = tempfile.mkstemp(
            prefix="compare-", suffix=".ndjson", dir=self.root
        )
        name = Path(name).name
        self.owned[name] = 0
        return name, os.fdopen(descriptor, "wb", buffering=64 << 10)

    def write(self, name, file, data):
        if len(data) > self.limit - self.used:
            raise CampaignError("comparator scratch quota exceeded")
        file.write(data)
        self.owned[name] += len(data)
        self.used += len(data)

    def release(self, name):
        size = self.owned.pop(name, None)
        if size is not None:
            try:
                (self.root / name).unlink()
            finally:
                self.used -= size

    def close(self):
        for name in tuple(self.owned):
            self.release(name)


def validate_schema(output):
    result = output.get("result", {})
    public = result.get("public_schema")
    bound = output.get("output_schema")
    if (
        not isinstance(public, list)
        or not 0 < len(public) <= 1024
        or not isinstance(bound, list)
        or len(public) != len(bound)
    ):
        raise CampaignError("incomplete public/bound schema")
    names = 0
    digest = hashlib.sha256(b"sirius-mo-output-schema-v1\0")
    for column, descriptor in zip(public, bound):
        if not isinstance(column, dict) or not isinstance(descriptor, dict):
            raise CampaignError("invalid schema descriptor")
        name = column.get("name")
        if (
            not isinstance(name, str)
            or not isinstance(column.get("sql_type"), str)
            or column.get("nullable_known") is not True
        ):
            raise CampaignError("public schema field unavailable")
        raw = name.encode("utf-8")
        names += len(raw)
        if names > 1 << 20:
            raise CampaignError("schema names exceed bound")
        if (
            set(descriptor) != {"oid", "precision", "scale", "nullable"}
            or any(
                type(descriptor[k]) is not int for k in ("oid", "precision", "scale")
            )
            or type(descriptor["nullable"]) is not bool
        ):
            raise CampaignError("invalid physical descriptor")
        if descriptor["nullable"] != column.get("nullable"):
            raise CampaignError("public/bound nullability drift")
        digest.update(
            struct.pack(
                "<IiiBI",
                descriptor["oid"],
                descriptor["precision"],
                descriptor["scale"],
                descriptor["nullable"],
                len(raw),
            )
        )
        digest.update(raw)
    if digest.hexdigest() != output.get("output_schema_sha256"):
        raise CampaignError("public/bound schema digest drift")
    return public


def normal_row(encoded, schema):
    try:
        row = json.loads(encoded)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise CampaignError("malformed typed row") from error
    if not isinstance(row, list) or len(row) != len(schema):
        raise CampaignError("typed row column count mismatch")
    result = []
    for entry, column in zip(row, schema):
        if (
            not isinstance(entry, dict)
            or not set(entry) <= {"type", "value", "scale", "null"}
            or not isinstance(entry.get("type"), str)
        ):
            raise CampaignError("invalid typed cell")
        if entry.get("null", False):
            if (
                entry.get("null") is not True
                or not column["nullable"]
                or entry.get("value", "")
            ):
                raise CampaignError("invalid NULL cell")
            result.append({"type": "null"})
            continue
        kind = entry["type"]
        value = entry.get("value", "")
        if not isinstance(value, str):
            raise CampaignError("untyped public value")
        sql_type = column["sql_type"].upper()
        if kind in ("int", "uint"):
            if "INT" not in sql_type and sql_type != "YEAR":
                raise CampaignError("integer cell/schema drift")
            if (kind == "uint") != ("UNSIGNED" in sql_type):
                raise CampaignError("integer signedness drift")
            try:
                number = int(value)
            except ValueError as error:
                raise CampaignError("invalid integer") from error
            lower, upper = (
                (0, (1 << 64) - 1) if kind == "uint" else (-(1 << 63), (1 << 63) - 1)
            )
            if not lower <= number <= upper or str(number) != value:
                raise CampaignError("noncanonical or overflowing integer")
        elif kind == "decimal":
            if (
                "DECIMAL" not in sql_type
                or column.get("decimal_known") is not True
                or type(entry.get("scale", 0)) is not int
                or entry.get("scale", 0) != column["scale"]
            ):
                raise CampaignError("decimal descriptor drift")
            try:
                number = int(value)
            except ValueError as error:
                raise CampaignError("invalid coefficient") from error
            if str(number) != value or len(str(abs(number))) > column["precision"]:
                raise CampaignError("noncanonical or overflowing coefficient")
        elif kind == "float":
            # Canonical decimal TPC-H has no approved approximate columns.
            raise CampaignError("approximate column has no approved v2 tolerance")
        elif kind == "binary":
            import base64

            try:
                base64.b64decode(value, validate=True)
            except ValueError as error:
                raise CampaignError("invalid binary cell") from error
        elif kind != sql_type.lower():
            raise CampaignError("string/date cell/schema drift")
        result.append(
            {
                "type": kind,
                "value": value,
                **({"scale": entry.get("scale", 0)} if kind == "decimal" else {}),
            }
        )
    return result


def rows(file, descriptor, schema):
    hasher = hashlib.sha256()
    count = total = 0
    while True:
        raw = file.readline(ROW_BYTES + 2)
        if not raw:
            break
        if len(raw) > ROW_BYTES + 1 or not raw.endswith(b"\n"):
            raise CampaignError("oversized or truncated spool row")
        hasher.update(raw)
        total += len(raw)
        count += 1
        if count > descriptor["row_count"] or total > descriptor["bytes"]:
            raise CampaignError("spool has trailing rows or bytes")
        yield normal_row(raw, schema)
    if (
        count != descriptor["row_count"]
        or total != descriptor["bytes"]
        or hasher.hexdigest() != descriptor["sha256"]
    ):
        raise CampaignError("spool count/byte/digest mismatch")


class ExternalBag:
    """8 MiB accounted runs and fan-in eight leave room below 32 MiB."""

    def __init__(self, scratch):
        self.scratch = scratch
        self.chunk = []
        self.size = 0
        self.runs = []

    def add(self, row):
        value = canonical(row) + b"\n"
        amount = sys.getsizeof(value) + 16
        if self.chunk and self.size + amount > CHUNK_BYTES:
            self.flush()
        if amount > ROW_BYTES + 256:
            raise CampaignError("normalized row exceeds bound")
        self.chunk.append(value)
        self.size += amount

    def flush(self):
        if not self.chunk:
            return
        self.chunk.sort()
        name, file = self.scratch.create()
        try:
            with file:
                for value in self.chunk:
                    self.scratch.write(name, file, value)
            self.runs.append(name)
        except BaseException:
            self.scratch.release(name)
            raise
        self.chunk = []
        self.size = 0

    def merged(self):
        self.flush()
        while len(self.runs) > 8:
            group = self.runs[:8]
            self.runs = self.runs[8:]
            name, destination = self.scratch.create()
            try:
                with contextlib.ExitStack() as stack:
                    inputs = [
                        stack.enter_context(
                            (self.scratch.root / item).open("rb", buffering=64 << 10)
                        )
                        for item in group
                    ]
                    with destination:
                        for value in heapq.merge(*inputs):
                            self.scratch.write(name, destination, value)
                for item in group:
                    self.scratch.release(item)
                self.runs.append(name)
            except BaseException:
                destination.close()
                self.scratch.release(name)
                raise
        with contextlib.ExitStack() as stack:
            inputs = [
                stack.enter_context(
                    (self.scratch.root / item).open("rb", buffering=64 << 10)
                )
                for item in self.runs
            ]
            yield from heapq.merge(*inputs)

    def close(self):
        self.chunk = []
        for name in self.runs:
            self.scratch.release(name)
        self.runs = []


def compare_bags(left, right, scratch):
    first, second = ExternalBag(scratch), ExternalBag(scratch)
    try:
        for row in left:
            first.add(row)
        for row in right:
            second.add(row)
        a, b = first.merged(), second.merged()
        try:
            while True:
                x, y = next(a, None), next(b, None)
                if x != y:
                    raise CampaignError("result value or duplicate-count mismatch")
                if x is None:
                    break
        finally:
            a.close()
            b.close()
    finally:
        first.close()
        second.close()


def compare_results(expected, actual, keys, scratch):
    schema = validate_schema(expected)
    other = validate_schema(actual)
    if (
        schema != other
        or expected["output_schema"] != actual["output_schema"]
        or expected["output_schema_sha256"] != actual["output_schema_sha256"]
    ):
        raise CampaignError("result schema mismatch")
    if expected["result"]["row_count"] != actual["result"]["row_count"]:
        raise CampaignError("result cardinality mismatch")
    if any(type(key) is not int or not 0 <= key < len(schema) for key in keys):
        raise CampaignError("invalid canonical ordering key")
    names = []
    try:
        with contextlib.ExitStack() as stack:
            a = stack.enter_context(scratch.admit(expected["result"]))
            names.append(expected["result"]["spool"])
            b = stack.enter_context(scratch.admit(actual["result"]))
            names.append(actual["result"]["spool"])
            first = rows(a, expected["result"], schema)
            second = rows(b, actual["result"], schema)
            if not keys:
                compare_bags(first, second, scratch)
            else:
                import itertools

                peer = lambda row: canonical([row[index] for index in keys])
                groups_a, groups_b = itertools.groupby(first, peer), itertools.groupby(
                    second, peer
                )
                while True:
                    group_a, group_b = next(groups_a, None), next(groups_b, None)
                    if group_a is None or group_b is None:
                        if group_a != group_b:
                            raise CampaignError("ordered result length mismatch")
                        break
                    if group_a[0] != group_b[0]:
                        raise CampaignError("ordered peer or LIMIT membership mismatch")
                    compare_bags(group_a[1], group_b[1], scratch)
    finally:
        for name in names:
            scratch.release(name)


def validate_run(spec, output):
    if (
        output.get("schema_version") != 2
        or output.get("sequence") != spec["sequence"]
        or output.get("route") != spec["route"]
    ):
        raise CampaignError("runner run identity mismatch")
    query_uuid(output.get("query_id"))
    wall, first = output.get("wall_seconds"), output.get("first_row_seconds")
    if not isinstance(wall, (int, float)) or not math.isfinite(wall) or wall <= 0:
        raise CampaignError("invalid dispatch/drain time")
    count = output.get("result", {}).get("row_count")
    if (
        (count == 0) != (first is None)
        or first is not None
        and (
            not isinstance(first, (int, float))
            or not math.isfinite(first)
            or not 0 <= first <= wall
        )
    ):
        raise CampaignError("invalid actual first-row evidence")
    validate_schema(output)
    if spec["backend"] == "embedded":
        event = output.get("terminal_event", {})
        if query_uuid(event.get("query_id")) != query_uuid(output["query_id"]):
            raise CampaignError("terminal query identity mismatch")
        if (
            event.get("backend") != "embedded"
            or event.get("scan_mode") != "mo"
            or event.get("fallback") is not False
            or event.get("native_capabilities") != 31
            or event.get("numeric_profile") != "mo-exact-decimal-v1"
            or event.get("gpu_streams") != spec["streams"]
            or event.get("terminal_health") != "healthy"
        ):
            raise CampaignError("embedded execution evidence mismatch")
        stats = event.get("execution_stats", {})
        if (
            stats.get("terminal") is not True
            or stats.get("fatal") is not False
            or type(stats.get("gpu_tasks_completed")) is not int
            or stats["gpu_tasks_completed"] <= 0
            or stats.get("mo_input_retained_charged_bytes") != 0
            or stats.get("result_retained_charged_bytes") != 0
        ):
            raise CampaignError("missing GPU work or unhealthy retained ownership")
        if (
            event.get("verified_output_schema") != output["output_schema"]
            or event.get("output_schema_sha256") != output["output_schema_sha256"]
        ):
            raise CampaignError("frontend/prepared schema drift")


def artifact_run(spec, output):
    # Explicit whitelist: never retain headings, values, paths or unknown fields.
    observations = {}
    for name in ("host_process", "go_heap", "cpu", "gpu_residency"):
        observation = output.get("observations", {}).get(name)
        if observation is None:
            continue
        allowed = {
            "source",
            "scope",
            "pid",
            "generation",
            "units",
            "interval_seconds",
            "samples",
            "peak_bytes",
            "baseline_bytes",
            "final_bytes",
            "value",
            "status",
        }
        if not isinstance(observation, dict) or not set(observation) <= allowed:
            raise CampaignError("invalid observation shape")
        if (
            observation.get("source")
            not in (None, "proc-stat", "go-prometheus", "nvml-process-v1")
            or observation.get("scope") not in (None, "cn-process")
            or observation.get("units") not in (None, "bytes", "seconds")
        ):
            raise CampaignError("invalid observation identity")
        for key in (
            "pid",
            "generation",
            "samples",
            "peak_bytes",
            "baseline_bytes",
            "final_bytes",
        ):
            if key in observation and (
                type(observation[key]) is not int or observation[key] < 0
            ):
                raise CampaignError("invalid observation counter")
        for key in ("value", "interval_seconds"):
            if key in observation and (
                type(observation[key]) not in (int, float)
                or not math.isfinite(observation[key])
                or observation[key] < 0
            ):
                raise CampaignError("invalid observation duration")
        if observation.get("status") not in (None, "N/A"):
            raise CampaignError("invalid observation status")
        observations[name] = observation
    terminal = output.get("terminal_event")
    execution = (
        {
            key: terminal[key]
            for key in (
                "backend",
                "scan_mode",
                "fallback",
                "native_capabilities",
                "numeric_profile",
                "gpu_streams",
                "terminal_health",
            )
        }
        if terminal
        else None
    )
    return {
        **spec,
        "query_id": query_uuid(output["query_id"]),
        "wall_seconds": output["wall_seconds"],
        "first_row_seconds": output["first_row_seconds"],
        "row_count": output["result"]["row_count"],
        "result_sha256": output["result"]["sha256"],
        "output_schema": output["output_schema"],
        "output_schema_sha256": output["output_schema_sha256"],
        "observations": observations,
        **(
            {"resource_span": output["resource_span"]}
            if "resource_span" in output
            else {}
        ),
        **(
            {"native_pools": output["native_pools"]} if "native_pools" in output else {}
        ),
        "execution": execution,
        "execution_stats": (output.get("terminal_event") or {}).get("execution_stats"),
    }


def validate_record(spec, record):
    if not isinstance(record, dict) or any(
        record.get(key) != value for key, value in spec.items()
    ):
        raise CampaignError("record schedule identity mismatch")
    query_uuid(record.get("query_id"))
    count, wall, first = (
        record.get("row_count"),
        record.get("wall_seconds"),
        record.get("first_row_seconds"),
    )
    if (
        type(count) is not int
        or count < 0
        or type(wall) not in (int, float)
        or not math.isfinite(wall)
        or wall <= 0
    ):
        raise CampaignError("invalid retained result count or timing")
    if (
        (count == 0) != (first is None)
        or first is not None
        and (
            type(first) not in (int, float)
            or not math.isfinite(first)
            or not 0 <= first <= wall
        )
    ):
        raise CampaignError("invalid retained first-row timing")
    if not legacy._is_sha256(record.get("result_sha256")) or not legacy._is_sha256(
        record.get("output_schema_sha256")
    ):
        raise CampaignError("invalid retained result/schema identity")
    schema = record.get("output_schema")
    if not isinstance(schema, list) or not 0 < len(schema) <= 1024:
        raise CampaignError("invalid retained physical schema")
    for descriptor in schema:
        if (
            not isinstance(descriptor, dict)
            or set(descriptor) != {"oid", "precision", "scale", "nullable"}
            or any(
                type(descriptor[k]) is not int for k in ("oid", "precision", "scale")
            )
            or type(descriptor["nullable"]) is not bool
        ):
            raise CampaignError("invalid retained physical descriptor")
    if spec["backend"] == "embedded":
        expected = {
            "backend": "embedded",
            "scan_mode": "mo",
            "fallback": False,
            "native_capabilities": 31,
            "numeric_profile": "mo-exact-decimal-v1",
            "gpu_streams": spec["streams"],
            "terminal_health": "healthy",
        }
        stats = record.get("execution_stats")
        if (
            record.get("execution") != expected
            or not isinstance(stats, dict)
            or stats.get("terminal") is not True
            or stats.get("fatal") is not False
            or type(stats.get("gpu_tasks_completed")) is not int
            or stats["gpu_tasks_completed"] <= 0
            or stats.get("mo_input_retained_charged_bytes") != 0
            or stats.get("result_retained_charged_bytes") != 0
            or stats.get("result_rows") != count
        ):
            raise CampaignError("retained terminal GPU proof is incomplete")
    elif spec["backend"] == "native" and (
        record.get("execution") is not None or record.get("execution_stats") is not None
    ):
        raise CampaignError("native retained evidence entered GPU execution")


def execute_campaign(campaign, executor, scratch):
    records, pending = [], {}
    identities = set()
    produced = set()
    try:
        for spec in campaign["schedule"]:
            request = copy.deepcopy(spec)
            request["spool_budget_bytes"] = min(
                SCRATCH_BYTES // 2, scratch.limit - scratch.used
            )
            output = executor(request)
            name = (
                output.get("result", {}).get("spool")
                if isinstance(output, dict)
                else None
            )
            if (
                isinstance(name, str)
                and Path(name).name == name
                and name not in ("", ".", "..")
            ):
                produced.add(name)
            validate_run(spec, output)
            identity = query_uuid(output["query_id"])
            if identity in identities:
                raise CampaignError("duplicate query identity")
            identities.add(identity)
            key = legacy._oracle_cell(spec)
            cell = pending.setdefault(key, {})
            if spec["route"] in cell:
                raise CampaignError("duplicate oracle route")
            cell[spec["route"]] = output
            required_routes = [route["id"] for route in ROUTES]
            optional = [
                profile["id"]
                for profile in campaign["flight_profiles"]
                if spec["query"] in profile["coverage"]
            ]
            if all(route in cell for route in required_routes + optional):
                # The native oracle remains open only through its finite peer set.
                if optional:
                    # Compare each optional result with the independently executed
                    # oracle before consuming the mandatory pair. Spool contents
                    # are fully compared, never accepted solely by a digest.
                    for route in optional:
                        oracle = copy.deepcopy(cell["mo-native"])
                        source = scratch.root / oracle["result"]["spool"]
                        name, file = scratch.create()
                        try:
                            with source.open("rb") as reader, file:
                                for chunk in iter(lambda: reader.read(64 << 10), b""):
                                    scratch.write(name, file, chunk)
                            # The duplicate is already owned as comparator scratch.
                            size = scratch.owned.pop(name)
                            scratch.used -= size
                            oracle["result"]["spool"] = name
                            compare_results(
                                oracle, cell[route], spec["order_keys"], scratch
                            )
                        finally:
                            scratch.release(name)
                            (scratch.root / name).unlink(missing_ok=True)
                compare_results(
                    cell["mo-native"], cell["embedded-mo"], spec["order_keys"], scratch
                )
                del pending[key]
            records.append(artifact_run(spec, output))
        if pending or len(records) != len(campaign["schedule"]):
            raise CampaignError("incomplete required campaign")
        return records
    finally:
        scratch.close()
        for name in produced:
            (scratch.root / name).unlink(missing_ok=True)
        # Unadmitted outputs are still invocation-owned; callers create a fresh root.
        for cell in pending.values():
            for output in cell.values():
                name = output.get("result", {}).get("spool")
                if (
                    isinstance(name, str)
                    and Path(name).name == name
                    and name not in ("", ".", "..")
                ):
                    path = scratch.root / name
                    if path.is_file() and not path.is_symlink():
                        path.unlink()


def summarize(campaign, records):
    trusted = build_campaign(
        campaign["provenance"],
        resource=campaign["resource_campaign"],
        flight_profiles=campaign["flight_profiles"],
    )
    if campaign != trusted:
        raise CampaignError("summary rejects a partial or modified acceptance schedule")
    if len(records) != len(campaign["schedule"]):
        raise CampaignError("summary requires complete ordered evidence")
    identities, pending = set(), {}
    for spec, record in zip(campaign["schedule"], records):
        validate_record(spec, record)
        identity = query_uuid(record["query_id"])
        if identity in identities:
            raise CampaignError("summary contains duplicate query identity")
        identities.add(identity)
        key = legacy._oracle_cell(spec)
        cell = pending.setdefault(key, {})
        if record["route"] in cell:
            raise CampaignError("summary contains duplicate oracle route")
        cell[record["route"]] = record
        required = {route["id"] for route in ROUTES} | {
            profile["id"]
            for profile in campaign["flight_profiles"]
            if spec["query"] in profile["coverage"]
        }
        if set(cell) == required:
            oracle = cell["mo-native"]
            for peer in cell.values():
                if peer["row_count"] != oracle["row_count"]:
                    raise CampaignError("retained oracle result cardinality mismatch")
                if any(
                    peer[field] != oracle[field]
                    for field in ("output_schema", "output_schema_sha256")
                ):
                    raise CampaignError("retained oracle result schema mismatch")
            # Raw spool hashes may differ for unordered rows and ordered peers.
            # The live full-row comparator supplies the value oracle; archive
            # checksums bind its retained observations after those spools retire.
            del pending[key]
    if pending:
        raise CampaignError("summary requires complete retained oracle pairs")
    scales = {}
    for scale in (1, 10):
        routes = {}
        for route in ROUTES:
            values = [
                row
                for row in records
                if row["scale_factor"] == scale and row["route"] == route["id"]
            ]
            if campaign["resource_campaign"]:
                routes[route["id"]] = {"resource_cells": len(values)}
                continue
            medians = {
                f"Q{q}": statistics.median(
                    row["wall_seconds"]
                    for row in values
                    if row["phase"] == "performance"
                    and row["measured"]
                    and row["query"] == q
                )
                for q in range(1, 23)
            }
            suite_times = [
                sum(
                    row["wall_seconds"]
                    for row in values
                    if row["phase"] == "performance" and row["suite"] == suite
                )
                for suite in range(1, 6)
            ]
            q9 = [
                row["wall_seconds"]
                for row in values
                if row["phase"] == "q9-concurrency"
            ]
            routes[route["id"]] = {
                "query_medians": medians,
                "sum_query_medians": sum(medians.values()),
                "median_full_suite": statistics.median(suite_times),
                "suite_seconds": suite_times,
                "q9_stress_median": statistics.median(q9),
                "q9_seconds": q9,
            }
        scales[f"SF{scale}"] = routes
    return {
        "schema_version": 2,
        "scales": scales,
        "required_runs": len(records),
        "acceptance_complete": False,
        "remaining_gates": [
            "resource-allocation",
            "lifecycle",
            "sanitizer",
            "numeric-baselines",
            "required-CI",
        ],
        "flight_comparisons": [
            {
                "id": profile["id"],
                "coverage": profile["coverage"],
                "status": "N/A" if not profile["coverage"] else "required",
            }
            for profile in campaign["flight_profiles"]
        ],
    }


def main_from_legacy(arguments, input_spec, runner):
    """V2 always owns real services and enriches from their observed events."""
    from types import SimpleNamespace
    from sirius_campaign import run_complete

    config = input_spec.get("controller", {})
    if (
        len(runner) != 1
        or not isinstance(config, dict)
        or not {"workspace", "service"} <= set(config)
    ):
        raise CampaignError(
            "v2 requires an owned service controller and one public runner binary"
        )
    options = SimpleNamespace(
        workspace=Path(config["workspace"]),
        service=Path(config["service"]),
        runner=Path(runner[0]),
        output=arguments.output,
        port_base=config.get("port_base", 36001),
        resource=input_spec.get("resource_campaign", False),
    )
    try:
        run_complete(options, provenance=input_spec["provenance"])
    except (CampaignError, OSError, ValueError, subprocess.SubprocessError):
        if (arguments.output / "failure.json").exists():
            return 1
        raise
    return 0
