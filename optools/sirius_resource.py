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

"""Stream Nsight allocation records into safe physical-memory summaries."""
import argparse
import json
from pathlib import Path
import sqlite3
import tempfile
import os
import contextlib
import resource as posix_resource
import signal

from sirius_campaign_v2 import CampaignError


MAX_LIVE_ALLOCATIONS = 65536
MAX_TRACE_BYTES = 16 << 30
MAX_LEDGER_BYTES = 4 << 30
MAX_SORT_FILE_BYTES = 8 << 30
MAX_ADAPTER_MEMORY_BYTES = 1 << 30


@contextlib.contextmanager
def bounded_sqlite_sort(parent):
    """Exclusive collector scope; bound SQLite's otherwise hidden sort files."""
    with tempfile.TemporaryDirectory(prefix="sirius-sort-", dir=parent) as directory:
        previous_directory = os.environ.get("SQLITE_TMPDIR")
        previous_signal = signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
        changed = []
        try:
            for kind, cap in (
                (posix_resource.RLIMIT_FSIZE, MAX_SORT_FILE_BYTES),
                (posix_resource.RLIMIT_AS, MAX_ADAPTER_MEMORY_BYTES),
            ):
                previous = posix_resource.getrlimit(kind)
                soft = (
                    cap
                    if previous[0] == posix_resource.RLIM_INFINITY
                    else min(cap, previous[0])
                )
                posix_resource.setrlimit(kind, (soft, previous[1]))
                changed.append((kind, previous))
            os.environ["SQLITE_TMPDIR"] = directory
            yield Path(directory)
        finally:
            if previous_directory is None:
                os.environ.pop("SQLITE_TMPDIR", None)
            else:
                os.environ["SQLITE_TMPDIR"] = previous_directory
            try:
                for kind, previous in reversed(changed):
                    posix_resource.setrlimit(kind, previous)
            finally:
                signal.signal(signal.SIGXFSZ, previous_signal)


class AllocationLedger:
    """Small live map, spilling exact generations to a private bounded database."""

    def __init__(self, parent):
        self.memory = {}
        self.directory = self.database = None
        self.parent = parent
        self.operations = 0

    def _spill(self):
        self.directory = tempfile.TemporaryDirectory(
            prefix="sirius-ledger-", dir=self.parent
        )
        path = Path(self.directory.name) / "live.sqlite"
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(descriptor)
        self.database = sqlite3.connect(path)
        self.database.execute("pragma cache_size=-8192")
        self.database.execute("pragma mmap_size=0")
        self.database.execute("pragma journal_mode=off")
        self.database.execute("pragma synchronous=off")
        page_size = self.database.execute("pragma page_size").fetchone()[0]
        max_pages = MAX_LEDGER_BYTES // page_size
        if max_pages < 2:
            raise CampaignError("allocation ledger scratch quota exceeded")
        self.database.execute(f"pragma max_page_count={max_pages}")
        try:
            self.database.execute(
                "create table live(context text,address text,category text,bytes integer,primary key(context,address,category)) without rowid"
            )
            self.database.executemany(
                "insert into live values(?,?,?,?)",
                ((*self._key(key), value) for key, value in self.memory.items()),
            )
        except sqlite3.OperationalError as error:
            if error.sqlite_errorcode == sqlite3.SQLITE_FULL:
                raise CampaignError(
                    "allocation ledger scratch quota exceeded"
                ) from error
            raise
        self.memory.clear()

    @staticmethod
    def _key(key):
        return tuple(str(value) for value in key)

    def _quota(self):
        self.operations += 1
        if self.operations % 4096 == 0:
            pages = self.database.execute("pragma page_count").fetchone()[0]
            page_size = self.database.execute("pragma page_size").fetchone()[0]
            if pages * page_size > MAX_LEDGER_BYTES:
                raise CampaignError("allocation ledger scratch quota exceeded")

    def add(self, key, size):
        if self.database is None:
            if key in self.memory:
                raise CampaignError("allocation generation was reused before free")
            if len(self.memory) < MAX_LIVE_ALLOCATIONS:
                self.memory[key] = size
                return
            self._spill()
        try:
            self.database.execute(
                "insert into live values(?,?,?,?)", (*self._key(key), size)
            )
        except sqlite3.IntegrityError as error:
            raise CampaignError(
                "allocation generation was reused before free"
            ) from error
        except sqlite3.OperationalError as error:
            if error.sqlite_errorcode == sqlite3.SQLITE_FULL:
                raise CampaignError(
                    "allocation ledger scratch quota exceeded"
                ) from error
            raise
        self._quota()

    def release(self, key):
        if self.database is None:
            if key not in self.memory:
                raise CampaignError("allocation trace lacks matching acquisition")
            return self.memory.pop(key)
        encoded = self._key(key)
        row = self.database.execute(
            "select bytes from live where context=? and address=? and category=?",
            encoded,
        ).fetchone()
        if row is None:
            raise CampaignError("allocation trace lacks matching acquisition")
        self.database.execute(
            "delete from live where context=? and address=? and category=?", encoded
        )
        self._quota()
        return row[0]

    def close(self):
        try:
            if self.database is not None:
                self.database.close()
        finally:
            if self.directory is not None:
                self.directory.cleanup()


def allocation_summary(path, pid, runs=()):
    with bounded_sqlite_sort(Path(path).resolve().parent):
        return _allocation_summary(path, pid, runs)


def _allocation_summary(path, pid, runs):
    if type(pid) is not int or pid <= 0 or Path(path).stat().st_size > MAX_TRACE_BYTES:
        raise CampaignError("allocation trace identity or quota invalid")
    connection = sqlite3.connect(f"file:{Path(path).resolve()}?mode=ro", uri=True)
    live = AllocationLedger(Path(path).resolve().parent)
    try:
        connection.execute("pragma cache_size=-8192")
        connection.execute("pragma mmap_size=0")
        # Single-threaded SQLite sorting uses a main and auxiliary PMA file.
        # Both have the OS file cap above and participate in the reserved quota.
        connection.execute("pragma threads=0")
        connection.execute("pragma temp_store=file")
        tables = {
            row[0]
            for row in connection.execute(
                "select name from sqlite_master where type='table'"
            )
        }
        if (
            not {
                "CUDA_GPU_MEMORY_USAGE_EVENTS",
                "ENUM_CUDA_MEM_KIND",
                "ENUM_CUDA_DEV_MEM_EVENT_OPER",
            }
            <= tables
        ):
            raise CampaignError("required allocation trace tables unavailable")
        if (
            "DIAGNOSTIC_EVENT" in tables
            and connection.execute(
                "select 1 from DIAGNOSTIC_EVENT where severity>=2 and (text like '%CUDA%' or text like '%CUPTI%') and (globalPid is null or globalPid<=0 or ((globalPid>>24)&16777215)=?) limit 1",
                (pid,),
            ).fetchone()
        ):
            raise CampaignError("CUDA allocation trace reports incomplete collection")
        kinds = {
            row[0]: row[1]
            for row in connection.execute("select id,label from ENUM_CUDA_MEM_KIND")
        }
        operations = {
            row[0]: row[1]
            for row in connection.execute(
                "select id,label from ENUM_CUDA_DEV_MEM_EVENT_OPER"
            )
        }
        totals = {"pinned": 0, "device": 0}
        peaks = totals.copy()
        counts = totals.copy()
        first = last = None
        spans = []
        for run in runs:
            if run["backend"] != "embedded":
                continue
            span = run.get("resource_span", {})
            if (
                span.get("time_domain") != "unix-epoch-ns"
                or span.get("scope") != "runner-lifetime"
                or type(span.get("start_ns")) is not int
                or type(span.get("end_ns")) is not int
                or span["start_ns"] >= span["end_ns"]
                or spans
                and span["start_ns"] < spans[-1]["end_ns"]
            ):
                raise CampaignError("invalid or overlapping resource observation span")
            spans.append(
                {
                    **span,
                    "sequence": run["sequence"],
                    "query_id": run["query_id"],
                    "phase": run["phase"],
                    "before_bytes": None,
                    "peak_bytes": None,
                    "after_bytes": None,
                }
            )
        if len(spans) > 1024:
            raise CampaignError("resource span count exceeds bound")
        span_index = 0
        query = "select start,globalPid,contextId,address,bytes,memKind,memoryOperationType from CUDA_GPU_MEMORY_USAGE_EVENTS where ((globalPid >> 24) & 16777215)=? order by start"
        for (
            timestamp,
            global_pid,
            context,
            address,
            size,
            kind,
            operation,
        ) in connection.execute(query, (pid,)):
            # Nsight GlobalId packs PID at bit 24; no process-name attribution.
            if (global_pid >> 24) & 0xFFFFFF != pid:
                continue
            label = str(kinds.get(kind, "")).upper()
            category = (
                "pinned"
                if "PINNED" in label
                else "device" if "DEVICE" in label or "ARRAY" in label else None
            )
            if category is None:
                continue
            while span_index < len(spans) and timestamp > spans[span_index]["end_ns"]:
                span = spans[span_index]
                if span["before_bytes"] is None:
                    span["before_bytes"], span["peak_bytes"] = (
                        totals.copy(),
                        totals.copy(),
                    )
                span["after_bytes"] = totals.copy()
                span_index += 1
            if (
                span_index < len(spans)
                and timestamp >= spans[span_index]["start_ns"]
                and spans[span_index]["before_bytes"] is None
            ):
                spans[span_index]["before_bytes"], spans[span_index]["peak_bytes"] = (
                    totals.copy(),
                    totals.copy(),
                )
            if type(size) is not int or size < 0:
                raise CampaignError("invalid traced allocation size")
            if (context is not None and type(context) is not int) or type(
                address
            ) is not int:
                raise CampaignError("invalid traced allocation identity")
            key = (context, address, category)
            action = str(operations.get(operation, "")).upper()
            if "FREE" in action or "DEALLOC" in action:
                totals[category] -= live.release(key)
            elif "ALLOC" in action:
                live.add(key, size)
                totals[category] += size
                peaks[category] = max(peaks[category], totals[category])
                counts[category] += 1
            else:
                raise CampaignError("unknown allocation operation")
            first = timestamp if first is None else first
            last = timestamp
            if (
                span_index < len(spans)
                and spans[span_index]["before_bytes"] is not None
            ):
                span = spans[span_index]
                for key in totals:
                    span["peak_bytes"][key] = max(span["peak_bytes"][key], totals[key])
        if any(counts[key] == 0 for key in totals):
            raise CampaignError("pinned or GPU allocation evidence unavailable")
        if spans:
            try:
                capture_start = connection.execute(
                    "select utcEpochNs from TARGET_INFO_SESSION_START_TIME"
                ).fetchone()[0]
                duration = connection.execute(
                    "select duration from ANALYSIS_DETAILS"
                ).fetchone()[0]
            except (sqlite3.Error, TypeError) as error:
                raise CampaignError(
                    "allocation trace lacks capture clock coverage"
                ) from error
            if (
                not capture_start
                <= spans[0]["start_ns"]
                <= spans[-1]["end_ns"]
                <= capture_start + duration
                or first < capture_start
                or last > capture_start + duration
            ):
                raise CampaignError(
                    "allocation trace does not bracket the resource runs"
                )
        while span_index < len(spans):
            span = spans[span_index]
            if span["before_bytes"] is None:
                span["before_bytes"], span["peak_bytes"] = totals.copy(), totals.copy()
            span["after_bytes"] = totals.copy()
            span_index += 1
        return {
            "source": "nsight-cupti-memory-events",
            "scope": "cn-process-lifetime",
            "pid": pid,
            "units": "bytes",
            "start_ns": first,
            "end_ns": last,
            "allocation_counts": counts,
            "peak_bytes": peaks,
            "final_bytes": totals,
            "runs": spans,
        }
    finally:
        try:
            live.close()
        finally:
            connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sqlite", type=Path)
    parser.add_argument("--pid", type=int, required=True)
    args = parser.parse_args()
    print(json.dumps(allocation_summary(args.sqlite, args.pid), sort_keys=True))
