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

"""Summarize structured fixture setup timings from a Go test JSON report."""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


FIELD_RE = re.compile(r"(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>[^\s]+)")
DURATION_COMPONENT_RE = re.compile(
    r"(?P<value>[0-9]+(?:\.[0-9]+)?)(?P<unit>h|ms|us|µs|ns|m|s)"
)
UNIT_SECONDS = {
    "h": 60.0 * 60.0,
    "ns": 1e-9,
    "µs": 1e-6,
    "us": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
    "m": 60.0,
}


def duration_seconds(value: str) -> Optional[float]:
    """Parse the compound format emitted by time.Duration.String."""
    if not value:
        return None
    offset = 0
    total = 0.0
    while offset < len(value):
        match = DURATION_COMPONENT_RE.match(value, offset)
        if match is None:
            return None
        total += float(match.group("value")) * UNIT_SECONDS[match.group("unit")]
        offset = match.end()
    return total


def format_duration(seconds: float) -> str:
    if seconds >= 60:
        return f"{seconds / 60:.2f}m"
    if seconds >= 1:
        return f"{seconds:.2f}s"
    if seconds >= 1e-3:
        return f"{seconds * 1e3:.2f}ms"
    return f"{seconds * 1e6:.2f}us"


def setup_records(
    report_path: Path, parse_errors: Optional[List[int]] = None
) -> Iterable[Tuple[Dict[str, str], float]]:
    """Yield valid setup records from a possibly truncated Go test report.

    Go's JSON stream can end in the middle of an event when the runner is
    cancelled.  Keep accepting the valid prefix, but expose the number of
    malformed lines so a summary cannot look complete by accident.
    """
    with report_path.open(encoding="utf-8") as report:
        for line in report:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                if parse_errors is not None:
                    parse_errors[0] += 1
                continue
            if not isinstance(event, dict):
                continue
            output = event.get("Output")
            if not isinstance(output, str):
                continue
            for output_line in output.splitlines():
                marker = output_line.find("MO_UT_SETUP ")
                if marker < 0:
                    continue
                fields = dict(
                    FIELD_RE.findall(output_line[marker + len("MO_UT_SETUP ") :])
                )
                package = event.get("Package")
                test = event.get("Test")
                if isinstance(package, str) and package:
                    fields["package"] = package
                if isinstance(test, str) and test:
                    fields["test"] = test
                fixture = fields.get("fixture")
                phase = fields.get("phase")
                duration = fields.get("duration")
                if not fixture or not phase or not duration:
                    continue
                seconds = duration_seconds(duration)
                if seconds is None:
                    continue
                yield fields, seconds


def summarize_records(
    records: Iterable[Tuple[Dict[str, str], float]],
) -> Dict[Tuple[str, str], List[float]]:
    totals: Dict[Tuple[str, str], List[float]] = defaultdict(
        lambda: [0, 0.0, 0.0, 0]
    )
    for fields, seconds in records:
        fixture = fields["fixture"]
        phase = fields["phase"]
        count, total, maximum, errors = totals[(fixture, phase)]
        totals[(fixture, phase)] = [
            int(count) + 1,
            float(total) + seconds,
            max(float(maximum), seconds),
            int(errors) + (fields.get("status") == "error"),
        ]
    return totals


def summarize(report_path: Path) -> Dict[Tuple[str, str], List[float]]:
    return summarize_records(setup_records(report_path))


def admission_holders(report_path: Path) -> List[str]:
    """Explain holder time separately from overlapping admission waits.

    Package termination bounds an OS-owned lease but does not prove service
    cleanup. Concurrent invocations of the same package cannot be associated
    with terminal events without a PID, so leave those generations unresolved.
    Keep only lifecycle metadata, not the potentially large test output.
    """
    leases = []
    active = {}
    invocations = defaultdict(int)
    current_invocations = {}
    live = defaultdict(int)
    ambiguous = set()
    terminals = {}
    invocation_pids = defaultdict(set)
    with report_path.open(encoding="utf-8") as report:
        for line in report:
            try:
                event = json.loads(line)
                if not isinstance(event, dict):
                    continue
                package = event.get("Package")
                if not isinstance(package, str) or not package:
                    continue
                stamp = datetime.fromisoformat(event["Time"].replace("Z", "+00:00"))
                if stamp.tzinfo is None:
                    continue
                timestamp = stamp.timestamp()
            except (ValueError, KeyError, TypeError, AttributeError):
                continue
            action = event.get("Action")
            if action == "start":
                if live[package]:
                    ambiguous.add(package)
                invocations[package] += 1
                live[package] += 1
                current_invocations[package] = invocations[package]
            invocation = (package, current_invocations.get(package, 0))
            if action in ("pass", "fail", "skip") and not event.get("Test"):
                terminals[invocation] = timestamp
                live[package] = max(0, live[package] - 1)
                # A later startless capture must not inherit this process's
                # start event or overwrite its terminal bound.
                current_invocations.pop(package, None)
            output = event.get("Output", "")
            if not isinstance(output, str):
                continue
            for output_line in output.splitlines():
                marker = output_line.find("MO_UT_SETUP ")
                if marker < 0:
                    continue
                fields = dict(FIELD_RE.findall(output_line[marker + 12 :]))
                if fields.get("fixture") != "embedded-cluster":
                    continue
                pid, cluster = fields.get("pid"), fields.get("cluster_id")
                if not pid or not cluster:
                    continue
                invocation_pids[invocation].add(pid)
                key = (invocation, pid, cluster)
                if fields.get("phase") == "admission-acquire" and fields.get("status") == "ready":
                    lease = {
                        "invocation": invocation, "pid": pid, "cluster": cluster,
                        "start": timestamp, "end": None, "evidence": "open-at-capture",
                        "test": event.get("Test", "?"),
                    }
                    leases.append(lease)
                    active[key] = lease
                elif fields.get("admission_released") == "true" and key in active:
                    lease = active.pop(key)
                    if timestamp >= lease["start"]:
                        lease.update(end=timestamp, evidence="explicit")
    for lease in leases:
        invocation = lease["invocation"]
        terminal = terminals.get(invocation)
        if (lease["end"] is None and invocation[1] > 0 and invocation[0] not in ambiguous
                and len(invocation_pids[invocation]) == 1
                and terminal is not None and terminal >= lease["start"]):
            lease.update(end=terminal, evidence="process-exit-inferred")
    if not leases:
        return []
    counts = defaultdict(int)
    for lease in leases:
        counts[lease["evidence"]] += 1
    lines = [
        "[ut_setup] admission generations: "
        f"explicit={counts['explicit']} process-exit-inferred={counts['process-exit-inferred']} "
        f"unresolved={counts['open-at-capture']}; "
        "process exit is an upper bound, not clean service shutdown; "
        "wait totals overlap holder work and are not potential wall-time savings"
    ]
    bounded = [lease for lease in leases if lease["end"] is not None]
    bounded.sort(key=lambda lease: lease["end"] - lease["start"], reverse=True)
    for lease in bounded[:5]:
        lines.append(
            "[ut_setup] admission holder "
            f"duration={format_duration(lease['end'] - lease['start'])} "
            f"evidence={lease['evidence']} package={lease['invocation'][0]} "
            f"acquire_test={lease['test']} pid={lease['pid']} cluster={lease['cluster']}"
        )
    return lines


def summarize_embedded_diagnostics(
    records: Iterable[Tuple[Dict[str, str], float]],
) -> Optional[str]:
    """Return one bounded diagnosis line for embedded-cluster lifecycle cost."""
    embedded = [
        (fields, seconds)
        for fields, seconds in records
        if fields["fixture"] == "embedded-cluster"
    ]
    if not embedded:
        return None

    phase_totals: Dict[str, List[float]] = defaultdict(list)
    holds: List[float] = []
    cluster_ids = set()
    # A cluster may be started more than once in one test process.  Keep the
    # current lease state per cluster key and reset it at every successful
    # admission acquire; a historical release must not hide a later lease.
    # `unknown` means that a truncated/legacy report had a hold or release
    # record without a matching acquire event.
    admission_state: Dict[Tuple[str, str], str] = {}
    release_seen = set()
    waiters: List[Tuple[float, str]] = []
    for fields, seconds in embedded:
        phase_totals[fields["phase"]].append(seconds)
        cluster_id = fields.get("cluster_id")
        cluster_key = None
        if cluster_id:
            cluster_key = (fields.get("pid", ""), cluster_id)
            cluster_ids.add(cluster_key)
        if (
            cluster_key is not None
            and fields["phase"] == "admission-acquire"
            and fields.get("status") != "error"
        ):
            admission_state[cluster_key] = "active"
            wait_seconds = duration_seconds(fields.get("wait", ""))
            if wait_seconds is not None:
                package = fields.get("package", "?")
                test = fields.get("test", "?")
                owner = f"{package}:{test}"
                pid = fields.get("pid")
                cluster = fields.get("cluster_id")
                if pid or cluster:
                    owner += f" pid={pid or '?'} cluster={cluster or '?'}"
                waiters.append((wait_seconds, owner))
        hold = fields.get("hold")
        if hold is not None:
            if cluster_key is not None:
                if admission_state.get(cluster_key) != "active":
                    # Keep truncated/legacy reports useful even when the
                    # acquire record is missing from the captured output.  A
                    # hold after a previous released generation is also
                    # unmatched; do not let it inherit that old generation's
                    # terminal state.
                    admission_state[cluster_key] = "unknown"
            hold_seconds = duration_seconds(hold)
            if hold_seconds is not None:
                holds.append(hold_seconds)
        if cluster_key is not None and fields.get("admission_released") == "true":
            release_seen.add(cluster_key)
            if admission_state.get(cluster_key) == "active":
                admission_state[cluster_key] = "released"
            else:
                # A release without the matching acquire may be the prefix or
                # suffix of a truncated report; do not call it complete.  This
                # also covers a release after an already-closed generation.
                admission_state[cluster_key] = "unknown"

    def phase_stats(phase: str) -> Optional[str]:
        values = phase_totals.get(phase)
        if not values:
            return None
        return (
            f"total={format_duration(sum(values))} "
            f"max={format_duration(max(values))}"
        )

    cluster_count = len(cluster_ids)
    if not cluster_count:
        cluster_count = max(
            len(phase_totals.get("cluster-construct", [])),
            len(phase_totals.get("admission-acquire", [])),
            len(phase_totals.get("service-start", [])),
        )
    details = [f"clusters={cluster_count}"]
    admission = phase_stats("admission-acquire")
    if admission is not None:
        details.append(f"admission_wait({admission})")
    service_start = phase_stats("service-start")
    if service_start is not None:
        details.append(f"service_start({service_start})")
    service_close = phase_stats("service-close")
    if service_close is not None:
        details.append(f"service_close({service_close})")
    if holds:
        details.append(
            "admission_hold_observed_max=" + format_duration(max(holds))
        )
    if admission_state:
        unreleased = sum(state != "released" for state in admission_state.values())
        details.append(f"admission_unreleased_observed={unreleased}")
        if all(state == "released" for state in admission_state.values()):
            evidence = "complete"
        elif release_seen:
            evidence = "partial"
        else:
            evidence = "none"
        details.append(f"admission_release_evidence={evidence}")
    if waiters:
        slow_waiters = sorted(waiters, reverse=True)[:3]
        details.append(
            "slowest_completed_admission_waits="
            + ",".join(
                f"{format_duration(seconds)}:{owner[:120]}"
                for seconds, owner in slow_waiters
            )
        )
    return "[ut_setup] embedded-cluster diagnosis: " + " ".join(details)


def main(argv: List[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} GO_TEST_JSON", file=sys.stderr)
        return 2
    report_path = Path(argv[1])
    if not report_path.is_file():
        print(f"UT JSON report does not exist: {report_path}", file=sys.stderr)
        return 2

    parse_errors = [0]
    records = list(setup_records(report_path, parse_errors))
    if parse_errors[0]:
        print(
            f"[ut_setup] ignored malformed JSON lines={parse_errors[0]} "
            "(report may be truncated by cancellation)"
        )
    totals = summarize_records(records)
    if not totals:
        return 0

    print(
        "[ut_setup] slow fixture phases (sorted by cumulative time; "
        "total rows may include subphases):"
    )
    ordered = sorted(
        totals.items(), key=lambda item: (-float(item[1][1]), item[0])
    )
    for (fixture, phase), (count, total, maximum, errors) in ordered[:20]:
        error_suffix = f" errors={errors}" if errors else ""
        print(
            f"[ut_setup] fixture={fixture} phase={phase} count={count} "
            f"total={format_duration(float(total))} max={format_duration(float(maximum))}"
            f"{error_suffix}"
        )
    diagnosis = summarize_embedded_diagnostics(records)
    if diagnosis is not None:
        print(diagnosis)
    for line in admission_holders(report_path):
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
