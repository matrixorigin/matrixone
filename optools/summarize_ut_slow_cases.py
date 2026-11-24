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

"""Report the slowest completed tests from a (possibly truncated) Go JSON log."""

from __future__ import annotations

import argparse
import json
import sys
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


from active_ut_incremental import (
    MAX_LIFECYCLE_LINE_BYTES, TERMINAL_ACTIONS, TEST_START_ACTIONS,
    apply_event, atomic_write, fresh, render,
)

MAX_FAILURES = 32
MAX_UNRESOLVED_OUTPUTS = 8
MAX_BUFFERED_OUTPUTS = 256
FAILURE_HEAD_CHARS = 4096
FAILURE_TAIL_CHARS = 12288


def parse_time(value: object) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def event_elapsed(
    event: Dict[str, object],
    started: Optional[datetime],
    ended: Optional[datetime],
) -> Optional[float]:
    elapsed = event.get("Elapsed")
    if isinstance(elapsed, (int, float)) and elapsed >= 0:
        return float(elapsed)
    if started is not None and ended is not None:
        seconds = (ended - started).total_seconds()
        if seconds >= 0:
            return seconds
    return None


def format_duration(seconds: float) -> str:
    if seconds >= 3600:
        return f"{seconds / 3600:.2f}h"
    if seconds >= 60:
        return f"{seconds / 60:.2f}m"
    if seconds >= 1:
        return f"{seconds:.2f}s"
    if seconds >= 1e-3:
        return f"{seconds * 1e3:.2f}ms"
    return f"{seconds * 1e6:.2f}us"


def summarize(
    report_path: Path, limit: int = 20, active_out: Optional[Path] = None,
    *, require_lifecycle_lf: bool = False, capture_failures: bool = False,
) -> Tuple[
    List[Tuple[float, str, str, str, str]],
    List[Tuple[float, str, str, str, str]],
    int,
    List[Tuple[str, str, str, str, str, int, bool]],
    int,
]:
    active_tests: Dict[Tuple[str, str], datetime] = {}
    active_packages: Dict[str, datetime] = {}
    completed_tests: List[Tuple[float, str, str, str, str]] = []
    completed_packages: List[Tuple[float, str, str, str, str]] = []
    malformed = 0
    failures: List[Tuple[str, str, str, str, str, int, bool]] = []
    unresolved_outputs: List[Tuple[str, str, str, str, str, int, bool]] = []
    omitted_failures = 0
    outputs = OrderedDict()
    output_evicted = False
    lifecycle = fresh(None) if active_out is not None or capture_failures else None
    unfinished = []

    with report_path.open("rb") as report:
        for raw_line in report:
            try:
                line = raw_line.decode("utf-8")
            except UnicodeDecodeError:
                # Cancellation can leave a partial multi-byte character in the
                # final line.  Preserve all complete preceding events.
                malformed += 1
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                malformed += 1
                continue
            if not isinstance(event, dict):
                continue

            action = event.get("Action")
            package = event.get("Package") or event.get("ImportPath")
            if not isinstance(action, str) or not isinstance(package, str) or not package:
                continue
            test = event.get("Test")
            complete = raw_line.endswith(b"\n") or not require_lifecycle_lf
            if capture_failures and complete:
                name = test if isinstance(test, str) else ""
                output_key = (package, name)
                if name:
                    entry = lifecycle["cases"].get(package, {}).get(name)
                    overlapping = bool(entry and entry[2] > 1)
                else:
                    entry = lifecycle["packages"].get(package)
                    overlapping = bool(entry and entry[1] > 1)
                if action in ("output", "build-output") and (
                    len(failures) < MAX_FAILURES or len(unresolved_outputs) < MAX_UNRESOLVED_OUTPUTS
                ):
                    output = event.get("Output")
                    if isinstance(output, str):
                        if output_key not in outputs and len(outputs) >= MAX_BUFFERED_OUTPUTS:
                            outputs.popitem(last=False)
                            output_evicted = True
                        head, tail, total, maybe_missing = outputs.pop(
                            output_key, ("", "", 0, output_evicted),
                        )
                        take = max(0, FAILURE_HEAD_CHARS - len(head))
                        head += output[:take]
                        remainder = output[take:]
                        if len(remainder) >= FAILURE_TAIL_CHARS:
                            tail = remainder[-FAILURE_TAIL_CHARS:]
                        else:
                            tail = (tail + remainder)[-FAILURE_TAIL_CHARS:]
                        outputs[output_key] = (head, tail, total + len(output), maybe_missing)
                elif action in ("fail", "build-fail"):
                    buffer = outputs.get(output_key, ("", "", 0, output_evicted))
                    if not overlapping:
                        outputs.pop(output_key, None)
                    key = (package, name, action)
                    # A package can have multiple independent batched test
                    # invocations. Each terminal event is authoritative.
                    if len(failures) < MAX_FAILURES:
                        failures.append((*key, *buffer))
                    else:
                        omitted_failures += 1
                    if action == "fail" and not name:
                        # A timed-out binary may emit a package failure without
                        # terminal events for its running tests. Other package
                        # invocations can overlap, so these are context only.
                        for (other_package, other_test), other_buffer in outputs.items():
                            if other_package != package or not other_test:
                                continue
                            context_key = (package, other_test, "unresolved-output")
                            if len(unresolved_outputs) < MAX_UNRESOLVED_OUTPUTS:
                                unresolved_outputs.append((*context_key, *other_buffer))
                            else:
                                omitted_failures += 1
                elif action in ("pass", "skip"):
                    if not overlapping:
                        outputs.pop(output_key, None)
                if not name and action in ("pass", "fail", "skip", "build-fail") and not overlapping:
                    # This was the last observed invocation of the package;
                    # unfinished context belongs to the generation just ended.
                    for buffered_key in list(outputs):
                        if buffered_key[0] == package:
                            del outputs[buffered_key]
            if action not in TEST_START_ACTIONS and action not in TERMINAL_ACTIONS and action != "start":
                continue
            if lifecycle is not None:
                has_lf = raw_line.endswith(b"\n")
                # A stopped writer's decoded EOF record is equivalent to the
                # merged record, including the separator in the size budget.
                framed_size = len(raw_line) + (0 if has_lf else 1)
                if framed_size <= MAX_LIFECYCLE_LINE_BYTES and (has_lf or not require_lifecycle_lf):
                    apply_event(lifecycle, event, unfinished)
            timestamp = parse_time(event.get("Time"))
            if isinstance(test, str) and test:
                key = (package, test)
                if action in TEST_START_ACTIONS:
                    if timestamp is not None:
                        active_tests.setdefault(key, timestamp)
                elif action in TERMINAL_ACTIONS:
                    started = active_tests.pop(key, None)
                    elapsed = event_elapsed(event, started, timestamp)
                    if elapsed is not None:
                        completed_tests.append(
                            (
                                elapsed,
                                package,
                                test,
                                action,
                                timestamp.isoformat() if timestamp else "?",
                            )
                        )
                continue

            if action == "start":
                if timestamp is not None:
                    active_packages.setdefault(package, timestamp)
            elif action in TERMINAL_ACTIONS:
                started = active_packages.pop(package, None)
                elapsed = event_elapsed(event, started, timestamp)
                if elapsed is not None:
                    completed_packages.append(
                        (
                            elapsed,
                            package,
                            "",
                            action,
                            timestamp.isoformat() if timestamp else "?",
                        )
                    )

    if active_out is not None:
        try:
            atomic_write(active_out, "\n".join(render({"report": lifecycle}, unfinished)) + "\n",
                         prefix=active_out.name + ".tmp.")
        except OSError as error:
            print(f"failed to publish UT lifecycle summary: {error}", file=sys.stderr)
    completed_tests.sort(key=lambda item: item[0], reverse=True)
    completed_packages.sort(key=lambda item: item[0], reverse=True)
    return completed_tests[:limit], completed_packages[:limit], malformed, failures + unresolved_outputs, omitted_failures


def print_failure_details(failures: List[Tuple[str, str, str, str, str, int, bool]], omitted: int) -> None:
    """Print the bounded output collected before each terminal failure."""
    print("[failed_ut_cases] failures and unresolved output:")
    print("[failed_ut_cases] output is bounded; overlapping invocations may interleave")
    for package, test, action, head, tail, total, maybe_missing in failures:
        print(f"[failed_ut_cases] action={action} package={json.dumps(package)} test={json.dumps(test)}")
        if maybe_missing:
            print("[failed_ut_output] earlier output may be missing (buffer limit reached)")
        output = head
        if total > len(head) + len(tail):
            output += "\n[output truncated]\n"
        output += tail
        if not output:
            print("[failed_ut_output] no output retained")
        for line in output.splitlines():
            print(f"[failed_ut_output] {line}")
    if not failures:
        print("[failed_ut_cases] none")
    if omitted:
        print(f"[failed_ut_cases] additional failure or context events omitted={omitted}")


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("limit", nargs="?", type=int, default=20)
    parser.add_argument("--active-out", type=Path)
    parser.add_argument("--require-lifecycle-lf", action="store_true")
    parser.add_argument("--failures", action="store_true", help="include bounded output from failed cases")
    args = parser.parse_args(argv[1:])
    limit = args.limit
    if limit <= 0:
        parser.error("LIMIT must be a positive integer")
    if not args.report.is_file():
        print(f"slow UT case report missing: {args.report}", file=sys.stderr)
        return 1

    tests, packages, malformed, failures, omitted = summarize(
        args.report, limit, args.active_out, require_lifecycle_lf=args.require_lifecycle_lf,
        capture_failures=args.failures,
    )
    print(f"[slow_ut_cases] completed test cases (top {limit}):")
    if tests:
        for elapsed, package, test, result, ended in tests:
            print(
                f"[slow_ut_cases] elapsed={format_duration(elapsed)} "
                f"result={result} package={package} test={test} ended={ended}"
            )
    else:
        print("[slow_ut_cases] none")

    print(f"[slow_ut_cases] completed packages (top {limit}):")
    if packages:
        for elapsed, package, _, result, ended in packages:
            print(
                f"[slow_ut_cases] elapsed={format_duration(elapsed)} "
                f"result={result} package={package} ended={ended}"
            )
    else:
        print("[slow_ut_cases] none")

    if malformed:
        print(
            f"[slow_ut_cases] ignored malformed JSON lines={malformed} "
            "(report may be truncated by cancellation)"
        )
    if args.failures:
        print_failure_details(failures, omitted)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
