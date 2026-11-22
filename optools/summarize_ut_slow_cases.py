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
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


from active_ut_incremental import (
    MAX_LIFECYCLE_LINE_BYTES, TERMINAL_ACTIONS, TEST_START_ACTIONS,
    apply_event, atomic_write, fresh, render,
)


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
    *, require_lifecycle_lf: bool = False,
) -> Tuple[
    List[Tuple[float, str, str, str, str]],
    List[Tuple[float, str, str, str, str]],
    int,
]:
    active_tests: Dict[Tuple[str, str], datetime] = {}
    active_packages: Dict[str, datetime] = {}
    completed_tests: List[Tuple[float, str, str, str, str]] = []
    completed_packages: List[Tuple[float, str, str, str, str]] = []
    malformed = 0
    lifecycle = fresh(None) if active_out is not None else None
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
            package = event.get("Package")
            if not isinstance(action, str) or not isinstance(package, str) or not package:
                continue
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
            test = event.get("Test")
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
    return completed_tests[:limit], completed_packages[:limit], malformed


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("limit", nargs="?", type=int, default=20)
    parser.add_argument("--active-out", type=Path)
    parser.add_argument("--require-lifecycle-lf", action="store_true")
    args = parser.parse_args(argv[1:])
    limit = args.limit
    if limit <= 0:
        parser.error("LIMIT must be a positive integer")
    if not args.report.is_file():
        print(f"slow UT case report missing: {args.report}", file=sys.stderr)
        return 1

    tests, packages, malformed = summarize(
        args.report, limit, args.active_out, require_lifecycle_lf=args.require_lifecycle_lf,
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
