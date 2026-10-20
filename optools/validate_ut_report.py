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

"""Validate the lifecycle and orthogonality of a Go test JSON report.

The race runner may concatenate reports from several processes for one Go
package.  Package start/pass events may therefore repeat, but a test root must
appear in exactly one process.  Go's ``pause`` and ``cont`` events are state
transitions, not a second execution of the test; accepting them here keeps the
validator aligned with the ``go test -json`` contract while the pinned
go-ut-analysis binary remains compatible with older event sets.

Some test helpers emit a terminal event for a very short subtest without a
separate ``run`` record in the merged stream. Such a terminal is treated as an
implicit root and reported as a warning; a later execution of that same root is
still rejected as a duplicate.

The raw report is deliberately never rewritten.  A truncated report is
diagnostic evidence after a failed/cancelled run, so incomplete roots and
malformed trailing lines are warnings in the default mode.  A successful run
uses ``--require-complete`` and turns those conditions into a failed quality
gate.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


TERMINAL_ACTIONS = {"pass", "fail", "skip"}


@dataclass
class ActiveTest:
    package: str
    name: str
    line: int
    action: str


@dataclass
class Validation:
    events: int = 0
    test_starts: int = 0
    test_terminals: int = 0
    package_events: int = 0
    malformed: int = 0
    incomplete: int = 0
    implicit_terminals: int = 0
    incomplete_packages: int = 0
    errors: Optional[List[str]] = None

    def __post_init__(self) -> None:
        if self.errors is None:
            self.errors = []


def _event_fields(event: object) -> Optional[Tuple[str, str, str]]:
    if not isinstance(event, dict):
        return None
    action = event.get("Action")
    package = event.get("Package")
    test = event.get("Test", "")
    if not isinstance(action, str) or not isinstance(package, str):
        return None
    if not isinstance(test, str):
        return None
    if not package:
        return None
    return action, package, test


def validate(report_path: Path, require_complete: bool = False) -> Validation:
    result = Validation()
    active: Dict[Tuple[str, str], ActiveTest] = {}
    completed: set[Tuple[str, str]] = set()
    active_packages: Dict[str, int] = {}

    with report_path.open("rb") as report:
        for line_number, raw_line in enumerate(report, 1):
            try:
                line = raw_line.decode("utf-8")
            except UnicodeDecodeError:
                result.malformed += 1
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                result.malformed += 1
                continue

            fields = _event_fields(event)
            if fields is None:
                # Build records and ordinary output records do not carry a
                # test lifecycle.  Ignore them, as the Go analysis tool does.
                continue
            action, package, test = fields
            result.events += 1
            if not test:
                if action in {"start", *TERMINAL_ACTIONS}:
                    result.package_events += 1
                    if action == "start":
                        active_packages[package] = active_packages.get(package, 0) + 1
                    elif active_packages.get(package, 0) > 0:
                        active_packages[package] -= 1
                continue

            key = (package, test)
            if action == "run":
                result.test_starts += 1
                previous = active.get(key)
                if previous is not None:
                    result.errors.append(
                        f"line {line_number}: duplicate test root {package}:{test} "
                        f"(already active from line {previous.line})"
                    )
                elif key in completed:
                    result.errors.append(
                        f"line {line_number}: duplicate test root {package}:{test} "
                        "(the root already completed in an earlier report)"
                    )
                    # Keep the duplicate root active so its terminal event is
                    # consumed without creating a second spurious error.
                    active[key] = ActiveTest(package, test, line_number, action)
                else:
                    active[key] = ActiveTest(package, test, line_number, action)
                continue

            if action in {"pause", "cont"}:
                previous = active.get(key)
                if previous is None:
                    result.errors.append(
                        f"line {line_number}: {action} without an active test root "
                        f"{package}:{test}"
                    )
                else:
                    previous.action = action
                continue

            if action in TERMINAL_ACTIONS:
                result.test_terminals += 1
                previous = active.pop(key, None)
                if previous is None:
                    if key in completed:
                        result.errors.append(
                            f"line {line_number}: duplicate terminal event for "
                            f"{package}:{test}"
                        )
                    else:
                        # A short subtest can be observed only through its
                        # terminal event after report aggregation. Preserve it
                        # as a completed root instead of rejecting a healthy
                        # run, while making the loss of its start observable.
                        result.implicit_terminals += 1
                completed.add(key)

    result.incomplete = len(active)
    result.incomplete_packages = sum(active_packages.values())
    if require_complete:
        if result.incomplete_packages:
            packages = ", ".join(
                f"{package} ({count})"
                for package, count in sorted(active_packages.items()) if count
            )
            result.errors.append(
                f"report ended with {result.incomplete_packages} incomplete package lifecycle(s): {packages}"
            )
        if result.test_starts + result.implicit_terminals == 0:
            result.errors.append("report contains no test lifecycle events")
        if result.malformed:
            result.errors.append(
                f"report contains {result.malformed} malformed JSON line(s)"
            )
        if result.incomplete:
            roots = ", ".join(
                f"{package}:{test}" for package, test in sorted(active)
            )
            result.errors.append(
                f"report ended with {result.incomplete} incomplete test root(s): {roots}"
            )
    return result


def _print_result(result: Validation) -> None:
    print(
        "[ut_report] events={} test_starts={} test_terminals={} "
        "package_events={} malformed={} incomplete={}".format(
            result.events,
            result.test_starts,
            result.test_terminals,
            result.package_events,
            result.malformed,
            result.incomplete,
        )
    )
    if result.incomplete_packages and not any("incomplete package lifecycle" in error for error in result.errors):
        print(
            f"[ut_report] WARNING report ended with {result.incomplete_packages} incomplete package lifecycle(s)",
            file=sys.stderr,
        )
    if result.implicit_terminals:
        print(
            f"[ut_report] WARNING terminal-only test roots={result.implicit_terminals} "
            "(the merged stream omitted their run event)",
            file=sys.stderr,
        )
    for error in result.errors:
        print(f"[ut_report] ERROR {error}", file=sys.stderr)
    if result.malformed and not any("malformed JSON" in error for error in result.errors):
        print(
            f"[ut_report] WARNING report contains {result.malformed} malformed JSON line(s) "
            "(the report may be truncated by cancellation)",
            file=sys.stderr,
        )
    if result.incomplete and not any("incomplete test root" in error for error in result.errors):
        print(
            f"[ut_report] WARNING report ended with {result.incomplete} incomplete test root(s) "
            "(the run may have been cancelled)",
            file=sys.stderr,
        )


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-complete", action="store_true")
    parser.add_argument("report", type=Path)
    arguments = parser.parse_args(argv)
    if not arguments.report.is_file():
        print(f"UT report missing: {arguments.report}", file=sys.stderr)
        return 1

    result = validate(arguments.report, arguments.require_complete)
    _print_result(result)
    return 1 if result.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
