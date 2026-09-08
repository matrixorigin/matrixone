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
from pathlib import Path
from typing import Dict, List, Optional, Tuple


FIELD_RE = re.compile(r"(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>[^\s]+)")
DURATION_RE = re.compile(r"^(?P<value>[0-9]+(?:\.[0-9]+)?)(?P<unit>ns|µs|us|ms|s|m)$")
UNIT_SECONDS = {
    "ns": 1e-9,
    "µs": 1e-6,
    "us": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
    "m": 60.0,
}


def duration_seconds(value: str) -> Optional[float]:
    match = DURATION_RE.fullmatch(value)
    if match is None:
        return None
    return float(match.group("value")) * UNIT_SECONDS[match.group("unit")]


def format_duration(seconds: float) -> str:
    if seconds >= 60:
        return f"{seconds / 60:.2f}m"
    if seconds >= 1:
        return f"{seconds:.2f}s"
    if seconds >= 1e-3:
        return f"{seconds * 1e3:.2f}ms"
    return f"{seconds * 1e6:.2f}us"


def summarize(report_path: Path) -> Dict[Tuple[str, str], List[float]]:
    totals: Dict[Tuple[str, str], List[float]] = defaultdict(
        lambda: [0, 0.0, 0.0, 0]
    )
    with report_path.open(encoding="utf-8") as report:
        for line in report:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            output = event.get("Output")
            if not isinstance(output, str):
                continue
            for output_line in output.splitlines():
                marker = output_line.find("MO_UT_SETUP ")
                if marker < 0:
                    continue
                fields = dict(FIELD_RE.findall(output_line[marker + len("MO_UT_SETUP ") :]))
                fixture = fields.get("fixture")
                phase = fields.get("phase")
                duration = fields.get("duration")
                if not fixture or not phase or not duration:
                    continue
                seconds = duration_seconds(duration)
                if seconds is None:
                    continue
                count, total, maximum, errors = totals[(fixture, phase)]
                totals[(fixture, phase)] = [
                    int(count) + 1,
                    float(total) + seconds,
                    max(float(maximum), seconds),
                    int(errors) + (fields.get("status") == "error"),
                ]
    return totals


def main(argv: List[str]) -> int:
    if len(argv) != 2:
        print(f"usage: {argv[0]} GO_TEST_JSON", file=sys.stderr)
        return 2
    report_path = Path(argv[1])
    if not report_path.is_file():
        print(f"UT JSON report does not exist: {report_path}", file=sys.stderr)
        return 2

    totals = summarize(report_path)
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
