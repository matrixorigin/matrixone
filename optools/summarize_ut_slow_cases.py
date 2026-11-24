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
import io
import json
import os
import re
import stat
import sys
import time
from collections import OrderedDict
from contextlib import ExitStack, nullcontext
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple


from active_ut_incremental import (
    MAX_LIFECYCLE_LINE_BYTES, TERMINAL_ACTIONS, TEST_START_ACTIONS,
    apply_event, atomic_write, fresh, render,
)

AUX_INPUT_BYTES = 8 * 1024 * 1024
AUX_TERMINAL_BYTES = 512 * 1024 * 1024
AUX_TERMINAL_SECONDS = 2
PREFIX_CONTEXT_BYTES = 4096
TERMINAL_NEEDLE = re.compile(rb'"Action"[ \t\r]*:[ \t\r]*"(?:build-)?fail"')
MAX_AUX_LIFECYCLE_KEYS = 4096
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


def open_report(path: Path):
    # A path can change after selection. Never block on a substituted FIFO.
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError("report is not a regular file")
        return os.fdopen(fd, "rb")
    except BaseException:
        os.close(fd)
        raise


def snapshot_lines(report, require_lf: bool, diagnostics=None, source_name=None, window=None):
    """Read only the regular-file prefix present when this descriptor opens."""
    name = source_name if source_name is not None else report.name
    discard_first = False
    if window is None:
        info = os.fstat(report.fileno())
        if not stat.S_ISREG(info.st_mode):
            raise OSError("report is not a regular file")
        remaining = info.st_size
    else:
        remaining, clipped = window
        if clipped and remaining:
            # The first budgeted byte is the predecessor of the payload.
            discard_first = report.read(1) != b"\n"
            remaining -= 1
    try:
        while remaining:
            line = report.readline(min(remaining, MAX_LIFECYCLE_LINE_BYTES + 1))
            if not line:
                if diagnostics is not None:
                    diagnostics.append(f"report truncated while reading: {name}")
                break
            remaining -= len(line)
            oversized = len(line) + (0 if line.endswith(b"\n") else 1) > MAX_LIFECYCLE_LINE_BYTES
            if not line.endswith(b"\n") and remaining:
                oversized = True
            if oversized:
                while remaining and not line.endswith(b"\n"):
                    line = report.readline(min(remaining, MAX_LIFECYCLE_LINE_BYTES + 1))
                    if not line:
                        if diagnostics is not None:
                            diagnostics.append(f"report truncated while reading: {name}")
                        break
                    remaining -= len(line)
                if not discard_first:
                    yield b"!"  # Count the discarded oversized or torn record.
                discard_first = False
                continue
            if not line.endswith(b"\n") and (require_lf or remaining):
                break
            if not discard_first:
                yield line
            discard_first = False
    except OSError as error:
        if diagnostics is not None:
            diagnostics.append(f"report read failed: {name}: {error}")
        else:
            raise


def auxiliary_budgets(sizes: List[int], remaining: int) -> List[int]:
    """Reclaim spare bytes from small sources before sharing the rest."""
    budgets = [0] * len(sizes)
    for done, index in enumerate(sorted(range(len(sizes)), key=sizes.__getitem__)):
        budget = min(sizes[index], remaining // (len(sizes) - done))
        budgets[index] = budget
        remaining -= budget
    return budgets


def prefix_context(stream, end, package, test):
    """Nearby matching text is context only: an invocation may overlap."""
    start = max(0, end - PREFIX_CONTEXT_BYTES)
    read_start = max(0, start - 1)
    data = os.pread(stream.fileno(), end - read_start, read_start)
    if len(data) != end - read_start:
        raise OSError("report truncated while reading context")
    if start:
        # Reserve the first byte for the predecessor, as in the tail window.
        data = data[1:] if data.startswith(b"\n") else data.partition(b"\n")[2]
    chunks = []
    for line in data.split(b"\n"):
        try:
            event = json.loads(line)
        except (ValueError, RecursionError):
            continue
        if (isinstance(event, dict) and event.get("Action") in ("output", "build-output")
                and (event.get("Package") or event.get("ImportPath")) == package
                and (event.get("Test") or "") == test and isinstance(event.get("Output"), str)):
            chunks.append(event["Output"])
    return "".join(chunks)


def prefix_failures(stream, budget, deadline, limit, context_limit, diagnostics):
    """Scan LF-framed Go events; do not parse nonterminal lines or track lifecycle."""
    stream.seek(0)
    pending = b""
    discard = False
    failures, contexts = [], []
    position = 0
    partial = False
    while budget and time.monotonic() < deadline:
        try:
            data = stream.raw.read(min(budget, 256 * 1024))
        except OSError as error:
            diagnostics.append(f"report read failed: {stream.name}: {error}")
            return failures + contexts, True
        if not data:
            break
        budget -= len(data)
        position += len(data)
        if discard:
            end = data.find(b"\n")
            if end == -1:
                continue
            data = data[end + 1:]
            discard = False
        data = pending + data
        last = data.rfind(b"\n") + 1
        pending = data[last:]
        if len(pending) >= MAX_LIFECYCLE_LINE_BYTES:
            pending = b""
            discard = True
            partial = True

        offset = 0
        while match := TERMINAL_NEEDLE.search(data, offset, last):
            if time.monotonic() >= deadline:
                return failures + contexts, True
            first = max(offset, data.rfind(b"\n", offset, match.start()) + 1)
            end = data.find(b"\n", match.end()) + 1
            offset = end
            if end - first > MAX_LIFECYCLE_LINE_BYTES:
                partial = True
                continue
            try:
                event = json.loads(data[first:end])
            except (ValueError, RecursionError):
                continue
            if not isinstance(event, dict) or event.get("Action") not in ("fail", "build-fail"):
                continue
            package = event.get("Package") or event.get("ImportPath")
            if not isinstance(package, str) or not package:
                continue
            if len(failures) >= limit:
                return failures + contexts, True
            test = event.get("Test")
            test = test if isinstance(test, str) else ""
            failures.append((package[:4096], test[:4096], event["Action"], "", "", 0, False))
            if context_limit:
                context_limit -= 1
                try:
                    context = prefix_context(stream, position - len(data) + first, package, test)
                except OSError:
                    partial = True
                    continue
                if context:
                    contexts.append((package[:4096], test[:4096], "unresolved-output",
                                     context, "", len(context), False))
    return failures + contexts, partial or budget != 0

def summarize(
    report_path: Path, limit: int = 20, active_out: Optional[Path] = None,
    *, require_lifecycle_lf: bool = False, capture_failures: bool = False,
    failure_only: bool = False, stream=None, diagnostics=None, window=None,
    failure_limits: Tuple[int, int] = (MAX_FAILURES, MAX_UNRESOLVED_OUTPUTS),
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
    # A clipped prefix can conceal concurrent invocations. Keep their
    # snippets as context, not as output attributed to a specific failure.
    lifecycle = fresh(None) if (active_out is not None or capture_failures) and not (
        window and window[1]
    ) else None
    lifecycle_updates = 0
    failure_limit, context_limit = failure_limits
    unfinished = []

    with (nullcontext(stream) if stream is not None else open_report(report_path)) as report:
        for raw_line in snapshot_lines(report, require_lifecycle_lf, diagnostics, report_path, window):
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
                if lifecycle is None:
                    overlapping = False
                elif name:
                    entry = lifecycle["cases"].get(package, {}).get(name)
                    overlapping = bool(entry and entry[2] > 1)
                else:
                    entry = lifecycle["packages"].get(package)
                    overlapping = bool(entry and entry[1] > 1)
                if action in ("output", "build-output") and (
                    len(failures) < failure_limit or len(unresolved_outputs) < context_limit
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
                    if len(failures) < failure_limit:
                        if lifecycle is None and buffer[2]:
                            if len(unresolved_outputs) < context_limit:
                                unresolved_outputs.append((package, name, "unresolved-output", *buffer))
                            else:
                                omitted_failures += 1
                            buffer = ("", "", 0, False)
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
                            if len(unresolved_outputs) < context_limit:
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
                    apply_event(lifecycle, event, None if failure_only else unfinished)
                    if failure_only:
                        lifecycle_updates += 1
                        # Sample the key count every 256 updates. Each update
                        # can add at most a package bucket plus one key.
                        if lifecycle_updates % 256 == 0 and (
                            len(lifecycle["packages"]) + len(lifecycle["cases"])
                            + sum(map(len, lifecycle["cases"].values())) > MAX_AUX_LIFECYCLE_KEYS
                        ):
                            lifecycle = None
                            if diagnostics is not None:
                                diagnostics.append(f"overlap tracking capped; subsequent snippets are context only: {report_path}")
            if failure_only:
                continue
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


def print_failure_details(failures: List[Tuple[str, str, str, str, str, int, bool]], omitted: int,
                          partial: bool = False) -> None:
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
        print("[failed_ut_cases] none found in scanned records" if partial else "[failed_ut_cases] none")
    if omitted:
        suffix = " (scanned records only)" if partial else ""
        print(f"[failed_ut_cases] additional failure or context events omitted={omitted}{suffix}")


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("limit", nargs="?", type=int, default=20)
    parser.add_argument("--active-out", type=Path)
    parser.add_argument("--require-lifecycle-lf", action="store_true")
    parser.add_argument("--failures", action="store_true", help="include bounded output from failed cases")
    parser.add_argument("--live-source", action="append", type=Path, default=[])
    parser.add_argument("--stopped-source", action="append", type=Path, default=[])
    args = parser.parse_args(argv[1:])
    limit = args.limit
    if limit <= 0:
        parser.error("LIMIT must be a positive integer")
    tests, packages, malformed, failures, omitted = [], [], 0, [], 0
    seen = set()
    diagnostics = []
    try:
        with open_report(args.report) as stream:
            info = os.fstat(stream.fileno())
            seen.add((info.st_dev, info.st_ino))
            tests, packages, malformed, failures, omitted = summarize(
                args.report, limit, args.active_out, require_lifecycle_lf=args.require_lifecycle_lf,
                capture_failures=args.failures, stream=stream, diagnostics=diagnostics,
            )
    except OSError as error:
        if not (args.live_source or args.stopped_source):
            print(f"slow UT case report unavailable: {args.report}: {error}", file=sys.stderr)
            return 1
        diagnostics.append(f"main report unavailable: {args.report}: {error}")
    partial = False
    if args.failures:
        with ExitStack() as opened:
            sources = []
            for path, live in [(p, True) for p in args.live_source] + [(p, False) for p in args.stopped_source]:
                try:
                    stream = opened.enter_context(open_report(path))
                    info = os.fstat(stream.fileno())
                    identity = (info.st_dev, info.st_ino)
                    if identity in seen:
                        stream.close()
                        continue
                    seen.add(identity)
                    sources.append((path, live, stream, info.st_size))
                except OSError as error:
                    partial = True
                    diagnostics.append(f"report unavailable: {path}: {error}")
            budgets = auxiliary_budgets([size for _, _, _, size in sources], AUX_INPUT_BYTES)
            prefixes = []
            deadline = time.monotonic() + AUX_TERMINAL_SECONDS
            for (path, live, stream, size), budget in zip(sources, budgets):
                start = size - budget
                if not budget:
                    if size:
                        prefixes.append((stream, size))
                    continue
                try:
                    # Read the exact window without buffered read-ahead. Keep
                    # the chosen descriptor even if its path rotates.
                    stream.seek(start)
                    data = stream.raw.read(budget)
                    if len(data) != budget:
                        partial = True
                        diagnostics.append(f"report truncated while reading: {path}")
                    if start:
                        # The tail skips its leading frame (which may be just
                        # the predecessor LF). Include it in the prefix only.
                        boundary = data.find(b"\n") + 1
                        prefixes.append((stream, start + (boundary or len(data))))
                    context_count = sum(item[2] == "unresolved-output" for item in failures)
                    _, _, source_malformed, source_failures, source_omitted = summarize(
                        path, require_lifecycle_lf=live or len(data) != budget, capture_failures=True,
                        failure_only=True, stream=io.BytesIO(data), diagnostics=diagnostics,
                        window=(len(data), start != 0),
                        failure_limits=(MAX_FAILURES - len(failures) + context_count,
                                        MAX_UNRESOLVED_OUTPUTS - context_count),
                    )
                except OSError as error:
                    partial = True
                    diagnostics.append(f"report unavailable: {path}: {error}")
                    continue
                malformed += source_malformed
                omitted += source_omitted
                failures.extend(source_failures)
            # Charge the full context allowance up front so the two reads
            # together still respect the shared terminal-byte cap.
            retained_context = sum(item[2] == "unresolved-output" for item in failures)
            context_left = MAX_UNRESOLVED_OUTPUTS - retained_context
            prefix_budgets = auxiliary_budgets(
                [end for _, end in prefixes], max(0, AUX_TERMINAL_BYTES - context_left * (PREFIX_CONTEXT_BYTES + 1)),
            )
            prefix_found = 0
            retained = len(failures) - retained_context
            for (stream, end), budget in zip(prefixes, prefix_budgets):
                partial |= budget < end
                try:
                    context_quota = min(context_left, MAX_FAILURES - retained - prefix_found)
                    found, incomplete = prefix_failures(
                        stream, budget, deadline, MAX_FAILURES - retained - prefix_found, context_quota, diagnostics,
                    )
                except OSError as error:
                    diagnostics.append(f"report unavailable: {stream.name}: {error}")
                    partial = True
                    continue
                partial |= incomplete
                terminals = sum(item[2] != "unresolved-output" for item in found)
                context_left -= min(context_quota, terminals)
                prefix_found += terminals
                failures.extend(found)
            if prefix_found:
                diagnostics.append("additional complete terminal events from earlier ranges; nearby text is context only;"
                                   " assertion context can be missing outside the 4 KiB window or context budget")
        if partial:
            diagnostics.append("supplemental scan partial; byte, time or evidence limit; failure counts may be incomplete")
    for diagnostic in diagnostics:
        print(f"[failed_ut_cases] {diagnostic}")
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
        print_failure_details(failures, omitted, partial)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
