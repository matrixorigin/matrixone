#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Heartbeat-only incremental view; raw Go JSON remains the source of truth."""

import argparse
import json
import os
import tempfile


STATE_VERSION = 2
MAX_LIFECYCLE_LINE_BYTES = 1024 * 1024
TEST_START_ACTIONS = ("run", "pause", "cont")
TERMINAL_ACTIONS = ("pass", "fail", "skip")


def fresh(identity):
    return {"identity": identity, "offset": 0, "guard": "", "packages": {}, "cases": {}}


def valid(state):
    if not (
        isinstance(state, dict)
        and isinstance(state.get("offset"), int)
        and state["offset"] >= 0
        and isinstance(state.get("guard"), str)
        and isinstance(state.get("packages"), dict)
        and isinstance(state.get("cases"), dict)
    ):
        return False

    def package_entry(entry):
        return (
            isinstance(entry, list) and len(entry) == 2
            and isinstance(entry[0], str)
            and type(entry[1]) is int and entry[1] > 0
        )

    def cases_entry(cases):
        return isinstance(cases, dict) and all(
            isinstance(entry, list) and len(entry) == 4
            and isinstance(entry[0], str) and isinstance(entry[1], str)
            and type(entry[2]) is int and entry[2] > 0
            and isinstance(entry[3], bool)
            for entry in cases.values()
        )

    return all(map(package_entry, state["packages"].values())) and all(
        map(cases_entry, state["cases"].values())
    )


def apply_event(state, event, unfinished=None):
    action, package = event.get("Action", ""), event.get("Package", "")
    test, when = event.get("Test", ""), event.get("Time", "")
    if not all(isinstance(v, str) for v in (action, package, test, when)) or not package:
        return
    packages, cases = state["packages"], state["cases"]
    invocation = packages.get(package)
    if not test:
        if action == "start":
            if invocation is None:
                packages[package] = [when, 1]
            else:
                invocation[1] += 1
        elif action in TERMINAL_ACTIONS:
            if invocation is not None and invocation[1] > 1:
                # The merged stream cannot tell which invocation exited.
                invocation[0] = "unknown"
                invocation[1] -= 1
                for entry in cases.get(package, {}).values():
                    entry[3] = True
            else:
                packages.pop(package, None)
                outstanding = cases.pop(package, {})
                if unfinished is not None:
                    unfinished.extend(
                        (package, name, last_action, started, "unknown" if ambiguous else action)
                        for name, (last_action, started, _, ambiguous) in outstanding.items()
                    )
        return
    if action in TEST_START_ACTIONS:
        current = cases.setdefault(package, {})
        entry = current.get(test)
        if entry is None:
            current[test] = [action, when, 1, False]
        elif (action == "run" and invocation is not None
              and (invocation[1] > 1 or entry[3])):
            # An unmatched run can also belong to an invocation that already
            # exited. A test terminal accounts for only one observed run.
            current[test] = ["unknown", "unknown", entry[2] + 1, True]
        elif entry[0] != "unknown":
            entry[0] = action
    elif action in TERMINAL_ACTIONS and package in cases:
        current = cases[package]
        entry = current.get(test)
        if entry is not None and entry[2] > 1:
            entry[2] -= 1
        else:
            current.pop(test, None)
        if not current:
            del cases[package]


def update(path, old):
    with open(path, "rb") as stream:
        stat = os.fstat(stream.fileno())
        identity = [stat.st_dev, stat.st_ino]
        state = old if valid(old) else fresh(identity)
        if state.get("identity") != identity or stat.st_size < state["offset"]:
            state = fresh(identity)
        # Also detect same-inode truncate/regrow at the previous read boundary.
        stream.seek(max(0, state["offset"] - 64))
        if stream.read(min(64, state["offset"])).hex() != state["guard"]:
            state = fresh(identity)
        stream.seek(state["offset"])
        while True:
            line = stream.readline(MAX_LIFECYCLE_LINE_BYTES)
            if not line:
                break
            oversized = not line.endswith(b"\n") and len(line) == MAX_LIFECYCLE_LINE_BYTES
            while oversized and line and not line.endswith(b"\n"):
                line = stream.readline(MAX_LIFECYCLE_LINE_BYTES)
            if not line.endswith(b"\n"):
                break  # Never checkpoint a writer's incomplete final line.
            state["offset"] = stream.tell()
            if oversized or b'"Action":"output"' in line or b'"Action": "output"' in line:
                continue
            try:
                event = json.loads(line)
            except (ValueError, UnicodeError):
                continue
            if isinstance(event, dict):
                apply_event(state, event)
        stream.seek(max(0, state["offset"] - 64))
        state["guard"] = stream.read(min(64, state["offset"])).hex()
        return state


def escaped(value):
    # Match Go/AWK's single-line representation; never allow names to inject logs.
    return (json.dumps(value, ensure_ascii=False)[1:-1]
            .replace("&", r"\u0026").replace("<", r"\u003c").replace(">", r"\u003e")
            .replace("\u2028", r"\u2028").replace("\u2029", r"\u2029"))


def render(states, unfinished=()):
    lines = set()
    for state in states.values():
        for package, cases in state["cases"].items():
            for test, (action, when, _, ambiguous) in cases.items():
                prefix = ("unresolved UT case (overlapping invocations)"
                          if ambiguous else "active UT case")
                lines.add(prefix + ": package=%s test=%s state=%s started=%s" %
                          tuple(escaped(v) for v in (package, test, action, when)))
        for package, (when, _) in state["packages"].items():
            if package not in state["cases"]:
                lines.add("active UT package (no active case event): package=%s started=%s" %
                          (escaped(package), escaped(when)))
    for package, test, action, started, result in unfinished:
        lines.add("unfinished UT case at package exit: package=%s test=%s state=%s started=%s package_result=%s" %
                  tuple(escaped(v) for v in (package, test, action, started, result)))
    return sorted(lines) or ["no active or incomplete UT package/test case found"]


def atomic_write(path, data, prefix=".ut-report-"):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=os.path.dirname(path) or ".",
                                         prefix=prefix, delete=False) as stream:
            temporary = stream.name
            stream.write(data)
        os.replace(temporary, path)
    finally:
        if temporary is not None and os.path.exists(temporary):
            os.unlink(temporary)


def snapshot(state_path, paths):
    try:
        with open(state_path, encoding="utf-8") as stream:
            cache = json.load(stream)
        old = cache["reports"] if cache["version"] == STATE_VERSION and isinstance(cache["reports"], dict) else {}
    except (OSError, ValueError, KeyError, TypeError):
        old = {}
    states = {}
    for path in sorted(set(paths)):
        try:
            states[path] = update(path, old.get(path))
        except OSError:
            continue  # A joined helper's report may disappear between discovery/read.
    atomic_write(state_path, json.dumps({"version": STATE_VERSION, "reports": states}),
                 prefix=os.path.basename(state_path) + ".tmp.")
    return render(states)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("reports", nargs="*")
    arguments = parser.parse_args()
    print("\n".join(snapshot(arguments.state, arguments.reports)))
