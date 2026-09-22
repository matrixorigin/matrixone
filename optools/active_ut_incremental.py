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


def fresh(identity):
    return {"identity": identity, "offset": 0, "guard": "", "packages": {}, "cases": {}}


def valid(state):
    return (
        isinstance(state, dict)
        and isinstance(state.get("offset"), int)
        and state["offset"] >= 0
        and isinstance(state.get("guard"), str)
        and isinstance(state.get("packages"), dict)
        and all(isinstance(k, str) and isinstance(v, str) for k, v in state["packages"].items())
        and isinstance(state.get("cases"), dict)
        and all(isinstance(v, list) and len(v) == 4 and all(isinstance(s, str) for s in v)
                for v in state["cases"].values())
    )


def apply_event(state, event):
    action, package = event.get("Action", ""), event.get("Package", "")
    test, when = event.get("Test", ""), event.get("Time", "")
    if not all(isinstance(v, str) for v in (action, package, test, when)) or not package:
        return
    if not test:
        if action == "start":
            state["packages"][package] = when
        elif action in ("pass", "fail", "skip"):
            state["packages"].pop(package, None)
        # Keep unfinished cases on a package failure, as the AWK oracle does.
        return
    key = json.dumps([package, test])
    if action in ("run", "pause", "cont"):
        started = state["cases"].get(key, [package, test, action, when])[3]
        state["cases"][key] = [package, test, action, started]
    elif action in ("pass", "fail", "skip"):
        state["cases"].pop(key, None)


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
            line = stream.readline(1024 * 1024)
            if not line:
                break
            oversized = not line.endswith(b"\n") and len(line) == 1024 * 1024
            while oversized and line and not line.endswith(b"\n"):
                line = stream.readline(1024 * 1024)
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


def render(states):
    lines = set()
    for state in states.values():
        packages_with_cases = set()
        for package, test, action, when in state["cases"].values():
            packages_with_cases.add(package)
            lines.add("active UT case: package=%s test=%s state=%s started=%s" %
                      tuple(escaped(v) for v in (package, test, action, when)))
        for package, when in state["packages"].items():
            if package not in packages_with_cases:
                lines.add("active UT package (no active case event): package=%s started=%s" %
                          (escaped(package), escaped(when)))
    return sorted(lines) or ["no active or incomplete UT package/test case found"]


def snapshot(state_path, paths):
    try:
        with open(state_path, encoding="utf-8") as stream:
            cache = json.load(stream)
        old = cache["reports"] if cache["version"] == 1 and isinstance(cache["reports"], dict) else {}
    except (OSError, ValueError, KeyError, TypeError):
        old = {}
    states = {}
    for path in sorted(set(paths)):
        try:
            states[path] = update(path, old.get(path))
        except OSError:
            continue  # A joined helper's report may disappear between discovery/read.
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=os.path.dirname(state_path) or ".",
                                         prefix=".active-ut-", delete=False) as stream:
            temporary = stream.name
            json.dump({"version": 1, "reports": states}, stream)
        os.replace(temporary, state_path)
    finally:
        if temporary is not None and os.path.exists(temporary):
            os.unlink(temporary)
    return render(states)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", required=True)
    parser.add_argument("reports", nargs="*")
    arguments = parser.parse_args()
    print("\n".join(snapshot(arguments.state, arguments.reports)))
