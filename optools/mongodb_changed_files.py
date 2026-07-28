#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import argparse
import fnmatch
import json
import subprocess
import sys

PATTERNS = (
    ".github/workflows/mongodb-connector.yml", "Makefile", "go.mod", "go.sum",
    "optools/mongodb_ci.bash", "optools/mongodb_changed_files.py",
    "etc/launch-mongodb-local/**", "etc/launch/**", "test/mongodb/**", "pkg/sql/mongodb/**",
    "pkg/sql/colexec/mongoscan/**", "pkg/sql/colexec/timewin/**",
    "pkg/sql/colexec/aggexec/**", "pkg/sql/plan/**", "pkg/sql/compile/**",
    "pkg/frontend/**", "pkg/sql/parsers/**", "pkg/config/**", "pkg/bootstrap/**",
    "pkg/cnservice/**", "pkg/vm/**", "pkg/pb/plan/**", "pkg/pb/pipeline/**",
    "pkg/util/metric/v2/**",
    "proto/plan.proto", "proto/pipeline.proto", "vendor/go.mongodb.org/**",
    "vendor/github.com/xdg-go/**", "vendor/github.com/youmark/pkcs8/**",
    "vendor/github.com/montanaflynn/stats/**", "vendor/github.com/klauspost/compress/**",
    "vendor/github.com/golang/snappy/**",
    "vendor/modules.txt",
)


def relevant(paths):
    return any(fnmatch.fnmatch(path, pattern) for path in paths for pattern in PATTERNS)


def event_range(event):
    name = event.get("event_name")
    payload = event.get("payload", event)
    if name == "pull_request" or "pull_request" in payload:
        pr = payload["pull_request"]
        return pr["base"]["sha"], pr["head"]["sha"]
    if name == "merge_group" or "merge_group" in payload:
        group = payload["merge_group"]
        return group["base_sha"], group["head_sha"]
    if name in ("schedule", "workflow_dispatch"):
        return None
    raise ValueError("unsupported or incomplete event payload")


def changed_files(event):
    if "changed_files" in event:
        return event["changed_files"]
    revision_range = event_range(event)
    if revision_range is None:
        return ["pkg/sql/mongodb/nightly"]
    base, head = revision_range
    if not base or not head:
        raise ValueError("event is missing base/head SHA")
    for revision in (base, head):
        present = subprocess.run(
            ["git", "cat-file", "-e", "%s^{commit}" % revision],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        ).returncode == 0
        if not present:
            subprocess.run(
                ["git", "fetch", "--no-tags", "origin", revision], check=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
    output = subprocess.check_output(["git", "diff", "--name-only", base, head], text=True)
    return output.splitlines()


def detect_relevance(event):
    try:
        return relevant(changed_files(event))
    except (OSError, KeyError, ValueError, subprocess.SubprocessError) as error:  # fail-safe
        print("MongoDB change detection failed; running E2E: %s" % error, file=sys.stderr)
        return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--event", required=True)
    parser.add_argument("--github-output")
    args = parser.parse_args()
    try:
        with open(args.event, encoding="utf-8") as source:
            event = json.load(source)
    except (OSError, ValueError) as error:
        print("MongoDB event loading failed; running E2E: %s" % error, file=sys.stderr)
        event = {}
    value = detect_relevance(event)
    line = "relevant=%s\n" % str(value).lower()
    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as target:
            target.write(line)
    else:
        sys.stdout.write(line)


if __name__ == "__main__":
    main()
