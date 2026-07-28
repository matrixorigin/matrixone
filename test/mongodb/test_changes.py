# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import importlib.util
import json
import pathlib
import subprocess
import unittest
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("mongodb_changed_files", ROOT / "optools/mongodb_changed_files.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
EVENTS = ROOT / "test/mongodb/events"


def load_event(name):
    with (EVENTS / name).open(encoding="utf-8") as source:
        return json.load(source)


class MongoDBChangesTest(unittest.TestCase):
    def test_relevant_and_unrelated_paths(self):
        self.assertTrue(MODULE.relevant(["pkg/sql/mongodb/driver.go"]))
        self.assertTrue(MODULE.relevant(["proto/plan.proto"]))
        self.assertTrue(MODULE.relevant(["vendor/github.com/xdg-go/scram/scram.go"]))
        self.assertTrue(MODULE.relevant(["etc/launch/cn.toml"]))
        self.assertTrue(MODULE.relevant(["pkg/util/metric/v2/mongodb.go"]))
        self.assertFalse(MODULE.relevant(["docs/README.md"]))

    def test_pull_request_range(self):
        event = load_event("pull_request.json")
        self.assertEqual(
            ("1111111111111111111111111111111111111111", "2222222222222222222222222222222222222222"),
            MODULE.event_range(event),
        )

    def test_merge_group_range(self):
        event = load_event("merge_group.json")
        self.assertEqual(
            ("3333333333333333333333333333333333333333", "4444444444444444444444444444444444444444"),
            MODULE.event_range(event),
        )

    def test_incomplete_event_fails(self):
        with self.assertRaises((KeyError, ValueError)):
            MODULE.event_range({"event_name": "merge_group", "merge_group": {}})

    def test_detection_failure_runs_e2e(self):
        with mock.patch.object(MODULE, "changed_files", side_effect=subprocess.CalledProcessError(1, "git")):
            self.assertTrue(MODULE.detect_relevance({"event_name": "pull_request"}))

    def test_detection_uses_complete_merge_group_diff(self):
        event = {
            "event_name": "merge_group",
            "payload": {"merge_group": {"base_sha": "base", "head_sha": "head"}},
        }
        with mock.patch.object(MODULE, "changed_files", return_value=["pkg/sql/mongodb/driver.go"]):
            self.assertTrue(MODULE.detect_relevance(event))


if __name__ == "__main__":
    unittest.main()
