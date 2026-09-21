#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import active_ut_incremental as reader


class IncrementalReaderTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.report = Path(self.directory.name) / "report.json"
        self.state = Path(self.directory.name) / "state.json"

    def event(self, action, test="TestOne", package="example/test"):
        value = {"Time": "2026-09-19T00:00:00Z", "Action": action, "Package": package}
        if test:
            value["Test"] = test
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode() + b"\n"

    def read(self, *reports):
        return reader.snapshot(str(self.state), [str(p) for p in reports or [self.report]])

    def oracle(self):
        output = subprocess.check_output([
            "awk", "-f", str(Path(reader.__file__).with_name("active_ut_cases.awk")), str(self.report)
        ], text=True)
        self.assertEqual(sorted(set(output.splitlines())), self.read())

    def test_append_pause_terminal_and_no_rescan(self):
        self.report.write_bytes(self.event("start", "") + self.event("run"))
        self.oracle()
        with mock.patch.object(reader, "apply_event", side_effect=AssertionError("rescanned history")):
            self.assertIn("TestOne", self.read()[0])
        for action in ("pause", "cont", "pass"):
            with self.report.open("ab") as stream:
                stream.write(self.event(action))
            self.oracle()
        with self.report.open("ab") as stream:
            stream.write(self.event("pass", ""))
        self.assertEqual(["no active or incomplete UT package/test case found"], self.read())

    def test_partial_line_and_oversized_output(self):
        line = self.event("run")
        self.report.write_bytes(line[:-3])
        self.assertEqual(["no active or incomplete UT package/test case found"], self.read())
        with self.report.open("ab") as stream:
            stream.write(line[-3:])
            stream.write(b'{"Action":"output","Output":"' + b"x" * (2 * 1024 * 1024) + b'"}\n')
        self.oracle()
        with self.report.open("ab") as stream:
            stream.write(self.event("pass"))
        self.oracle()

    def test_truncate_regrow_replace_and_remove(self):
        self.report.write_bytes(self.event("run", "TestOld"))
        self.read()
        # Regrow past the previous offset without changing the inode.
        self.report.write_bytes(self.event("run", "TestNewLongerName"))
        self.oracle()
        self.report.write_bytes(b"")
        self.assertNotIn("TestNew", "\n".join(self.read()))
        replacement = self.report.with_suffix(".new")
        replacement.write_bytes(self.event("run", "TestReplacement"))
        os.replace(replacement, self.report)
        self.oracle()
        self.report.unlink()
        self.assertEqual(["no active or incomplete UT package/test case found"], self.read())
        self.assertEqual({}, json.loads(self.state.read_text())["reports"])

    def test_corrupt_state_and_escaped_names(self):
        self.report.write_bytes(self.event("run", 'TestQuote/"/\\/\n/中文'))
        for corrupt in ("not JSON", "null", "[]", '{"version":1,"reports":null}'):
            self.state.write_text(corrupt)
            self.oracle()
        self.assertEqual(1, len(self.read()[0].splitlines()))

    def test_multiple_reports_and_package_failure(self):
        self.report.write_bytes(self.event("run") + self.event("fail", ""))
        second = self.report.with_suffix(".second")
        second.write_bytes(self.event("run"))
        self.assertEqual(self.read(), self.read(self.report, second, second))
        self.oracle()  # unfinished case must survive the package failure
        second.unlink()
        self.assertEqual(self.read(), self.read(self.report, second))


if __name__ == "__main__":
    unittest.main()
