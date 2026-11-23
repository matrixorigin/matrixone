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
from summarize_ut_slow_cases import summarize


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
        self.report.write_bytes(self.event("start", "") + self.event("run"))
        self.assertIn("TestOne", self.read()[0])
        with self.report.open("ab") as stream:
            stream.write(self.event("fail", ""))
        self.assertEqual(["no active or incomplete UT package/test case found"], self.read())
        second = self.report.with_suffix(".second")
        second.write_bytes(self.event("run"))
        self.assertEqual(self.read(second), self.read(self.report, second, second))
        second.unlink()
        self.assertEqual(self.read(), self.read(self.report, second))

        with self.report.open("ab") as stream:
            stream.write(self.event("start", "") + self.event("run"))
        self.assertIn("TestOne", self.read()[0])
        active_out = self.report.with_suffix(".active")
        summarize(self.report, active_out=active_out)
        lines = active_out.read_text().splitlines()
        self.assertEqual(2, len(lines))
        self.assertTrue(lines[0].startswith("active UT case: "))
        self.assertTrue(lines[1].startswith("unfinished UT case at package exit: "))
        self.assertTrue(lines[1].endswith("package_result=fail"))

    def test_overlapping_package_invocations(self):
        self.report.write_bytes(
            self.event("start", "") + self.event("run", "TestA")
            + self.event("start", "") + self.event("run", "TestB")
            + self.event("fail", "")
        )
        lines = self.read()
        self.assertEqual(2, len(lines))
        self.assertTrue(all(line.startswith("unresolved UT case") for line in lines))
        with self.report.open("ab") as stream:
            stream.write(self.event("pass", "TestB") + self.event("pass", ""))
        self.assertEqual(["no active or incomplete UT package/test case found"], self.read())
        active_out = self.report.with_suffix(".active")
        summarize(self.report, active_out=active_out)
        final = active_out.read_text().splitlines()
        self.assertEqual(1, len(final))
        self.assertIn("test=TestA", final[0])
        self.assertTrue(final[0].endswith("package_result=unknown"))

    def test_same_name_overlap_preserves_unmatched_run(self):
        for events in (
            self.event("run") + self.event("fail", ""),
            self.event("fail", "") + self.event("run"),
        ):
            with self.subTest(events=events):
                self.report.write_bytes(
                    self.event("start", "") + self.event("run") + self.event("start", "")
                    + events + self.event("pass")
                )
                self.assertIn("unresolved UT case", self.read()[0])
                with self.report.open("ab") as stream:
                    stream.write(self.event("pass", ""))
                active_out = self.report.with_suffix(".active")
                summarize(self.report, active_out=active_out)
                self.assertEqual(1, len(active_out.read_text().splitlines()))
                self.assertIn("test=TestOne state=unknown started=unknown package_result=unknown",
                              active_out.read_text())

    def test_final_package_exit_without_newline(self):
        self.report.write_bytes(
            self.event("start", "") + self.event("run") + self.event("fail", "")[:-1]
        )
        active_out = self.report.with_suffix(".active")
        _, packages, malformed = summarize(self.report, active_out=active_out)
        self.assertEqual(0, malformed)
        self.assertEqual("fail", packages[0][3])
        self.assertIn("unfinished UT case at package exit:", active_out.read_text())
        self.assertIn("test=TestOne", active_out.read_text())
        self.assertIn("package_result=fail", active_out.read_text())

        summarize(self.report, active_out=active_out, require_lifecycle_lf=True)
        self.assertEqual(self.read(), active_out.read_text().splitlines())
        self.assertTrue(self.read()[0].startswith("active UT case: "))
        with self.report.open("ab") as stream:
            stream.write(b"\n")
        self.assertEqual(["no active or incomplete UT package/test case found"], self.read())

    def test_final_lifecycle_size_matches_merged_size(self):
        terminal = self.event("fail", "")[:-1]
        prefix = self.event("start", "") + self.event("run")
        active_out = self.report.with_suffix(".active")
        for admitted in (True, False):
            payload_size = reader.MAX_LIFECYCLE_LINE_BYTES - int(admitted)
            event = terminal + b" " * (payload_size - len(terminal))
            for separator in (b"", b"\n"):
                with self.subTest(admitted=admitted, separator=separator):
                    self.report.write_bytes(prefix + event + separator)
                    summarize(self.report, active_out=active_out)
                    expected = "unfinished UT case at package exit:" if admitted else "active UT case:"
                    self.assertTrue(active_out.read_text().startswith(expected))

    def test_final_replay_ignores_corrupt_tail(self):
        prefix = self.event("start", "") + self.event("run")
        active_out = self.report.with_suffix(".active")
        for tail in (self.event("fail", "")[:-2], b"\xff"):
            with self.subTest(tail=tail):
                self.report.write_bytes(prefix + tail)
                _, packages, malformed = summarize(self.report, active_out=active_out)
                self.assertEqual([], packages)
                self.assertEqual(1, malformed)
                self.assertTrue(active_out.read_text().startswith("active UT case: "))

    def test_late_distinct_case_has_known_attribution(self):
        self.report.write_bytes(
            self.event("start", "") + self.event("run", "TestOld")
            + self.event("start", "") + self.event("fail", "")
            + self.event("run", "TestNew")
        )
        live, unresolved = self.read()
        self.assertIn("active UT case:", live)
        self.assertIn("test=TestNew", live)
        self.assertIn("unresolved UT case", unresolved)
        self.assertIn("test=TestOld", unresolved)
        with self.report.open("ab") as stream:
            stream.write(self.event("fail", ""))
        active_out = self.report.with_suffix(".active")
        summarize(self.report, active_out=active_out)
        first, second = active_out.read_text().splitlines()
        self.assertIn("test=TestNew", first)
        self.assertTrue(first.endswith("package_result=fail"))
        self.assertIn("test=TestOld", second)
        self.assertTrue(second.endswith("package_result=unknown"))

    def test_slow_summary_survives_lifecycle_publication_error(self):
        self.report.write_bytes(self.event("run") + self.event("pass"))
        script = Path(reader.__file__).with_name("summarize_ut_slow_cases.py")
        result = subprocess.run(
            [sys.executable, "-B", str(script), str(self.report), "--active-out",
             str(self.report / "cannot-create")], capture_output=True, text=True, check=True,
        )
        self.assertIn("package=example/test test=TestOne", result.stdout)
        self.assertIn("failed to publish UT lifecycle summary", result.stderr)


if __name__ == "__main__":
    unittest.main()
