#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("upgrade-4.2-preflight.sh").resolve()


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def snapshot(child, account_name="sys"):
    return {
        "catalog_account_id": "0",
        "snapshot_id": "01a17d34-05f1-752c-acbd-52c7a8fe04fe",
        "name": "__mo_branch_" + child,
        "timestamp": "123",
        "level": "table",
        "account_name": account_name,
        "database_name": None,
        "table_name": "tab\tline\n雪",
        "object_id": "9",
        "prior_kind": "user",
        "edge": None,
        "decision": None,
    }


def edge(child):
    return {
        "table_id": child,
        "parent_id": "9",
        "clone_ts": "123",
        "creator": "0",
        "level": "table",
        "table_deleted": False,
    }


class PreflightTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = self.root / "mysql.tsv"
        self.output = self.root / "decisions.ndjson"
        mysql = self.root / "mysql"
        mysql.write_text(
            '#!/bin/sh\n'
            'if [ "${TEST_MYSQL_BLOCK:-0}" = 1 ]; then\n'
            '  eval "printf \'%s\\n\' $$ >&${TEST_READY_FD}"\n'
            '  exec sleep 60\n'
            'fi\n'
            'cat "$TEST_MYSQL_DATA"\nexit "${TEST_MYSQL_EXIT:-0}"\n'
        )
        mysql.chmod(0o700)
        self.env = os.environ.copy()
        self.env["PATH"] = str(self.root) + os.pathsep + self.env["PATH"]
        self.env["TMPDIR"] = str(self.root)
        self.env["TEST_MYSQL_DATA"] = str(self.data)

    def run_export(self, lines, exit_code=0, newline=True):
        self.data.write_text("\n".join(lines) + ("\n" if newline else ""))
        self.env["TEST_MYSQL_EXIT"] = str(exit_code)
        return subprocess.run(
            [str(SCRIPT), str(self.output), "--defaults-extra-file=protected.cnf"],
            env=self.env, text=True, capture_output=True, check=False,
        )

    def test_merges_canonical_ids_without_guessing(self):
        low, high = "18446744073709551614", "18446744073709551615"
        values = [
            "A\t0\t{}", "C\t3\t{}",
            "S\t" + high + "\t" + encoded(snapshot(high, "sys\\name")),
            "E\t" + low + "\t" + encoded(edge(low)),
            "S\t0\t" + encoded(snapshot("0")),
            "S\t" + low + "\t" + encoded(snapshot(low)),
        ]
        result = self.run_export(values)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("CDC tasks: 3", result.stderr)
        self.assertEqual(0o600, self.output.stat().st_mode & 0o777)
        records = [json.loads(line) for line in self.output.read_text().splitlines()]
        self.assertEqual({"format": "matrixone-catalog-snapshot-decisions-v1", "target": "4.0.7"}, records[0])
        self.assertEqual(["__mo_branch_0", "__mo_branch_" + low, "__mo_branch_" + high], [r["name"] for r in records[1:]])
        self.assertEqual(edge(low), records[2]["edge"])
        self.assertIsNone(records[1]["edge"])
        self.assertIsNone(records[3]["edge"])
        self.assertTrue(all(r["decision"] is None for r in records[1:]))
        self.assertEqual("tab\tline\n雪", records[2]["table_name"])
        self.assertEqual("sys\\name", records[3]["account_name"])

    def test_failures_never_publish_or_replace(self):
        good = ["A\t0\t{}", "C\t0\t{}", "S\t42\t" + encoded(snapshot("42"))]
        cases = {
            "mysql failure": (good, 1, True),
            "non-system account": (["A\t7\t{}", *good[1:]], 0, True),
            "unexpected kind": ([*good, "X\t1\t{}"], 0, True),
            "duplicate snapshot": ([*good, good[-1]], 0, True),
            "duplicate edge": ([*good, "E\t42\t" + encoded(edge("42")), "E\t42\t" + encoded(edge("42"))], 0, True),
            "malformed json": ([*good[:2], "S\t42\t{"], 0, True),
            "mismatched edge": ([*good, "E\t42\t" + encoded(edge("43"))], 0, True),
            "oversized row": ([*good[:2], "S\t42\t" + "x" * 262144], 0, True),
            "truncated row": (good, 0, False),
        }
        for name, (lines, exit_code, newline) in cases.items():
            with self.subTest(name=name):
                result = self.run_export(lines, exit_code, newline)
                self.assertNotEqual(0, result.returncode, result.stderr)
                self.assertFalse(self.output.exists())
        self.output.write_text("keep")
        result = self.run_export(good)
        self.assertNotEqual(0, result.returncode)
        self.assertEqual("keep", self.output.read_text())

    def test_cancellation_joins_mysql_and_does_not_publish(self):
        read_fd, write_fd = os.pipe()
        self.addCleanup(os.close, read_fd)
        self.env["TEST_MYSQL_BLOCK"] = "1"
        self.env["TEST_READY_FD"] = str(write_fd)
        proc = subprocess.Popen(
            [str(SCRIPT), str(self.output), "--defaults-extra-file=protected.cnf"],
            env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            pass_fds=(write_fd,), start_new_session=True,
        )
        os.close(write_fd)
        try:
            self.assertTrue(select.select([read_fd], [], [], 5)[0], "mysql did not start")
            child_pid = int(os.read(read_fd, 100).strip())
            proc.terminate()
            proc.communicate(timeout=5)
            self.assertNotEqual(0, proc.returncode)
            self.assertFalse(self.output.exists())
            with self.assertRaises(ProcessLookupError):
                os.kill(child_pid, 0)
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.communicate()

    def test_cancellation_after_publication_keeps_complete_file(self):
        fake_ln = self.root / "ln"
        fake_ln.write_text('#!/bin/sh\n/usr/bin/ln "$@" || exit\nkill -TERM "$PPID"\n')
        fake_ln.chmod(0o700)
        rows = ["A\t0\t{}", "C\t0\t{}", "S\t42\t" + encoded(snapshot("42"))]
        result = self.run_export(rows)
        self.assertNotEqual(0, result.returncode)
        records = [json.loads(line) for line in self.output.read_text().splitlines()]
        self.assertEqual(2, len(records))
        self.assertEqual("__mo_branch_42", records[1]["name"])
        self.assertIsNone(records[1]["decision"])


if __name__ == "__main__":
    unittest.main()
