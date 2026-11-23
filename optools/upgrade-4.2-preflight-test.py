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
            'if [ "${TEST_MYSQL_FORCE_FAILURE:-0}" = 1 ]; then\n'
            '  printf "A\\t0\\t{}\\nC\\t0\\t{}\\n"\n'
            '  for last do :; done\n'
            '  if [ "$last" = --skip-force ]; then exit 1; fi\n'
            '  printf "Z\\tdone\\t{}\\n"\nexit 0\n'
            'fi\n'
            'cat "$TEST_MYSQL_DATA"\nexit "${TEST_MYSQL_EXIT:-0}"\n'
        )
        mysql.chmod(0o700)
        self.env = os.environ.copy()
        self.env["PATH"] = str(self.root) + os.pathsep + self.env["PATH"]
        self.env["TMPDIR"] = str(self.root)
        self.env["TEST_MYSQL_DATA"] = str(self.data)

    def run_export(self, lines, exit_code=0, newline=True, terminal=True):
        rows = [*lines, "Z\tdone\t{}"] if terminal else lines
        self.data.write_text("\n".join(rows) + ("\n" if newline else ""))
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

    def test_exports_more_than_one_startup_page(self):
        values = ["A\t0\t{}", "C\t0\t{}"]
        for i in range(129):
            child = str(i)
            row = snapshot(child)
            row["snapshot_id"] = f"00000000-0000-4000-8000-{i:012x}"
            values.append("S\t" + child + "\t" + encoded(row))
            if i % 2:
                values.append("E\t" + child + "\t" + encoded(edge(child)))
        result = self.run_export(values)
        self.assertEqual(0, result.returncode, result.stderr)
        records = [json.loads(line) for line in self.output.read_text().splitlines()][1:]
        self.assertEqual(sorted("__mo_branch_" + str(i) for i in range(129)), [r["name"] for r in records])
        for record in records:
            child = record["name"].removeprefix("__mo_branch_")
            self.assertEqual(edge(child) if int(child) % 2 else None, record["edge"])
            self.assertIsNone(record["decision"])

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
        result = self.run_export(good[:2], terminal=False)
        self.assertNotEqual(0, result.returncode, "complete-line truncation must be rejected")
        self.assertFalse(self.output.exists())
        self.output.write_text("keep")
        result = self.run_export(good)
        self.assertNotEqual(0, result.returncode)
        self.assertEqual("keep", self.output.read_text())

    def test_mysql_force_cannot_hide_query_failure(self):
        self.env["TEST_MYSQL_FORCE_FAILURE"] = "1"
        result = self.run_export([])
        self.assertNotEqual(0, result.returncode)
        self.assertFalse(self.output.exists())

    def test_existing_directory_is_not_an_output_file(self):
        self.output.mkdir()
        result = self.run_export(["A\t0\t{}", "C\t0\t{}"])
        self.assertNotEqual(0, result.returncode)
        self.assertEqual([], list(self.output.iterdir()))

    def assert_cancellation_joins_child(self):
        read_fd, write_fd = os.pipe()
        self.addCleanup(os.close, read_fd)
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

    def test_cancellation_joins_mysql_and_does_not_publish(self):
        self.env["TEST_MYSQL_BLOCK"] = "1"
        self.assert_cancellation_joins_child()

    def test_cancellation_joins_count_and_publish_commands(self):
        self.data.write_text("A\t0\t{}\nC\t0\t{}\nZ\tdone\t{}\n")
        for command in ("wc", "ln"):
            with self.subTest(command=command):
                fake = self.root / command
                fake.write_text(
                    '#!/bin/sh\n'
                    'eval "printf \'%s\\n\' $$ >&${TEST_READY_FD}"\n'
                    'exec sleep 60\n'
                )
                fake.chmod(0o700)
                try:
                    self.assert_cancellation_joins_child()
                finally:
                    fake.unlink()

    def test_cancellation_at_launch_boundaries(self):
        self.data.write_text("A\t0\t{}\nC\t0\t{}\nZ\tdone\t{}\n")
        envfile = self.root / "debug-env"
        envfile.write_text('''set -T
_inject() {
    [[ "$2" == "$TEST_COMMAND "* ]] || return 0
    if [[ "$TEST_PHASE" == before && "$1" == '"$@" 0<&0' ]]; then
        injected=1
        printf 'hit\\n' > "$TEST_HIT"
        kill "-$TEST_SIGNAL" "$$"
    elif [[ "$TEST_PHASE" == before && "$1" == 'child=$!' && ${injected:-0} == 1 ]]; then
        # A broken handler may resume and launch ln despite the signal. Wait
        # for that incorrect launch to make any publication observable.
        wait "$!" || :
    elif [[ "$TEST_PHASE" == after && "$1" == 'child=$!' ]]; then
        trap - DEBUG
        printf '%s\\n' "$!" > "$TEST_CHILD"
        printf 'hit\\n' > "$TEST_HIT"
        kill "-$TEST_SIGNAL" "$$"
    fi
}
trap '_inject "$BASH_COMMAND" "$*"' DEBUG
''')
        self.env["BASH_ENV"] = str(envfile)
        fake_ln = self.root / "ln"
        for phase, command in (("before", "ln"), ("after", "ln"), ("after", "mysql")):
            for sig in (signal.SIGHUP, signal.SIGINT, signal.SIGTERM):
                with self.subTest(phase=phase, command=command, signal=sig.name):
                    hit = self.root / f"hit-{phase}-{command}-{sig.name}"
                    child_file = self.root / f"child-{phase}-{command}-{sig.name}"
                    started = self.root / f"ln-started-{phase}-{sig.name}"
                    if phase == "before":
                        fake_ln.write_text('#!/bin/sh\nprintf "started\\n" > "$TEST_STARTED"\nexec /usr/bin/ln "$@"\n')
                    else:
                        fake_ln.write_text('#!/bin/sh\nexec sleep 60\n')
                    fake_ln.chmod(0o700)
                    self.env.update({
                        "TEST_PHASE": phase, "TEST_COMMAND": command,
                        "TEST_SIGNAL": sig.name.removeprefix("SIG"),
                        "TEST_HIT": str(hit), "TEST_CHILD": str(child_file),
                        "TEST_STARTED": str(started),
                    })
                    proc = subprocess.Popen(
                        [str(SCRIPT), str(self.output), "--defaults-extra-file=protected.cnf"],
                        env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                        start_new_session=True,
                    )
                    try:
                        _, stderr = proc.communicate(timeout=5)
                        self.assertTrue(hit.exists(), stderr.decode())
                        self.assertEqual(128 + sig, proc.returncode, stderr.decode())
                        self.assertFalse(self.output.exists())
                        self.assertFalse(started.exists(), "ln started after cancellation")
                        self.assertEqual([], list(self.root.glob(".mo-upgrade-decisions.*")))
                        self.assertEqual([], list(self.root.glob("mo-upgrade-preflight.*")))
                        if phase == "after":
                            child_pid = int(child_file.read_text())
                            with self.assertRaises(ProcessLookupError):
                                os.kill(child_pid, 0)
                    finally:
                        try:
                            os.killpg(proc.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        if proc.poll() is None:
                            proc.communicate()

    def test_reentrant_signal_does_not_skip_cleanup(self):
        envfile = self.root / "reentrant-debug-env"
        envfile.write_text('''set -T
_inject() {
    if [[ "$1" == 'wait "$child"' && "$2" == 143 ]]; then
        trap - DEBUG
        printf 'hit\\n' > "$TEST_NESTED"
        kill -HUP "$$"
    fi
}
trap '_inject "$BASH_COMMAND" "$*"' DEBUG
''')
        hit = self.root / "reentrant-hit"
        read_fd, write_fd = os.pipe()
        self.addCleanup(os.close, read_fd)
        self.env.update({
            "BASH_ENV": str(envfile), "TEST_NESTED": str(hit),
            "TEST_MYSQL_BLOCK": "1", "TEST_READY_FD": str(write_fd),
        })
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
            _, stderr = proc.communicate(timeout=5)
            self.assertTrue(hit.exists(), "second signal was not injected")
            self.assertIn(proc.returncode, (129, 143), stderr.decode())
            self.assertFalse(self.output.exists())
            self.assertEqual([], list(self.root.glob(".mo-upgrade-decisions.*")))
            self.assertEqual([], list(self.root.glob("mo-upgrade-preflight.*")))
            with self.assertRaises(ProcessLookupError):
                os.kill(child_pid, 0)
        finally:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            if proc.poll() is None:
                proc.communicate()

    def test_repeated_signal_does_not_interrupt_cleanup(self):
        mysql = self.root / "mysql"
        mysql.write_text('''#!/usr/bin/env python3
import os, signal, sys
fd = int(os.environ["TEST_READY_FD"])
signal.signal(signal.SIGTERM, lambda *_: os.write(fd, b"term\\n"))
signal.signal(signal.SIGUSR1, lambda *_: sys.exit(0))
os.write(fd, (str(os.getpid()) + "\\n").encode())
while True:
    signal.pause()
''')
        mysql.chmod(0o700)
        read_fd, write_fd = os.pipe()
        self.addCleanup(os.close, read_fd)
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
            self.assertTrue(select.select([read_fd], [], [], 5)[0], "cleanup did not signal mysql")
            self.assertEqual(b"term\n", os.read(read_fd, 100))
            proc.send_signal(signal.SIGHUP)
            os.kill(child_pid, signal.SIGUSR1)
            proc.communicate(timeout=5)
            self.assertEqual(143, proc.returncode)
            self.assertFalse(self.output.exists())
            with self.assertRaises(ProcessLookupError):
                os.kill(child_pid, 0)
        finally:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            if proc.poll() is None:
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
