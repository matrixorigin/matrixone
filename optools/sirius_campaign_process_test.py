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


"""Live campaign executor process ownership tests."""

import os
from pathlib import Path
import select
import sys
import tempfile
import unittest
from unittest import mock

import sirius_campaign_v2 as campaign
from sirius_campaign_v2 import common


class CampaignProcessTest(unittest.TestCase):
    @unittest.skipUnless(
        sys.platform == "linux"
        and hasattr(os, "pidfd_open")
        and hasattr(common.signal, "pidfd_send_signal"),
        "detached-descendant proof requires Linux subreapers and pidfds",
    )
    def test_subprocess_timeout_kills_descendants(self):
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            child_pid = directory_path / "child.pid"
            runner = directory_path / "runner.py"
            runner.write_text(
                """
import pathlib
import signal
import subprocess
import sys

child = subprocess.Popen([
    sys.executable,
    "-c",
    "import os, signal; os.setsid(); signal.pause()",
])
pathlib.Path(sys.argv[1]).write_text(str(child.pid), encoding="utf-8")
signal.pause()
""",
                encoding="utf-8",
            )
            executor = campaign.BoundedExecutor(
                [sys.executable, str(runner), str(child_pid)],
                timeout_seconds=2,
            )
            with self.assertRaisesRegex(campaign.CampaignError, "runner deadline exceeded"):
                executor({"sequence": 1})
            pid = int(child_pid.read_text(encoding="utf-8"))
            try:
                descriptor = os.pidfd_open(pid)
            except ProcessLookupError:
                return
            try:
                poller = select.poll()
                poller.register(descriptor, select.POLLIN)
                self.assertTrue(poller.poll(1000), "runner descendant survived timeout cleanup")
            finally:
                os.close(descriptor)

    @unittest.skipUnless(
        sys.platform == "linux"
        and hasattr(os, "pidfd_open")
        and hasattr(common.signal, "pidfd_send_signal"),
        "detached-descendant proof requires Linux subreapers and pidfds",
    )
    def test_successful_runner_cannot_leave_detached_descendant(self):
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            child_pid = directory_path / "child.pid"
            runner = directory_path / "runner.py"
            runner.write_text(
                """
import pathlib
import subprocess
import sys

child = subprocess.Popen(
    [sys.executable, "-c", "import os, signal; os.setsid(); signal.pause()"],
    stdin=subprocess.DEVNULL,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    close_fds=True,
)
pathlib.Path(sys.argv[1]).write_text(str(child.pid), encoding="utf-8")
print("{}")
""",
                encoding="utf-8",
            )
            executor = campaign.BoundedExecutor(
                [sys.executable, str(runner), str(child_pid)],
                timeout_seconds=2,
            )
            self.assertEqual({}, executor({"sequence": 1}))
            pid = int(child_pid.read_text(encoding="utf-8"))
            with self.assertRaises(ProcessLookupError):
                os.pidfd_open(pid)

    @unittest.skipUnless(sys.platform == "linux", "exclusive child ownership requires procfs")
    def test_subprocess_rejects_preexisting_child(self):
        child = common.subprocess.Popen(
            [sys.executable, "-c", "import signal; signal.pause()"]
        )
        self.addCleanup(child.wait)
        self.addCleanup(lambda: child.poll() is None and child.kill())
        executor = campaign.BoundedExecutor(["runner"], timeout_seconds=1)
        with self.assertRaisesRegex(campaign.CampaignError, "exclusive subprocess ownership"):
            executor({"sequence": 1})


    def test_cancellation_kills_and_joins_the_runner(self):
        previous = common._get_child_subreaper()
        executor = campaign.BoundedExecutor(
            [sys.executable, "-c", "import signal; signal.pause()"], 5
        )
        with (
            mock.patch.object(campaign.selectors.DefaultSelector, "select", side_effect=KeyboardInterrupt),
            mock.patch.object(common, "_cleanup_owned_processes", wraps=common._cleanup_owned_processes) as cleanup,
            self.assertRaises(KeyboardInterrupt),
        ):
            executor({})
        cleanup.assert_called_once()
        self.assertFalse(common._direct_child_pids())
        self.assertEqual(previous, common._get_child_subreaper())

    def test_failed_cleanup_poisons_future_admission(self):
        previous = common._get_child_subreaper()
        self.addCleanup(common._set_child_subreaper, previous)
        cleanup = common._cleanup_owned_processes
        def fail_after_cleanup(process, close_pipes):
            cleanup(process, close_pipes)
            raise campaign.CampaignError("cleanup deadline")
        executor = campaign.BoundedExecutor([sys.executable, "-c", "print('{}')"], 5)
        with (
            mock.patch.object(common, "_cleanup_owned_processes", side_effect=fail_after_cleanup),
            self.assertRaisesRegex(campaign.CampaignError, "cleanup deadline"),
        ):
            executor({})
        self.assertTrue(executor.poisoned)
        self.assertFalse(common._direct_child_pids())
        with self.assertRaisesRegex(campaign.CampaignError, "poisoned"):
            executor({})
