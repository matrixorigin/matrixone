#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Focused tests for the opt-in Sirius-to-MO Pixi upgrade helper."""

from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import sync_sirius_mo


SIRIUS = """[feature.mo.dependencies]
cuda-version = "13.3.*"
libcuvs = "==26.08.01"
librmm = "26.08.*"
gcc_linux-64 = "14.*"
gxx_linux-64 = "14.*"
patchelf = "*"
"""

MO = """[workspace]
platforms = [{ name = "linux-64-cuda13", platform = "linux-64", cuda = "13.3" }]

[dependencies]
cuda-version = "13.3.*"
libcuvs = "==26.08.01"
librmm = "26.08.*"
gcc_linux-64 = "14.*"
gxx_linux-64 = "14.*"
patchelf = "*"
cuda-nvcc = "13.3.*"
libraft = "26.08.*"
"""


class SiriusToolchainSyncTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="mo-sirius-sync-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.sirius = self.root / "sirius.toml"
        self.mo = self.root / "mo.toml"
        self.sirius.write_text(SIRIUS)
        self.mo.write_text(MO)

    def test_matching_profiles_only_check_sirius_lock(self):
        with patch.object(sync_sirius_mo.subprocess, "run") as run:
            status = sync_sirius_mo.main(
                ["--sirius-manifest", str(self.sirius), "--check"], self.mo
            )
        self.assertEqual(status, 0)
        run.assert_called_once_with(
            ["pixi", "lock", "--check", "--offline", "--manifest-path",
             str(self.sirius)],
            check=True,
        )

    def test_check_reports_cuda_and_cuvs_upgrade_without_writes(self):
        self.sirius.write_text(
            SIRIUS.replace("13.3.*", "13.4.*").replace("26.08.01", "26.10.02")
        )
        with patch.object(sync_sirius_mo.subprocess, "run") as run:
            status = sync_sirius_mo.main(
                ["--sirius-manifest", str(self.sirius), "--check"], self.mo
            )
        self.assertEqual(status, 1)
        run.assert_called_once_with(
            ["pixi", "lock", "--check", "--offline", "--manifest-path",
             str(self.sirius)],
            check=True,
        )
        self.assertEqual(self.mo.read_text(), MO)

    def test_update_asks_pixi_to_resolve_mo_lock_once(self):
        self.sirius.write_text(SIRIUS.replace("26.08.01", "26.10.02"))
        with patch.object(sync_sirius_mo.subprocess, "run") as run:
            status = sync_sirius_mo.main(
                ["--sirius-manifest", str(self.sirius)], self.mo
            )
        self.assertEqual(status, 0)
        self.assertEqual(run.call_count, 2)
        run.assert_any_call(
            ["pixi", "lock", "--check", "--offline", "--manifest-path",
             str(self.sirius)],
            check=True,
        )
        run.assert_any_call(
            ["pixi", "add", "--no-install", "--manifest-path", str(self.mo),
             "libcuvs==26.10.02", "libraft=26.10.*"],
            check=True,
        )

    def test_cuda_upgrade_requires_explicit_virtual_platform_edit(self):
        self.sirius.write_text(SIRIUS.replace("13.3.*", "13.4.*"))
        with patch.object(sync_sirius_mo.subprocess, "run") as run:
            status = sync_sirius_mo.main(
                ["--sirius-manifest", str(self.sirius)], self.mo
            )
        self.assertEqual(status, 2)
        self.assertEqual(run.call_count, 1)
        self.assertEqual(self.mo.read_text(), MO)

    def test_missing_shared_dependency_fails_closed(self):
        self.sirius.write_text(SIRIUS.replace('libcuvs = "==26.08.01"\n', ""))
        with self.assertRaisesRegex(ValueError, "missing shared MO dependencies"):
            sync_sirius_mo.desired_constraints(self.sirius)


if __name__ == "__main__":
    unittest.main()
