"""Offline failure-path checks for the bounded Linux probe wrapper."""

import json
from pathlib import Path
import tempfile
import tarfile
import unittest
from unittest.mock import patch

import native_rebase_probe_linux as probe


class ProbeWrapperTests(unittest.TestCase):
    def test_git_archive_root_directory_is_accepted(self):
        root = tarfile.TarInfo("source")
        root.type = tarfile.DIRTYPE
        probe.validate_source_members([root, tarfile.TarInfo("source/go.mod")])

    def test_archive_escape_is_rejected(self):
        for name in ("manifest.json", "source/../manifest.json", "/source/go.mod"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "source archive"):
                probe.validate_source_members([tarfile.TarInfo(name)])

    def test_wrong_root_rejected_before_lease_mutation(self):
        with self.assertRaisesRegex(ValueError, "unexpected root"):
            probe.reserve(Path("/tmp/not-an-owned-root"))

    def test_wrong_base_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve() / "owned"
            root.mkdir()
            (root / "manifest.json").write_text(json.dumps({"base": "wrong", "inputs": {}}))
            with patch.object(probe, "PARENT", root.parent):
                with self.assertRaisesRegex(ValueError, "base changed"):
                    probe.verify(root)

    def test_missing_test_result_cannot_pass_or_retain_owned_lease(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "owned"
            lock = Path(tmp) / "lock"
            root.mkdir()
            lock.mkdir()
            identity = {"unit": "owned-worker"}
            probe.save(lock / "owner.json", identity)
            state = {"MainPID": "0", "ActiveState": "inactive", "Result": "success", "ExecMainStatus": "0"}
            with patch.object(probe, "verify"), patch.object(probe, "owned", return_value=identity), \
                    patch.object(probe, "LOCK", lock), patch.object(probe, "props", return_value=state), \
                    patch.object(probe, "available", return_value=30 * probe.GIB), patch.object(probe, "call"):
                with self.assertRaisesRegex(RuntimeError, "P1 probe failed"):
                    probe.supervise(root)
            result = json.loads((root / "result.json").read_text())
            self.assertEqual(result["status"], "FAIL")
            self.assertTrue(result["released"])
            self.assertFalse(lock.exists())

    def test_active_worker_never_releases_lease(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            state = {"MainPID": "42", "ActiveState": "active"}
            with patch.object(probe, "verify"), patch.object(probe, "owned", return_value={"unit": "owned-worker"}), \
                    patch.object(probe, "props", return_value=state), patch.object(probe, "call", side_effect=RuntimeError("dispatch failed")):
                with self.assertRaisesRegex(RuntimeError, "dispatch failed"):
                    probe.supervise(root)
            # A failed stop has not established quiescence, so no release record
            # or success result may be manufactured.
            self.assertFalse((root / "result.json").exists())


if __name__ == "__main__":
    unittest.main()
