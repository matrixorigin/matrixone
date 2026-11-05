"""Offline failure-path checks for the bounded Linux probe wrapper."""

import json
from pathlib import Path
import tempfile
import tarfile
import unittest
from unittest.mock import patch

import native_rebase_probe_linux as probe


class ProbeWrapperTests(unittest.TestCase):
    def test_native_namespace_keeps_the_frozen_rootfs_and_no_ptrace_launcher(self):
        command = probe.namespace_prefix(Path("/owned/run"), Path("/owned/old"))
        self.assertEqual(command[0], "/usr/bin/bwrap")
        self.assertIn("--die-with-parent", command)
        self.assertIn("--unshare-pid", command)
        self.assertIn(str(probe.COMPILER / "rootfs"), command)
        self.assertNotIn(str(probe.COMPILER / "bootstrap/usr/bin/proot"), command)
        self.assertEqual(command[command.index("--ro-bind") + 2], "/")

    def test_closed_probe_rejects_foreign_path(self):
        with self.assertRaisesRegex(ValueError, "unexpected closed probe"):
            probe.closed_probe(Path("/tmp/foreign"))

    def test_closed_probe_rejects_live_original_owner(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve() / "old"
            root.mkdir()
            probe.save(root / "identity.json", {"root": str(root), "unit": "old-worker", "supervisor": "old-supervisor"})
            with patch.object(probe, "PARENT", root.parent), patch.object(
                    probe, "props", return_value={"MainPID": "42", "ActiveState": "active", "ControlGroup": "owned"}):
                with self.assertRaisesRegex(ValueError, "owner still active"):
                    probe.closed_probe(root)

    def test_child_abort_is_not_lost_when_parent_only_reports_eof(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            child = root / "cases/mo-rebase-crash-owned"
            child.mkdir(parents=True)
            (child / "prepare.log").write_text("double free or corruption (out)\n")
            self.assertEqual(probe.native_abort_locations(root, "EOF"),
                             ["cases/mo-rebase-crash-owned/prepare.log"])

    def test_absent_child_log_is_not_native_abort_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(probe.native_abort_locations(Path(tmp), "EOF"), [])

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
