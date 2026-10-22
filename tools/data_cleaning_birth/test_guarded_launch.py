import json
from pathlib import Path
import tempfile
import unittest
import guarded_launch as guard


class GuardTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.binary = self.root / "candidate"
        self.binary.write_bytes(b"candidate executable identity")
        self.native = self.root / "native.json"
        self.native.write_text("{}")
        self.launch = self.root / "launch.toml"
        launch = []
        for key, role in (("logservices", "LOG"), ("tnservices", "TN"), ("cnservices", "CN")):
            config = self.root / (role + ".toml")
            config.write_text(f'service-type = "{role}"\ndata-dir = "{self.root / "data"}"\n')
            launch.append(f'{key} = ["{config}"]')
        self.launch.write_text("\n".join(launch))

    def prepare(self):
        return guard.prepare(self.root, self.binary, "1" * 40, self.native, self.launch, "unit")

    def test_frozen_identity_and_no_overwrite(self):
        manifest = self.prepare()
        self.assertEqual("unit", guard.verify(manifest, self.binary)["run_id"])
        with self.assertRaises(ValueError):
            self.prepare()
        old = self.root / "old"
        old.write_bytes(b"old executable")
        with self.assertRaisesRegex(ValueError, "old binary"):
            guard.verify(manifest, old)

    def test_configuration_drift_and_marker(self):
        manifest = self.prepare()
        original = self.launch.read_text()
        self.launch.write_text(original + "\n# changed\n")
        with self.assertRaisesRegex(ValueError, "configuration"):
            guard.verify(manifest, self.binary)
        self.launch.write_text(original)
        marker = self.root / guard.MARKER
        value = json.loads(marker.read_text())
        value["format_version"] = 2
        marker.write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, "binding"):
            guard.verify(manifest, self.binary)

    def test_existing_data_and_escaping_path(self):
        data = self.root / "data"
        data.mkdir()
        (data / "evidence").write_bytes(b"retain")
        with self.assertRaisesRegex(ValueError, "populating"):
            self.prepare()
        self.assertEqual(b"retain", (data / "evidence").read_bytes())
        with tempfile.TemporaryDirectory() as outside:
            link = self.root / "escape"
            link.symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "escapes"):
                guard.inside(self.root, link / "new")

    def test_data_root_rebinding_rejected(self):
        manifest_path = self.prepare()
        manifest = json.loads(manifest_path.read_text())
        wrong = {key: manifest[key] for key in guard.MARKER_KEYS}
        wrong["binary_sha256"] = "4" * 64
        guard.set_data_xattr(manifest["data_roots"][0], json.dumps(wrong).encode())
        with self.assertRaisesRegex(ValueError, "data-directory"):
            guard.verify(manifest_path, self.binary)

    def test_dynamic_and_unknown_json_rejected(self):
        self.launch.write_text(self.launch.read_text() + "\n[dynamic]\nenable=true\n")
        with self.assertRaisesRegex(ValueError, "static"):
            self.prepare()
        bad = self.root / "bad.json"
        bad.write_text('{"x": 1, "x": 2}')
        with self.assertRaisesRegex(ValueError, "duplicate"):
            guard.read_json(bad, {"x"})


if __name__ == "__main__":
    unittest.main()
