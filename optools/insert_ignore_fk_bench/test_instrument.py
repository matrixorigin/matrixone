import importlib.util
import tempfile
import unittest
from pathlib import Path

SPEC = importlib.util.spec_from_file_location("instrument", Path(__file__).with_name("instrument.py"))
instrument = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(instrument)


class InstrumentTests(unittest.TestCase):
    def fixture(self, root):
        reader = "\tvar blkInfo *objectio.BlockInfo\n\n\tstart := time.Now()"
        allocator = "import (\n)\n\tfrom, to, lastAllocateAt, err := a.store.Allocate("
        for name, text in ((instrument.READER, reader), (instrument.ALLOCATOR, allocator)):
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        (root / instrument.PROBE).parent.mkdir(parents=True)
        return {instrument.READER: reader, instrument.ALLOCATOR: allocator}

    def test_roundtrip_and_repeated_install_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            originals = self.fixture(root)
            instrument.install(root)
            self.assertIn("CLAUDEObserveReader", (root / instrument.READER).read_text())
            with self.assertRaises(ValueError):
                instrument.install(root)
            instrument.remove(root)
            for name, expected in originals.items():
                self.assertEqual((root / name).read_text(), expected)
            for name in (instrument.PROBE, instrument.ATTACH, instrument.MANIFEST):
                self.assertFalse((root / name).exists())

    def test_changed_source_cannot_be_overwritten_by_remove(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            instrument.install(root)
            path = root / instrument.READER
            path.write_text(path.read_text() + "\n// independent WIP\n")
            with self.assertRaises(ValueError):
                instrument.remove(root)
            self.assertIn("independent WIP", path.read_text())

    def test_invalid_anchor_does_not_partially_install(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            originals = self.fixture(root)
            (root / instrument.ALLOCATOR).write_text("no anchor")
            with self.assertRaises(ValueError):
                instrument.install(root)
            self.assertEqual((root / instrument.READER).read_text(), originals[instrument.READER])
            self.assertFalse((root / instrument.MANIFEST).exists())


if __name__ == "__main__":
    unittest.main()
