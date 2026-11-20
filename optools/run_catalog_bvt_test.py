import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import yaml

import run_catalog_bvt as runner


class CatalogRoutingTest(unittest.TestCase):
    def test_order_config_and_failure(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scripts = []
            for i, sql in enumerate(["select 1;", " SHOW\naccounts;", "show accounts like 'x';", "-- show accounts;\nselect 2;"]):
                path = root / f"{i}.sql"
                path.write_text(sql)
                scripts.append(str(path))
            conf = {"jdbc": {"server": [{"addr": "proxy:1"}]}, "debug": {"port": 1}}
            (root / "mo.yml").write_text(yaml.safe_dump(conf))
            calls = []

            def run(cmd, check, cwd=None):
                self.assertTrue(check)
                if cmd[0] == "javac":
                    return
                cfg = yaml.safe_load((cwd / "mo.yml").read_text())
                args = cmd[cmd.index("CatalogBvtBatch") + 1:]
                self.assertEqual(args[:2], [str(root), "resources"])
                calls.append((cfg["jdbc"]["server"][0]["addr"], *args[2:]))
                if cwd != root:
                    (cwd / "report").mkdir(exist_ok=True)
                    (cwd / "report" / "report.txt").write_text("full")

            def invoke(effect):
                with patch.object(runner.sys, "stdin", io.StringIO("\n".join(scripts))), \
                        patch.object(runner.sys, "argv", ["run", str(root), str(root), "resources", "full:2", "3"]), \
                        patch.object(runner.subprocess, "run", side_effect=effect):
                    runner.main()

            invoke(run)
            self.assertEqual(calls, [("proxy:1", "true", "false", scripts[0]),
                                     ("full:2", "false", "false", *scripts[1:3]),
                                     ("proxy:1", "false", "true", scripts[3])])
            self.assertEqual((root / "report/batch-1/report.txt").read_text(), "full")
            with self.assertRaises(runner.subprocess.CalledProcessError):
                invoke(runner.subprocess.CalledProcessError(1, "run.sh"))
            self.assertEqual(yaml.safe_load((root / "mo.yml").read_text()), conf)


if __name__ == "__main__":
    unittest.main()
