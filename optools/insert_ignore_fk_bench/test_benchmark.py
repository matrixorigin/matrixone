import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    import pymysql  # noqa: F401
except ImportError as exc:
    raise unittest.SkipTest("SQL 驱动测试需要 PyMySQL 1.1.2") from exc

SPEC = importlib.util.spec_from_file_location("benchmark", Path(__file__).with_name("benchmark.py"))
benchmark = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(benchmark)


class BenchmarkDriverTests(unittest.TestCase):
    def test_all_shapes_have_requested_cardinality(self):
        for shape in benchmark.SHAPES:
            with self.subTest(shape=shape):
                columns, rows = benchmark.payload(shape, 4096, 16)
                self.assertEqual(len(rows), 16)
                self.assertTrue(all(len(row.split(",")) == len(columns.split(",")) for row in rows))
        self.assertEqual(benchmark.payload("self_composite", 256, 1),
                         ("id,a,b,ca,cb", ["257,514,771,2,3"]))

    def test_load_file_and_explicit_ignore(self):
        with tempfile.TemporaryDirectory(prefix="CLAUDE_fkbench_test_") as directory:
            root = Path(directory)
            for mode in ("load", "local"):
                statement = benchmark.make_statement("fkbench_child", "auto_pk", 256, 3, mode, root)
                self.assertIn("ignore into table fkbench_child", statement)
                self.assertEqual("load data local" in statement, mode == "local")
                self.assertEqual((root / "input.csv").read_text(), "1\n1\n1\n")

    def test_flush_requires_structured_ack(self):
        for results, valid in (([{"returnStr": "OK"}], True),
                               ([{"returnStr": "error"}], False), ([], False)):
            with patch.object(benchmark, "sql", return_value=((json.dumps({"result": results}),),)):
                if valid:
                    benchmark.flush_parent(None, "test", "fkbench_parent")
                else:
                    with self.assertRaises(AssertionError):
                        benchmark.flush_parent(None, "test", "fkbench_parent")

    def test_generated_id_oracle_rejects_wrong_mapping(self):
        with patch.object(benchmark, "scalar", side_effect=[3, 0]), \
             patch.object(benchmark, "sql", return_value=((2, 4, 9),)):
            with self.assertRaisesRegex(AssertionError, "最终 ID"):
                benchmark.oracle(None, "fkbench_child", "auto_pk", 256, 3)


if __name__ == "__main__":
    unittest.main()
