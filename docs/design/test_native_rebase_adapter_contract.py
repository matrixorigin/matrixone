"""Run frozen adapter methods without a database; not SQL/transaction proof.

NATIVE_REBASE_ADAPTER names dblib/matrixone.py at c2737bea. Only the two
specified methods are compiled. No adapter initialization or service is run.
"""

import ast
from collections import Counter
import hashlib
import os
from pathlib import Path
import unittest


ADAPTER_SHA256 = "e3b71444682a18b7caf75fe06225e137ada938d190ae0d3d3889084d90001c52"


def load_methods():
    source = Path(os.environ["NATIVE_REBASE_ADAPTER"]).read_bytes()
    if hashlib.sha256(source).hexdigest() != ADAPTER_SHA256:
        raise ValueError("adapter source differs from frozen c2737bea")
    tree = ast.parse(source)
    wanted = {"_apply_nopk_delta", "_conflict_keys"}
    methods = [node for cls in tree.body if isinstance(cls, ast.ClassDef)
               for node in cls.body if isinstance(node, ast.FunctionDef)
               and node.name in wanted]
    if {node.name for node in methods} != wanted or len(methods) != 2:
        raise ValueError("missing or ambiguous adapter methods")
    quotes = [node for node in tree.body if isinstance(node, ast.FunctionDef)
              and node.name == "_quote"]
    if len(quotes) != 1:
        raise ValueError("missing identifier quoting helper")
    namespace = {}
    exec(compile(ast.Module(body=quotes + methods, type_ignores=[]), "frozen_adapter", "exec"), namespace)
    return namespace


class Cursor:
    def __init__(self, rows):
        self.rows = Counter(rows)
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def executemany(self, sql, values):
        if not sql.startswith("INSERT INTO "):
            raise ValueError("unexpected statement")
        values = list(values)
        self.statements.append((sql, values))
        self.rows.update(values)

    def execute(self, sql, values):
        if not sql.startswith("DELETE FROM ") or not sql.endswith("LIMIT 1"):
            raise ValueError("expected multiplicity-limited delete")
        self.statements.append((sql, values))
        if self.rows[values]:
            self.rows[values] -= 1

    def cursor(self):
        return self


class AdapterContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        methods = load_methods()
        cls.adapter = type("FrozenMethods", (), methods)

    def replay(self, upstream, rows):
        adapter = self.adapter()
        adapter.conn = Cursor(upstream)
        adapter._chunks = lambda values: [values] if values else []
        touched = adapter._apply_nopk_delta("destination", ["v"], (["v"], rows))
        return +adapter.conn.rows, touched, adapter.conn.statements

    def test_independent_equal_inserts_are_additive_not_count_coalesced(self):
        rows, touched, _ = self.replay([(7,)], [("INSERT", (7,))])
        self.assertEqual(rows, Counter({(7,): 2}))
        self.assertEqual(touched, 1)

    def test_null_duplicate_delete_removes_one_only(self):
        rows, touched, statements = self.replay([(None,), (None,)], [("DELETE", (None,))])
        self.assertEqual(rows, Counter({(None,): 1}))
        self.assertEqual(touched, 1)
        self.assertIn("<=>", statements[0][0])

    def test_overlapping_deletes_report_attempts_not_actual_mutations(self):
        rows, touched, _ = self.replay([], [("DELETE", (7,))])
        self.assertEqual(rows, Counter())
        self.assertEqual(touched, 1)

    def test_delete_update_is_visible_delete_plus_insert(self):
        rows, _, _ = self.replay([(20,)], [("DELETE", (10,)), ("INSERT", (11,))])
        self.assertEqual(rows, Counter({(20,): 1, (11,): 1}))

    def test_equal_pk_changes_and_deletes_are_not_conflicts(self):
        adapter = self.adapter()
        self.assertEqual(adapter._conflict_keys(
            {(1,): ("UPDATE", {"v": None}), (2,): ("DELETE", {})},
            {(1,): ("INSERT", {"v": None}), (2,): ("DELETE", {})}, ["v"]), [])

    def test_pk_delete_update_and_divergent_values_conflict(self):
        adapter = self.adapter()
        self.assertEqual(set(adapter._conflict_keys(
            {(1,): ("DELETE", {}), (2,): ("UPDATE", {"v": 20})},
            {(1,): ("UPDATE", {"v": 11}), (2,): ("UPDATE", {"v": 21})}, ["v"])),
            {(1,), (2,)})


if __name__ == "__main__":
    unittest.main()
