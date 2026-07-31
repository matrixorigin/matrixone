# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import importlib.util
import pathlib
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("compare_results", ROOT / "test/mongodb/compare_results.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class CompareResultsTest(unittest.TestCase):
    def test_numeric_tolerance_and_exact_hash_contract(self):
        key = MODULE.canonical_values(["device-001", "site-east", "2026-07-27 10:00:00"])
        expected = {key: {"device_id": "device-001", "site_id": "site-east", "window_start": key[2], "measurement_avg": 1.0}}
        actual = {key: {"device_id": "device-001", "site_id": "site-east", "window_start": key[2], "measurement_avg": 1.0 + 1e-10}}
        failures, comparisons, counts = MODULE.compare(expected, actual, {"measurement_avg"}, 1e-9, {})
        self.assertFalse(failures)
        self.assertEqual(1, counts["tolerance-equal"])
        hashes = [item for item in comparisons if item["class"] == "exact-hash"]
        self.assertEqual(hashes[0]["expected"], hashes[0]["actual"])

    def test_expected_difference_is_exact_and_must_be_used(self):
        key_values = ["device-001", "site-east", "t"]
        key = MODULE.canonical_values(key_values)
        expected = {key: {"device_id": "device-001", "site_id": "site-east", "window_start": "t", "source_batch": "old"}}
        actual = {key: {"device_id": "device-001", "site_id": "site-east", "window_start": "t", "source_batch": None}}
        rule = {"key": key_values, "column": "source_batch", "expected": "old", "actual": None,
                "rule": "latest-document-missing-source-batch"}
        failures, _, counts = MODULE.compare(expected, actual, set(), 0, {(key, "source_batch"): rule})
        self.assertFalse(failures)
        self.assertEqual(1, counts["expected-inconsistent"])

        failures, _, _ = MODULE.compare(expected, expected, set(), 0, {(key, "source_batch"): rule})
        self.assertEqual("unused-expected-rule", failures[0]["class"])

    def test_duplicate_row_key_is_rejected(self):
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as source:
            source.write('{"device_id":"device-001","site_id":"site-east","window_start":"t","measurement":1}\n')
            source.write('{"device_id":"device-001","site_id":"site-east","window_start":"t","measurement":2}\n')
            source.flush()
            with self.assertRaises(ValueError):
                MODULE.load_rows(source.name, ["device_id", "site_id", "window_start"])

    def test_row_key_identity_is_type_sensitive(self):
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as source:
            source.write('{"device_id":1,"site_id":"site-east","window_start":"t","measurement":1}\n')
            source.write('{"device_id":"1","site_id":"site-east","window_start":"t","measurement":2}\n')
            source.flush()
            rows = MODULE.load_rows(source.name, ["device_id", "site_id", "window_start"])
        self.assertEqual(2, len(rows))

    def test_expected_difference_key_identity_is_type_sensitive(self):
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as source:
            source.write('{"rules":['
                         '{"key":[1,"site-east","t"],"column":"source_batch","expected":1,"actual":2,"rule":"integer"},'
                         '{"key":["1","site-east","t"],"column":"source_batch","expected":1,"actual":2,"rule":"string"}'
                         ']}')
            source.flush()
            rules = MODULE.load_rules(source.name)
        self.assertEqual(2, len(rules))

    def test_exact_contract_is_type_sensitive(self):
        key = MODULE.canonical_values(["device-001", "site-east", "t"])
        expected = {key: {"device_id": "device-001", "site_id": "site-east", "window_start": "t", "count": 1}}
        actual = {key: {"device_id": "device-001", "site_id": "site-east", "window_start": "t", "count": 1.0}}
        failures, _, _ = MODULE.compare(expected, actual, set(), 0, {})
        self.assertEqual(["must-equal", "exact-hash-mismatch"], [item["class"] for item in failures])

    def test_missing_column_is_not_explicit_null(self):
        key = MODULE.canonical_values(["device-001", "site-east", "t"])
        expected = {key: {"device_id": "device-001", "site_id": "site-east", "window_start": "t", "source_batch": None}}
        actual = {key: {"device_id": "device-001", "site_id": "site-east", "window_start": "t"}}
        failures, _, _ = MODULE.compare(expected, actual, set(), 0, {})
        self.assertEqual("missing-column", failures[0]["class"])
        self.assertIn("exact-hash-mismatch", [item["class"] for item in failures])

    def test_result_rows_require_every_key_column(self):
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as source:
            source.write('{"device_id":"device-001","window_start":"t","measurement":1}\n')
            source.flush()
            with self.assertRaisesRegex(ValueError, "missing key columns"):
                MODULE.load_rows(source.name, ["device_id", "site_id", "window_start"])

    def test_boolean_does_not_use_numeric_tolerance(self):
        key = MODULE.canonical_values(["device-001", "site-east", "t"])
        expected = {key: {"device_id": "device-001", "site_id": "site-east", "window_start": "t", "measurement_avg": False}}
        actual = {key: {"device_id": "device-001", "site_id": "site-east", "window_start": "t", "measurement_avg": 0}}
        failures, _, counts = MODULE.compare(expected, actual, {"measurement_avg"}, 1, {})
        self.assertEqual("must-equal", failures[0]["class"])
        self.assertEqual(0, counts["tolerance-equal"])

    def test_exact_comparison_is_recursively_type_sensitive(self):
        self.assertFalse(MODULE.exact_equal({"nested": [1]}, {"nested": [1.0]}))
        self.assertFalse(MODULE.exact_equal({"zero": -0.0}, {"zero": 0.0}))

    def test_expected_difference_document_shape_is_validated(self):
        with tempfile.NamedTemporaryFile("w", encoding="utf-8") as source:
            source.write('[]')
            source.flush()
            with self.assertRaisesRegex(ValueError, "rules array"):
                MODULE.load_rules(source.name)


if __name__ == "__main__":
    unittest.main()
