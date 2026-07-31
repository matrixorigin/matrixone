#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Versioned MongoDB aggregate differential oracle for sorted JSON-lines results."""
import argparse
import hashlib
import json
import math
import sys


def canonical_value(value):
    """Return a sortable, type-preserving identity for a JSON value."""
    if value is None:
        return ("null", "")
    if type(value) is bool:
        return ("bool", "true" if value else "false")
    if type(value) is int:
        return ("integer", str(value))
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("result keys must not contain non-finite numbers")
        return ("float", value.hex())
    if type(value) is str:
        return ("string", value)
    return ("json", json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False))


def canonical_values(values):
    return tuple(canonical_value(value) for value in values)


def canonical(row, keys):
    return canonical_values(row[key] for key in keys)


def digest(row):
    encoded = json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(b"mongodb-aggregate-v1\0" + encoded).hexdigest()


def exact_equal(left, right):
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(exact_equal(left[key], right[key]) for key in left)
    if isinstance(left, list):
        return len(left) == len(right) and all(exact_equal(x, y) for x, y in zip(left, right))
    if isinstance(left, float) and isinstance(right, float) and left == right == 0:
        return math.copysign(1, left) == math.copysign(1, right)
    return left == right


def load_rows(path, keys):
    rows = {}
    with open(path, encoding="utf-8") as source:
        for line_number, line in enumerate(source, 1):
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError("result row must be a JSON object at %s:%d" % (path, line_number))
            missing_keys = [key for key in keys if key not in row]
            if missing_keys:
                raise ValueError("result row is missing key columns %r at %s:%d" %
                                 (missing_keys, path, line_number))
            key = canonical(row, keys)
            if key in rows:
                raise ValueError("duplicate row key %r at %s:%d" % (key, path, line_number))
            rows[key] = row
    return rows


def load_rules(path):
    if not path:
        return {}
    with open(path, encoding="utf-8") as source:
        document = json.load(source)
    if not isinstance(document, dict) or not isinstance(document.get("rules", []), list):
        raise ValueError("expected-difference document requires a rules array")
    rules = {}
    for rule in document.get("rules", []):
        if not isinstance(rule, dict):
            raise ValueError("every expected-difference rule must be a JSON object")
        key = canonical_values(rule.get("key", []))
        column = rule.get("column")
        if not key or not column or not rule.get("rule") or "expected" not in rule or "actual" not in rule:
            raise ValueError("every expected-difference rule requires key, column, expected, actual, and rule")
        identity = (key, column)
        if identity in rules:
            raise ValueError("duplicate expected-difference rule for %r" % (identity,))
        rules[identity] = rule
    return rules


def compare(expected, actual, tolerant, abs_tolerance, rules):
    failures = []
    comparisons = []
    used_rules = set()
    counts = {"exact-equal": 0, "tolerance-equal": 0, "expected-inconsistent": 0, "failed": 0}
    for key in sorted(expected.keys() | actual.keys()):
        left, right = expected.get(key), actual.get(key)
        if left is None or right is None:
            item = {"key": key, "class": "missing-row"}
            failures.append(item)
            comparisons.append(item)
            counts["failed"] += 1
            continue
        expected_difference_columns = set()
        for column in sorted(left.keys() | right.keys()):
            if column not in left or column not in right:
                item = {"key": key, "column": column, "class": "missing-column",
                        "expected": left.get(column, "<missing>"),
                        "actual": right.get(column, "<missing>")}
                failures.append(item)
                comparisons.append(item)
                counts["failed"] += 1
                continue
            old, new = left[column], right[column]
            if exact_equal(old, new):
                counts["exact-equal"] += 1
                continue
            if (column in tolerant and not isinstance(old, bool) and not isinstance(new, bool)
                    and isinstance(old, (int, float)) and isinstance(new, (int, float))
                    and math.isclose(old, new, rel_tol=0, abs_tol=abs_tolerance)):
                comparisons.append({"key": key, "column": column, "class": "tolerance-equal",
                                    "expected": old, "actual": new})
                counts["tolerance-equal"] += 1
                continue
            identity = (key, column)
            rule = rules.get(identity)
            if rule and exact_equal(rule["expected"], old) and exact_equal(rule["actual"], new):
                comparisons.append({"key": key, "column": column, "class": "expected-inconsistent",
                                    "rule": rule["rule"], "expected": old, "actual": new})
                used_rules.add(identity)
                expected_difference_columns.add(column)
                counts["expected-inconsistent"] += 1
                continue
            item = {"key": key, "column": column, "class": "must-equal",
                    "expected": old, "actual": new}
            failures.append(item)
            comparisons.append(item)
            counts["failed"] += 1

        exact_left = {column: value for column, value in left.items()
                      if column not in tolerant and column not in expected_difference_columns}
        exact_right = {column: value for column, value in right.items()
                       if column not in tolerant and column not in expected_difference_columns}
        try:
            expected_hash, actual_hash = digest(exact_left), digest(exact_right)
            item = {"key": key, "class": "exact-hash", "expected": expected_hash,
                    "actual": actual_hash}
            comparisons.append(item)
            if expected_hash != actual_hash:
                failure = dict(item)
                failure["class"] = "exact-hash-mismatch"
                failures.append(failure)
                counts["failed"] += 1
        except ValueError:
            # Non-finite values are already classified column-by-column and
            # cannot participate in canonical JSON hashing.
            comparisons.append({"key": key, "class": "exact-hash-unavailable"})

    for identity, rule in rules.items():
        if identity not in used_rules:
            item = {"key": identity[0], "column": identity[1], "class": "unused-expected-rule",
                    "rule": rule["rule"]}
            failures.append(item)
            comparisons.append(item)
            counts["failed"] += 1
    return failures, comparisons, counts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("expected")
    parser.add_argument("actual")
    parser.add_argument("--keys", default="device_id,site_id,window_start")
    parser.add_argument(
        "--tolerance-columns",
        default=("temperature_celsius,humidity_percent,pressure_kpa,flow_rate_lpm,"
                 "vibration_mm_s,voltage_volts,total_runtime_hours,active_runtime_hours"),
    )
    parser.add_argument("--abs-tolerance", type=float, default=1e-9)
    parser.add_argument("--expected-differences", help="versioned exact expected-difference rules JSON")
    args = parser.parse_args()
    keys = args.keys.split(",")
    tolerant = set(filter(None, args.tolerance_columns.split(",")))
    try:
        expected = load_rows(args.expected, keys)
        actual = load_rows(args.actual, keys)
        rules = load_rules(args.expected_differences)
        failures, comparisons, counts = compare(expected, actual, tolerant, args.abs_tolerance, rules)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        json.dump({"contract": "mongodb-aggregate-v1", "failures": [{"class": "invalid-input", "error": str(error)}]}, sys.stdout, indent=2)
        print()
        return 2
    json.dump({"contract": "mongodb-aggregate-v1", "summary": counts,
               "comparisons": comparisons, "failures": failures}, sys.stdout, indent=2)
    print()
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
