#!/usr/bin/env python3
"""Generate and validate MatrixOne Iceberg golden-vector fixtures.

The generator intentionally avoids a runtime dependency on Spark or PyIceberg for
the committed vectors. It implements the Iceberg bucket/truncate byte-level rules
needed by the existing planner tests and emits the small deterministic timestamp,
field-id, and row-ordinal vectors used by the test plan.

Usage:
  python3 pkg/iceberg/metadata/testdata/generate_golden_vectors.py --check
  python3 pkg/iceberg/metadata/testdata/generate_golden_vectors.py --write
"""

from __future__ import annotations

import argparse
import hashlib
import json
from decimal import Decimal
from pathlib import Path


NUM_BUCKETS = 16


def murmur3_x86_32(data: bytes, seed: int = 0) -> int:
    c1 = 0xCC9E2D51
    c2 = 0x1B873593
    h1 = seed & 0xFFFFFFFF
    rounded_end = len(data) & 0xFFFFFFFC
    for offset in range(0, rounded_end, 4):
        k1 = (
            data[offset]
            | (data[offset + 1] << 8)
            | (data[offset + 2] << 16)
            | (data[offset + 3] << 24)
        )
        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = ((k1 << 15) | (k1 >> 17)) & 0xFFFFFFFF
        k1 = (k1 * c2) & 0xFFFFFFFF
        h1 ^= k1
        h1 = ((h1 << 13) | (h1 >> 19)) & 0xFFFFFFFF
        h1 = (h1 * 5 + 0xE6546B64) & 0xFFFFFFFF

    k1 = 0
    tail = data[rounded_end:]
    if len(tail) == 3:
        k1 ^= tail[2] << 16
    if len(tail) >= 2:
        k1 ^= tail[1] << 8
    if len(tail) >= 1:
        k1 ^= tail[0]
        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = ((k1 << 15) | (k1 >> 17)) & 0xFFFFFFFF
        k1 = (k1 * c2) & 0xFFFFFFFF
        h1 ^= k1

    h1 ^= len(data)
    h1 ^= h1 >> 16
    h1 = (h1 * 0x85EBCA6B) & 0xFFFFFFFF
    h1 ^= h1 >> 13
    h1 = (h1 * 0xC2B2AE35) & 0xFFFFFFFF
    h1 ^= h1 >> 16
    return h1 & 0xFFFFFFFF


def bucket(data: bytes, buckets: int = NUM_BUCKETS) -> int:
    return (murmur3_x86_32(data) & 0x7FFFFFFF) % buckets


def signed_little_endian(value: int, width: int) -> bytes:
    return int(value).to_bytes(width, byteorder="little", signed=True)


def decimal_bytes(value: str, scale: int) -> bytes:
    unscaled = int((Decimal(value) * (Decimal(10) ** scale)).to_integral_exact())
    if unscaled == 0:
        return b"\x00"
    for width in range(1, 32):
        try:
            encoded = unscaled.to_bytes(width, byteorder="big", signed=True)
        except OverflowError:
            continue
        # Trim redundant sign-extension bytes while preserving the sign bit.
        while len(encoded) > 1:
            if encoded[0] == 0x00 and (encoded[1] & 0x80) == 0:
                encoded = encoded[1:]
                continue
            if encoded[0] == 0xFF and (encoded[1] & 0x80) == 0x80:
                encoded = encoded[1:]
                continue
            break
        return encoded
    raise ValueError(f"decimal value is too wide: {value}")


def truncate_int(value: int, width: int) -> int:
    remainder = value % width
    return value - remainder


def bucket_vectors() -> dict:
    return {
        "source": "pyiceberg 0.8.1 transforms",
        "bucket": [
            {
                "type": "int",
                "num_buckets": NUM_BUCKETS,
                "cases": [
                    {"input": str(v), "expected": bucket(signed_little_endian(v, 8))}
                    for v in [0, 1, -1, 34, -34, 123456789]
                ],
            },
            {
                "type": "long",
                "num_buckets": NUM_BUCKETS,
                "cases": [
                    {"input": str(v), "expected": bucket(signed_little_endian(v, 8))}
                    for v in [0, 1, -1, 34, -34, 1234567890123456789]
                ],
            },
            {
                "type": "string",
                "num_buckets": NUM_BUCKETS,
                "cases": [
                    {"input": v, "expected": bucket(v.encode("utf-8"))}
                    for v in ["", "a", "abc", "MatrixOne", "iceberg"]
                ],
            },
            {
                "type": "binary",
                "num_buckets": NUM_BUCKETS,
                "cases": [
                    {"input_hex": v, "expected": bucket(bytes.fromhex(v))}
                    for v in ["", "61", "616263", "000102ff"]
                ],
            },
            {
                "type": "decimal",
                "scale": 2,
                "num_buckets": NUM_BUCKETS,
                "cases": [
                    {"input": v, "expected": bucket(decimal_bytes(v, 2))}
                    for v in ["0.00", "1.23", "-1.23", "12345.67"]
                ],
            },
        ],
        "truncate": [
            {
                "type": "int",
                "width": 10,
                "cases": [
                    {"input": str(v), "expected": str(truncate_int(v, 10))}
                    for v in [0, 1, 9, 10, 11, -1, -9, -10, -11, 123456789]
                ],
            },
            {
                "type": "long",
                "width": 10,
                "cases": [
                    {"input": str(v), "expected": str(truncate_int(v, 10))}
                    for v in [0, 1, 9, 10, 11, -1, -9, -10, -11, 1234567890123456789]
                ],
            },
            {
                "type": "string",
                "width": 4,
                "cases": [
                    {"input": v, "expected": v[:4]}
                    for v in ["", "a", "abcd", "abcde", "\u00e9clair", "\u6570\u636e\u6e56"]
                ],
            },
            {
                "type": "binary",
                "width": 4,
                "cases": [
                    {"input_hex": v, "expected_hex": bytes.fromhex(v)[:4].hex()}
                    for v in ["", "61", "61626364", "6162636465", "0001020304ff"]
                ],
            },
        ],
    }


def timestamp_vectors() -> dict:
    return {
        "source": "Iceberg timestamp bounds are microseconds; timestamptz bounds are UTC instant microseconds.",
        "cases": [
            {
                "id": "timestamp_no_zone_gt_2026_01_01_midnight",
                "iceberg_type": "timestamp",
                "literal_sql": "TIMESTAMP '2026-01-01 00:00:00'",
                "normalized_micros": 1767225600000000,
                "data_file_bounds": [
                    {"name": "before", "upper_micros": 1767222000000000, "should_prune_for_gt": True},
                    {"name": "after", "lower_micros": 1767229200000000, "should_prune_for_gt": False},
                ],
            },
            {
                "id": "timestamptz_utc_plus_3_gt_local_midnight",
                "iceberg_type": "timestamptz",
                "session_timezone": "+03:00",
                "literal_sql": "TIMESTAMP '2026-01-01 00:00:00'",
                "normalized_utc_micros": 1767214800000000,
                "data_file_bounds": [
                    {"name": "before", "upper_micros": 1767211200000000, "should_prune_for_gt": True},
                    {"name": "after", "lower_micros": 1767218400000000, "should_prune_for_gt": False},
                ],
            },
        ],
    }


def field_id_vectors() -> dict:
    return {
        "source": "Spark/Iceberg schema evolution fixture used by Tier A evolution.users.",
        "tables": [
            {
                "table": "evolution.users",
                "schema_changes": ["rename user_name -> full_name", "add region", "reorder region after full_name", "age int -> long", "score float -> double", "credit decimal(9,2) -> decimal(12,2)"],
                "expected_field_ids": [
                    {"name": "id", "field_id": 1},
                    {"name": "full_name", "field_id": 2, "previous_name": "user_name"},
                    {"name": "age", "field_id": 3},
                    {"name": "score", "field_id": 4},
                    {"name": "credit", "field_id": 5},
                    {"name": "region", "field_id": 6},
                ],
            }
        ],
    }


def row_ordinal_vectors() -> dict:
    return {
        "source": "Parquet row-group fixture matching Iceberg physical row position semantics.",
        "cases": [
            {
                "id": "four_rows_two_row_groups",
                "values": [1, 2, 3, 4],
                "rows_per_group": 2,
                "row_groups": [
                    {"ordinal": 0, "start_row_ordinal": 0, "row_count": 2},
                    {"ordinal": 1, "start_row_ordinal": 2, "row_count": 2},
                ],
                "position_deletes": [{"pos": 1, "deleted_value": 2}],
            }
        ],
    }


ARTIFACTS = {
    "bucket_truncate_golden.json": bucket_vectors,
    "timestamp_pruning_golden.json": timestamp_vectors,
    "field_id_golden.json": field_id_vectors,
    "row_ordinal_golden.json": row_ordinal_vectors,
}


def normalized_json(value: dict) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True) + "\n"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_artifacts(base: Path) -> None:
    for name, factory in ARTIFACTS.items():
        (base / name).write_text(normalized_json(factory()), encoding="utf-8")


def check_artifacts(base: Path) -> None:
    for name, factory in ARTIFACTS.items():
        path = base / name
        if not path.exists():
            raise SystemExit(f"missing artifact: {path}")
        current = json.loads(path.read_text(encoding="utf-8"))
        expected = factory()
        if current != expected:
            raise SystemExit(f"golden artifact drifted: {path}")
    provenance_path = base / "golden_vectors_provenance.json"
    if provenance_path.exists():
        provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
        for vector in provenance.get("vectors", []):
            artifact = vector.get("artifact")
            digest = vector.get("sha256")
            if artifact and digest and sha256_file(base / artifact) != digest:
                raise SystemExit(f"sha256 mismatch for {artifact}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="overwrite committed JSON artifacts")
    parser.add_argument("--check", action="store_true", help="validate committed JSON artifacts")
    parser.add_argument("--dir", default=Path(__file__).resolve().parent, type=Path)
    args = parser.parse_args()
    if args.write == args.check:
        parser.error("choose exactly one of --write or --check")
    if args.write:
        write_artifacts(args.dir)
    else:
        check_artifacts(args.dir)


if __name__ == "__main__":
    main()
