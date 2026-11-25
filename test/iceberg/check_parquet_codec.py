#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Check committed SQL-written files using PyIceberg and PyArrow."""
import argparse

import pyarrow.parquet as parquet
from pyiceberg.catalog import load_catalog


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", required=True, help="namespace from the E2E summary")
    parser.add_argument("--catalog-uri", default="http://127.0.0.1:19120/iceberg")
    parser.add_argument("--warehouse", default="s3://mo-iceberg/warehouse")
    parser.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--s3-access-key", default="minio")
    parser.add_argument("--s3-secret-key", default="minio123")
    args = parser.parse_args()
    catalog = load_catalog("codec-check", type="rest", uri=args.catalog_uri,
                           warehouse=args.warehouse, **{"s3.endpoint": args.s3_endpoint,
                           "s3.access-key-id": args.s3_access_key,
                           "s3.secret-access-key": args.s3_secret_key,
                           "s3.region": "us-east-1"})
    expected = [(1, 1, 10, "repeat"), (2, 1, 20, "repeat"),
                (3, 2, 30, None), (4, 2, 40, "")]
    for name, codec in (("codec_default", "ZSTD"), ("codec_zstd", "ZSTD"),
                        ("codec_uncompressed", "UNCOMPRESSED")):
        table = catalog.load_table((args.namespace, name))
        tasks = list(table.scan().plan_files())
        if not tasks:
            raise AssertionError(f"{name}: no committed data files")
        dictionary = False
        for task in tasks:
            with table.io.new_input(task.file.file_path).open() as stream:
                metadata = parquet.ParquetFile(stream).metadata
                if not metadata.num_row_groups:
                    raise AssertionError(f"{name}: no row groups")
                for i in range(metadata.num_row_groups):
                    group = metadata.row_group(i)
                    for j in range(group.num_columns):
                        column = group.column(j)
                        if column.compression != codec:
                            raise AssertionError(f"{name}: {column.path_in_schema} codec {column.compression}, expected {codec}")
                        if column.path_in_schema == "region" and "RLE_DICTIONARY" in column.encodings:
                            dictionary = True
        if not dictionary:
            raise AssertionError(f"{name}: region has no dictionary encoded data page")
        rows = table.scan().to_arrow().to_pylist()
        actual = sorted((r["order_id"], r["bucket"], r["amount"], r["region"]) for r in rows)
        if actual != expected:
            raise AssertionError(f"{name}: independent read mismatch: {actual!r}")
        print(f"{name}: {len(tasks)} committed files, {codec}, dictionary and independent read OK")


if __name__ == "__main__":
    main()
