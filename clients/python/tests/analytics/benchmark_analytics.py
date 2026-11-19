# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Reproducible local RSS comparison; not a database performance benchmark.

Run with a server-visible --staging-dir. Each sample uses a fresh Python process,
imports the same modules, and validates the same row count and sum. RSS values
include the interpreter and imported libraries, and are Linux process peaks.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
import uuid

import pyarrow as pa
import pyarrow.compute as pc

from matrixone import Client


def client():
    logging.getLogger("matrixone").disabled = True
    return Client(
        host=os.environ.get("MO_HOST", "127.0.0.1"),
        port=int(os.environ.get("MO_PORT", "6001")),
        user=os.environ.get("MO_USER", "root"),
        password=os.environ.get("MO_PASSWORD", "111"),
        database=os.environ.get("MO_DATABASE", "mo_analytics_test"),
        sql_log_mode="off",
    )


def worker(mode, batch_size, table):
    # Workers receive only a generated simple identifier, never SQL.
    if not table.startswith("analytics_benchmark_") or not table[len("analytics_benchmark_") :].isalnum():
        raise ValueError("Invalid benchmark table name")
    c = client()
    c.execute("SELECT 1")
    # Arrow's first conversion can import pandas/numpy. Warm the exact same
    # conversion libraries in both workers before establishing their baselines.
    pc.sum(pa.array([1], type=pa.int64()))
    baseline = _memory_kib("VmRSS")
    start = time.perf_counter()
    count, checksum, max_batch = 0, 0, 0
    if mode == "buffered":
        result = c.execute("SELECT id, payload FROM `" + table + "`")
        count = len(result.rows)
        checksum = sum(row[0] for row in result.rows)
    else:
        with c.analytics.query("SELECT id, payload FROM `" + table + "`", batch_size=batch_size) as stream:
            for batch in stream:
                count += batch.num_rows
                checksum += pc.sum(batch.column(0)).as_py()
                max_batch = max(max_batch, batch.num_rows)
    elapsed = time.perf_counter() - start
    peak = _memory_kib("VmHWM")
    c.disconnect()
    return {
        "mode": mode,
        "rows": count,
        "checksum": checksum,
        "seconds": elapsed,
        "baseline_rss_mib": baseline / 1024,
        "peak_rss_mib": peak / 1024,
        "incremental_peak_mib": (peak - baseline) / 1024,
        "max_batch_rows": max_batch,
    }


def _memory_kib(key):
    # ru_maxrss can inherit the parent's fork-time high-water mark across exec.
    # /proc's memory status describes this fresh executable's address space.
    with open("/proc/self/status") as status:
        for line in status:
            if line.startswith(key + ":"):
                return int(line.split()[1])
    raise RuntimeError("Linux /proc memory counters are required")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", choices=["buffered", "streamed"])
    parser.add_argument("--staging-dir")
    parser.add_argument("--table", help="Parent-owned table passed to a worker")
    parser.add_argument("--rows", type=int, default=250000)
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(worker(args.worker, args.batch_size, args.table or "")))
        return
    if not args.staging_dir:
        parser.error("--staging-dir is required")
    c = client()
    records = pa.table({"id": range(args.rows), "payload": ["x" * 128] * args.rows})
    start = time.perf_counter()
    target = "analytics_benchmark_" + uuid.uuid4().hex
    created = False
    try:
        c.analytics.upload_arrow(target, records, args.staging_dir)
        created = True
        upload_seconds = time.perf_counter() - start
        samples = []
        for repeat in range(args.repeats):
            for mode in ("buffered", "streamed"):
                out = subprocess.check_output(
                    [
                        sys.executable,
                        __file__,
                        "--worker",
                        mode,
                        "--batch-size",
                        str(args.batch_size),
                        "--table",
                        target,
                    ],
                    text=True,
                )
                sample = json.loads(out.strip().splitlines()[-1])
                assert sample["rows"] == args.rows
                assert sample["checksum"] == args.rows * (args.rows - 1) // 2
                sample["repeat"] = repeat + 1
                samples.append(sample)
        report = {
            "rows": args.rows,
            "payload_bytes_per_row": 128,
            "arrow_source_bytes": records.nbytes,
            "batch_size": args.batch_size,
            "upload_seconds": upload_seconds,
            "python": sys.version.split()[0],
            "pyarrow": pa.__version__,
            "samples": samples,
        }
        text = json.dumps(report, indent=2)
        if args.output:
            with open(args.output, "w") as f:
                f.write(text + "\n")
        print(text)
    finally:
        try:
            if created:
                c.execute("DROP TABLE IF EXISTS `" + target + "`")
        finally:
            c.disconnect()


if __name__ == "__main__":
    main()
