#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Export primary TPC-H query comparisons from the validated report CSV."""

import argparse
import csv
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    with args.report.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source))
    groups = {}
    for row in rows:
        if row["cell_status"] == "complete" and row["cell"].startswith("warm/"):
            groups.setdefault(row["cell"], []).append(row)
    args.output.mkdir(parents=True, exist_ok=False)
    for cell, values in sorted(groups.items()):
        values.sort(key=lambda row: int(row["query"]))
        if len(values) != 22:
            raise ValueError(f"incomplete primary query cell: {cell}")
        x = list(range(1, 23))
        mo = [float(row["mo_median_s"]) for row in values]
        duck = [float(row["duckdb_median_s"]) for row in values]
        fig, (latency, ratio) = plt.subplots(2, 1, figsize=(12, 7), sharex=True)
        latency.bar([i - 0.2 for i in x], mo, 0.4, label="MatrixOne")
        latency.bar([i + 0.2 for i in x], duck, 0.4, label="DuckDB")
        latency.set_yscale("log")
        latency.set_ylabel("Median query seconds (log)")
        latency.legend()
        speed = [float(row["duckdb_over_mo"]) for row in values]
        ratio.bar(x, speed)
        ratio.axhline(1, color="black", linewidth=1)
        ratio.set_ylabel("DuckDB / MO; >1 favors MO")
        ratio.set_xticks(x, [f"Q{i}" for i in x])
        fig.suptitle(cell)
        fig.tight_layout()
        stem = cell.replace("/", "-")
        fig.savefig(args.output / f"{stem}.svg")
        fig.savefig(args.output / f"{stem}.png", dpi=180)
        plt.close(fig)


if __name__ == "__main__":
    main()
