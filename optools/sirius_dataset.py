#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate immutable decimal TPC-H source manifests with frozen DuckDB."""
import argparse
import hashlib
import json
import os
from pathlib import Path

TABLES = ("nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem")


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            value.update(chunk)
    return value.hexdigest()


def generate(root, scale, source_root):
    import duckdb

    if duckdb.__version__ != "1.5.6" or scale not in (1, 10):
        raise ValueError("campaign requires pinned DuckDB 1.5.6 and SF1/SF10")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.is_symlink() or root.stat().st_mode & 0o077:
        raise ValueError("dataset root must be private")
    target = root / f"SF{scale}"
    target.mkdir(mode=0o700)
    db = target / "generation.duckdb"
    connection = duckdb.connect(str(db))
    try:
        connection.execute("SET memory_limit='8GB'")
        connection.execute("SET threads=4")
        connection.execute("INSTALL tpch FROM 'https://extensions.duckdb.org'")
        connection.load_extension("tpch")
        version = connection.execute("PRAGMA version").fetchall()
        extension = connection.execute("select install_path from duckdb_extensions() where extension_name='tpch'").fetchone()[0]
        extension_hash = digest(Path(extension))
        connection.execute(f"CALL dbgen(sf={scale})")
        ddl = (source_root / "pkg/sql/plan/tpch/ddl.sql").read_text()
        old = "L_QUANTITY    INTEGER /*DECIMAL(15,2)*/ NOT NULL"
        if ddl.count(old) != 1:
            raise ValueError("canonical quantity DDL changed")
        ddl = ddl.replace(old, "L_QUANTITY    DECIMAL(15,2) NOT NULL")
        ddl_path = target / "schema.sql"
        ddl_path.write_text(ddl)
        ddl_path.chmod(0o600)
        tables = {}
        for table in TABLES:
            path = target / f"{table}.csv"
            quoted = str(path).replace("'", "''")
            connection.execute(f"COPY {table} TO '{quoted}' (FORMAT CSV, HEADER false, DELIMITER '|', NULL '\\N')")
            path.chmod(0o600)
            rows = connection.execute(f"select count(*) from {table}").fetchone()[0]
            columns = connection.execute("select column_name,data_type,is_nullable from information_schema.columns where table_name=? order by ordinal_position", [table]).fetchall()
            tables[table] = {"file": path.name, "sha256": digest(path), "bytes": path.stat().st_size, "row_count": rows, "generator_columns": columns}
        manifest = {
            "schema_version": 2, "scale_factor": scale,
            "generator": {"version": version, "tpch_extension_sha256": extension_hash, "parameters": {"sf": scale}},
            "ddl_sha256": digest(ddl_path), "tables": tables,
        }
        data = json.dumps(manifest, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        manifest["dataset_sha256"] = hashlib.sha256(data).hexdigest()
        output = target / "manifest.json"
        with output.open("x") as destination:
            json.dump(manifest, destination, sort_keys=True, indent=2)
            destination.write("\n")
        output.chmod(0o600)
    finally:
        connection.close()
    db.unlink()
    return manifest["dataset_sha256"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--scale", type=int, choices=(1, 10), required=True)
    parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parent.parent)
    args = parser.parse_args()
    print(f"SF{args.scale} dataset digest: {generate(args.root, args.scale, args.source_root)}")
