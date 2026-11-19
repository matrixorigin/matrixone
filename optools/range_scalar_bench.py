#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""比较等价范围标量聚合的 SQL 成本；需要 PyMySQL，服务由调用者独占管理。

用法：MO_TEST_PASSWORD=... python3 optools/range_scalar_bench.py --port 6001
每个尺寸预热一次、交替执行两种写法并校验每一行。没有墙钟通过阈值。
临时数据库采用随机名称，不删除已有数据库；成功/失败均清理本次创建的库。
"""

import argparse
import json
import os
import statistics
import time
import uuid

import pymysql


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6001)
    parser.add_argument("--user", default="dump")
    parser.add_argument("--samples", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.samples <= 20:
        parser.error("samples 必须介于 1 和 20")
    database = "range_scalar_bench_" + uuid.uuid4().hex
    with pymysql.connect(host=args.host, port=args.port, user=args.user,
                         password=os.environ.get("MO_TEST_PASSWORD", ""),
                         autocommit=True, connect_timeout=10, read_timeout=120,
                         write_timeout=120) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {database}")
            try:
                cur.execute(f"USE {database}")
                cur.execute("CREATE TABLE outer_rows(id INT)")
                cur.execute("CREATE TABLE inner_rows(k INT,v INT)")
                for outer, inner in ((100, 2000), (400, 2000), (100, 8000), (400, 8000)):
                    cur.execute("TRUNCATE outer_rows")
                    cur.execute("TRUNCATE inner_rows")
                    cur.execute(f"INSERT INTO outer_rows SELECT result FROM generate_series(1,{outer}) g")
                    cur.execute(f"INSERT INTO inner_rows SELECT result,2*result FROM generate_series(1,{inner}) g")
                    expected = tuple((i, 2 * i + 1) for i in range(1, outer + 1))
                    sql = {
                        "outside": "SELECT o.id,(SELECT MAX(i.v) FROM inner_rows i WHERE i.k<=o.id)+1 FROM outer_rows o ORDER BY o.id",
                        "inside": "SELECT o.id,(SELECT MAX(i.v)+1 FROM inner_rows i WHERE i.k<=o.id) FROM outer_rows o ORDER BY o.id",
                    }
                    samples = {name: [] for name in sql}
                    for iteration in range(args.samples + 1):
                        for name in (list(sql) if iteration % 2 == 0 else list(reversed(sql))):
                            start = time.perf_counter()
                            cur.execute(sql[name])
                            rows = cur.fetchall()
                            elapsed = (time.perf_counter() - start) * 1000
                            if rows != expected:
                                raise AssertionError(f"{name}, E={outer}, I={inner}: {rows!r} != {expected!r}")
                            if iteration:
                                samples[name].append(elapsed)
                    print(json.dumps({"outer_rows": outer, "inner_rows": inner,
                                      "samples_ms": samples,
                                      "median_ms": {name: statistics.median(values) for name, values in samples.items()},
                                      "checked_rows_per_query": len(expected)}), flush=True)
            finally:
                cur.execute(f"DROP DATABASE {database}")
                cur.execute("SHOW DATABASES LIKE %s", (database,))
                if cur.fetchall():
                    raise AssertionError("性能 fixture 数据库未清理")


if __name__ == "__main__":
    main()
