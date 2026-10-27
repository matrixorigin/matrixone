#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Bounded client-side SQL measurements; the caller owns the test service.

Requires PyMySQL. See float_decimal_bench.md. No services are started or stopped.
The dedicated database must not exist; it is dropped on success or failure.
"""

import argparse
from collections import Counter
from decimal import Decimal, ROUND_HALF_UP, localcontext
import hashlib
import json
import math
import os
from pathlib import Path
import re
import struct
import time

import pymysql


# Each cohort has multiple stored values at both signs. Every row is numeric;
# NULL/diagnostics belong to the separate small BVT, not numeric throughput.
COHORTS = [
    ("ordinary64", 16, 6, [12345.6789, 12.3456789]),
    ("integral128", 38, 6, [12345.0, 12346.0]),
    ("tie64", 16, 2, [1.005, 1.015]),
    ("near_tie128", 38, 2, [math.nextafter(1.005, 0), math.nextafter(1.015, 0)]),
    ("tiny64", 16, 6, [1e-20, 2e-20]),
    ("tiny128", 38, 6, [1e-20, 2e-20]),
    ("large128", 38, 6, [123456789012.34567, 234567890123.45678]),
    ("high_scale64", 16, 16, [0.12345678901234568, 0.23456789012345678]),
    ("scale19", 38, 19, [1e-20, 2e-20]),
    ("scale20", 38, 20, [1e-20, 2e-20]),
    ("scale38", 38, 38, [1e-20, 2e-20]),
]
CANDIDATES = [1_000_000, 4_000_000, 16_000_000]
WARMUPS, SAMPLES = 2, 5
TIMEOUT = 120


def query(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def expected(values, scale, rows):
    with localcontext() as ctx:
        ctx.prec = 100
        quantum = Decimal(1).scaleb(-scale)
        counts = Counter()
        for value in values:
            counts[Decimal(str(value)).quantize(quantum, rounding=ROUND_HALF_UP)] += rows // 4
        return sorted(counts.items())


def checked_groups(conn, table, want):
    got = [(Decimal(str(value)), count) for value, count in query(conn, f"select d,count(*) from {table} group by d order by d")]
    if got != want:
        raise AssertionError(f"{table}: {got} != {want}")
    text = json.dumps([(str(v), n) for v, n in got])
    return hashlib.sha256(text.encode()).hexdigest()


def prepare(conn, name, values, rows):
    query(conn, f"drop table if exists src_{name}")
    query(conn, f"create table src_{name}(d double)")
    expr = "case result % 4 " + " ".join(
        f"when {i} then {value!r}e0" if "e" not in repr(value) else f"when {i} then {value!r}"
        for i, value in enumerate(values)
    ) + " end"
    # Python repr round-trips each selected binary64. Scientific SQL literals
    # force approximate-number typing before storage (also for integral control).
    query(conn, f"insert into src_{name} select {expr} from generate_series(1,{rows}) g")
    groups = query(conn, f"select d,count(*) from src_{name} group by d order by d")
    bits = {struct.pack('>d', value).hex(): count for value, count in groups}
    want = {struct.pack('>d', value).hex(): rows // 4 for value in values}
    if bits != want:
        raise AssertionError(f"stored DOUBLE bits for {name}: {bits} != {want}")
    return bits


def statements(name, width, scale):
    typ = f"decimal({width},{scale})"
    # MIN/MAX consume the cast result without overflowing a high-scale SUM.
    scan = f"select count(*),min(cast(d as {typ})),max(cast(d as {typ})) from src_{name}"
    insert = f"insert into dst_{name} select d from src_{name}"
    return typ, scan, insert


def timed(conn, sql):
    start = time.perf_counter()
    result = query(conn, sql)
    return time.perf_counter() - start, result


def load_plan(path):
    plan = json.loads(path.read_text())
    if not isinstance(plan, dict) or set(plan) != {c[0] for c in COHORTS}:
        raise ValueError('plan must contain exactly the configured cohorts')
    for name, width, scale, _ in COHORTS:
        cell = plan[name]
        if not isinstance(cell, dict) or type(cell.get('rows')) is not int or cell['rows'] not in CANDIDATES:
            raise ValueError(f'{name}: rows must be one of {CANDIDATES}')
        typ, scan, insert = statements(name, width, scale)
        if cell != {'rows': cell['rows'], 'type': typ, 'scan': scan, 'insert': insert}:
            raise ValueError(f'{name}: frozen workload differs from this runner')
    return plan


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--port', type=int, required=True)
    ap.add_argument('--user', default='dump')
    ap.add_argument('--database', default='float_decimal_perf')
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument('--pilot', type=Path, help='write a frozen workload plan; baseline only')
    mode.add_argument('--plan', type=Path, help='run a previously frozen plan')
    ap.add_argument('--output', type=Path, required=True)
    args = ap.parse_args()
    if not re.fullmatch(r'[a-z][a-z0-9_]{0,63}', args.database):
        ap.error('database must be a simple lowercase identifier')
    if args.pilot and args.pilot.exists():
        ap.error('refusing to overwrite an existing pilot plan')
    if args.output.exists() or (args.pilot and args.output.resolve() == args.pilot.resolve()):
        ap.error('output must be a new file distinct from the pilot plan')
    try:
        plan = load_plan(args.plan) if args.plan else {}
    except (OSError, ValueError) as exc:
        ap.error(str(exc))
    report = {'protocol': {'warmups': WARMUPS, 'samples': SAMPLES, 'timeout_s': TIMEOUT}, 'cohorts': []}
    args.output.parent.mkdir(parents=True, exist_ok=True)

    def save():
        args.output.write_text(json.dumps(report, indent=2, default=str) + '\n')

    conn = pymysql.connect(host=args.host, port=args.port, user=args.user,
                           password=os.environ.get('MYSQL_PWD', '111'), autocommit=True,
                           connect_timeout=10, read_timeout=TIMEOUT, write_timeout=TIMEOUT)
    created = False
    try:
        query(conn, f"create database {args.database}")  # refuses existing user data
        created = True
        query(conn, f"use {args.database}")
        query(conn, f"set max_execution_time={TIMEOUT * 1000}")
        for name, width, scale, positive in COHORTS:
            values = [positive[0], -positive[0], positive[1], -positive[1]]
            typ, scan, insert = statements(name, width, scale)
            paths = []
            for value in values:
                path = 'formatting fallback'
                if width > 16 and value.is_integer():
                    path = 'exact integral'
                elif scale <= 19:
                    z = abs(value) * float(10 ** scale)
                    if 0.25 <= z < 2 ** 52 and abs((z - int(z)) - 0.5) > z * 2 ** -50:
                        path = 'existing fast path'
                    elif z < 0.25:
                        path = 'small scaled value (formatting fallback)'
                paths.append(path)
            entry = {'name': name, 'type': typ, 'scan_sql': scan, 'insert_sql': insert,
                     'current_source_paths': paths, 'pilot': []}
            report['cohorts'].append(entry)
            prep_start = time.perf_counter()
            sizes = CANDIDATES if args.pilot else [plan[name]['rows']]
            for rows in sizes:
                entry['source_bits'] = prepare(conn, name, values, rows)
                want = expected(values, scale, rows)
                expected_scan = ((rows, want[0][0], want[-1][0]),)
                # Validate the full converted multiset independently, outside timing.
                cast_table = f"(select cast(d as {typ}) d from src_{name}) converted"
                entry['scan_checksum'] = checked_groups(conn, cast_table, want)
                if args.pilot:
                    seconds, result = timed(conn, scan)
                    if result != expected_scan:
                        raise AssertionError((result, expected_scan))
                    entry['pilot'].append({'rows': rows, 'seconds': seconds})
                    if seconds >= 0.2:
                        break
            entry['rows'] = rows
            entry['prepare_s'] = time.perf_counter() - prep_start
            entry['explain'] = query(conn, 'explain ' + scan)
            query(conn, f"create table dst_{name}(d {typ})")
            entry['target_metadata'] = query(conn, f"show columns from dst_{name}")
            if args.pilot:
                plan[name] = {'rows': rows, 'type': typ, 'scan': scan, 'insert': insert}
            else:
                entry['samples'] = []
                for kind, sql in [('scan', scan), ('insert', insert)]:
                    for index in range(WARMUPS + SAMPLES):
                        if kind == 'insert':
                            query(conn, f"truncate table dst_{name}")  # outside timing
                        sample = {'kind': kind, 'warmup': index < WARMUPS, 'index': index}
                        entry['samples'].append(sample)
                        try:
                            seconds, result = timed(conn, sql)
                            sample['seconds'] = seconds
                            if kind == 'scan' and result != expected_scan:
                                raise AssertionError((result, expected_scan))
                            if kind == 'insert':
                                sample['checksum'] = checked_groups(conn, f'dst_{name}', want)
                        except Exception as exc:
                            sample['error'] = repr(exc)
                            save()
                            raise  # no replacement of failed samples
                        save()
            query(conn, f"drop table dst_{name}")
            query(conn, f"drop table src_{name}")
            save()
            print(name, rows, 'complete', flush=True)
        if args.pilot:
            args.pilot.write_text(json.dumps(plan, indent=2) + '\n')
    except Exception as exc:
        report['error'] = repr(exc)
        raise
    finally:
        try:
            if created:
                query(conn, f"drop database {args.database}")
                report['teardown_absent'] = query(conn, f"select schema_name from information_schema.schemata where schema_name='{args.database}'") == ()
                if not report['teardown_absent']:
                    raise AssertionError('database teardown incomplete')
        finally:
            conn.close()
            save()


if __name__ == '__main__':
    main()
