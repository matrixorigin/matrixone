# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Real-service concurrency and constrained-memory stress with inspectable JSON.

Run against an isolated local test database. All table names are owned UUIDs.
The process constrains only its own address space and file descriptor limits.
"""

import argparse
import json
import logging
import os
import random
import resource
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from matrixone import Client


def client():
    logging.getLogger('matrixone').disabled = True
    return Client(
        host='127.0.0.1',
        port=int(os.environ.get('MO_PORT', '6001')),
        user='root',
        password='111',
        database='mo_analytics_test',
        pool_size=8,
        max_overflow=8,
        pool_timeout=5,
        query_timeout=10,
        sql_log_mode='off',
    )


def memory(key):
    for line in Path('/proc/self/status').read_text().splitlines():
        if line.startswith(key + ':'):
            return int(line.split()[1]) * 1024
    raise RuntimeError(key)


def pinned():
    data = urllib.request.urlopen('http://127.0.0.1:7001/metrics', timeout=5).read().decode()
    for line in data.splitlines():
        if line.startswith('mo_arrow_load_pinned_bytes '):
            return float(line.split()[1])
    raise RuntimeError('Arrow pinned-byte metric missing')


def read(c, sql):
    with c.analytics.query(sql, batch_size=128) as stream:
        return stream.to_arrow()


def memory_worker(table, mode, rows):
    c = client()
    pc.sum(pa.array([1]))
    c.execute('SELECT 1')
    baseline = memory('VmRSS')
    virtual = memory('VmSize')
    resource.setrlimit(resource.RLIMIT_AS, (virtual + 128 * 1024**2, resource.RLIM_INFINITY))
    start = time.monotonic()
    count = 0
    checksum = 0
    error = None
    try:
        if mode == 'streamed':
            with c.analytics.query('SELECT * FROM `' + table + '`', batch_size=4096) as stream:
                for batch in stream:
                    count += batch.num_rows
                    checksum += pc.sum(batch.column(0)).as_py()
        else:
            result = c.execute('SELECT * FROM `' + table + '`')
            count = len(result.rows)
            checksum = sum(r[0] for r in result.rows)
    except Exception as exc:
        chain = []
        cursor = exc
        while cursor is not None and len(chain) < 8:
            chain.append(type(cursor).__name__)
            cursor = cursor.__context__
        error = {'classes': chain, 'message': str(exc)[:200]}
    peak = memory('VmHWM')
    c.disconnect()
    report = {
        'mode': mode,
        'rows': count,
        'checksum': checksum,
        'expected_rows': rows,
        'error': error,
        'baseline_rss_mib': baseline / 1024**2,
        'peak_rss_mib': peak / 1024**2,
        'virtual_headroom_mib': 128,
        'seconds': time.monotonic() - start,
    }
    print(json.dumps(report))


def memory_pressure(c, args, directory):
    """Delete only a table whose upload completed with an acknowledgement."""
    table = 'soak_memory_' + uuid.uuid4().hex
    created = False
    try:
        data = pa.table({'id': range(args.rows), 'payload': ['x' * 256] * args.rows})
        c.analytics.upload_arrow(table, data, directory)
        created = True
        del data
        workers = []
        for mode in ['streamed', 'buffered']:
            try:
                output = subprocess.check_output(
                    [sys.executable, __file__, '--memory-worker', table, '--mode', mode, '--rows', str(args.rows)],
                    text=True,
                    timeout=30,
                )
                workers.append(json.loads(output.strip().splitlines()[-1]))
            except subprocess.TimeoutExpired:
                workers.append(
                    {
                        'mode': mode,
                        'error': {
                            'classes': ['TimeoutExpired'],
                            'message': '30-second hard deadline under constrained address space',
                        },
                        'virtual_headroom_mib': 128,
                    }
                )
        streamed, buffered = workers
        assert streamed['error'] is None and streamed['rows'] == args.rows
        assert streamed['checksum'] == args.rows * (args.rows - 1) // 2
        assert buffered['error'] and any(
            t in buffered['error']['classes'] for t in ['MemoryError', 'TimeoutExpired']
        ), buffered
        return workers
    except BaseException as error:
        print(
            json.dumps(
                {'attempted_table': table, 'created': created, 'committed': getattr(error, 'committed', 'unspecified')}
            ),
            file=sys.stderr,
        )
        raise
    finally:
        if created:
            c.execute('DROP TABLE IF EXISTS ' + table)


def main(args):
    c = client()
    source = None
    try:
        directory = Path(args.staging_dir)
        directory.mkdir(exist_ok=True, parents=True)
        source = directory / ('soak_' + uuid.uuid4().hex + '.parquet')
        pq.write_table(pa.table({'id': range(257), 'name': ['中文😀'] * 257}), source)
        pc.sum(pa.array([1]))
        read(c, 'SELECT 1 AS n')
        warm = 'soak_warm_' + uuid.uuid4().hex
        c.analytics.upload_arrow(warm, pa.table({'id': [1]}), directory)
        c.execute('DROP TABLE ' + warm)
        initial = {'rss': memory('VmRSS'), 'fds': len(os.listdir('/proc/self/fd')), 'pinned': pinned()}
        before = dict(initial)
        old_as = resource.getrlimit(resource.RLIMIT_AS)
        old_fds = resource.getrlimit(resource.RLIMIT_NOFILE)
        # Native allocators reserve large virtual arenas per thread. Bound resident
        # growth separately; do not mistake unused virtual reservations for RSS.
        resource.setrlimit(resource.RLIMIT_AS, (memory('VmSize') + 1024 * 1024**2, old_as[1]))
        resource.setrlimit(resource.RLIMIT_NOFILE, (64, old_fds[1]))
        start = time.monotonic()
        measured_start = start + args.warmup_seconds
        deadline = measured_start + args.seconds
        lock = threading.Lock()
        counts = {}
        warmup_counts = {}
        errors = []
        owned = set()
        samples = []

        def work(worker):
            rng = random.Random(1000 + worker)
            while time.monotonic() < deadline:
                op = rng.choice(['stream', 'stream', 'file', 'upload', 'reject'])
                name = None
                try:
                    if op == 'stream':
                        with c.analytics.query('SELECT 1 AS n UNION ALL SELECT 2 AS n', batch_size=1) as out:
                            assert next(out)['n'].to_pylist() == [1]
                    elif op == 'file':
                        with c.analytics.read_parquet(source) as rel:
                            with rel.query('SELECT count(*) n,sum(id) total FROM ' + rel.sql + ' AS f') as out:
                                assert out.to_arrow().to_pylist() == [{'n': 257, 'total': 32896}]
                    elif op == 'upload':
                        name = 'soak_' + uuid.uuid4().hex
                        c.analytics.upload_arrow(name, pa.table({'id': range(33)}), directory, batch_size=7)
                        with lock:
                            owned.add(name)
                        assert read(c, 'SELECT count(*) n,sum(id) total FROM ' + name).to_pylist() == [
                            {'n': 33, 'total': 528}
                        ]
                        c.execute('DROP TABLE ' + name)
                        with lock:
                            owned.remove(name)
                    else:
                        try:
                            with c.analytics.query('SELECT 1000 AS n', schema=pa.schema([('n', pa.int8())])) as out:
                                out.to_arrow()
                        except pa.ArrowInvalid:
                            pass
                        else:
                            raise AssertionError('overflow was accepted')
                    with lock:
                        counter = counts if time.monotonic() >= measured_start else warmup_counts
                        counter[op] = counter.get(op, 0) + 1
                except Exception as exc:
                    with lock:
                        errors.append(
                            {
                                'worker': worker,
                                'operation': op,
                                'attempted_table': name,
                                'error': type(exc).__name__ + ': ' + str(exc)[:200],
                            }
                        )
                    break

        try:
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = [pool.submit(work, i) for i in range(args.workers)]
                while not all(f.done() for f in futures):
                    sample = {'rss': memory('VmRSS'), 'fds': len(os.listdir('/proc/self/fd'))}
                    if time.monotonic() >= measured_start:
                        if not samples:
                            before = {'rss': sample['rss'], 'fds': sample['fds'], 'pinned': None}
                        samples.append(sample)
                    if int((time.monotonic() - start) * 4) % 20 == 0:
                        print(
                            'soak operations='
                            + str(sum(counts.values()))
                            + ' warmup='
                            + str(sum(warmup_counts.values()))
                            + ' rss_mib='
                            + str(round(sample['rss'] / 1024**2, 1)),
                            file=sys.stderr,
                            flush=True,
                        )
                    time.sleep(0.25)
                for f in futures:
                    f.result()
        finally:
            resource.setrlimit(resource.RLIMIT_AS, old_as)
            resource.setrlimit(resource.RLIMIT_NOFILE, old_fds)
        after = {
            'rss': memory('VmRSS'),
            'fds': len(os.listdir('/proc/self/fd')),
            'pinned': pinned(),
            'checkedout': c._engine.pool.checkedout(),
        }
        for name in list(owned):
            c.execute('DROP TABLE IF EXISTS ' + name)
        external = [r[0] for r in c.execute('SHOW TABLES').rows if r[0].startswith('mo_py_files_')]
        soak = {
            'seconds': args.seconds,
            'warmup_seconds': args.warmup_seconds,
            'workers': args.workers,
            'counts': counts,
            'warmup_counts': warmup_counts,
            'errors': errors,
            'initial': initial,
            'before': before,
            'after': after,
            'arrow_live_bytes': pa.total_allocated_bytes(),
            'peak_rss_mib': max(s['rss'] for s in samples) / 1024**2,
            'peak_fds': max(s['fds'] for s in samples),
            'fd_soft_limit': 64,
            'as_headroom_mib': 1024,
            'external_leaks': external,
            'owned_tables_left': sorted(owned),
        }
        Path(args.output).write_text(json.dumps({'soak': soak, 'passed': False}, indent=2) + '\n')
        assert not errors and not owned and not external
        assert after['checkedout'] == 0 and after['pinned'] == 0
        assert pa.total_allocated_bytes() == 0
        assert after['fds'] <= before['fds'] + 16
        assert max(s['rss'] for s in samples) - before['rss'] < 64 * 1024**2
        workers = memory_pressure(c, args, directory)
        report = {'soak': soak, 'memory_pressure': workers, 'passed': True}
        Path(args.output).write_text(json.dumps(report, indent=2) + '\n')
        print(
            json.dumps(
                {'counts': counts, 'operations': sum(counts.values()), 'memory_pressure': workers, 'passed': True},
                indent=2,
            )
        )
    finally:
        try:
            c.disconnect()
        finally:
            if source is not None:
                source.unlink(missing_ok=True)


def memory_only(args):
    """Resume the independent pressure stage without repeating a passed soak."""
    previous = json.loads(Path(args.output).read_text())
    c = client()
    try:
        workers = memory_pressure(c, args, Path(args.staging_dir))
        previous.update({'memory_pressure': workers, 'passed': True})
        Path(args.output).write_text(json.dumps(previous, indent=2) + '\n')
        print(json.dumps(workers, indent=2))
    finally:
        c.disconnect()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--staging-dir')
    parser.add_argument('--output')
    parser.add_argument('--seconds', type=int, default=90)
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--warmup-seconds', type=int, default=30)
    parser.add_argument('--rows', type=int, default=1000000)
    parser.add_argument('--memory-worker')
    parser.add_argument('--mode', choices=['streamed', 'buffered'])
    parser.add_argument('--memory-only', action='store_true')
    args = parser.parse_args()
    if args.memory_worker:
        memory_worker(args.memory_worker, args.mode, args.rows)
    elif args.memory_only:
        memory_only(args)
    else:
        main(args)
