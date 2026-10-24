# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""SIGKILL/restart tests for an OWNED local service process and owned data.

Requires the test endpoint to be stopped first. Only kills PIDs spawned here.
Crash artifacts are recorded before their explicit, narrowly scoped cleanup.
"""

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import time
import urllib.request
import uuid
from pathlib import Path

import pyarrow as pa
from sqlalchemy import event

from matrixone import Client


def client(port=6001):
    logging.getLogger('matrixone').disabled = True
    return Client(
        host='127.0.0.1',
        port=port,
        user='root',
        password='111',
        database='mo_analytics_test',
        query_timeout=3,
        connection_timeout=2,
        sql_log_mode='off',
    )


def wait_json(path, process, seconds=45):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if path.exists():
            try:
                return json.loads(path.read_text())
            except json.JSONDecodeError:
                pass
        if process.poll() is not None:
            raise RuntimeError('child exited: ' + str(process.returncode))
        time.sleep(0.01)
    raise TimeoutError(str(path))


def child(args):
    c = client()
    directory = Path(args.work) / args.mode
    directory.mkdir(parents=True, exist_ok=True)
    barrier = directory / 'barrier.json'
    release = directory / 'release'
    result = directory / 'result.json'

    def signal_ready(fields):
        barrier.write_text(json.dumps(fields))

    def pause():
        deadline = time.monotonic() + 60
        while not release.exists():
            if time.monotonic() > deadline:
                raise TimeoutError('parent did not release barrier')
            time.sleep(0.01)

    try:
        if args.mode == 'client_external':
            p = directory / 'input.parquet'
            import pyarrow.parquet as pq

            pq.write_table(pa.table({'id': [1, 2]}), p)
            with c.analytics.read_parquet(p) as relation:
                signal_ready({'tables': relation._tables})
                pause()
            return
        if args.mode == 'streaming':
            count = 0
            try:
                with c.analytics.query('SELECT * FROM ' + args.table, batch_size=64, network_timeout=2) as stream:
                    count = next(stream).num_rows
                    signal_ready({'first_batch': count})
                    pause()
                    for batch in stream:
                        count += batch.num_rows
                result.write_text(json.dumps({'rows': count, 'error': None, 'checkedout': c._engine.pool.checkedout()}))
            except Exception as error:
                result.write_text(
                    json.dumps(
                        {'rows': count, 'error': type(error).__name__, 'checkedout': c._engine.pool.checkedout()}
                    )
                )
            return
        armed = False

        def after(conn, cursor, sql, params, context, many):
            nonlocal armed
            if sql.startswith('LOAD DATA INFILE'):
                armed = True

        def commit(conn):
            if armed and args.mode in ('precommit', 'client_precommit'):
                signal_ready({'phase': 'loaded_uncommitted'})
                pause()

        def before(conn, cursor, sql, params, context, many):
            if sql.startswith('LOAD DATA INFILE') and args.mode == 'active_load':
                signal_ready({'phase': 'starting_load'})

        event.listen(c._engine, 'after_cursor_execute', after)
        event.listen(c._engine, 'before_cursor_execute', before)
        event.listen(c._engine, 'commit', commit)
        data = pa.table({'id': range(args.rows), 'payload': ['x' * 256] * args.rows})
        try:
            c.analytics.upload_arrow(args.table, data, directory, create=False, batch_size=32768)
            if args.mode == 'postcommit':
                signal_ready({'phase': 'commit_acknowledged'})
                pause()
            result.write_text(json.dumps({'error': None, 'committed': True, 'checkedout': c._engine.pool.checkedout()}))
        except Exception as error:
            result.write_text(
                json.dumps(
                    {
                        'error': type(error).__name__,
                        'committed': getattr(error, 'committed', 'unspecified'),
                        'checkedout': c._engine.pool.checkedout(),
                    }
                )
            )
    finally:
        c.disconnect()


class Service:
    def __init__(self, args):
        self.args = args
        self.process = None
        self.run = 0

    def start(self):
        self.run += 1
        sink = open(Path(self.args.work) / ('service-%d.log' % self.run), 'ab')
        self.process = subprocess.Popen(
            [self.args.binary, '-launch', self.args.config], cwd=self.args.cwd, stdout=sink, stderr=sink
        )
        sink.close()
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError('owned service failed to start')
            try:
                c = client()
                c.execute('SELECT 1')
                c.disconnect()
                return
            except Exception:
                time.sleep(0.2)
        raise TimeoutError('service startup')

    def kill(self):
        self.process.kill()
        self.process.wait(timeout=10)

    def stop(self):
        if self.process is not None and self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.kill()


def summary(table):
    c = client()
    try:
        with c.analytics.query('SELECT count(*) n,sum(id) total,min(id) lo,max(id) hi FROM ' + table) as stream:
            return stream.to_arrow().to_pylist()[0]
    finally:
        c.disconnect()


def main(args):
    work = Path(args.work)
    work.mkdir(parents=True, exist_ok=True)
    service = Service(args)
    cases = []
    tables = []
    children = []
    try:
        service.start()
        for mode in ['precommit', 'postcommit', 'client_precommit', 'active_load', 'streaming', 'client_external']:
            directory = work / mode
            directory.mkdir(exist_ok=True)
            table = 'chaos_' + uuid.uuid4().hex
            tables.append(table)
            c = client()
            c.execute('CREATE TABLE ' + table + '(id BIGINT PRIMARY KEY,payload TEXT)')
            c.execute("INSERT INTO " + table + " VALUES (-1,'sentinel')")
            rows = 1000000 if mode == 'active_load' else 10000
            if mode == 'streaming':
                rows = 200000
                c.analytics.upload_arrow(
                    table, pa.table({'id': range(rows), 'payload': ['x' * 256] * rows}), directory, create=False
                )
            c.disconnect()
            sink = open(directory / 'child.log', 'wb')
            process = subprocess.Popen(
                [
                    sys.executable,
                    __file__,
                    '--child',
                    '--mode',
                    mode,
                    '--table',
                    table,
                    '--rows',
                    str(rows),
                    '--work',
                    str(work),
                ],
                cwd=args.cwd,
                stdout=sink,
                stderr=sink,
            )
            sink.close()
            children.append(process)
            barrier = wait_json(directory / 'barrier.json', process)
            if mode.startswith('client_'):
                process.kill()
                process.wait(timeout=5)
            else:
                if mode == 'active_load':
                    pinned = False
                    end = time.monotonic() + 15
                    while time.monotonic() < end and not (directory / 'result.json').exists():
                        text = urllib.request.urlopen('http://127.0.0.1:7001/metrics', timeout=2).read().decode()
                        if any(
                            l.startswith('mo_arrow_load_pinned_bytes ') and float(l.split()[1]) > 0
                            for l in text.splitlines()
                        ):
                            pinned = True
                            break
                        time.sleep(0.005)
                    assert pinned, 'failed to hit actual live Arrow reader'
                    barrier['live_arrow_reader_observed'] = True
                service.kill()
                (directory / 'release').touch()
                process.wait(timeout=15)
                service.start()
            info = None
            if not mode.startswith('client_'):
                info = json.loads((directory / 'result.json').read_text())
                assert info['checkedout'] == 0, info
                if mode == 'streaming':
                    assert info['error'] and 0 < info['rows'] < rows, info
                elif mode == 'postcommit':
                    assert info['committed'] is True, info
                else:
                    assert info['error'], info
            if mode == 'client_external':
                c = client()
                remaining = {r[0] for r in c.execute('SHOW TABLES').rows}
                assert set(barrier['tables']).issubset(remaining)
                for name in barrier['tables']:
                    assert name.startswith('mo_py_files_') and len(name) == 44
                    c.execute('DROP TABLE `' + name + '`')
                c.disconnect()
            expected = rows + 1 if mode in ['postcommit', 'streaming'] else 1
            end = time.monotonic() + 10
            while True:
                state = summary(table)
                if state['n'] == expected or time.monotonic() > end:
                    break
                time.sleep(0.1)
            assert state['n'] == expected, state
            expected_sum = rows * (rows - 1) // 2 - 1 if expected > 1 else -1
            assert state['total'] == expected_sum, state
            orphans = list(directory.glob('mo_py_arrow_*'))
            if mode == 'client_precommit':
                assert orphans, 'SIGKILL must record interrupted staging artifact'
            for p in orphans:
                p.unlink()
            cases.append(
                {
                    'mode': mode,
                    'barrier': barrier,
                    'child': info,
                    'recovered': {k: str(v) for k, v in state.items()},
                    'orphan_staging_files_recorded_and_cleaned': len(orphans),
                    'passed': True,
                }
            )
            (work / 'chaos.json').write_text(json.dumps({'cases': cases, 'passed': False}, indent=2) + '\n')
            print('PASS ' + mode, flush=True)
        (work / 'chaos.json').write_text(json.dumps({'cases': cases, 'passed': True}, indent=2) + '\n')
    finally:
        for p in children:
            if p.poll() is None:
                p.kill()
                p.wait(timeout=5)
        if service.process is not None and service.process.poll() is None:
            c = client()
            for table in tables:
                c.execute('DROP TABLE IF EXISTS ' + table)
            c.disconnect()
        service.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary')
    parser.add_argument('--config')
    parser.add_argument('--cwd')
    parser.add_argument('--work', required=True)
    parser.add_argument('--child', action='store_true')
    parser.add_argument('--mode')
    parser.add_argument('--table')
    parser.add_argument('--rows', type=int, default=10000)
    args = parser.parse_args()
    if args.child:
        child(args)
    else:
        main(args)
