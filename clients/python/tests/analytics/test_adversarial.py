# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Deterministic adversarial tests against a real MatrixOne service.

Every write uses a uniquely named owned table. Tests verify values before cleanup;
cleanup cannot turn a partially committed failure into a passing assertion.
"""

import csv
import io
import math
import os
import random
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import event

from matrixone import Client
from matrixone.analytics import _identifier

pa = pytest.importorskip("pyarrow", minversion="18.0.0")
pq = pytest.importorskip("pyarrow.parquet")


@pytest.fixture(scope="module")
def hard_client():
    if os.environ.get("MO_ANALYTICS_ONLINE") != "1":
        pytest.skip("real database required")
    c = Client(
        host=os.environ.get("MO_HOST", "127.0.0.1"),
        port=int(os.environ.get("MO_PORT", "6001")),
        user=os.environ.get("MO_USER", "root"),
        password=os.environ.get("MO_PASSWORD", "111"),
        database=os.environ.get("MO_DATABASE", "mo_analytics_test"),
        sql_log_mode="off",
    )
    yield c
    c.disconnect()


class Trial:
    def __init__(self, client, directory):
        self.client, self.directory = client, directory
        self.analytics = client.analytics
        self.tables = []

    def table(self):
        name = "adv_" + uuid.uuid4().hex
        self.tables.append(name)
        return name

    def query(self, sql, **kwargs):
        with self.analytics.query(sql, **kwargs) as stream:
            return stream.to_arrow()

    def rows(self, table, **kwargs):
        return self.query("SELECT * FROM " + _identifier(table), **kwargs)

    def exists(self, table):
        return table in {r[0] for r in self.client.execute("SHOW TABLES").rows}


@pytest.fixture
def trial(hard_client, tmp_path):
    t = Trial(hard_client, tmp_path)
    baseline = {r[0] for r in hard_client.execute("SHOW TABLES").rows}
    yield t
    for name in t.tables:
        hard_client.execute("DROP TABLE IF EXISTS " + _identifier(name))
    assert not list(tmp_path.glob("mo_py_arrow_*")), "staging file leak"
    after = {r[0] for r in hard_client.execute("SHOW TABLES").rows}
    assert not {n for n in after - baseline if n.startswith("mo_py_files_")}, "external catalog leak"
    assert hard_client._engine.pool.checkedout() == 0, "connection leak"


INTEGER_TYPES = [pa.int8(), pa.uint8(), pa.int16(), pa.uint16(), pa.int32(), pa.uint32(), pa.int64(), pa.uint64()]


@pytest.mark.parametrize("dtype", INTEGER_TYPES, ids=str)
@pytest.mark.parametrize("batch_size", [1, 7, 1024])
def test_integer_endpoints(trial, dtype, batch_size):
    bits = dtype.bit_width
    low = 0 if pa.types.is_unsigned_integer(dtype) else -(1 << (bits - 1))
    high = (1 << bits) - 1 if low == 0 else (1 << (bits - 1)) - 1
    values = [low, low + 1, 0, 1, high - 1, high, None] * 3
    schema = pa.schema([("v", dtype)])
    data = pa.table({"v": pa.array(values, type=dtype)})
    name = trial.table()
    assert trial.analytics.upload_arrow(name, data, trial.directory, batch_size=batch_size) == len(values)
    result = trial.rows(name, schema=schema, batch_size=batch_size)
    assert sorted(v for v in result["v"].to_pylist() if v is not None) == sorted(v for v in values if v is not None)
    assert result["v"].null_count == values.count(None)


@pytest.mark.parametrize("precision,scale", [(1, 0), (8, 2), (18, 0), (18, 18), (19, 2), (38, 0), (38, 18), (38, 38)])
def test_decimal_extremes(trial, precision, scale):
    from decimal import localcontext

    with localcontext() as ctx:
        ctx.prec = 80
        high = Decimal(10**precision - 1).scaleb(-scale)
        values = [high, -high, Decimal(0).scaleb(-scale), None]
    dtype = pa.decimal128(precision, scale)
    schema = pa.schema([("v", dtype)])
    data = pa.table({"v": pa.array(values, type=dtype)})
    name = trial.table()
    trial.analytics.upload_arrow(name, data, trial.directory, batch_size=1)
    assert sorted(str(v) for v in trial.rows(name, schema=schema)["v"].to_pylist()) == sorted(str(v) for v in values)


@pytest.mark.parametrize("dtype", [pa.float32(), pa.float64()], ids=str)
def test_float_special_values(trial, dtype):
    values = [0.0, -0.0, 1.5, -1.5, math.inf, -math.inf, math.nan, None]
    data = pa.table({"v": pa.array(values, type=dtype)})
    name = trial.table()
    trial.analytics.upload_arrow(name, data, trial.directory, batch_size=1)
    result = trial.rows(name, schema=data.schema)["v"].to_pylist()
    assert len(result) == len(values)
    assert sum(v is None for v in result) == 1
    assert sum(v is not None and math.isnan(v) for v in result) == 1
    assert sorted(math.copysign(1.0, v) for v in result if v is not None and v == 0.0) == [-1.0, 1.0]
    assert sorted(v for v in result if v is not None and not math.isnan(v)) == sorted(
        v for v in values if v is not None and not math.isnan(v)
    )


@pytest.mark.parametrize("seed", range(16))
def test_random_mixed_roundtrip(trial, seed):
    rng = random.Random(seed)
    n = 157
    rows = []
    text = ["", "中文😀", "e\u0301", "é", "\x00", "\\N", "a'b\"c`%\\", "\r\n\t", "x" * 16384]
    for i in range(n):
        rows.append(
            {
                "id": i,
                "signed": rng.randint(-(2**63), 2**63 - 1),
                "unsigned": rng.randrange(2**64),
                "text": rng.choice(text) if rng.random() > 0.2 else None,
                "binary": bytes(rng.randrange(256) for _ in range(rng.randrange(24))) if i % 7 else None,
                "decimal": Decimal(rng.randint(-99999999, 99999999)).scaleb(-4),
                "ok": rng.choice([True, False, None]),
                "f": rng.uniform(-1e20, 1e20),
            }
        )
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("signed", pa.int64()),
            ("unsigned", pa.uint64()),
            ("text", pa.string()),
            ("binary", pa.binary()),
            ("decimal", pa.decimal128(18, 4)),
            ("ok", pa.bool_()),
            ("f", pa.float64()),
        ]
    )
    data = pa.Table.from_pylist(rows, schema=schema)
    name = trial.table()
    trial.analytics.upload_arrow(name, data, trial.directory, batch_size=rng.choice([1, 3, 17, 128]))
    result = trial.query("SELECT * FROM " + name + " ORDER BY id", schema=schema, batch_size=rng.choice([1, 11, 64]))
    assert result.equals(data), "seed=%d" % seed


@pytest.mark.parametrize("format", ["parquet", "csv", "jsonline"])
@pytest.mark.parametrize("seed", range(8))
def test_file_differential(trial, format, seed):
    import json

    import duckdb

    rng = random.Random(seed)
    values = [{"id": i, "grp": rng.choice(["中文", "x", "z"]), "v": rng.randrange(-100000, 100000)} for i in range(113)]
    p = trial.directory / ("source." + format)
    if format == "parquet":
        pq.write_table(pa.Table.from_pylist(values), p, row_group_size=7, compression="zstd")
        reader = "read_parquet"
    elif format == "csv":
        with p.open("w", newline="") as sink:
            w = csv.DictWriter(sink, fieldnames=["id", "grp", "v"])
            w.writeheader()
            w.writerows(values)
        reader = "read_csv"
    else:
        p.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in values))
        reader = "read_json"
    oracle = duckdb.sql(
        "SELECT grp, count(*) n, sum(v) total FROM " + reader + "(?) GROUP BY grp ORDER BY grp", params=[str(p)]
    ).fetchall()
    with trial.analytics.read_files(p, format=format) as rel:
        result = trial.query("SELECT grp, count(*) n, sum(v) total FROM " + rel.sql + " AS f GROUP BY grp ORDER BY grp")
        assert [tuple(r.values()) for r in result.to_pylist()] == oracle
        rows = trial.query("SELECT * FROM " + rel.sql + " AS f ORDER BY id")
        assert rows.to_pylist() == values


@pytest.mark.parametrize("payload", [b'id,name\n1,"unterminated\n', b'id,name\n1,a,extra\n', b'id,name\n1,\xff\n'])
def test_bad_csv_fails_closed(trial, payload):
    p = trial.directory / "bad.csv"
    p.write_bytes(payload)
    with pytest.raises(Exception):
        with trial.analytics.read_csv(p, schema=pa.schema([("id", pa.int64()), ("name", pa.string())])) as rel:
            trial.query("SELECT * FROM " + rel.sql + " AS f")


@pytest.mark.parametrize("payload", [b'{"id":1}\n{"id":', b'{"id":1}\nnot json\n', b'{"id":1}\n{"id":"bad"}\n'])
def test_bad_json_fails_closed(trial, payload):
    p = trial.directory / "bad.jsonl"
    p.write_bytes(payload)
    with pytest.raises(Exception):
        with trial.analytics.read_json(p, schema=pa.schema([("id", pa.int64())])) as rel:
            trial.query("SELECT * FROM " + rel.sql + " AS f")


@pytest.mark.parametrize("cut", [1, 8, 20, 100])
def test_corrupt_parquet_rejected(trial, cut):
    p = trial.directory / "bad.parquet"
    pq.write_table(pa.table({"id": range(100)}), p)
    p.write_bytes(p.read_bytes()[:-cut])
    with pytest.raises(Exception):
        trial.analytics.read_parquet(p)


@pytest.mark.parametrize("values", [[-1, 2**63 - 1], [1, 2**53 + 1]])
def test_union_integer_precision(trial, values):
    p1, p2 = trial.directory / "a.parquet", trial.directory / "b.parquet"
    pq.write_table(pa.table({"v": pa.array(values[:1], type=pa.int32())}), p1)
    pq.write_table(pa.table({"v": pa.array(values[1:], type=pa.int64())}), p2)
    with trial.analytics.read_parquet([p1, p2], union_by_name=True) as rel:
        result = trial.query("SELECT v FROM " + rel.sql + " AS f ORDER BY v")
        assert result["v"].to_pylist() == sorted(values)


def test_union_decimal_does_not_become_float(trial):
    p1, p2 = trial.directory / "a.parquet", trial.directory / "b.parquet"
    pq.write_table(pa.table({"v": pa.array([Decimal("999999999999999999.123456")], type=pa.decimal128(30, 6))}), p1)
    pq.write_table(pa.table({"v": pa.array([1.25], type=pa.float64())}), p2)
    with pytest.raises((ValueError, TypeError)):
        trial.analytics.read_parquet([p1, p2], union_by_name=True)


def test_file_import_strict_under_permissive_session(trial):
    p = trial.directory / "overflow.parquet"
    pq.write_table(pa.table({"id": [1, 1000]}), p)
    name = trial.table()
    trial.client.execute("CREATE TABLE " + name + "(id TINYINT)")
    with trial.client._engine.begin() as c:
        old = c.exec_driver_sql("SELECT @@sql_mode").scalar()
        c.exec_driver_sql("SET sql_mode = ''")
    try:
        with trial.analytics.read_parquet(p) as rel:
            with pytest.raises(Exception):
                rel.import_into(name)
        assert trial.rows(name).num_rows == 0
    finally:
        with trial.client._engine.begin() as c:
            c.exec_driver_sql("SET sql_mode = %s", (old,))


@pytest.mark.parametrize("name", ["x`); DROP TABLE victim; --", "百分%号", "select", "e\u0301", "😀列"])
def test_hostile_column_names(trial, name):
    source = pa.table({name: ["'\\%\x00😀", None]})
    table = trial.table()
    trial.analytics.upload_arrow(table, source, trial.directory)
    result = trial.rows(table, schema=source.schema)
    assert sorted(str(v) for v in result[name].to_pylist()) == sorted(str(v) for v in source[name].to_pylist())


def test_concurrent_same_target_exactly_one_creator(trial):
    table = trial.table()

    def write(i):
        try:
            trial.analytics.upload_arrow(table, pa.table({"id": [i]}), trial.directory)
            return i
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        winners = [v for v in pool.map(write, range(8)) if v is not None]
    assert len(winners) == 1
    assert trial.rows(table)["id"].to_pylist() == winners


def test_concurrent_duplicate_append_no_partial_rows(trial):
    table = trial.table()
    trial.client.execute("CREATE TABLE " + table + "(id BIGINT PRIMARY KEY)")

    def write(i):
        try:
            trial.analytics.upload_arrow(table, pa.table({"id": [0, i + 1]}), trial.directory, create=False)
            return i + 1
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        winners = [v for v in pool.map(write, range(8)) if v is not None]
    assert len(winners) == 1
    assert sorted(trial.rows(table)["id"].to_pylist()) == [0, winners[0]]


def test_rollback_if_late_batch_duplicate(trial):
    table = trial.table()
    trial.client.execute("CREATE TABLE " + table + "(id BIGINT PRIMARY KEY)")
    trial.client.execute("INSERT INTO " + table + " VALUES (7)")
    values = list(range(1000, 21000)) + [7]
    with pytest.raises(Exception):
        trial.analytics.upload_arrow(table, pa.table({"id": values}), trial.directory, create=False, batch_size=127)
    assert trial.rows(table)["id"].to_pylist() == [7]


def test_result_conversion_failure_releases_connection(trial):
    with pytest.raises((pa.ArrowInvalid, pa.ArrowTypeError)):
        trial.query("SELECT 1000 AS n", schema=pa.schema([("n", pa.int8())]))
    assert trial.client._engine.pool.checkedout() == 0
    assert trial.query("SELECT 9 AS n")["n"].to_pylist() == [9]


@pytest.mark.parametrize(
    "sql,dtype",
    [
        ("SELECT CAST(1.5 AS DOUBLE) AS n", pa.int64()),
        ("SELECT CAST(0.1 AS DOUBLE) AS n", pa.float32()),
        ("SELECT CAST(9007199254740993 AS BIGINT) AS n", pa.float64()),
        ("SELECT CAST('2026-10-13 01:02:03.000001' AS DATETIME(6)) AS n", pa.timestamp("ms")),
        ("SELECT CAST('2026-10-13 01:02:03' AS DATETIME(6)) AS n", pa.date32()),
    ],
)
def test_explicit_result_schema_cannot_silently_truncate(trial, sql, dtype):
    with pytest.raises((ValueError, pa.ArrowInvalid)):
        trial.query(sql, schema=pa.schema([("n", dtype)]))


def test_boolean_to_integer_is_lossless(trial):
    assert trial.query('SELECT CAST(1 AS BOOL) AS n', schema=pa.schema([('n', pa.int8())]))['n'].to_pylist() == [1]


def test_small_pool_exhaustion_recovers(trial):
    c = Client(
        host=os.environ.get("MO_HOST", "127.0.0.1"),
        port=int(os.environ.get('MO_PORT', '6001')),
        user=os.environ.get("MO_USER", "root"),
        password=os.environ.get("MO_PASSWORD", "111"),
        database=os.environ.get("MO_DATABASE", "mo_analytics_test"),
        pool_size=1,
        max_overflow=0,
        pool_timeout=0.2,
        sql_log_mode="off",
    )
    from sqlalchemy.exc import TimeoutError

    try:
        with c.analytics.query("SELECT 1 AS n") as held:
            with pytest.raises(TimeoutError):
                c.analytics.query("SELECT 2 AS n")
        with c.analytics.query("SELECT 3 AS n") as result:
            assert result.to_arrow()["n"].to_pylist() == [3]
        assert c._engine.pool.checkedout() == 0
    finally:
        c.disconnect()


def test_session_restore_failure_reports_committed_write(trial):
    table = trial.table()

    def fail_restore(conn, cursor, statement, params, context, executemany):
        if statement == "SET time_zone = %s":
            raise RuntimeError("Injected restore failure")

    event.listen(trial.client._engine, "before_cursor_execute", fail_restore)
    try:
        with pytest.raises(Exception) as caught:
            trial.analytics.upload_arrow(table, pa.table({"id": [123]}), trial.directory)
    finally:
        event.remove(trial.client._engine, "before_cursor_execute", fail_restore)
    assert trial.rows(table)["id"].to_pylist() == [123]
    assert getattr(caught.value, "committed", None) is True, "failure must distinguish already committed data"


def test_all_null_explicit_schema(trial):
    schema = pa.schema([("id", pa.int64()), ("text", pa.string()), ("d", pa.decimal128(8, 2))])
    data = pa.Table.from_pylist([dict.fromkeys(schema.names)] * 33, schema=schema)
    table = trial.table()
    trial.analytics.upload_arrow(table, data, trial.directory, batch_size=1)
    assert trial.rows(table, schema=schema).equals(data)


@pytest.mark.parametrize("format", ["parquet", "csv", "jsonline"])
def test_changed_schema_rejected_before_query(trial, format):
    path = trial.directory / ('source.' + format)
    if format == 'parquet':
        pq.write_table(pa.table({'id': [1]}), path)
    elif format == 'csv':
        path.write_text('id\n1\n')
    else:
        path.write_text('{"id":1}\n')
    with trial.analytics.read_files(path, format=format) as rel:
        if format == 'parquet':
            pq.write_table(pa.table({'wrong': ['lost']}), path)
        elif format == 'csv':
            path.write_text('id,extra\n1,lost\n')
        else:
            path.write_text('{"wrong":"lost"}\n')
        with pytest.raises(Exception):
            with rel.query() as out:
                out.to_arrow()


@pytest.mark.parametrize("format", ['parquet', 'csv', 'jsonline'])
def test_source_deleted_after_creation_fails(trial, format):
    path = trial.directory / ('source.' + format)
    if format == 'parquet':
        pq.write_table(pa.table({'id': [1]}), path)
    elif format == 'csv':
        path.write_text('id\n1\n')
    else:
        path.write_text('{"id":1}\n')
    with trial.analytics.read_files(path, format=format) as rel:
        path.unlink()
        with pytest.raises(Exception):
            rel.query()


def test_csv_late_extra_field_rejected(trial):
    p = trial.directory / 'late.csv'
    p.write_text('id,name\n' + ''.join('%d,%s\n' % (i, 'x' * 40) for i in range(30000)) + '30000,last,extra\n')
    with pytest.raises(ValueError):
        trial.analytics.read_csv(p)


def test_csv_embedded_newline_and_bom(trial):
    p = trial.directory / 'bom.csv'
    p.write_bytes(b'\xef\xbb\xbfid,name\r\n1,"hello\nworld"\r\n2,"a""b"\r\n')
    with trial.analytics.read_csv(p) as rel:
        with rel.query() as out:
            result = out.to_arrow()
        assert result.to_pylist() == [{'id': 1, 'name': 'hello\nworld'}, {'id': 2, 'name': 'a"b'}]


def test_wide_table_roundtrip(trial):
    data = pa.table({'c%d' % i: pa.array([i, None, i + 1], type=pa.int32()) for i in range(256)})
    name = trial.table()
    trial.analytics.upload_arrow(name, data, trial.directory, batch_size=1)
    result = trial.rows(name, schema=data.schema, batch_size=1)
    assert result.equals(data)


def test_many_files_and_limit(trial):
    for i in range(64):
        pq.write_table(pa.table({'id': [i]}), trial.directory / ('%03d.parquet' % i))
    with pytest.raises(ValueError, match='source files'):
        trial.analytics.read_parquet(str(trial.directory / '*.parquet'), max_files=63)
    with trial.analytics.read_parquet(str(trial.directory / '*.parquet')) as rel:
        with rel.query('SELECT count(*) n,sum(id) s FROM ' + rel.sql + ' AS f') as out:
            assert out.to_arrow().to_pylist() == [{'n': 64, 's': Decimal(2016)}]


def test_sparse_nonnullable_schema_merge(trial):
    p1, p2 = trial.directory / 'a.parquet', trial.directory / 'b.parquet'
    pq.write_table(
        pa.Table.from_arrays([pa.array([1])], schema=pa.schema([pa.field('a', pa.int64(), nullable=False)])), p1
    )
    pq.write_table(
        pa.Table.from_arrays([pa.array([2])], schema=pa.schema([pa.field('b', pa.int64(), nullable=False)])), p2
    )
    with trial.analytics.read_parquet([p1, p2], union_by_name=True) as rel:
        assert all(f.nullable for f in rel.schema)
        with rel.query() as out:
            rows = out.to_arrow().to_pylist()
        assert sorted(rows, key=lambda r: r['a'] is None) == [{'a': 1, 'b': None}, {'a': None, 'b': 2}]


@pytest.mark.parametrize("i", range(8))
def test_repeated_partial_result_no_fd_leak(trial, i):
    def fds():
        return len(os.listdir('/proc/self/fd'))

    before = fds()
    for _ in range(20):
        with trial.analytics.query("SELECT 1 AS n UNION ALL SELECT 2 AS n", batch_size=1) as stream:
            assert next(stream)["n"].to_pylist() == [1]
    assert fds() <= before + 2
    assert trial.client._engine.pool.checkedout() == 0
