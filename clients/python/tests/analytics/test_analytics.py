# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Run with MO_ANALYTICS_ONLINE=1 for real MatrixOne integration tests.

No mocked database is used by online cases. DuckDB is an independent result
oracle for file aggregates, not the execution backend of this SDK feature.
"""

import os
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from matrixone import Client
from matrixone.analytics import AnalyticsManager, _identifier, _sql_type, _validate_schema, _wire_schema

pa = pytest.importorskip("pyarrow", minversion="18.0.0")
pq = pytest.importorskip("pyarrow.parquet")


def test_arrow18_json_inference_is_bounded(monkeypatch):
    import pyarrow.json as json

    from matrixone.analytics import _infer_schema

    monkeypatch.delattr(json, "open_json", raising=False)
    reads = []

    class Source:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def read(self, size):
            reads.append(size)
            record = b'{"id":1,"text":"' + b"x" * 2000 + b'"}\n'
            return (record * 2000)[:size]

    fs = SimpleNamespace(open_input_file=lambda path: Source())
    schema = _infer_schema("unused", "jsonline", ",", fs)
    assert schema == pa.schema([("id", pa.int64()), ("text", pa.string())])
    assert reads == [1 << 20]


def test_arrow18_json_oversize_record_rejected(monkeypatch, tmp_path):
    import pyarrow.json as json

    from matrixone.analytics import _infer_schema

    monkeypatch.delattr(json, "open_json", raising=False)
    path = tmp_path / "large.jsonline"
    path.write_bytes(b'{"text":"' + b"x" * (1 << 20) + b'"}\n')
    with pytest.raises(ValueError, match="1 MiB"):
        _infer_schema(str(path), "jsonline", ",", None)


@pytest.fixture(scope="module")
def client():
    if os.environ.get("MO_ANALYTICS_ONLINE") != "1":
        pytest.skip("Set MO_ANALYTICS_ONLINE=1 to run against a real MatrixOne")
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


@pytest.fixture
def analytics(client):
    yield client.analytics
    # Every test must leave no owned external tables or open pooled connections.
    tables = client.execute("SHOW TABLES").rows
    assert not [row for row in tables if row[0].startswith("mo_py_files_")]
    assert client._engine.pool.checkedout() == 0


def _query(analytics, sql, **kwargs):
    with analytics.query(sql, **kwargs) as result:
        return result.to_arrow()


@pytest.mark.parametrize(
    "dtype,sql",
    [
        (pa.int8(), "TINYINT"),
        (pa.uint64(), "BIGINT UNSIGNED"),
        (pa.decimal128(8, 2), "DECIMAL(19,2)"),
        (pa.timestamp("ns", tz="Asia/Shanghai"), "TIMESTAMP(6)"),
        (pa.string(), "TEXT"),
        (pa.binary(), "BLOB"),
    ],
)
def test_type_mapping(dtype, sql):
    assert _sql_type(dtype) == sql


@pytest.mark.parametrize(
    "dtype", [pa.list_(pa.int64()), pa.struct([("a", pa.int64())]), pa.null(), pa.decimal256(50, 2)]
)
def test_unsupported_types_fail(dtype):
    with pytest.raises(TypeError):
        _sql_type(dtype)


def test_schema_and_identifiers():
    assert _identifier("a`b") == "`a``b`"
    for names in [["A", "a"], ["__mo_filepath"], ["bad\x00name"], []]:
        with pytest.raises(ValueError):
            _validate_schema(pa.schema([(n, pa.int64()) for n in names]))
    wire = _wire_schema(pa.schema([("d", pa.decimal128(10, 2)), ("t", pa.timestamp("ns", "Asia/Shanghai"))]))
    assert wire.types == [pa.decimal128(19, 2), pa.timestamp("us", "UTC")]


def test_disconnected_and_invalid_inputs(tmp_path):
    a = AnalyticsManager(SimpleNamespace(_engine=None))
    with pytest.raises(RuntimeError):
        a.query("SELECT 1")
    with pytest.raises(FileNotFoundError):
        a.read_parquet(str(tmp_path / "*.parquet"))
    with pytest.raises(ValueError):
        a.read_csv([], delimiter='"')


@pytest.mark.parametrize("format", ["parquet", "csv", "jsonline"])
def test_file_query_duckdb_oracle(analytics, tmp_path, format):
    import duckdb

    rows = [{"id": 1, "name": "甲,乙", "amount": 1.5}, {"id": 2, "name": 'a"b', "amount": 2.5}]
    if format == "parquet":
        path = tmp_path / "a.parquet"
        pq.write_table(pa.Table.from_pylist(rows), path)
        oracle = duckdb.sql("SELECT count(*), sum(amount) FROM read_parquet(?)", params=[str(path)]).fetchall()[0]
    elif format == "csv":
        path = tmp_path / "a.csv"
        path.write_text('id,name,amount\n1,"甲,乙",1.5\n2,"a""b",2.5\n')
        oracle = duckdb.sql("SELECT count(*), sum(amount) FROM read_csv(?)", params=[str(path)]).fetchall()[0]
    else:
        import json

        path = tmp_path / "a.jsonl"
        path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows))
        oracle = duckdb.sql("SELECT count(*), sum(amount) FROM read_json(?)", params=[str(path)]).fetchall()[0]
    with analytics.read_files(str(path), format=format) as relation:
        with relation.query("SELECT count(*) AS n, sum(amount) AS total FROM " + relation.sql + " AS f") as result:
            assert tuple(result.to_arrow().to_pylist()[0].values()) == oracle
        with relation.query("SELECT * FROM " + relation.sql + " AS f ORDER BY id") as result:
            assert result.to_arrow().to_pylist() == rows
    with pytest.raises(RuntimeError):
        _ = relation.sql


def test_multifile_union_and_hive(analytics, tmp_path):
    p1 = tmp_path / "region=01" / "a.parquet"
    p2 = tmp_path / "region=02" / "b.parquet"
    p1.parent.mkdir()
    p2.parent.mkdir()
    pq.write_table(pa.table({"id": [1], "amount": [1.5]}), p1)
    pq.write_table(pa.table({"id": [2], "extra": ["new"]}), p2)
    with pytest.raises(ValueError, match="schemas differ"):
        analytics.read_parquet([str(p1), str(p2)])
    with analytics.read_parquet([str(p1), str(p2)], union_by_name=True, hive_partitioning=True) as relation:
        result = _query(analytics, "SELECT * FROM " + relation.sql + " AS f ORDER BY id")
        assert result.to_pylist() == [
            {"id": 1, "amount": 1.5, "region": "01", "extra": None},
            {"id": 2, "amount": None, "region": "02", "extra": "new"},
        ]


def test_glob_and_quoted_paths_and_columns(analytics, tmp_path):
    for i in range(3):
        pq.write_table(pa.table({"select": [i], "a`b": [i * 2]}), tmp_path / ("part_%d's%%.parquet" % i))
    with analytics.read_parquet(str(tmp_path / "*.parquet")) as relation:
        result = _query(analytics, "SELECT count(*) AS n, sum(`a``b`) AS s FROM " + relation.sql + " AS f")
        assert result.to_pylist() == [{"n": 3, "s": Decimal(6)}]


def test_empty_parquet_and_explicit_all_null(analytics, tmp_path):
    schema = pa.schema([("id", pa.int64()), ("name", pa.string())])
    pq.write_table(pa.Table.from_batches([], schema), tmp_path / "empty.parquet")
    with analytics.read_parquet(str(tmp_path / "empty.parquet")) as relation:
        result = _query(analytics, "SELECT * FROM " + relation.sql + " AS f")
        assert result.num_rows == 0
        assert result.schema.names == schema.names
    result = _query(analytics, "SELECT CAST(NULL AS BIGINT) AS id, CAST(NULL AS VARCHAR(20)) AS name")
    assert result.schema.types == schema.types
    assert result.to_pylist() == [{"id": None, "name": None}]


def test_query_errors_cleanup_and_pool_reuse(analytics, tmp_path):
    pq.write_table(pa.table({"id": [1, 2, 3]}), tmp_path / "a.parquet")
    with pytest.raises(Exception):
        with analytics.read_parquet(str(tmp_path / "a.parquet")) as relation:
            with relation.query("SELECT missing FROM " + relation.sql + " AS f") as result:
                result.to_arrow()
    with pytest.raises(ValueError, match="names/order"):
        analytics.query("SELECT 1 AS id", schema=pa.schema([("other", pa.int64())]))
    assert _query(analytics, "SELECT 7 AS n").to_pylist() == [{"n": 7}]


def test_arrow_upload_scalar_roundtrip(analytics, tmp_path):
    schema = pa.schema(
        [
            ("i", pa.int8()),
            ("u", pa.uint64()),
            ("s", pa.string()),
            ("b", pa.binary()),
            ("ok", pa.bool_()),
            ("d", pa.decimal128(8, 2)),
            ("day", pa.date32()),
            ("naive", pa.timestamp("us")),
            ("utc", pa.timestamp("us", "UTC")),
            ("clock", pa.time64("us")),
        ]
    )
    row = {
        "i": -128,
        "u": 2**64 - 1,
        "s": "中文\x00'\\%",
        "b": b"\x00\xff",
        "ok": True,
        "d": Decimal("123.45"),
        "day": date(2026, 10, 13),
        "naive": datetime(2026, 10, 13, 1, 2, 3, 456789),
        "utc": datetime(2026, 10, 13, 1, 2, 3, 456789, tzinfo=timezone.utc),
        "clock": time(1, 2, 3, 456789),
    }
    data = pa.Table.from_pylist([row, {k: None for k in row}], schema=schema)
    try:
        assert analytics.upload_arrow("analytics_roundtrip", data, tmp_path, batch_size=1) == 2
        result = _query(analytics, "SELECT * FROM analytics_roundtrip ORDER BY i IS NULL", schema=schema, batch_size=1)
        assert result.equals(data)
        assert not list(tmp_path.glob("mo_py_arrow_*"))
    finally:
        analytics.client.execute("DROP TABLE IF EXISTS analytics_roundtrip")


def test_dataframe_pandas_polars_and_append(analytics, tmp_path):
    import pandas as pd
    import polars as pl

    dataframe = pd.DataFrame({"id": pd.array([1, None], dtype="Int64"), "name": ["甲", "乙"]})
    try:
        assert analytics.upload_dataframe("analytics_df", dataframe, tmp_path) == 2
        assert (
            analytics.upload_dataframe(
                "analytics_df", pl.DataFrame({"id": [3], "name": ["丙"]}), tmp_path, create=False
            )
            == 1
        )
        result = _query(analytics, "SELECT * FROM analytics_df ORDER BY id IS NULL, id")
        assert result.to_pylist() == [{"id": 1, "name": "甲"}, {"id": 3, "name": "丙"}, {"id": None, "name": "乙"}]
    finally:
        analytics.client.execute("DROP TABLE IF EXISTS analytics_df")


def test_arrow_reader_batches_and_empty_table(analytics, tmp_path):
    schema = pa.schema([("id", pa.int64())])
    reader = pa.RecordBatchReader.from_batches(schema, [pa.record_batch({"id": [1, 2]}), pa.record_batch({"id": [3]})])
    try:
        assert analytics.upload_arrow("analytics_reader", reader, tmp_path, batch_size=1) == 3
        with analytics.query("SELECT id FROM analytics_reader ORDER BY id", batch_size=2) as stream:
            batches = list(stream)
        assert [b.num_rows for b in batches] == [2, 1]
        assert analytics.upload_arrow("analytics_empty", pa.Table.from_batches([], schema), tmp_path) == 0
        assert _query(analytics, "SELECT * FROM analytics_empty").num_rows == 0
    finally:
        analytics.client.execute("DROP TABLE IF EXISTS analytics_reader")
        analytics.client.execute("DROP TABLE IF EXISTS analytics_empty")


def test_upload_failures_atomic_and_cleanup(analytics, tmp_path):
    try:
        analytics.client.execute("CREATE TABLE analytics_atomic (id BIGINT PRIMARY KEY)")
        analytics.client.execute("INSERT INTO analytics_atomic VALUES (1)")
        with pytest.raises(Exception):
            analytics.upload_arrow(
                "analytics_atomic", pa.table({"id": [2, 1, 3]}), tmp_path, create=False, batch_size=1
            )
        assert _query(analytics, "SELECT * FROM analytics_atomic").to_pylist() == [{"id": 1}]
        with pytest.raises(Exception):
            analytics.upload_arrow("analytics_atomic", pa.table({"id": [9]}), tmp_path, create=True)
        assert _query(analytics, "SELECT * FROM analytics_atomic").num_rows == 1
        with pytest.raises(ValueError, match="byte limit"):
            analytics.upload_arrow("analytics_limit", pa.table({"s": ["x" * 10000]}), tmp_path, max_staging_bytes=100)
        data = pa.table({"t": pa.array([1], type=pa.timestamp("ns"))})
        with pytest.raises(pa.ArrowInvalid):
            analytics.upload_arrow("analytics_precision", data, tmp_path)
        assert not list(tmp_path.glob("mo_py_arrow_*"))
        names = {r[0] for r in analytics.client.execute("SHOW TABLES").rows}
        assert not names & {"analytics_limit", "analytics_precision"}
    finally:
        analytics.client.execute("DROP TABLE IF EXISTS analytics_atomic")


def test_early_close_and_bound_batch(analytics, tmp_path):
    data = pa.table({"id": range(20000), "text": ["x" * 50] * 20000})
    try:
        analytics.upload_arrow("analytics_stream", data, tmp_path)
        with analytics.query("SELECT * FROM analytics_stream", batch_size=127) as stream:
            assert next(stream).num_rows == 127
        assert analytics.engine.pool.checkedout() == 0
        with analytics.query("SELECT * FROM analytics_stream", batch_size=127) as stream:
            count, largest = 0, 0
            for batch in stream:
                count += batch.num_rows
                largest = max(largest, batch.num_rows)
        assert count == 20000 and largest == 127
        assert _query(analytics, "SELECT 1 AS n").num_rows == 1
    finally:
        analytics.client.execute("DROP TABLE IF EXISTS analytics_stream")


def test_file_import_and_timezone_pool_isolation(analytics, tmp_path):
    pq.write_table(pa.table({"id": [1, 2]}), tmp_path / "a.parquet")
    try:
        analytics.client.execute("CREATE TABLE analytics_import(id BIGINT)")
        with analytics.read_parquet(str(tmp_path / "a.parquet")) as relation:
            assert relation.import_into("analytics_import") == 2
        assert _query(analytics, "SELECT count(*) AS n FROM analytics_import").to_pylist() == [{"n": 2}]
        with analytics.engine.begin() as conn:
            conn.exec_driver_sql("SET time_zone = '+08:00'")
            old_mode = conn.exec_driver_sql("SELECT @@sql_mode").scalar()
            old_pk_check = conn.exec_driver_sql("SELECT @@mo_pk_check_by_dn").scalar()
        analytics.upload_arrow("analytics_tz", pa.table({"id": [1]}), tmp_path)
        with analytics.engine.begin() as conn:
            assert conn.exec_driver_sql("SELECT @@time_zone").scalar() == "+08:00"
        _query(analytics, "SELECT 1 AS n")
        with analytics.engine.begin() as conn:
            assert conn.exec_driver_sql("SELECT @@time_zone").scalar() == "+08:00"
            assert conn.exec_driver_sql("SELECT @@sql_mode").scalar() == old_mode
            assert conn.exec_driver_sql("SELECT @@mo_pk_check_by_dn").scalar() == old_pk_check
        assert analytics.engine.pool.checkedout() == 0
    finally:
        analytics.client.execute("DROP TABLE IF EXISTS analytics_import")
        analytics.client.execute("DROP TABLE IF EXISTS analytics_tz")


def test_csv_explicit_schema_header_mismatch(analytics, tmp_path):
    path = tmp_path / "header.csv"
    path.write_text("b,a\n10,20\n")
    with pytest.raises(ValueError, match="names/order"):
        analytics.read_csv(path, schema=pa.schema([("a", pa.int64()), ("b", pa.int64())]))


def test_reject_lossy_union_promotion(analytics, tmp_path):
    for dtype in [pa.float64(), pa.uint64()]:
        pq.write_table(pa.table({"id": pa.array([1], type=pa.int64())}), tmp_path / "a.parquet")
        pq.write_table(pa.table({"id": pa.array([1], type=dtype)}), tmp_path / "b.parquet")
        with pytest.raises(ValueError, match="promotion"):
            analytics.read_parquet(str(tmp_path / "*.parquet"), union_by_name=True)


def test_dictionary_and_timezone_normalization(analytics, tmp_path):
    table = pa.table(
        {
            "category": pa.array(["a", "b", None]).dictionary_encode(),
            "instant": pa.array(
                [datetime(2026, 10, 13, 0, 0, tzinfo=timezone(timedelta(hours=8))), None, None],
                type=pa.timestamp("ns", "Asia/Shanghai"),
            ),
        }
    )
    try:
        analytics.upload_arrow("analytics_dictionary", table, tmp_path)
        result = _query(analytics, "SELECT * FROM analytics_dictionary ORDER BY category IS NULL, category")
        assert result["category"].to_pylist() == ["a", "b", None]
        assert result["instant"].to_pylist() == [datetime(2026, 10, 12, 16, 0, tzinfo=timezone.utc), None, None]
    finally:
        analytics.client.execute("DROP TABLE IF EXISTS analytics_dictionary")


def test_protocol_time_duration_and_boolean(analytics):
    table = _query(analytics, "SELECT CAST('-25:00:00' AS TIME) AS span, CAST(1 AS BOOL) AS ok")
    # This CAST expression is advertised as TINYINT by the MySQL protocol.
    assert table.schema.types == [pa.duration("us"), pa.int8()]
    assert table.to_pylist() == [{"span": timedelta(hours=-25), "ok": 1}]
    explicit = _query(analytics, "SELECT CAST(1 AS BOOL) AS ok", schema=pa.schema([("ok", pa.bool_())]))
    assert explicit.to_pylist() == [{"ok": True}]
    with pytest.raises(ValueError, match="clock day"):
        _query(analytics, "SELECT CAST('-25:00:00' AS TIME) AS span", schema=pa.schema([("span", pa.time64("us"))]))


def test_upload_rollback_after_target_creation(analytics, tmp_path):
    with pytest.raises(Exception):
        analytics.upload_arrow(
            "analytics_bad_path", pa.table({"id": [1]}), tmp_path, server_staging_dir=str(tmp_path / "does-not-exist")
        )
    assert "analytics_bad_path" not in {r[0] for r in analytics.client.execute("SHOW TABLES").rows}
    assert not list(tmp_path.glob("mo_py_arrow_*"))


def test_append_type_overflow_rollback(analytics, tmp_path):
    try:
        analytics.client.execute("CREATE TABLE analytics_narrow(id TINYINT)")
        with pytest.raises(Exception):
            analytics.upload_arrow("analytics_narrow", pa.table({"id": [1, 1000]}), tmp_path, create=False)
        assert _query(analytics, "SELECT * FROM analytics_narrow").num_rows == 0
    finally:
        analytics.client.execute("DROP TABLE IF EXISTS analytics_narrow")


def test_partial_relation_creation_cleanup(analytics, tmp_path):
    from sqlalchemy import event

    pq.write_table(pa.table({"id": [1]}), tmp_path / "a.parquet")
    pq.write_table(pa.table({"id": [2]}), tmp_path / "b.parquet")
    creates = []

    def fail_second_ddl(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("CREATE EXTERNAL TABLE"):
            creates.append(statement)
            if len(creates) == 2:
                raise RuntimeError("Injected second-DDL admission failure")

    event.listen(analytics.engine, "before_cursor_execute", fail_second_ddl)
    try:
        with pytest.raises(RuntimeError, match="second-DDL"):
            analytics.read_parquet([str(tmp_path / "a.parquet"), str(tmp_path / "b.parquet")])
    finally:
        event.remove(analytics.engine, "before_cursor_execute", fail_second_ddl)


def test_concurrent_relations(analytics, tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    pq.write_table(pa.table({"id": [1, 2, 3]}), tmp_path / "a.parquet")

    def run(i):
        with analytics.read_parquet(str(tmp_path / "a.parquet")) as relation:
            result = _query(analytics, "SELECT sum(id) AS s FROM " + relation.sql + " AS f")
            return result.to_pylist()[0]["s"]

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(run, range(8))) == [Decimal(6)] * 8


def test_csv_null_and_custom_delimiter(analytics, tmp_path):
    path = tmp_path / "a.csv"
    path.write_text('id|name\n1|\\N\n2|""\n3|"\\N"\n')
    with analytics.read_csv(path, delimiter="|") as relation:
        result = _query(analytics, "SELECT * FROM " + relation.sql + " AS f ORDER BY id")
        # MatrixOne treats \\N as NULL even inside quotes. Match schema inference
        # to the server convention rather than claiming DuckDB CSV equivalence.
        assert result.to_pylist() == [{"id": 1, "name": None}, {"id": 2, "name": ""}, {"id": 3, "name": None}]


def test_hive_default_null(analytics, tmp_path):
    p = tmp_path / "region=__HIVE_DEFAULT_PARTITION__" / "a.parquet"
    p.parent.mkdir()
    pq.write_table(pa.table({"id": [1]}), p)
    with analytics.read_parquet(p, hive_partitioning=True) as relation:
        result = _query(analytics, "SELECT * FROM " + relation.sql + " AS f")
        assert result.to_pylist() == [{"id": 1, "region": None}]


def test_s3_compatible_external_query(analytics, tmp_path):
    endpoint = os.environ.get("MO_ANALYTICS_S3_ENDPOINT")
    if not endpoint:
        pytest.skip("Set MO_ANALYTICS_S3_ENDPOINT for local S3-compatible integration")
    import boto3
    import pyarrow.fs as fs

    # Synthetic credentials belong only to the local test server.
    s3 = boto3.client(
        "s3", endpoint_url=endpoint, aws_access_key_id="test", aws_secret_access_key="test", region_name="us-east-1"
    )
    from contextlib import ExitStack
    import uuid

    bucket = "mo-analytics-" + uuid.uuid4().hex
    stage = "analytics_s3_" + uuid.uuid4().hex
    path = tmp_path / "remote.parquet"
    pq.write_table(pa.table({"id": [1, 2], "amount": [2.5, 3.5]}), path)
    arrow_fs = fs.S3FileSystem(
        access_key="test",
        secret_key="test",
        region="us-east-1",
        endpoint_override=endpoint,
        scheme="http",
        force_virtual_addressing=False,
    )
    with ExitStack() as cleanup:
        s3.create_bucket(Bucket=bucket)
        cleanup.callback(s3.delete_bucket, Bucket=bucket)
        s3.put_object(Bucket=bucket, Key="remote.parquet", Body=path.read_bytes())
        cleanup.callback(s3.delete_object, Bucket=bucket, Key="remote.parquet")
        analytics.client.stage.create(
            stage,
            "s3://" + bucket + "/",
            credentials={
                "AWS_KEY_ID": "test",
                "AWS_SECRET_KEY": "test",
                "AWS_REGION": "us-east-1",
                "ENDPOINT": endpoint,
                "PROVIDER": "minio",
            },
        )
        cleanup.callback(analytics.client.stage.drop, stage, if_exists=True)
        with analytics.read_parquet(
            [bucket + "/remote.parquet"], filesystem=arrow_fs, server_paths=["stage://" + stage + "/remote.parquet"]
        ) as relation:
            result = _query(analytics, "SELECT count(*) AS n, sum(amount) AS total FROM " + relation.sql + " AS f")
            assert result.to_pylist() == [{"n": 2, "total": 6.0}]


def test_parquet_scalar_types_and_large_text(analytics, tmp_path):
    schema = pa.schema(
        [
            ("id", pa.int8()),
            ("u", pa.uint64()),
            ("b", pa.binary()),
            ("ok", pa.bool_()),
            ("d", pa.decimal128(8, 2)),
            ("day", pa.date32()),
            ("s", pa.string()),
            ("instant", pa.timestamp("us", "UTC")),
            ("naive", pa.timestamp("us")),
        ]
    )
    rows = [
        {
            "id": 1,
            "u": 2**64 - 1,
            "b": b"\x00\xff",
            "ok": True,
            "d": Decimal("123.45"),
            "day": date(2026, 10, 13),
            "s": "甲" * 20000,
            "instant": datetime(2026, 10, 13, 1, 2, 3, 456789, tzinfo=timezone.utc),
            "naive": datetime(2026, 10, 13, 1, 2, 3, 456789),
        },
        {k: None for k in schema.names},
    ]
    source = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(source, tmp_path / "types.parquet")
    with analytics.read_parquet(tmp_path / "types.parquet") as relation:
        result = _query(analytics, "SELECT * FROM " + relation.sql + " AS f ORDER BY id IS NULL", schema=schema)
        assert result.equals(source)


def test_input_reader_failure_and_not_null_cleanup(analytics, tmp_path):
    def fail():
        yield pa.record_batch({"id": [1]})
        raise RuntimeError("input reader failed")

    reader = pa.RecordBatchReader.from_batches(pa.schema([("id", pa.int64())]), fail())
    with pytest.raises(RuntimeError, match="input reader failed"):
        analytics.upload_arrow("analytics_reader_fail", reader, tmp_path)
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    bad = pa.Table.from_arrays([pa.array([None], type=pa.int64())], schema=schema)
    with pytest.raises(ValueError, match="non-nullable"):
        analytics.upload_arrow("analytics_not_null", bad, tmp_path)
    assert not list(tmp_path.glob("mo_py_arrow_*"))
    assert not {"analytics_reader_fail", "analytics_not_null"} & {
        r[0] for r in analytics.client.execute("SHOW TABLES").rows
    }


def test_csv_invalid_value_fails_in_scan(analytics, tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text('id\n1\nnot-an-integer\n')
    with analytics.read_csv(path, schema=pa.schema([("id", pa.int64())])) as relation:
        with pytest.raises(Exception):
            _query(analytics, "SELECT * FROM " + relation.sql + " AS f")


@pytest.mark.parametrize("explicit", [False, True])
def test_nanosecond_parquet_rejected_before_loss(analytics, tmp_path, explicit):
    path = tmp_path / "ns.parquet"
    pq.write_table(pa.table({"t": pa.array([1], type=pa.timestamp("ns"))}), path)
    schema = pa.schema([("t", pa.timestamp("us"))]) if explicit else None
    with pytest.raises(ValueError, match="Nanosecond"):
        analytics.read_parquet(path, schema=schema)
