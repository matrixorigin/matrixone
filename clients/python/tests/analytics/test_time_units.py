# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
import pytest

from matrixone.analytics import ArrowStream


def stream_for(value, dtype):
    # The real Arrow conversion, with the DBAPI's documented TIME timedelta.
    stream = ArrowStream.__new__(ArrowStream)
    stream._closed = stream._complete = False
    stream.batch_size = 8
    stream.schema = pa.schema([('clock', dtype)])
    stream._source_schema = pa.schema([('clock', pa.duration('us'))])
    stream._cursor = SimpleNamespace(fetchmany=Mock(side_effect=[[(value,), (None,)], []]))
    stream.close = Mock()
    return stream


@pytest.mark.parametrize('dtype', [pa.time32('s'), pa.time32('ms'), pa.time64('us'), pa.time64('ns')])
def test_time_rescales_microsecond_source(dtype):
    stream = stream_for(timedelta(seconds=2), dtype)
    batch = next(stream)
    expected = pa.array([2000000, None], type=pa.time64('us')).cast(dtype, safe=True)
    assert batch.column(0).equals(expected)
    assert batch.column(0).to_pylist()[0].second == 2


@pytest.mark.parametrize('dtype', [pa.time32('s'), pa.time32('ms')])
def test_time_rejects_lossy_unit_narrowing_and_closes_stream(dtype):
    stream = stream_for(timedelta(seconds=2, microseconds=1), dtype)
    with pytest.raises(pa.ArrowInvalid, match='lose data'):
        next(stream)
    stream.close.assert_called_once()


@pytest.mark.parametrize('dtype', [pa.time32('s'), pa.time32('ms'), pa.time64('us'), pa.time64('ns')])
def test_native_time_units(analytics, dtype):
    with analytics.query("SELECT CAST('00:00:02' AS TIME) AS clock", schema=pa.schema([('clock', dtype)])) as stream:
        assert stream.to_arrow().column(0).to_pylist()[0].second == 2


def test_native_parquet_default_millisecond_time(analytics, tmp_path):
    import pyarrow.parquet as pq

    expected = pa.table({'clock': pa.array([2000, None], type=pa.time32('ms'))})
    path = tmp_path / 'clock.parquet'
    pq.write_table(expected, path)
    with analytics.read_parquet(path) as files:
        with files.query() as stream:
            actual = stream.to_arrow()
            assert actual.sort_by('clock').equals(expected.sort_by('clock'))


@pytest.mark.parametrize('dtype', [pa.time32('s'), pa.time32('ms')])
def test_native_time_rejects_fractional_narrowing(analytics, dtype):
    with pytest.raises(pa.ArrowInvalid, match='lose data'):
        with analytics.query(
            "SELECT CAST('00:00:02.000001' AS TIME(6)) AS clock", schema=pa.schema([('clock', dtype)])
        ) as stream:
            stream.to_arrow()
    assert analytics.engine.pool.checkedout() == 0
    with analytics.query('SELECT 7 AS ok') as stream:
        assert stream.to_arrow().column(0).to_pylist() == [7]


# Reuse the real-server fixture; the six conversion tests above stay offline.
from test_analytics import analytics, client  # noqa: E402, F401
