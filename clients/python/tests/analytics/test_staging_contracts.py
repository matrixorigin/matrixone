# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""File preparation is bounded and detached from database ownership."""

import os
import threading
from concurrent.futures import CancelledError

import pytest

pa = pytest.importorskip('pyarrow', minversion='18.0.0')

from matrixone.analytics import _plan_files, _stage_arrow


def stage(data, tmp_path, limit=1 << 20, cancelled=None):
    paths = []
    try:
        result = _stage_arrow(data, tmp_path, None, 2, limit, paths.append, cancelled)
        return result, paths
    except BaseException as error:
        error.test_owned_paths = paths
        raise


def test_staging_is_readable_ipc_with_checked_types(tmp_path):
    table = pa.table({'id': [1, 2, None], 'value': ['a', '', 'b']})
    staged, paths = stage(table, tmp_path)
    try:
        assert staged.rows == 3
        assert paths == [staged.path]
        with pa.ipc.open_file(staged.path) as reader:
            assert reader.read_all().equals(table)
            assert reader.num_record_batches == 2
    finally:
        for path in paths:
            os.unlink(path)


@pytest.mark.parametrize('limit', [1, 100, 1024])
def test_byte_budget_applies_before_large_value_or_footer_write(tmp_path, limit):
    with pytest.raises(ValueError, match='byte limit') as caught:
        stage(pa.table({'s': ['x' * 10000]}), tmp_path, limit)
    for path in caught.value.test_owned_paths:
        assert os.stat(path).st_size <= limit
        os.unlink(path)


def test_empty_file_footer_cannot_exceed_budget(tmp_path):
    empty = pa.table({'id': pa.array([], type=pa.int64())})
    staged, paths = stage(empty, tmp_path)
    full_size = os.stat(staged.path).st_size
    os.unlink(staged.path)
    with pytest.raises(ValueError, match='byte limit') as caught:
        stage(empty, tmp_path, full_size - 1)
    assert caught.value.test_owned_paths
    for path in caught.value.test_owned_paths:
        assert os.stat(path).st_size < full_size
        os.unlink(path)


def test_reader_error_remains_primary_and_reserved_file_is_closed(tmp_path):
    original = RuntimeError('reader failed')

    def batches():
        yield pa.record_batch({'id': [1]})
        raise original

    reader = pa.RecordBatchReader.from_batches(pa.schema([('id', pa.int64())]), batches())
    with pytest.raises(RuntimeError) as caught:
        stage(reader, tmp_path)
    assert caught.value is original
    for path in caught.value.test_owned_paths:
        os.unlink(path)
    reader.close()


def test_worker_cancellation_does_not_hide_primary(tmp_path):
    cancel = threading.Event()

    def batches():
        yield pa.record_batch({'id': [1]})
        cancel.set()
        yield pa.record_batch({'id': [2]})

    reader = pa.RecordBatchReader.from_batches(pa.schema([('id', pa.int64())]), batches())
    with pytest.raises(CancelledError) as caught:
        stage(reader, tmp_path, cancelled=cancel)
    assert len(caught.value.test_owned_paths) == 1
    for path in caught.value.test_owned_paths:
        os.unlink(path)
    reader.close()


def test_pre_cancelled_worker_does_not_reserve_file(tmp_path):
    cancel = threading.Event()
    cancel.set()
    with pytest.raises(CancelledError) as caught:
        stage(pa.table({'id': [1]}), tmp_path, cancelled=cancel)
    assert caught.value.test_owned_paths == []
    assert list(tmp_path.iterdir()) == []


def test_file_discovery_stops_at_limit_before_consuming_source(tmp_path):
    consumed = []

    def paths():
        for index in range(100):
            consumed.append(index)
            yield '/remote/%d.parquet' % index

    with pytest.raises(ValueError, match='1..2'):
        _plan_files(paths(), 'parquet', max_files=2, filesystem=object())
    assert consumed == [0, 1, 2]


def test_bounded_glob_preserves_sorted_unique_paths(tmp_path):
    import pyarrow.parquet as pq

    for name, value in [('b', 2), ('a', 1)]:
        pq.write_table(pa.table({'id': [value]}), tmp_path / (name + '.parquet'))
    plan = _plan_files([str(tmp_path / '*.parquet'), str(tmp_path / 'a.parquet')], 'parquet', max_files=2)
    assert [scan.params[0] for scan in plan.scans] == [str(tmp_path / 'a.parquet'), str(tmp_path / 'b.parquet')]
    assert plan.sources == (str(tmp_path / 'a.parquet'), str(tmp_path / 'b.parquet'))
    assert len(plan.checks) == 2
    with pytest.raises(ValueError, match='1..1'):
        _plan_files(str(tmp_path / '*.parquet'), 'parquet', max_files=1)


@pytest.mark.parametrize(
    'name,encoded',
    [
        ("two words", 'two%20words'),
        ("o'connor{v1}", 'o%27connor%7Bv1%7D'),
        ('part/name', 'part%2Fname'),
        ('literal%2F', 'literal%252F'),
    ],
)
def test_stage_path_authority_preserves_logical_name_and_file_mapping(name, encoded):
    from matrixone.analytics import _server_file_source, _server_staging_path

    uri = 'stage:///' + encoded + '/a%20b.csv'
    scan, check = _server_file_source(uri)
    assert scan == uri and check == uri + '?offset=0&size=1'
    assert _server_staging_path('stage:///' + encoded + '/shared%20dir', 'next.arrow') == (
        'stage:///' + encoded + '/shared%20dir/next.arrow'
    )


@pytest.mark.parametrize(
    'uri',
    [
        'stage:foo/bar',
        'stage:/foo/bar',
        'stage:///',
        'stage:///%ZZ/file',
        'stage:///%00/file',
        'stage:///%FF/file',
        'stage:///%2e%2e/file',
    ],
)
def test_invalid_stage_uri_never_retargets_a_file(uri):
    from matrixone.analytics import _server_file_source, _server_staging_path

    with pytest.raises((ValueError, UnicodeError)):
        _server_file_source(uri)
    with pytest.raises((ValueError, UnicodeError)):
        _server_staging_path(uri, 'file.arrow')
