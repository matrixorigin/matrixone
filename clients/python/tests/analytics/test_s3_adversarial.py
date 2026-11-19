# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

import os
import uuid

import pytest
from test_adversarial import hard_client, trial

pa = pytest.importorskip('pyarrow', minversion='18.0.0')
pq = pytest.importorskip('pyarrow.parquet')


@pytest.fixture
def remote(trial):
    endpoint = os.environ.get('MO_ANALYTICS_S3_ENDPOINT')
    if not endpoint:
        pytest.skip('local S3-compatible service required')
    import boto3
    import pyarrow.fs as fs

    bucket = 'adv-' + uuid.uuid4().hex
    stage = 'adv_s3_' + uuid.uuid4().hex
    s3 = boto3.client(
        's3', endpoint_url=endpoint, aws_access_key_id='test', aws_secret_access_key='test', region_name='us-east-1'
    )
    s3.create_bucket(Bucket=bucket)
    arrow = fs.S3FileSystem(
        access_key='test', secret_key='test', region='us-east-1', endpoint_override=endpoint, scheme='http'
    )
    trial.client.stage.create(
        stage,
        's3://' + bucket + '/',
        credentials={
            'AWS_KEY_ID': 'test',
            'AWS_SECRET_KEY': 'test',
            'AWS_REGION': 'us-east-1',
            'ENDPOINT': endpoint,
            'PROVIDER': 'minio',
        },
    )

    def put(key, data):
        s3.put_object(Bucket=bucket, Key=key, Body=data)

    def read(key, format, **kwargs):
        return trial.analytics.read_files(
            [bucket + '/' + key],
            format=format,
            filesystem=arrow,
            server_paths=['stage://' + stage + '/' + key],
            **kwargs,
        )

    yield put, read, s3, bucket, arrow, stage
    trial.client.execute('DROP STAGE IF EXISTS `' + stage + '`')
    entries = s3.list_objects_v2(Bucket=bucket).get('Contents', [])
    for entry in entries:
        s3.delete_object(Bucket=bucket, Key=entry['Key'])
    s3.delete_bucket(Bucket=bucket)


@pytest.mark.parametrize('compression', [None, 'snappy', 'zstd', 'gzip'])
def test_remote_parquet_codecs(trial, remote, compression):
    put, read, *_ = remote
    source = pa.table({'id': range(257), 'name': ['中文😀'] * 257})
    sink = pa.BufferOutputStream()
    pq.write_table(source, sink, compression=compression, row_group_size=7)
    put('many.parquet', sink.getvalue().to_pybytes())
    with read('many.parquet', 'parquet') as rel:
        with rel.query() as stream:
            assert stream.to_arrow().sort_by([('id', 'ascending')]).equals(source)


def test_remote_csv_explicit_schema_with_newlines(trial, remote):
    put, read, *_ = remote
    put('data.csv', b'id,name\n1,"hello\nworld"\n2,"a""b"\n')
    with read('data.csv', 'csv', schema=pa.schema([('id', pa.int64()), ('name', pa.string())])) as rel:
        with rel.query() as stream:
            assert stream.to_arrow().to_pylist() == [{'id': 1, 'name': 'hello\nworld'}, {'id': 2, 'name': 'a"b'}]


def test_remote_csv_extra_columns_rejected(trial, remote):
    put, read, *_ = remote
    put('bad.csv', b'id,name\n1,a,extra\n')
    with pytest.raises(ValueError):
        read('bad.csv', 'csv', schema=pa.schema([('id', pa.int64()), ('name', pa.string())]))


@pytest.mark.parametrize('format', ['parquet', 'csv', 'jsonline'])
def test_remote_missing_object_is_error(trial, remote, format):
    put, read, s3, bucket, *_ = remote
    if format == 'parquet':
        sink = pa.BufferOutputStream()
        pq.write_table(pa.table({'id': [1]}), sink)
        data = sink.getvalue().to_pybytes()
    else:
        data = b'id\n1\n' if format == 'csv' else b'{"id":1}\n'
    put('data', data)
    with read('data', format) as rel:
        s3.delete_object(Bucket=bucket, Key='data')
        with pytest.raises(Exception):
            rel.query()


def test_remote_schema_drift_rejected(trial, remote):
    put, read, *_ = remote

    def data(fields):
        sink = pa.BufferOutputStream()
        pq.write_table(pa.table(fields), sink)
        return sink.getvalue().to_pybytes()

    put('drift.parquet', data({'id': [1]}))
    with read('drift.parquet', 'parquet') as rel:
        put('drift.parquet', data({'renamed': [1]}))
        with pytest.raises(ValueError, match='schema changed'):
            rel.query()


def test_remote_live_same_schema_update_is_visible(trial, remote):
    put, read, *_ = remote

    def data(v):
        sink = pa.BufferOutputStream()
        pq.write_table(pa.table({'id': [v]}), sink)
        return sink.getvalue().to_pybytes()

    put('live.parquet', data(1))
    with read('live.parquet', 'parquet') as rel:
        with rel.query() as stream:
            assert stream.to_arrow()['id'].to_pylist() == [1]
        put('live.parquet', data(2))
        with rel.query() as stream:
            assert stream.to_arrow()['id'].to_pylist() == [2]
