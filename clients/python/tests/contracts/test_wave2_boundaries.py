"""Boundary failures must not manufacture results or admit writes."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import declarative_base

from matrixone import Client, AsyncClient
from matrixone._sql_literals import fulltext_term_token
from matrixone.exceptions import QueryError, PitrError
from matrixone.search_vector_index import PineconeCompatibleIndex
from matrixone.stage import StageManager, AsyncStageManager
from matrixone.metadata import BaseMetadataManager
from matrixone.vector_manager import _VectorManagerBase


@pytest.mark.parametrize('value', [None, 'not json', '[[1,2]]', '[true,2]', '[NaN,2]', '[1]', '["1",2]'])
def test_malformed_vector_response_rejected(value):
    h = PineconeCompatibleIndex(None, 't', 'v')
    with pytest.raises(QueryError, match='Invalid vector response'):
        h._materialize_matches(
            [{'id': 1, 'v': value, 'distance': 0}], 'id', ['id', 'v'], {'dimensions': 2}, True, False, ''
        )


def test_vector_response_copies_existing_numeric_list():
    h = PineconeCompatibleIndex(None, 't', 'v')
    source = [1, 2]
    row = h._materialize_matches(
        [{'key': 1, 'v': source, 'distance': 0, 'tag': 'a'}],
        'key',
        ['key', 'v', 'tag'],
        {'dimensions': 2},
        True,
        True,
        '',
    ).matches[0]
    assert row.values == [1.0, 2.0] and row.values is not source
    assert row.metadata == {'tag': 'a'}


def test_duplicate_labels_rejected_even_without_rows():
    result = Mock()
    result.keys.return_value = ['Distance', 'distance']
    with pytest.raises(QueryError, match='Ambiguous'):
        _VectorManagerBase._result_dicts(result)


@pytest.mark.parametrize('term', ['bad\0value', r'bad\$value'])
def test_unrepresentable_fulltext_term_rejected(term):
    with pytest.raises(ValueError):
        fulltext_term_token(term)


def test_fulltext_term_is_one_token():
    assert fulltext_term_token("O'Reilly $tag$") == "'O''Reilly $tag$'"
    assert fulltext_term_token(r'C:\path') == r'$sdk$C:\path$sdk$'


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize('error_type', [ValueError, QueryError])
async def test_stage_optional_readback_does_not_swallow_errors(asynchronous, error_type):
    client = Mock()
    error = error_type('catalog decoding failed')
    client.execute = AsyncMock(side_effect=[Mock(), error]) if asynchronous else Mock(side_effect=[Mock(), error])
    manager = AsyncStageManager(client) if asynchronous else StageManager(client)
    with pytest.raises(error_type, match='ALTER acknowledged') as caught:
        if asynchronous:
            await manager.alter('stage', url='file:///new', if_exists=True)
        else:
            manager.alter('stage', url='file:///new', if_exists=True)
    assert caught.value.__cause__ is error


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
async def test_pitr_acknowledged_readback_error_retains_cause(asynchronous):
    client = AsyncClient() if asynchronous else Client()
    client._version_manager.set_backend_version('4.2.5')
    if not asynchronous:
        from matrixone.pitr import PitrManager

        client._pitr = PitrManager(client)
    error = QueryError('catalog unavailable')
    client.execute = AsyncMock(side_effect=[Mock(), error]) if asynchronous else Mock(side_effect=[Mock(), error])
    with pytest.raises(PitrError, match='write acknowledged') as caught:
        if asynchronous:
            await client.pitr.create_database_pitr('MixedName', 'db')
        else:
            client.pitr.create_database_pitr('MixedName', 'db')
    assert caught.value.__cause__ is not None
    assert client.execute.call_count == 2


@pytest.mark.parametrize('asynchronous', [False, True])
@pytest.mark.parametrize('enabled', [False, True])
def test_local_infile_driver_flag(monkeypatch, asynchronous, enabled):
    client = AsyncClient() if asynchronous else Client()
    client._connection_params = dict(
        host='localhost',
        port=6001,
        user='root',
        password='',
        database='db',
        charset='utf8mb4',
        connect_timeout=5,
        ssl_mode='disabled',
        local_infile=enabled,
    )
    factory = Mock(return_value=MagicMock())
    if asynchronous:
        monkeypatch.setattr('matrixone.async_client.create_async_engine', factory)
        client._create_async_engine()
    else:
        monkeypatch.setattr('matrixone.client.create_engine', factory)
        client._create_engine()
    assert factory.call_args.kwargs['connect_args']['local_infile'] is enabled


def test_join_missing_and_ambiguous_foreign_keys_are_not_cartesian():
    base = declarative_base()

    class Parent(base):
        __tablename__ = 'parent'
        id = sa.Column(sa.Integer, primary_key=True)

    class Ambiguous(base):
        __tablename__ = 'ambiguous'
        id = sa.Column(sa.Integer, primary_key=True)
        a = sa.Column(sa.Integer, sa.ForeignKey('parent.id'))
        b = sa.Column(sa.Integer, sa.ForeignKey('parent.id'))

    q = Client().query(Parent)
    with pytest.raises(ValueError, match='ambiguous'):
        q.join(Ambiguous)
    assert q._joins == []
    with pytest.raises(ValueError, match='explicit ON'):
        q.join('raw_table')
    assert q._joins == []


def test_object_metadata_inconsistency_is_not_arbitrarily_ignored():
    rows = [
        Mock(_mapping=dict(object_name='o', rows_cnt=1, origin_size=10)),
        Mock(_mapping=dict(object_name='o', rows_cnt=1, origin_size=20)),
    ]
    with pytest.raises(ValueError, match='Inconsistent'):
        BaseMetadataManager._object_rows(rows)


@pytest.mark.asyncio
async def test_async_upsert_budget_covers_preflight():
    client = AsyncClient(query_timeout=0.01)
    finalized = asyncio.Event()

    async def stalled(*args, **kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            finalized.set()

    client.execute = stalled
    with pytest.raises(asyncio.TimeoutError):
        await client.get_pinecone_index('t', 'v').upsert_async([{'id': 1, 'v': [1, 2]}])
    assert finalized.is_set()


def test_generated_fulltext_token_survives_subquery_boundary():
    from matrixone.sqlalchemy_ext.fulltext_search import natural_match

    query = Client().query('docs').filter(natural_match('content', query=r'C:\path'))
    sql, params = query._build_sql()
    assert params == []
    assert query.subquery('s') == '(' + sql + ') AS s'
    assert r'$sdk$C:\path$sdk$' in sql


@pytest.mark.parametrize('score', [None, float('nan'), float('inf'), 'bad'])
def test_invalid_vector_score_is_typed_even_without_values(score):
    h = PineconeCompatibleIndex(None, 't', 'v')
    with pytest.raises(QueryError, match='Invalid vector response'):
        h._materialize_matches([{'id': 1, 'distance': score}], 'id', ['id', 'v'], {'dimensions': 2}, False, False, '')


@pytest.mark.parametrize('alias_first', [False, True])
def test_self_join_inference_cannot_alias_both_sides(alias_first):
    base = declarative_base()

    class Node(base):
        __tablename__ = 'nodes'
        id = sa.Column(sa.Integer, primary_key=True)
        parent_id = sa.Column(sa.Integer, sa.ForeignKey('nodes.id'))

    query = Client().query(Node)
    if alias_first:
        query.alias('a')
    with pytest.raises(ValueError, match='Self JOIN'):
        query.join(Node)
    assert query._joins == []
    query.alias('a').join(Node, 'a.id=nodes.parent_id')
    sql, _ = query._build_sql()
    assert 'ON a.id=nodes.parent_id' in sql
