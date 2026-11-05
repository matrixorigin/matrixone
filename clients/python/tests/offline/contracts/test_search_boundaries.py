"""Search builders share lexical and validation boundaries, including mutation."""

from unittest.mock import MagicMock, AsyncMock
from types import SimpleNamespace
import numpy as np
import pytest
from sqlalchemy import Column, Integer, MetaData, Table, select
from sqlalchemy.orm import declarative_base
from sqlalchemy.schema import CreateIndex
from sqlalchemy.sql.elements import quoted_name
from matrixone.vector_manager import VectorManager, AsyncVectorManager
from matrixone.exceptions import QueryError
from matrixone.sqlalchemy_ext import MatrixOneDialect
from matrixone.sqlalchemy_ext.fulltext_search import (
    FulltextQueryBuilder,
    FulltextSearchBuilder,
    FulltextSearchMode,
    boolean_match,
    natural_match,
)
from matrixone.sqlalchemy_ext.fulltext_index import FulltextSearchBuilder as LegacySearchBuilder
from matrixone.sqlalchemy_ext.vector_index import IVFVectorIndex, HnswVectorIndex, VectorIndex


@pytest.mark.parametrize(
    'column,expected',
    [
        ('body', '`body`'),
        ('order', '`order`'),
        ('fulltext2', '`fulltext2`'),
        ('中文', '`中文`'),
        ('a`b', '`a``b`'),
        ('db.order', '`db`.`order`'),
        ('db.docs.order', '`db`.`docs`.`order`'),
        ('`db`.`d``ocs`.`order`', '`db`.`d``ocs`.`order`'),
        (quoted_name('a.b', True), '`a.b`'),
        (Column('a.b', Integer), '`a.b`'),
        (Column('a`b', Integer), '`a``b`'),
        ('x) AGAINST(1); --', '`x) AGAINST(1); --`'),
    ],
)
def test_match_identifiers_across_public_renderers(column, expected):
    sqls = [
        FulltextQueryBuilder().must('alpha').as_sql('docs', [column], include_score=True),
        boolean_match(column).must('alpha').compile(),
        str(select(boolean_match(column).must('alpha')).compile(dialect=MatrixOneDialect())),
        natural_match(column, query='alpha').compile(),
        FulltextSearchBuilder(None).table('docs').columns([column]).query('alpha').with_score().explain(),
        LegacySearchBuilder('docs', [column]).search('alpha').set_with_score().build_sql(),
    ]
    for sql in sqls:
        assert 'MATCH(' + expected + ')' in sql
    assert sqls[0].count('MATCH(' + expected + ')') == 2
    assert sqls[4].count('MATCH(' + expected + ')') == 2


def test_match_group_column_identity_and_qualification():
    base = declarative_base()

    class Doc(base):
        __tablename__ = 'docs'
        id = Column(Integer, primary_key=True)
        body = Column(Integer)

    assert '+(alpha)' in boolean_match(Doc.body).group(boolean_match('body').must('alpha')).compile()
    assert '+(alpha)' in boolean_match(Column('body')).group(boolean_match(Doc.body).must('alpha')).compile()
    with pytest.raises(ValueError):
        boolean_match(Column('a.b')).group(boolean_match('a.b').must('alpha'))
    for invalid in ('', 'a..b', 'a.b.c.d', '`x` trailing', 'a\x00b'):
        with pytest.raises(ValueError):
            boolean_match(invalid).must('alpha').compile()


@pytest.mark.parametrize('limit', [None, 0, 5])
@pytest.mark.parametrize('offset', [None, 0, 2])
def test_explicit_pagination_all_builders(limit, offset):
    modern = FulltextSearchBuilder(None).table('docs').columns(['body']).query('alpha')
    legacy = LegacySearchBuilder('docs', ['body']).search('alpha')
    for builder in (modern, legacy):
        if limit is not None:
            builder.limit(limit)
        if offset is not None:
            builder.offset(offset)
    sqls = [
        FulltextQueryBuilder().must('alpha').as_sql('docs', ['body'], limit=limit, offset=offset),
        modern.explain(),
        legacy.build_sql(),
    ]
    for sql in sqls:
        assert ('LIMIT ' in sql) is (limit is not None)
        assert ('OFFSET ' in sql) is (offset is not None)
        if limit is not None:
            assert 'LIMIT %s' % limit in sql
        if offset is not None:
            assert 'OFFSET %s' % offset in sql


def test_query_expansion_mode_is_grammar_not_raw_sql():
    sql = FulltextQueryBuilder().must('alpha').as_sql('docs', ['body'], mode=FulltextSearchMode.QUERY_EXPANSION)
    assert ' WITH QUERY EXPANSION)' in sql and 'IN query expansion mode' not in sql
    with pytest.raises(ValueError, match='Unsupported fulltext search mode'):
        FulltextSearchBuilder(None).table('docs').columns(['body']).query('alpha').with_mode(
            'evil) OR 1=1 --'
        ).explain()
    with pytest.raises(ValueError, match='Unsupported fulltext search mode'):
        FulltextQueryBuilder().must('alpha').as_sql('docs', ['body'], mode='evil) OR 1=1 --')


OPTIONS = [
    (IVFVectorIndex, 'lists'),
    (HnswVectorIndex, 'm'),
    (HnswVectorIndex, 'ef_construction'),
    (HnswVectorIndex, 'ef_search'),
    (VectorIndex, 'lists'),
    (VectorIndex, 'm'),
    (VectorIndex, 'ef_construction'),
    (VectorIndex, 'ef_search'),
]


@pytest.mark.parametrize('cls,option', OPTIONS)
@pytest.mark.parametrize('value', [0, -1, 2**63, True, np.bool_(True), 1.5, '1', '1; DROP TABLE docs'])
def test_vector_options_reject_invalid_before_execution_and_mutation(cls, option, value):
    engine = MagicMock()
    with pytest.raises(ValueError):
        cls.create_index(engine, 'docs', 'ix', 'v', **{option: value})
    engine.begin.assert_not_called()
    index = cls('ix', 'v')
    old = getattr(index, option)
    with pytest.raises(ValueError):
        setattr(index, option, value)
    assert getattr(index, option) == old


@pytest.mark.parametrize('cls', [IVFVectorIndex, HnswVectorIndex, VectorIndex])
def test_vector_create_drop_quoting_and_error_propagation(cls):
    table = Table('order', MetaData(), Column('fulltext2', Integer), schema='db')
    index = cls('ix`name', table.c.fulltext2)
    for sql in (index.create_sql('db.order'), str(CreateIndex(index).compile(dialect=MatrixOneDialect()))):
        assert '`ix``name`' in sql and '(`fulltext2`)' in sql
    assert index.drop_sql('db.order') == 'DROP INDEX `ix``name` ON `db`.`order`'
    drop_engine = MagicMock()
    assert cls.drop_index(drop_engine, 'db.order', '`ix`') is True
    drop_engine.begin.return_value.__enter__.return_value.exec_driver_sql.assert_called_once_with(
        'DROP INDEX `ix` ON `db`.`order`'
    )
    engine = MagicMock()
    original = RuntimeError('real DDL failure')
    engine.begin.return_value.__enter__.return_value.exec_driver_sql.side_effect = original
    for action in (
        lambda: index.create(engine, 'db.order'),
        lambda: index.drop(engine, 'db.order'),
        lambda: cls.create_index(engine, 'db.order', 'ix', 'v'),
        lambda: cls.drop_index(engine, 'db.order', 'ix'),
    ):
        with pytest.raises(RuntimeError) as caught:
            action()
        assert caught.value is original


def test_vector_optional_and_server_metric_contracts():
    for cls, option in OPTIONS:
        for value in (None, 1, np.int64(2), 2**63 - 1):
            index = cls('ix', 'v', **{option: value})
            assert getattr(index, option) == value
            assert index.create_sql('docs')
    for metric in ('', 'vector_l2_ops', 'VECTOR_L2SQ_OPS', 'vector_l1_ops', 'vector_cosine_ops', 'vector_ip_ops'):
        assert VectorIndex('ix', 'v', op_type=metric).op_type == metric.lower()
    for field, value in [('op_type', 'unknown'), ('index_type', 'ivfflat; DROP TABLE docs')]:
        with pytest.raises(ValueError):
            VectorIndex('ix', 'v', **{field: value})
    assert 'USING future_plugin' in VectorIndex('ix', 'v', index_type='future_plugin').create_sql('docs')


@pytest.mark.asyncio
@pytest.mark.parametrize('asynchronous', [False, True])
async def test_vector_manager_rejects_before_setup(asynchronous):
    client = SimpleNamespace(execute=AsyncMock() if asynchronous else MagicMock())
    manager = AsyncVectorManager(client) if asynchronous else VectorManager(client)
    with pytest.raises(QueryError, match='positive signed 64-bit integer'):
        result = manager.create_ivf('docs', 'ix', 'v', lists='not-a-number')
        if asynchronous:
            await result
    client.execute.assert_not_called()


@pytest.mark.parametrize('mode', ['boolean mode', 'BOOLEAN MODE', ' Boolean  Mode '])
def test_search_mode_preserves_sql_keyword_case_and_spacing(mode):
    sql = FulltextQueryBuilder().must('alpha').as_sql('docs', ['body'], mode=mode)
    assert "AGAINST('+alpha' IN BOOLEAN MODE)" in sql
    sql = FulltextSearchBuilder(None).table('docs').columns(['body']).query('alpha').with_mode(mode).explain()
    assert "AGAINST('alpha' IN BOOLEAN MODE)" in sql


@pytest.mark.parametrize(
    'mode,suffix',
    [
        ('natural language mode', ' IN natural language mode'),
        ('boolean mode', ' IN BOOLEAN MODE'),
        ('query expansion mode', ' WITH QUERY EXPANSION'),
        ('BM25 MODE', ' IN BM25 MODE'),
        ('natural language mode with query expansion', ' IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION'),
    ],
)
def test_raw_mode_builders_preserve_all_native_modes(mode, suffix):
    queries = [
        FulltextQueryBuilder().must('alpha').as_sql('docs', ['body'], mode=mode),
        FulltextSearchBuilder(None).table('docs').columns(['body']).query('alpha').with_mode(mode).explain(),
    ]
    for sql in queries:
        assert suffix + ')' in sql


@pytest.mark.parametrize('term,token', [('alpha?', "'alpha?'"), (r'alpha\path', r'$sdk$alpha\path$sdk$')])
def test_fulltext_legacy_consumer_keeps_zero_bind_query_tokens(term, token):
    sql = LegacySearchBuilder('docs', ['body']).search(term).build_sql()
    assert 'AGAINST(' + token + ')' in sql
