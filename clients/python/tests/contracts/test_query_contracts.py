# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Observable sync/async query invariants; SQLite is only a SELECT oracle."""

import inspect
import sqlite3
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import mysql
from sqlalchemy.orm import declarative_base

from matrixone import Client, AsyncClient
from matrixone.client import SnapshotClient
from matrixone._snapshot_sql import rewrite_snapshot
from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect
from matrixone.export import _build_jsonl_export_sql

Base = declarative_base()


class Item(Base):
    __tablename__ = 'items'
    id = sa.Column(sa.Integer, primary_key=True)
    value = sa.Column(sa.Numeric)
    text = sa.Column(sa.String)


@pytest.fixture(params=['sync', 'async'])
def query_client(request):
    db = sqlite3.connect(':memory:')
    try:
        db.executescript(
            "CREATE TABLE items(id INT, value DECIMAL, text TEXT);"
            "INSERT INTO items VALUES(1,2,'red'),(2,1,'blue'),(3,NULL,NULL);"
            "CREATE TABLE other(id INT, ref INT); INSERT INTO other VALUES(10,1),(20,8);"
        )

        def execute(sql, params=None):
            cursor = db.execute(sql, params or [])
            return SimpleNamespace(rows=cursor.fetchall(), columns=[c[0] for c in cursor.description or []])

        async def async_execute(sql, params=None):
            return execute(sql, params)

        client = Client() if request.param == 'sync' else AsyncClient()
        client.execute = execute if request.param == 'sync' else async_execute
        yield client
    finally:
        db.close()


async def resolve(value):
    return await value if inspect.isawaitable(value) else value


@pytest.mark.asyncio
@pytest.mark.parametrize('reverse', [False, True])
async def test_parameter_ownership_and_rebuild(query_client, reverse):
    q = query_client.query(Item)
    if reverse:
        q.filter('id = ?', 1).filter_by(value=Decimal('2'))
    else:
        q.filter_by(value=Decimal('2')).filter('id = ?', 1)
    before = q._build_sql()
    assert [r.id for r in await resolve(q.all())] == [1]
    assert q._build_sql() == before
    assert [r.id for r in await resolve(q.all())] == [1]


@pytest.mark.asyncio
async def test_nullable_string_table_and_model_results(query_client):
    assert [r.id for r in await resolve(query_client.query(Item).filter_by(value=None).all())] == [3]
    q = query_client.query('items').filter_by(text='red')
    assert [tuple(r) for r in await resolve(q.all())] == [(1, 2, 'red')]
    assert (await resolve(q.first())).text == 'red'
    assert await resolve(query_client.query('items').filter_by(id=99).all()) == []
    assert await resolve(query_client.query('items').filter_by(id=99).first()) is None
    with pytest.raises(ValueError, match='Unknown model field'):
        query_client.query(Item).filter_by(typo='red')


@pytest.mark.asyncio
async def test_join_qualifiers_and_right_rows(query_client):
    q = query_client.query(Item).join('other', 'items.id=other.ref').filter(Item.id == 1)
    q.select(Item.id, 'other.id AS other_id').group_by(Item.id, 'other.id').order_by(Item.id)
    assert [tuple(r) for r in await resolve(q.all())] == [(1, 10)]
    q = query_client.query(Item).rightjoin('other', 'items.id=other.ref').select(Item.id, 'other.id')
    assert [tuple(r) for r in await resolve(q.order_by('other.id').all())] == [(1, 10), (None, 20)]


@pytest.mark.asyncio
async def test_expression_literals_are_values_not_columns(query_client):
    q = query_client.query(Item).filter(Item.text == 'red').select(sa.func.upper('a.b').label('value'))
    assert [r.value for r in await resolve(q.all())] == ['A.B']
    q = query_client.query(Item).alias('a').filter(Item.id == 1).select(Item.id).group_by(Item.id)
    assert [tuple(r) for r in await resolve(q.all())] == [(1,)]


@pytest.mark.parametrize('value', ["O'Reilly", "?:id%s", '中文😀', 'a.b', Decimal('2.3'), None, b'abc'])
def test_fragment_values_do_not_borrow_or_mutate(value):
    q = Client().query(Item).filter('text = ?', value)
    before = q._build_sql()
    with pytest.raises(ValueError, match='parameters'):
        q.filter('id = ? AND value = ?', 1)
    assert q._build_sql() == before
    assert before[1] == []


@pytest.mark.parametrize(
    'type_',
    [
        mysql.BIGINT(unsigned=True),
        mysql.INTEGER(unsigned=True),
        mysql.TIMESTAMP(fsp=6),
        mysql.DATETIME(fsp=6),
        mysql.TIME(fsp=6),
        mysql.BIT(8),
        mysql.YEAR(),
        mysql.DECIMAL(12, 3),
        mysql.ENUM('a', "b'c"),
        mysql.SET('a', 'b'),
        mysql.LONGTEXT(),
        mysql.MEDIUMTEXT(),
        mysql.TINYBLOB(),
        mysql.LONGBLOB(),
        mysql.FLOAT(),
        mysql.DOUBLE(),
        sa.String(30),
        sa.Numeric(12, 3),
        sa.LargeBinary(),
    ],
)
def test_scalar_compiler_matches_upstream(type_):
    assert type_.compile(dialect=MatrixOneDialect()) == type_.compile(dialect=mysql.dialect())


@pytest.mark.parametrize(
    'sql,expected',
    [
        ('SELECT * FROM db.t AS a', "SELECT * FROM db.t{snapshot = 'sp'} AS a"),
        (
            'SELECT * FROM `db`.`t` a JOIN b ON a.id=b.id',
            "SELECT * FROM `db`.`t`{snapshot = 'sp'} a JOIN b{snapshot = 'sp'} ON a.id=b.id",
        ),
        (
            "SELECT 'FROM fake JOIN bad' FROM t /* JOIN fake */ WHERE v='a.b'",
            "SELECT 'FROM fake JOIN bad' FROM t{snapshot = 'sp'} /* JOIN fake */ WHERE v='a.b'",
        ),
        ('SELECT extract(year FROM now()) FROM t;', "SELECT extract(year FROM now()) FROM t{snapshot = 'sp'};"),
        ('SELECT 1', 'SELECT 1'),
    ],
)
def test_snapshot_complete_rewrite(sql, expected):
    assert rewrite_snapshot(sql, 'sp') == expected


@pytest.mark.parametrize(
    'sql',
    [
        'SELECT * FROM a,b',
        'SELECT * FROM a JOIN (SELECT * FROM b) x ON 1=1',
        'WITH x AS (SELECT * FROM a) SELECT * FROM x',
        'SELECT * FROM a UNION SELECT * FROM b',
        'SELECT * FROM a; SELECT * FROM b',
        'SELECT * FROM a /*! JOIN b ON 1=1 */',
        'SELECT * FROM a{snapshot="old"}',
        'SELECT * FROM a USE INDEX (x)',
        'SELECT * FROM a INTO OUTFILE "x"',
        'SELECT * FROM a FOR UPDATE',
        'SELECT * FROM function(1)',
        'SELECT * FROM "a"',
        'SELECT * FROM a /*',
        "SELECT 'unclosed FROM a",
        'SELECT * FROM a WHERE id IN (SELECT id FROM b)',
        'SELECT * FROM a NATURAL JOIN b',
        'DELETE FROM a',
        'SELECT * FROM a AS',
        'SELECT ' + '(' * 129 + '1' + ')' * 129,
    ],
)
def test_snapshot_rejects_before_dispatch(sql):
    client = Mock()
    with pytest.raises(ValueError):
        SnapshotClient(client, 'sp').execute(sql)
    client.execute.assert_not_called()


def test_snapshot_orm_alias_and_every_join():
    q = Client().query(Item, snapshot='sp').alias('a').join('other', 'a.id=other.ref').select('a.id')
    sql, params = q._build_sql()
    assert "items{snapshot = 'sp'} AS a" in sql
    assert "other{snapshot = 'sp'}" in sql
    assert params == []


@pytest.mark.parametrize('suffix', ['jsonl', 'txt'])
def test_explicit_export_format(suffix):
    assert _build_jsonl_export_sql('SELECT 1', '/tmp/a.' + suffix).endswith(" FORMAT 'jsonline'")
    assert "OUTFILE '/tmp/a''b.txt' FORMAT 'jsonline'" in _build_jsonl_export_sql('SELECT 1', "/tmp/a'b.txt")


def test_repeated_predicate_values_property():
    from hypothesis import given, settings, strategies as st

    @settings(max_examples=80, derandomize=True, deadline=None)
    @given(st.text(alphabet=st.characters(blacklist_categories=('Cs',), blacklist_characters='\\\x00'), max_size=40))
    def check(value):
        db = sqlite3.connect(':memory:')
        try:
            q = Client().query(Item).filter('? = ?', value, value)
            sql, params = q._build_sql()
            predicate = sql.split(' WHERE ', 1)[1]
            assert db.execute('SELECT ' + predicate, params).fetchone() == (1,)
            assert q._build_sql() == (sql, params)
        finally:
            db.close()

    check()


@pytest.mark.asyncio
async def test_chained_or_cannot_broaden_existing_predicate(query_client):
    for condition in ('id=1 OR id=2', sa.or_(Item.id == 1, Item.id == 2)):
        q = query_client.query(Item).filter('id=1').filter(condition)
        assert [row.id for row in await resolve(q.all())] == [1]


def test_mapped_attribute_name_is_not_assumed_to_equal_sql_column():
    base = declarative_base()

    class Renamed(base):
        __tablename__ = 'renamed'
        identifier = sa.Column('db_id', sa.Integer, primary_key=True)

    query = Client().query(Renamed).filter_by(identifier=7)
    assert 'db_id = 7' in query._build_sql()[0]
    rows = query._materialize(SimpleNamespace(rows=[(7,)], columns=['db_id']))
    assert rows[0].identifier == 7


@pytest.mark.asyncio
async def test_alias_order_and_named_join_sources(query_client):
    q = query_client.query(Item).filter(Item.id == 1).group_by(Item.id).order_by(Item.id).alias('a').select(Item.id)
    assert [tuple(r) for r in await resolve(q.all())] == [(1,)]
    q = query_client.query(Item).filter_by(id=1).join('items AS b', 'a.id=b.id').alias('a').select(Item.id)
    assert [tuple(r) for r in await resolve(q.all())] == [(1,)]
    q = query_client.query(Item).alias('a').logical_in(Item.id, [1]).select(Item.id)
    assert [tuple(r) for r in await resolve(q.all())] == [(1,)]


def test_expression_processors_and_callable_bind_identity():
    import enum
    from matrixone._query_compilation import compile_expression

    class Color(enum.Enum):
        RED = 'r'

    class Tagged(sa.TypeDecorator):
        impl = sa.String
        cache_ok = True

        def process_bind_param(self, value, dialect):
            return 'prefix_' + value if value is not None else None

    table = sa.table('typed', sa.column('color', sa.Enum(Color)), sa.column('tag', Tagged()))
    assert compile_expression(table.c.color == Color.RED) == "typed.color = 'RED'"
    assert compile_expression(table.c.color.in_([Color.RED])) == "typed.color IN ('RED')"
    assert compile_expression(table.c.tag == 'x') == "typed.tag = 'prefix_x'"
    assert compile_expression(table.c.tag.in_(['x', 'y'])) == "typed.tag IN ('prefix_x', 'prefix_y')"
    import itertools

    values = itertools.count(1)
    bind = sa.bindparam('same', callable_=lambda: next(values))
    assert compile_expression(sa.and_(sa.column('a') == bind, sa.column('b') == bind)) == 'a = 1 AND b = 1'


@pytest.mark.parametrize(
    'sql', ['SELECT (TABLE b) FROM a', 'SELECT 1 WHERE 1 IN (TABLE b)', 'SELECT (VALUES ROW(1)) FROM a']
)
def test_snapshot_rejects_implicit_query_terms(sql):
    with pytest.raises(ValueError):
        rewrite_snapshot(sql, 'sp')


def test_real_async_dialect_ping_passes_reconnect_false():
    client = AsyncClient()
    client._connection_params = dict(
        host='localhost', port=6001, user='root', password='', database=None, charset='utf8mb4', connect_timeout=5
    )
    engine = client._create_async_engine()

    class Connection:
        def __init__(self):
            self.calls = []

        def ping(self, reconnect):
            self.calls.append(reconnect)

    connection = Connection()
    assert engine.dialect.do_ping(connection)
    assert connection.calls == [False]
    engine.sync_engine.dispose()


@pytest.mark.asyncio
async def test_multiline_disjunction_never_broadens_chain(query_client):
    q = query_client.query(Item).filter('id=1').filter('id=1 OR\nid=2')
    assert [r.id for r in await resolve(q.all())] == [1]


def test_logical_in_method_uses_real_fulltext_source():
    from matrixone.sqlalchemy_ext.fulltext_search import boolean_match

    sql, _ = Client().query(Item).logical_in(Item.id, boolean_match('text').must('red'))._build_sql()
    assert 'items.id IN (SELECT id FROM items WHERE MATCH(text)' in sql


@pytest.mark.asyncio
@pytest.mark.parametrize('operation', ['update', 'delete'])
async def test_disjunction_does_not_broaden_mutations(query_client, operation):
    q = query_client.query(Item).filter('id=1').filter('id=1 OR\nid=2')
    if operation == 'update':
        await resolve(q.update(text=sa.func.upper('a.b')).execute())
        assert (await resolve(query_client.query(Item).filter_by(id=1).first())).text == 'A.B'
    else:
        await resolve(q.delete())
        assert await resolve(query_client.query(Item).filter_by(id=1).first()) is None
    assert (await resolve(query_client.query(Item).filter_by(id=2).first())).text == 'blue'
