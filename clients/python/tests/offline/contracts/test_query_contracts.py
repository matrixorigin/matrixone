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


def test_legacy_filter_by_never_discards_predicates():
    from matrixone.orm import Query

    db = sqlite3.connect(':memory:')
    try:
        db.executescript(
            "CREATE TABLE items(id INT, value INT, text TEXT); INSERT INTO items VALUES(1,2,'red'),(2,NULL,'blue');"
        )
        client = Client()

        def execute(sql, params=None):
            cursor = db.execute(sql, params or [])
            return SimpleNamespace(rows=cursor.fetchall(), columns=[c[0] for c in cursor.description or []])

        client.execute = execute
        assert Query(Item, client).filter_by(text='red').count() == 1
        assert Query('items', client).filter_by(text='red').count() == 1
        assert Query(Item, client).filter_by(value=None).count() == 1
        q = Query(Item, client).filter_by(id=1)
        with pytest.raises(ValueError, match='Unknown model field'):
            q.filter_by(text='red', typo='blue')
        assert q._build_select_sql()[1] == [1]  # Failed calls must not partially add predicates.
        assert Query(Item, client).count() == 2
        with pytest.raises(ValueError, match='Unknown model field'):
            Query(Item, client).filter_by(typo='red').delete()
        assert Query(Item, client).count() == 2
    finally:
        db.close()


def test_legacy_filter_by_uses_mapped_attribute_and_physical_column():
    from matrixone.orm import Query

    class Renamed(Base):
        __tablename__ = 'renamed_filter'
        key = sa.Column('id', sa.Integer, primary_key=True)
        display = sa.Column('select', sa.String)

    query = Query(Renamed, Mock()).filter_by(display="O'Reilly")
    sql, params = query._build_select_sql()
    assert '`select` = ?' in sql
    assert params == ["O'Reilly"]


def test_legacy_filter_by_excluded_columns_do_not_break_valid_fields():
    from matrixone.orm import Query

    table = sa.Table(
        'partial_filter', Base.metadata, sa.Column('id', sa.Integer, primary_key=True), sa.Column('ignored', sa.String)
    )

    class Partial(Base):
        __table__ = table
        __mapper_args__ = {'exclude_properties': ['ignored']}

    q = Query(Partial, Mock()).filter_by(id=1)
    assert q._build_select_sql()[1] == [1]
    with pytest.raises(ValueError, match='Unknown model field'):
        q.filter_by(ignored='x')
    assert q._build_select_sql()[1] == [1]


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
    assert 'items.id IN (SELECT id FROM items WHERE MATCH(`text`)' in sql


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


@pytest.mark.asyncio
@pytest.mark.parametrize('alias_first', [False, True])
async def test_aliased_mutations_share_select_target(query_client, alias_first):
    q = query_client.query(Item)
    if alias_first:
        q.alias('a')
    q.filter(Item.id == 1).update(value=Item.value + 10)
    if not alias_first:
        q.alias('a')
    await resolve(q.execute())
    assert (await resolve(query_client.query(Item).filter_by(id=1).first())).value == 12
    await resolve(query_client.query(Item).filter(Item.id == 2).alias('a').delete())
    assert [r.id for r in await resolve(query_client.query(Item).order_by(Item.id).all())] == [1, 3]


@pytest.mark.asyncio
@pytest.mark.parametrize('query_client', ['async'], indirect=True)
async def test_async_database_routes_all_mutations_and_preserves_default(query_client):
    from matrixone.async_orm import AsyncMatrixOneQuery

    await query_client.execute("ATTACH DATABASE ':memory:' AS alternate")
    await query_client.execute('CREATE TABLE alternate.items(id INT, value INT, text TEXT)')
    await query_client.execute("INSERT INTO alternate.items VALUES(1,100,'other')")
    q = AsyncMatrixOneQuery('items', query_client, database='alternate').filter('id=?', 1)
    before = q._build_sql()
    assert (await q.first()).value == 100
    await q.update(value=101).execute()
    assert (await query_client.query('alternate.items').first()).value == 101
    assert (await query_client.query(Item).filter_by(id=1).first()).value == 2
    assert q._build_sql() == before
    await q.delete()
    assert await query_client.query('alternate.items').all() == []
    q.database = 'missing'
    with pytest.raises(sqlite3.OperationalError):
        await q.update(value=0).execute()
    assert (await query_client.query(Item).filter_by(id=1).first()).value == 2


@pytest.mark.parametrize('query_client', ['sync'], indirect=True)
def test_direct_legacy_query_executes_all_public_operations(query_client):
    from matrixone.orm import Query

    for model in (
        Item,
        type(
            'Legacy',
            (),
            {
                '_table_name': 'items',
                '__tablename__': 'unused',
                '__init__': lambda self, **kw: self.__dict__.update(kw),
            },
        ),
    ):
        q = Query(model, query_client)
        assert q.count() == 3
        assert q.select('id', 'value', 'text').filter('id=?', 1).first().value == 2
        assert len(Query(model, query_client).execute()) == 3
        Query(model, query_client).insert(id=4, value=9, text='new').execute()
        Query(model, query_client).filter('id=?', 4).update(value=10).execute()
        assert Query(model, query_client).select('id', 'value', 'text').filter('id=?', 4).all()[0].value == 10
        Query(model, query_client).filter('id=?', 4).delete()
        assert Query(model, query_client).count() == 3


def test_generated_fulltext_sql_composes_without_raw_fragment_revalidation():
    from matrixone.export import _query_to_sql
    from matrixone.orm import logical_in
    from matrixone.sqlalchemy_ext.fulltext_search import natural_match

    q = Client().query('items').select('id').filter(natural_match('text', query=r'C:\path?'))
    sql, params = q._build_sql()
    assert not params
    assert _query_to_sql(q) == sql
    assert logical_in('id', q).compile() == 'id IN (' + sql + ')'

    class BoundQuery:
        def _build_sql(self):
            return r'SELECT $sdk$C:\path?$sdk$ AS text FROM items WHERE id=?', [1]

    assert _query_to_sql(BoundQuery()).endswith('WHERE id=1')
    assert r'$sdk$C:\path?$sdk$' in logical_in('text', BoundQuery()).compile()
    with pytest.raises(ValueError, match='ambiguous'):
        Client().query('items').filter(r"text='C:\path'")


@pytest.mark.asyncio
async def test_recursive_cte_executes_and_nonrecursive_control(query_client):
    from matrixone.orm import CTE

    recursive = CTE('r', 'SELECT 1 AS n UNION ALL SELECT n+1 FROM r WHERE n<3', recursive=True)
    q = query_client.query(recursive).select('n').order_by('n')
    assert [r.n for r in await resolve(q.all())] == [1, 2, 3]
    assert q._build_sql()[0].startswith('WITH RECURSIVE ')
    ordinary = query_client.query(CTE('plain', 'SELECT 7 AS n')).select('n')
    assert (await resolve(ordinary.first())).n == 7
    assert ordinary._build_sql()[0].startswith('WITH plain ')


def test_async_export_manager_is_stable_public_entry():
    from matrixone.export import AsyncExportManager

    client = AsyncClient()
    assert isinstance(client.export, AsyncExportManager)
    assert client.export is client.export
    assert client.export.client is client


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['sync', 'async'])
async def test_string_query_materializes_real_session_cursor_result(kind):
    from matrixone.orm import MatrixOneQuery
    from matrixone.async_orm import AsyncMatrixOneQuery

    engine = sa.create_engine('sqlite:///:memory:')
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql('CREATE TABLE records(id INT,value INT)')
            connection.exec_driver_sql('INSERT INTO records VALUES(1,10)')

            def execute(sql, params=None):
                return connection.exec_driver_sql(sql, tuple(params or ()))

            async def async_execute(sql, params=None):
                return execute(sql, params)

            query_type = MatrixOneQuery if kind == 'sync' else AsyncMatrixOneQuery
            wrapper = SimpleNamespace(execute=execute if kind == 'sync' else async_execute)
            q = query_type('records', None, transaction_wrapper=wrapper)
            assert (await resolve(q.first())).value == 10
            await resolve(q.filter('id=?', 1).update(value=11).execute())
            assert (await resolve(q.first())).value == 11
    finally:
        engine.dispose()


def test_explicit_database_and_qualified_table_conflict_before_dispatch():
    from matrixone.async_orm import AsyncMatrixOneQuery
    from matrixone.orm import CTE

    q = AsyncMatrixOneQuery('other.items', AsyncClient(), database='other')
    q.update(value=1)
    assert 'UPDATE other.items SET' in q._build_update_sql()[0]
    q.database = 'different'
    for build in (q._build_sql, q._build_update_sql, q._build_delete_sql):
        with pytest.raises(ValueError, match='conflicts'):
            build()
    cte = AsyncClient().query(CTE('local_rows', 'SELECT 1 AS n'))
    cte.database = 'other'
    assert 'FROM local_rows' in cte._build_sql()[0]


@pytest.mark.asyncio
async def test_mapped_schema_is_the_read_and_write_identity(query_client):
    from matrixone.orm import Query

    base = declarative_base()

    class Mapped(base):
        __tablename__ = 'items'
        __table_args__ = {'schema': 'alternate'}
        id = sa.Column(sa.Integer, primary_key=True)
        value = sa.Column(sa.Integer)
        text = sa.Column(sa.String)

    await resolve(query_client.execute("ATTACH DATABASE ':memory:' AS alternate"))
    await resolve(query_client.execute('CREATE TABLE alternate.items(id INT,value INT,text TEXT)'))
    await resolve(query_client.execute("INSERT INTO alternate.items VALUES(1,100,'chosen')"))
    q = query_client.query(Mapped).filter(Mapped.id == 1)
    assert (await resolve(q.first())).value == 100
    await resolve(q.update(value=101).execute())
    assert (await resolve(q.first())).value == 101
    assert (await resolve(query_client.query(Item).filter_by(id=1).first())).value == 2
    if isinstance(query_client, Client):
        legacy = Query(Mapped, query_client).filter('id=?', 1)
        assert legacy.first().value == 101
        Query(Mapped, query_client).filter('id=?', 1).update(value=102).execute()
        assert legacy.first().value == 102
    else:
        q.database = 'alternate'
        assert (await q.first()).value == 101
        q.database = 'different'
        for build in (q._build_sql, q._build_update_sql, q._build_delete_sql):
            with pytest.raises(ValueError, match='conflicts'):
                build()
        q.database = None
    await resolve(q.delete())
    assert await resolve(query_client.query(Mapped).all()) == []
    assert (await resolve(query_client.query(Item).filter_by(id=1).first())).value == 2


@pytest.mark.parametrize('legacy', [False, True])
@pytest.mark.parametrize('operation', ['update', 'delete'])
@pytest.mark.parametrize('query_client', ['sync'], indirect=True)
def test_joined_mutation_fails_before_changing_rows(query_client, legacy, operation):
    from matrixone.orm import Query

    query = Query(Item, query_client) if legacy else query_client.query(Item)
    query.join('other', 'items.id=other.ref')
    if operation == 'update':
        with pytest.raises(ValueError, match='Joined UPDATE/DELETE'):
            query.update(value=99).execute()
    else:
        with pytest.raises(ValueError, match='Joined UPDATE/DELETE'):
            query.delete()
    assert [r.value for r in query_client.query(Item).order_by(Item.id).all()] == [2, 1, None]


@pytest.mark.parametrize('query_client', ['sync'], indirect=True)
def test_legacy_mapped_physical_names_and_projection_order(query_client):
    from matrixone.orm import Query

    base = declarative_base()

    class Renamed(base):
        __tablename__ = 'items'
        key = sa.Column('id', sa.Integer, primary_key=True)
        amount = sa.Column('value', sa.Integer)
        label = sa.Column('text', sa.String)

    assert Query(Renamed, query_client).filter('id=?', 1).first().label == 'red'
    row = Query(Renamed, query_client).select('text', 'id').filter('id=?', 1).first()
    assert (row.label, row.key) == ('red', 1)
    assert Query(Renamed, query_client).filter('id=?', 1).scalar() == 1
    metadata = sa.MetaData()
    table = sa.Table(
        'items',
        metadata,
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('value', sa.Integer),
        sa.Column('text', sa.String),
    )
    table_model = type('DifferentClassName', (declarative_base(),), {'__table__': table})
    assert Query(table_model, query_client).filter('id=?', 1).first().text == 'red'


@pytest.mark.parametrize('table_name', ['order', 'line items'])
def test_async_database_quotes_mapped_components_once(table_name):
    from matrixone.async_orm import AsyncMatrixOneQuery

    model = type(
        'Quoted',
        (declarative_base(),),
        {
            '__tablename__': table_name,
            'id': sa.Column(sa.Integer, primary_key=True),
        },
    )
    q = AsyncMatrixOneQuery(model, AsyncClient(), database='other').filter(model.id == 1).update(id=2)
    target = 'other.`' + table_name + '`'
    for build in (q._build_sql, q._build_update_sql, q._build_delete_sql):
        sql, _ = build()
        assert target in sql
        assert '```' not in sql


@pytest.mark.parametrize('driver', ['pymysql', 'aiomysql'])
def test_dialect_adaptation_preserves_native_decimal_binding(driver):
    from matrixone.sqlalchemy_ext.dialect import adapt_engine_dialect

    engine = sa.create_engine('mysql+' + driver + '://unused@localhost/unused')
    try:
        value = Decimal('12345678901234567890.123456789012345678')
        assert engine.dialect.supports_native_decimal
        adapt_engine_dialect(engine)
        assert engine.dialect.supports_native_decimal
        column_type = sa.Numeric(38, 18).dialect_impl(engine.dialect)
        processor = column_type.bind_processor(engine.dialect)
        bound = processor(value) if processor else value
        assert isinstance(bound, Decimal)
        assert bound == value
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    'text',
    ['12345678901234567890.123456789012345678', '-12345678901234567890.123456789012345678', '0.000000000000000001'],
)
def test_decimal_query_compilation_keeps_exact_values(text):
    value = Decimal(text)
    for query in (
        Client().query(Item).filter(Item.value == value),
        Client().query(Item).filter_by(value=value),
        Client().query(Item).filter(Item.value.in_([value])),
    ):
        sql, params = query._build_sql()
        assert format(value, 'f') in sql
        assert params == []
    sql, params = Client().query(Item).filter_by(id=1).update(value=Item.value + value)._build_update_sql()
    assert format(value, 'f') in sql
    assert params == []
