# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Compare builder semantics with SQLAlchemy using one immutable SQLite fixture."""

import asyncio
from inspect import isawaitable
from unittest.mock import Mock

import pytest
from sqlalchemy import Column, ForeignKey, Integer, String, bindparam, create_engine, exists, func, select, tuple_
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import aliased, declarative_base

from matrixone import AsyncClient, Client
from matrixone.client import ResultSet, Session as LegacySession
from matrixone.orm import LogicalIn, logical_in
from matrixone.session import AsyncSession, Session
from matrixone._query_compilation import compile_expression, statement_compilation

Base = declarative_base()


class Parent(Base):
    __tablename__ = 'context_parent'
    id = Column(Integer, primary_key=True)


class Child(Base):
    __tablename__ = 'context_child'
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey('context_parent.id'))
    amount = Column(Integer)


class Detail(Base):
    __tablename__ = 'context_detail'
    id = Column(Integer, primary_key=True)
    child_id = Column(Integer, ForeignKey('context_child.id'))


@pytest.fixture(scope='module')
def rows():
    # SQLAlchemy versions can share cache keys for auto-correlation and
    # correlate(None); the reference must compile each intended statement.
    engine = create_engine('sqlite://').execution_options(compiled_cache=None)
    Base.metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(Parent.__table__.insert(), [{'id': 1}, {'id': 2}, {'id': 3}])
        connection.execute(
            Child.__table__.insert(),
            [
                {'id': 10, 'parent_id': 1, 'amount': 2},
                {'id': 11, 'parent_id': 1, 'amount': 3},
                {'id': 12, 'parent_id': 2, 'amount': None},
                {'id': 13, 'parent_id': None, 'amount': 7},
            ],
        )
        connection.execute(Detail.__table__.insert(), [{'id': 100, 'child_id': 10}])
    try:
        with engine.connect() as connection:
            yield connection
    finally:
        engine.dispose()


@pytest.fixture(params=['client', 'async_client', 'session', 'async_session', 'legacy_session'])
def owner(request):
    if request.param == 'client':
        return Client()
    if request.param == 'async_client':
        return AsyncClient()
    if request.param == 'async_session':
        return AsyncSession(client=AsyncClient())
    cls = Session if request.param == 'session' else LegacySession
    return cls(client=Client())


def execute(rows, query):
    sql, parameters = query._build_sql()
    return rows.exec_driver_sql(sql, tuple(parameters)).all()


def connect_owner_to_rows(owner, rows):
    calls = []

    def dispatch(sql, parameters=None):
        calls.append(sql)
        result = rows.exec_driver_sql(sql, tuple(parameters or ()))
        return ResultSet(list(result.keys()), result.all())

    async def async_dispatch(sql, parameters=None):
        return dispatch(sql, parameters)

    owner.execute = async_dispatch if isinstance(owner, (AsyncClient, AsyncSession)) else dispatch
    return calls


async def resolve(value):
    return await value if isawaitable(value) else value


@pytest.mark.asyncio
@pytest.mark.parametrize('alias_form', ['named', 'anonymous', 'realias'])
async def test_derived_alias_binds_are_deferred_and_refreshed_per_public_execution(rows, owner, alias_form):
    calls = []
    bound = bindparam('threshold', callable_=lambda: calls.append(len(calls) + 1) or calls[-1], type_=Integer)
    name = None if alias_form == 'anonymous' else 'derived'
    parent = aliased(Parent, select(Parent).where(Parent.id > bound).subquery(name))
    connect_owner_to_rows(owner, rows)
    query = owner.query(parent).order_by(parent.id)
    if alias_form == 'realias':
        query.alias('renamed')
    assert calls == []
    assert [row.id for row in await resolve(query.all())] == [2, 3]
    assert calls == [1]
    assert [row.id for row in await resolve(query.all())] == [3]
    assert calls == [1, 2]


@pytest.mark.asyncio
async def test_derived_alias_required_default_and_null_binds(rows, owner):
    calls = connect_owner_to_rows(owner, rows)

    def mapped_alias(bound):
        return aliased(Parent, select(Parent).where(Parent.id > bound).subquery('derived'))

    missing = mapped_alias(bindparam('threshold', required=True, type_=Integer))
    query = owner.query(missing)
    with pytest.raises(InvalidRequestError, match='threshold'):
        await resolve(query.all())
    assert calls == []
    for value, expected in ((1, [2, 3]), (None, [])):
        parent = mapped_alias(bindparam('threshold', value, type_=Integer))
        query = owner.query(parent).order_by(parent.id)
        assert [row.id for row in await resolve(query.all())] == expected
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_derived_alias_mutations_reject_before_callable_or_dispatch(rows, owner):
    calls = []
    bound = bindparam('threshold', callable_=lambda: calls.append('bind') or 1, type_=Integer)
    parent = aliased(Parent, select(Parent).where(Parent.id > bound).subquery('derived'))
    owner.execute = Mock(side_effect=AssertionError('must reject before execution'))
    for operation in ('update', 'delete'):
        query = owner.query(parent)
        with pytest.raises(ValueError, match='Derived query sources'):
            await resolve(query.update(id=9).execute() if operation == 'update' else query.delete())
    assert calls == []
    owner.execute.assert_not_called()
    assert rows.exec_driver_sql('SELECT id FROM context_parent ORDER BY id').all() == [(1,), (2,), (3,)]


def test_derived_alias_shares_bind_and_isolates_reentrant_query(rows):
    client = Client()
    connect_owner_to_rows(client, rows)
    calls, inner_values = [], []

    def value():
        inner_values.append(client.query(Parent).select(bindparam('threshold', 9)).filter(Parent.id == 1).scalar())
        calls.append(len(calls) + 1)
        return calls[-1]

    shared = bindparam('threshold', callable_=value, type_=Integer)
    parent = aliased(Parent, select(Parent).where(Parent.id > shared).subquery('derived'))
    query = client.query(parent, shared.label('value')).order_by(parent.id)
    assert calls == inner_values == []
    assert [tuple(row) for row in query.all()] == [(2, 1), (3, 1)]
    assert [tuple(row) for row in query.all()] == [(3, 2)]
    assert calls == [1, 2] and inner_values == [9, 9]


def test_derived_alias_failed_callable_does_not_leak_scope(rows):
    client = Client()
    dispatches = connect_owner_to_rows(client, rows)
    calls = []

    def value():
        calls.append('called')
        if len(calls) == 1:
            raise RuntimeError('callable failed')
        return 1

    parent = aliased(Parent, select(Parent).where(Parent.id > bindparam('threshold', callable_=value)).subquery('s'))
    query = client.query(parent).order_by(parent.id)
    assert calls == []
    with pytest.raises(RuntimeError, match='callable failed'):
        query.all()
    assert dispatches == []
    assert [row.id for row in query.all()] == [2, 3]
    assert calls == ['called', 'called']
    assert len(dispatches) == 1


def test_derived_alias_logical_in_does_not_compile_list_source_twice(rows):
    calls = []
    bound = bindparam('threshold', callable_=lambda: calls.append(1) or 1, type_=Integer)
    parent = aliased(Parent, select(Parent).where(Parent.id > bound).subquery('derived'))
    query = Client().query(parent).logical_in(parent.id, [2, 3])
    assert calls == []
    assert execute(rows, query) == [(2,), (3,)]
    assert calls == [1]


def test_derived_alias_fulltext_source_uses_the_statement_bind_scope():
    from matrixone.sqlalchemy_ext.fulltext_search import boolean_match

    calls = []
    bound = bindparam('threshold', callable_=lambda: calls.append(len(calls) + 1) or calls[-1], type_=Integer)
    parent = aliased(Parent, select(Parent).where(Parent.id > bound).subquery('derived'))
    query = Client().query(parent).logical_in(parent.id, boolean_match('id').must('term'))
    assert calls == []
    sql, _ = query._build_sql()
    assert 'MATCH(id)' in sql and calls == [1]
    sql, _ = query._build_sql()
    assert 'MATCH(id)' in sql and calls == [1, 2]


def test_alias_fulltext_filter_attribute_is_checked_once():
    from matrixone.sqlalchemy_ext.fulltext_search import boolean_match

    class Filter:
        def __init__(self):
            self.filter = boolean_match('id').must('term')
            self.lookups = 0

        @property
        def query_builder(self):
            self.lookups += 1
            if self.lookups > 1:
                raise RuntimeError('query_builder accessed twice')
            return self.filter.query_builder

        def compile(self, **kwargs):
            return self.filter.compile(**kwargs)

    parent, value = aliased(Parent, name='p'), Filter()
    query = Client().query(parent).logical_in(parent.id, value)
    assert 'MATCH(id)' in query.to_sql()
    assert value.lookups == 1


@pytest.mark.parametrize('inferred', [False, True])
@pytest.mark.parametrize('method', ['logical_in', 'filter'])
@pytest.mark.parametrize('realias', [False, True])
@pytest.mark.parametrize('column_kind', ['expression', 'string'])
def test_logical_in_uses_current_derived_source(rows, inferred, method, realias, column_kind):
    class Predicate:
        query_builder = object()

        def compile(self, **kwargs):
            return 'id > 11'

    calls = []
    bound = bindparam('threshold', callable_=lambda: calls.append(len(calls) + 1) or calls[-1], type_=Integer)
    source = select((Parent.id + 10).label('id')).where(Parent.id > bound).subquery('derived')
    parent = aliased(Parent, source, adapt_on_names=True)
    query = Client().query(parent.id if inferred else parent).select(parent.id).order_by(parent.id)
    column = parent.id if column_kind == 'expression' else 'id'
    if method == 'logical_in':
        query.logical_in(column, Predicate())
    else:
        query.filter(logical_in(column, Predicate()))
    if realias:
        query.alias('renamed')
    assert calls == []
    assert execute(rows, query) == [(12,), (13,)] and calls == [1]
    assert execute(rows, query) == [(13,)] and calls == [1, 2]


def test_logical_in_fulltext_alias_rendering_and_extension_controls():
    from matrixone.sqlalchemy_ext.fulltext_search import boolean_match

    parent = aliased(Parent, name='p')
    for method in ('logical_in', 'filter'):
        query = Client().query(parent)
        condition = boolean_match('id').must('term')
        query = (
            query.logical_in('id', condition) if method == 'logical_in' else query.filter(logical_in('id', condition))
        )
        sql = query.to_sql()
        assert 'SELECT id FROM context_parent AS p WHERE MATCH(id)' in sql

    class CustomLogicalIn(LogicalIn):
        def compile_for_query(self, model, alias):
            return '1=1'

    assert 'WHERE 1=1' in Client().query(Parent).filter(CustomLogicalIn('id', [])).to_sql()
    with pytest.raises(ValueError, match='carry their own parameters'):
        Client().query(Parent).filter(logical_in('id', [1]), 1).to_sql()
    sql = Client().query('odd;table').logical_in('id', boolean_match('id').must('term')).to_sql()
    assert 'SELECT id FROM `odd;table` WHERE MATCH(id)' in sql


@pytest.mark.parametrize('method', ['logical_in', 'filter'])
def test_logical_in_retains_quoted_schema_and_table(rows, method):
    class Predicate:
        query_builder = object()

        def compile(self, **kwargs):
            return 'id > 1'

    base = declarative_base()

    class Dotted(base):
        __tablename__ = 'odd.table'
        __table_args__ = {'schema': 'main'}
        id = Column(Integer, primary_key=True)

    base.metadata.create_all(rows)
    try:
        rows.execute(Dotted.__table__.insert(), [{'id': 1}, {'id': 2}])
        source = aliased(Dotted, name='order')
        query = Client().query(source).select(source.id)
        if method == 'logical_in':
            query.logical_in(source.id, Predicate())
        else:
            query.filter(logical_in(source.id, Predicate()))
        sql, _ = query._build_sql()
        assert 'SELECT id FROM main.`odd.table` AS `order` WHERE' in sql
        assert execute(rows, query) == [(2,)]
    finally:
        base.metadata.drop_all(rows)


@pytest.mark.parametrize('alias_name', [None, 'named', 'order', 'name.with.dot'])
@pytest.mark.parametrize('realias', [False, True])
def test_async_explicit_database_renders_physical_alias_identity(rows, alias_name, realias):
    from matrixone.async_orm import AsyncMatrixOneQuery

    parent = aliased(Parent, name=alias_name)
    query = AsyncMatrixOneQuery(parent, AsyncClient(), database='main').select(parent.id).order_by(parent.id)
    if realias:
        query.alias('renamed')
    assert execute(rows, query) == [(1,), (2,), (3,)]


@pytest.mark.asyncio
async def test_async_explicit_database_anonymous_alias_public_result(rows):
    from matrixone.async_orm import AsyncMatrixOneQuery

    parent = aliased(Parent)
    client = AsyncClient()
    connect_owner_to_rows(client, rows)
    query = AsyncMatrixOneQuery(parent, client, database='main').order_by(parent.id)
    assert [row.id for row in await query.all()] == [1, 2, 3]


def test_async_explicit_database_quotes_reserved_physical_name(rows):
    from sqlalchemy.sql.elements import quoted_name
    from matrixone.async_orm import AsyncMatrixOneQuery

    base = declarative_base()

    class Reserved(base):
        __tablename__ = quoted_name('select', False)
        id = Column(Integer, primary_key=True)

    rows.exec_driver_sql('CREATE TABLE "select" (id INTEGER PRIMARY KEY)')
    try:
        rows.exec_driver_sql('INSERT INTO "select" VALUES (1)')
        for name in ('p', None):
            parent = aliased(Reserved, name=name)
            assert execute(rows, AsyncMatrixOneQuery(parent, AsyncClient(), database='main')) == [(1,)]
    finally:
        rows.exec_driver_sql('DROP TABLE "select"')


def test_async_explicit_database_keeps_explicit_alias_identifier_quoting(rows):
    from sqlalchemy.sql.elements import quoted_name
    from matrixone.async_orm import AsyncMatrixOneQuery

    parent = aliased(Parent, name=quoted_name('alias; select 1', False))
    query = AsyncMatrixOneQuery(parent, AsyncClient(), database='main').select('id')
    sql, _ = query._build_sql()
    assert 'AS `alias; select 1`' in sql
    assert execute(rows, query) == [(1,), (2,), (3,)]


def test_async_explicit_database_accepts_long_anonymous_source(rows):
    from matrixone.async_orm import AsyncMatrixOneQuery

    base = declarative_base()

    class LongName(base):
        __tablename__ = 't' * 64
        id = Column(Integer, primary_key=True)

    base.metadata.create_all(rows)
    try:
        rows.execute(LongName.__table__.insert(), [{'id': 1}])
        parent = aliased(LongName)
        query = AsyncMatrixOneQuery(parent, AsyncClient(), database='main').select(parent.id)
        assert execute(rows, query) == [(1,)]
        with pytest.raises(ValueError, match='1..64'):
            execute(rows, AsyncMatrixOneQuery(aliased(LongName, name='x' * 65), AsyncClient(), database='main'))
    finally:
        base.metadata.drop_all(rows)


def test_alias_root_target_chain_and_realias_share_one_identity(rows, owner):
    parent, child = aliased(Parent, name='p'), aliased(Child, name='c')
    query = owner.query(parent).select(parent.id, child.amount).filter(parent.id > 0).join(child)
    assert execute(rows, query.order_by(child.id)) == [(1, 2), (1, 3), (2, None)]
    assert execute(rows, query.alias('renamed')) == [(1, 2), (1, 3), (2, None)]
    chained = owner.query(parent).select(parent.id).join(child).join(Detail).order_by(parent.id)
    assert execute(rows, chained) == [(1,)]
    root = owner.query(Parent.id).filter(Parent.id == 2).alias('inferred')
    assert execute(rows, root) == [(2,)]


def test_scalar_exists_and_explicit_correlation_controls(rows, owner):
    parent = aliased(Parent, name='p')
    for correlation in ('auto', 'explicit', 'except', 'none'):
        scalar = select(func.sum(Child.amount)).where(Child.parent_id == parent.id)
        if correlation == 'explicit':
            scalar = scalar.correlate(parent)
        elif correlation == 'except':
            scalar = scalar.correlate_except(Child)
        elif correlation == 'none':
            scalar = scalar.correlate(None)
        value = scalar.scalar_subquery().label('total')
        reference = rows.execute(select(parent.id, value).order_by(parent.id)).all()
        expected = [(1, 5), (2, 5), (3, 5)] if correlation == 'none' else [(1, 5), (2, None), (3, None)]
        assert reference == expected
        query = owner.query(parent).select(parent.id, value).order_by(parent.id)
        assert execute(rows, query) == reference
        assert execute(rows, query.alias('changed')) == reference
    predicate = exists(select(Child.id).where(Child.parent_id == Parent.id, Child.amount > 0))
    assert execute(rows, owner.query(Parent).select(Parent.id).filter(predicate)) == [(1,)]
    assert execute(rows, owner.query(Parent).select(Parent.id).filter(~predicate).order_by(Parent.id)) == [(2,), (3,)]


def test_filter_before_join_and_normalized_string_sources(rows):
    predicate = exists(select(Detail.id).where(Detail.child_id == Child.id))
    client = Client()
    for before in (True, False):
        query = client.query(Parent).select(Parent.id)
        if before:
            query.filter(predicate)
        query.join(Child)
        if not before:
            query.filter(predicate)
        assert execute(rows, query) == [(1,)]
    scalar = select(func.sum(Child.amount)).where(Child.parent_id == Parent.id).scalar_subquery().label('total')
    query = client.query('context_parent').select(Parent.id, scalar).order_by(Parent.id).alias('strings')
    assert execute(rows, query) == [(1, 5), (2, None), (3, None)]


def test_raw_join_correlation_rejects_opaque_context_without_rewriting_sql(rows):
    child = aliased(Child, name='c')
    predicate = exists(select(Detail.id).where(Detail.child_id == child.id))
    client = Client()
    client.execute = Mock(side_effect=AssertionError('must reject before execution'))
    query = client.query(Parent).select(Parent.id).join('context_child AS c', 'c.parent_id=context_parent.id')
    with pytest.raises(ValueError, match='raw JOIN'):
        query.filter(predicate).all()
    client.execute.assert_not_called()
    assert execute(rows, client.query(Parent).select(Parent.id).join(child).filter(predicate)) == [(1,)]
    raw = client.query(Parent).select(Parent.id).join('context_child AS c', 'c.parent_id=context_parent.id')
    assert execute(rows, raw.filter(predicate.correlate(None)).order_by(Parent.id)) == [(1,), (1,), (2,)]


def test_explicit_async_database_keeps_alias_and_cte_identity(rows):
    from matrixone.async_orm import AsyncMatrixOneQuery

    parent = aliased(Parent, name='p')
    query = AsyncMatrixOneQuery(parent, AsyncClient(), database='main').filter(parent.id == 2).alias('renamed')
    assert execute(rows, query) == [(2,)]
    cte = Client().query(Parent).select(Parent.id).filter(Parent.id == 3).cte('order')
    query = AsyncClient().query(cte)
    query.database = 'main'
    assert execute(rows, query) == [(3,)]


def test_missing_required_binds_reject_before_execution_and_reuse(rows, owner):
    required = bindparam('threshold', required=True, type_=Integer)
    queries = [
        owner.query(Parent).filter(Parent.id > required),
        owner.query(Parent).select(required.label('missing')),
        owner.query(Parent).group_by(Parent.id).having(func.count() > required),
        owner.query(Parent).update(id=required),
    ]
    owner.execute = Mock(side_effect=AssertionError('must not execute'))
    for query in queries:
        with pytest.raises(InvalidRequestError, match='threshold'):
            query._build_current_sql()
    owner.execute.assert_not_called()
    assert execute(rows, owner.query(Parent).filter(Parent.id > bindparam('threshold', None))) == []
    assert execute(rows, owner.query(Parent).filter(Parent.id > bindparam('threshold', 2))) == [(3,)]
    assert rows.exec_driver_sql('SELECT id FROM context_parent ORDER BY id').all() == [(1,), (2,), (3,)]


@pytest.mark.parametrize('unique', [False, True])
def test_callable_bind_has_one_value_per_statement_and_survives_alias(rows, unique):
    calls = []

    def value():
        calls.append(len(calls) + 1)
        return calls[-1]

    shared = bindparam('value', callable_=value, unique=unique, type_=Integer)
    query = Client().query(Parent).select(shared.label('a'), shared.label('b')).filter(Parent.id == shared).alias('p')
    assert execute(rows, query) == [(1, 1)] and calls == [1]
    assert execute(rows, query) == [(2, 2)] and calls == [1, 2]


def test_distinct_keys_and_explicit_unique_binds_stay_independent(rows):
    calls = []

    def value():
        calls.append(len(calls) + 1)
        return calls[-1]

    query = (
        Client()
        .query(Parent)
        .select(bindparam('first', callable_=value).label('a'), bindparam('second', callable_=value).label('b'))
        .filter(Parent.id == 1)
    )
    assert execute(rows, query) == [(1, 2)] and calls == [1, 2]
    shared = bindparam('value', callable_=value)
    query = (
        Client()
        .query(Parent)
        .select((shared + 0).unique_params().label('a'), (shared + 0).unique_params().label('b'))
        .filter(Parent.id == 1)
    )
    assert execute(rows, query) == [(3, 4)] and calls == [1, 2, 3, 4]


def test_callable_reentrant_query_owns_an_independent_statement_scope(rows):
    from matrixone.client import ResultSet

    client = Client()
    inner_values = []

    def dispatch(sql, parameters=None):
        result = rows.exec_driver_sql(sql, tuple(parameters or ()))
        return ResultSet(list(result.keys()), result.all())

    client.execute = dispatch

    def value():
        inner = client.query(Parent).select(bindparam('shared_name', 2)).filter(Parent.id == 1).scalar()
        inner_values.append(inner)
        return 3

    shared = bindparam('shared_name', callable_=value)
    outer = client.query(Parent).select(shared.label('a'), shared.label('b')).filter(Parent.id == 1)
    assert [tuple(row) for row in outer.all()] == [(3, 3)] and inner_values == [2]


def test_type_processor_reentrant_query_has_an_independent_scope(rows):
    from sqlalchemy import TypeDecorator
    from matrixone.client import ResultSet

    client = Client()
    inner_values = []

    def dispatch(sql, parameters=None):
        result = rows.exec_driver_sql(sql, tuple(parameters or ()))
        return ResultSet(list(result.keys()), result.all())

    client.execute = dispatch

    class Converted(TypeDecorator):
        impl = Integer
        cache_ok = True

        def process_bind_param(self, value, dialect):
            inner_values.append(
                client.query(Parent).select(bindparam('shared_name', 2)).filter(Parent.id == 1).scalar()
            )
            return value + 2

    value = bindparam('shared_name', 3, type_=Converted())
    query = client.query(Parent).select(value.label('value')).filter(Parent.id == 1)
    assert [tuple(row) for row in query.all()] == [(5,)] and inner_values == [2]


def test_string_query_cannot_silently_switch_to_a_same_named_derived_source(rows):
    source = select(Child.id).subquery('context_parent')
    query = Client().query('context_parent').select('id').filter(source.c.id > 0)
    for _ in range(2):
        with pytest.raises(ValueError, match='String table names cannot be replaced'):
            query._build_sql()
    physical = Client().query('context_parent').select('id').filter(Parent.id > 1)
    assert execute(rows, physical) == execute(rows, physical) == [(2,), (3,)]


def test_custom_clause_normalization_is_once_and_outside_parent_bind_scope(rows):
    from matrixone.client import ResultSet

    client = Client()
    inner_values = []

    def dispatch(sql, parameters=None):
        result = rows.exec_driver_sql(sql, tuple(parameters or ()))
        return ResultSet(list(result.keys()), result.all())

    client.execute = dispatch

    class Custom:
        name = 'value'
        type = Integer()

        def __clause_element__(self):
            inner_values.append(
                client.query(Parent).select(bindparam('shared_name', 2)).filter(Parent.id == 1).scalar()
            )
            return bindparam('shared_name', 3).label('value')

        def compile(self, **kwargs):
            return self.__clause_element__().compile(**kwargs)

    query = client.query('context_parent').select(Custom()).filter(Parent.id == 1)
    assert [tuple(row) for row in query.all()] == [(3,)] and inner_values == [2]


@pytest.mark.parametrize('distinct', [False, True])
def test_result_naming_does_not_evaluate_projected_callable_again(rows, distinct):
    from matrixone.client import ResultSet

    client = Client()
    calls = []

    def dispatch(sql, parameters=None):
        result = rows.exec_driver_sql(sql, tuple(parameters or ()))
        return ResultSet(list(result.keys()), result.all())

    client.execute = dispatch
    value = bindparam('value', callable_=lambda: calls.append(3) or 3)
    query = client.query(Parent).select(func.distinct(value) if distinct else value).filter(Parent.id == 1)
    assert [tuple(row) for row in query.all()] == [(3,)] and calls == [3]


@pytest.mark.parametrize(
    'second', [bindparam('value', 2), bindparam('value', None), bindparam('value', '1', type_=String)]
)
def test_conflicting_fragment_bind_definitions_reject_and_scope_is_reset(rows, second):
    client = Client()
    query = client.query(Parent).select(bindparam('value', 1).label('a'), second.label('b'))
    with pytest.raises(InvalidRequestError, match='Conflicting definitions'):
        query._build_sql()
    assert execute(rows, client.query(Parent).select(bindparam('value', 3)).filter(Parent.id == 1)) == [(3,)]


def test_expansion_tuple_types_and_wrapper_from_callable_isolation(rows):
    query = Client().query(Parent).filter(tuple_(Parent.id, Parent.id).in_([(1, 1), (3, 3)])).order_by(Parent.id)
    assert execute(rows, query) == [(1,), (3,)]
    calls = []
    source = (
        select(Parent.id).where(Parent.id > bindparam('source', callable_=lambda: calls.append(0) or 0)).subquery('s')
    )
    assert 's.id' in compile_expression(source.c.id, outer_froms=(source,))
    assert calls == []
    query = Client().query(source.c.id).filter(source.c.id < 3).alias('renamed')
    assert execute(rows, query) == [(1,), (2,)] and calls == [0]
    with pytest.raises(ValueError, match='Derived'):
        Client().query(source.c.id).update(id=1)._build_update_sql()


def test_query_backed_cte_does_not_cache_callables_between_statements(rows):
    calls = []
    shared = bindparam('value', callable_=lambda: calls.append(len(calls) + 1) or calls[-1])
    cte = Client().query(Parent).select(shared.label('value')).filter(Parent.id == 1).cte('generated')
    query = Client().query(cte)
    assert execute(rows, query) == [(1,)]
    assert execute(rows, query) == [(2,)] and calls == [1, 2]

    @statement_compilation
    def repeated():
        return cte.as_sql(), cte.as_sql()

    left, right = repeated()
    assert left == right and calls == [1, 2, 3]


@pytest.mark.asyncio
async def test_inherited_completed_compilation_scope_is_not_reused():
    calls, tasks = [], []
    release = asyncio.Event()
    query = None

    async def inherited():
        await release.wait()
        return query.to_sql()

    def value():
        calls.append(len(calls) + 1)
        if not tasks:
            tasks.append(asyncio.create_task(inherited()))
        return calls[-1]

    shared = bindparam('value', callable_=value)
    query = Client().query(Parent).select(shared.label('value'))
    assert 'SELECT 1 AS value' in query.to_sql()
    release.set()
    try:
        assert 'SELECT 2 AS value' in await asyncio.wait_for(tasks[0], 1)
    finally:
        await asyncio.gather(*tasks, return_exceptions=True)
    assert calls == [1, 2]
