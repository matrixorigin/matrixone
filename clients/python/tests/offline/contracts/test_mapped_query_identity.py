# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Mapped queries preserve SQLAlchemy's physical identity and type contract."""

import enum
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import Column, Enum, Integer, JSON, String, TypeDecorator, create_engine, select
from sqlalchemy.orm import aliased, declarative_base
from sqlalchemy.schema import CreateTable

from matrixone import AsyncClient, Client
from matrixone.client import ResultSet
from matrixone._query_compilation import buffer_result
from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect
from matrixone.sqlalchemy_ext.table_builder import simplified_table

Base = declarative_base()


class Encoded(TypeDecorator):
    impl = String
    cache_ok = True

    def process_bind_param(self, value, dialect):
        return 'encoded:' + value if value is not None else None

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return value[len('encoded:') :] if value.startswith('encoded:') else value


class State(enum.Enum):
    READY = 'ready'


class Item(Base):
    __tablename__ = 'item'
    id = Column(Integer, primary_key=True)
    label = Column('physical_label', String(20))
    tag = Column(Encoded())
    doc = Column(JSON)
    state = Column(Enum(State))


@pytest.mark.parametrize('factory', [Client, AsyncClient])
def test_expression_inference_and_complete_mixed_projection(factory):
    client = factory()
    sql, _ = client.query(Item.id.label('ident'))._build_sql()
    assert 'FROM item' in sql and 'item.id AS ident' in sql
    sql, _ = client.query(Item, Item.id.label('ident'))._build_sql()
    # SQLAlchemy 1.4 and 2.x may order mapped properties differently. Preserve
    # each mapper's public projection contract, including the extra expression.
    reference = str(select(Item, Item.id.label('ident')).compile(dialect=MatrixOneDialect(paramstyle='qmark')))
    assert sql.startswith(reference.split('\nFROM', 1)[0].strip())
    a = aliased(Item, name='other')
    sql, _ = client.query(a.id.label('ident'))._build_sql()
    assert 'FROM item AS other' in sql and 'other.id AS ident' in sql


@pytest.mark.asyncio
@pytest.mark.parametrize('factory', [Client, AsyncClient])
async def test_scalar_keeps_ordinal_and_query_state(factory):
    client = factory()
    response = ResultSet(['duplicate', 'duplicate'], [(10, 20)])
    client.execute = AsyncMock(return_value=response) if factory is AsyncClient else Mock(return_value=response)
    query = client.query(Item).select(Item.id.label('duplicate'), Item.id.label('duplicate')).limit(7)
    value = query.scalar()
    if factory is AsyncClient:
        value = await value
    assert value == 10
    assert query._limit_count == 7


def test_raw_and_typed_results_use_declared_processors_once():
    engine = create_engine('sqlite://')
    try:
        Base.metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(
                Item.__table__.insert(),
                {'id': 1, 'label': 'name', 'tag': 'value', 'doc': {'a': 1}, 'state': State.READY},
            )
        client = Client()
        client._engine = engine
        with engine.connect() as connection:
            native = connection.execute(select(Item.__table__)).one()
            raw = buffer_result(ResultSet, connection.exec_driver_sql('SELECT * FROM item'))
            typed = buffer_result(ResultSet, connection.execute(select(Item.__table__)))
        for result in (raw, typed):
            row = client.query(Item)._materialize(result)[0]
            assert (row.id, row.label, row.tag, row.doc, row.state) == tuple(native)
    finally:
        engine.dispose()


def test_changed_physical_order_and_renamed_attribute():
    client = Client()
    client.execute = Mock(
        return_value=ResultSet(
            ['added', 'doc', 'state', 'tag', 'physical_label', 'id'],
            [(99, '{"a":1}', 'READY', 'encoded:value', 'name', 1)],
        )
    )
    row = client.query(Item).all()[0]
    assert (row.id, row.label, row.tag, row.doc, row.state) == (1, 'name', 'value', {'a': 1}, State.READY)


@pytest.mark.parametrize('factory', [Client, AsyncClient])
def test_snapshot_mutations_are_rejected_before_dispatch(factory):
    client = factory()
    client.execute = Mock(side_effect=AssertionError('must not dispatch'))
    query = client.query(Item, snapshot='historical').update(tag='new')
    with pytest.raises(ValueError, match='read-only'):
        query._build_update_sql()
    query._query_type = 'DELETE'
    with pytest.raises(ValueError, match='read-only'):
        query._build_delete_sql()
    client.execute.assert_not_called()


def test_typed_update_uses_mapping_and_bind_processors():
    client = Client()
    sql = client.query(Item).update(tag='value', label='renamed', state=State.READY).to_sql()
    assert "tag = 'encoded:value'" in sql
    assert "physical_label = 'renamed'" in sql
    assert "state = 'READY'" in sql
    with pytest.raises(ValueError, match='Unknown mapped'):
        client.query(Item).update(typo='value')


@pytest.mark.parametrize(
    'definition,expected',
    [
        ('double', 'DOUBLE'),
        ('binary(4)', 'BINARY(4)'),
        ('varchar(20)', 'VARCHAR(20)'),
        ('char(4)', 'CHAR(4)'),
        ('decimal(20,6)', 'DECIMAL(20, 6)'),
        ('boolean', 'BOOL'),
        ('datetime(6)', 'DATETIME(6)'),
        ('timestamp(3)', 'TIMESTAMP(3)'),
        ('time(6)', 'TIME(6)'),
    ],
)
def test_simplified_types_and_primary_keys(definition, expected):
    table = simplified_table('typed', {'key': definition}, primary_key='key')
    ddl = str(CreateTable(table).compile(dialect=MatrixOneDialect()))
    assert expected in ddl and 'PRIMARY KEY' in ddl and table.c.key.primary_key


def test_unknown_primary_key_fails_instead_of_being_ignored():
    with pytest.raises(ValueError, match='existing columns'):
        simplified_table('typed', {'key': 'int'}, primary_key='missing')


@pytest.mark.parametrize('kind', ['datetime', 'timestamp', 'time'])
@pytest.mark.parametrize('precision', [0, 3, 6])
def test_temporal_shorthand_uses_fractional_precision(kind, precision):
    from sqlalchemy.dialects import mysql

    actual = simplified_table('typed', {'value': '%s(%d)' % (kind, precision)}).c.value.type
    expected = getattr(mysql, kind.upper())(fsp=precision)
    assert actual.compile(dialect=MatrixOneDialect()) == expected.compile(dialect=MatrixOneDialect())
    assert actual.fsp == precision and actual.timezone is False


@pytest.mark.parametrize('definition', ['datetime()', 'timestamp(7)', 'time(-1)', 'time(2,3)', 'datetime(bad)'])
def test_invalid_temporal_precision_is_rejected(definition):
    with pytest.raises(ValueError, match='Temporal precision'):
        simplified_table('typed', {'value': definition})


def test_table_names_keep_schema_quoted_dots_and_metadata_defaults():
    from sqlalchemy import MetaData
    from sqlalchemy.sql.elements import quoted_name

    metadata = MetaData(schema='default_db')
    for name in ['archive.events', '`archive`.`events`', '"archive"."events"']:
        actual = simplified_table(name, {'id': 'int'}, metadata=metadata, extend_existing=True)
        assert (actual.schema, actual.name) == ('archive', 'events')
    for name in ['`atomic.dot`', quoted_name('atomic.dot', True)]:
        actual = simplified_table(name, {'id': 'int'})
        assert actual.schema is None and actual.name == 'atomic.dot' and actual.name.quote
    assert simplified_table('plain', {'id': 'int'}, metadata=metadata).schema == 'default_db'
    assert simplified_table('archive.same', {'id': 'int'}, schema='`archive`').schema == 'archive'
    with pytest.raises(ValueError, match='conflicts'):
        simplified_table('archive.wrong', {'id': 'int'}, schema='other')


@pytest.mark.asyncio
async def test_async_qualified_create_drop_and_precise_ddl():
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy import event

    engine = create_async_engine('sqlite+aiosqlite://')
    client = AsyncClient()
    client._engine = engine
    emitted = []
    event.listen(engine.sync_engine, 'before_execute', lambda conn, stmt, *args: emitted.append(stmt))
    try:
        async with engine.begin() as connection:
            await connection.exec_driver_sql("ATTACH DATABASE ':memory:' AS archive")
        await client.create_table('archive.events', {'id': 'int', 'occurred_at': 'datetime(6)'})
        statement = next(stmt for stmt in emitted if isinstance(stmt, CreateTable))
        assert 'DATETIME(6)' in str(statement.compile(dialect=MatrixOneDialect()))
        async with engine.connect() as connection:
            assert (await connection.exec_driver_sql('SELECT name FROM main.sqlite_master')).all() == []
            assert (await connection.exec_driver_sql('SELECT name FROM archive.sqlite_master')).all() == [('events',)]
        await client.drop_table('archive.events')
        async with engine.connect() as connection:
            assert (await connection.exec_driver_sql('SELECT name FROM archive.sqlite_master')).all() == []
    finally:
        await engine.dispose()


@pytest.mark.parametrize('factory', [Client, AsyncClient])
def test_update_marker_looking_literals_and_expressions_preserve_assignments(factory):
    client = factory()
    sql, params = (
        client.query(Item)
        .filter(Item.id == 1)
        .update(label='x = ?', tag='bound = ?', id=Item.id + 1)
        ._build_current_sql()
    )
    assert "physical_label = 'x = ?'" in sql
    assert "tag = 'encoded:bound = ?'" in sql
    assert 'id = item.id + 1' in sql
    assert params == []
    sql, params = client.query('item').update(label='x = ?', id=Item.id + 1)._build_current_sql()
    assert 'label = ?' in sql and 'id = item.id + 1' in sql
    assert params == ['x = ?']


@pytest.mark.parametrize('factory', [Client, AsyncClient])
def test_column_only_join_requires_all_sources_and_preserves_identity(factory):
    remote_base = declarative_base()

    class Remote(remote_base):
        __tablename__ = 'remote_item'
        __table_args__ = {'schema': 'remote_db', 'extend_existing': True}
        id = Column(Integer, primary_key=True)
        label = Column(String(20))

    client = factory()
    query = client.query(Item.id, Remote.label)
    with pytest.raises(ValueError, match='explicit JOIN'):
        query._build_sql()
    query.join(Remote, Item.id == Remote.id)
    sql, _ = query._build_sql()
    assert 'FROM item INNER JOIN remote_db.remote_item' in sql
    assert 'remote_db.remote_item.label' in sql
    assert query._build_sql()[0] == sql

    peer = aliased(Item, name='peer')
    query = client.query(Item.id, peer.id.label('peer_id')).join(peer, peer.id == Item.id + 1)
    sql, _ = query._build_sql()
    assert 'JOIN item AS peer' in sql and 'peer.id AS peer_id' in sql


@pytest.mark.asyncio
@pytest.mark.parametrize('factory', [Client, AsyncClient])
async def test_unjoined_projection_rejects_mutations_before_dispatch(factory):
    client = factory()
    client.execute = AsyncMock() if factory is AsyncClient else Mock()
    peer = aliased(Item, name='peer_mutation')
    with pytest.raises(ValueError, match='explicit JOIN'):
        result = client.query(Item.id, peer.id).update(label='changed').execute()
        if factory is AsyncClient:
            await result
    with pytest.raises(ValueError, match='explicit JOIN'):
        result = client.query(Item.id, peer.id).delete()
        if factory is AsyncClient:
            await result
    client.execute.assert_not_called()


def test_unjoined_projection_mutations_leave_real_rows_unchanged():
    engine = create_engine('sqlite://')
    try:
        Base.metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(
                Item.__table__.insert(), [{'id': 1, 'physical_label': 'first'}, {'id': 2, 'physical_label': 'second'}]
            )
        client = Client()
        client._engine = engine
        peer = aliased(Item, name='peer_mutation')
        with pytest.raises(ValueError, match='explicit JOIN'):
            client.query(Item.id, peer.id).update(physical_label='changed').execute()
        with pytest.raises(ValueError, match='explicit JOIN'):
            client.query(Item.id, peer.id).delete()
        with engine.connect() as connection:
            assert connection.execute(select(Item.id, Item.label).order_by(Item.id)).all() == [
                (1, 'first'),
                (2, 'second'),
            ]
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_column_only_shorthand_works_through_all_session_entrypoints():
    from matrixone.client import Session as ClientSession
    from matrixone.session import Session, AsyncSession

    sync, asynchronous = Client(), AsyncClient()
    sessions = [ClientSession(client=sync), Session(client=sync), AsyncSession(client=asynchronous)]
    try:
        peer = aliased(Item, name='peer_session')
        for owner in [sync, asynchronous, *sessions]:
            query = owner.query(Item.id, peer.id).join(peer, peer.id == Item.id + 1)
            assert 'JOIN item AS peer_session' in query._build_sql()[0]
    finally:
        sessions[0].close()
        sessions[1].close()
        await sessions[2].close()
