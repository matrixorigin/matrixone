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
    ],
)
def test_simplified_types_and_primary_keys(definition, expected):
    table = simplified_table('typed', {'key': definition}, primary_key='key')
    ddl = str(CreateTable(table).compile(dialect=MatrixOneDialect()))
    assert expected in ddl and 'PRIMARY KEY' in ddl and table.c.key.primary_key


def test_unknown_primary_key_fails_instead_of_being_ignored():
    with pytest.raises(ValueError, match='existing columns'):
        simplified_table('typed', {'key': 'int'}, primary_key='missing')


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
