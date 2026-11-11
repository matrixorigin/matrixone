# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""The public vector decorator must adapt to a usable MySQL TEXT instance."""

import pytest
from sqlalchemy import Column, MetaData, Table, insert, literal, select
from sqlalchemy.dialects import mysql
from sqlalchemy.schema import CreateTable

from matrixone.sqlalchemy_ext import VectorTypeDecorator


@pytest.mark.parametrize('precision', ['f32', 'f64'])
def test_vector_decorator_mysql_compilation_and_processors(precision):
    dialect = mysql.dialect()
    vector = VectorTypeDecorator(dimension=3, precision=precision)
    docs = Table('docs', MetaData(), Column('embedding', vector))
    assert 'embedding TEXT' in str(CreateTable(docs).compile(dialect=dialect))

    statement = select(literal([1, 2.5, -3], type_=vector))
    assert "'[1,2.5,-3]'" in str(statement.compile(dialect=dialect, compile_kwargs={'literal_binds': True}))

    compiled = insert(docs).values(embedding=[1, 2.5, -3]).compile(dialect=dialect)
    assert compiled.params == {'embedding': [1, 2.5, -3]}
    implementation = vector.dialect_impl(dialect)
    bind = implementation.bind_processor(dialect)
    result = implementation.result_processor(dialect, None)
    assert bind([1, 2.5, -3]) == '[1,2.5,-3]'
    assert result('[1,2.5,-3]') == [1.0, 2.5, -3.0]
    assert bind(None) is None
    assert result(None) is None
