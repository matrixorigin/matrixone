# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Typed vector literals use the same validation as binds and dialect escaping."""

import numpy as np
import pytest
from sqlalchemy import Column, MetaData, String, Table, insert, literal, select
from sqlalchemy.exc import CompileError

from matrixone.sqlalchemy_ext import VectorType, Vectorf32, Vectorf64
from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect


@pytest.mark.parametrize('type_class', [VectorType, Vectorf32, Vectorf64])
@pytest.mark.parametrize(
    'value, wire',
    [
        ([1, 2.5, -3], '[1,2.5,-3]'),
        ((1, 2.5, -3), '[1,2.5,-3]'),
        (np.array([1, 2.5, -3]), '[1.0,2.5,-3.0]'),
        ('[1,2,3]', '[1,2,3]'),
        ("[1]'\\quoted%", "[1]'\\quoted%"),
        (None, None),
    ],
)
def test_vector_literal_compilation_matches_string_rendering(type_class, value, wire):
    dialect = MatrixOneDialect()
    vector = type_class(3)
    options = {'literal_binds': True}
    expected = str(select(literal(wire, type_=String())).compile(dialect=dialect, compile_kwargs=options))
    statement = select(literal(value, type_=vector))
    assert str(statement.compile(dialect=dialect, compile_kwargs=options)) == expected

    table = Table('docs', MetaData(), Column('embedding', vector))
    typed_insert = insert(table).values(embedding=value)
    string_insert = insert(table).values(embedding=literal(wire, type_=String()))
    assert str(typed_insert.compile(dialect=dialect, compile_kwargs=options)) == str(
        string_insert.compile(dialect=dialect, compile_kwargs=options)
    )
    implementation = vector.dialect_impl(dialect)
    assert implementation.bind_processor(dialect)(value) == wire


@pytest.mark.parametrize('type_class', [VectorType, Vectorf32, Vectorf64])
@pytest.mark.parametrize('value', [[float('nan')], [float('inf')], [True], ['not a number'], np.array([[1, 2]])])
def test_vector_literal_rejects_invalid_bind_values(type_class, value):
    dialect = MatrixOneDialect()
    vector = type_class()
    with pytest.raises((TypeError, ValueError)):
        vector.dialect_impl(dialect).bind_processor(dialect)(value)
    with pytest.raises(CompileError):
        select(literal(value, type_=vector)).compile(dialect=dialect, compile_kwargs={'literal_binds': True})
