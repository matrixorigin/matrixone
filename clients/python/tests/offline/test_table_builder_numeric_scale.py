# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""The table builder forwards both numeric precision and scale to SQLAlchemy."""

import pytest
from sqlalchemy.schema import CreateTable

from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect
from matrixone.sqlalchemy_ext.table_builder import VectorTableBuilder


@pytest.mark.parametrize('kind', ['float', 'double', 'decimal', 'numeric'])
@pytest.mark.parametrize('scale', [0, 2])
def test_numeric_column_preserves_precision_and_scale(kind, scale):
    table = (
        VectorTableBuilder('measurements')
        .add_numeric_column('amount', kind, precision=10, scale=scale, nullable=False)
        .build()
    )
    assert table.c.amount.type.precision == 10
    assert table.c.amount.type.scale == scale
    sql_type = kind.upper() if kind in ('float', 'double') else 'NUMERIC'
    ddl = str(CreateTable(table).compile(dialect=MatrixOneDialect()))
    assert f'amount {sql_type}(10, {scale}) NOT NULL' in ddl


@pytest.mark.parametrize('kind', ['float', 'double'])
def test_floating_column_without_precision_or_scale(kind):
    table = VectorTableBuilder('measurements').add_numeric_column('amount', kind).build()
    assert table.c.amount.type.precision is None
    assert table.c.amount.type.scale is None
    ddl = str(CreateTable(table).compile(dialect=MatrixOneDialect()))
    assert f'amount {kind.upper()}\n' in ddl


def test_float_precision_only_remains_supported():
    table = VectorTableBuilder('measurements').add_numeric_column('amount', 'float', precision=24).build()
    ddl = str(CreateTable(table).compile(dialect=MatrixOneDialect()))
    assert 'amount FLOAT(24)' in ddl
