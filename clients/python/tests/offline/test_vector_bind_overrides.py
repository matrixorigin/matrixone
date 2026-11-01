# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Vector decorators must honor the SQLAlchemy bind customization hook."""

from contextlib import closing

import pytest
from sqlalchemy import create_engine, literal, select
from sqlalchemy.exc import StatementError

from matrixone.sqlalchemy_ext.dialect import MatrixOneDialect
from matrixone.sqlalchemy_ext.vector_type import Vectorf32, Vectorf64, VectorTypeDecorator


@pytest.mark.parametrize('base_type', [Vectorf32, Vectorf64, VectorTypeDecorator])
@pytest.mark.parametrize('reject', [False, True])
@pytest.mark.parametrize('backend', ['matrixone', 'sqlite'])
def test_vector_bind_override(base_type, reject, backend):
    class CustomVector(base_type):
        cache_ok = True

        def process_bind_param(self, value, dialect):
            if reject:
                raise ValueError('custom vector rejected')
            return super().process_bind_param([item * 2 for item in value], dialect)

    vector = CustomVector()
    if backend == 'matrixone':
        dialect = MatrixOneDialect()
        processor = vector.dialect_impl(dialect).bind_processor(dialect)
        if reject:
            with pytest.raises(ValueError, match='custom vector rejected'):
                processor([1, 2])
        else:
            assert processor([1, 2]) == '[2,4]'
        return

    # Exercise a real SQLAlchemy execution, including error propagation.
    engine = create_engine('sqlite:///:memory:')
    try:
        with engine.connect() as connection:
            statement = select(literal([1, 2], type_=vector))
            if reject:
                with pytest.raises(StatementError, match='custom vector rejected'):
                    connection.execute(statement)
            else:
                with closing(connection.execute(statement)) as result:
                    assert result.scalar_one() == [2.0, 4.0]
    finally:
        engine.dispose()
