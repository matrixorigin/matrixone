# Copyright 2021 - 2022 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Vector result conversion through SQLAlchemy's real result pipeline."""
import pytest
from sqlalchemy import create_engine, literal, select, type_coerce
from matrixone.sqlalchemy_ext.vector_type import VectorType, Vectorf32, Vectorf64, VectorTypeDecorator


@pytest.mark.parametrize('type_class', [VectorType, Vectorf32, Vectorf64, VectorTypeDecorator])
@pytest.mark.parametrize(
    'value,expected',
    [
        ('[1,2]', [1.0, 2.0]),
        (' \n[1, 2]\t', [1.0, 2.0]),
        ('[ ]', []),
        (' \t[]\n', []),
        (None, None),
        ('invalid', 'invalid'),
    ],
)
def test_vector_results_with_whitespace(type_class, value, expected):
    engine = create_engine('sqlite:///:memory:')
    try:
        with engine.connect() as connection:
            result = connection.execute(select(type_coerce(literal(value), type_class()))).scalar_one()
            assert result == expected
    finally:
        engine.dispose()
