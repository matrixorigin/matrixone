"""Raw driver tuple parameters must not be mistaken for executemany."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, event

from matrixone._driver_params import single_driver_parameters


@pytest.mark.parametrize(
    "raw, expected",
    [
        (((1, 2), 3), [((1, 2), 3)]),
        ((1, 2), [(1, 2)]),
        ((), ()),
        (None, None),
        ({"p": 1}, {"p": 1}),
        ([(1, 2), (3, 4)], [(1, 2), (3, 4)]),
    ],
)
def test_single_driver_parameter_shape(raw, expected):
    assert single_driver_parameters(raw) == expected


class _CapturedExecution(Exception):
    pass


@pytest.mark.parametrize(
    "raw, expected_params, expected_many",
    [
        (((1, 2), 3), ((1, 2), 3), False),
        ((11, 22), (11, 22), False),
        ([(1, 2), (3, 4)], [(1, 2), (3, 4)], True),
    ],
)
def test_sqlalchemy_exec_driver_sql_preserves_single_execution(
    raw, expected_params, expected_many
):
    # Inspect the DBAPI boundary without requiring a MatrixOne server or
    # driver support for tuple-valued SQL placeholders.
    engine = create_engine("sqlite://")
    captured = []

    @event.listens_for(engine, "before_cursor_execute")
    def trap(conn, cursor, statement, parameters, context, executemany):
        captured.append((parameters, executemany))
        raise _CapturedExecution()

    with engine.connect() as conn:
        with pytest.raises(_CapturedExecution):
            conn.exec_driver_sql(
                "SELECT ? AS nested, ? AS scalar", single_driver_parameters(raw)
            )
    assert captured == [(expected_params, expected_many)]
