"""Offline contract for ``driver_parameters`` across SQLAlchemy versions.

SQLAlchemy decides between ``execute`` and ``executemany`` from the *shape* of
the parameter payload, which makes a tuple of collections ambiguous. The SDK
resolves that ambiguity explicitly: a tuple is always one parameter collection,
a list is always a sequence of collections. These assertions pin that contract
so a SQLAlchemy upgrade cannot silently change execution mode.
"""

from __future__ import annotations

import warnings

import pytest

from matrixone._driver_execution import driver_parameters


@pytest.mark.parametrize(
    "params, expected",
    [
        # A tuple is one positional collection, including collection-valued bindings.
        ((1,), [(1,)]),
        ((1, "a"), [(1, "a")]),
        ((None, 2), [(None, 2)]),
        (([1, 2, 3],), [([1, 2, 3],)]),
        (((1, 2), (3, 4)), [((1, 2), (3, 4))]),
        (((1,), (2,)), [((1,), (2,))]),
        ((), [()]),
        # A list keeps SQLAlchemy's executemany form unchanged.
        ([], []),
        ([(1,), (2,)], [(1,), (2,)]),
        ([[1], [2]], [[1], [2]]),
        # Named binding is passed through untouched.
        ({"a": 1}, {"a": 1}),
        ({}, {}),
        # No parameters means no wrapping.
        (None, None),
    ],
)
def test_driver_parameters_keeps_one_collection_per_tuple(params, expected) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        assert driver_parameters(params) == expected


def test_driver_parameters_preserves_identity_for_passthrough_shapes() -> None:
    """Lists and dicts must reach the driver unwrapped, without copying."""
    many = [(1,), (2,)]
    named = {"a": 1}
    assert driver_parameters(many) is many
    assert driver_parameters(named) is named


def test_driver_parameters_does_not_inspect_element_types() -> None:
    """A collection-valued first binding is data, never a mode switch."""
    wrapped = driver_parameters(([1, 2],))
    assert wrapped == [([1, 2],)]
    assert isinstance(wrapped, list) and len(wrapped) == 1


@pytest.mark.parametrize(
    "params",
    [
        ((1,), (2,)),  # tuple of tuples used to reach executemany
        ([1], [2]),  # tuple of lists used to reach executemany
        ({"a": 1}, {"a": 2}),  # tuple of mappings used to reach executemany
    ],
)
def test_former_executemany_shapes_warn_about_the_list_form(params) -> None:
    """The silent switch from executemany to execute must be announced."""
    with pytest.warns(RuntimeWarning, match="executemany"):
        driver_parameters(params)


@pytest.mark.parametrize(
    "params",
    [
        (1, "a"),  # scalar bindings: never ambiguous
        ([1, 2],),  # single collection with a list binding
        (1,),  # single scalar binding
        (),  # no bindings
        [(1,), (2,)],  # the supported executemany form
        {"a": 1},
        None,
    ],
)
def test_unambiguous_shapes_do_not_warn(params) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        driver_parameters(params)
