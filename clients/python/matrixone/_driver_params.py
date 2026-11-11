"""Preserve single positional DBAPI parameter sets across SQLAlchemy releases."""


def single_driver_parameters(params):
    """Normalize a tuple of values to one DBAPI parameter set.

    SQLAlchemy 1.4 may mistake a tuple whose first *value* is itself a
    tuple for executemany parameters. A one-element list forces a single
    execution without changing the bound values. Actual executemany callers
    already use a list of dictionaries/tuples and remain unchanged.
    """
    if isinstance(params, tuple) and params:
        return [params]
    return params
