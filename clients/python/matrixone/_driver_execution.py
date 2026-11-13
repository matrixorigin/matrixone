# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Parameter-shape contracts for SQLAlchemy's ``exec_driver_sql`` boundary."""

import warnings

_EXECUTEMANY_MIGRATION = (
    'A tuple of parameter collections is now executed as a single positional '
    'collection. Pass a list to keep executemany semantics: %r -> %r'
)


def _looks_like_many(params):
    """Detect the shape that previously selected SQLAlchemy's executemany path."""
    return len(params) > 1 and all(isinstance(item, (tuple, list, dict)) for item in params)


def driver_parameters(params):
    """Preserve a positional tuple as one parameter collection.

    SQLAlchemy otherwise uses the first tuple item to distinguish execute from
    executemany. A collection-valued first binding is therefore mistaken for a
    sequence of parameter collections. Wrapping the outer tuple makes the
    execute contract explicit without inspecting or rewriting any bound value.

    Lists retain SQLAlchemy's executemany form and dictionaries retain named
    binding. Callers that need executemany should pass a list of parameter
    collections rather than an ambiguous tuple of tuples.

    Compatibility: a tuple of parameter collections used to reach
    ``executemany``. It now runs as one collection, so that shape emits a
    ``RuntimeWarning`` pointing at the list form instead of changing behaviour
    silently.
    """
    if isinstance(params, tuple):
        if _looks_like_many(params):
            warnings.warn(
                _EXECUTEMANY_MIGRATION % (params, list(params)),
                RuntimeWarning,
                stacklevel=2,
            )
        return [params]
    return params
