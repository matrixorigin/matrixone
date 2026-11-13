# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.

"""Parameter-shape contracts for SQLAlchemy's ``exec_driver_sql`` boundary."""


def driver_parameters(params):
    """Preserve a positional tuple as one parameter collection.

    SQLAlchemy otherwise uses the first tuple item to distinguish execute from
    executemany. A collection-valued first binding is therefore mistaken for a
    sequence of parameter collections. Wrapping the outer tuple makes the
    execute contract explicit without inspecting or rewriting any bound value.

    Lists retain SQLAlchemy's executemany form and dictionaries retain named
    binding. Callers that need executemany should pass a list of parameter
    collections rather than an ambiguous tuple of tuples.
    """
    if isinstance(params, tuple):
        return [params]
    return params
