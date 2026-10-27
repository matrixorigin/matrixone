# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Pure compilation shared by buffered sync/async queries and exports."""

import re
from typing import Any, cast

from sqlalchemy.dialects.mysql import dialect
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.compiler import SQLCompiler

from ._sql_literals import substitute_parameters


def render_fragment(sql, params=()):
    """Consume only this fragment's values, respecting SQL lexical boundaries."""
    if '\\' in sql:
        raise ValueError('Raw builder SQL containing backslashes is SQL-mode ambiguous; bind values instead')
    return substitute_parameters(sql, params)


def compile_expression(expression, model=None, alias=None, projection=False):
    if not isinstance(expression, ClauseElement) and hasattr(expression, '__clause_element__'):
        expression = expression.__clause_element__()
    if not isinstance(expression, ClauseElement):
        return str(expression.compile(compile_kwargs={'literal_binds': True}))
    table = getattr(model, '__table__', None)
    if alias and table is not None:
        target = table.alias(alias)

        def replace(element, **kw):
            if getattr(element, 'table', None) is table:
                return target.c.get(element.key)
            return None

        expression = visitors.replacement_traverse(cast(Any, expression), {}, replace)
    compiled = cast(
        SQLCompiler,
        ClauseElement.compile(
            expression,
            dialect=dialect(paramstyle='qmark'),
            compile_kwargs={'render_postcompile': True, 'within_columns_clause': projection},
        ),
    )
    # SQLAlchemy owns expression syntax; our shared renderer owns value quoting.
    parameters = compiled.params
    processors = dict(compiled._bind_processors)
    expanded = getattr(compiled, '_post_compile_expanded_state', None)
    if expanded is not None:
        processors.update(expanded.processors)
    values = []
    for key in compiled.positiontup or ():
        value = parameters[key]
        processor = processors.get(key)
        if processor is not None:
            if not callable(processor):
                raise ValueError('Unexpanded composite SQL parameter')
            value = processor(value)
        values.append(value)
    return substitute_parameters(str(compiled), values)


def compile_predicate(condition, params=(), model=None, alias=None):
    if hasattr(condition, 'compile'):
        if params:
            raise ValueError('SQLAlchemy expressions carry their own parameters')
        if hasattr(condition, 'compile_for_query'):
            return condition.compile_for_query(model, alias)
        return compile_expression(condition, model, alias)
    return render_fragment(str(condition), params)


def conjoin_conditions(conditions):
    """Preserve each disjunction when chained predicates are combined with AND."""
    if len(conditions) > 1:
        conditions = ['(' + c + ')' if re.search(r'\bOR\b', c, re.IGNORECASE) else c for c in conditions]
    return ' AND '.join(conditions)
