# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Pure compilation shared by buffered sync/async queries and exports."""

import copy
import re
from typing import Any, cast

from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.compiler import SQLCompiler

from ._sql_literals import substitute_parameters
from ._telemetry import phase


def buffer_result(result_type, result):
    """Retain the execution's type context before fetch closes its cursor."""
    context = getattr(result, 'context', None)
    cursor = getattr(result, 'cursor', None)
    description = getattr(cursor, 'description', None)
    compiled = getattr(context, 'compiled', None)
    columns = list(result.keys())
    with phase('fetch'):
        buffered = result_type(columns, result.fetchall())
    buffered._matrixone_dialect = getattr(context, 'dialect', None)
    buffered._matrixone_description = description
    buffered._matrixone_typed = bool(getattr(compiled, '_result_columns', ()))
    return buffered


def compile_from(source):
    """Let the dialect render qualified tables, joins and selectable aliases."""
    from sqlalchemy import inspect
    from .sqlalchemy_ext.dialect import MatrixOneDialect

    inspected = inspect(source, raiseerr=False)
    if inspected is not None and getattr(inspected, 'is_aliased_class', False):
        source = inspected.selectable
    else:
        source = getattr(source, '__table__', source)
    dialect = MatrixOneDialect(paramstyle='qmark')
    return dialect.statement_compiler(dialect, None).process(source, asfrom=True)


def render_fragment(sql, params=()):
    """Consume only this fragment's values, respecting SQL lexical boundaries."""
    if '\\' in sql:
        raise ValueError('Raw builder SQL containing backslashes is SQL-mode ambiguous; bind values instead')
    return substitute_parameters(sql, params)


def compile_expression(expression, model=None, alias=None, projection=False, compiler_dialect=None):
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
    from .sqlalchemy_ext.dialect import MatrixOneDialect

    if compiler_dialect is None:
        compiler_dialect = MatrixOneDialect(paramstyle='qmark')
    else:
        # A string-producing API owns literal substitution, not DBAPI percent
        # expansion. Preserve the supplied dialect's types without mutating it.
        compiler_dialect = copy.copy(compiler_dialect)
        compiler_dialect.paramstyle = 'qmark'
        compiler_dialect.positional = True
        compiler_dialect.identifier_preparer = compiler_dialect.preparer(compiler_dialect)
    compiled = cast(
        SQLCompiler,
        ClauseElement.compile(
            expression,
            dialect=compiler_dialect,
            compile_kwargs={'render_postcompile': True, 'within_columns_clause': projection},
        ),
    )
    # SQLAlchemy owns expression syntax; our shared renderer owns value quoting.
    expanded = getattr(compiled, '_post_compile_expanded_state', None)
    parameters = expanded.parameters if expanded is not None else compiled.params
    processors = dict(compiled._bind_processors)
    if expanded is not None:
        processors.update(expanded.processors)
    values = []
    for key in compiled.positiontup or ():
        parameter_key = key if expanded is not None else compiled.escaped_bind_names.get(key, key)
        value = parameters[parameter_key]
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
