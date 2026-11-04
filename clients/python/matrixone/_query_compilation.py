# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Pure compilation shared by buffered sync/async queries and exports."""

import copy
import re
import uuid
from contextvars import ContextVar
from contextlib import contextmanager
from functools import wraps, lru_cache
from typing import cast

from sqlalchemy.sql import visitors
from sqlalchemy import inspect, select
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.elements import BindParameter, ClauseElement, ColumnElement
from sqlalchemy.sql.selectable import FromClause, SelectBase
from sqlalchemy.sql.compiler import SQLCompiler

from ._sql_literals import substitute_parameters
from ._telemetry import phase

_compilation = ContextVar('matrixone_statement_compilation', default=None)


def _same_value(left, right):
    if left is right:
        return True
    try:
        equal = left == right
        return equal if isinstance(equal, bool) else False
    except (TypeError, ValueError):
        return False


class _StatementBindings:
    def __init__(self):
        self.active = True
        self.values, self.definitions, self.ctes = {}, {}, {}

    def wrap(self, bind):
        if bind.callable is None or hasattr(bind, '_matrixone_original_bind'):
            return bind
        original = bind.callable
        key = bind.key, id(original)

        def value():
            if key not in self.values:
                self.values[key] = original()
            return self.values[key]

        clone = bind._clone(maintain_key=True)
        clone.callable = value
        clone._matrixone_original_bind = bind
        return clone

    def register(self, definitions):
        for bind, value in definitions:
            original = getattr(bind, '_matrixone_original_bind', bind)
            previous = self.definitions.get(original.key)
            if previous is not None:
                other, old_value = previous
                same_type = original.type is other.type
                if not same_type:
                    left, right = original.type._static_cache_key, other.type._static_cache_key
                    same_type = isinstance(left, tuple) and isinstance(right, tuple) and _same_value(left, right)
                if not (
                    same_type
                    and original.callable is other.callable
                    and original.required == other.required
                    and original.expanding == other.expanding
                    and original.literal_execute == other.literal_execute
                    and _same_value(value, old_value)
                ):
                    raise InvalidRequestError(
                        'Conflicting definitions for bind %r across query fragments; use unique names, '
                        'reuse one bind, or execute a full SQLAlchemy statement' % original.key
                    )
            self.definitions[original.key] = original, value


def statement_compilation(function):
    """One lexical bind/CTE owner; nested builders share it, later builds do not."""

    @wraps(function)
    def run(*args, **kwargs):
        current = _compilation.get()
        if current is not None and current.active:
            return function(*args, **kwargs)
        current = _StatementBindings()
        token = _compilation.set(current)
        try:
            return function(*args, **kwargs)
        finally:
            current.active = False
            current.values.clear()
            current.definitions.clear()
            current.ctes.clear()
            _compilation.reset(token)

    return run


def compile_cte(cte):
    if isinstance(cte.query, str):
        return cte.query, []
    if not hasattr(cte.query, '_build_sql'):
        raise ValueError('CTE query must be a BaseMatrixOneQuery object or SQL string')
    scope = _compilation.get()
    if cte not in scope.ctes:
        scope.ctes[cte] = cte.query._build_sql()
    return scope.ctes[cte]


@contextmanager
def _extension_context():
    bindings = _compilation.get()
    token = _compilation.set(None)
    try:
        yield bindings
    finally:
        _compilation.reset(token)


def normalize_expression(expression):
    if not isinstance(expression, ClauseElement) and hasattr(expression, '__clause_element__'):
        with _extension_context():
            return expression.__clause_element__()
    return expression


def _compiler_boundary(function):
    """Keep compiler extension callbacks outside the enclosing builder scope."""

    @wraps(function)
    def run(*args, **kwargs):
        with _extension_context() as bindings:
            return function(*args, _bindings=bindings, **kwargs)

    return run


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


def selectable_of(source):
    """Preserve aliases and derived sources instead of recovering their table."""
    inspected = inspect(source, raiseerr=False)
    if inspected is not None and hasattr(inspected, 'selectable'):
        return inspected.selectable
    return source if isinstance(source, FromClause) else None


def compile_from(source):
    """FROM binds use the same processors and literal owner as expressions."""
    selectable = selectable_of(source)
    return compile_expression(source if selectable is None else selectable, asfrom=True)


def adapt_expression(expression, source, target):
    if source is None or target is None or source is target:
        return expression
    original = source._deannotate()

    def replace(element):
        if isinstance(element, BindParameter):
            # Our own column adaptation must not regenerate unique bind keys.
            return element
        # Explicit correlate/correlate_except also contain annotated FROMs.
        if isinstance(element, FromClause) and element._deannotate() is original:
            return target
        table = getattr(element, 'table', None)
        if table is not None and table._deannotate() is original:
            return target.corresponding_column(element)
        return None

    return visitors.replacement_traverse(expression, {}, replace)


class _ExpressionCapture(ColumnElement):
    """Compile one fragment while SQLAlchemy owns its enclosing SELECT stack."""

    inherit_cache = False

    def __init__(self, expression, projection):
        self.fragment, self.projection = expression, projection
        marker = 'matrixone_expression_' + uuid.uuid4().hex
        self.start, self.end = '/*' + marker + '_start*/', '/*' + marker + '_end*/'


@compiles(_ExpressionCapture)
def _capture_expression(element, compiler, **kwargs):
    compiler._matrixone_capture_active = True
    try:
        # Do not inherit the wrapper label's within_label_clause flag.
        sql = compiler.process(element.fragment, within_columns_clause=element.projection, render_postcompile=True)
        return element.start + sql + element.end
    finally:
        compiler._matrixone_capture_active = False


def render_fragment(sql, params=()):
    """Consume only this fragment's values, respecting SQL lexical boundaries."""
    if '\\' in sql:
        raise ValueError('Raw builder SQL containing backslashes is SQL-mode ambiguous; bind values instead')
    return substitute_parameters(sql, params)


class _ExpressionCompilerMixin:
    def __init__(self, *args, _matrixone_bindings=None, _matrixone_capture_active=True, **kwargs):
        self._matrixone_bindings = _matrixone_bindings
        self._matrixone_capture_active = _matrixone_capture_active
        self._matrixone_definitions = None
        self._matrixone_expanded_state = None
        super().__init__(*args, **kwargs)

    def _process_parameters_for_postcompile(self, *args, **kwargs):
        state = super()._process_parameters_for_postcompile(*args, **kwargs)
        # 1.4's debug render_postcompile mutates bind_names incorrectly
        # for tuple IN; retain the actual execution expansion result.
        self._matrixone_expanded_state = state
        return state

    def visit_bindparam(self, bindparam, **kwargs):
        if not self._matrixone_capture_active:
            # Synthetic FROMs establish scope, not executable parameters.
            return 'NULL'
        if self._matrixone_bindings is not None and self._matrixone_bindings.active:
            bindparam = self._matrixone_bindings.wrap(bindparam)
        return super().visit_bindparam(bindparam, **kwargs)

    def construct_params(self, *args, **kwargs):
        # SQLAlchemy also owns statement-level params() (2.1+) and
        # callable/default values. Validate without evaluating them twice.
        kwargs['_check'] = True
        parameters = super().construct_params(*args, **kwargs)
        if self._matrixone_definitions is None:
            # Capture before 1.4 rewrites expanding binds; this is the
            # effective parameter map, including 2.1 statement overrides.
            self._matrixone_definitions = [
                (
                    self.binds.get(name, bind),
                    parameters.get(self.escaped_bind_names.get(name, name), parameters.get(name)),
                )
                for bind, name in self.bind_names.items()
            ]
        return parameters

    def visit_select(self, select_stmt, **kwargs):
        if not self._matrixone_capture_active and getattr(select_stmt, '_params', None):
            # Synthetic FROMs must not publish their statement-level
            # parameter overrides into the captured expression either.
            select_stmt = select_stmt._generate()
            select_stmt._params = None
        return super().visit_select(select_stmt, **kwargs)


@lru_cache(maxsize=16)
def _expression_compiler(base):
    # Cache immutable backend adapters only, never a query, engine or bindings.
    return type('MatrixOneExpression_' + base.__name__, (_ExpressionCompilerMixin, base), {})


@_compiler_boundary
def compile_expression(
    expression,
    model=None,
    alias=None,
    projection=False,
    compiler_dialect=None,
    *,
    outer_froms=(),
    source=None,
    target=None,
    asfrom=False,
    _bindings=None,
):
    expression = normalize_expression(expression)
    if not isinstance(expression, ClauseElement):
        return str(expression.compile(compile_kwargs={'literal_binds': True}))
    if source is None:
        source = selectable_of(model)
    if target is None and alias and source is not None:
        target = source.alias(alias)
    expression = adapt_expression(expression, source, target)
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
    contextual = outer_froms and any(isinstance(node, SelectBase) for node in visitors.iterate(expression))
    capture = _ExpressionCapture(expression, projection) if contextual else None

    compiler_dialect = copy.copy(compiler_dialect)
    compiler_dialect.statement_compiler = _expression_compiler(compiler_dialect.statement_compiler)
    statement = select(capture).select_from(*outer_froms) if capture is not None else expression
    compiled = cast(
        SQLCompiler,
        ClauseElement.compile(
            statement,
            dialect=compiler_dialect,
            compile_kwargs={'render_postcompile': True, 'within_columns_clause': projection, 'asfrom': asfrom},
            _matrixone_bindings=_bindings,
            _matrixone_capture_active=capture is None,
        ),
    )
    # SQLAlchemy owns expression syntax; our shared renderer owns value quoting.
    expanded = compiled._matrixone_expanded_state
    parameters = getattr(expanded, 'parameters', getattr(expanded, 'additional_parameters', None))
    if parameters is None:
        parameters = compiled.params
    if _bindings is not None and _bindings.active:
        _bindings.register(compiled._matrixone_definitions or ())
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
    sql = substitute_parameters(str(compiled), values)
    if capture is not None:
        if sql.count(capture.start) != 1 or sql.count(capture.end) != 1:
            raise ValueError('Ambiguous contextual SQL expression capture')
        sql = sql.split(capture.start, 1)[1].split(capture.end, 1)[0]
    return sql


def compile_predicate(condition, params=(), model=None, alias=None, **context):
    if hasattr(condition, 'compile'):
        if params:
            raise ValueError('SQLAlchemy expressions carry their own parameters')
        if hasattr(condition, 'compile_for_query'):
            return condition.compile_for_query(model, alias)
        return compile_expression(condition, model, alias, **context)
    return render_fragment(str(condition), params)


def conjoin_conditions(conditions):
    """Preserve each disjunction when chained predicates are combined with AND."""
    if len(conditions) > 1:
        conditions = ['(' + c + ')' if re.search(r'\bOR\b', c, re.IGNORECASE) else c for c in conditions]
    return ' AND '.join(conditions)
