# Copyright 2026 Matrix Origin
# Licensed under the Apache License, Version 2.0.
"""Optional bounded query measurements at existing execution boundaries."""

import inspect
import threading
import time
import uuid
from contextlib import nullcontext
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
from typing import Optional

from sqlalchemy import event
from sqlalchemy.engine import Engine

_current = ContextVar('matrixone_query_measurement', default=None)
_observing = ContextVar('matrixone_query_observer_active', default=False)
_registration = threading.Lock()
_unmeasured = nullcontext()


@dataclass(frozen=True)
class QueryEvent:
    operation: str
    outcome: str
    correlation_id: str
    total_seconds: float
    pool_wait_seconds: Optional[float]
    execute_seconds: Optional[float]
    fetch_seconds: Optional[float]
    conversion_seconds: Optional[float]
    commit_seconds: Optional[float]
    rows: Optional[int]
    committed: Optional[bool]


def _client(owner):
    if hasattr(owner, '_telemetry_client'):
        return owner._telemetry_client
    manager = getattr(owner, '_manager', owner)
    return getattr(manager, 'client', manager)


def _attach(engine):
    engine = getattr(engine, 'sync_engine', engine)
    if not isinstance(engine, Engine):
        return
    with _registration:
        if getattr(engine, '_matrixone_telemetry_attached', False):
            return
        event.listen(engine, 'before_cursor_execute', _executing)
        event.listen(engine, 'after_cursor_execute', _executed)
        event.listen(engine, 'handle_error', _failed)
        event.listen(engine, 'checkout', _checked_out)
        engine._matrixone_telemetry_attached = True


def _executing(connection, cursor, statement, parameters, context, many):
    measurement = _current.get()
    if measurement is not None:
        measurement.begin('execute')


def _executed(connection, cursor, statement, parameters, context, many):
    measurement = _current.get()
    if measurement is not None:
        measurement.end('execute')


def _failed(context):
    measurement = _current.get()
    if measurement is not None:
        measurement.end('execute')


def _checked_out(driver, record, proxy):
    measurement = _current.get()
    if measurement is not None:
        measurement.end('pool_wait')


def pool_wait():
    """Call immediately before the existing owner requests its connection."""
    measurement = _current.get()
    if measurement is not None:
        measurement.begin('pool_wait')


class _Phase:
    def __init__(self, measurement, name):
        self.measurement, self.name = measurement, name

    def __enter__(self):
        self.measurement.begin(self.name)

    def __exit__(self, *args):
        self.measurement.end(self.name)


def phase(name):
    measurement = _current.get()
    return _unmeasured if measurement is None else _Phase(measurement, name)


class _Measurement:
    def __init__(self, client, observer, operation):
        self.client, self.observer, self.operation = client, observer, operation
        self.started = time.perf_counter()
        self.correlation_id = uuid.uuid4().hex
        self.active, self.durations = {}, {}
        self._lock = threading.RLock()
        self._finished = False

    def begin(self, name):
        with self._lock:
            if not self._finished:
                self.active[name] = time.perf_counter()

    def end(self, name):
        with self._lock:
            if self._finished:
                return
            started = self.active.pop(name, None)
            if started is not None:
                self.durations[name] = self.durations.get(name, 0) + time.perf_counter() - started

    def finish(self, result, error):
        with self._lock:
            for name in tuple(self.active):
                self.end(name)
            self._finished = True
            values = dict(self.durations)
        outcome = 'success' if error is None else 'error' if isinstance(error, Exception) else 'cancelled'
        if isinstance(error, (StopIteration, StopAsyncIteration)):
            outcome = 'success'
        rows = getattr(result, 'affected_rows', None)
        if rows == 0 and getattr(result, 'rows', None) is not None:
            rows = len(result.rows)
        if rows is None:
            rows = getattr(result, 'num_rows', None)
        if rows is None and getattr(result, 'returns_rows', None) is False:
            rows = result.rowcount
        if rows is None and isinstance(result, int) and not isinstance(result, bool):
            rows = result
        if rows is None and error is not None:
            rows = getattr(error, 'rows', None)
        if isinstance(rows, int) and rows < 0:
            rows = None
        committed = getattr(error, 'committed', None)
        if error is None and self.operation in ('arrow_upload', 'file_import'):
            committed = True
        emitted = QueryEvent(
            self.operation,
            outcome,
            self.correlation_id,
            time.perf_counter() - self.started,
            values.get('pool_wait'),
            values.get('execute'),
            values.get('fetch'),
            values.get('conversion'),
            values.get('commit'),
            rows,
            committed,
        )
        token = _observing.set(True)
        try:
            returned = self.observer(emitted)
            if inspect.iscoroutine(returned):
                returned.close()
        except BaseException:
            pass  # Observation cannot change database outcome or error precedence.
        finally:
            _observing.reset(token)


def observe(operation):
    """Reuse the owning execution method; disabled calls allocate no event/ID."""

    def decorate(function):
        def prepare(owner):
            client = owner if operation == 'query' else _client(owner)
            observer = getattr(client, '_query_observer', None)
            if observer is None:
                return None
            parent = _current.get()
            if _observing.get() or (parent is not None and parent.client is client):
                return None
            _attach(getattr(client, '_engine', None))
            return _Measurement(client, observer, operation)

        if inspect.iscoroutinefunction(function):

            @wraps(function)
            async def run(owner, *args, **kwargs):
                measurement = prepare(owner)
                if measurement is None:
                    return await function(owner, *args, **kwargs)
                token = _current.set(measurement)
                result = error = None
                try:
                    result = await function(owner, *args, **kwargs)
                    return result
                except BaseException as failure:
                    error = failure
                    raise
                finally:
                    _current.reset(token)
                    try:
                        measurement.finish(result, error)
                    except BaseException:
                        pass

        else:

            @wraps(function)
            def run(owner, *args, **kwargs):
                measurement = prepare(owner)
                if measurement is None:
                    return function(owner, *args, **kwargs)
                token = _current.set(measurement)
                result = error = None
                try:
                    result = function(owner, *args, **kwargs)
                    return result
                except BaseException as failure:
                    error = failure
                    raise
                finally:
                    _current.reset(token)
                    try:
                        measurement.finish(result, error)
                    except BaseException:
                        pass

        return run

    return decorate
