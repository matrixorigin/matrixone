# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Arrow Flight worker for the MatrixOne Python routine contract.

The process is deliberately a small protocol endpoint.  It does not install
packages, read SQL data, or interpret user supplied paths.  Production
launchers must place it in the sandbox class selected by the routine policy.
"""

from __future__ import annotations

import argparse
import datetime as _datetime
import decimal as _decimal
import hashlib
import importlib
import inspect
import json
import logging
import threading
import time
import uuid as _uuid
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

import pyarrow as pa
import pyarrow.flight as flight

PROTOCOL_VERSION = 1
MAX_CONTROL_BYTES = 1 << 20
MAX_TERMINAL_RECORDS = 10000
MAX_TERMINAL_BYTES = 16 << 20
TERMINAL_TTL_SECONDS = 300.0
ACK_TIMEOUT_SECONDS = 60.0
DEFAULT_MAX_BATCH_ROWS = 65536

BOOL = 10
INT8, INT16, INT32, INT64 = 20, 21, 22, 23
UINT8, UINT16, UINT32, UINT64 = 25, 26, 27, 28
FLOAT32, FLOAT64 = 30, 31
DECIMAL64, DECIMAL128 = 32, 33
DATE, TIME, DATETIME, TIMESTAMP = 50, 51, 52, 53
CHAR, VARCHAR, JSON, UUID = 60, 61, 62, 63
BINARY, VARBINARY = 64, 65
BLOB, TEXT = 70, 71
VECF32, VECF64 = 224, 225

MODE_SCALAR = "SCALAR"
MODE_VECTOR = "VECTOR"
NULL_CALL = "CALLED_ON_NULL_INPUT"
NULL_RETURN = "RETURNS_NULL_ON_NULL_INPUT"
ABI_CONTRACT = "PYTHON_ARROW"
ADAPTER_VERSION = "2026-09"
SDK_VERSION = "1.0"

log = logging.getLogger("matrixone.python.runtime")


@dataclass(frozen=True, slots=True)
class SqlDate:
    is_zero: bool
    value: Optional[_datetime.date]


@dataclass(frozen=True, slots=True)
class SqlDatetime:
    is_zero: bool
    value: Optional[_datetime.datetime]


@dataclass(frozen=True, slots=True)
class SqlTimestamp:
    is_zero: bool
    value: Optional[_datetime.datetime]


@dataclass(frozen=True, slots=True)
class TimezoneContext:
    kind: str
    name: Optional[str] = None
    offset_minutes: Optional[int] = None
    tzdb_version: Optional[str] = None


@dataclass(frozen=True, slots=True)
class StatementContext:
    statement_timestamp_utc: _datetime.datetime
    session_timezone: TimezoneContext
    sql_mode: tuple[str, ...]
    current_database: Optional[str]
    current_user: str
    current_role: Optional[str]
    connection_collation: str


class _CallLogger:
    __slots__ = ()

    def info(self, message: str, *args: Any) -> None:
        log.info("user: " + str(message), *args)

    def warning(self, message: str, *args: Any) -> None:
        log.warning("user: " + str(message), *args)

    def error(self, message: str, *args: Any) -> None:
        log.error("user: " + str(message), *args)


@dataclass(frozen=True, slots=True)
class ScalarContext:
    sdk_version: str
    logger: _CallLogger
    statement: Optional[StatementContext] = None


@dataclass(frozen=True, slots=True)
class VectorContext:
    sdk_version: str
    logger: _CallLogger
    statement: Optional[StatementContext] = None
    num_rows: int = 0


_FENCING_CONTEXT_KEYS = frozenset(
    {"account_id", "statement_id", "group_id", "group_epoch", "invocation_id", "lease_epoch"}
)
_STATEMENT_CONTEXT_KEYS = frozenset(
    {
        "statement_timestamp_utc",
        "session_timezone_kind",
        "session_timezone_name",
        "session_timezone_offset_minutes",
        "session_timezone_tzdb_version",
        "sql_mode",
        "current_database",
        "current_user",
        "current_role",
        "connection_collation",
    }
)


def _required_context_value(context: Dict[str, Any], key: str) -> str:
    value = context.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"PROTOCOL: missing statement context field {key}")
    return value


def _statement_context(raw: Any) -> Optional[StatementContext]:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError("PROTOCOL: statement context must be an object")
    unknown = set(raw) - _FENCING_CONTEXT_KEYS - _STATEMENT_CONTEXT_KEYS
    if unknown:
        raise ValueError("PROTOCOL: unsupported statement context field")
    if not (set(raw) & _STATEMENT_CONTEXT_KEYS):
        # The fencing fields are transport identity, not user-visible context.
        return None

    timestamp_text = _required_context_value(raw, "statement_timestamp_utc")
    try:
        timestamp_micros = int(timestamp_text, 10)
        seconds, micros = divmod(timestamp_micros, 1_000_000)
        timestamp = _datetime.datetime(1970, 1, 1, tzinfo=_datetime.timezone.utc) + _datetime.timedelta(seconds=seconds, microseconds=micros)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("PROTOCOL: invalid statement_timestamp_utc") from exc

    timezone_kind = _required_context_value(raw, "session_timezone_kind").upper()
    if timezone_kind == "IANA":
        timezone_name = _required_context_value(raw, "session_timezone_name")
        tzdb_version = _required_context_value(raw, "session_timezone_tzdb_version")
        if raw.get("session_timezone_offset_minutes"):
            raise ValueError("PROTOCOL: IANA timezone cannot carry a fixed offset")
        timezone = TimezoneContext("IANA", name=timezone_name, tzdb_version=tzdb_version)
    elif timezone_kind == "FIXED_OFFSET":
        offset_text = _required_context_value(raw, "session_timezone_offset_minutes")
        try:
            offset_minutes = int(offset_text, 10)
        except (TypeError, ValueError) as exc:
            raise ValueError("PROTOCOL: invalid fixed timezone offset") from exc
        if offset_minutes < -839 or offset_minutes > 840:
            raise ValueError("PROTOCOL: fixed timezone offset is outside SQL range")
        if raw.get("session_timezone_name") or raw.get("session_timezone_tzdb_version"):
            raise ValueError("PROTOCOL: fixed timezone cannot carry IANA identity")
        timezone = TimezoneContext("FIXED_OFFSET", offset_minutes=offset_minutes)
    else:
        raise ValueError("PROTOCOL: unsupported timezone kind")

    sql_mode_text = _required_context_value(raw, "sql_mode")
    try:
        sql_mode_value = json.loads(sql_mode_text)
    except (TypeError, ValueError) as exc:
        raise ValueError("PROTOCOL: sql_mode must be canonical JSON array") from exc
    if not isinstance(sql_mode_value, list) or any(not isinstance(item, str) for item in sql_mode_value):
        raise ValueError("PROTOCOL: sql_mode must be an array of strings")
    sql_mode = tuple(sorted(set(sql_mode_value)))
    if list(sql_mode) != sql_mode_value:
        raise ValueError("PROTOCOL: sql_mode is not canonical")

    current_user = _required_context_value(raw, "current_user")
    connection_collation = _required_context_value(raw, "connection_collation")
    return StatementContext(
        statement_timestamp_utc=timestamp,
        session_timezone=timezone,
        sql_mode=sql_mode,
        current_database=raw.get("current_database") or None,
        current_user=current_user,
        current_role=raw.get("current_role") or None,
        connection_collation=connection_collation,
    )


def _tuple_key(value: Dict[str, Any]) -> tuple:
    required = ("account_id", "statement_id", "group_id", "group_epoch", "invocation_id", "lease_epoch")
    if any(not value.get(k) for k in required):
        raise ValueError("PROTOCOL: incomplete fencing tuple")
    return tuple(value[k] for k in required)


def _decode_control(data: bytes) -> Dict[str, Any]:
    if not data or len(data) > MAX_CONTROL_BYTES:
        raise ValueError("PROTOCOL: invalid control size")
    value = json.loads(bytes(data).decode("utf-8"))
    if value.get("version") != PROTOCOL_VERSION or not value.get("kind"):
        raise ValueError("PROTOCOL: unsupported control")
    _tuple_key(value.get("tuple") or {})
    return value


def _encode_control(value: Dict[str, Any]) -> bytes:
    value = dict(value)
    value.setdefault("version", PROTOCOL_VERSION)
    _tuple_key(value["tuple"])
    data = json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    if len(data) > MAX_CONTROL_BYTES:
        raise ValueError("PROTOCOL: control is too large")
    return data


def _canonical_descriptor(descriptor: Dict[str, Any]) -> bytes:
    order = ("type_id", "width", "scale", "charset", "offset_width", "json_encoding", "temporal_encoding")
    result = {}
    for key in order:
        value = descriptor.get(key)
        if key == "type_id" or value not in (None, 0, ""):
            result[key] = value
    return json.dumps(result, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _physical_fingerprint(descriptor: Dict[str, Any]) -> str:
    type_id = int(descriptor["type_id"])
    primitive = {
        BOOL: "@B", INT8: "@D", INT16: "@F", INT32: "@H", INT64: "@J",
        UINT8: "@C", UINT16: "@E", UINT32: "@G", UINT64: "@I",
        FLOAT32: "@L", FLOAT64: "@M", BINARY: "@O", BLOB: "@O", VARBINARY: "@O",
        TIME: "@bu",
    }
    if type_id in primitive:
        return primitive[type_id]
    if type_id in (CHAR, VARCHAR, TEXT, JSON):
        return "@d" if int(descriptor.get("offset_width", 32)) == 64 else "@N"
    if type_id == UUID:
        return "@P"
    if type_id in (DECIMAL64, DECIMAL128):
        precision = int(descriptor.get("width") or (18 if type_id == DECIMAL64 else 38))
        return f"@X[128,{precision},{int(descriptor.get('scale') or 0)}]"
    if type_id == DATE:
        return "@[{FNis_zero{@B};FNvalue{@Q};}"
    if type_id == DATETIME:
        return "@[{FNis_zero{@B};FNvalue{@Su0:};}"
    if type_id == TIMESTAMP:
        return "@[{FNis_zero{@B};FNvalue{@Su3:UTC};}"
    if type_id == VECF32:
        return f"@a[{int(descriptor.get('width') or 0)}]{{@L}}"
    if type_id == VECF64:
        return f"@a[{int(descriptor.get('width') or 0)}]{{@M}}"
    raise ValueError(f"TYPE_CONTRACT: unsupported type id {type_id}")


def _fingerprint(descriptor: Dict[str, Any]) -> str:
    data = b"matrixone-python-udf-type\x00" + _canonical_descriptor(descriptor) + b"\x00" + _physical_fingerprint(descriptor).encode()
    return hashlib.sha256(data).hexdigest()


def _arrow_type(descriptor: Dict[str, Any]) -> pa.DataType:
    type_id = int(descriptor["type_id"])
    simple = {
        BOOL: pa.bool_, INT8: pa.int8, INT16: pa.int16, INT32: pa.int32, INT64: pa.int64,
        UINT8: pa.uint8, UINT16: pa.uint16, UINT32: pa.uint32, UINT64: pa.uint64,
        FLOAT32: pa.float32, FLOAT64: pa.float64,
    }
    if type_id in simple:
        return simple[type_id]()
    if type_id in (DECIMAL64, DECIMAL128):
        precision = int(descriptor.get("width") or (18 if type_id == DECIMAL64 else 38))
        return pa.decimal128(precision, int(descriptor.get("scale") or 0))
    if type_id in (CHAR, VARCHAR, TEXT, JSON):
        return pa.large_string() if int(descriptor.get("offset_width", 32)) == 64 else pa.string()
    if type_id in (BINARY, VARBINARY, BLOB):
        return pa.large_binary() if int(descriptor.get("offset_width", 32)) == 64 else pa.binary()
    if type_id == UUID:
        return pa.binary(16)
    if type_id == TIME:
        return pa.duration("us")
    if type_id in (VECF32, VECF64):
        width = int(descriptor.get("width") or 0)
        if width <= 0:
            raise ValueError("TYPE_CONTRACT: vector dimension must be positive")
        return pa.list_(pa.float32() if type_id == VECF32 else pa.float64(), width)
    if type_id == DATE:
        child = pa.date32()
    elif type_id == DATETIME:
        child = pa.timestamp("us")
    elif type_id == TIMESTAMP:
        child = pa.timestamp("us", tz="UTC")
    else:
        raise ValueError(f"TYPE_CONTRACT: unsupported type id {type_id}")
    return pa.struct([pa.field("is_zero", pa.bool_(), nullable=False), pa.field("value", child, nullable=False)])


def _field(name: str, descriptor: Dict[str, Any]) -> pa.Field:
    metadata = {
        TypeMetadataKey: _canonical_descriptor(descriptor),
        TypeFingerprintKey: _fingerprint(descriptor).encode("ascii"),
    }
    return pa.field(name, _arrow_type(descriptor), nullable=True, metadata=metadata)


TypeMetadataKey = b"mo.udf.type"
TypeFingerprintKey = b"mo.udf.type_fingerprint"


def _validate_field(field: pa.Field, name: str, descriptor: Dict[str, Any]) -> None:
    expected = _field(name, descriptor)
    if field.name != name:
        raise ValueError(f"TYPE_CONTRACT: Arrow field name {field.name!r} does not match {name!r}")
    if field.type != expected.type:
        raise ValueError(f"TYPE_CONTRACT: Arrow type {field.type} does not match {expected.type}")
    if field.metadata != expected.metadata:
        raise ValueError("TYPE_CONTRACT: Arrow logical metadata does not match the frozen descriptor")


def _validate_schema(schema: pa.Schema, descriptors: Iterable[Dict[str, Any]]) -> None:
    descriptors = list(descriptors)
    if len(schema) != len(descriptors):
        raise ValueError("TYPE_CONTRACT: input column count does not match the routine signature")
    for index, descriptor in enumerate(descriptors):
        _validate_field(schema.field(index), f"arg_{index}", descriptor)


def _load_handler(source: str, handler: str):
    namespace: Dict[str, Any] = {"__name__": "__matrixone_routine__"}
    exec(compile(source, "<routine>", "exec"), namespace, namespace)
    if ":" in handler:
        module_name, function_name = handler.split(":", 1)
        function = namespace.get(function_name)
        if function is None and module_name:
            function = getattr(importlib.import_module(module_name), function_name, None)
    else:
        function = namespace.get(handler)
    if not callable(function) or inspect.iscoroutinefunction(function):
        raise ValueError("USER_CODE: handler must be a synchronous callable")
    return function


def _scalar_input(array: pa.Array, row: int, descriptor: Dict[str, Any]):
    value = array[row].as_py()
    if value is None:
        return None
    type_id = int(descriptor["type_id"])
    if type_id in (VECF32, VECF64):
        # A scalar vector is exposed as read-only bytes.  Consumers can use
        # memoryview.cast("f"/"d") without receiving a mutable Arrow buffer.
        import struct
        values = value
        fmt = "f" if type_id == VECF32 else "d"
        return memoryview(struct.pack("<" + fmt * len(values), *values))
    if type_id in (DATE, DATETIME, TIMESTAMP):
        zero = bool(value["is_zero"])
        child = value["value"]
        if type_id == DATE:
            return SqlDate(zero, None if zero else child)
        if type_id == DATETIME:
            return SqlDatetime(zero, None if zero else child)
        return SqlTimestamp(zero, None if zero else child)
    if type_id == UUID:
        return _uuid.UUID(bytes=bytes(value))
    return value


def _check_json(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("TYPE_CONTRACT: JSON result must be canonical text")
    try:
        parsed = json.loads(value)
    except Exception as exc:
        raise ValueError("TYPE_CONTRACT: invalid JSON result") from exc
    canonical = json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))
    if canonical != value:
        raise ValueError("TYPE_CONTRACT: JSON result is not canonical text")
    return value


def _check_scalar(value: Any, descriptor: Dict[str, Any]):
    if value is None:
        return None
    type_id = int(descriptor["type_id"])
    if type_id == BOOL and type(value) is not bool: raise ValueError("TYPE_CONTRACT: expected bool")
    if type_id in (INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, UINT64) and (type(value) is not int): raise ValueError("TYPE_CONTRACT: expected integer")
    if type_id in (FLOAT32, FLOAT64) and type(value) is not float: raise ValueError("TYPE_CONTRACT: expected float")
    if type_id in (CHAR, VARCHAR, TEXT) and type(value) is not str: raise ValueError("TYPE_CONTRACT: expected string")
    if type_id == JSON: return _check_json(value)
    if type_id in (BINARY, VARBINARY, BLOB) and type(value) is not bytes: raise ValueError("TYPE_CONTRACT: expected bytes")
    if type_id == UUID:
        if not isinstance(value, _uuid.UUID): raise ValueError("TYPE_CONTRACT: expected uuid.UUID")
        return value.bytes
    if type_id == TIME and not isinstance(value, _datetime.timedelta): raise ValueError("TYPE_CONTRACT: expected datetime.timedelta")
    if type_id == DATE:
        if not isinstance(value, SqlDate) or (value.is_zero and value.value is not None) or (not value.is_zero and type(value.value) is not _datetime.date): raise ValueError("TYPE_CONTRACT: invalid SqlDate")
        return value
    if type_id == DATETIME:
        if not isinstance(value, SqlDatetime) or (value.is_zero and value.value is not None) or (not value.is_zero and (not isinstance(value.value, _datetime.datetime) or value.value.tzinfo is not None)): raise ValueError("TYPE_CONTRACT: invalid SqlDatetime")
        return value
    if type_id == TIMESTAMP:
        if not isinstance(value, SqlTimestamp) or (value.is_zero and value.value is not None) or (not value.is_zero and (not isinstance(value.value, _datetime.datetime) or value.value.tzinfo is None or value.value.utcoffset() != _datetime.timedelta(0))): raise ValueError("TYPE_CONTRACT: invalid SqlTimestamp")
        if not value.is_zero and value.value.tzinfo is not _datetime.timezone.utc:
            return SqlTimestamp(False, value.value.astimezone(_datetime.timezone.utc))
        return value
    if type_id in (VECF32, VECF64):
        if not isinstance(value, memoryview) or not value.readonly:
            raise ValueError("TYPE_CONTRACT: vector scalar result must be a read-only memoryview")
        width = int(descriptor.get("width") or 0)
        fmt = "f" if type_id == VECF32 else "d"
        try:
            values = list(value.cast(fmt))
        except (TypeError, ValueError) as exc:
            raise ValueError("TYPE_CONTRACT: invalid vector memoryview") from exc
        if len(values) != width:
            raise ValueError("TYPE_CONTRACT: vector dimension mismatch")
        return values
    if type_id in (DECIMAL64, DECIMAL128):
        if not isinstance(value, _decimal.Decimal): raise ValueError("TYPE_CONTRACT: expected decimal.Decimal")
        scale = int(descriptor.get("scale") or 0)
        if -value.as_tuple().exponent != scale: raise ValueError("TYPE_CONTRACT: decimal scale mismatch")
    return value


def _temporal_array(values: list, descriptor: Dict[str, Any]) -> pa.Array:
    type_id = int(descriptor["type_id"])
    valid = []
    zeroes = []
    child_values = []
    for value in values:
        if value is None:
            valid.append(False); zeroes.append(False); child_values.append(0); continue
        value = _check_scalar(value, descriptor)
        valid.append(True); zeroes.append(bool(value.is_zero)); child_values.append(0 if value.is_zero else value.value)
    if type_id == DATE: child_type = pa.date32()
    elif type_id == DATETIME: child_type = pa.timestamp("us")
    else: child_type = pa.timestamp("us", tz="UTC")
    children = [pa.array(zeroes, type=pa.bool_()), pa.array(child_values, type=child_type)]
    return pa.StructArray.from_arrays(children, names=["is_zero", "value"], mask=pa.array([not item for item in valid]))


def _output_array(values: Any, descriptor: Dict[str, Any], rows: int) -> pa.Array:
    if isinstance(values, pa.ChunkedArray):
        values = values.combine_chunks()
    if isinstance(values, pa.Array):
        if len(values) != rows: raise ValueError("TYPE_CONTRACT: result length does not match input rows")
        if values.type != _arrow_type(descriptor): raise ValueError("TYPE_CONTRACT: result Arrow type does not match the return descriptor")
        _validate_array_values(values, descriptor)
        return values
    if not isinstance(values, (list, tuple)):
        raise ValueError("TYPE_CONTRACT: VECTOR handler must return an Arrow Array or ChunkedArray")
    if len(values) != rows: raise ValueError("TYPE_CONTRACT: result length does not match input rows")
    type_id = int(descriptor["type_id"])
    if type_id in (DATE, DATETIME, TIMESTAMP): return _temporal_array(list(values), descriptor)
    if type_id in (VECF32, VECF64):
        width = int(descriptor.get("width") or 0)
        checked = []
        for value in values:
            if value is None:
                checked.append(None)
                continue
            checked.append(_check_scalar(value, descriptor))
        try:
            child_type = pa.float32() if type_id == VECF32 else pa.float64()
            flat = [item for row in checked if row is not None for item in row]
            child = pa.array(flat, type=child_type)
            mask = pa.array([row is None for row in checked])
            return pa.FixedSizeListArray.from_arrays(child, width, mask=mask)
        except (TypeError, ValueError, pa.ArrowException) as exc:
            raise ValueError("TYPE_CONTRACT: vector result is outside the declared SQL domain") from exc
    checked = [_check_scalar(value, descriptor) for value in values]
    if type_id == UUID: checked = [None if value is None else value.bytes for value in checked]
    try: return pa.array(checked, type=_arrow_type(descriptor))
    except (TypeError, ValueError, pa.ArrowException) as exc: raise ValueError("TYPE_CONTRACT: result value is outside the declared SQL domain") from exc


def _validate_array_values(array: pa.Array, descriptor: Dict[str, Any]) -> None:
    type_id = int(descriptor["type_id"])
    if type_id == JSON:
        for index in range(len(array)):
            if not array.is_null(index): _check_json(array[index].as_py())
    elif type_id in (DATE, DATETIME, TIMESTAMP):
        struct = array
        for index in range(len(struct)):
            if struct.is_null(index): continue
            is_zero = bool(struct.field("is_zero")[index].as_py())
            value = struct.field("value")[index].as_py()
            if value is None: raise ValueError("TYPE_CONTRACT: temporal child is null")
            if is_zero and ((type_id == DATE and value != _datetime.date(1970, 1, 1)) or (type_id == DATETIME and value != _datetime.datetime(1970, 1, 1)) or (type_id == TIMESTAMP and value != _datetime.datetime(1970, 1, 1, tzinfo=_datetime.timezone.utc))):
                raise ValueError("TYPE_CONTRACT: temporal zero placeholder is invalid")


class _InvocationState:
    def __init__(self, tuple_value: Dict[str, Any], terminal_bytes: int):
        self.tuple = tuple_value
        self.terminal_bytes = terminal_bytes
        self.condition = threading.Condition()
        self.last_result = 0
        self.acked_result = 0
        self.finish_id: Optional[str] = None
        self.finish_acked = False

    def ack_result(self, sequence: int) -> None:
        with self.condition:
            if sequence < self.acked_result or sequence > self.last_result:
                raise ValueError("PROTOCOL: result ACK is outside the received range")
            self.acked_result = sequence
            self.condition.notify_all()

    def wait_result_ack(self, sequence: int) -> None:
        deadline = time.monotonic() + ACK_TIMEOUT_SECONDS
        with self.condition:
            while self.acked_result < sequence:
                remaining = deadline - time.monotonic()
                if remaining <= 0: raise TimeoutError("DEADLINE_EXCEEDED: result ACK timeout")
                self.condition.wait(remaining)

    def ack_finish(self, finish_id: str) -> None:
        with self.condition:
            if not self.finish_id or finish_id != self.finish_id: raise ValueError("PROTOCOL: invalid finish id")
            self.finish_acked = True
            self.condition.notify_all()

    def wait_finish_ack(self) -> None:
        deadline = time.monotonic() + ACK_TIMEOUT_SECONDS
        with self.condition:
            while not self.finish_acked:
                remaining = deadline - time.monotonic()
                if remaining <= 0: raise TimeoutError("DEADLINE_EXCEEDED: Finish ACK timeout")
                self.condition.wait(remaining)


class RoutineFlightServer(flight.FlightServerBase):
    def __init__(self, location: str):
        super().__init__(location)
        self._lock = threading.RLock()
        self._active: Dict[tuple, _InvocationState] = {}
        self._terminal: OrderedDict[tuple, tuple[float, int]] = OrderedDict()
        self._terminal_bytes = 0
        self._active_bytes = 0

    @staticmethod
    def _entry_bytes(key: tuple) -> int:
        # The admission estimate is deliberately conservative.  The tuple is
        # already bounded by MAX_CONTROL_BYTES, and its serialized size is
        # stable for the lifetime of the fence.
        encoded = json.dumps(key, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        return len(encoded) + 128

    def _purge_terminal_locked(self, now: float) -> None:
        expired = [key for key, (deadline, _) in self._terminal.items() if deadline <= now]
        for key in expired:
            _, size = self._terminal.pop(key)
            self._terminal_bytes -= size

    def _admit(self, key: tuple) -> _InvocationState:
        size = self._entry_bytes(key)
        with self._lock:
            self._purge_terminal_locked(time.monotonic())
            if key in self._terminal:
                raise ValueError("PROTOCOL: invocation fence is terminal")
            if len(self._active) + len(self._terminal) >= MAX_TERMINAL_RECORDS:
                raise ValueError("RESOURCE_EXHAUSTED: terminal ledger entries are full")
            if self._active_bytes + self._terminal_bytes + size > MAX_TERMINAL_BYTES:
                raise ValueError("RESOURCE_EXHAUSTED: terminal ledger bytes are full")
            state = _InvocationState({}, size)
            self._active[key] = state
            self._active_bytes += size
            return state

    def _remember_terminal_locked(self, key: tuple, state: _InvocationState) -> None:
        deadline = time.monotonic() + TERMINAL_TTL_SECONDS
        self._terminal[key] = (deadline, state.terminal_bytes)
        self._terminal.move_to_end(key)
        self._terminal_bytes += state.terminal_bytes

    def do_action(self, context, action):
        control = _decode_control(action.body)
        key = _tuple_key(control["tuple"])
        with self._lock:
            self._purge_terminal_locked(time.monotonic())
            state = self._active.get(key)
            terminal = key in self._terminal
        if state is None:
            if terminal:
                yield _encode_control({"kind": "Ack", "tuple": control["tuple"], "status": "OK"})
                return
            raise ValueError("PROTOCOL: unknown invocation")
        if action.type == "AcknowledgeResults":
            state.ack_result(int(control.get("ack_sequence", 0)))
        elif action.type == "AcknowledgeFinish":
            state.ack_finish(str(control.get("finish_id", "")))
        else:
            raise ValueError("PROTOCOL: unknown action")
        yield _encode_control({"kind": "Ack", "tuple": control["tuple"], "status": "OK", "ack_sequence": control.get("ack_sequence", 0)})

    def do_exchange(self, context, descriptor, reader, writer):
        state = None
        key = None
        writer_started = False
        try:
            command = getattr(descriptor, "command", None)
            if command is None:
                raise ValueError("PROTOCOL: exchange is missing the invocation descriptor")
            open_control = _decode_control(command)
            if open_control["kind"] != "OpenInvocation": raise ValueError("PROTOCOL: first message must open an invocation")
            key = _tuple_key(open_control["tuple"])
            payload = open_control.get("payload") or {}
            if isinstance(payload, str):
                payload = json.loads(payload)
            args = list(payload.get("args", [])); result_descriptor = payload["return"]
            statement_context = _statement_context(payload.get("context"))
            mode = payload.get("mode", MODE_SCALAR); null_policy = payload.get("null_policy", NULL_CALL)
            abi_contract = payload.get("abi_contract", ABI_CONTRACT)
            adapter_version = payload.get("adapter_version", ADAPTER_VERSION)
            sdk_version = payload.get("sdk_version", SDK_VERSION)
            max_batch_rows = int(payload.get("max_batch_rows", DEFAULT_MAX_BATCH_ROWS))
            if mode not in (MODE_SCALAR, MODE_VECTOR) or null_policy not in (NULL_CALL, NULL_RETURN): raise ValueError("PROTOCOL: unsupported call mode or NULL policy")
            if abi_contract != ABI_CONTRACT or adapter_version != ADAPTER_VERSION: raise ValueError("PROTOCOL: unsupported Python ABI contract")
            if max_batch_rows <= 0: raise ValueError("PROTOCOL: invalid max batch rows")
            state = self._admit(key)
            state.tuple = open_control["tuple"]
            handler = _load_handler(payload["source"], payload["handler"])
            schema = None
            result_field = _field("result", result_descriptor)
            result_schema = pa.schema([result_field])
            writer.begin(result_schema)
            writer_started = True
            writer.write_metadata(_encode_control({"kind": "ResultSchema", "tuple": open_control["tuple"]}))
            ended = False
            while True:
                try:
                    chunk = reader.read_chunk()
                except StopIteration:
                    break
                if chunk.data is not None:
                    if schema is None:
                        schema = reader.schema
                        _validate_schema(schema, args)
                    control = _decode_control(chunk.app_metadata)
                    if control["kind"] != "InputBatch": raise ValueError("PROTOCOL: data batch is missing InputBatch")
                    sequence = int(control.get("sequence", 0))
                    if sequence != state.last_result + 1: raise ValueError("PROTOCOL: input sequence is not contiguous")
                    batch = chunk.data
                    if batch.num_rows <= 0: raise ValueError("PROTOCOL: empty input batch")
                    if batch.num_rows > max_batch_rows: raise ValueError("RESOURCE_EXHAUSTED: input batch has too many rows")
                    if mode == MODE_VECTOR:
                        call_context = VectorContext(sdk_version, _CallLogger(), statement_context, batch.num_rows)
                        try:
                            output = handler(call_context, *[batch.column(index) for index in range(batch.num_columns)])
                        except Exception as exc:
                            raise ValueError("USER_CODE: handler failed") from exc
                        if not isinstance(output, (pa.Array, pa.ChunkedArray)):
                            raise ValueError("TYPE_CONTRACT: VECTOR handler must return an Arrow Array or ChunkedArray")
                    else:
                        call_context = ScalarContext(sdk_version, _CallLogger(), statement_context)
                        values = []
                        for row in range(batch.num_rows):
                            params = [_scalar_input(batch.column(index), row, args[index]) for index in range(batch.num_columns)]
                            if null_policy == NULL_RETURN and any(value is None for value in params): values.append(None)
                            else:
                                try:
                                    values.append(handler(call_context, *params))
                                except Exception as exc:
                                    raise ValueError("USER_CODE: handler failed") from exc
                        output = values
                    output_array = _output_array(output, result_descriptor, batch.num_rows)
                    output_batch = pa.RecordBatch.from_arrays([output_array], schema=result_schema)
                    state.last_result = sequence
                    writer.write_metadata(_encode_control({"kind": "InputConsumed", "tuple": open_control["tuple"], "sequence": sequence}))
                    writer.write_with_metadata(output_batch, _encode_control({"kind": "ResultBatch", "tuple": open_control["tuple"], "sequence": sequence}))
                    state.wait_result_ack(sequence)
                elif chunk.app_metadata:
                    control = _decode_control(chunk.app_metadata)
                    if control["kind"] == "EndInput":
                        if ended or int(control.get("last_sequence", -1)) != state.last_result: raise ValueError("PROTOCOL: invalid EndInput")
                        ended = True
                    elif control["kind"] != "OpenInvocation":
                        raise ValueError("PROTOCOL: unexpected control without Arrow data")
            if not ended: raise ValueError("PROTOCOL: input stream ended before EndInput")
            if state.acked_result != state.last_result: raise ValueError("PROTOCOL: result is not acknowledged")
            state.finish_id = _uuid.uuid4().hex
            writer.write_metadata(_encode_control({"kind": "Finish", "tuple": open_control["tuple"], "status": "OK", "finish_id": state.finish_id, "last_sequence": state.last_result}))
            state.wait_finish_ack()
        except Exception as exc:
            if writer_started and key is not None:
                try:
                    writer.write_metadata(_encode_control({"kind": "Error", "tuple": open_control["tuple"], "status": "ERROR", "reason": _safe_error(exc)}))
                except Exception:
                    pass
            raise
        finally:
            if key is not None:
                with self._lock:
                    current = self._active.pop(key, None)
                    if current is not None:
                        self._active_bytes -= current.terminal_bytes
                        # A started invocation is terminal even when the worker
                        # reports an error.  Retaining the fence prevents a late
                        # retry from running user code a second time; the caller's
                        # error path remains responsible for reporting failure.
                        self._remember_terminal_locked(key, current)


def _safe_error(exc: Exception) -> str:
    message = str(exc)
    categories = ("PROTOCOL:", "TYPE_CONTRACT:", "USER_CODE:", "RESOURCE_EXHAUSTED:", "DEADLINE_EXCEEDED:")
    if not message.startswith(categories):
        return "USER_CODE: handler or runtime failed"
    return message[:512]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", required=True)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
    address = args.address if "://" in args.address else "grpc://" + args.address
    server = RoutineFlightServer(address)
    server.serve()


if __name__ == "__main__":
    main()
