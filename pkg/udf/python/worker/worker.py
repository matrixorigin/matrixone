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
import contextlib
import datetime as _datetime
import decimal as _decimal
import hashlib
import importlib
import inspect
import json
import logging
import math
import os
import pickle
import selectors
import signal
import struct
import subprocess
import sys
import threading
import time
import uuid as _uuid
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pyarrow as pa
import pyarrow.flight as flight

PROTOCOL_VERSION = 1
MAX_CONTROL_BYTES = 1 << 20
# Admission bounds cover active fences and retained terminal tombstones.  The
# names deliberately describe the whole ledger so a future cleanup change
# cannot mistake active work for reclaimable terminal state.
MAX_LEDGER_ENTRIES = 10000
MAX_LEDGER_BYTES = 16 << 20
TERMINAL_TTL_SECONDS = 300.0
ACK_TIMEOUT_SECONDS = 60.0
MAX_EXECUTION_FRAME_BYTES = 1 << 30
_CONTROL_KEYS = frozenset(
    {
        "version",
        "kind",
        "tuple",
        "sequence",
        "last_sequence",
        "ack_sequence",
        "finish_id",
        "status",
        "reason",
        "payload",
    }
)
_CONTROL_BASE_KEYS = frozenset({"version", "kind", "tuple"})
_CONTROL_FIELD_RULES = {
    "OpenInvocation": (frozenset({"payload"}), frozenset({"payload"})),
    "InputBatch": (frozenset({"sequence"}), frozenset({"sequence"})),
    "EndInput": (frozenset({"last_sequence"}), frozenset({"last_sequence"})),
    "ResultSchema": (frozenset(), frozenset()),
    "InputConsumed": (frozenset({"sequence"}), frozenset({"sequence"})),
    "ResultBatch": (frozenset({"sequence"}), frozenset({"sequence"})),
    "Finish": (
        frozenset({"last_sequence", "finish_id", "status"}),
        frozenset({"last_sequence", "finish_id", "status"}),
    ),
    "AcknowledgeResults": (
        frozenset({"ack_sequence"}),
        frozenset({"ack_sequence"}),
    ),
    "AcknowledgeFinish": (frozenset({"finish_id"}), frozenset({"finish_id"})),
    "Ack": (
        frozenset({"ack_sequence", "finish_id", "status"}),
        frozenset({"status"}),
    ),
    "Error": (frozenset({"status", "reason"}), frozenset({"status", "reason"})),
}
_TUPLE_KEYS = frozenset(
    {
        "account_id",
        "statement_id",
        "group_id",
        "group_epoch",
        "invocation_id",
        "lease_epoch",
    }
)
_DESCRIPTOR_KEYS = frozenset(
    {
        "type_id",
        "width",
        "scale",
        "charset",
        "offset_width",
        "json_encoding",
        "temporal_encoding",
    }
)
_DESCRIPTOR_INT_KEYS = frozenset(
    {"type_id", "width", "scale", "charset", "offset_width"}
)
_DESCRIPTOR_TEXT_KEYS = frozenset({"json_encoding", "temporal_encoding"})
_HANDLER_RESPONSE_ERROR = 0
_HANDLER_RESPONSE_OK = 1
_HANDLER_RESPONSE_FD_ENV = "MATRIXONE_HANDLER_RESPONSE_FD"
_HANDLER_ENV_ALLOWLIST = frozenset({"PATH"})
_MICROS_PER_SECOND = 1_000_000
_MAX_TIME_MICROS = (838 * 60 * 60 + 59 * 60 + 59) * _MICROS_PER_SECOND
_MIN_TIMESTAMP = _datetime.datetime(1970, 1, 1, 0, 0, 1, tzinfo=_datetime.timezone.utc)
_MAX_TIMESTAMP = _datetime.datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=_datetime.timezone.utc)

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

log = logging.getLogger("matrixone.python.udf.worker")


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


@dataclass(frozen=True, slots=True)
class _TerminalRecord:
    expires_at: float
    bytes: int
    last_result: int
    finish_id: Optional[str]


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


def _optional_context_value(context: Dict[str, Any], key: str) -> Optional[str]:
    value = context.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"PROTOCOL: statement context field {key} must be text")
    return value or None


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
        if "session_timezone_offset_minutes" in raw:
            raise ValueError("PROTOCOL: IANA timezone cannot carry a fixed offset")
        try:
            ZoneInfo(timezone_name)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError("PROTOCOL: IANA timezone is not present in the local tzdb") from exc
        timezone = TimezoneContext("IANA", name=timezone_name, tzdb_version=tzdb_version)
    elif timezone_kind == "FIXED_OFFSET":
        offset_text = _required_context_value(raw, "session_timezone_offset_minutes")
        try:
            offset_minutes = int(offset_text, 10)
        except (TypeError, ValueError) as exc:
            raise ValueError("PROTOCOL: invalid fixed timezone offset") from exc
        if offset_minutes < -839 or offset_minutes > 840:
            raise ValueError("PROTOCOL: fixed timezone offset is outside SQL range")
        if "session_timezone_name" in raw or "session_timezone_tzdb_version" in raw:
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
    if list(sql_mode) != sql_mode_value or json.dumps(sql_mode_value, separators=(",", ":"), ensure_ascii=True) != sql_mode_text:
        raise ValueError("PROTOCOL: sql_mode is not canonical")

    current_user = _required_context_value(raw, "current_user")
    connection_collation = _required_context_value(raw, "connection_collation")
    return StatementContext(
        statement_timestamp_utc=timestamp,
        session_timezone=timezone,
        sql_mode=sql_mode,
        current_database=_optional_context_value(raw, "current_database"),
        current_user=current_user,
        current_role=_optional_context_value(raw, "current_role"),
        connection_collation=connection_collation,
    )


def _tuple_key(value: Dict[str, Any]) -> tuple:
    if not isinstance(value, dict) or set(value) - _TUPLE_KEYS:
        raise ValueError("PROTOCOL: unsupported fencing tuple field")
    string_fields = ("statement_id", "group_id", "invocation_id")
    if not isinstance(value, dict) or any(not isinstance(value.get(k), str) or not value[k] for k in string_fields):
        raise ValueError("PROTOCOL: incomplete fencing tuple")
    try:
        for key in string_fields:
            value[key].encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("PROTOCOL: fencing tuple contains invalid UTF-8") from exc
    account_id = value.get("account_id")
    if (
        isinstance(account_id, bool)
        or not isinstance(account_id, int)
        or account_id < 0
        or account_id > (1 << 64) - 1
    ):
        raise ValueError("PROTOCOL: incomplete fencing tuple")
    if any(
        isinstance(value.get(k), bool)
        or not isinstance(value.get(k), int)
        or value[k] <= 0
        or value[k] > (1 << 64) - 1
        for k in ("group_epoch", "lease_epoch")
    ):
        raise ValueError("PROTOCOL: incomplete fencing tuple")
    return (
        value["account_id"],
        value["statement_id"],
        value["group_id"],
        value["group_epoch"],
        value["invocation_id"],
        value["lease_epoch"],
    )


def _reject_duplicate_json_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"PROTOCOL: duplicate control JSON field {key!r}")
        result[key] = value
    return result


def _reject_nonstandard_json_constant(value):
    raise ValueError(f"PROTOCOL: invalid JSON constant {value}")


def _decode_control(data: bytes) -> Dict[str, Any]:
    if not data or len(data) > MAX_CONTROL_BYTES:
        raise ValueError("PROTOCOL: invalid control size")
    try:
        text = bytes(data).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("PROTOCOL: invalid control UTF-8") from exc
    try:
        value = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_json_pairs,
            parse_constant=_reject_nonstandard_json_constant,
        )
    except json.JSONDecodeError as exc:
        raise ValueError("PROTOCOL: invalid control JSON") from exc
    except RecursionError as exc:
        raise ValueError("PROTOCOL: control JSON is too deeply nested") from exc
    if (
        not isinstance(value, dict)
        or type(value.get("version")) is not int
        or value["version"] != PROTOCOL_VERSION
        or not isinstance(value.get("kind"), str)
        or not value["kind"]
    ):
        raise ValueError("PROTOCOL: unsupported control")
    if set(value) - _CONTROL_KEYS:
        raise ValueError("PROTOCOL: unsupported control field")
    _tuple_key(value.get("tuple") or {})
    _validate_control_fields(value, wire=True)
    return value


def _require_tuple(value: Dict[str, Any], expected: Dict[str, Any]) -> None:
    if _tuple_key(value) != _tuple_key(expected):
        raise ValueError("PROTOCOL: fencing tuple changed")


def _required_string(value: Dict[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"PROTOCOL: missing invocation field {key}")
    return item


def _required_positive_int(value: Dict[str, Any], key: str, maximum: int) -> int:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int) or item <= 0 or item > maximum:
        raise ValueError(f"PROTOCOL: invalid invocation field {key}")
    return item


def _required_positive_float(value: Dict[str, Any], key: str, maximum: float) -> float:
    item = value.get(key)
    if (
        isinstance(item, bool)
        or not isinstance(item, (int, float))
        or not math.isfinite(item)
        or item <= 0
        or item > maximum
    ):
        raise ValueError(f"PROTOCOL: invalid invocation field {key}")
    return float(item)


def _required_uint64(value: Dict[str, Any], key: str, allow_zero: bool = False) -> int:
    item = value.get(key)
    minimum = 0 if allow_zero else 1
    if isinstance(item, bool) or not isinstance(item, int) or item < minimum or item > (1 << 64) - 1:
        raise ValueError(f"PROTOCOL: invalid control field {key}")
    return item


def _validate_control_fields(value: Dict[str, Any], wire: bool) -> None:
    kind = value["kind"]
    rule = _CONTROL_FIELD_RULES.get(kind)
    if rule is None:
        raise ValueError(f"PROTOCOL: unsupported control kind {kind!r}")
    allowed, required = rule
    fields = set(value) - _CONTROL_BASE_KEYS
    invalid = fields - allowed
    if invalid:
        raise ValueError(
            f"PROTOCOL: field {next(iter(invalid))!r} is not valid for control kind {kind!r}"
        )
    if wire:
        missing = required - set(value)
        if missing:
            raise ValueError(
                f"PROTOCOL: control kind {kind!r} is missing field {next(iter(missing))!r}"
            )
    if "sequence" in value:
        _required_uint64(value, "sequence")
    if "last_sequence" in value:
        _required_uint64(value, "last_sequence", allow_zero=True)
    if "ack_sequence" in value:
        _required_uint64(value, "ack_sequence")
    if "finish_id" in value:
        _required_string(value, "finish_id")
    if "status" in value:
        _required_string(value, "status")
    if "reason" in value:
        _required_string(value, "reason")
    if kind == "Ack":
        if ("ack_sequence" in value) == ("finish_id" in value):
            raise ValueError("PROTOCOL: Ack requires exactly one acknowledgement identity")
    elif kind == "OpenInvocation" and (
        "payload" not in value or (wire and value["payload"] is None)
    ):
        raise ValueError("PROTOCOL: control kind 'OpenInvocation' is missing field 'payload'")


def _encode_control(value: Dict[str, Any]) -> bytes:
    value = dict(value)
    if set(value) - _CONTROL_KEYS:
        raise ValueError("PROTOCOL: unsupported control field")
    value.setdefault("version", PROTOCOL_VERSION)
    _tuple_key(value["tuple"])
    _validate_control_fields(value, wire=False)
    try:
        data = json.dumps(
            value,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ValueError("PROTOCOL: invalid control JSON") from exc
    if len(data) > MAX_CONTROL_BYTES:
        raise ValueError("PROTOCOL: control is too large")
    return data


def _canonical_descriptor(descriptor: Dict[str, Any]) -> bytes:
    if not isinstance(descriptor, dict):
        raise ValueError("TYPE_CONTRACT: descriptor must be an object")
    unknown = set(descriptor) - _DESCRIPTOR_KEYS
    if unknown:
        raise ValueError("TYPE_CONTRACT: unsupported descriptor field")
    if type(descriptor.get("type_id")) is not int:
        raise ValueError("TYPE_CONTRACT: descriptor type_id must be an integer")
    for key in _DESCRIPTOR_INT_KEYS - {"type_id"}:
        if key in descriptor and type(descriptor[key]) is not int:
            raise ValueError(f"TYPE_CONTRACT: descriptor {key} must be an integer")
    for key in _DESCRIPTOR_TEXT_KEYS:
        if key in descriptor and type(descriptor[key]) is not str:
            raise ValueError(f"TYPE_CONTRACT: descriptor {key} must be text")
    _validate_descriptor_domain(descriptor)
    order = ("type_id", "width", "scale", "charset", "offset_width", "json_encoding", "temporal_encoding")
    result = {}
    for key in order:
        value = descriptor.get(key)
        if key == "type_id" or value not in (None, 0, ""):
            result[key] = value
    return json.dumps(result, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _validate_descriptor_domain(descriptor: Dict[str, Any]) -> None:
    type_id = int(descriptor["type_id"])
    width = int(descriptor.get("width") or 0)
    scale = int(descriptor.get("scale") or 0)
    charset = int(descriptor.get("charset") or 0)
    expected_offset_width = 0 if type_id in (UUID, VECF32, VECF64) else 32
    if width < 0:
        raise ValueError("TYPE_CONTRACT: descriptor width must be non-negative")
    if scale < 0:
        raise ValueError("TYPE_CONTRACT: descriptor scale must be non-negative")
    if int(descriptor.get("offset_width") or expected_offset_width) != expected_offset_width:
        raise ValueError(
            f"TYPE_CONTRACT: descriptor offset_width must be {expected_offset_width}"
        )
    json_encoding = descriptor.get("json_encoding")
    if json_encoding not in (None, "") and (type_id != JSON or json_encoding != "canonical_text"):
        raise ValueError("TYPE_CONTRACT: unsupported JSON encoding")
    temporal_encoding = descriptor.get("temporal_encoding")
    if temporal_encoding not in (None, "") and (
        type_id not in (DATE, DATETIME, TIMESTAMP) or temporal_encoding != "sql_zero_struct"
    ):
        raise ValueError("TYPE_CONTRACT: unsupported temporal encoding")

    if type_id in (BOOL, INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, UINT64, JSON):
        if width or scale or charset:
            raise ValueError("TYPE_CONTRACT: descriptor carries unused fields")
    elif type_id in (DECIMAL64, DECIMAL128):
        maximum = 18 if type_id == DECIMAL64 else 38
        if width < 1 or width > maximum:
            raise ValueError("TYPE_CONTRACT: decimal precision is outside the supported range")
        if scale > width:
            raise ValueError("TYPE_CONTRACT: decimal scale exceeds precision")
        if charset:
            raise ValueError("TYPE_CONTRACT: decimal descriptor carries a charset")
    elif type_id == DATE:
        if scale or charset:
            raise ValueError("TYPE_CONTRACT: DATE descriptor has invalid scale or charset")
    elif type_id in (TIME, DATETIME, TIMESTAMP):
        if scale > 6 or charset:
            raise ValueError("TYPE_CONTRACT: temporal scale or charset is outside the supported range")
    elif type_id in (CHAR, VARCHAR, TEXT):
        if charset > 3:
            raise ValueError("TYPE_CONTRACT: unsupported text charset")
    elif type_id in (BINARY, VARBINARY, BLOB):
        if charset != 1:
            raise ValueError("TYPE_CONTRACT: binary descriptor must use the binary charset")
    elif type_id == UUID:
        if width or scale or charset:
            raise ValueError("TYPE_CONTRACT: UUID descriptor carries unused fields")
    elif type_id in (VECF32, VECF64):
        if width < 1 or width > 65535 or scale or charset:
            raise ValueError("TYPE_CONTRACT: vector dimension or metadata is outside the supported range")
    else:
        raise ValueError(f"TYPE_CONTRACT: unsupported type id {type_id}")


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
        return "@P[16]"
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
    if type_id == JSON:
        return _canonical_json_text(value)
    if type_id == UUID:
        return _uuid.UUID(bytes=bytes(value))
    return value


def _canonical_json_text(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("TYPE_CONTRACT: JSON value must be text")

    def reject_nonstandard_number(_constant: str):
        raise ValueError("non-standard JSON number")

    def reject_nonfinite_float(number: str):
        parsed = float(number)
        if not math.isfinite(parsed):
            raise ValueError("non-finite JSON number")
        return parsed

    try:
        parsed = json.loads(
            value,
            parse_constant=reject_nonstandard_number,
            parse_float=reject_nonfinite_float,
        )
    except Exception as exc:
        raise ValueError("TYPE_CONTRACT: invalid JSON input") from exc
    return json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))


def _check_json(value: str) -> str:
    try:
        canonical = _canonical_json_text(value)
    except ValueError as exc:
        raise ValueError("TYPE_CONTRACT: JSON result must be canonical text") from exc
    if canonical != value:
        raise ValueError("TYPE_CONTRACT: JSON result is not canonical text")
    return value


def _check_microsecond_scale(value: int, descriptor: Dict[str, Any]) -> None:
    scale = int(descriptor.get("scale") or 0)
    if scale < 0 or scale > 6:
        raise ValueError("TYPE_CONTRACT: temporal scale is outside the supported range")
    quantum = 10 ** (6 - scale)
    if value % quantum != 0:
        raise ValueError("TYPE_CONTRACT: temporal value exceeds the declared scale")


def _timedelta_micros(value: _datetime.timedelta) -> int:
    return (value.days * 24 * 60 * 60 + value.seconds) * _MICROS_PER_SECOND + value.microseconds


def _check_scalar(value: Any, descriptor: Dict[str, Any]):
    if value is None:
        return None
    type_id = int(descriptor["type_id"])
    if type_id == BOOL and type(value) is not bool: raise ValueError("TYPE_CONTRACT: expected bool")
    if type_id in (INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, UINT64) and (type(value) is not int): raise ValueError("TYPE_CONTRACT: expected integer")
    if type_id in (FLOAT32, FLOAT64) and type(value) is not float: raise ValueError("TYPE_CONTRACT: expected float")
    if type_id in (CHAR, VARCHAR, TEXT):
        if type(value) is not str: raise ValueError("TYPE_CONTRACT: expected string")
        width = int(descriptor.get("width") or 0)
        if width > 0 and len(value) > width: raise ValueError("TYPE_CONTRACT: string exceeds the declared width")
        return value
    if type_id == JSON: return _check_json(value)
    if type_id in (BINARY, VARBINARY, BLOB):
        if type(value) is not bytes: raise ValueError("TYPE_CONTRACT: expected bytes")
        width = int(descriptor.get("width") or 0)
        if width > 0 and len(value) > width: raise ValueError("TYPE_CONTRACT: binary value exceeds the declared width")
        return value
    if type_id == UUID:
        if not isinstance(value, _uuid.UUID): raise ValueError("TYPE_CONTRACT: expected uuid.UUID")
        return value
    if type_id == TIME:
        if not isinstance(value, _datetime.timedelta): raise ValueError("TYPE_CONTRACT: expected datetime.timedelta")
        micros = _timedelta_micros(value)
        if abs(micros) > _MAX_TIME_MICROS: raise ValueError("TYPE_CONTRACT: TIME is outside the SQL range")
        _check_microsecond_scale(micros, descriptor)
        return value
    if type_id == DATE:
        if not isinstance(value, SqlDate) or (value.is_zero and value.value is not None) or (not value.is_zero and type(value.value) is not _datetime.date): raise ValueError("TYPE_CONTRACT: invalid SqlDate")
        return value
    if type_id == DATETIME:
        if not isinstance(value, SqlDatetime) or (value.is_zero and value.value is not None) or (not value.is_zero and (not isinstance(value.value, _datetime.datetime) or value.value.tzinfo is not None)): raise ValueError("TYPE_CONTRACT: invalid SqlDatetime")
        if not value.is_zero: _check_microsecond_scale(value.value.microsecond, descriptor)
        return value
    if type_id == TIMESTAMP:
        if not isinstance(value, SqlTimestamp) or (value.is_zero and value.value is not None) or (not value.is_zero and (not isinstance(value.value, _datetime.datetime) or value.value.tzinfo is None or value.value.utcoffset() != _datetime.timedelta(0))): raise ValueError("TYPE_CONTRACT: invalid SqlTimestamp")
        if not value.is_zero and not (_MIN_TIMESTAMP <= value.value <= _MAX_TIMESTAMP): raise ValueError("TYPE_CONTRACT: TIMESTAMP is outside the SQL range")
        if not value.is_zero: _check_microsecond_scale(value.value.microsecond, descriptor)
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
        if not value.is_finite(): raise ValueError("TYPE_CONTRACT: decimal must be finite")
        scale = int(descriptor.get("scale") or 0)
        exponent = value.as_tuple().exponent
        if not isinstance(exponent, int) or -exponent != scale: raise ValueError("TYPE_CONTRACT: decimal scale mismatch")
        precision = int(descriptor.get("width") or (18 if type_id == DECIMAL64 else 38))
        if len(value.as_tuple().digits) > precision: raise ValueError("TYPE_CONTRACT: decimal precision exceeded")
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
            # A NULL parent still owns exactly `width` child slots in a
            # FixedSizeListArray.  Leaving those slots out changes the row
            # layout and makes the following non-NULL rows shift left.
            flat = [item for row in checked for item in (row if row is not None else [0] * width)]
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
    if type_id in (DATE, DATETIME, TIMESTAMP):
        struct = array
        for index in range(len(struct)):
            if not struct[index].is_valid: continue
            is_zero = bool(struct.field("is_zero")[index].as_py())
            value = struct.field("value")[index].as_py()
            if value is None: raise ValueError("TYPE_CONTRACT: temporal child is null")
            if is_zero and ((type_id == DATE and value != _datetime.date(1970, 1, 1)) or (type_id == DATETIME and value != _datetime.datetime(1970, 1, 1)) or (type_id == TIMESTAMP and value != _datetime.datetime(1970, 1, 1, tzinfo=_datetime.timezone.utc))):
                raise ValueError("TYPE_CONTRACT: temporal zero placeholder is invalid")
            if not is_zero:
                if type_id == DATE: _check_scalar(SqlDate(False, value), descriptor)
                elif type_id == DATETIME: _check_scalar(SqlDatetime(False, value), descriptor)
                else: _check_scalar(SqlTimestamp(False, value), descriptor)
    elif type_id in (VECF32, VECF64):
        # A FixedSizeListArray may have both null parents and a non-zero row
        # offset.  Its backing child array can therefore contain nulls that
        # are either hidden behind a null parent or outside the visible slice.
        # Only the children belonging to visible, non-null rows are part of
        # the SQL value domain.
        for row in range(len(array)):
            value = array[row]
            if not value.is_valid:
                continue
            for child in value.values:
                if not child.is_valid:
                    raise ValueError("TYPE_CONTRACT: vector child validity must be non-null")
    elif type_id in (CHAR, VARCHAR, TEXT, JSON, BINARY, VARBINARY, BLOB, UUID, TIME, DECIMAL64, DECIMAL128):
        for index in range(len(array)):
            if not array[index].is_valid: continue
            value = array[index].as_py()
            if type_id == UUID:
                if not isinstance(value, (bytes, bytearray)) or len(value) != 16: raise ValueError("TYPE_CONTRACT: UUID must contain 16 bytes")
            else:
                _check_scalar(value, descriptor)


class _DiscardText:
    def write(self, value: str) -> int:
        return len(value)

    def flush(self) -> None:
        return None


def _serialize_record_batch(record: pa.RecordBatch) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, record.schema) as stream:
        stream.write_batch(record)
    return sink.getvalue().to_pybytes()


def _deserialize_record_batch(data: bytes) -> pa.RecordBatch:
    if not isinstance(data, (bytes, bytearray)) or not data:
        raise ValueError("PROTOCOL: execution payload is missing an Arrow batch")
    try:
        reader = pa.ipc.open_stream(pa.py_buffer(data))
    except pa.ArrowException as exc:
        raise ValueError("PROTOCOL: execution payload is not a valid Arrow stream") from exc
    try:
        try:
            record = reader.read_next_batch()
        except StopIteration as exc:
            raise ValueError("PROTOCOL: execution payload contains no Arrow batch") from exc
        try:
            reader.read_next_batch()
        except StopIteration:
            return record
        raise ValueError("PROTOCOL: execution payload contains multiple Arrow batches")
    finally:
        reader.close()


def _execute_handler_batch(request: Dict[str, Any]) -> bytes:
    batch = _deserialize_record_batch(request["input"])
    args = request["args"]
    result_descriptor = request["return"]
    mode = request["mode"]
    null_policy = request["null_policy"]
    sdk_version = request["sdk_version"]
    statement_context = _statement_context(request.get("context"))

    # User code has no stdout/stderr channel in the Flight protocol.  Discard
    # it so a print() cannot corrupt the length-prefixed response frame.
    with contextlib.redirect_stdout(_DiscardText()), contextlib.redirect_stderr(_DiscardText()):
        handler = _load_handler(request["source"], request["handler"])
        if mode == MODE_VECTOR:
            call_context = VectorContext(sdk_version, _CallLogger(), statement_context, batch.num_rows)
            output = handler(call_context, *[batch.column(index) for index in range(batch.num_columns)])
            if not isinstance(output, (pa.Array, pa.ChunkedArray)):
                raise ValueError("TYPE_CONTRACT: VECTOR handler must return an Arrow Array or ChunkedArray")
        else:
            call_context = ScalarContext(sdk_version, _CallLogger(), statement_context)
            values = []
            for row in range(batch.num_rows):
                params = [_scalar_input(batch.column(index), row, args[index]) for index in range(batch.num_columns)]
                if null_policy == NULL_RETURN and any(value is None for value in params):
                    values.append(None)
                else:
                    values.append(handler(call_context, *params))
            output = values
        output_array = _output_array(output, result_descriptor, batch.num_rows)
        result_field = _field("result", result_descriptor)
        result_batch = pa.RecordBatch.from_arrays(
            [output_array], schema=pa.schema([result_field])
        )
        if pa.ipc.get_record_batch_size(result_batch) > request["max_batch_bytes"]:
            raise ValueError("RESOURCE_EXHAUSTED: output batch exceeds byte limit")
        return _serialize_record_batch(result_batch)


def _read_exact(stream, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        chunk = stream.read(size - len(result))
        if not chunk:
            raise EOFError("execution frame ended unexpectedly")
        result.extend(chunk)
    return bytes(result)


def _write_execution_frame(stream, payload: bytes) -> None:
    if len(payload) > MAX_EXECUTION_FRAME_BYTES:
        raise ValueError("RESOURCE_EXHAUSTED: execution frame is too large")
    stream.write(struct.pack(">Q", len(payload)))
    stream.write(payload)
    stream.flush()


def _execute_handler_subprocess() -> None:
    response_fd_text = os.environ.pop(_HANDLER_RESPONSE_FD_ENV, None)
    if response_fd_text is None:
        raise ValueError("PROTOCOL: handler response channel is missing")
    try:
        response_fd = int(response_fd_text)
        response_stream = os.fdopen(response_fd, "wb", buffering=0, closefd=True)
    except (OSError, TypeError, ValueError) as exc:
        raise ValueError("PROTOCOL: handler response channel is invalid") from exc
    try:
        size = struct.unpack(">Q", _read_exact(sys.stdin.buffer, 8))[0]
        if size > MAX_EXECUTION_FRAME_BYTES:
            raise ValueError("RESOURCE_EXHAUSTED: execution request is too large")
        request = pickle.loads(_read_exact(sys.stdin.buffer, size))
        response = bytes([_HANDLER_RESPONSE_OK]) + _execute_handler_batch(request)
    except Exception as exc:
        response = bytes([_HANDLER_RESPONSE_ERROR]) + _safe_error(exc).encode("utf-8")
    try:
        # This descriptor is created by the adapter and is distinct from the
        # process stdout/stderr descriptors available to handler code.
        _write_execution_frame(response_stream, response)
    finally:
        response_stream.close()


def _context_is_cancelled(context) -> bool:
    if context is None:
        return False
    try:
        return bool(context.is_cancelled())
    except Exception:
        return False


def _kill_execution_process(process: subprocess.Popen) -> None:
    # The leader can already have exited while one of its descendants keeps
    # the execution alive.  Always address the process group first; checking
    # poll() before killpg() leaves that descendant outside the cleanup path.
    if os.name == "posix":
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    elif process.poll() is None:
        process.kill()
    try:
        process.wait(timeout=1.0)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def _execution_group_alive(process: subprocess.Popen) -> bool:
    if os.name != "posix":
        return process.poll() is None
    try:
        os.killpg(process.pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _run_handler_process(context, request: Dict[str, Any], timeout_seconds: float) -> bytes:
    deadline = time.monotonic() + timeout_seconds
    request_wire = pickle.dumps(request, protocol=5)
    if len(request_wire) > MAX_EXECUTION_FRAME_BYTES:
        raise ValueError("RESOURCE_EXHAUSTED: execution request is too large")
    request_frame = struct.pack(">Q", len(request_wire)) + request_wire
    response_read_fd, response_write_fd = os.pipe()
    process = None
    selector = None
    popen_kwargs = {
        "stdin": subprocess.PIPE,
        # Handler output is diagnostic-only.  It must never share the
        # adapter response channel, even when user code writes directly to
        # file descriptor 1.
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "close_fds": True,
    }
    if os.name == "posix":
        popen_kwargs["start_new_session"] = True
        popen_kwargs["pass_fds"] = (response_write_fd,)
    # User code runs under a separate execution identity.  In particular, do
    # not inherit CN, object-store, database, or tenant credentials from the
    # worker process.  Routine dependencies are supplied by the selected Python
    # environment and do not need ambient environment variables.
    child_env = {
        name: value
        for name, value in os.environ.items()
        if name in _HANDLER_ENV_ALLOWLIST
    }
    child_env[_HANDLER_RESPONSE_FD_ENV] = str(response_write_fd)
    # The extra descriptor is a protocol-channel separation mechanism, not a
    # sandbox boundary.  An unisolated handler with arbitrary OS access can
    # still inspect inherited descriptors; production isolation must enforce
    # the stronger policy that handler code cannot write this channel.
    popen_kwargs["env"] = child_env
    response = bytearray()
    expected = None
    response_eof = False
    request_offset = 0
    try:
        process = subprocess.Popen(
            [sys.executable, os.path.abspath(__file__), "--execute-handler"],
            **popen_kwargs,
        )
        os.close(response_write_fd)
        response_write_fd = -1
        selector = selectors.DefaultSelector()
        stdin_fd = process.stdin.fileno()
        os.set_blocking(stdin_fd, False)
        os.set_blocking(response_read_fd, False)
        selector.register(stdin_fd, selectors.EVENT_WRITE, "request")
        selector.register(response_read_fd, selectors.EVENT_READ, "response")
        while True:
            if _context_is_cancelled(context):
                raise TimeoutError("DEADLINE_EXCEEDED: handler execution cancelled")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("DEADLINE_EXCEEDED: handler execution timeout")
            events = selector.select(min(remaining, 0.1))
            for event, _ in events:
                if event.data == "request":
                    try:
                        written = os.write(stdin_fd, request_frame[request_offset:])
                    except BlockingIOError:
                        continue
                    except BrokenPipeError as exc:
                        raise ValueError("USER_CODE: handler process closed its request channel") from exc
                    request_offset += written
                    if request_offset == len(request_frame):
                        selector.unregister(stdin_fd)
                        process.stdin.close()
                else:
                    try:
                        chunk = os.read(response_read_fd, 65536)
                    except BlockingIOError:
                        continue
                    if not chunk:
                        response_eof = True
                        selector.unregister(response_read_fd)
                        if expected is None or len(response) < expected + 8:
                            raise ValueError("USER_CODE: handler process exited without a response")
                    else:
                        response.extend(chunk)
                        if expected is None and len(response) >= 8:
                            expected = struct.unpack(">Q", response[:8])[0]
                            if expected > MAX_EXECUTION_FRAME_BYTES:
                                raise ValueError("RESOURCE_EXHAUSTED: execution response is too large")
                        if expected is not None and len(response) > expected + 8:
                            raise ValueError("PROTOCOL: handler process returned trailing response data")

            # A complete frame is not enough.  The adapter wrapper must have
            # returned normally, and all of its process-group descendants
            # must be gone before the adapter accepts the result.
            if process.poll() is not None:
                if process.returncode != 0:
                    raise ValueError("USER_CODE: handler process exited abnormally")
                if _execution_group_alive(process):
                    raise ValueError("USER_CODE: handler process left descendant processes")
                if response_eof and expected is not None and len(response) == expected + 8:
                    break
        payload = bytes(response[8 : expected + 8])
        if not payload:
            raise ValueError("PROTOCOL: handler process returned an empty response")
        if payload[0] == _HANDLER_RESPONSE_ERROR:
            try:
                message = payload[1:].decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ValueError("PROTOCOL: handler process returned an invalid error") from exc
            raise ValueError(message or "USER_CODE: handler process failed")
        if payload[0] != _HANDLER_RESPONSE_OK:
            raise ValueError("PROTOCOL: handler process returned an unknown status")
        return payload[1:]
    finally:
        if selector is not None:
            selector.close()
        if process is not None:
            _kill_execution_process(process)
            if process.stdin is not None:
                process.stdin.close()
        if response_write_fd >= 0:
            os.close(response_write_fd)
        os.close(response_read_fd)



class _InvocationState:
    def __init__(self, tuple_value: Dict[str, Any], terminal_bytes: int):
        self.tuple = tuple_value
        self.terminal_bytes = terminal_bytes
        self.condition = threading.Condition()
        self.last_input = 0
        self.last_result = 0
        self.acked_result = 0
        self.finish_id: Optional[str] = None
        self.finish_acked = False

    def ack_result(self, sequence: int) -> None:
        with self.condition:
            if sequence <= 0 or sequence < self.acked_result or sequence > self.last_result:
                raise ValueError("PROTOCOL: result ACK is outside the received range")
            self.acked_result = sequence
            self.condition.notify_all()

    def wait_result_ack(self, sequence: int, context=None) -> None:
        deadline = time.monotonic() + ACK_TIMEOUT_SECONDS
        with self.condition:
            while self.acked_result < sequence:
                if context is not None and context.is_cancelled():
                    raise TimeoutError("DEADLINE_EXCEEDED: result ACK wait cancelled")
                remaining = deadline - time.monotonic()
                if remaining <= 0: raise TimeoutError("DEADLINE_EXCEEDED: result ACK timeout")
                self.condition.wait(min(remaining, 0.2))

    def ack_finish(self, finish_id: str) -> None:
        with self.condition:
            if not self.finish_id or finish_id != self.finish_id: raise ValueError("PROTOCOL: invalid finish id")
            self.finish_acked = True
            self.condition.notify_all()

    def wait_finish_ack(self, context=None) -> None:
        deadline = time.monotonic() + ACK_TIMEOUT_SECONDS
        with self.condition:
            while not self.finish_acked:
                if context is not None and context.is_cancelled():
                    raise TimeoutError("DEADLINE_EXCEEDED: Finish ACK wait cancelled")
                remaining = deadline - time.monotonic()
                if remaining <= 0: raise TimeoutError("DEADLINE_EXCEEDED: Finish ACK timeout")
                self.condition.wait(min(remaining, 0.2))


class RoutineFlightServer(flight.FlightServerBase):
    def __init__(self, location: str):
        super().__init__(location)
        self._lock = threading.RLock()
        self._active: Dict[tuple, _InvocationState] = {}
        self._terminal: OrderedDict[tuple, _TerminalRecord] = OrderedDict()
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
        expired = [key for key, record in self._terminal.items() if record.expires_at <= now]
        for key in expired:
            record = self._terminal.pop(key)
            self._terminal_bytes -= record.bytes

    def _admit(self, key: tuple) -> _InvocationState:
        size = self._entry_bytes(key)
        with self._lock:
            self._purge_terminal_locked(time.monotonic())
            if key in self._active:
                raise ValueError("PROTOCOL: invocation fence is active")
            if key in self._terminal:
                raise ValueError("PROTOCOL: invocation fence is terminal")
            if len(self._active) + len(self._terminal) >= MAX_LEDGER_ENTRIES:
                raise ValueError("RESOURCE_EXHAUSTED: terminal ledger entries are full")
            if self._active_bytes + self._terminal_bytes + size > MAX_LEDGER_BYTES:
                raise ValueError("RESOURCE_EXHAUSTED: terminal ledger bytes are full")
            state = _InvocationState({}, size)
            self._active[key] = state
            self._active_bytes += size
            return state

    def _remember_terminal_locked(self, key: tuple, state: _InvocationState) -> None:
        deadline = time.monotonic() + TERMINAL_TTL_SECONDS
        self._terminal[key] = _TerminalRecord(
            deadline, state.terminal_bytes, state.last_result, state.finish_id
        )
        self._terminal.move_to_end(key)
        self._terminal_bytes += state.terminal_bytes

    def _finish_invocation(self, key: tuple, state: Optional[_InvocationState]) -> None:
        # A rejected duplicate Open has no ownership of the existing state.
        # Cleanup must therefore be identity based, not key based.
        if key is None or state is None:
            return
        with self._lock:
            current = self._active.get(key)
            if current is not state:
                return
            self._active.pop(key, None)
            self._active_bytes -= state.terminal_bytes
            # A started invocation is terminal even when the worker reports an
            # error.  Retaining the fence prevents a late retry from running
            # user code a second time.
            self._remember_terminal_locked(key, state)

    def do_action(self, context, action):
        control = _decode_control(action.body)
        key = _tuple_key(control["tuple"])
        if action.type not in ("AcknowledgeResults", "AcknowledgeFinish"):
            raise ValueError("PROTOCOL: unknown action")
        if control["kind"] != action.type:
            raise ValueError("PROTOCOL: action kind does not match action type")
        with self._lock:
            self._purge_terminal_locked(time.monotonic())
            state = self._active.get(key)
            terminal = self._terminal.get(key)
        if state is None:
            if terminal is None:
                raise ValueError("PROTOCOL: unknown invocation")
            if not terminal.finish_id:
                raise ValueError("PROTOCOL: invocation did not complete successfully")
            if action.type == "AcknowledgeResults":
                sequence = _required_uint64(control, "ack_sequence")
                if sequence != terminal.last_result:
                    raise ValueError("PROTOCOL: terminal result ACK does not match the completed result")
            else:
                finish_id = _required_string(control, "finish_id")
                if finish_id != terminal.finish_id:
                    raise ValueError("PROTOCOL: terminal Finish ACK does not match the completed Finish")
            yield _encode_control(self._ack_control(control))
            return
        if action.type == "AcknowledgeResults":
            state.ack_result(_required_uint64(control, "ack_sequence"))
        elif action.type == "AcknowledgeFinish":
            state.ack_finish(_required_string(control, "finish_id"))
        yield _encode_control(self._ack_control(control))

    @staticmethod
    def _ack_control(control: Dict[str, Any]) -> Dict[str, Any]:
        value = {"kind": "Ack", "tuple": control["tuple"], "status": "OK"}
        if control["kind"] == "AcknowledgeResults":
            value["ack_sequence"] = control["ack_sequence"]
        else:
            value["finish_id"] = control["finish_id"]
        return value

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
            if not isinstance(open_control.get("payload"), dict):
                raise ValueError("PROTOCOL: invocation payload must be an object")
            key = _tuple_key(open_control["tuple"])
            payload = open_control["payload"]
            args = payload.get("args")
            result_descriptor = payload.get("return")
            if not isinstance(args, list) or any(not isinstance(item, dict) for item in args):
                raise ValueError("PROTOCOL: invocation args must be an array of descriptors")
            if not isinstance(result_descriptor, dict):
                raise ValueError("PROTOCOL: invocation return must be a descriptor")
            statement_context = _statement_context(payload.get("context"))
            mode = _required_string(payload, "mode")
            null_policy = _required_string(payload, "null_policy")
            abi_contract = _required_string(payload, "abi_contract")
            adapter_version = _required_string(payload, "adapter_version")
            sdk_version = _required_string(payload, "sdk_version")
            max_batch_bytes = _required_positive_int(payload, "max_batch_bytes", 1 << 30)
            max_batch_rows = _required_positive_int(payload, "max_batch_rows", 1 << 30)
            handler_timeout_seconds = _required_positive_float(
                payload, "handler_timeout_seconds", 3600.0
            )
            if mode not in (MODE_SCALAR, MODE_VECTOR) or null_policy not in (NULL_CALL, NULL_RETURN): raise ValueError("PROTOCOL: unsupported call mode or NULL policy")
            if abi_contract != ABI_CONTRACT or adapter_version != ADAPTER_VERSION: raise ValueError("PROTOCOL: unsupported Python ABI contract")
            if sdk_version != SDK_VERSION: raise ValueError("PROTOCOL: unsupported Python SDK")
            source = _required_string(payload, "source")
            handler_name = _required_string(payload, "handler")
            # Validate the complete frozen type contract before reserving a
            # ledger entry.  A malformed Open is rejected before it can leave
            # a terminal tombstone behind.
            for index, descriptor in enumerate(args):
                _field(f"arg_{index}", descriptor)
            result_field = _field("result", result_descriptor)
            state = self._admit(key)
            state.tuple = open_control["tuple"]
            schema = None
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
                if chunk is None or (chunk.data is None and not chunk.app_metadata):
                    raise ValueError("PROTOCOL: empty input frame")
                if chunk.data is not None:
                    if ended:
                        raise ValueError("PROTOCOL: input arrived after EndInput")
                    if schema is None:
                        schema = reader.schema
                        _validate_schema(schema, args)
                    control = _decode_control(chunk.app_metadata)
                    _require_tuple(control["tuple"], open_control["tuple"])
                    if control["kind"] != "InputBatch": raise ValueError("PROTOCOL: data batch is missing InputBatch")
                    sequence = _required_uint64(control, "sequence")
                    if sequence != state.last_input + 1: raise ValueError("PROTOCOL: input sequence is not contiguous")
                    batch = chunk.data
                    if batch.num_rows <= 0: raise ValueError("PROTOCOL: empty input batch")
                    if batch.num_rows > max_batch_rows: raise ValueError("RESOURCE_EXHAUSTED: input batch has too many rows")
                    if batch.nbytes > max_batch_bytes: raise ValueError("RESOURCE_EXHAUSTED: input batch exceeds byte limit")
                    for index, descriptor in enumerate(args):
                        _validate_array_values(batch.column(index), descriptor)
                    execution_request = {
                        "source": source,
                        "handler": handler_name,
                        "mode": mode,
                        "null_policy": null_policy,
                        "sdk_version": sdk_version,
                        "context": payload.get("context"),
                        "args": args,
                        "return": result_descriptor,
                        "max_batch_bytes": max_batch_bytes,
                        "input": _serialize_record_batch(batch),
                    }
                    output_wire = _run_handler_process(
                        context, execution_request, handler_timeout_seconds
                    )
                    output_batch = _deserialize_record_batch(output_wire)
                    if output_batch.num_rows != batch.num_rows or output_batch.num_columns != 1:
                        raise ValueError("TYPE_CONTRACT: handler result has the wrong shape")
                    _validate_field(output_batch.schema.field(0), "result", result_descriptor)
                    output_array = output_batch.column(0)
                    _validate_array_values(output_array, result_descriptor)
                    if pa.ipc.get_record_batch_size(output_batch) > max_batch_bytes:
                        raise ValueError("RESOURCE_EXHAUSTED: output batch exceeds byte limit")
                    state.last_input = sequence
                    state.last_result = sequence
                    writer.write_metadata(_encode_control({"kind": "InputConsumed", "tuple": open_control["tuple"], "sequence": sequence}))
                    writer.write_with_metadata(output_batch, _encode_control({"kind": "ResultBatch", "tuple": open_control["tuple"], "sequence": sequence}))
                    state.wait_result_ack(sequence, context)
                elif chunk.app_metadata:
                    control = _decode_control(chunk.app_metadata)
                    _require_tuple(control["tuple"], open_control["tuple"])
                    if control["kind"] == "EndInput":
                        last_sequence = _required_uint64(control, "last_sequence", allow_zero=True)
                        if ended:
                            if last_sequence != state.last_input:
                                raise ValueError("PROTOCOL: EndInput changed after input was closed")
                            continue
                        if last_sequence != state.last_input:
                            raise ValueError("PROTOCOL: invalid EndInput")
                        ended = True
                    elif control["kind"] == "OpenInvocation":
                        raise ValueError("PROTOCOL: duplicate OpenInvocation")
                    else:
                        raise ValueError("PROTOCOL: unexpected control without Arrow data")
            if not ended: raise ValueError("PROTOCOL: input stream ended before EndInput")
            if state.acked_result != state.last_result: raise ValueError("PROTOCOL: result is not acknowledged")
            state.finish_id = _uuid.uuid4().hex
            writer.write_metadata(_encode_control({"kind": "Finish", "tuple": open_control["tuple"], "status": "OK", "finish_id": state.finish_id, "last_sequence": state.last_input}))
            state.wait_finish_ack(context)
        except Exception as exc:
            if writer_started and key is not None:
                try:
                    writer.write_metadata(_encode_control({"kind": "Error", "tuple": open_control["tuple"], "status": "ERROR", "reason": _safe_error(exc)}))
                except Exception:
                    pass
            raise
        finally:
            self._finish_invocation(key, state)


def _safe_error(exc: Exception) -> str:
    message = str(exc)
    categories = ("PROTOCOL:", "TYPE_CONTRACT:", "USER_CODE:", "RESOURCE_EXHAUSTED:", "DEADLINE_EXCEEDED:")
    if not message.startswith(categories):
        return "USER_CODE: handler or runtime failed"
    return message[:512]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute-handler", action="store_true")
    parser.add_argument("--address")
    args = parser.parse_args()
    if args.execute_handler:
        _execute_handler_subprocess()
        return
    if not args.address:
        parser.error("--address is required for the Flight server")
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
    address = args.address if "://" in args.address else "grpc://" + args.address
    server = RoutineFlightServer(address)
    server.serve()


if __name__ == "__main__":
    main()
