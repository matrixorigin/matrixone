# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Arrow Flight worker for the MatrixOne Python routine contract.

The process is deliberately a small protocol endpoint.  It does not install
packages, read SQL data, or interpret user supplied paths.  This development
adapter is not a security boundary; a future production launcher must wrap it
in the sandbox and deployment policy selected for the routine.
"""

from __future__ import annotations

import sys

# Handler children execute only the length-prefixed Arrow/pipe contract.  Keep
# service-side modules out of that import path; the child is started for each
# invocation burst and cannot reuse the worker's imported Python heap.
_HANDLER_ENTRY = "--execute-handler" in sys.argv

import contextlib
import datetime as _datetime
import decimal as _decimal
import hashlib
import inspect
import json
import logging
import math
import os
import pickle
import signal
import struct
import threading
import time
import uuid as _uuid
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional
from zoneinfo import TZPATH, ZoneInfo, ZoneInfoNotFoundError

if not _HANDLER_ENTRY:
    import queue
    import selectors
    import subprocess

import pyarrow as pa

# Handler children only need Arrow. Avoid importing Flight on that entry path;
# the annotations in this module are postponed, and the Flight server class is
# never instantiated by the handler child.

class _NoFlightServerBase:
    pass


if _HANDLER_ENTRY:
    flight = None
    _FlightServerBase = _NoFlightServerBase
else:
    import pyarrow.flight as flight

    _FlightServerBase = flight.FlightServerBase

PROTOCOL_VERSION = 1
MAX_CONTROL_BYTES = 1 << 20
# Keep control parsing shallow even when the total JSON envelope is below the
# byte limit.  This is the Python counterpart of protocol.MaxJSONNesting; a
# byte scanner makes the bound independent of the interpreter's recursion
# limit and ignores bracket characters inside JSON strings.
MAX_JSON_NESTING = 64
# Keep the Python wire boundary identical to protocol.FencingTuple's
# component limit.  The control frame limit alone is not sufficient: a
# single oversized identity would otherwise consume most of every ledger
# entry and would be accepted by the worker after Go rejected it.
MAX_FENCE_COMPONENT_BYTES = 256
# SecurityFrame uses uint32 principal identifiers in the language-neutral
# contract.  Keep the Python decoder's acceptance domain identical to Go;
# checking only for a non-negative Python int would admit values that no Go
# caller can represent and would make quota/security identity non-canonical.
MAX_SECURITY_PRINCIPAL_ID = (1 << 32) - 1
MIN_INT64 = -(1 << 63)
MAX_INT64 = (1 << 63) - 1
# StatementContext is materialized as datetime.datetime. This is narrower
# than the wire int64 domain and must match the Go context validator before an
# invocation reaches user code.
MIN_STATEMENT_TIMESTAMP_UTC = -62135596800000000
MAX_STATEMENT_TIMESTAMP_UTC = 253402300799999999
MIN_INT32 = -(1 << 31)
MAX_INT32 = (1 << 31) - 1
# Admission bounds cover active fences and retained terminal tombstones.  The
# names deliberately describe the whole ledger so a future cleanup change
# cannot mistake active work for reclaimable terminal state.
MAX_LEDGER_ENTRIES = 10000
MAX_LEDGER_BYTES = 16 << 20
# Closed group fences are retained separately from terminal records. They are
# the proof that permits TTL collection without allowing an old tuple to
# regain an execution grant. The cap makes the safety cost explicit: once the
# owner fence is full, admission fails closed until a durable scheduler fence
# is available.
MAX_CLOSED_GROUP_ENTRIES = MAX_LEDGER_ENTRIES
MAX_CLOSED_GROUP_BYTES = MAX_LEDGER_BYTES
TERMINAL_TTL_SECONDS = 300.0
ACK_TIMEOUT_SECONDS = 60.0
MAX_EXECUTION_FRAME_BYTES = 1 << 30
# The handler IPC frame keeps the control metadata in pickle and transports
# the Arrow bytes through protocol 5's out-of-band buffer.  The Arrow payload
# itself is a RecordBatch message; its schema is reconstructed from the
# frozen descriptors on both sides, so a schema message is not repeated for
# every batch in a bounded invocation burst.
HANDLER_ARROW_STREAM = "stream"
HANDLER_ARROW_RECORD_BATCH = "record_batch"
# A definition validation action carries one immutable source plus its typed
# contract. The source itself is bounded by the artifact limit below; keep
# the action envelope separate from MAX_CONTROL_BYTES, which bounds the
# smaller per-invocation control frames.
MAX_DEFINITION_VALIDATION_BYTES = (2 << 20) + MAX_CONTROL_BYTES
MAX_HANDLER_PROCESSES = 8
# H is a worker-wide child limit.  The two narrower limits prevent one
# account, or one principal inside an account, from consuming all H slots.
# They are advertised as part of the worker capability so a Gateway can fail
# closed when it talks to a worker with a different fairness contract.
MAX_ACCOUNT_HANDLER_PROCESSES = MAX_HANDLER_PROCESSES
MAX_OWNER_HANDLER_PROCESSES = MAX_HANDLER_PROCESSES // 2
_DEFAULT_HANDLER_SLOTS = threading.BoundedSemaphore(MAX_HANDLER_PROCESSES)
# Materializing a scalar column once is useful for a substantial batch, but
# costs more than indexed access for the tiny batches common in short calls.
# Keep this threshold local to the worker conversion path; it is not a wire
# or admission limit and does not change the handler's batch semantics.
SCALAR_MATERIALIZATION_MIN_ROWS = 64
_CONTROL_KEYS = frozenset(
    {
        "version",
        "kind",
        "tuple",
        "sequence",
        "last_sequence",
        "last_result_sequence",
        "ack_sequence",
        "released_bytes",
        "released_batches",
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
    "InputConsumed": (
        frozenset({"sequence", "released_bytes", "released_batches"}),
        frozenset({"sequence", "released_bytes", "released_batches"}),
    ),
    "ResultSchema": (frozenset(), frozenset()),
    "ResultBatch": (frozenset({"sequence"}), frozenset({"sequence"})),
    "Finish": (
        frozenset({"last_sequence", "last_result_sequence", "finish_id", "status"}),
        frozenset({"last_sequence", "last_result_sequence", "finish_id", "status"}),
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
_FUNCTION_REF_KEYS = frozenset(
    {"account_id", "database_id", "function_id", "revision", "namespace_version"}
)
_OPEN_PAYLOAD_KEYS = frozenset(
    {
        "function_ref",
        "handler",
        "source",
        "mode",
        "null_policy",
        "abi_contract",
        "adapter_version",
        "sdk_version",
        "definition_schema_version",
        "artifact_digest",
        "environment_digest",
        "definition_fingerprint",
        "context",
        "callsite_id",
        "may_error",
        "security_mode",
        "leakproof",
        "statement_context",
        "security_frame",
        "args",
        "return",
        "max_batch_bytes",
        "max_batch_rows",
        "max_invocation_rows",
        "max_invocation_result_bytes",
        "handler_timeout_seconds",
    }
)
_OPEN_PAYLOAD_REQUIRED_KEYS = _OPEN_PAYLOAD_KEYS - {
    "context",
}
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
_HANDLER_REQUEST_KIND_KEY = "__matrixone_handler_request_kind"
_HANDLER_REQUEST_FULL = "full"
_HANDLER_REQUEST_BATCH = "batch"
_HANDLER_RESPONSE_FD_ENV = "MATRIXONE_HANDLER_RESPONSE_FD"
_HANDLER_PARENT_WATCH_FD_ENV = "MATRIXONE_HANDLER_PARENT_WATCH_FD"
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
DEFINITION_SCHEMA_VERSION = 1
PLAN_CONTRACT_VERSION = 1
TYPE_DESCRIPTOR_CONTRACT = "ARROW_DESCRIPTOR"


def _timezone_database_version() -> str:
    for root in TZPATH:
        try:
            with open(
                os.path.join(root, "tzdata.zi"), "r", encoding="utf-8"
            ) as source:
                line = source.readline().strip()
        except (OSError, UnicodeError):
            continue
        prefix = "# version "
        if line.startswith(prefix) and line[len(prefix) :].strip():
            return line[len(prefix) :].strip()
    return ""


TIMEZONE_DATABASE_VERSION = _timezone_database_version()


def _contract_digest(domain: str, *parts: str) -> str:
    digest = hashlib.sha256()
    for part in (domain, *parts):
        encoded = part.encode("utf-8")
        digest.update(struct.pack(">Q", len(encoded)))
        digest.update(encoded)
    return digest.hexdigest()


def _inline_artifact_digest(handler: str, source: str) -> str:
    return _contract_digest("matrixone-python-inline-artifact", handler, source)


def _environment_digest() -> str:
    if not TIMEZONE_DATABASE_VERSION:
        return ""
    return _contract_digest(
        "matrixone-python-environment",
        str(PROTOCOL_VERSION),
        ABI_CONTRACT,
        ADAPTER_VERSION,
        SDK_VERSION,
        str(DEFINITION_SCHEMA_VERSION),
        str(PLAN_CONTRACT_VERSION),
        TYPE_DESCRIPTOR_CONTRACT,
        TIMEZONE_DATABASE_VERSION,
    )


def _canonical_definition_json(value: Any) -> bytes:
    """Match Go encoding/json's canonical UTF-8 output for this contract."""
    encoded = json.dumps(
        value, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )
    # encoding/json escapes these characters even when the payload is UTF-8.
    # U+2028/U+2029 are also escaped by Go for safe embedding in JavaScript.
    return (
        encoded.replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
        .encode("utf-8")
    )


def _definition_fingerprint(payload: Dict[str, Any]) -> str:
    """Hash the immutable executable contract in an Open.

    The original source is a catalog/audit payload.  The artifact digest
    identifies the exact bytes resolved by the trusted Gateway, so source is
    intentionally absent from this canonical definition body.
    """
    body = {
        "definition_schema_version": payload["definition_schema_version"],
        "handler": payload["handler"],
        "mode": payload["mode"],
        "null_policy": payload["null_policy"],
        "abi_contract": payload["abi_contract"],
        "adapter_version": payload["adapter_version"],
        "artifact_digest": payload["artifact_digest"],
        "environment_digest": payload["environment_digest"],
        "sdk_version": payload["sdk_version"],
        # Go's canonical definition body uses `omitempty` for ArgTypes. An
        # empty VECTOR signature must therefore omit the field rather than
        # encode it as an empty array, otherwise zero-argument calls would
        # have different fingerprints on the two sides of the ABI.
        "arg_types": [
            json.loads(_canonical_descriptor(descriptor).decode("utf-8"))
            for descriptor in payload["args"]
        ],
        "return_type": json.loads(
            _canonical_descriptor(payload["return"]).decode("utf-8")
        ),
    }
    if not body["arg_types"]:
        del body["arg_types"]
    return hashlib.sha256(_canonical_definition_json(body)).hexdigest()

_CAPABILITY_REQUEST_KEYS = frozenset({"protocol_version"})
_DEFINITION_VALIDATION_KEYS = frozenset(
    {
        "account_id",
        "handler",
        "source",
        "mode",
        "null_policy",
        "abi_contract",
        "adapter_version",
        "sdk_version",
        "definition_schema_version",
        "artifact_digest",
        "environment_digest",
        "definition_fingerprint",
        "args",
        "return",
    }
)
_CAPABILITY_RESPONSE_KEYS = frozenset(
    {
        "protocol_version",
        "abi_contract",
        "adapter_version",
        "sdk_version",
        "definition_schema_version",
        "plan_contract_version",
        "type_descriptor_contract",
        "timezone_database_version",
        "modes",
        "null_policies",
        "window_batches",
        "cumulative_ack",
        "max_execution_frame_bytes",
        "max_handler_processes",
        "max_account_handler_processes",
        "max_owner_handler_processes",
        "lease_epoch",
    }
)

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
    outcome: str


_TERMINAL_SUCCESS = "SUCCESS"
_TERMINAL_FINISH_UNCONFIRMED = "FINISH_UNCONFIRMED"
_TERMINAL_CANCELLED = "CANCELLED"
_TERMINAL_FAILED = "FAILED"


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
_TYPED_STATEMENT_CONTEXT_KEYS = frozenset(
    {
        "contract_version",
        "statement_timestamp_utc",
        "timezone_kind",
        "timezone_name",
        "timezone_offset_minutes",
        "timezone_database_version",
        "sql_mode",
        "current_database",
        "current_user",
        "current_role",
        "connection_collation",
    }
)
_SECURITY_FRAME_KEYS = frozenset(
    {
        "contract_version",
        "mode",
        "invoker_user_id",
        "invoker_role_id",
        "effective_user_id",
        "effective_role_id",
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


def _typed_statement_context(raw: Any) -> Optional[Dict[str, Any]]:
    """Validate the typed Go execution snapshot and return canonical map data."""
    if raw is None:
        return None
    if not isinstance(raw, dict) or set(raw) - _TYPED_STATEMENT_CONTEXT_KEYS:
        raise ValueError("PROTOCOL: unsupported typed statement context field")
    if type(raw.get("contract_version")) is not int or raw["contract_version"] != 1:
        raise ValueError("UNSUPPORTED_ROUTINE_VERSION: unsupported statement context contract")
    timestamp = raw.get("statement_timestamp_utc")
    if (
        isinstance(timestamp, bool)
        or not isinstance(timestamp, int)
        or timestamp < MIN_STATEMENT_TIMESTAMP_UTC
        or timestamp > MAX_STATEMENT_TIMESTAMP_UTC
    ):
        raise ValueError("PROTOCOL: invalid typed statement timestamp")
    timezone_kind = raw.get("timezone_kind")
    if not isinstance(timezone_kind, str):
        raise ValueError("PROTOCOL: typed statement timezone is missing")
    timezone_name = raw.get("timezone_name", "")
    tzdb_version = raw.get("timezone_database_version", "")
    offset = raw.get("timezone_offset_minutes", 0)
    if isinstance(offset, bool) or not isinstance(offset, int):
        raise ValueError("PROTOCOL: invalid typed timezone offset")
    if timezone_kind == "IANA":
        if not isinstance(timezone_name, str) or not timezone_name or not isinstance(tzdb_version, str) or not tzdb_version or offset != 0:
            raise ValueError("PROTOCOL: incomplete typed IANA timezone")
    elif timezone_kind == "FIXED_OFFSET":
        if timezone_name != "" or tzdb_version != "" or offset < -839 or offset > 840:
            raise ValueError("PROTOCOL: invalid typed fixed timezone")
    else:
        raise ValueError("PROTOCOL: unsupported typed statement timezone")
    sql_mode = raw.get("sql_mode")
    if not isinstance(sql_mode, list) or any(not isinstance(item, str) or not item for item in sql_mode):
        raise ValueError("PROTOCOL: typed statement sql_mode must be an array of strings")
    if sql_mode != sorted(set(sql_mode)):
        raise ValueError("PROTOCOL: typed statement sql_mode is not canonical")
    current_user = raw.get("current_user")
    collation = raw.get("connection_collation")
    if not isinstance(current_user, str) or not current_user or not isinstance(collation, str) or not collation:
        raise ValueError("PROTOCOL: typed statement principal or collation is missing")
    optional_text = ("current_database", "current_role")
    for key in optional_text:
        if key in raw and not isinstance(raw[key], str):
            raise ValueError(f"PROTOCOL: typed statement field {key} must be text")
    value = {
        "statement_timestamp_utc": str(timestamp),
        "session_timezone_kind": timezone_kind,
        "sql_mode": json.dumps(sql_mode, separators=(",", ":"), ensure_ascii=True),
        "current_user": current_user,
        "connection_collation": collation,
    }
    if timezone_kind == "IANA":
        value["session_timezone_name"] = timezone_name
        value["session_timezone_tzdb_version"] = tzdb_version
    else:
        value["session_timezone_offset_minutes"] = str(offset)
    for key in optional_text:
        if raw.get(key, ""):
            value[key] = raw[key]
    return value


def _validate_typed_call_contract(payload: Dict[str, Any]) -> None:
    fields = {"callsite_id", "may_error", "security_mode", "leakproof"}
    present = fields & set(payload)
    if not present:
        return
    if present != fields:
        raise ValueError("UNSUPPORTED_ROUTINE_VERSION: incomplete typed routine semantic contract")
    callsite_id = payload.get("callsite_id")
    if not isinstance(callsite_id, str) or not callsite_id or len(callsite_id) > 256 or "\n" in callsite_id or "\r" in callsite_id:
        raise ValueError("UNSUPPORTED_ROUTINE_VERSION: invalid typed routine callsite id")
    if payload.get("may_error") is not True or payload.get("security_mode") != "INVOKER" or payload.get("leakproof") is not False:
        raise ValueError("UNSUPPORTED_ROUTINE_VERSION: unsupported typed routine semantic contract")
    frame = payload.get("security_frame")
    if not isinstance(frame, dict) or set(frame) != _SECURITY_FRAME_KEYS:
        raise ValueError("UNSUPPORTED_ROUTINE_VERSION: incomplete Python security frame")
    if (
        type(frame.get("contract_version")) is not int
        or frame["contract_version"] != 1
        or frame.get("mode") != "INVOKER"
    ):
        raise ValueError("UNSUPPORTED_ROUTINE_VERSION: unsupported Python security frame")
    integer_fields = ("invoker_user_id", "invoker_role_id", "effective_user_id", "effective_role_id")
    if any(
        type(frame.get(key)) is not int
        or frame[key] < 0
        or frame[key] > MAX_SECURITY_PRINCIPAL_ID
        for key in integer_fields
    ):
        raise ValueError("UNSUPPORTED_ROUTINE_VERSION: invalid Python security frame")
    if frame["invoker_user_id"] != frame["effective_user_id"] or frame["invoker_role_id"] != frame["effective_role_id"]:
        raise ValueError("UNSUPPORTED_ROUTINE_VERSION: Python security frame changed effective principal")


def _handler_quota_owner(payload: Dict[str, Any]) -> str:
    """Return the stable effective principal key used by the H budget.

    A session display username is mutable text and is not an identity boundary:
    two principals can share it across roles or accounts.  The typed security
    frame has already been validated before this helper is called, so the
    numeric effective IDs are the only values used for account/owner quota
    accounting.
    """
    frame = payload.get("security_frame") if isinstance(payload, dict) else None
    if not isinstance(frame, dict):
        raise ValueError("UNSUPPORTED_ROUTINE_VERSION: missing Python security frame")
    user_id = frame.get("effective_user_id")
    role_id = frame.get("effective_role_id")
    if type(user_id) is not int or user_id < 0 or type(role_id) is not int or role_id < 0:
        raise ValueError("UNSUPPORTED_ROUTINE_VERSION: invalid Python effective principal")
    return f"user:{user_id}/role:{role_id}"


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
        if timestamp_micros < MIN_STATEMENT_TIMESTAMP_UTC or timestamp_micros > MAX_STATEMENT_TIMESTAMP_UTC:
            raise ValueError("timestamp is outside the Python datetime range")
        seconds, micros = divmod(timestamp_micros, 1_000_000)
        timestamp = _datetime.datetime(1970, 1, 1, tzinfo=_datetime.timezone.utc) + _datetime.timedelta(seconds=seconds, microseconds=micros)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("PROTOCOL: invalid statement_timestamp_utc") from exc

    timezone_kind = _required_context_value(raw, "session_timezone_kind")
    if timezone_kind == "IANA":
        timezone_name = _required_context_value(raw, "session_timezone_name")
        tzdb_version = _required_context_value(raw, "session_timezone_tzdb_version")
        if "session_timezone_offset_minutes" in raw:
            raise ValueError("PROTOCOL: IANA timezone cannot carry a fixed offset")
        if not TIMEZONE_DATABASE_VERSION or tzdb_version != TIMEZONE_DATABASE_VERSION:
            raise ValueError("PROTOCOL: IANA timezone database version does not match the worker contract")
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
            encoded = value[key].encode("utf-8")
            if len(encoded) > MAX_FENCE_COMPONENT_BYTES:
                raise ValueError("PROTOCOL: fencing tuple component is too large")
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


def _function_ref(value: Any, tuple_value: Dict[str, Any]) -> tuple:
    if not isinstance(value, dict) or set(value) != _FUNCTION_REF_KEYS:
        raise ValueError("PROTOCOL: incomplete FunctionRef")
    fields = {}
    for key in _FUNCTION_REF_KEYS:
        item = value.get(key)
        minimum = 0 if key == "account_id" else 1
        if (
            isinstance(item, bool)
            or not isinstance(item, int)
            or item < minimum
            or item > (1 << 64) - 1
        ):
            raise ValueError(f"PROTOCOL: invalid FunctionRef field {key}")
        fields[key] = item
    if fields["account_id"] != tuple_value["account_id"]:
        raise ValueError("PROTOCOL: FunctionRef account does not match the fencing tuple")
    return tuple(fields[key] for key in ("account_id", "database_id", "function_id", "revision", "namespace_version"))


def _reject_duplicate_json_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"PROTOCOL: duplicate control JSON field {key!r}")
        result[key] = value
    return result


def _reject_nonstandard_json_constant(value):
    raise ValueError(f"PROTOCOL: invalid JSON constant {value}")


def _validate_json_nesting(data: bytes) -> None:
    depth = 0
    in_string = False
    escaped = False
    for byte in data:
        if in_string:
            if escaped:
                escaped = False
            elif byte == 0x5C:  # backslash
                escaped = True
            elif byte == 0x22:  # double quote
                in_string = False
            continue
        if byte == 0x22:
            in_string = True
        elif byte == 0x7B or byte == 0x5B:  # { or [
            depth += 1
            if depth > MAX_JSON_NESTING:
                raise ValueError(
                    f"PROTOCOL: control JSON nesting exceeds {MAX_JSON_NESTING} levels"
                )
        elif byte == 0x7D or byte == 0x5D:  # } or ]
            depth -= 1


def _decode_control(data: bytes) -> Dict[str, Any]:
    if not data or len(data) > MAX_CONTROL_BYTES:
        raise ValueError("PROTOCOL: invalid control size")
    _validate_json_nesting(data)
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


def _decode_capability_request(data: bytes) -> Dict[str, Any]:
    if not data or len(data) > MAX_CONTROL_BYTES:
        raise ValueError("PROTOCOL: invalid capability request size")
    _validate_json_nesting(data)
    try:
        value = json.loads(
            bytes(data).decode("utf-8"),
            object_pairs_hook=_reject_duplicate_json_pairs,
            parse_constant=_reject_nonstandard_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("PROTOCOL: invalid capability request JSON") from exc
    if not isinstance(value, dict) or set(value) != _CAPABILITY_REQUEST_KEYS:
        raise ValueError("PROTOCOL: invalid capability request fields")
    if type(value.get("protocol_version")) is not int or value["protocol_version"] != PROTOCOL_VERSION:
        raise ValueError("PROTOCOL: unsupported capability protocol version")
    return value


def _decode_definition_validation(data: bytes) -> Dict[str, Any]:
    """Decode the strict, pre-publication definition validation payload."""
    if not data or len(data) > MAX_DEFINITION_VALIDATION_BYTES:
        raise ValueError("PROTOCOL: invalid definition validation size")
    _validate_json_nesting(data)
    try:
        value = json.loads(
            bytes(data).decode("utf-8"),
            object_pairs_hook=_reject_duplicate_json_pairs,
            parse_constant=_reject_nonstandard_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("PROTOCOL: invalid definition validation JSON") from exc
    if not isinstance(value, dict) or set(value) != _DEFINITION_VALIDATION_KEYS:
        raise ValueError("PROTOCOL: invalid definition validation fields")

    account_id = value.get("account_id")
    if (
        isinstance(account_id, bool)
        or not isinstance(account_id, int)
        or account_id < 0
        or account_id > (1 << 64) - 1
    ):
        raise ValueError("PROTOCOL: invalid definition validation account")
    handler = _required_string(value, "handler")
    if ":" in handler:
        raise ValueError(
            "UNSUPPORTED_ROUTINE_VERSION: Python external handler import requires the immutable artifact catalog"
        )
    source = _required_string(value, "source")
    try:
        source_bytes = source.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("PROTOCOL: definition validation source is not valid UTF-8") from exc
    if len(source_bytes) > (1 << 20):
        raise ValueError("RESOURCE_EXHAUSTED: Python artifact exceeds 1048576 bytes")

    mode = _required_string(value, "mode")
    null_policy = _required_string(value, "null_policy")
    abi_contract = _required_string(value, "abi_contract")
    adapter_version = _required_string(value, "adapter_version")
    sdk_version = _required_string(value, "sdk_version")
    if mode not in (MODE_SCALAR, MODE_VECTOR) or null_policy not in (NULL_CALL, NULL_RETURN):
        raise ValueError("UNSUPPORTED_ROUTINE_VERSION: unsupported Python mode or NULL policy")
    if abi_contract != ABI_CONTRACT or adapter_version != ADAPTER_VERSION:
        raise ValueError("UNSUPPORTED_ROUTINE_VERSION: unsupported Python ABI contract")
    if sdk_version != SDK_VERSION:
        raise ValueError("UNSUPPORTED_ROUTINE_VERSION: unsupported Python SDK")
    definition_schema_version = value.get("definition_schema_version")
    if (
        type(definition_schema_version) is not int
        or definition_schema_version != DEFINITION_SCHEMA_VERSION
    ):
        raise ValueError("UNSUPPORTED_ROUTINE_VERSION: unsupported Python definition schema")
    artifact_digest = _required_digest(value, "artifact_digest")
    environment_digest = _required_digest(value, "environment_digest")
    definition_fingerprint = _required_digest(value, "definition_fingerprint")
    if artifact_digest != _inline_artifact_digest(handler, source):
        raise ValueError(
            "UNSUPPORTED_ROUTINE_VERSION: Python artifact digest does not match the source"
        )
    if environment_digest != _environment_digest():
        raise ValueError(
            "UNSUPPORTED_ROUTINE_VERSION: Python environment digest does not match the worker contract"
        )

    args = value.get("args")
    result_descriptor = value.get("return")
    if not isinstance(args, list) or any(not isinstance(item, dict) for item in args):
        raise ValueError("TYPE_CONTRACT: definition validation args must be an array of descriptors")
    if not isinstance(result_descriptor, dict):
        raise ValueError("TYPE_CONTRACT: definition validation return must be a descriptor")
    for index, descriptor in enumerate(args):
        _field(f"arg_{index}", descriptor)
    _field("return", result_descriptor)
    expected_fingerprint = _definition_fingerprint(value)
    if definition_fingerprint != expected_fingerprint:
        raise ValueError(
            "UNSUPPORTED_ROUTINE_VERSION: Python definition fingerprint does not match the typed definition"
        )
    return value


def _validate_definition_syntax(value: Dict[str, Any]) -> None:
    """Validate source and the static handler binding without executing it."""
    import ast

    try:
        compile(value["source"], "<matrixone-python-udf>", "exec")
    except SyntaxError as exc:
        line = exc.lineno or 0
        column = exc.offset or 0
        message = exc.msg or "invalid syntax"
        raise ValueError(
            f"USER_CODE: Python syntax error at line {line}, column {column}: {message}"
        ) from exc
    handler = value["handler"]
    if not handler.isidentifier():
        raise ValueError(
            "USER_CODE: Python handler must be a module-level identifier"
        )
    tree = ast.parse(value["source"], filename="<matrixone-python-udf>", mode="exec")
    binding = None

    class NestedModuleBindingVisitor(ast.NodeVisitor):
        """Find handler bindings below a module statement.

        A handler must be established by an unconditional module-level
        function definition or lambda assignment.  Assignments in control
        flow, imports, and pattern targets can otherwise leave CREATE looking
        valid while the value exposed by exec() is absent or non-callable.
        Function/class bodies and comprehension targets have their own scope
        and are deliberately not treated as module rebinding.
        """

        def __init__(self):
            self.found = False

        def _mark(self):
            self.found = True

        def visit_Name(self, node):
            if node.id == handler and isinstance(node.ctx, (ast.Store, ast.Del)):
                self._mark()

        def visit_FunctionDef(self, node):
            if node.name == handler:
                self._mark()

        def visit_AsyncFunctionDef(self, node):
            if node.name == handler:
                self._mark()

        def visit_ClassDef(self, node):
            if node.name == handler:
                self._mark()

        def visit_Lambda(self, node):
            return

        def visit_ListComp(self, node):
            self.visit(node.elt)
            for generator in node.generators:
                self.visit(generator.iter)
                for condition in generator.ifs:
                    self.visit(condition)

        visit_SetComp = visit_ListComp
        visit_GeneratorExp = visit_ListComp

        def visit_DictComp(self, node):
            self.visit(node.key)
            self.visit(node.value)
            for generator in node.generators:
                self.visit(generator.iter)
                for condition in generator.ifs:
                    self.visit(condition)

        def visit_Import(self, node):
            for alias in node.names:
                bound = alias.asname or alias.name.split(".", 1)[0]
                if bound == handler:
                    self._mark()

        def visit_ImportFrom(self, node):
            for alias in node.names:
                if alias.name == "*" or (alias.asname or alias.name) == handler:
                    self._mark()

        def visit_ExceptHandler(self, node):
            if node.name == handler:
                self._mark()
            # A bare ``except:`` has no exception-expression node.  Keep
            # walking its body without asking NodeVisitor to visit None.
            if node.type is not None:
                self.visit(node.type)
            self.visit_nodes(node.body)

        def visit_MatchAs(self, node):
            if node.name == handler:
                self._mark()
            if node.pattern is not None:
                self.visit(node.pattern)

        def visit_MatchStar(self, node):
            if node.name == handler:
                self._mark()

        def visit_MatchMapping(self, node):
            # ``rest`` is the capture name in a ``**name`` mapping pattern,
            # represented as a plain string rather than an ast.Name.
            if node.rest == handler:
                self._mark()
            self.visit_nodes(node.keys)
            self.visit_nodes(node.patterns)

        def visit_nodes(self, nodes):
            for child in nodes:
                self.visit(child)

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == handler:
            # Continue scanning: a later module-level assignment can replace
            # the function object that exec() will expose to the runtime.
            binding = "async" if isinstance(node, ast.AsyncFunctionDef) else "sync"
            continue
        if isinstance(node, ast.ClassDef) and node.name == handler:
            binding = "other"
            continue
        if isinstance(node, ast.Assign):
            if any(isinstance(target, ast.Name) and target.id == handler for target in node.targets):
                binding = "lambda" if isinstance(node.value, ast.Lambda) else "other"
                continue
        if isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == handler:
                binding = "lambda" if isinstance(node.value, ast.Lambda) else "other"
                continue
        if isinstance(node, ast.Delete):
            if any(isinstance(target, ast.Name) and target.id == handler for target in node.targets):
                binding = "other"
                continue
        visitor = NestedModuleBindingVisitor()
        visitor.visit(node)
        if visitor.found:
            binding = "other"
    if binding == "async":
        raise ValueError("USER_CODE: Python handler must be synchronous")
    if binding not in ("sync", "lambda"):
        raise ValueError(
            f"USER_CODE: Python handler {handler!r} is not a module-level function"
        )


def _encode_definition_validation_result(
    value: Dict[str, Any], status: str, reason: Optional[str] = None
) -> bytes:
    response = {"status": status}
    if reason:
        response["reason"] = reason
    if status == "OK":
        response["artifact_digest"] = value["artifact_digest"]
        response["definition_fingerprint"] = value["definition_fingerprint"]
    return json.dumps(response, separators=(",", ":"), sort_keys=True, allow_nan=False).encode(
        "utf-8"
    )


def _encode_capabilities(request: Dict[str, Any], lease_epoch: int = 1) -> bytes:
    _decode_capability_request(json.dumps(request, separators=(",", ":")).encode("utf-8"))
    if isinstance(lease_epoch, bool) or not isinstance(lease_epoch, int) or lease_epoch <= 0 or lease_epoch > (1 << 64) - 1:
        raise ValueError("PROTOCOL: invalid worker lease epoch")
    value = {
        "protocol_version": PROTOCOL_VERSION,
        "abi_contract": ABI_CONTRACT,
        "adapter_version": ADAPTER_VERSION,
        "sdk_version": SDK_VERSION,
        "definition_schema_version": DEFINITION_SCHEMA_VERSION,
        "plan_contract_version": PLAN_CONTRACT_VERSION,
        "type_descriptor_contract": TYPE_DESCRIPTOR_CONTRACT,
        "timezone_database_version": TIMEZONE_DATABASE_VERSION,
        "modes": [MODE_SCALAR, MODE_VECTOR],
        "null_policies": [NULL_CALL, NULL_RETURN],
        # The current implementation intentionally exposes one in-flight
        # batch per invocation. This is a capability, not an implied promise
        # that a future worker accepts cumulative ACKs.
        "window_batches": 1,
        "cumulative_ack": False,
        "max_execution_frame_bytes": MAX_EXECUTION_FRAME_BYTES,
        "max_handler_processes": MAX_HANDLER_PROCESSES,
        "max_account_handler_processes": MAX_ACCOUNT_HANDLER_PROCESSES,
        "max_owner_handler_processes": MAX_OWNER_HANDLER_PROCESSES,
        # This is an instance lease, not a liveness timer.  A new worker
        # process gets a new value, so a Gateway cannot accidentally reuse an
        # invocation tuple that was admitted by a previous worker instance.
        "lease_epoch": lease_epoch,
    }
    if set(value) != _CAPABILITY_RESPONSE_KEYS:
        raise ValueError("PROTOCOL: invalid capability response")
    encoded = json.dumps(
        value, separators=(",", ":"), sort_keys=True, allow_nan=False
    ).encode("utf-8")
    if len(encoded) > MAX_CONTROL_BYTES:
        raise ValueError("RESOURCE_EXHAUSTED: capability response is too large")
    return encoded


def _require_tuple(value: Dict[str, Any], expected: Dict[str, Any]) -> None:
    if _tuple_key(value) != _tuple_key(expected):
        raise ValueError("PROTOCOL: fencing tuple changed")


def _required_string(value: Dict[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"PROTOCOL: missing invocation field {key}")
    return item


def _required_digest(value: Dict[str, Any], key: str) -> str:
    item = _required_string(value, key)
    if len(item) != hashlib.sha256().digest_size * 2:
        raise ValueError(f"PROTOCOL: invalid invocation field {key}")
    try:
        decoded = bytes.fromhex(item)
    except ValueError as exc:
        raise ValueError(f"PROTOCOL: invalid invocation field {key}") from exc
    if item.lower() != item or len(decoded) != hashlib.sha256().digest_size:
        raise ValueError(f"PROTOCOL: invalid invocation field {key}")
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
    if "last_result_sequence" in value:
        _required_uint64(value, "last_result_sequence", allow_zero=True)
    if "ack_sequence" in value:
        _required_uint64(value, "ack_sequence")
    if "released_bytes" in value:
        _required_positive_int(value, "released_bytes", MAX_EXECUTION_FRAME_BYTES)
    if "released_batches" in value:
        _required_uint64(value, "released_batches")
    if "finish_id" in value:
        _required_string(value, "finish_id")
    if "status" in value:
        _required_string(value, "status")
    if "reason" in value:
        _required_string(value, "reason")
    if kind == "Ack":
        if ("ack_sequence" in value) == ("finish_id" in value):
            raise ValueError("PROTOCOL: Ack requires exactly one acknowledgement identity")
    elif kind == "OpenInvocation":
        if "payload" not in value:
            raise ValueError("PROTOCOL: control kind 'OpenInvocation' is missing field 'payload'")
        if not isinstance(value["payload"], dict):
            raise ValueError("PROTOCOL: control kind 'OpenInvocation' payload must be an object")


def _encode_control(value: Dict[str, Any]) -> bytes:
    value = dict(value)
    if set(value) - _CONTROL_KEYS:
        raise ValueError("PROTOCOL: unsupported control field")
    value.setdefault("version", PROTOCOL_VERSION)
    _tuple_key(value["tuple"])
    # The encoder is the last boundary before bytes leave the worker. It
    # must enforce the same required fields as the decoder; otherwise a new
    # producer can emit a frame that Go's MarshalControl would reject.
    _validate_control_fields(value, wire=True)
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
    for key in _DESCRIPTOR_INT_KEYS:
        if key in descriptor and not MIN_INT32 <= descriptor[key] <= MAX_INT32:
            raise ValueError(f"TYPE_CONTRACT: descriptor {key} is outside int32 range")
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
    # A 32-bit offset is part of the canonical descriptor for ordinary values
    # and is serialized by the Go descriptor.  Only fixed-width UUID/vector
    # descriptors omit the zero-valued field.  Do not turn a missing or
    # explicitly zero ordinary offset into an implicit default: that would
    # accept a damaged Catalog row and produce a different ABI on the two
    # sides.
    if expected_offset_width == 32 and "offset_width" not in descriptor:
        raise ValueError("TYPE_CONTRACT: descriptor offset_width is required")
    if int(descriptor.get("offset_width", expected_offset_width)) != expected_offset_width:
        raise ValueError(
            f"TYPE_CONTRACT: descriptor offset_width must be {expected_offset_width}"
        )
    json_encoding = descriptor.get("json_encoding")
    if json_encoding not in (None, "") and (type_id != JSON or json_encoding != "canonical_text"):
        raise ValueError("TYPE_CONTRACT: unsupported JSON encoding")
    if type_id == JSON and json_encoding != "canonical_text":
        raise ValueError("TYPE_CONTRACT: JSON descriptor must use canonical_text encoding")
    temporal_encoding = descriptor.get("temporal_encoding")
    if temporal_encoding not in (None, "") and (
        type_id not in (DATE, DATETIME, TIMESTAMP) or temporal_encoding != "sql_zero_struct"
    ):
        raise ValueError("TYPE_CONTRACT: unsupported temporal encoding")
    if type_id in (DATE, DATETIME, TIMESTAMP) and temporal_encoding != "sql_zero_struct":
        raise ValueError("TYPE_CONTRACT: temporal descriptor must use sql_zero_struct encoding")

    if type_id in (
        BOOL, INT8, INT16, INT32, INT64, UINT8, UINT16, UINT32, UINT64,
        FLOAT32, FLOAT64, JSON,
    ):
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
        if width or scale or charset:
            raise ValueError("TYPE_CONTRACT: DATE descriptor has invalid scale or charset")
    elif type_id in (TIME, DATETIME, TIMESTAMP):
        if width or scale > 6 or charset:
            raise ValueError("TYPE_CONTRACT: temporal scale or charset is outside the supported range")
    elif type_id in (CHAR, VARCHAR, TEXT):
        if scale:
            raise ValueError("TYPE_CONTRACT: text descriptor carries an unused scale")
        if charset not in (0, 2, 3):
            raise ValueError("TYPE_CONTRACT: unsupported text charset")
    elif type_id in (BINARY, VARBINARY, BLOB):
        if scale or charset != 1:
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


def _schema_from_descriptors(
    descriptors: Iterable[Dict[str, Any]], prefix: str
) -> pa.Schema:
    return pa.schema(
        [_field(f"{prefix}_{index}", descriptor) for index, descriptor in enumerate(descriptors)]
    )


TypeMetadataKey = b"mo.udf.type"
TypeFingerprintKey = b"mo.udf.type_fingerprint"


def _validate_field(field: pa.Field, name: str, descriptor: Dict[str, Any]) -> None:
    expected = _field(name, descriptor)
    if field.name != name:
        raise ValueError(f"TYPE_CONTRACT: Arrow field name {field.name!r} does not match {name!r}")
    if field.type != expected.type:
        raise ValueError(f"TYPE_CONTRACT: Arrow type {field.type} does not match {expected.type}")
    if field.nullable != expected.nullable:
        raise ValueError(
            f"TYPE_CONTRACT: Arrow field nullable={field.nullable} does not match {expected.nullable}"
        )
    if field.metadata != expected.metadata:
        raise ValueError("TYPE_CONTRACT: Arrow logical metadata does not match the frozen descriptor")


def _validate_schema(schema: pa.Schema, descriptors: Iterable[Dict[str, Any]]) -> None:
    descriptors = list(descriptors)
    if len(schema) != len(descriptors):
        raise ValueError("TYPE_CONTRACT: input column count does not match the routine signature")
    for index, descriptor in enumerate(descriptors):
        _validate_field(schema.field(index), f"arg_{index}", descriptor)


def _validate_input_batch_schema(
    batch: pa.RecordBatch, expected_schema: pa.Schema, descriptors: Iterable[Dict[str, Any]]
) -> None:
    # Flight carries one stream schema, but each received RecordBatch still
    # owns a schema object.  Validate every batch so a later frame cannot
    # replace a fixed-width Arrow type after the first frame was admitted.
    # The first batch validates every field against the descriptor.  Later
    # batches only need the schema equality check in the normal case; rebuilding
    # every Field and its metadata for every batch was measurable overhead on
    # small W=1 bursts.  Retain the detailed validation on the mismatch path so
    # a malformed later frame still gets the same contract diagnosis.
    # PyArrow's Schema equality ignores metadata by default.  The metadata
    # carries the frozen logical descriptor and fingerprint, so a later batch
    # with the same physical type but a different contract must not pass this
    # fast path.
    if batch.schema.equals(expected_schema, check_metadata=True):
        return
    _validate_schema(batch.schema, descriptors)
    raise ValueError("TYPE_CONTRACT: input batch schema changed")


def _load_handler(source: str, handler: str):
    if ":" in handler:
        raise ValueError(
            "UNSUPPORTED_ROUTINE_VERSION: Python external handler import requires an immutable artifact catalog"
        )
    # CREATE validates the source without executing it, but execution may be
    # reached through a direct Flight request or a stale/malformed caller.
    # Reapply the static binding contract before exec so invalid module-level
    # rebinding cannot run top-level user code before being rejected.
    _validate_definition_syntax({"source": source, "handler": handler})
    namespace: Dict[str, Any] = {"__name__": "__matrixone_routine__"}
    exec(compile(source, "<routine>", "exec"), namespace, namespace)
    function = namespace.get(handler)
    if not callable(function) or inspect.iscoroutinefunction(function):
        raise ValueError("USER_CODE: handler must be a synchronous callable")
    return function


def _scalar_input(array: pa.Array, row: int, descriptor: Dict[str, Any]):
    return _scalar_input_value(array[row].as_py(), descriptor)


def _scalar_input_value(value: Any, descriptor: Dict[str, Any]):
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


def _scalar_column_values(array: pa.Array):
    """Materialize cheap fixed-width scalar columns once per batch.

    Calling Array.__getitem__().as_py() for every argument and row crosses
    the PyArrow/Python boundary repeatedly.  Primitive values have a bounded
    representation under the batch row/byte limits, so one to_pylist() call
    lets the scalar loop reuse those Python objects.  Keep small, nested, and
    variable-width values on the indexed path: that avoids retaining a second
    copy of user data while preserving the same per-row conversion contract.
    """
    if len(array) < SCALAR_MATERIALIZATION_MIN_ROWS:
        return None
    data_type = array.type
    if (
        pa.types.is_boolean(data_type)
        or pa.types.is_integer(data_type)
        or pa.types.is_floating(data_type)
    ):
        return array.to_pylist()
    return None


def _canonical_json_text(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("TYPE_CONTRACT: JSON value must be text")

    def reject_nonstandard_number(_constant: str):
        raise ValueError("non-standard JSON number")

    try:
        json.loads(
            value,
            parse_constant=reject_nonstandard_number,
            # JSON values cross the ABI as compact text.  Parsing numbers as
            # strings keeps validation independent of Python's binary-float
            # range; Go's json.Compact accepts valid tokens such as 1e999 and
            # the handler must observe that exact token unchanged.
            parse_int=str,
            parse_float=str,
        )
    except Exception as exc:
        raise ValueError("TYPE_CONTRACT: invalid JSON input") from exc
    # Match the Gateway's json.Compact contract: remove insignificant
    # whitespace without rewriting number or string tokens. A loads/dumps
    # round trip changes e.g. 1e-7 to 1e-07 and makes SCALAR and VECTOR
    # handlers observe different text for the same SQL value.
    compact = []
    in_string = False
    escaped = False
    for char in value:
        if in_string:
            compact.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
        elif char == '"':
            in_string = True
            compact.append(char)
        elif char not in " \t\r\n":
            compact.append(char)
    return "".join(compact)


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
    # Keep this helper safe when called directly by a runtime adapter or a
    # conformance harness. The normal exchange path validates the Open payload
    # first, but _arrow_type alone can construct a physical type for an
    # invalid semantic descriptor (for example DECIMAL64 precision 19).
    _canonical_descriptor(descriptor)
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
            is_zero_scalar = struct.field("is_zero")[index]
            value_scalar = struct.field("value")[index]
            if not is_zero_scalar.is_valid:
                raise ValueError("TYPE_CONTRACT: temporal zero flag is null")
            if not value_scalar.is_valid:
                raise ValueError("TYPE_CONTRACT: temporal child is null")
            is_zero = bool(is_zero_scalar.as_py())
            value = value_scalar.as_py()
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


def _serialize_record_batch_message(record: pa.RecordBatch) -> memoryview:
    """Serialize one Arrow record-batch message without repeating its schema."""
    try:
        # Keep the Arrow-owned buffer alive through the returned memoryview so
        # the parent can hand it directly to protocol 5's out-of-band buffer
        # without first copying it into a Python bytes object.
        return memoryview(record.serialize())
    except (pa.ArrowException, OSError, ValueError, TypeError) as exc:
        raise ValueError("PROTOCOL: cannot serialize Arrow record batch") from exc


def _deserialize_record_batch(data: bytes) -> pa.RecordBatch:
    if isinstance(data, pickle.PickleBuffer):
        data = data.raw()
    if not isinstance(data, (bytes, bytearray, memoryview)) or not data:
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


def _deserialize_record_batch_message(data: Any, schema: pa.Schema) -> pa.RecordBatch:
    """Decode one schema-free Arrow IPC record-batch message.

    The schema is derived from the already validated routine descriptor.  This
    is an internal handler IPC format, not a replacement for the Flight
    schema, which remains validated independently at the worker boundary.
    """
    if isinstance(data, pickle.PickleBuffer):
        data = data.raw()
    if not isinstance(data, (bytes, bytearray, memoryview)) or not data:
        raise ValueError("PROTOCOL: execution payload is missing an Arrow batch")
    if not isinstance(schema, pa.Schema):
        raise ValueError("PROTOCOL: execution payload schema is invalid")
    try:
        reader = pa.BufferReader(pa.py_buffer(data))
        record = pa.ipc.read_record_batch(reader, schema)
        if reader.tell() != reader.size():
            raise ValueError("trailing bytes after Arrow record batch")
        return record
    except (pa.ArrowException, OSError, ValueError, TypeError) as exc:
        raise ValueError("PROTOCOL: execution payload is not a valid Arrow record batch") from exc


def _execute_handler_batch(
    request: Dict[str, Any],
    *,
    handler=None,
    statement_context: Optional[StatementContext] = None,
    input_schema: Optional[pa.Schema] = None,
    result_schema: Optional[pa.Schema] = None,
) -> bytes:
    args = request["args"]
    result_descriptor = request["return"]
    arrow_encoding = request.get("arrow_encoding", HANDLER_ARROW_STREAM)
    if arrow_encoding == HANDLER_ARROW_RECORD_BATCH:
        if input_schema is None:
            input_schema = _schema_from_descriptors(args, "arg")
        batch = _deserialize_record_batch_message(
            request["input"], input_schema
        )
    elif arrow_encoding == HANDLER_ARROW_STREAM:
        batch = _deserialize_record_batch(request["input"])
    else:
        raise ValueError("PROTOCOL: unsupported handler Arrow encoding")
    mode = request["mode"]
    null_policy = request["null_policy"]
    sdk_version = request["sdk_version"]
    if statement_context is None:
        statement_context = _statement_context(request.get("context"))

    # User code has no stdout/stderr channel in the Flight protocol.  Discard
    # it so a print() cannot corrupt the length-prefixed response frame.
    with contextlib.redirect_stdout(_DiscardText()), contextlib.redirect_stderr(_DiscardText()):
        if handler is None:
            handler = _load_handler(request["source"], request["handler"])
        if mode == MODE_VECTOR:
            call_context = VectorContext(sdk_version, _CallLogger(), statement_context, batch.num_rows)
            output = handler(call_context, *[batch.column(index) for index in range(batch.num_columns)])
            if not isinstance(output, (pa.Array, pa.ChunkedArray)):
                raise ValueError("TYPE_CONTRACT: VECTOR handler must return an Arrow Array or ChunkedArray")
        else:
            call_context = ScalarContext(sdk_version, _CallLogger(), statement_context)
            values = []
            if batch.num_rows < SCALAR_MATERIALIZATION_MIN_ROWS:
                for row in range(batch.num_rows):
                    params = [
                        _scalar_input(batch.column(index), row, args[index])
                        for index in range(batch.num_columns)
                    ]
                    if null_policy == NULL_RETURN and any(value is None for value in params):
                        values.append(None)
                    else:
                        values.append(handler(call_context, *params))
            else:
                columns = [batch.column(index) for index in range(batch.num_columns)]
                scalar_columns = [
                    _scalar_column_values(column) for column in columns
                ]
                if not any(column_values is not None for column_values in scalar_columns):
                    for row in range(batch.num_rows):
                        params = [
                            _scalar_input(columns[index], row, args[index])
                            for index in range(batch.num_columns)
                        ]
                        if null_policy == NULL_RETURN and any(value is None for value in params):
                            values.append(None)
                        else:
                            values.append(handler(call_context, *params))
                else:
                    for row in range(batch.num_rows):
                        params = [
                            _scalar_input_value(
                                column_values[row]
                                if column_values is not None
                                else columns[index][row].as_py(),
                                args[index],
                            )
                            for index, column_values in enumerate(scalar_columns)
                        ]
                        if null_policy == NULL_RETURN and any(value is None for value in params):
                            values.append(None)
                        else:
                            values.append(handler(call_context, *params))
            output = values
        output_array = _output_array(output, result_descriptor, batch.num_rows)
        if result_schema is None:
            result_schema = pa.schema([_field("result", result_descriptor)])
        result_batch = pa.RecordBatch.from_arrays(
            [output_array], schema=result_schema
        )
        if pa.ipc.get_record_batch_size(result_batch) > request["max_batch_bytes"]:
            raise ValueError("RESOURCE_EXHAUSTED: output batch exceeds byte limit")
        if arrow_encoding == HANDLER_ARROW_RECORD_BATCH:
            return _serialize_record_batch_message(result_batch)
        return _serialize_record_batch(result_batch)


def _read_exact(stream, size: int) -> bytearray:
    if size < 0:
        raise ValueError("execution frame read size is negative")
    result = bytearray(size)
    readinto = getattr(stream, "readinto", None)
    if callable(readinto):
        view = memoryview(result)
        offset = 0
        try:
            while offset < size:
                count = readinto(view[offset:])
                if not isinstance(count, int) or count <= 0:
                    raise EOFError("execution frame ended unexpectedly")
                offset += count
        finally:
            view.release()
        return result

    # Keep compatibility with the small stream doubles used by the contract
    # tests and with file-like readers that expose only read().
    offset = 0
    while offset < size:
        chunk = stream.read(size - offset)
        if not chunk:
            raise EOFError("execution frame ended unexpectedly")
        result[offset : offset + len(chunk)] = chunk
        offset += len(chunk)
    return result


def _write_all(stream, data) -> None:
    """Write one frame component completely to a byte stream.

    The handler response channel is a pipe-backed ``FileIO`` object.  A
    blocking write normally transfers all bytes, but the file API still
    permits a short write (for example when a signal interrupts a large
    transfer).  Treating that count as success would publish a truncated
    length-prefixed frame and desynchronize every later response.
    """
    view = memoryview(data)
    try:
        offset = 0
        while offset < len(view):
            written = stream.write(view[offset:])
            if not isinstance(written, int) or written <= 0 or written > len(view) - offset:
                raise OSError("execution frame stream made no valid progress")
            offset += written
    finally:
        view.release()


def _write_execution_frame_parts(stream, parts: Iterable[bytes]) -> None:
    parts = tuple(parts)
    payload_size = sum(len(part) for part in parts)
    if payload_size > MAX_EXECUTION_FRAME_BYTES:
        raise ValueError("RESOURCE_EXHAUSTED: execution frame is too large")
    _write_all(stream, struct.pack(">Q", payload_size))
    for part in parts:
        _write_all(stream, part)
    stream.flush()


def _write_execution_frame(stream, payload: bytes) -> None:
    _write_execution_frame_parts(stream, (payload,))


def _encode_execution_request(request: Dict[str, Any], *, compact: bool = False):
    """Build a handler frame without copying Arrow bytes into pickle.

    The outer length bounds the complete frame.  The first eight bytes of the
    payload bound the pickle metadata, and protocol 5 carries exactly one
    out-of-band Arrow buffer.  Returning write parts lets the nonblocking
    sender avoid concatenating another full request-sized byte string.
    """
    if not isinstance(request, dict) or "input" not in request:
        raise ValueError("PROTOCOL: execution request is missing an Arrow batch")
    input_wire = request["input"]
    if isinstance(input_wire, pickle.PickleBuffer):
        input_wire = input_wire.raw()
    if not isinstance(input_wire, (bytes, bytearray, memoryview)):
        raise ValueError("PROTOCOL: execution request Arrow batch is not bytes-like")
    if not input_wire:
        raise ValueError("PROTOCOL: execution request Arrow batch is empty")
    if compact:
        metadata = {_HANDLER_REQUEST_KIND_KEY: _HANDLER_REQUEST_BATCH}
    else:
        metadata = dict(request)
        metadata[_HANDLER_REQUEST_KIND_KEY] = _HANDLER_REQUEST_FULL
    buffers = []
    try:
        metadata["input"] = pickle.PickleBuffer(input_wire)
        metadata_wire = pickle.dumps(
            metadata, protocol=5, buffer_callback=buffers.append
        )
        if len(buffers) != 1:
            raise ValueError("PROTOCOL: execution request must contain one Arrow buffer")
        arrow_wire = buffers[0].raw()
    except (BufferError, pickle.PickleError, TypeError, ValueError) as exc:
        raise ValueError("PROTOCOL: cannot encode execution request") from exc
    payload_size = 8 + len(metadata_wire) + len(arrow_wire)
    if payload_size > MAX_EXECUTION_FRAME_BYTES:
        raise ValueError("RESOURCE_EXHAUSTED: execution request is too large")
    parts = (
        struct.pack(">Q", payload_size),
        struct.pack(">Q", len(metadata_wire)),
        metadata_wire,
        arrow_wire,
    )
    return parts, payload_size


def _read_execution_request(stream) -> Optional[Dict[str, Any]]:
    """Read and validate one metadata plus out-of-band Arrow frame."""
    # A pipe is a byte stream: one read is allowed to return a prefix even
    # when the sender has already written the complete frame.  Read one byte
    # first so only an empty read means clean burst EOF, then complete the
    # fixed-width header through the same exact-read path as the body.
    first = stream.read(1)
    if not first:
        return None
    if len(first) != 1:
        raise EOFError("execution request header ended unexpectedly")
    header = bytearray(8)
    header[0] = first[0]
    header[1:] = _read_exact(stream, 7)
    payload_size = struct.unpack(">Q", header)[0]
    if payload_size > MAX_EXECUTION_FRAME_BYTES or payload_size < 8:
        raise ValueError("RESOURCE_EXHAUSTED: execution request is too large")
    metadata_size = struct.unpack(">Q", _read_exact(stream, 8))[0]
    if metadata_size > payload_size - 8:
        raise ValueError("PROTOCOL: execution request metadata is too large")
    metadata_wire = _read_exact(stream, metadata_size)
    arrow_size = payload_size - 8 - metadata_size
    if arrow_size <= 0:
        raise ValueError("PROTOCOL: execution request Arrow batch is empty")
    arrow_wire = _read_exact(stream, arrow_size)
    try:
        request = pickle.loads(
            metadata_wire,
            # A readonly memoryview preserves the protocol-5 PickleBuffer
            # type when the exact-read backing is a bytearray, while keeping
            # the Arrow payload zero-copy and preventing handler code from
            # mutating the receive buffer through the request object.
            buffers=[pickle.PickleBuffer(memoryview(arrow_wire).toreadonly())],
        )
    except (EOFError, pickle.PickleError, TypeError, ValueError) as exc:
        raise ValueError("PROTOCOL: execution request metadata is invalid") from exc
    if not isinstance(request, dict) or not isinstance(
        request.get("input"), pickle.PickleBuffer
    ):
        raise ValueError("PROTOCOL: execution request does not contain one Arrow buffer")
    return request


def _watch_parent_liveness(read_fd: int) -> None:
    """Terminate the whole handler group when the worker disappears.

    The worker cannot run its normal cleanup after SIGKILL/OOM.  A pipe whose
    write end is owned by the worker gives the handler a kernel-owned death
    notification, including the race where the worker exits before the
    handler has finished starting.  This is a reliability owner for ordinary
    handler processes; it is deliberately not described as a security
    sandbox, because unisolated user code can inspect or interfere with its
    own process.
    """
    try:
        while True:
            if os.read(read_fd, 1) == b"":
                if os.name == "posix":
                    try:
                        os.killpg(os.getpgrp(), signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                os._exit(137)
    except (OSError, ValueError):
        # A broken watch descriptor is also a lost parent notification.  The
        # handler must terminate its whole process group before leaving; an
        # ordinary thread exit would leave descendants alive after the worker
        # has already lost ownership of them.
        if os.name == "posix":
            try:
                os.killpg(os.getpgrp(), signal.SIGKILL)
            except ProcessLookupError:
                pass
        os._exit(137)


def _execute_handler_subprocess() -> None:
    response_fd_text = os.environ.pop(_HANDLER_RESPONSE_FD_ENV, None)
    parent_watch_fd_text = os.environ.pop(_HANDLER_PARENT_WATCH_FD_ENV, None)
    if response_fd_text is None or parent_watch_fd_text is None:
        raise ValueError("PROTOCOL: handler response channel is missing")
    try:
        response_fd = int(response_fd_text)
        parent_watch_fd = int(parent_watch_fd_text)
        response_stream = os.fdopen(response_fd, "wb", buffering=0, closefd=True)
        threading.Thread(
            target=_watch_parent_liveness,
            args=(parent_watch_fd,),
            name="matrixone-udf-parent-watch",
            daemon=True,
        ).start()
    except (OSError, TypeError, ValueError) as exc:
        raise ValueError("PROTOCOL: handler response channel is invalid") from exc
    try:
        # A child is reused only within one bounded invocation burst.  The
        # source is compiled once and every later request must carry the same
        # frozen contract; input Arrow batches remain independently validated
        # by the parent and by _execute_handler_batch.
        frozen = None
        frozen_request = None
        handler = None
        statement_context = None
        input_schema = None
        result_schema = None
        while True:
            try:
                request = _read_execution_request(sys.stdin.buffer)
                if request is None:
                    # EOF before a new frame is the normal burst shutdown.
                    break
                request_kind = request.get(_HANDLER_REQUEST_KIND_KEY)
                if request_kind not in (
                    _HANDLER_REQUEST_FULL,
                    _HANDLER_REQUEST_BATCH,
                ):
                    raise ValueError("PROTOCOL: handler request kind is invalid")
                if request_kind == _HANDLER_REQUEST_BATCH:
                    if frozen is None or frozen_request is None:
                        raise ValueError(
                            "PROTOCOL: compact handler request arrived before the full contract"
                        )
                    batch_input = request.get("input")
                    request = dict(frozen_request)
                    request["input"] = batch_input
                contract = {
                    key: request.get(key)
                    for key in (
                        "source", "handler", "mode", "null_policy", "abi_contract",
                        "adapter_version", "sdk_version", "definition_schema_version",
                        "artifact_digest", "environment_digest", "definition_fingerprint",
                        "context", "args", "return", "max_batch_bytes", "max_batch_rows",
                        "max_invocation_rows", "max_invocation_result_bytes", "arrow_encoding",
                    )
                }
                if frozen is None:
                    frozen = contract
                    # Do not retain the first Arrow batch while the child is
                    # reused. The immutable contract is enough to reconstruct
                    # later compact requests.
                    frozen_request = {
                        key: value for key, value in request.items() if key != "input"
                    }
                    statement_context = _statement_context(request.get("context"))
                    handler = _load_handler(request["source"], request["handler"])
                    if request.get("arrow_encoding") == HANDLER_ARROW_RECORD_BATCH:
                        input_schema = _schema_from_descriptors(request["args"], "arg")
                        result_schema = pa.schema([_field("result", request["return"])])
                elif contract != frozen:
                    raise ValueError("PROTOCOL: handler burst contract changed")
                output = _execute_handler_batch(
                    request,
                    handler=handler,
                    statement_context=statement_context,
                    input_schema=input_schema,
                    result_schema=result_schema,
                )
                response_parts = (bytes([_HANDLER_RESPONSE_OK]), output)
                succeeded = True
            except Exception as exc:
                response_parts = (
                    bytes([_HANDLER_RESPONSE_ERROR]),
                    _safe_error(exc).encode("utf-8"),
                )
                succeeded = False
            # This descriptor is created by the adapter and is distinct from
            # process stdout/stderr descriptors available to handler code.
            _write_execution_frame_parts(response_stream, response_parts)
            if not succeeded:
                break
    finally:
        response_stream.close()


def _context_is_cancelled(context) -> bool:
    if context is None:
        return False
    try:
        return bool(context.is_cancelled())
    except Exception:
        return False


class _ExchangeContext:
    """Combine the Flight cancellation state with server shutdown intent."""

    __slots__ = ("_context", "_shutdown")

    def __init__(self, context, shutdown: threading.Event):
        self._context = context
        self._shutdown = shutdown

    def is_cancelled(self) -> bool:
        return self._shutdown.is_set() or _context_is_cancelled(self._context)


class _ExchangeInputReader:
    """Read Flight input without letting native reader ownership escape.

    The reader passed to a Flight server callback is a PyArrow C++ object whose
    lifetime is owned by the RPC.  It must be read by the callback thread: a
    daemon thread that is still inside ``read_chunk`` when
    ``FlightServerBase.shutdown`` destroys the RPC can dereference a freed
    native reader and crash the worker.  The Flight implementation itself
    interrupts a direct ``read_chunk`` when the RPC is cancelled.  A
    ``FlightServerBase.shutdown`` waits for an open input RPC; it is not a
    cancellation mechanism, so the owner must cancel the client exchange (or
    terminate the worker process) before waiting for server shutdown.

    Some contract tests and non-native adapters expose an explicit
    ``cancel``/``close`` operation but do not have Flight's RPC cancellation
    semantics.  Keep the bounded polling thread only for those cooperative
    readers.  Never use it for a reader from PyArrow's native Flight module.
    """

    _POLL_SECONDS = 0.05

    def __init__(self, reader):
        self._reader = reader
        self._stop = threading.Event()
        self._items = queue.Queue(maxsize=1)
        self._thread = None
        self._native_cancel = self._find_cancel(reader)
        if not self._is_native_flight_reader(reader) and self._native_cancel is not None:
            self._thread = threading.Thread(
                target=self._read_loop,
                name="matrixone-udf-flight-reader",
                daemon=True,
            )
            self._thread.start()

    @staticmethod
    def _is_native_flight_reader(reader):
        # PyArrow exposes the server-side reader as a private extension type
        # (currently MetadataRecordBatchReader).  The module check is kept at
        # this narrow boundary so the execution path does not depend on a
        # particular private class name across supported PyArrow releases.
        return type(reader).__module__.startswith("pyarrow._flight")

    @staticmethod
    def _find_cancel(reader):
        cancel = getattr(reader, "cancel", None)
        if callable(cancel):
            return cancel
        close = getattr(reader, "close", None)
        return close if callable(close) else None

    def _publish(self, kind, value=None):
        item = (kind, value)
        while not self._stop.is_set():
            try:
                self._items.put(item, timeout=self._POLL_SECONDS)
                return True
            except queue.Full:
                continue
        return False

    def _read_loop(self):
        try:
            while not self._stop.is_set():
                try:
                    chunk = self._reader.read_chunk()
                except StopIteration:
                    self._publish("eof")
                    return
                self._publish("chunk", chunk)
        except Exception as exc:
            self._publish("error", exc)

    def _cancel_native_reader(self):
        if self._native_cancel is not None:
            try:
                self._native_cancel()
            except Exception:
                # The RPC may already be tearing down.  The join below still
                # verifies whether a cooperative reader actually returned.
                pass

    def next(self, context):
        if self._thread is None:
            if _context_is_cancelled(context):
                self.cancel()
                raise TimeoutError("DEADLINE_EXCEEDED: input stream cancelled")
            try:
                chunk = self._reader.read_chunk()
            except StopIteration:
                if _context_is_cancelled(context):
                    raise TimeoutError("DEADLINE_EXCEEDED: input stream cancelled")
                raise
            if _context_is_cancelled(context):
                self.cancel()
                raise TimeoutError("DEADLINE_EXCEEDED: input stream cancelled")
            return chunk
        while True:
            if _context_is_cancelled(context):
                self.cancel()
                raise TimeoutError("DEADLINE_EXCEEDED: input stream cancelled")
            try:
                kind, value = self._items.get(timeout=self._POLL_SECONDS)
            except queue.Empty:
                continue
            if kind == "chunk":
                return value
            if kind == "eof":
                raise StopIteration
            raise value

    def cancel(self):
        self._stop.set()
        self._cancel_native_reader()

    def close(self):
        self.cancel()
        if self._thread is None:
            return
        self._thread.join(timeout=1.0)
        if self._thread.is_alive():
            raise ValueError(
                "RESOURCE_EXHAUSTED: Flight input reader did not stop after cancellation"
            )
def _kill_execution_process(process: subprocess.Popen) -> None:
    # Check the process group before reaping the leader.  A handler can exit
    # while a descendant keeps the group alive; polling first would reap the
    # leader and skip killpg, leaving that descendant behind until the
    # asynchronous watchdog happens to run.  Before process.poll() reaps the
    # leader, its PID is still owned by this session and the group identity
    # cannot have been reused.
    if os.name == "posix":
        group_alive = _execution_group_alive(process)
        leader_alive = process.poll() is None
        if group_alive:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except PermissionError:
                # A process-group kill can be denied during a platform or
                # teardown race even though the worker still owns the leader.
                # Kill the Popen child directly so the handler slot is not
                # stranded; the parent-liveness watchdog remains responsible
                # for descendants when the group cannot be addressed here.
                if leader_alive:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                else:
                    raise
        elif leader_alive:
            # The group disappeared in the small race between the liveness
            # probe and poll(); still make sure the owned leader is stopped.
            process.kill()
    elif process.poll() is None:
        process.kill()
    try:
        process.wait(timeout=1.0)
    except subprocess.TimeoutExpired:
        process.kill()
        try:
            process.wait(timeout=1.0)
        except subprocess.TimeoutExpired as exc:
            # The caller retains the handler session, H slot, and invocation
            # ledger in the bounded pending-cleanup owner. Do not let an
            # unbounded reap wait turn a cancellation path into a worker-wide
            # hang; the process group has already received SIGKILL and the
            # failure is surfaced to the owner.
            raise TimeoutError(
                "DEADLINE_EXCEEDED: handler process did not exit after SIGKILL"
            ) from exc


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


DEFAULT_BURST_BATCHES = 64
DEFAULT_BURST_BYTES = 64 << 20
DEFAULT_BURST_SECONDS = 30.0
# A pending cleanup retains an admitted invocation, so it is bounded by the
# same ledger entry budget as active invocations. H is only the bound for
# child processes; empty/ALL-NULL invocations can still own a Flight reader
# without owning a handler child.
MAX_PENDING_CLEANUPS = MAX_LEDGER_ENTRIES
PENDING_HANDLER_CLEANUP_INITIAL_DELAY = 0.05
PENDING_HANDLER_CLEANUP_MAX_DELAY = 1.0


class _HandlerQuotaLease:
    """Idempotent ownership token for one live handler child."""

    def __init__(self, quota, account_id: int, owner_id: str):
        self._quota = quota
        self._account_id = account_id
        self._owner_id = owner_id
        self._released = False
        self._lock = threading.Lock()

    def release(self) -> None:
        with self._lock:
            if self._released:
                return
            self._released = True
        self._quota._release(self._account_id, self._owner_id)


class _HandlerQuota:
    """Non-blocking worker, account, and principal handler budgets.

    The owner key is scoped by account.  Two tenants using the same username
    therefore do not contend for one another's principal budget.  Acquisition
    is deliberately all-or-nothing and never waits while a caller owns an
    input buffer or a process slot.
    """

    def __init__(
        self,
        max_account_handlers: int = MAX_ACCOUNT_HANDLER_PROCESSES,
        max_owner_handlers: int = MAX_OWNER_HANDLER_PROCESSES,
    ):
        if (
            type(max_account_handlers) is not int
            or max_account_handlers <= 0
            or max_account_handlers > MAX_HANDLER_PROCESSES
        ):
            raise ValueError("PROTOCOL: invalid account handler budget")
        if (
            type(max_owner_handlers) is not int
            or max_owner_handlers <= 0
            or max_owner_handlers > max_account_handlers
        ):
            raise ValueError("PROTOCOL: invalid owner handler budget")
        self._max_account_handlers = max_account_handlers
        self._max_owner_handlers = max_owner_handlers
        self._lock = threading.Lock()
        self._account_counts: Dict[int, int] = {}
        self._owner_counts: Dict[tuple[int, str], int] = {}

    def acquire(self, account_id: int, owner_id: str) -> _HandlerQuotaLease:
        if type(account_id) is not int or account_id < 0:
            raise ValueError("PROTOCOL: invalid handler budget account")
        if not isinstance(owner_id, str) or not owner_id:
            raise ValueError("PROTOCOL: invalid handler budget owner")
        owner_key = (account_id, owner_id)
        with self._lock:
            account_count = self._account_counts.get(account_id, 0)
            if account_count >= self._max_account_handlers:
                raise ValueError("RESOURCE_EXHAUSTED: account handler budget is full")
            owner_count = self._owner_counts.get(owner_key, 0)
            if owner_count >= self._max_owner_handlers:
                raise ValueError("RESOURCE_EXHAUSTED: owner handler budget is full")
            self._account_counts[account_id] = account_count + 1
            self._owner_counts[owner_key] = owner_count + 1
        return _HandlerQuotaLease(self, account_id, owner_id)

    def _release(self, account_id: int, owner_id: str) -> None:
        owner_key = (account_id, owner_id)
        with self._lock:
            account_count = self._account_counts.get(account_id, 0)
            owner_count = self._owner_counts.get(owner_key, 0)
            if account_count <= 0 or owner_count <= 0:
                raise RuntimeError("handler quota release without ownership")
            if account_count == 1:
                del self._account_counts[account_id]
            else:
                self._account_counts[account_id] = account_count - 1
            if owner_count == 1:
                del self._owner_counts[owner_key]
            else:
                self._owner_counts[owner_key] = owner_count - 1

    def counts(self) -> tuple[Dict[int, int], Dict[tuple[int, str], int]]:
        """Return copies for deterministic tests and bounded diagnostics."""
        with self._lock:
            return dict(self._account_counts), dict(self._owner_counts)


class _HandlerProcessSession:
    """One handler child reused only inside a bounded invocation burst."""

    def __init__(
        self,
        execution_slots: Optional[threading.BoundedSemaphore] = None,
        *,
        handler_quota: Optional[_HandlerQuota] = None,
        account_id: int = 0,
        owner_id: str = "__anonymous__",
    ):
        self._slots = execution_slots or _DEFAULT_HANDLER_SLOTS
        self._slot_acquired = False
        self._quota_lease = None
        self._process = None
        self._watchdog_process = None
        self._selector = None
        self._response_read_fd = -1
        self._parent_watch_write_fd = -1
        self._response_buffer = bytearray()
        self._closed = False
        self._close_lock = threading.Lock()
        self._burst_started = time.monotonic()
        self._burst_batches = 0
        self._burst_bytes = 0
        try:
            if handler_quota is not None:
                self._quota_lease = handler_quota.acquire(account_id, owner_id)
            if not self._slots.acquire(blocking=False):
                raise ValueError("RESOURCE_EXHAUSTED: handler execution slots are full")
            self._slot_acquired = True
            response_read_fd, response_write_fd = os.pipe()
            # Publish each acquired descriptor to its cleanup owner before
            # the next allocation can fail (in particular under FD pressure).
            self._response_read_fd = response_read_fd
            parent_watch_read_fd, parent_watch_write_fd = os.pipe()
            self._parent_watch_write_fd = parent_watch_write_fd
            watchdog_read_fd = os.dup(parent_watch_read_fd)
            popen_kwargs = {
                "stdin": subprocess.PIPE,
                # Handler output is diagnostic-only.  It must never share the
                # adapter response channel, even when user code writes directly
                # to file descriptor 1.
                "stdout": subprocess.DEVNULL,
                "stderr": subprocess.DEVNULL,
                "close_fds": True,
            }
            if os.name == "posix":
                popen_kwargs["start_new_session"] = True
                popen_kwargs["pass_fds"] = (response_write_fd, parent_watch_read_fd)
            # User code runs under a separate execution identity.  In
            # particular, do not inherit CN, object-store, database, or tenant
            # credentials from the worker process.  This is process hygiene,
            # not a sandbox boundary.
            child_env = {
                name: value
                for name, value in os.environ.items()
                if name in _HANDLER_ENV_ALLOWLIST
            }
            child_env[_HANDLER_RESPONSE_FD_ENV] = str(response_write_fd)
            child_env[_HANDLER_PARENT_WATCH_FD_ENV] = str(parent_watch_read_fd)
            popen_kwargs["env"] = child_env
            self._process = subprocess.Popen(
                [sys.executable, os.path.abspath(__file__), "--execute-handler"],
                **popen_kwargs,
            )
            os.close(response_write_fd)
            response_write_fd = -1
            os.close(parent_watch_read_fd)
            parent_watch_read_fd = -1
            if os.name == "posix":
                watchdog_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), "watchdog.py"
                )
                watchdog_env = {
                    name: value
                    for name, value in os.environ.items()
                    if name in _HANDLER_ENV_ALLOWLIST
                }
                self._watchdog_process = subprocess.Popen(
                    [
                        sys.executable,
                        # The watchdog is a standard-library-only helper;
                        # skipping site-package discovery trims its startup
                        # without changing the handler's Python environment.
                        "-S",
                        watchdog_path,
                        str(watchdog_read_fd),
                        str(self._process.pid),
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    close_fds=True,
                    env=watchdog_env,
                    start_new_session=True,
                    pass_fds=(watchdog_read_fd,),
                )
            os.close(watchdog_read_fd)
            watchdog_read_fd = -1
            self._selector = selectors.DefaultSelector()
            stdin_fd = self._process.stdin.fileno()
            os.set_blocking(stdin_fd, False)
            os.set_blocking(response_read_fd, False)
            self._selector.register(response_read_fd, selectors.EVENT_READ, "response")
        except Exception:
            for fd in (
                locals().get("response_write_fd", -1),
                locals().get("parent_watch_read_fd", -1),
                locals().get("watchdog_read_fd", -1),
            ):
                if fd >= 0:
                    try:
                        os.close(fd)
                    except OSError:
                        pass
            # Preserve the construction failure.  Cleanup is best effort and
            # must not hide the contract error that caused session creation
            # to fail.
            try:
                self.close()
            except Exception:
                pass
            raise

    @property
    def should_rollover(self) -> bool:
        return (
            self._burst_batches >= DEFAULT_BURST_BATCHES
            or self._burst_bytes >= DEFAULT_BURST_BYTES
            or time.monotonic() - self._burst_started >= DEFAULT_BURST_SECONDS
        )

    def _take_response(self) -> Optional[tuple[int, bytes]]:
        if len(self._response_buffer) < 8:
            return None
        expected = struct.unpack(">Q", self._response_buffer[:8])[0]
        if expected > MAX_EXECUTION_FRAME_BYTES:
            raise ValueError("RESOURCE_EXHAUSTED: execution response is too large")
        if len(self._response_buffer) < expected + 8:
            return None
        if expected < 1:
            raise ValueError("PROTOCOL: handler process returned an empty response")
        status = self._response_buffer[8]
        # A memoryview avoids an intermediate bytearray slice. Release it
        # before resizing the receive buffer below; the returned Arrow bytes
        # remain the only copy needed by the parent decoder.
        response_view = memoryview(self._response_buffer)
        try:
            payload = bytes(response_view[9 : expected + 8])
        finally:
            response_view.release()
        del self._response_buffer[: expected + 8]
        return status, payload

    def run(self, context, request: Dict[str, Any], timeout_seconds: float) -> bytes:
        if self._closed or self._process is None or self._selector is None:
            raise ValueError("PROTOCOL: handler session is closed")
        # The handler budget is an absolute deadline for the whole request,
        # including serialization and the non-blocking write. Starting the
        # clock after pickle.dumps would let a large request consume an
        # unbounded part of the caller's budget before the deadline was even
        # installed.
        deadline = time.monotonic() + timeout_seconds
        request_parts, request_size = _encode_execution_request(
            request, compact=self._burst_batches > 0
        )

        def check_budget() -> None:
            if _context_is_cancelled(context):
                raise TimeoutError("DEADLINE_EXCEEDED: handler execution cancelled")
            if time.monotonic() >= deadline:
                raise TimeoutError("DEADLINE_EXCEEDED: handler execution timeout")

        check_budget()
        request_part_index = 0
        request_part_offset = 0
        stdin_fd = self._process.stdin.fileno()
        self._selector.register(stdin_fd, selectors.EVENT_WRITE, "request")
        try:
            while True:
                check_budget()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("DEADLINE_EXCEEDED: handler execution timeout")
                response = self._take_response()
                if response is not None:
                    break
                events = self._selector.select(min(remaining, 0.1))
                for event, _ in events:
                    if event.data == "request":
                        try:
                            while request_part_index < len(request_parts):
                                # A single writable notification can drain many
                                # small frame parts. Re-check the caller budget
                                # inside that loop so serialization/write work
                                # cannot outrun cancellation or the deadline.
                                check_budget()
                                part = request_parts[request_part_index]
                                if request_part_offset == len(part):
                                    request_part_index += 1
                                    request_part_offset = 0
                                    continue
                                written = os.write(stdin_fd, part[request_part_offset:])
                                if written <= 0:
                                    raise BrokenPipeError("handler request channel made no progress")
                                request_part_offset += written
                                check_budget()
                                if request_part_offset < len(part):
                                    break
                        except BlockingIOError:
                            continue
                        except BrokenPipeError as exc:
                            raise ValueError(
                                "USER_CODE: handler process closed its request channel"
                            ) from exc
                        if request_part_index == len(request_parts):
                            self._selector.unregister(stdin_fd)
                    else:
                        try:
                            chunk = os.read(self._response_read_fd, 65536)
                        except BlockingIOError:
                            continue
                        if not chunk:
                            raise ValueError("USER_CODE: handler process exited without a response")
                        self._response_buffer.extend(chunk)
                if self._process.poll() is not None:
                    if self._process.returncode != 0:
                        raise ValueError("USER_CODE: handler process exited abnormally")
                    if _execution_group_alive(self._process):
                        raise ValueError("USER_CODE: handler process left descendant processes")
                    raise ValueError("USER_CODE: handler process exited before the burst completed")
            status, output = response
            if status == _HANDLER_RESPONSE_ERROR:
                try:
                    message = output.decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise ValueError("PROTOCOL: handler process returned an invalid error") from exc
                raise ValueError(message or "USER_CODE: handler process failed")
            if status != _HANDLER_RESPONSE_OK:
                raise ValueError("PROTOCOL: handler process returned an unknown status")
            self._burst_batches += 1
            self._burst_bytes += request_size + len(output) + 1
            return output
        finally:
            # A failed write/read must not leave a stale registration that a
            # later close or diagnostic path mistakes for active work.
            try:
                self._selector.unregister(stdin_fd)
            except (KeyError, ValueError):
                pass

    def close(self) -> None:
        # Mark the session closed before taking the lock so no new batch can
        # enter while another terminal path is retrying cleanup.  Ownership
        # fields are cleared only after their operation succeeds; otherwise a
        # transient kill/close error would make a later cleanup call unable to
        # find the resource it still owns.
        self._closed = True
        with self._close_lock:
            close_error = None

            def attempt(operation, commit) -> None:
                nonlocal close_error
                operation_error = None
                # Process reaping can race a child exiting and descriptor
                # close can transiently fail during RPC teardown.  A bounded
                # second attempt makes cleanup retryable without turning
                # cancellation into an unbounded wait.
                for _ in range(2):
                    try:
                        operation()
                        commit()
                        return
                    except Exception as exc:
                        operation_error = exc
                if close_error is None:
                    close_error = operation_error

            if self._selector is not None:
                selector = self._selector
                attempt(selector.close, lambda: setattr(self, "_selector", None))
            if self._parent_watch_write_fd >= 0:
                parent_watch_write_fd = self._parent_watch_write_fd

                def close_parent_watch() -> None:
                    os.close(parent_watch_write_fd)

                attempt(
                    close_parent_watch,
                    lambda: setattr(self, "_parent_watch_write_fd", -1),
                )
            if self._process is not None:
                process = self._process

                def close_process() -> None:
                    _kill_execution_process(process)
                    if process.stdin is not None:
                        process.stdin.close()

                attempt(close_process, lambda: setattr(self, "_process", None))
            if self._watchdog_process is not None:
                watchdog_process = self._watchdog_process

                def wait_for_watchdog() -> None:
                    try:
                        watchdog_process.wait(timeout=0.1)
                    except subprocess.TimeoutExpired:
                        try:
                            watchdog_process.kill()
                        except ProcessLookupError:
                            pass
                        watchdog_process.wait(timeout=1.0)

                attempt(
                    wait_for_watchdog,
                    lambda: setattr(self, "_watchdog_process", None),
                )
            if self._response_read_fd >= 0:
                response_read_fd = self._response_read_fd

                def close_response_read() -> None:
                    os.close(response_read_fd)

                attempt(
                    close_response_read,
                    lambda: setattr(self, "_response_read_fd", -1),
                )
            # H and the account/owner quota describe a live handler child.  A
            # failed kill must therefore keep both leases held while this
            # session is owned by the pending-cleanup reaper.  Releasing them
            # merely because the close attempt returned an error would let a
            # later invocation exceed the real process budget.  The fields are
            # cleared only after every process/IPC owner has been cleared.
            resources_closed = (
                self._selector is None
                and self._process is None
                and self._watchdog_process is None
                and self._response_read_fd < 0
                and self._parent_watch_write_fd < 0
            )
            if resources_closed and self._slot_acquired:
                attempt(self._slots.release, lambda: setattr(self, "_slot_acquired", False))
            quota_lease = getattr(self, "_quota_lease", None)
            if resources_closed and quota_lease is not None:
                attempt(quota_lease.release, lambda: setattr(self, "_quota_lease", None))
            if close_error is not None:
                raise close_error


# Keep stable class identities for cleanup ownership checks. Contract tests
# replace the module constructors to inject failures; using the mutable module
# attributes in isinstance() would then turn a valid cleanup path into a
# TypeError and could mask the original exchange outcome.
_EXCHANGE_INPUT_READER_TYPE = _ExchangeInputReader
_HANDLER_PROCESS_SESSION_TYPE = _HandlerProcessSession


@dataclass
class _PendingInvocationCleanup:
    input_reader: Optional[_ExchangeInputReader]
    handler_session: Optional[_HandlerProcessSession]
    key: tuple
    state: _InvocationState
    next_attempt: float
    failures: int = 0


def _run_handler_process(
    context, request: Dict[str, Any], timeout_seconds: float,
    execution_slots: Optional[threading.BoundedSemaphore] = None,
) -> bytes:
    session = _HandlerProcessSession(execution_slots)
    try:
        result = session.run(context, request, timeout_seconds)
    except BaseException:
        # Preserve the handler/transport failure as the primary diagnosis.
        # Cleanup is still mandatory, but a close failure must not replace a
        # deadline, user-code, or protocol error and hide the actual cause.
        try:
            session.close()
        except Exception:
            logging.exception("Python UDF handler cleanup failed after an execution error")
        raise
    else:
        session.close()
        return result



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
        self.finish_sent = False
        self.cancelled = False
        self.terminal_outcome: Optional[str] = None

    def input_sequence(self) -> int:
        with self.condition:
            return self.last_input

    def record_input(self, sequence: int) -> None:
        with self.condition:
            if self.terminal_outcome is not None or self.cancelled:
                raise ValueError("PROTOCOL: invocation is already terminal")
            if sequence != self.last_input + 1:
                raise ValueError("PROTOCOL: input sequence is not contiguous")
            self.last_input = sequence
            self.condition.notify_all()

    def sequences(self) -> tuple:
        with self.condition:
            return self.last_input, self.last_result, self.acked_result

    def record_result(self, sequence: int) -> None:
        with self.condition:
            if self.terminal_outcome is not None or self.cancelled:
                raise ValueError("PROTOCOL: invocation is already terminal")
            if sequence != self.last_result + 1 or sequence > self.last_input:
                raise ValueError("PROTOCOL: result sequence is not contiguous")
            self.last_result = sequence
            self.condition.notify_all()

    def terminal_snapshot(self) -> tuple:
        with self.condition:
            outcome = self.terminal_outcome
            if outcome is None:
                if self.finish_acked:
                    outcome = _TERMINAL_SUCCESS
                elif self.finish_sent:
                    outcome = _TERMINAL_FINISH_UNCONFIRMED
                elif self.cancelled:
                    outcome = _TERMINAL_CANCELLED
                else:
                    outcome = _TERMINAL_FAILED
            return self.last_result, self.finish_id, outcome

    def mark_finish_sent(self, finish_id: str) -> str:
        with self.condition:
            if self.terminal_outcome is not None or self.cancelled:
                raise ValueError("PROTOCOL: invocation is already terminal")
            if self.finish_sent or not finish_id:
                raise ValueError("PROTOCOL: Finish was already sent")
            self.finish_id = finish_id
            self.finish_sent = True
            return finish_id

    def freeze_terminal(self) -> str:
        """Linearize the outcome before the active entry is removed.

        A Finish identifier only proves that the worker emitted Finish.  It
        does not prove that the peer accepted AcknowledgeFinish.  The latter
        is the condition for SUCCESS; otherwise a sent Finish is frozen as
        FINISH_UNCONFIRMED and cannot be promoted by a late action.
        """
        with self.condition:
            if self.terminal_outcome is None:
                if self.finish_acked:
                    self.terminal_outcome = _TERMINAL_SUCCESS
                elif self.finish_sent:
                    self.terminal_outcome = _TERMINAL_FINISH_UNCONFIRMED
                elif self.cancelled:
                    self.terminal_outcome = _TERMINAL_CANCELLED
                else:
                    self.terminal_outcome = _TERMINAL_FAILED
            return self.terminal_outcome

    def mark_cancelled(self) -> None:
        """Freeze cancellation intent before the active lease is removed.

        Cancellation is recorded on the invocation state rather than inferred
        from cleanup timing.  This lets a concurrent ACK observe the same
        terminal gate: an already accepted Finish ACK still wins, while a
        result or Finish ACK arriving after cancellation cannot advance the
        invocation.
        """
        with self.condition:
            if self.terminal_outcome is None:
                # An accepted Finish ACK is the terminal proof.  Cancellation
                # may race the transport teardown after that ACK, but it must
                # not overwrite SUCCESS or make a later cleanup publish a
                # contradictory CANCELLED tombstone.
                if self.finish_acked:
                    return
                self.cancelled = True
                self.condition.notify_all()

    def ack_result(self, sequence: int) -> None:
        with self.condition:
            if self.cancelled:
                raise ValueError("PROTOCOL: invocation terminal outcome is CANCELLED")
            if self.terminal_outcome is not None:
                if self.terminal_outcome != _TERMINAL_SUCCESS:
                    raise ValueError(
                        f"PROTOCOL: invocation terminal outcome is {self.terminal_outcome}"
                    )
                # An action can retain a state reference while cleanup moves
                # that state to the terminal ledger.  Once SUCCESS is frozen,
                # only the final result sequence is an idempotent confirmation;
                # accepting an earlier cumulative ACK would bypass the
                # terminal ledger's exact-fence rule.
                if sequence != self.last_result:
                    raise ValueError(
                        "PROTOCOL: terminal result ACK does not match the completed result"
                    )
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
            if self.terminal_outcome is not None:
                if self.terminal_outcome == _TERMINAL_SUCCESS and finish_id == self.finish_id:
                    return
                raise ValueError(
                    f"PROTOCOL: invocation terminal outcome is {self.terminal_outcome}"
                )
            # Cancellation may race the response after the peer has already
            # accepted this exact Finish ACK.  The accepted acknowledgement
            # is the terminal proof and remains idempotent; a cancellation
            # that won the race before the ACK is still rejected below.
            if self.finish_acked and finish_id == self.finish_id:
                return
            if self.cancelled:
                raise ValueError("PROTOCOL: invocation terminal outcome is CANCELLED")
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


class RoutineFlightServer(_FlightServerBase):
    def __init__(
        self,
        location: str,
        *,
        clock=time.monotonic,
        terminal_ttl_seconds=TERMINAL_TTL_SECONDS,
        lease_epoch=1,
    ):
        if isinstance(lease_epoch, bool) or not isinstance(lease_epoch, int) or lease_epoch <= 0 or lease_epoch > (1 << 64) - 1:
            raise ValueError("PROTOCOL: invalid worker lease epoch")
        super().__init__(location)
        self._lock = threading.RLock()
        self._active: Dict[tuple, _InvocationState] = {}
        self._terminal: OrderedDict[tuple, _TerminalRecord] = OrderedDict()
        self._terminal_bytes = 0
        self._active_bytes = 0
        # A group id is scoped by the account in the fencing tuple. Keeping
        # only the textual id here would let one tenant block or close a
        # same-named group owned by another tenant.
        self._active_groups: Dict[tuple, int] = {}
        self._closed_groups: Dict[tuple, int] = {}
        self._closed_group_bytes = 0
        self._reserved_groups: set[tuple] = set()
        self._reserved_group_bytes = 0
        self._clock = clock
        self._terminal_ttl_seconds = terminal_ttl_seconds
        self._lease_epoch = lease_epoch
        # H is independent from the number of admitted Flight invocations K.
        # A rejected acquire fails before the handler process is created and
        # therefore cannot accumulate a hidden Python task queue.
        self._handler_slots = threading.BoundedSemaphore(MAX_HANDLER_PROCESSES)
        self._handler_quota = _HandlerQuota()
        # A Flight reader or handler session remains owned by the Flight
        # server until every resource close has succeeded. This queue is
        # bounded by the active ledger budget, so a transient cleanup failure
        # cannot create an unbounded reaper queue or silently release an
        # invocation while a native reader or child is still live.
        self._pending_cleanup_condition = threading.Condition()
        self._pending_cleanups: Dict[tuple, _PendingInvocationCleanup] = {}
        self._pending_cleanup_thread: Optional[threading.Thread] = None
        self._pending_cleanup_stopping = False
        # FlightServerBase.shutdown waits for active RPC methods to return.
        # Set this before entering that wait so input readers, handler loops,
        # and ACK waits have an independent cancellation source during a
        # graceful worker shutdown.
        self._shutdown_event = threading.Event()

    @staticmethod
    def _entry_bytes(key: tuple) -> int:
        # The admission estimate is deliberately conservative.  The tuple is
        # already bounded by MAX_CONTROL_BYTES, and its serialized size is
        # stable for the lifetime of the fence.
        encoded = json.dumps(key, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        return len(encoded) + 128

    @staticmethod
    def _group_key(key: tuple) -> tuple:
        """Return the account-scoped identity of a group epoch."""
        return key[0], key[2]

    @staticmethod
    def _group_fence_bytes(group_key: tuple) -> int:
        # Account id is a fixed-width uint64 on the wire. Include it in the
        # bounded fence estimate so the capacity accounting matches the
        # account-scoped key rather than only charging the display id.
        return len(group_key[1].encode("utf-8")) + 16

    def _purge_terminal_locked(self, now: float) -> None:
        expired = [
            key
            for key, record in self._terminal.items()
            if record.expires_at <= now
            and self._closed_groups.get(self._group_key(key), 0) >= key[3]
        ]
        for key in expired:
            record = self._terminal.pop(key)
            self._terminal_bytes -= record.bytes

    def _admit(self, key: tuple) -> _InvocationState:
        size = self._entry_bytes(key)
        group_key, group_epoch = self._group_key(key), key[3]
        with self._lock:
            if self._closed_groups.get(group_key, 0) >= group_epoch:
                raise ValueError("PROTOCOL: execution group epoch is already closed")
            self._purge_terminal_locked(self._clock())
            if key in self._active:
                raise ValueError("PROTOCOL: invocation fence is active")
            if key in self._terminal:
                raise ValueError("PROTOCOL: invocation fence is terminal")
            if group_key in self._active_groups:
                raise ValueError("PROTOCOL: execution group epoch is already active")
            if len(self._active) + len(self._terminal) >= MAX_LEDGER_ENTRIES:
                raise ValueError("RESOURCE_EXHAUSTED: terminal ledger entries are full")
            if self._active_bytes + self._terminal_bytes + size > MAX_LEDGER_BYTES:
                raise ValueError("RESOURCE_EXHAUSTED: terminal ledger bytes are full")
            if group_key not in self._closed_groups and group_key not in self._reserved_groups:
                fence_bytes = self._group_fence_bytes(group_key)
                if (
                    len(self._closed_groups) + len(self._reserved_groups)
                    >= MAX_CLOSED_GROUP_ENTRIES
                    or self._closed_group_bytes + self._reserved_group_bytes + fence_bytes
                    > MAX_CLOSED_GROUP_BYTES
                ):
                    raise ValueError("RESOURCE_EXHAUSTED: closed-group fence is full")
                # Reserve the future tombstone before creating the handler.
                # Terminal cleanup converts this reservation into the
                # closed-generation fence.
                self._reserved_groups.add(group_key)
                self._reserved_group_bytes += fence_bytes
            state = _InvocationState({}, size)
            self._active[key] = state
            self._active_bytes += size
            self._active_groups[group_key] = group_epoch
            return state

    def _require_current_lease(self, key: tuple) -> None:
        if key[5] != self._lease_epoch:
            raise ValueError(
                "STALE_LEASE_EPOCH: invocation belongs to a different worker instance"
            )

    def _remember_terminal_locked(self, key: tuple, state: _InvocationState) -> None:
        deadline = self._clock() + self._terminal_ttl_seconds
        last_result, finish_id, outcome = state.terminal_snapshot()
        self._terminal[key] = _TerminalRecord(
            deadline, state.terminal_bytes, last_result, finish_id, outcome
        )
        self._terminal.move_to_end(key)
        self._terminal_bytes += state.terminal_bytes

    def _close_group_epoch_locked(self, key: tuple) -> None:
        group_key, group_epoch = self._group_key(key), key[3]
        current = self._closed_groups.get(group_key, 0)
        if group_epoch <= current:
            return
        if group_key not in self._closed_groups:
            fence_bytes = self._group_fence_bytes(group_key)
            if group_key in self._reserved_groups:
                self._reserved_groups.remove(group_key)
                self._reserved_group_bytes -= fence_bytes
            elif (
                len(self._closed_groups) >= MAX_CLOSED_GROUP_ENTRIES
                or self._closed_group_bytes + fence_bytes > MAX_CLOSED_GROUP_BYTES
            ):
                # This is only reachable for state restored from an adapter
                # predating group-fence reservations. Keep the terminal record
                # and refuse collection until the fence is reconstructed.
                return
            self._closed_group_bytes += fence_bytes
        self._closed_groups[group_key] = group_epoch

    def _finish_invocation(self, key: tuple, state: Optional[_InvocationState]) -> None:
        # A rejected duplicate Open has no ownership of the existing state.
        # Cleanup must therefore be identity based, not key based.
        if key is None or state is None:
            return
        state.freeze_terminal()
        with self._lock:
            current = self._active.get(key)
            if current is not state:
                return
            self._active.pop(key, None)
            self._active_bytes -= state.terminal_bytes
            group_key, group_epoch = self._group_key(key), key[3]
            if self._active_groups.get(group_key) == group_epoch:
                self._active_groups.pop(group_key, None)
            # A started invocation is terminal even when the worker reports an
            # error.  Retaining the fence prevents a late retry from running
            # user code a second time.
            self._remember_terminal_locked(key, state)
            # The current Gateway contract uses one member per group. The
            # worker records that owner closure before allowing TTL collection;
            # a future multi-member scheduler must send an explicit group
            # closure before reusing this hook.
            self._close_group_epoch_locked(key)

    def _ensure_pending_cleanup_thread_locked(self) -> None:
        thread = self._pending_cleanup_thread
        if thread is not None and thread.is_alive():
            return
        thread = threading.Thread(
            target=self._pending_cleanup_loop,
            name="matrixone-python-udf-cleanup",
            daemon=True,
        )
        self._pending_cleanup_thread = thread
        thread.start()

    def _retain_pending_cleanup(
        self,
        input_reader: Optional[_ExchangeInputReader],
        handler_session: Optional[_HandlerProcessSession],
        key: Optional[tuple],
        state: Optional[_InvocationState],
    ) -> bool:
        """Retain exchange resources until every cleanup operation succeeds."""
        if key is None or state is None or (input_reader is None and handler_session is None):
            return False
        with self._pending_cleanup_condition:
            existing = self._pending_cleanups.get(key)
            if existing is not None:
                return True
            if len(self._pending_cleanups) >= MAX_PENDING_CLEANUPS:
                # An admitted invocation must not lose its cleanup owner. A
                # full queue is an adapter invariant violation; keeping the
                # invocation active is safer than pretending ownership was
                # released.
                logging.critical(
                    "Python UDF pending cleanup capacity is full for %r",
                    key,
                )
                return False
            self._pending_cleanups[key] = _PendingInvocationCleanup(
                input_reader=input_reader,
                handler_session=handler_session,
                key=key,
                state=state,
                next_attempt=time.monotonic(),
            )
            self._ensure_pending_cleanup_thread_locked()
            self._pending_cleanup_condition.notify_all()
            return True

    def _pending_cleanup_loop(self) -> None:
        while True:
            with self._pending_cleanup_condition:
                while True:
                    if self._pending_cleanup_stopping and not self._pending_cleanups:
                        return
                    if not self._pending_cleanups:
                        self._pending_cleanup_condition.wait()
                        continue
                    now = time.monotonic()
                    ready = [
                        item for item in self._pending_cleanups.values()
                        if item.next_attempt <= now
                    ]
                    if ready:
                        break
                    wait_for = min(
                        item.next_attempt
                        for item in self._pending_cleanups.values()
                    ) - now
                    self._pending_cleanup_condition.wait(max(0.01, wait_for))

            for item in ready:
                errors = []
                if item.input_reader is not None:
                    try:
                        item.input_reader.close()
                    except Exception as exc:
                        errors.append(exc)
                    else:
                        with self._pending_cleanup_condition:
                            current = self._pending_cleanups.get(item.key)
                            if current is item:
                                item.input_reader = None
                if item.handler_session is not None:
                    try:
                        item.handler_session.close()
                    except Exception as exc:
                        errors.append(exc)
                    else:
                        with self._pending_cleanup_condition:
                            current = self._pending_cleanups.get(item.key)
                            if current is item:
                                item.handler_session = None
                if errors:
                    exc = errors[0]
                    item.failures += 1
                    delay = min(
                        PENDING_HANDLER_CLEANUP_MAX_DELAY,
                        PENDING_HANDLER_CLEANUP_INITIAL_DELAY
                        * (2 ** min(item.failures, 5)),
                    )
                    with self._pending_cleanup_condition:
                        current = self._pending_cleanups.get(item.key)
                        if current is item:
                            item.next_attempt = time.monotonic() + delay
                            self._pending_cleanup_condition.notify_all()
                    logging.error(
                        "Python UDF exchange cleanup retry failed for %r: %s",
                        item.key,
                        exc,
                    )
                    continue

                # The session is now fully closed.  Only then can the
                # invocation owner release K, the ledger entry, and the group
                # fence.  _finish_invocation is idempotent with respect to any
                # late ACK that arrived while the cleanup was pending.
                finish_error = None
                try:
                    self._finish_invocation(item.key, item.state)
                except Exception as exc:
                    finish_error = exc
                with self._pending_cleanup_condition:
                    current = self._pending_cleanups.get(item.key)
                    if current is item and finish_error is None:
                        del self._pending_cleanups[item.key]
                    elif current is item:
                        item.failures += 1
                        item.next_attempt = time.monotonic() + PENDING_HANDLER_CLEANUP_MAX_DELAY
                    self._pending_cleanup_condition.notify_all()
                if finish_error is not None:
                    logging.error(
                        "Python UDF invocation cleanup retry failed for %r: %s",
                        item.key,
                        finish_error,
                    )

    def _stop_pending_cleanup_reaper(self) -> None:
        # The base Flight server may fail while interrupting an RPC. The
        # worker still owns its cleanup reaper in that case, so stopping and
        # joining it cannot depend on the base shutdown returning normally.
        with self._pending_cleanup_condition:
            self._pending_cleanup_stopping = True
            self._pending_cleanup_condition.notify_all()
            thread = self._pending_cleanup_thread
        if thread is not None:
            # Normal shutdown has no pending sessions and joins immediately.
            # A persistent OS/resource failure remains owned by the daemon
            # reaper; the handler's parent-liveness pipe still guarantees
            # child-group cleanup if this worker process exits.
            thread.join(timeout=2.0)
            with self._pending_cleanup_condition:
                pending = len(self._pending_cleanups)
            if pending:
                logging.error(
                    "Python UDF worker shutdown left %d handler cleanups pending",
                    pending,
                )

    def shutdown(self):
        self._shutdown_event.set()
        try:
            return super().shutdown()
        finally:
            self._stop_pending_cleanup_reaper()

    def _cleanup_exchange(
        self,
        key: Optional[tuple],
        state: Optional[_InvocationState],
        input_reader,
        handler_session,
    ) -> Optional[Exception]:
        """Run every exchange cleanup step and return its first error.

        Closing a native Flight reader or a handler process is allowed to
        fail while an RPC is already being cancelled.  Those failures must
        not skip the invocation fence cleanup: otherwise K, handler slots,
        group reservations, and terminal de-duplication can disagree about
        whether the invocation still owns resources.  The caller decides
        whether a cleanup error should replace an existing exchange error.
        """
        cleanup_error: Optional[Exception] = None

        def close_with_retry(resource) -> tuple[bool, Optional[Exception]]:
            last_error = None
            for _ in range(2):
                try:
                    resource.close()
                    return True, None
                except Exception as exc:
                    last_error = exc
            return False, last_error

        input_cleanup_succeeded = input_reader is None
        if input_reader is not None:
            input_cleanup_succeeded, cleanup_error = close_with_retry(input_reader)

        handler_cleanup_succeeded = handler_session is None
        handler_error = None
        if handler_session is not None:
            handler_cleanup_succeeded, handler_error = close_with_retry(handler_session)
            if cleanup_error is None:
                cleanup_error = handler_error

        # Real native readers and process sessions retain ownership when close
        # fails. Test doubles deliberately keep the old direct-cleanup
        # behaviour so the helper tests do not create a false production
        # resource owner.
        pending_reader = (
            input_reader
            if isinstance(input_reader, _EXCHANGE_INPUT_READER_TYPE) and not input_cleanup_succeeded
            else None
        )
        pending_session = (
            handler_session
            if isinstance(handler_session, _HANDLER_PROCESS_SESSION_TYPE) and not handler_cleanup_succeeded
            else None
        )
        if pending_reader is None and pending_session is None:
            try:
                self._finish_invocation(key, state)
            except Exception as exc:
                if cleanup_error is None:
                    cleanup_error = exc
        elif not self._retain_pending_cleanup(pending_reader, pending_session, key, state):
            # Keep the invocation active if no cleanup owner could be
            # registered. Returning the error makes the failure visible and
            # prevents a false terminal record from authorizing a retry.
            if cleanup_error is None:
                cleanup_error = RuntimeError(
                    "Python UDF exchange cleanup owner could not be retained"
                )
        return cleanup_error

    def do_action(self, context, action):
        self._ensure_action_active(context)
        if action.type == "GetPythonCapabilities":
            request = _decode_capability_request(action.body)
            # pyarrow exposes Result as a one positional-buffer value across
            # supported releases; using a keyword here breaks the real Flight
            # path before any routine is opened.
            yield flight.Result(_encode_capabilities(request, self._lease_epoch))
            return
        if action.type == "ValidatePythonDefinition":
            value = _decode_definition_validation(action.body)
            try:
                _validate_definition_syntax(value)
            except Exception as exc:
                # A syntax error is a definition result, not an invocation
                # failure. Returning a bounded structured result lets the
                # Gateway reject CREATE/REPLACE without creating an active
                # invocation, handler process, or terminal tombstone.
                yield flight.Result(
                    _encode_definition_validation_result(
                        value, "ERROR", _safe_error(exc)
                    )
                )
            else:
                yield flight.Result(_encode_definition_validation_result(value, "OK"))
            return
        control = _decode_control(action.body)
        key = _tuple_key(control["tuple"])
        self._require_current_lease(key)
        if action.type not in ("AcknowledgeResults", "AcknowledgeFinish"):
            raise ValueError("PROTOCOL: unknown action")
        if control["kind"] != action.type:
            raise ValueError("PROTOCOL: action kind does not match action type")
        with self._lock:
            self._purge_terminal_locked(self._clock())
            state = self._active.get(key)
            terminal = self._terminal.get(key)
        # A cancelled action must not advance an active invocation.  This is
        # checked again after decoding and ledger lookup because an action can
        # be queued while its Flight context or the worker is being closed.
        # If cancellation races this check, the state mutation is the action
        # acceptance point; the caller may then safely retry the idempotent
        # acknowledgement when its parent context is still alive.
        self._ensure_action_active(context)
        if state is None:
            if terminal is None:
                raise ValueError("PROTOCOL: unknown invocation")
            if terminal.outcome != _TERMINAL_SUCCESS:
                raise ValueError(
                    f"PROTOCOL: invocation terminal outcome is {terminal.outcome}"
                )
            if not terminal.finish_id:
                raise ValueError("PROTOCOL: successful invocation has no Finish")
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

    def _ensure_action_active(self, context) -> None:
        if self._shutdown_event.is_set() or _context_is_cancelled(context):
            raise TimeoutError("DEADLINE_EXCEEDED: Flight action is cancelled")

    @staticmethod
    def _ack_control(control: Dict[str, Any]) -> Dict[str, Any]:
        value = {"kind": "Ack", "tuple": control["tuple"], "status": "OK"}
        if control["kind"] == "AcknowledgeResults":
            value["ack_sequence"] = control["ack_sequence"]
        else:
            value["finish_id"] = control["finish_id"]
        return value

    def do_exchange(self, context, descriptor, reader, writer):
        exchange_context = _ExchangeContext(context, self._shutdown_event)
        state = None
        key = None
        handler_session = None
        input_reader = None
        writer_started = False
        try:
            if exchange_context.is_cancelled():
                raise TimeoutError("DEADLINE_EXCEEDED: Flight server is shutting down")
            command = getattr(descriptor, "command", None)
            if command is None:
                raise ValueError("PROTOCOL: exchange is missing the invocation descriptor")
            open_control = _decode_control(command)
            if open_control["kind"] != "OpenInvocation": raise ValueError("PROTOCOL: first message must open an invocation")
            if not isinstance(open_control.get("payload"), dict):
                raise ValueError("PROTOCOL: invocation payload must be an object")
            key = _tuple_key(open_control["tuple"])
            self._require_current_lease(key)
            payload = open_control["payload"]
            unknown_payload = set(payload) - _OPEN_PAYLOAD_KEYS
            missing_payload = _OPEN_PAYLOAD_REQUIRED_KEYS - set(payload)
            if unknown_payload:
                raise ValueError("PROTOCOL: unsupported invocation payload field")
            if missing_payload:
                raise ValueError("PROTOCOL: invocation payload is missing a required field")
            _validate_typed_call_contract(payload)
            _function_ref(payload.get("function_ref"), open_control["tuple"])
            args = payload.get("args")
            result_descriptor = payload.get("return")
            if not isinstance(args, list) or any(not isinstance(item, dict) for item in args):
                raise ValueError("PROTOCOL: invocation args must be an array of descriptors")
            if not isinstance(result_descriptor, dict):
                raise ValueError("PROTOCOL: invocation return must be a descriptor")
            context_values = payload.get("context")
            statement_context = _statement_context(context_values)
            typed_context_values = _typed_statement_context(payload.get("statement_context"))
            if typed_context_values is None:
                raise ValueError("UNSUPPORTED_ROUTINE_VERSION: missing typed statement context")
            typed_statement_context = _statement_context(typed_context_values)
            if statement_context is not None and statement_context != typed_statement_context:
                raise ValueError("PROTOCOL: typed statement context does not match context")
            statement_context = typed_statement_context
            effective_context = dict(context_values or {})
            # The typed form is the canonical wire representation.  The
            # trusted CN map predates it and may use equivalent textual
            # encodings such as "+480" for a fixed offset.  The parsed
            # StatementContext comparison above rejects semantic drift;
            # overwrite the handler-facing map with canonical values so
            # equivalent encodings do not become a false protocol error.
            effective_context.update(typed_context_values)
            mode = _required_string(payload, "mode")
            null_policy = _required_string(payload, "null_policy")
            abi_contract = _required_string(payload, "abi_contract")
            adapter_version = _required_string(payload, "adapter_version")
            sdk_version = _required_string(payload, "sdk_version")
            definition_schema_version = payload.get("definition_schema_version")
            if type(definition_schema_version) is not int or definition_schema_version != DEFINITION_SCHEMA_VERSION:
                raise ValueError("UNSUPPORTED_ROUTINE_VERSION: unsupported Python definition schema")
            artifact_digest = _required_digest(payload, "artifact_digest")
            environment_digest = _required_digest(payload, "environment_digest")
            definition_fingerprint = _required_digest(payload, "definition_fingerprint")
            max_batch_bytes = _required_positive_int(payload, "max_batch_bytes", 1 << 30)
            max_batch_rows = _required_positive_int(payload, "max_batch_rows", 1 << 30)
            max_invocation_rows = _required_positive_int(payload, "max_invocation_rows", 1 << 32)
            max_invocation_result_bytes = _required_positive_int(
                payload, "max_invocation_result_bytes", 1 << 40
            )
            handler_timeout_seconds = _required_positive_float(
                payload, "handler_timeout_seconds", 3600.0
            )
            if mode not in (MODE_SCALAR, MODE_VECTOR) or null_policy not in (NULL_CALL, NULL_RETURN): raise ValueError("PROTOCOL: unsupported call mode or NULL policy")
            if abi_contract != ABI_CONTRACT or adapter_version != ADAPTER_VERSION: raise ValueError("PROTOCOL: unsupported Python ABI contract")
            if sdk_version != SDK_VERSION: raise ValueError("PROTOCOL: unsupported Python SDK")
            source = _required_string(payload, "source")
            handler_name = _required_string(payload, "handler")
            if artifact_digest != _inline_artifact_digest(handler_name, source):
                raise ValueError("UNSUPPORTED_ROUTINE_VERSION: Python artifact digest does not match the source")
            if environment_digest != _environment_digest():
                raise ValueError("UNSUPPORTED_ROUTINE_VERSION: Python environment digest does not match the worker contract")
            # Validate the complete frozen type contract before reserving a
            # ledger entry.  A malformed Open is rejected before it can leave
            # a terminal tombstone behind.
            for index, descriptor in enumerate(args):
                _field(f"arg_{index}", descriptor)
            result_field = _field("result", result_descriptor)
            expected_fingerprint = _definition_fingerprint(payload)
            if definition_fingerprint != expected_fingerprint:
                raise ValueError(
                    "UNSUPPORTED_ROUTINE_VERSION: Python definition fingerprint does not match the typed definition"
                )
            state = self._admit(key)
            state.tuple = open_control["tuple"]
            schema = None
            result_schema = pa.schema([result_field])
            writer.begin(result_schema)
            writer_started = True
            writer.write_metadata(_encode_control({"kind": "ResultSchema", "tuple": open_control["tuple"]}))
            ended = False
            invocation_rows = 0
            invocation_result_bytes = 0
            input_reader = _ExchangeInputReader(reader)
            while True:
                try:
                    chunk = input_reader.next(exchange_context)
                except StopIteration:
                    break
                # The reader can publish a chunk at the same instant that the
                # RPC is cancelled.  Re-check after the blocking handoff so a
                # chunk already dequeued after cancellation cannot start user
                # code or consume another handler slot.
                if exchange_context.is_cancelled():
                    raise TimeoutError("DEADLINE_EXCEEDED: input stream cancelled")
                if chunk is None or (chunk.data is None and not chunk.app_metadata):
                    raise ValueError("PROTOCOL: empty input frame")
                if chunk.data is not None:
                    if ended:
                        raise ValueError("PROTOCOL: input arrived after EndInput")
                    if schema is None:
                        schema = reader.schema
                        _validate_schema(schema, args)
                    batch = chunk.data
                    _validate_input_batch_schema(batch, schema, args)
                    control = _decode_control(chunk.app_metadata)
                    _require_tuple(control["tuple"], open_control["tuple"])
                    if control["kind"] != "InputBatch": raise ValueError("PROTOCOL: data batch is missing InputBatch")
                    sequence = _required_uint64(control, "sequence")
                    if sequence != state.input_sequence() + 1: raise ValueError("PROTOCOL: input sequence is not contiguous")
                    if batch.num_rows <= 0: raise ValueError("PROTOCOL: empty input batch")
                    if batch.num_rows > max_batch_rows: raise ValueError("RESOURCE_EXHAUSTED: input batch has too many rows")
                    if invocation_rows > max_invocation_rows - batch.num_rows:
                        raise ValueError("RESOURCE_EXHAUSTED: invocation has too many rows")
                    if batch.nbytes > max_batch_bytes: raise ValueError("RESOURCE_EXHAUSTED: input batch exceeds byte limit")
                    for index, descriptor in enumerate(args):
                        _validate_array_values(batch.column(index), descriptor)
                    state.record_input(sequence)
                    if handler_session is None:
                        # Start the bounded child while the parent builds the
                        # handler request.  The child imports the frozen
                        # runtime concurrently with this Arrow snapshot;
                        # admission and all input validation still happen
                        # before a handler slot is acquired.
                        owner_id = _handler_quota_owner(payload)
                        handler_session = _HandlerProcessSession(
                            self._handler_slots,
                            handler_quota=self._handler_quota,
                            account_id=key[0],
                            owner_id=owner_id,
                        )
                    execution_request = {
                        "source": source,
                        "handler": handler_name,
                        "mode": mode,
                        "null_policy": null_policy,
                        "abi_contract": abi_contract,
                        "adapter_version": adapter_version,
                        "sdk_version": sdk_version,
                        "definition_schema_version": definition_schema_version,
                        "artifact_digest": artifact_digest,
                        "environment_digest": environment_digest,
                        "definition_fingerprint": definition_fingerprint,
                        "context": effective_context,
                        "args": args,
                        "return": result_descriptor,
                        "max_batch_bytes": max_batch_bytes,
                        "max_batch_rows": max_batch_rows,
                        "max_invocation_rows": max_invocation_rows,
                        "max_invocation_result_bytes": max_invocation_result_bytes,
                        "arrow_encoding": HANDLER_ARROW_RECORD_BATCH,
                        "input": _serialize_record_batch_message(batch),
                    }
                    output_wire = handler_session.run(
                        exchange_context, execution_request, handler_timeout_seconds
                    )
                    output_batch = _deserialize_record_batch_message(
                        output_wire, result_schema
                    )
                    if output_batch.num_rows != batch.num_rows or output_batch.num_columns != 1:
                        raise ValueError("TYPE_CONTRACT: handler result has the wrong shape")
                    _validate_field(output_batch.schema.field(0), "result", result_descriptor)
                    output_array = output_batch.column(0)
                    _validate_array_values(output_array, result_descriptor)
                    if pa.ipc.get_record_batch_size(output_batch) > max_batch_bytes:
                        raise ValueError("RESOURCE_EXHAUSTED: output batch exceeds byte limit")
                    invocation_result_bytes += pa.ipc.get_record_batch_size(output_batch)
                    if invocation_result_bytes > max_invocation_result_bytes:
                        raise ValueError("RESOURCE_EXHAUSTED: invocation result exceeds byte limit")
                    invocation_rows += batch.num_rows
                    state.record_result(sequence)
                    writer.write_metadata(_encode_control({
                        "kind": "InputConsumed",
                        "tuple": open_control["tuple"],
                        "sequence": sequence,
                        # This is the decoded Arrow backing released by the
                        # worker after the handler has returned. It is bounded
                        # by max_batch_bytes and is distinct from the CN's
                        # transport framing bytes. A zero-column batch has
                        # no value buffers, but still consumes one logical
                        # accounting unit so the positive-credit protocol
                        # remains valid for zero-argument VECTOR calls.
                        "released_bytes": max(int(batch.nbytes), 1),
                        "released_batches": 1,
                    }))
                    writer.write_with_metadata(output_batch, _encode_control({"kind": "ResultBatch", "tuple": open_control["tuple"], "sequence": sequence}))
                    state.wait_result_ack(sequence, exchange_context)
                    if handler_session.should_rollover:
                        handler_session.close()
                        handler_session = None
                elif chunk.app_metadata:
                    control = _decode_control(chunk.app_metadata)
                    _require_tuple(control["tuple"], open_control["tuple"])
                    if control["kind"] == "EndInput":
                        last_sequence = _required_uint64(control, "last_sequence", allow_zero=True)
                        if ended:
                            if last_sequence != state.input_sequence():
                                raise ValueError("PROTOCOL: EndInput changed after input was closed")
                            continue
                        if last_sequence != state.input_sequence():
                            raise ValueError("PROTOCOL: invalid EndInput")
                        ended = True
                    elif control["kind"] == "OpenInvocation":
                        raise ValueError("PROTOCOL: duplicate OpenInvocation")
                    else:
                        raise ValueError("PROTOCOL: unexpected control without Arrow data")
            if not ended: raise ValueError("PROTOCOL: input stream ended before EndInput")
            last_input, last_result, acked_result = state.sequences()
            if acked_result != last_result: raise ValueError("PROTOCOL: result is not acknowledged")
            finish_id = state.mark_finish_sent(_uuid.uuid4().hex)
            writer.write_metadata(_encode_control({
                "kind": "Finish",
                "tuple": open_control["tuple"],
                "status": "OK",
                "finish_id": finish_id,
                "last_sequence": last_input,
                "last_result_sequence": last_result,
            }))
            state.wait_finish_ack(exchange_context)
        except Exception as exc:
            if state is not None and _is_cancellation_error(exc, exchange_context):
                state.mark_cancelled()
            if writer_started and key is not None:
                try:
                    writer.write_metadata(_encode_control({"kind": "Error", "tuple": open_control["tuple"], "status": "ERROR", "reason": _safe_error(exc)}))
                except Exception:
                    pass
            raise
        finally:
            exchange_error = sys.exc_info()[1]
            cleanup_error = self._cleanup_exchange(
                key, state, input_reader, handler_session
            )
            if cleanup_error is not None and exchange_error is None:
                raise cleanup_error
            if cleanup_error is not None:
                logging.error(
                    "Python UDF exchange cleanup failed after an exchange error: %s",
                    cleanup_error,
                )


def _is_cancellation_error(exc: Exception, context) -> bool:
    if _context_is_cancelled(context):
        return True
    # Handler, result-ACK, and Finish-ACK deadlines use this stable protocol
    # category.  Once observed, they are a cancellation terminal outcome even
    # if the transport context has not yet reported cancellation locally.
    return isinstance(exc, TimeoutError) and str(exc).startswith("DEADLINE_EXCEEDED:")


def _safe_error(exc: Exception) -> str:
    message = str(exc)
    categories = ("PROTOCOL:", "TYPE_CONTRACT:", "USER_CODE:", "RESOURCE_EXHAUSTED:", "DEADLINE_EXCEEDED:")
    if not message.startswith(categories):
        return "USER_CODE: handler or runtime failed"
    return message[:512]


def main() -> None:
    if _HANDLER_ENTRY:
        _execute_handler_subprocess()
        return

    import argparse
    import secrets

    parser = argparse.ArgumentParser()
    parser.add_argument("--execute-handler", action="store_true")
    parser.add_argument("--address")
    args = parser.parse_args()
    if not args.address:
        parser.error("--address is required for the Flight server")
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
    address = args.address if "://" in args.address else "grpc://" + args.address
    # Every worker process is a distinct execution owner.  A random positive
    # epoch prevents a restarted worker at the same endpoint from accepting
    # control messages issued to the previous process.  Direct unit-test
    # servers keep the deterministic default of one.
    lease_epoch = secrets.randbits(63) or 1
    server = RoutineFlightServer(address, lease_epoch=lease_epoch)
    server.serve()


if __name__ == "__main__":
    main()
