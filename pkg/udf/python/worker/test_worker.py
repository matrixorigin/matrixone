#!/usr/bin/env python3

import datetime
import decimal
import io
import importlib.util
import json
import os
import pathlib
import pickle
import signal
import struct
import subprocess
import sys
import tempfile
import threading
import time
import types
import unittest
import uuid
from unittest import mock

import pyarrow as pa
import pyarrow.flight as flight


WORKER_PATH = pathlib.Path(__file__).with_name("worker.py")
spec = importlib.util.spec_from_file_location("matrixone_python_worker", WORKER_PATH)
worker = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = worker
spec.loader.exec_module(worker)


def complete_open_payload(payload):
    """Build a current-contract Open payload for direct Flight tests."""
    payload = dict(payload)
    payload.update(
        {
            "definition_schema_version": worker.DEFINITION_SCHEMA_VERSION,
            "artifact_digest": worker._inline_artifact_digest(
                payload["handler"], payload["source"]
            ),
            "environment_digest": worker._environment_digest(),
            "max_invocation_rows": 1 << 20,
            "max_invocation_result_bytes": 256 << 20,
        }
    )
    payload.setdefault("callsite_id", "python/test")
    payload.setdefault("may_error", True)
    payload.setdefault("security_mode", "INVOKER")
    payload.setdefault("leakproof", False)
    payload.setdefault(
        "statement_context",
        {
            "contract_version": 1,
            "statement_timestamp_utc": 1704067200123456,
            "timezone_kind": "FIXED_OFFSET",
            "timezone_offset_minutes": 480,
            "sql_mode": ["ANSI", "STRICT_TRANS_TABLES"],
            "current_database": "test",
            "current_user": "root",
            "current_role": "writer",
            "connection_collation": "utf8mb4_bin",
        },
    )
    payload.setdefault(
        "security_frame",
        {
            "contract_version": 1,
            "mode": "INVOKER",
            "invoker_user_id": 0,
            "invoker_role_id": 0,
            "effective_user_id": 0,
            "effective_role_id": 0,
        },
    )
    try:
        payload["definition_fingerprint"] = worker._definition_fingerprint(payload)
    except ValueError:
        # Some tests intentionally carry an invalid Arrow descriptor and must
        # reach that validation branch before fingerprint computation.
        payload["definition_fingerprint"] = "a" * 64
    return payload


def complete_definition_validation_payload(source="def f(ctx, value): return value", handler="f"):
    descriptor = {"type_id": worker.INT64, "offset_width": 32}
    payload = {
        "account_id": 1,
        "handler": handler,
        "source": source,
        "mode": worker.MODE_SCALAR,
        "null_policy": worker.NULL_CALL,
        "abi_contract": worker.ABI_CONTRACT,
        "adapter_version": worker.ADAPTER_VERSION,
        "sdk_version": worker.SDK_VERSION,
        "definition_schema_version": worker.DEFINITION_SCHEMA_VERSION,
        "artifact_digest": worker._inline_artifact_digest(handler, source),
        "environment_digest": worker._environment_digest(),
        "definition_fingerprint": "",
        "args": [descriptor],
        "return": descriptor,
    }
    payload["definition_fingerprint"] = worker._definition_fingerprint(payload)
    return payload


class WorkerContractTest(unittest.TestCase):
    def test_definition_validation_compiles_without_executing_module_code(self):
        with tempfile.TemporaryDirectory(prefix="mo-udf-definition-") as directory:
            marker = pathlib.Path(directory) / "executed"
            source = (
                "from pathlib import Path\n"
                f"Path({str(marker)!r}).write_text('executed')\n"
                "def f(ctx, value): return value\n"
            )
            payload = complete_definition_validation_payload(source)
            server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
            try:
                results = list(
                    server.do_action(
                        None,
                        flight.Action(
                            "ValidatePythonDefinition",
                            json.dumps(payload, separators=(",", ":")).encode(),
                        ),
                    )
                )
                self.assertEqual(1, len(results))
                self.assertEqual("OK", json.loads(bytes(results[0].body))["status"])
                self.assertFalse(marker.exists(), "definition validation executed module code")
                self.assertEqual({}, server._active)
                self.assertEqual({}, server._terminal)
            finally:
                server.shutdown()

    def test_definition_validation_rejects_syntax_before_catalog_state(self):
        payload = complete_definition_validation_payload("def f(ctx, value) return value")
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        try:
            results = list(
                server.do_action(
                    None,
                    flight.Action(
                        "ValidatePythonDefinition",
                        json.dumps(payload, separators=(",", ":")).encode(),
                    ),
                )
            )
            response = json.loads(bytes(results[0].body))
            self.assertEqual("ERROR", response["status"])
            self.assertRegex(response["reason"], r"USER_CODE: Python syntax error at line 1, column")
            self.assertEqual({}, server._active)
            self.assertEqual({}, server._terminal)
        finally:
            server.shutdown()

    def test_definition_validation_rejects_missing_handler_before_catalog_state(self):
        payload = complete_definition_validation_payload(
            "def other(ctx, value): return value"
        )
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        try:
            results = list(
                server.do_action(
                    None,
                    flight.Action(
                        "ValidatePythonDefinition",
                        json.dumps(payload, separators=(",", ":")).encode(),
                    ),
                )
            )
            response = json.loads(bytes(results[0].body))
            self.assertEqual("ERROR", response["status"])
            self.assertIn("not a module-level function", response["reason"])
            self.assertEqual({}, server._active)
            self.assertEqual({}, server._terminal)
        finally:
            server.shutdown()

    def test_definition_validation_rejects_async_handler_before_catalog_state(self):
        payload = complete_definition_validation_payload(
            "async def f(ctx, value): return value"
        )
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        try:
            results = list(
                server.do_action(
                    None,
                    flight.Action(
                        "ValidatePythonDefinition",
                        json.dumps(payload, separators=(",", ":")).encode(),
                    ),
                )
            )
            response = json.loads(bytes(results[0].body))
            self.assertEqual("ERROR", response["status"])
            self.assertIn("must be synchronous", response["reason"])
            self.assertEqual({}, server._active)
            self.assertEqual({}, server._terminal)
        finally:
            server.shutdown()

    def test_definition_validation_rejects_boolean_schema_version(self):
        payload = complete_definition_validation_payload()
        payload["definition_schema_version"] = True
        # A malformed sender could otherwise recompute its own malformed
        # fingerprint and pass the later integrity check. The version domain
        # must be rejected before that comparison.
        payload["definition_fingerprint"] = worker._definition_fingerprint(payload)
        with self.assertRaisesRegex(ValueError, "unsupported Python definition schema"):
            worker._decode_definition_validation(
                json.dumps(payload, separators=(",", ":")).encode()
            )

    def test_definition_validation_rejects_handler_rebound_after_definition(self):
        payload = complete_definition_validation_payload(
            "def f(ctx, value): return value\n"
            "f = 1\n"
        )
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        try:
            results = list(
                server.do_action(
                    None,
                    flight.Action(
                        "ValidatePythonDefinition",
                        json.dumps(payload, separators=(",", ":")).encode(),
                    ),
                )
            )
            response = json.loads(bytes(results[0].body))
            self.assertEqual("ERROR", response["status"])
            self.assertIn("not a module-level function", response["reason"])
            self.assertEqual({}, server._active)
            self.assertEqual({}, server._terminal)
        finally:
            server.shutdown()

    def test_definition_validation_rejects_handler_rebound_in_module_control_flow(self):
        sources = (
            "def f(ctx, value): return value\n"
            "if True:\n"
            "    f = 1\n",
            "def f(ctx, value): return value\n"
            "for f in (1,):\n"
            "    pass\n",
            "def f(ctx, value): return value\n"
            "from math import pi as f\n",
        )
        for source in sources:
            with self.subTest(source=source):
                with self.assertRaisesRegex(ValueError, "not a module-level function"):
                    worker._validate_definition_syntax(
                        complete_definition_validation_payload(source)
                    )

        # A same-named local in a nested function does not rebind the module
        # handler and must remain valid.
        worker._validate_definition_syntax(
            complete_definition_validation_payload(
                "def f(ctx, value):\n"
                "    def inner():\n"
                "        f = 1\n"
                "    return value\n"
            )
        )

    def test_capability_advertises_worker_instance_lease(self):
        encoded = worker._encode_capabilities(
            {"protocol_version": worker.PROTOCOL_VERSION}, lease_epoch=17
        )
        value = json.loads(encoded)
        self.assertEqual(17, value["lease_epoch"])
        self.assertEqual(worker.MAX_HANDLER_PROCESSES, value["max_handler_processes"])
        self.assertEqual(
            worker.MAX_ACCOUNT_HANDLER_PROCESSES,
            value["max_account_handler_processes"],
        )
        self.assertEqual(
            worker.MAX_OWNER_HANDLER_PROCESSES,
            value["max_owner_handler_processes"],
        )
        with self.assertRaisesRegex(ValueError, "invalid worker lease epoch"):
            worker._encode_capabilities(
                {"protocol_version": worker.PROTOCOL_VERSION}, lease_epoch=0
            )

    def test_typed_statement_context_matches_canonical_handler_context(self):
        typed = {
            "contract_version": 1,
            "statement_timestamp_utc": 1704067200123456,
            "timezone_kind": "FIXED_OFFSET",
            "timezone_offset_minutes": 510,
            "sql_mode": ["ANSI", "STRICT_TRANS_TABLES"],
            "current_database": "app",
            "current_user": "alice",
            "current_role": "writer",
            "connection_collation": "utf8mb4_bin",
        }
        values = worker._typed_statement_context(typed)
        statement = worker._statement_context(values)
        self.assertEqual("alice", statement.current_user)
        self.assertEqual(510, statement.session_timezone.offset_minutes)
        self.assertEqual(("ANSI", "STRICT_TRANS_TABLES"), statement.sql_mode)

    def test_typed_statement_context_accepts_unix_epoch(self):
        typed = {
            "contract_version": 1,
            "statement_timestamp_utc": 0,
            "timezone_kind": "FIXED_OFFSET",
            "timezone_offset_minutes": 0,
            "sql_mode": [],
            "current_user": "alice",
            "connection_collation": "utf8mb4_bin",
        }
        values = worker._typed_statement_context(typed)
        statement = worker._statement_context(values)
        self.assertEqual(
            datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc),
            statement.statement_timestamp_utc,
        )

    def test_statement_timezone_kind_requires_canonical_spelling(self):
        typed = {
            "contract_version": 1,
            "statement_timestamp_utc": 0,
            "timezone_kind": "fixed_offset",
            "timezone_offset_minutes": 0,
            "sql_mode": [],
            "current_user": "alice",
            "connection_collation": "utf8mb4_bin",
        }
        with self.assertRaisesRegex(ValueError, "unsupported typed statement timezone"):
            worker._typed_statement_context(typed)

        with self.assertRaisesRegex(ValueError, "unsupported timezone kind"):
            worker._statement_context(
                {
                    "statement_timestamp_utc": "0",
                    "session_timezone_kind": "fixed_offset",
                    "session_timezone_offset_minutes": "0",
                    "sql_mode": "[]",
                    "current_user": "alice",
                    "connection_collation": "utf8mb4_bin",
                }
            )

    def test_typed_statement_context_rejects_timestamp_outside_int64(self):
        typed = {
            "contract_version": 1,
            "statement_timestamp_utc": worker.MAX_INT64 + 1,
            "timezone_kind": "FIXED_OFFSET",
            "timezone_offset_minutes": 0,
            "sql_mode": [],
            "current_user": "alice",
            "connection_collation": "utf8mb4_bin",
        }
        with self.assertRaisesRegex(ValueError, "invalid typed statement timestamp"):
            worker._typed_statement_context(typed)

        with self.assertRaisesRegex(ValueError, "invalid statement_timestamp_utc"):
            worker._statement_context(
                {
                    "statement_timestamp_utc": str(worker.MIN_INT64 - 1),
                    "session_timezone_kind": "FIXED_OFFSET",
                    "session_timezone_offset_minutes": "0",
                    "sql_mode": "[]",
                    "current_user": "alice",
                    "connection_collation": "utf8mb4_bin",
                }
            )

    def test_typed_contract_versions_require_integer_wire_values(self):
        typed_context = {
            "contract_version": True,
            "statement_timestamp_utc": 0,
            "timezone_kind": "FIXED_OFFSET",
            "timezone_offset_minutes": 0,
            "sql_mode": [],
            "current_user": "alice",
            "connection_collation": "utf8mb4_bin",
        }
        with self.assertRaisesRegex(ValueError, "unsupported statement context contract"):
            worker._typed_statement_context(typed_context)

        payload = {
            "callsite_id": "python/1",
            "may_error": True,
            "security_mode": "INVOKER",
            "leakproof": False,
            "security_frame": {
                "contract_version": True,
                "mode": "INVOKER",
                "invoker_user_id": 7,
                "invoker_role_id": 8,
                "effective_user_id": 7,
                "effective_role_id": 8,
            },
        }
        with self.assertRaisesRegex(ValueError, "unsupported Python security frame"):
            worker._validate_typed_call_contract(payload)

    def test_typed_routine_semantics_reject_partial_or_definer_contract(self):
        payload = {
            "callsite_id": "python/1",
            "may_error": True,
            "security_mode": "INVOKER",
            "leakproof": False,
            "security_frame": {
                "contract_version": 1,
                "mode": "INVOKER",
                "invoker_user_id": 7,
                "invoker_role_id": 8,
                "effective_user_id": 7,
                "effective_role_id": 8,
            },
        }
        worker._validate_typed_call_contract(payload)
        payload["security_mode"] = "DEFINER"
        with self.assertRaisesRegex(ValueError, "semantic contract"):
            worker._validate_typed_call_contract(payload)
        payload.pop("security_mode")
        with self.assertRaisesRegex(ValueError, "incomplete typed routine semantic contract"):
            worker._validate_typed_call_contract(payload)

    def test_typed_routine_semantics_rejects_non_uint32_principal(self):
        payload = {
            "callsite_id": "python/1",
            "may_error": True,
            "security_mode": "INVOKER",
            "leakproof": False,
            "security_frame": {
                "contract_version": 1,
                "mode": "INVOKER",
                "invoker_user_id": worker.MAX_SECURITY_PRINCIPAL_ID + 1,
                "invoker_role_id": 8,
                "effective_user_id": worker.MAX_SECURITY_PRINCIPAL_ID + 1,
                "effective_role_id": 8,
            },
        }
        with self.assertRaisesRegex(ValueError, "invalid Python security frame"):
            worker._validate_typed_call_contract(payload)

    def test_handler_quota_uses_typed_effective_principal(self):
        payload = complete_open_payload(
            {
                "function_ref": {
                    "account_id": 1,
                    "database_id": 2,
                    "function_id": 3,
                    "revision": 1,
                    "namespace_version": 1,
                },
                "source": "def f(ctx, value): return value",
                "handler": "f",
                "mode": worker.MODE_SCALAR,
                "null_policy": worker.NULL_CALL,
                "abi_contract": worker.ABI_CONTRACT,
                "adapter_version": worker.ADAPTER_VERSION,
                "sdk_version": worker.SDK_VERSION,
                "args": [{"type_id": worker.INT64, "offset_width": 32}],
                "return": {"type_id": worker.INT64, "offset_width": 32},
                "max_batch_bytes": 1 << 20,
                "max_batch_rows": 1,
                "handler_timeout_seconds": 1.0,
            }
        )
        payload["security_frame"].update(
            {
                "invoker_user_id": 23,
                "invoker_role_id": 7,
                "effective_user_id": 23,
                "effective_role_id": 7,
            }
        )
        self.assertEqual("user:23/role:7", worker._handler_quota_owner(payload))
        payload["statement_context"]["current_user"] = "same-name-different-principal"
        self.assertEqual("user:23/role:7", worker._handler_quota_owner(payload))

    def test_exchange_rejects_old_demo_open_before_user_code(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        fence = {
            "account_id": 1,
            "statement_id": "legacy-demo",
            "group_id": "legacy-group",
            "group_epoch": 1,
            "invocation_id": "legacy-invocation",
            "lease_epoch": 1,
        }
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        payload = complete_open_payload(
            {
                "function_ref": {
                    "account_id": 1,
                    "database_id": 2,
                    "function_id": 3,
                    "revision": 1,
                    "namespace_version": 1,
                },
                "source": "def f(ctx, x): return x",
                "handler": "f",
                "mode": worker.MODE_SCALAR,
                "null_policy": worker.NULL_CALL,
                "abi_contract": worker.ABI_CONTRACT,
                "adapter_version": worker.ADAPTER_VERSION,
                "sdk_version": worker.SDK_VERSION,
                "args": [descriptor],
                "return": descriptor,
                "max_batch_bytes": 1 << 20,
                "max_batch_rows": 8,
                "handler_timeout_seconds": 2,
            }
        )
        payload.pop("statement_context")
        command = worker._encode_control(
            {"kind": "OpenInvocation", "tuple": fence, "payload": payload}
        )
        reader = types.SimpleNamespace(schema=None, read_chunk=lambda: None)
        writer = types.SimpleNamespace(
            begin=lambda schema: None,
            write_metadata=lambda data: None,
            write_with_metadata=lambda record, data: None,
        )
        try:
            with self.assertRaisesRegex(ValueError, "invocation payload is missing a required field"):
                server.do_exchange(
                    types.SimpleNamespace(is_cancelled=lambda: False),
                    types.SimpleNamespace(command=command),
                    reader,
                    writer,
                )
            self.assertFalse(server._active)
            self.assertFalse(server._terminal)
        finally:
            server.shutdown()

    def test_exchange_rejects_missing_typed_statement_context_before_user_code(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        fence = {
            "account_id": 1,
            "statement_id": "missing-context",
            "group_id": "missing-context-group",
            "group_epoch": 1,
            "invocation_id": "missing-context-invocation",
            "lease_epoch": 1,
        }
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        payload = complete_open_payload(
            {
                "function_ref": {
                    "account_id": 1,
                    "database_id": 2,
                    "function_id": 3,
                    "revision": 1,
                    "namespace_version": 1,
                },
                "source": "def f(ctx, x): return x",
                "handler": "f",
                "mode": worker.MODE_SCALAR,
                "null_policy": worker.NULL_CALL,
                "abi_contract": worker.ABI_CONTRACT,
                "adapter_version": worker.ADAPTER_VERSION,
                "sdk_version": worker.SDK_VERSION,
                "args": [descriptor],
                "return": descriptor,
                "max_batch_bytes": 1 << 20,
                "max_batch_rows": 8,
                "handler_timeout_seconds": 2,
            }
        )
        payload["statement_context"] = None
        command = worker._encode_control(
            {"kind": "OpenInvocation", "tuple": fence, "payload": payload}
        )
        reader = types.SimpleNamespace(schema=None, read_chunk=lambda: None)
        writer = types.SimpleNamespace(
            begin=lambda schema: None,
            write_metadata=lambda data: None,
            write_with_metadata=lambda record, data: None,
        )
        try:
            with self.assertRaisesRegex(ValueError, "missing typed statement context"):
                server.do_exchange(
                    types.SimpleNamespace(is_cancelled=lambda: False),
                    types.SimpleNamespace(command=command),
                    reader,
                    writer,
                )
            self.assertFalse(server._active)
            self.assertFalse(server._terminal)
        finally:
            server.shutdown()

    def test_worker_rejects_a_tuple_from_another_instance(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0", lease_epoch=17)
        with self.assertRaisesRegex(ValueError, "STALE_LEASE_EPOCH"):
            server._require_current_lease((1, "statement", "group", 1, "invocation", 16))
        with self.assertRaisesRegex(ValueError, "invalid worker lease epoch"):
            worker.RoutineFlightServer("grpc://127.0.0.1:0", lease_epoch=0)

    @unittest.skipUnless(os.name == "posix", "Flight shutdown cancellation test")
    def test_flight_shutdown_cancels_an_active_exchange(self):
        entered = threading.Event()
        original_reader = worker._ExchangeInputReader

        class NotifyingReader(original_reader):
            def __init__(self, reader):
                entered.set()
                super().__init__(reader)

        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        client = flight.FlightClient(("127.0.0.1", server.port))
        server_thread = threading.Thread(target=server.serve, daemon=True)
        server_thread.start()
        writer = None
        shutdown_error = []
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        payload = complete_open_payload(
            {
                "function_ref": {
                    "account_id": 1,
                    "database_id": 2,
                    "function_id": 3,
                    "revision": 1,
                    "namespace_version": 1,
                },
                "source": "def f(ctx, x): return x",
                "handler": "f",
                "mode": worker.MODE_SCALAR,
                "null_policy": worker.NULL_CALL,
                "abi_contract": worker.ABI_CONTRACT,
                "adapter_version": worker.ADAPTER_VERSION,
                "sdk_version": worker.SDK_VERSION,
                "args": [descriptor],
                "return": descriptor,
                "max_batch_bytes": 1 << 20,
                "max_batch_rows": 8,
                "handler_timeout_seconds": 60,
            }
        )
        fence = {
            "account_id": 1,
            "statement_id": "shutdown",
            "group_id": "shutdown-group",
            "group_epoch": 1,
            "invocation_id": "shutdown-invocation",
            "lease_epoch": 1,
        }

        def control(kind, **fields):
            return worker._encode_control({"kind": kind, "tuple": fence, **fields})

        try:
            with mock.patch.object(worker, "_ExchangeInputReader", NotifyingReader):
                writer, reader = client.do_exchange(
                    flight.FlightDescriptor.for_command(
                        control("OpenInvocation", payload=payload)
                    ),
                    options=flight.FlightCallOptions(timeout=5),
                )
                self.assertTrue(entered.wait(2), "exchange did not reach its input reader")

                def shutdown():
                    try:
                        server.shutdown()
                    except Exception as exc:
                        shutdown_error.append(exc)

                shutdown_thread = threading.Thread(target=shutdown)
                shutdown_thread.start()
                shutdown_thread.join(5)
                self.assertFalse(
                    shutdown_thread.is_alive(),
                    "server shutdown waited for an active exchange",
                )
            self.assertEqual([], shutdown_error)
            self.assertFalse(server._active)
            with server._pending_cleanup_condition:
                self.assertFalse(server._pending_cleanups)
        finally:
            if writer is not None:
                try:
                    writer.close()
                except Exception:
                    pass
            client.close()
            if server_thread.is_alive():
                server_thread.join(2)

    def test_inline_contract_rejects_external_handler_import(self):
        with self.assertRaisesRegex(ValueError, "immutable artifact catalog"):
            worker._load_handler("def add(ctx, value): return value", "module:add")

    def test_go_python_type_descriptor_fixtures(self):
        fixture_path = WORKER_PATH.parent.parent / "testdata" / "type_descriptors.json"
        fixtures = json.loads(fixture_path.read_text())
        self.assertGreaterEqual(len(fixtures), 20)
        for fixture in fixtures:
            descriptor = fixture["descriptor"]
            field = worker._field("value", descriptor)
            self.assertEqual(
                fixture["fingerprint"],
                worker._fingerprint(descriptor),
                fixture["name"],
            )
            self.assertEqual(
                fixture["fingerprint"],
                field.metadata[worker.TypeFingerprintKey].decode("ascii"),
                fixture["name"],
            )

    def test_all_type_fixtures_round_trip_scalar_values_and_nulls(self):
        fixture_path = WORKER_PATH.parent.parent / "testdata" / "type_descriptors.json"
        fixtures = json.loads(fixture_path.read_text())
        values = {
            "bool": False,
            "int8": 1,
            "int16": 1,
            "int32": 1,
            "int64": 1,
            "uint8": 1,
            "uint16": 1,
            "uint32": 1,
            "uint64": 1,
            "float32": 1.25,
            "float64": 1.25,
            "decimal64": decimal.Decimal("0.123456"),
            "decimal128": decimal.Decimal("0.0000000001"),
            "date": worker.SqlDate(False, datetime.date(2024, 1, 2)),
            "time": datetime.timedelta(microseconds=123456),
            "datetime": worker.SqlDatetime(
                False, datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)
            ),
            "timestamp": worker.SqlTimestamp(
                False,
                datetime.datetime(
                    2024, 1, 2, 3, 4, 5, 123456, tzinfo=datetime.timezone.utc
                ),
            ),
            "char": "中",
            "varchar": "中",
            "text": "matrixone",
            "json": '{"a":1,"b":[true,null]}',
            "binary": b"\\x01\\x02",
            "varbinary": b"\\x01\\x02",
            "blob": b"\\x01\\x02",
            "uuid": uuid.UUID("123e4567-e89b-12d3-a456-426614174000"),
            "vecf32": memoryview(struct.pack("<fff", 1.0, 2.0, 3.0)),
            "vecf64": memoryview(struct.pack("<ddd", 1.0, 2.0, 3.0)),
        }
        for fixture in fixtures:
            name = fixture["name"]
            descriptor = fixture["descriptor"]
            value = values[name]
            field = worker._field("value", descriptor)
            worker._validate_field(field, "value", descriptor)
            array = worker._output_array([None, value], descriptor, 2)
            self.assertEqual(2, len(array), name)
            self.assertIsNone(array[0].as_py(), name)
            round_trip = worker._scalar_input(array, 1, descriptor)
            if name in ("vecf32", "vecf64"):
                fmt = "f" if name == "vecf32" else "d"
                self.assertEqual(
                    list(value.cast(fmt)), list(round_trip.cast(fmt)), name
                )
            else:
                self.assertEqual(value, round_trip, name)

    def test_scalar_column_materialization_preserves_fixed_width_values(self):
        small_arrays = (
            pa.array([1, None, 3], type=pa.int64()),
            pa.array([True, None, False], type=pa.bool_()),
            pa.array([1.25, None, 3.5], type=pa.float64()),
        )
        for array in small_arrays:
            self.assertIsNone(worker._scalar_column_values(array))
        for array in (
            pa.array([1, None] + list(range(2, 64)), type=pa.int64()),
            pa.array([True, None] + [False] * 62, type=pa.bool_()),
            pa.array([1.25, None] + [float(value) for value in range(2, 64)], type=pa.float64()),
        ):
            with self.subTest(type=array.type):
                self.assertEqual(array.to_pylist(), worker._scalar_column_values(array))
        self.assertIsNone(worker._scalar_column_values(pa.array(["a", None], type=pa.string())))

    def test_go_python_fingerprint_vectors(self):
        cases = [
            ({"type_id": worker.INT64, "offset_width": 32}, "482d3c55b8a9d662c2b41326b3ca55cbae65153e662c47c4919ba020d006262b"),
            ({"type_id": worker.VARCHAR, "width": 65535, "charset": 3, "offset_width": 32}, "e35cb66adb4bce519a6f3ec9cfa8a33adad6202cbe678f5fecffdeb28d65b824"),
            ({"type_id": worker.JSON, "offset_width": 32, "json_encoding": "canonical_text"}, "b156af5241a070ff80a287da19c46d1593dabee4fb5429f964452e711cd275eb"),
            ({"type_id": worker.UUID, "offset_width": 0}, "370e2939c178acee55aaae27a7bcf92937a203e2788e9e0140d84884be042780"),
            ({"type_id": worker.DATE, "offset_width": 32, "temporal_encoding": "sql_zero_struct"}, "8c401bde992eb173ca8aedca97dbd9f47453b7a860fae199ae0439307e858b4b"),
        ]
        for descriptor, expected in cases:
            self.assertEqual(expected, worker._fingerprint(descriptor))

    def test_go_python_definition_digest_vector(self):
        # The runtime environment digest contains the worker's tzdb version,
        # so it is intentionally different across images.  Cross-language
        # fingerprint conformance uses a fixed test environment identity;
        # the execution path still validates the real negotiated digest.
        test_environment_digest = "e" * 64
        self.assertEqual(
            "d3b06769662f0e30fca60e6dedc479c5aacef6c488c6489366fdb540ee374f26",
            worker._inline_artifact_digest(
                "add", "def add(ctx, value): return value"
            ),
        )
        self.assertEqual(64, len(worker._environment_digest()))
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        payload = {
            "definition_schema_version": worker.DEFINITION_SCHEMA_VERSION,
            "handler": "add",
            "source": "def add(ctx, value): return value",
            "mode": worker.MODE_SCALAR,
            "null_policy": worker.NULL_CALL,
            "abi_contract": worker.ABI_CONTRACT,
            "adapter_version": worker.ADAPTER_VERSION,
            "artifact_digest": worker._inline_artifact_digest(
                "add", "def add(ctx, value): return value"
            ),
            "environment_digest": test_environment_digest,
            "sdk_version": worker.SDK_VERSION,
            "args": [descriptor],
            "return": descriptor,
        }
        self.assertEqual(
            "d0348f99deac2a1518fdf0f32d37a4e7924acbce450c7328d1e83d67d5f7e635",
            worker._definition_fingerprint(payload),
        )

    def test_metadata_is_owned_by_adapter(self):
        descriptor = {"type_id": worker.VARCHAR, "width": 65535, "charset": 3, "offset_width": 32}
        field = worker._field("result", descriptor)
        self.assertEqual(field.metadata, {
            worker.TypeMetadataKey: b'{"type_id":61,"width":65535,"charset":3,"offset_width":32}',
            worker.TypeFingerprintKey: b"e35cb66adb4bce519a6f3ec9cfa8a33adad6202cbe678f5fecffdeb28d65b824",
        })
        with_extra = pa.field("result", field.type, metadata={**field.metadata, b"untrusted": b"1"})
        with self.assertRaisesRegex(ValueError, "logical metadata"):
            worker._validate_field(with_extra, "result", descriptor)
        with self.assertRaisesRegex(ValueError, "field name"):
            worker._validate_field(pa.field("other", field.type, metadata=field.metadata), "result", descriptor)
        with self.assertRaisesRegex(ValueError, "nullable"):
            worker._validate_field(
                pa.field("result", field.type, nullable=False, metadata=field.metadata),
                "result",
                descriptor,
            )

    def test_type_descriptor_shape_is_strict(self):
        with self.assertRaisesRegex(ValueError, "unsupported descriptor field"):
            worker._field(
                "result",
                {"type_id": worker.INT64, "offset_width": 32, "future": 1},
            )
        with self.assertRaisesRegex(ValueError, "type_id must be an integer"):
            worker._field("result", {"type_id": "23", "offset_width": 32})
        with self.assertRaisesRegex(ValueError, "offset_width must be an integer"):
            worker._field("result", {"type_id": worker.INT64, "offset_width": "32"})

    def test_text_descriptor_rejects_opaque_binary_charset(self):
        for type_id in (worker.CHAR, worker.VARCHAR, worker.TEXT):
            with self.subTest(type_id=type_id):
                with self.assertRaisesRegex(ValueError, "unsupported text charset"):
                    worker._field(
                        "result",
                        {"type_id": type_id, "width": 16, "charset": 1, "offset_width": 32},
                    )

    def test_malformed_control_is_reported_as_protocol_error(self):
        with self.assertRaisesRegex(ValueError, "PROTOCOL: invalid control UTF-8"):
            worker._decode_control(b"\xff")
        with self.assertRaisesRegex(ValueError, "PROTOCOL: invalid control JSON"):
            worker._decode_control(b"{")
        self.assertEqual(
            "PROTOCOL: invalid control JSON",
            worker._safe_error(ValueError("PROTOCOL: invalid control JSON")),
        )

    def test_malformed_arrow_payload_is_reported_as_protocol_error(self):
        with self.assertRaisesRegex(ValueError, "PROTOCOL: execution payload is not a valid Arrow stream"):
            worker._deserialize_record_batch(b"not-an-arrow-stream")

    def test_descriptor_domain_is_strict(self):
        invalid = [
            {"type_id": worker.VARCHAR, "width": -1, "offset_width": 32},
            {"type_id": worker.INT64, "offset_width": 64},
            {"type_id": worker.INT64},
            {"type_id": worker.INT64, "offset_width": 0},
            {"type_id": worker.JSON, "offset_width": 32},
            {"type_id": worker.DATE, "offset_width": 32},
            {"type_id": worker.DECIMAL64, "width": 3, "scale": 4, "offset_width": 32},
            {"type_id": worker.VECF32, "width": 65536, "offset_width": 0},
        ]
        for descriptor in invalid:
            with self.assertRaisesRegex(ValueError, "TYPE_CONTRACT"):
                worker._field("value", descriptor)

    def test_descriptor_integer_fields_match_go_int32_wire_domain(self):
        descriptor = {
            "type_id": worker.VARCHAR,
            "width": worker.MAX_INT32,
            "offset_width": 32,
        }
        self.assertEqual(pa.string(), worker._field("value", descriptor).type)
        for key in ("type_id", "width", "scale", "offset_width"):
            invalid = dict(descriptor)
            invalid[key] = worker.MAX_INT32 + 1
            if key == "scale":
                invalid["type_id"] = worker.DECIMAL128
                invalid["width"] = 38
            with self.subTest(key=key):
                with self.assertRaisesRegex(ValueError, "outside int32 range"):
                    worker._field("value", invalid)

    def test_float_descriptors_round_trip_for_scalar_and_vector_contracts(self):
        for type_id, arrow_type in (
            (worker.FLOAT32, pa.float32()),
            (worker.FLOAT64, pa.float64()),
        ):
            descriptor = {"type_id": type_id, "offset_width": 32}
            field = worker._field("value", descriptor)
            self.assertEqual(arrow_type, field.type)
            self.assertEqual(
                [None, -1.25, 3.5],
                worker._output_array([None, -1.25, 3.5], descriptor, 3).to_pylist(),
            )

    def test_every_input_batch_keeps_the_frozen_schema(self):
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        valid_schema = pa.schema([worker._field("arg_0", descriptor)])
        invalid_batch = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int32())],
            schema=pa.schema(
                [pa.field("arg_0", pa.int32(), nullable=True, metadata=valid_schema.field(0).metadata)]
            ),
        )
        with self.assertRaisesRegex(ValueError, "Arrow type"):
            worker._validate_input_batch_schema(invalid_batch, valid_schema, [descriptor])

        metadata_changed = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int64())],
            schema=pa.schema(
                [pa.field("arg_0", pa.int64(), nullable=True, metadata={b"mo.udf.type": b"tampered"})]
            ),
        )
        with self.assertRaisesRegex(ValueError, "logical metadata"):
            worker._validate_input_batch_schema(metadata_changed, valid_schema, [descriptor])

    def test_closing_control_preserves_zero_last_sequence(self):
        fence = {
            "account_id": 1,
            "statement_id": "review",
            "group_id": "group",
            "group_epoch": 1,
            "invocation_id": "zero-sequence",
            "lease_epoch": 1,
        }
        controls = {
            "EndInput": {"last_sequence": 0},
            "Finish": {
                "last_sequence": 0,
                "last_result_sequence": 0,
                "finish_id": "finish",
                "status": "OK",
            },
        }
        for kind, fields in controls.items():
            wire = worker._encode_control(
                {"kind": kind, "tuple": fence, **fields}
            )
            self.assertEqual(0, worker._decode_control(wire)["last_sequence"])
            self.assertIn(b'"last_sequence":0', wire)
            if kind == "Finish":
                self.assertEqual(0, worker._decode_control(wire)["last_result_sequence"])
                self.assertIn(b'"last_result_sequence":0', wire)

    def test_input_consumed_requires_release_credit(self):
        fence = {
            "account_id": 1,
            "statement_id": "statement",
            "group_id": "group",
            "group_epoch": 1,
            "invocation_id": "invocation",
            "lease_epoch": 1,
        }
        value = {
            "kind": "InputConsumed",
            "tuple": fence,
            "sequence": 1,
            "released_bytes": 64,
            "released_batches": 1,
        }
        wire = worker._encode_control(value)
        self.assertEqual(64, worker._decode_control(wire)["released_bytes"])
        self.assertEqual(1, worker._decode_control(wire)["released_batches"])
        value["version"] = worker.PROTOCOL_VERSION
        value.pop("released_batches")
        with self.assertRaisesRegex(ValueError, "missing.*released_batches"):
            worker._decode_control(json.dumps(value).encode())

    def test_zero_temporal_is_distinct_from_null(self):
        descriptor = {"type_id": worker.DATE, "offset_width": 32, "temporal_encoding": "sql_zero_struct"}
        zero = worker.SqlDate(True, None)
        array = worker._output_array([zero, None, worker.SqlDate(False, datetime.date(2024, 1, 2))], descriptor, 3)
        self.assertTrue(array[0].is_valid)
        self.assertFalse(array[1].is_valid)
        self.assertTrue(array[2].is_valid)
        self.assertEqual(True, array.field("is_zero")[0].as_py())
        self.assertEqual(datetime.date(1970, 1, 1), array.field("value")[0].as_py())

    def test_temporal_valid_row_requires_both_child_fields(self):
        descriptor = {"type_id": worker.DATE, "offset_width": 32, "temporal_encoding": "sql_zero_struct"}
        array = pa.StructArray.from_arrays(
            [
                pa.array([None, False], type=pa.bool_()),
                pa.array([datetime.date(1970, 1, 1), datetime.date(2024, 1, 2)], type=pa.date32()),
            ],
            names=["is_zero", "value"],
        )
        with self.assertRaisesRegex(ValueError, "temporal zero flag is null"):
            worker._validate_array_values(array, descriptor)

    def test_json_input_is_canonicalized_before_handler(self):
        descriptor = {"type_id": worker.JSON, "offset_width": 32, "json_encoding": "canonical_text"}
        array = pa.array(['{"b": [true, null, "中"], "a": 1}'], type=pa.string())
        self.assertEqual(
            '{"b":[true,null,"中"],"a":1}',
            worker._scalar_input(array, 0, descriptor),
        )

    def test_json_canonical_text_preserves_number_and_string_tokens(self):
        descriptor = {"type_id": worker.JSON, "offset_width": 32, "json_encoding": "canonical_text"}
        for value in ('1e-7', '1e+20', r'{"escaped":"\u4e2d","number":1e-7}'):
            with self.subTest(value=value):
                self.assertEqual(value, worker._check_scalar(value, descriptor))
                self.assertEqual(value, worker._scalar_input(pa.array([value]), 0, descriptor))
        self.assertEqual('{"space":"a b","number":1e-7}',
                         worker._canonical_json_text(' { "space" : "a b", "number" : 1e-7 } '))

    def test_json_rejects_nonstandard_numbers(self):
        descriptor = {"type_id": worker.JSON, "offset_width": 32, "json_encoding": "canonical_text"}
        self.assertEqual("1e9999", worker._canonical_json_text("1e9999"))
        for value in ("NaN", "Infinity", "-Infinity"):
            with self.assertRaisesRegex(ValueError, "invalid JSON"):
                worker._canonical_json_text(value)
            with self.assertRaisesRegex(ValueError, "JSON result"):
                worker._check_scalar(value, descriptor)

    def test_uuid_scalar_round_trip_keeps_uuid_object_until_array_encoding(self):
        descriptor = {"type_id": worker.UUID, "offset_width": 0}
        value = uuid.UUID("123e4567-e89b-12d3-a456-426614174000")
        checked = worker._check_scalar(value, descriptor)
        self.assertIsInstance(checked, uuid.UUID)
        array = worker._output_array([checked, None], descriptor, 2)
        self.assertEqual(value.bytes, array[0].as_py())
        self.assertIsNone(array[1].as_py())

    def test_scalar_value_domain_is_checked_before_arrow_encoding(self):
        string_descriptor = {"type_id": worker.VARCHAR, "width": 3, "offset_width": 32}
        with self.assertRaisesRegex(ValueError, "string exceeds"):
            worker._output_array(["abcd"], string_descriptor, 1)
        time_descriptor = {"type_id": worker.TIME, "scale": 6, "offset_width": 32}
        with self.assertRaisesRegex(ValueError, "TIME is outside"):
            worker._output_array([datetime.timedelta(hours=839)], time_descriptor, 1)

    def test_sql_string_width_counts_characters(self):
        descriptor = {"type_id": worker.VARCHAR, "width": 1, "offset_width": 32}
        array = worker._output_array(["中"], descriptor, 1)
        self.assertEqual(["中"], array.to_pylist())

    def test_decimal_precision_and_vector_child_validity_are_checked(self):
        decimal_descriptor = {"type_id": worker.DECIMAL128, "width": 3, "scale": 0, "offset_width": 32}
        with self.assertRaisesRegex(ValueError, "precision exceeded"):
            worker._output_array([__import__("decimal").Decimal("1000")], decimal_descriptor, 1)

        vector_descriptor = {"type_id": worker.VECF32, "width": 2, "offset_width": 0}
        child = pa.array([1.0, None, 3.0, 4.0], type=pa.float32())
        vector = pa.FixedSizeListArray.from_arrays(child, 2)
        with self.assertRaisesRegex(ValueError, "child validity"):
            worker._output_array(vector, vector_descriptor, 2)

        valid = pa.array([None, [1.0, 2.0]], type=pa.list_(pa.float32(), 2))
        self.assertEqual(valid.to_pylist(), worker._output_array(valid, vector_descriptor, 2).to_pylist())
        visible = valid.slice(1, 1)
        self.assertEqual(visible.to_pylist(), worker._output_array(visible, vector_descriptor, 1).to_pylist())

    def test_descriptor_rejects_unused_domain_fields(self):
        invalid = [
            {"type_id": worker.DATE, "width": 1, "offset_width": 32, "temporal_encoding": "sql_zero_struct"},
            {"type_id": worker.DATETIME, "width": 1, "offset_width": 32, "temporal_encoding": "sql_zero_struct"},
            {"type_id": worker.TIME, "width": 1, "scale": 6, "offset_width": 32},
            {"type_id": worker.VARCHAR, "width": 16, "scale": 1, "charset": 3, "offset_width": 32},
            {"type_id": worker.VARBINARY, "width": 16, "scale": 1, "charset": 1, "offset_width": 32},
        ]
        for descriptor in invalid:
            with self.subTest(descriptor=descriptor):
                with self.assertRaises(ValueError):
                    worker._field("value", descriptor)

    def test_fencing_tuple_rejects_oversized_identity_components(self):
        for field in ("statement_id", "group_id", "invocation_id"):
            value = {
                "account_id": 1,
                "statement_id": "statement",
                "group_id": "group",
                "group_epoch": 1,
                "invocation_id": "invocation",
                "lease_epoch": 1,
            }
            value[field] = "x" * (worker.MAX_FENCE_COMPONENT_BYTES + 1)
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, "component is too large"):
                    worker._tuple_key(value)

    def test_scalar_vector_null_keeps_fixed_size_child_slots(self):
        descriptor = {"type_id": worker.VECF32, "width": 2, "offset_width": 0}
        array = worker._output_array(
            [memoryview(struct.pack("<ff", 1.0, 2.0)), None,
             memoryview(struct.pack("<ff", 3.0, 4.0))],
            descriptor,
            3,
        )
        self.assertEqual(6, len(array.values))
        self.assertEqual([[1.0, 2.0], None, [3.0, 4.0]], array.to_pylist())

    def test_duplicate_open_cleanup_requires_state_ownership(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        key = (1, "statement", "group", 1, "invocation", 1)
        state = server._admit(key)
        server._finish_invocation(key, None)
        self.assertIn(key, server._active)
        server._finish_invocation(key, state)
        self.assertNotIn(key, server._active)
        self.assertIn(key, server._terminal)

    def test_exchange_cleanup_releases_invocation_when_resource_close_fails(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        key = (1, "statement", "cleanup-group", 1, "cleanup", 1)
        state = server._admit(key)

        class FailingClose:
            def close(self):
                raise RuntimeError("close failed")

        try:
            cleanup_error = server._cleanup_exchange(
                key, state, FailingClose(), FailingClose()
            )
            self.assertIsInstance(cleanup_error, RuntimeError)
            self.assertEqual("close failed", str(cleanup_error))
            self.assertNotIn(key, server._active)
            self.assertIn(key, server._terminal)
            self.assertNotIn((key[0], key[2]), server._active_groups)
        finally:
            server.shutdown()

    def test_exchange_cleanup_retries_a_handler_close_owned_by_the_session(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        key = (1, "statement", "cleanup-retry-group", 1, "cleanup-retry", 1)
        state = server._admit(key)

        class RetryingClose:
            def __init__(self):
                self.calls = 0

            def close(self):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("transient close failed")

        handler = RetryingClose()
        try:
            cleanup_error = server._cleanup_exchange(key, state, None, handler)
            self.assertIsNone(cleanup_error)
            self.assertEqual(2, handler.calls)
            self.assertNotIn(key, server._active)
            self.assertIn(key, server._terminal)
        finally:
            server.shutdown()

    def test_persistent_handler_cleanup_retains_invocation_until_owner_recovers(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        key = (1, "statement", "cleanup-owner-group", 1, "cleanup-owner", 1)
        state = server._admit(key)

        class RetainedSession(worker._HandlerProcessSession):
            def __init__(self):
                self.allow_close = False
                self.calls = 0
                self.closed = threading.Event()

            def close(self):
                self.calls += 1
                if not self.allow_close:
                    raise RuntimeError("persistent close failure")
                self.closed.set()

        session = RetainedSession()
        try:
            cleanup_error = server._cleanup_exchange(key, state, None, session)
            self.assertIsInstance(cleanup_error, RuntimeError)
            self.assertGreaterEqual(session.calls, 2)
            self.assertIn(key, server._active)
            with server._pending_cleanup_condition:
                self.assertEqual(1, len(server._pending_cleanups))

            # The retry owner, rather than a later invocation or a caller
            # retry, is responsible for releasing the active fence.  Opening
            # another invocation with this group must remain impossible until
            # the original resource owner has completed cleanup.
            with self.assertRaisesRegex(ValueError, "execution group epoch is already active"):
                server._admit((1, "statement", key[2], 1, "late", 1))

            session.allow_close = True
            self.assertTrue(session.closed.wait(2), "cleanup owner did not retry")
            deadline = time.monotonic() + 2
            with server._pending_cleanup_condition:
                while key in server._active or server._pending_cleanups:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    server._pending_cleanup_condition.wait(remaining)
            self.assertNotIn(key, server._active)
            self.assertIn(key, server._terminal)
            with server._pending_cleanup_condition:
                self.assertEqual(0, len(server._pending_cleanups))
        finally:
            session.allow_close = True
            server.shutdown()

    def test_failed_native_reader_cleanup_retains_invocation_until_reader_stops(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        key = (1, "statement", "reader-cleanup-group", 1, "reader-cleanup", 1)
        state = server._admit(key)

        class RetainedReader(worker._ExchangeInputReader):
            def __init__(self):
                self.allow_close = False
                self.calls = 0
                self.closed = threading.Event()

            def close(self):
                self.calls += 1
                if not self.allow_close:
                    raise RuntimeError("reader close failed")
                self.closed.set()

        reader = RetainedReader()
        try:
            cleanup_error = server._cleanup_exchange(key, state, reader, None)
            self.assertIsInstance(cleanup_error, RuntimeError)
            self.assertGreaterEqual(reader.calls, 2)
            self.assertIn(key, server._active)
            with server._pending_cleanup_condition:
                self.assertEqual(1, len(server._pending_cleanups))

            reader.allow_close = True
            self.assertTrue(reader.closed.wait(2), "reader cleanup owner did not retry")
            deadline = time.monotonic() + 2
            with server._pending_cleanup_condition:
                while key in server._active or server._pending_cleanups:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    server._pending_cleanup_condition.wait(remaining)
            self.assertNotIn(key, server._active)
            self.assertIn(key, server._terminal)
        finally:
            reader.allow_close = True
            server.shutdown()

    def test_open_rejects_stale_definition_digest_before_admission(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        fence = {
            "account_id": 1,
            "statement_id": "digest",
            "group_id": "digest-group",
            "group_epoch": 1,
            "invocation_id": "stale-artifact",
            "lease_epoch": 1,
        }
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        payload = complete_open_payload(
            {
                "function_ref": {
                    "account_id": 1,
                    "database_id": 2,
                    "function_id": 3,
                    "revision": 1,
                    "namespace_version": 1,
                },
                "source": "def f(ctx, x): return x",
                "handler": "f",
                "mode": worker.MODE_SCALAR,
                "null_policy": worker.NULL_CALL,
                "abi_contract": worker.ABI_CONTRACT,
                "adapter_version": worker.ADAPTER_VERSION,
                "sdk_version": worker.SDK_VERSION,
                "args": [descriptor],
                "return": descriptor,
                "max_batch_bytes": 1 << 20,
                "max_batch_rows": 8,
                "handler_timeout_seconds": 2,
            }
        )
        payload["artifact_digest"] = "0" * 64
        command = worker._encode_control(
            {"kind": "OpenInvocation", "tuple": fence, "payload": payload}
        )
        reader = types.SimpleNamespace(
            schema=None,
            read_chunk=lambda: (_ for _ in ()).throw(StopIteration),
        )
        writer = types.SimpleNamespace(
            begin=lambda schema: None,
            write_metadata=lambda data: None,
            write_with_metadata=lambda record, data: None,
        )
        try:
            with self.assertRaisesRegex(ValueError, "artifact digest"):
                server.do_exchange(
                    types.SimpleNamespace(is_cancelled=lambda: False),
                    types.SimpleNamespace(command=command),
                    reader,
                    writer,
                )
            key = worker._tuple_key(fence)
            self.assertNotIn(key, server._active)
            self.assertNotIn(key, server._terminal)
        finally:
            server.shutdown()

    def test_open_rejects_invalid_invocation_budget_before_admission(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        fence = {
            "account_id": 1,
            "statement_id": "budget",
            "group_id": "budget-group",
            "group_epoch": 1,
            "invocation_id": "invalid-budget",
            "lease_epoch": 1,
        }
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        payload = complete_open_payload(
            {
                "function_ref": {
                    "account_id": 1,
                    "database_id": 2,
                    "function_id": 3,
                    "revision": 1,
                    "namespace_version": 1,
                },
                "source": "def f(ctx, x): return x",
                "handler": "f",
                "mode": worker.MODE_SCALAR,
                "null_policy": worker.NULL_CALL,
                "abi_contract": worker.ABI_CONTRACT,
                "adapter_version": worker.ADAPTER_VERSION,
                "sdk_version": worker.SDK_VERSION,
                "args": [descriptor],
                "return": descriptor,
                "max_batch_bytes": 1 << 20,
                "max_batch_rows": 8,
                "handler_timeout_seconds": 2,
            }
        )
        payload["max_invocation_rows"] = 0
        command = worker._encode_control(
            {"kind": "OpenInvocation", "tuple": fence, "payload": payload}
        )
        reader = types.SimpleNamespace(
            schema=None,
            read_chunk=lambda: (_ for _ in ()).throw(StopIteration),
        )
        writer = types.SimpleNamespace(
            begin=lambda schema: None,
            write_metadata=lambda data: None,
            write_with_metadata=lambda record, data: None,
        )
        try:
            with self.assertRaisesRegex(ValueError, "max_invocation_rows"):
                server.do_exchange(
                    types.SimpleNamespace(is_cancelled=lambda: False),
                    types.SimpleNamespace(command=command),
                    reader,
                    writer,
                )
            key = worker._tuple_key(fence)
            self.assertNotIn(key, server._active)
            self.assertNotIn(key, server._terminal)
        finally:
            server.shutdown()

    def test_invocation_budget_is_cumulative_across_batches(self):
        """A valid first batch cannot make the invocation unbounded."""
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        input_schema = pa.schema([worker._field("arg_0", descriptor)])
        result_schema = pa.schema([worker._field("result", descriptor)])
        first = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int64())], schema=input_schema
        )
        second = pa.RecordBatch.from_arrays(
            [pa.array([2], type=pa.int64())], schema=input_schema
        )
        output = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int64())], schema=result_schema
        )
        output_wire = worker._serialize_record_batch_message(output)
        output_bytes = pa.ipc.get_record_batch_size(output)

        for limit_name, limit, error_text, expected_calls in (
            ("max_invocation_rows", 1, "invocation has too many rows", 1),
            (
                "max_invocation_result_bytes",
                output_bytes,
                "invocation result exceeds byte limit",
                2,
            ),
        ):
            with self.subTest(limit_name=limit_name):
                server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
                fence = {
                    "account_id": 1,
                    "statement_id": "cumulative-budget",
                    "group_id": f"cumulative-{limit_name}",
                    "group_epoch": 1,
                    "invocation_id": f"cumulative-{limit_name}-invocation",
                    "lease_epoch": 1,
                }
                payload = complete_open_payload(
                    {
                        "function_ref": {
                            "account_id": 1,
                            "database_id": 2,
                            "function_id": 3,
                            "revision": 1,
                            "namespace_version": 1,
                        },
                        "mode": worker.MODE_SCALAR,
                        "null_policy": worker.NULL_CALL,
                        "abi_contract": worker.ABI_CONTRACT,
                        "adapter_version": worker.ADAPTER_VERSION,
                        "sdk_version": worker.SDK_VERSION,
                        "args": [descriptor],
                        "return": descriptor,
                        "source": "def f(ctx, x): return x",
                        "handler": "f",
                        "max_batch_bytes": 1 << 20,
                        "max_batch_rows": 1024,
                        "handler_timeout_seconds": 2,
                    }
                )
                payload[limit_name] = limit

                def control(kind, **fields):
                    return worker._encode_control(
                        {"kind": kind, "tuple": fence, **fields}
                    )

                chunks = iter(
                    [
                        types.SimpleNamespace(
                            data=first,
                            app_metadata=control("InputBatch", sequence=1),
                        ),
                        types.SimpleNamespace(
                            data=second,
                            app_metadata=control("InputBatch", sequence=2),
                        ),
                        types.SimpleNamespace(
                            data=None,
                            app_metadata=control("EndInput", last_sequence=2),
                        ),
                    ]
                )
                reader = types.SimpleNamespace(
                    schema=input_schema, read_chunk=lambda: next(chunks)
                )
                writer = types.SimpleNamespace(
                    begin=lambda schema: None,
                    write_metadata=lambda data: None,
                    write_with_metadata=lambda record, data: None,
                )
                session = mock.Mock()
                session.run.return_value = output_wire
                session.should_rollover = False
                command = worker._encode_control(
                    {"kind": "OpenInvocation", "tuple": fence, "payload": payload}
                )
                context = types.SimpleNamespace(is_cancelled=lambda: False)
                key = worker._tuple_key(fence)
                try:
                    with mock.patch.object(
                        worker, "_HandlerProcessSession", return_value=session
                    ):
                        with mock.patch.object(
                            worker._InvocationState, "wait_result_ack"
                        ), mock.patch.object(
                            worker._InvocationState, "wait_finish_ack"
                        ):
                            with self.assertRaisesRegex(ValueError, error_text):
                                server.do_exchange(
                                    context,
                                    types.SimpleNamespace(command=command),
                                    reader,
                                    writer,
                                )
                    self.assertEqual(expected_calls, session.run.call_count)
                    self.assertNotIn(key, server._active)
                    self.assertEqual(
                        worker._TERMINAL_FAILED,
                        server._terminal[key].outcome,
                    )
                finally:
                    server.shutdown()

    def test_handler_process_is_fresh_and_can_be_terminated(self):
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        batch = pa.RecordBatch.from_arrays([pa.array([1], type=pa.int64())], ["arg_0"])
        request = {
            "source": "counter = globals().get('counter', 0) + 1\ndef f(ctx, x): return counter",
            "handler": "f",
            "mode": worker.MODE_SCALAR,
            "null_policy": worker.NULL_CALL,
            "sdk_version": worker.SDK_VERSION,
            "context": None,
            "args": [descriptor],
            "return": descriptor,
            "max_batch_bytes": 1 << 20,
            "input": worker._serialize_record_batch(batch),
        }
        first = worker._deserialize_record_batch(worker._run_handler_process(None, request, 3))
        second = worker._deserialize_record_batch(worker._run_handler_process(None, request, 3))
        self.assertEqual([1], first.column(0).to_pylist())
        self.assertEqual([1], second.column(0).to_pylist())

        request["source"] = "import time\ndef f(ctx, x): time.sleep(5); return x"
        started = time.monotonic()
        with self.assertRaisesRegex(TimeoutError, "handler execution timeout"):
            worker._run_handler_process(None, request, 0.1)
        self.assertLess(time.monotonic() - started, 2)

    def test_record_batch_message_reuses_the_frozen_schema(self):
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        schema = worker._schema_from_descriptors([descriptor], "arg")
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2], type=pa.int64())], schema=schema
        )
        stream_wire = worker._serialize_record_batch(batch)
        message_wire = worker._serialize_record_batch_message(batch)
        self.assertLess(len(message_wire), len(stream_wire))

        decoded = worker._deserialize_record_batch_message(message_wire, schema)
        self.assertEqual([1, 2], decoded.column(0).to_pylist())
        with self.assertRaisesRegex(ValueError, "valid Arrow record batch"):
            worker._deserialize_record_batch_message(
                message_wire,
                worker._schema_from_descriptors(
                    [{"type_id": worker.VARCHAR, "offset_width": 32}], "arg"
                ),
            )
        with self.assertRaisesRegex(ValueError, "valid Arrow record batch"):
            worker._deserialize_record_batch_message(bytes(message_wire) + b"junk", schema)

    def test_handler_frame_keeps_arrow_bytes_out_of_pickle_metadata(self):
        arrow_wire = b"arrow-payload"
        parts, payload_size = worker._encode_execution_request(
            {
                "arrow_encoding": worker.HANDLER_ARROW_RECORD_BATCH,
                "input": arrow_wire,
            }
        )
        self.assertEqual(4, len(parts))
        self.assertEqual(payload_size, len(parts[1]) + len(parts[2]) + len(parts[3]))
        metadata = pickle.loads(parts[2], buffers=[pickle.PickleBuffer(parts[3])])
        self.assertIsInstance(metadata["input"], pickle.PickleBuffer)
        self.assertEqual(arrow_wire, bytes(metadata["input"].raw()))

        compact_parts, compact_size = worker._encode_execution_request(
            {"source": "must-not-repeat", "input": arrow_wire}, compact=True
        )
        compact_metadata = pickle.loads(
            compact_parts[2], buffers=[pickle.PickleBuffer(compact_parts[3])]
        )
        self.assertEqual(
            worker._HANDLER_REQUEST_BATCH,
            compact_metadata[worker._HANDLER_REQUEST_KIND_KEY],
        )
        self.assertNotIn("source", compact_metadata)
        self.assertEqual(arrow_wire, bytes(compact_metadata["input"].raw()))
        self.assertLess(compact_size, payload_size)

    def test_handler_request_reader_accepts_fragmented_headers(self):
        parts, _ = worker._encode_execution_request(
            {
                "arrow_encoding": worker.HANDLER_ARROW_RECORD_BATCH,
                "input": b"arrow-payload",
            }
        )

        class OneByteReader:
            def __init__(self, data):
                self.data = data

            def read(self, size=-1):
                if not self.data:
                    return b""
                count = 1 if size < 0 else min(1, size)
                value, self.data = self.data[:count], self.data[count:]
                return value

        request = worker._read_execution_request(OneByteReader(b"".join(parts)))
        self.assertEqual(worker.HANDLER_ARROW_RECORD_BATCH, request["arrow_encoding"])
        self.assertEqual(b"arrow-payload", bytes(request["input"].raw()))

    def test_handler_request_reader_keeps_readinto_payload_as_pickle_buffer(self):
        parts, _ = worker._encode_execution_request(
            {
                "arrow_encoding": worker.HANDLER_ARROW_RECORD_BATCH,
                "input": b"arrow-payload",
            }
        )
        request = worker._read_execution_request(io.BytesIO(b"".join(parts)))
        self.assertIsInstance(request["input"], pickle.PickleBuffer)
        self.assertEqual(b"arrow-payload", bytes(request["input"].raw()))
        self.assertTrue(request["input"].raw().readonly)

    def test_handler_request_encoder_rejects_non_contiguous_arrow_buffer(self):
        with self.assertRaisesRegex(ValueError, "cannot encode execution request"):
            worker._encode_execution_request(
                {"input": memoryview(b"arrow-payload")[::2]}
            )

    def test_handler_response_frame_separates_status_from_payload(self):
        session = object.__new__(worker._HandlerProcessSession)
        session._response_buffer = bytearray(struct.pack(">Q", 4) + b"\x01abc")
        self.assertEqual(
            (worker._HANDLER_RESPONSE_OK, b"abc"), session._take_response()
        )
        self.assertEqual(bytearray(), session._response_buffer)
        session._response_buffer = bytearray(struct.pack(">Q", 0))
        with self.assertRaisesRegex(ValueError, "empty response"):
            session._take_response()

    def test_handler_response_frame_parts_keep_one_wire_frame(self):
        stream = io.BytesIO()
        worker._write_execution_frame_parts(
            stream, (b"\x01", memoryview(b"abc"))
        )
        self.assertEqual(struct.pack(">Q", 4) + b"\x01abc", stream.getvalue())

    def test_handler_process_accepts_schema_free_record_batch_messages(self):
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        schema = worker._schema_from_descriptors([descriptor], "arg")
        result_schema = pa.schema([worker._field("result", descriptor)])
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2], type=pa.int64())], schema=schema
        )
        request = {
            "source": "def f(ctx, x): return x + 1",
            "handler": "f",
            "mode": worker.MODE_SCALAR,
            "null_policy": worker.NULL_CALL,
            "sdk_version": worker.SDK_VERSION,
            "context": None,
            "args": [descriptor],
            "return": descriptor,
            "max_batch_bytes": 1 << 20,
            "arrow_encoding": worker.HANDLER_ARROW_RECORD_BATCH,
            "input": worker._serialize_record_batch_message(batch),
        }
        output_wire = worker._run_handler_process(None, request, 3)
        output = worker._deserialize_record_batch_message(output_wire, result_schema)
        self.assertEqual([2, 3], output.column(0).to_pylist())

    @unittest.skipUnless(os.name == "posix", "process-group cleanup race")
    def test_handler_error_after_leader_exit_releases_quota(self):
        """A normal handler error must not strand H while its leader exits."""
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int64())], ["arg_0"]
        )
        request = {
            "source": "def f(ctx, x): return 1 / 0",
            "handler": "f",
            "mode": worker.MODE_SCALAR,
            "null_policy": worker.NULL_CALL,
            "sdk_version": worker.SDK_VERSION,
            "context": None,
            "args": [descriptor],
            "return": descriptor,
            "max_batch_bytes": 1 << 20,
            "input": worker._serialize_record_batch(batch),
        }
        quota = worker._HandlerQuota()
        slots = threading.BoundedSemaphore(worker.MAX_HANDLER_PROCESSES)
        for _ in range(worker.MAX_OWNER_HANDLER_PROCESSES * 3):
            session = worker._HandlerProcessSession(
                slots,
                handler_quota=quota,
                account_id=1,
                owner_id="user:2/role:3",
            )
            try:
                with self.assertRaisesRegex(
                    ValueError, "USER_CODE: handler or runtime failed"
                ):
                    session.run(None, request, 3)
                # The response is sent before the handler child exits. Wait
                # for that exact lifecycle edge instead of using a timing
                # sleep, then exercise close() with an already exited leader.
                session._process.wait(timeout=2)
            finally:
                session.close()
            self.assertEqual(({}, {}), quota.counts())
            self.assertEqual(worker.MAX_HANDLER_PROCESSES, slots._value)

    def test_handler_burst_has_explicit_rollover_budget(self):
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        batch = pa.RecordBatch.from_arrays([pa.array([1], type=pa.int64())], ["arg_0"])
        request = {
            "source": "def f(ctx, x): return x",
            "handler": "f",
            "mode": worker.MODE_SCALAR,
            "null_policy": worker.NULL_CALL,
            "sdk_version": worker.SDK_VERSION,
            "context": None,
            "args": [descriptor],
            "return": descriptor,
            "max_batch_bytes": 1 << 20,
            "input": worker._serialize_record_batch(batch),
        }
        old_batches = worker.DEFAULT_BURST_BATCHES
        worker.DEFAULT_BURST_BATCHES = 1
        session = None
        try:
            session = worker._HandlerProcessSession()
            output = worker._deserialize_record_batch(session.run(None, request, 3))
            self.assertEqual([1], output.column(0).to_pylist())
            self.assertTrue(session.should_rollover)
            self.assertGreater(worker.DEFAULT_BURST_BYTES, 0)
            self.assertGreater(worker.DEFAULT_BURST_SECONDS, 0)
        finally:
            if session is not None:
                session.close()
            worker.DEFAULT_BURST_BATCHES = old_batches

    def test_handler_slot_rejection_does_not_create_a_hidden_queue(self):
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int64())], ["arg_0"]
        )
        request = {
            "source": "def f(ctx, x): return x",
            "handler": "f",
            "mode": worker.MODE_SCALAR,
            "null_policy": worker.NULL_CALL,
            "sdk_version": worker.SDK_VERSION,
            "context": None,
            "args": [descriptor],
            "return": descriptor,
            "max_batch_bytes": 1 << 20,
            "input": worker._serialize_record_batch(batch),
        }
        slots = threading.BoundedSemaphore(1)
        self.assertTrue(slots.acquire(blocking=False))
        try:
            with self.assertRaisesRegex(ValueError, "handler execution slots are full"):
                worker._run_handler_process(None, request, 1, slots)
        finally:
            slots.release()

    def test_handler_quota_is_scoped_by_account_and_owner(self):
        quota = worker._HandlerQuota(max_account_handlers=2, max_owner_handlers=1)
        first = quota.acquire(1, "alice")
        with self.assertRaisesRegex(ValueError, "owner handler budget is full"):
            quota.acquire(1, "alice")

        # The account still has capacity for a different owner.
        second = quota.acquire(1, "bob")
        with self.assertRaisesRegex(ValueError, "account handler budget is full"):
            quota.acquire(1, "carol")

        # The same principal name in a different account has an independent
        # budget; tenant identity is part of the owner key.
        third = quota.acquire(2, "alice")
        first.release()
        first.release()
        second.release()
        third.release()
        self.assertEqual(({}, {}), quota.counts())

    def test_handler_initialization_closes_partial_pipe_ownership(self):
        real_pipe = os.pipe
        for failure in ("second_pipe", "watchdog_dup"):
            with self.subTest(failure=failure):
                opened = []
                slots = threading.BoundedSemaphore(1)
                quota = worker._HandlerQuota(max_account_handlers=1, max_owner_handlers=1)

                def allocate_pipe():
                    if failure == "second_pipe" and opened:
                        raise OSError("injected pipe exhaustion")
                    pair = real_pipe()
                    opened.extend(pair)
                    return pair

                try:
                    with mock.patch.object(worker.os, "pipe", side_effect=allocate_pipe), mock.patch.object(
                        worker.os, "dup", side_effect=OSError("injected dup exhaustion")
                    ), mock.patch.object(worker.subprocess, "Popen") as popen:
                        with self.assertRaisesRegex(OSError, "injected"):
                            worker._HandlerProcessSession(slots, handler_quota=quota, account_id=1, owner_id="owner")
                        popen.assert_not_called()
                    self.assertEqual(({}, {}), quota.counts())
                    self.assertTrue(slots.acquire(blocking=False))
                    slots.release()
                    for fd in opened:
                        with self.assertRaises(OSError, msg=f"descriptor {fd} leaked after {failure}"):
                            os.fstat(fd)
                finally:
                    for fd in opened:
                        try:
                            os.close(fd)
                        except OSError:
                            pass

    def test_handler_session_releases_account_owner_quota_idempotently(self):
        quota = worker._HandlerQuota(max_account_handlers=2, max_owner_handlers=1)
        session = worker._HandlerProcessSession(
            threading.BoundedSemaphore(1),
            handler_quota=quota,
            account_id=9,
            owner_id="alice",
        )
        with self.assertRaisesRegex(ValueError, "owner handler budget is full"):
            quota.acquire(9, "alice")
        session.close()
        session.close()
        lease = quota.acquire(9, "alice")
        lease.release()

    def test_handler_close_retries_failed_process_cleanup_without_losing_ownership(self):
        slots = threading.BoundedSemaphore(1)
        self.assertTrue(slots.acquire(blocking=False))
        session = object.__new__(worker._HandlerProcessSession)
        session._slots = slots
        session._slot_acquired = True
        session._process = mock.Mock(stdin=mock.Mock())
        session._watchdog_process = mock.Mock()
        session._selector = mock.Mock()
        session._response_read_fd = -1
        session._parent_watch_write_fd = -1
        session._closed = False
        session._close_lock = threading.Lock()
        process = session._process
        watchdog_process = session._watchdog_process

        try:
            with mock.patch.object(
                worker,
                "_kill_execution_process",
                side_effect=[RuntimeError("kill failed"), None],
            ) as kill:
                session.close()
            self.assertIsNone(session._process)
            self.assertIsNone(session._watchdog_process)
            self.assertIsNone(session._selector)
            self.assertFalse(session._slot_acquired)
            self.assertEqual(2, kill.call_count)
            self.assertTrue(slots.acquire(blocking=False))
            slots.release()
            process.stdin.close.assert_called_once_with()
            watchdog_process.wait.assert_called_once_with(timeout=0.1)
        finally:
            if session._slot_acquired:
                slots.release()

    @unittest.skipUnless(os.name == "posix", "process-group cleanup fallback")
    def test_process_cleanup_falls_back_to_owned_leader_on_group_permission_error(self):
        process = mock.Mock(pid=1234)
        process.poll.return_value = None
        process.wait.return_value = 0

        with mock.patch.object(worker.os, "killpg", side_effect=PermissionError):
            worker._kill_execution_process(process)

        process.kill.assert_called_once_with()
        process.wait.assert_called_once_with(timeout=1.0)

    def test_handler_close_retains_process_for_a_later_cleanup_retry(self):
        session = object.__new__(worker._HandlerProcessSession)
        session._slots = threading.BoundedSemaphore(1)
        session._slot_acquired = False
        session._process = mock.Mock(stdin=None)
        session._watchdog_process = None
        session._selector = None
        session._response_read_fd = -1
        session._parent_watch_write_fd = -1
        session._closed = False
        session._close_lock = threading.Lock()
        process = session._process

        with mock.patch.object(
            worker, "_kill_execution_process", side_effect=RuntimeError("kill failed")
        ) as kill:
            with self.assertRaisesRegex(RuntimeError, "kill failed"):
                session.close()
            self.assertIs(process, session._process)
            self.assertEqual(2, kill.call_count)

        with mock.patch.object(worker, "_kill_execution_process") as kill:
            session.close()
            kill.assert_called_once_with(process)
        self.assertIsNone(session._process)

    def test_handler_close_keeps_h_slot_while_process_cleanup_is_pending(self):
        slots = threading.BoundedSemaphore(1)
        self.assertTrue(slots.acquire(blocking=False))
        session = object.__new__(worker._HandlerProcessSession)
        session._slots = slots
        session._slot_acquired = True
        session._process = mock.Mock(stdin=mock.Mock())
        session._watchdog_process = None
        session._selector = None
        session._response_read_fd = -1
        session._parent_watch_write_fd = -1
        session._closed = False
        session._close_lock = threading.Lock()
        process = session._process

        try:
            with mock.patch.object(
                worker, "_kill_execution_process", side_effect=RuntimeError("kill failed")
            ) as kill:
                with self.assertRaisesRegex(RuntimeError, "kill failed"):
                    session.close()
                self.assertEqual(2, kill.call_count)
            # The pending reaper still owns the live child, so another
            # invocation cannot acquire H merely because close() reported an
            # error.
            self.assertFalse(slots.acquire(blocking=False))
            self.assertTrue(session._slot_acquired)
            self.assertIs(process, session._process)

            with mock.patch.object(worker, "_kill_execution_process") as kill:
                session.close()
                kill.assert_called_once_with(process)
            self.assertFalse(session._slot_acquired)
            self.assertIsNone(session._process)
            self.assertTrue(slots.acquire(blocking=False))
            slots.release()
        finally:
            if session._slot_acquired:
                slots.release()

    def test_handler_cleanup_does_not_mask_execution_error(self):
        session = mock.Mock()
        session.run.side_effect = ValueError("USER_CODE: handler failed")
        session.close.side_effect = RuntimeError("close failed")
        with mock.patch.object(worker, "_HandlerProcessSession", return_value=session):
            with self.assertRaisesRegex(ValueError, "handler failed"):
                worker._run_handler_process(None, {}, 1)
        session.close.assert_called_once_with()

    def test_real_flight_half_close_delivers_delayed_float_result_and_finish(self):
        """Exercise the worker through a real Flight client and server.

        EndInput is sent before the client starts reading results.  The
        worker must treat that as a normal input half-close, preserve the
        result direction, validate a FLOAT64 result, and only finish after
        both result and Finish acknowledgements have been accepted.
        """
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        client = flight.FlightClient(("127.0.0.1", server.port))
        server_thread = threading.Thread(target=server.serve, daemon=True)
        server_thread.start()
        writer = None
        try:
            fence = {
                "account_id": 1,
                "statement_id": "flight-contract",
                "group_id": "flight-group",
                "group_epoch": 1,
                "invocation_id": "delayed-float",
                "lease_epoch": 1,
            }
            int_descriptor = {"type_id": worker.INT64, "offset_width": 32}
            float_descriptor = {"type_id": worker.FLOAT64, "offset_width": 32}
            payload = complete_open_payload({
                "function_ref": {"account_id": 1, "database_id": 2, "function_id": 3, "revision": 1, "namespace_version": 1},
                "source": "def f(ctx, x): return float(x)",
                "handler": "f",
                "mode": worker.MODE_SCALAR,
                "null_policy": worker.NULL_CALL,
                "abi_contract": worker.ABI_CONTRACT,
                "adapter_version": worker.ADAPTER_VERSION,
                "sdk_version": worker.SDK_VERSION,
                "args": [int_descriptor],
                "return": float_descriptor,
                "max_batch_bytes": 1 << 20,
                "max_batch_rows": 8,
                "handler_timeout_seconds": 2,
            })

            def control(kind, **fields):
                return worker._encode_control(
                    {"kind": kind, "tuple": fence, **fields}
                )

            command = control("OpenInvocation", payload=payload)
            writer, reader = client.do_exchange(
                flight.FlightDescriptor.for_command(command),
                options=flight.FlightCallOptions(timeout=5),
            )
            schema = pa.schema([worker._field("arg_0", int_descriptor)])
            writer.begin(schema)
            batch = pa.RecordBatch.from_arrays(
                [pa.array([1, 2], type=pa.int64())], schema=schema
            )
            writer.write_with_metadata(
                batch, control("InputBatch", sequence=1)
            )
            writer.write_metadata(control("EndInput", last_sequence=1))
            writer.done_writing()

            saw_result = False
            saw_finish = False
            for _ in range(8):
                chunk = reader.read_chunk()
                self.assertIsNotNone(chunk)
                if chunk.data is not None and chunk.app_metadata:
                    metadata = worker._decode_control(chunk.app_metadata)
                    if metadata["kind"] != "ResultBatch":
                        continue
                    saw_result = True
                    self.assertEqual([1.0, 2.0], chunk.data.column(0).to_pylist())
                    list(
                        client.do_action(
                            flight.Action(
                                "AcknowledgeResults",
                                control("AcknowledgeResults", ack_sequence=1),
                            )
                        )
                    )
                    continue
                if chunk.app_metadata:
                    metadata = worker._decode_control(chunk.app_metadata)
                    if metadata["kind"] != "Finish":
                        continue
                    saw_finish = True
                    self.assertTrue(saw_result)
                    list(
                        client.do_action(
                            flight.Action(
                                "AcknowledgeFinish",
                                control(
                                    "AcknowledgeFinish",
                                    finish_id=metadata["finish_id"],
                                ),
                            )
                        )
                    )
                    break
            self.assertTrue(saw_result)
            self.assertTrue(saw_finish)
        finally:
            if writer is not None:
                try:
                    writer.close()
                except Exception:
                    pass
            client.close()
            server.shutdown()
            server_thread.join(3)

    def test_real_flight_reuses_handler_within_one_burst(self):
        """A bounded invocation burst compiles the handler only once."""
        with tempfile.TemporaryDirectory(prefix="mo-udf-burst-") as artifact:
            marker = pathlib.Path(artifact) / "loaded"
            server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
            client = flight.FlightClient(("127.0.0.1", server.port))
            server_thread = threading.Thread(target=server.serve, daemon=True)
            server_thread.start()
            writer = None
            try:
                fence = {
                    "account_id": 1,
                    "statement_id": "flight-burst",
                    "group_id": "burst-group",
                    "group_epoch": 1,
                    "invocation_id": "reuse-handler",
                    "lease_epoch": 1,
                }
                descriptor = {"type_id": worker.INT64, "offset_width": 32}
                payload = complete_open_payload({
                    "function_ref": {"account_id": 1, "database_id": 2, "function_id": 3, "revision": 1, "namespace_version": 1},
                    "source": (
                        "import pathlib\n"
                        f"pathlib.Path({str(marker)!r}).write_text('loaded')\n"
                        "def f(ctx, x): return x + 1\n"
                    ),
                    "handler": "f",
                    "mode": worker.MODE_SCALAR,
                    "null_policy": worker.NULL_CALL,
                    "abi_contract": worker.ABI_CONTRACT,
                    "adapter_version": worker.ADAPTER_VERSION,
                    "sdk_version": worker.SDK_VERSION,
                    "args": [descriptor],
                    "return": descriptor,
                    "max_batch_bytes": 1 << 20,
                    "max_batch_rows": 8,
                    "handler_timeout_seconds": 2,
                })

                def control(kind, **fields):
                    return worker._encode_control({"kind": kind, "tuple": fence, **fields})

                writer, reader = client.do_exchange(
                    flight.FlightDescriptor.for_command(
                        control("OpenInvocation", payload=payload)
                    ),
                    options=flight.FlightCallOptions(timeout=5),
                )
                schema = pa.schema([worker._field("arg_0", descriptor)])
                writer.begin(schema)

                def read_result(sequence, expected):
                    while True:
                        chunk = reader.read_chunk()
                        self.assertIsNotNone(chunk)
                        if not chunk.app_metadata:
                            continue
                        metadata = worker._decode_control(chunk.app_metadata)
                        if metadata["kind"] != "ResultBatch":
                            continue
                        self.assertEqual(sequence, metadata["sequence"])
                        self.assertEqual(expected, chunk.data.column(0).to_pylist())
                        list(
                            client.do_action(
                                flight.Action(
                                    "AcknowledgeResults",
                                    control("AcknowledgeResults", ack_sequence=sequence),
                                )
                            )
                        )
                        return

                batch = pa.RecordBatch.from_arrays([pa.array([1], type=pa.int64())], schema=schema)
                writer.write_with_metadata(batch, control("InputBatch", sequence=1))
                read_result(1, [2])
                writer.write_with_metadata(batch, control("InputBatch", sequence=2))
                read_result(2, [2])
                writer.write_metadata(control("EndInput", last_sequence=2))
                writer.done_writing()
                while True:
                    chunk = reader.read_chunk()
                    self.assertIsNotNone(chunk)
                    if not chunk.app_metadata:
                        continue
                    metadata = worker._decode_control(chunk.app_metadata)
                    if metadata["kind"] != "Finish":
                        continue
                    list(
                        client.do_action(
                            flight.Action(
                                "AcknowledgeFinish",
                                control("AcknowledgeFinish", finish_id=metadata["finish_id"]),
                            )
                        )
                    )
                    break
                self.assertEqual("loaded", marker.read_text())
            finally:
                if writer is not None:
                    try:
                        writer.close()
                    except Exception:
                        pass
                client.close()
                server.shutdown()
                server_thread.join(3)

    def test_real_flight_finish_ack_response_loss_is_idempotent(self):
        """An accepted Finish ACK remains SUCCESS when its RPC response is lost."""

        class DropFirstFinishAckServer(worker.RoutineFlightServer):
            def __init__(self, location):
                super().__init__(location)
                self.drop_first_finish_ack = True
                self.terminalized = threading.Event()

            def _finish_invocation(self, key, state):
                super()._finish_invocation(key, state)
                self.terminalized.set()

            def do_action(self, context, action):
                if action.type == "AcknowledgeFinish" and self.drop_first_finish_ack:
                    self.drop_first_finish_ack = False
                    list(super().do_action(context, action))
                    raise RuntimeError("injected Finish ACK response loss")
                yield from super().do_action(context, action)

        server = DropFirstFinishAckServer("grpc://127.0.0.1:0")
        client = flight.FlightClient(("127.0.0.1", server.port))
        server_thread = threading.Thread(target=server.serve, daemon=True)
        server_thread.start()
        writer = None
        retry_client = None
        try:
            fence = {
                "account_id": 1,
                "statement_id": "finish-response-loss",
                "group_id": "finish-group",
                "group_epoch": 1,
                "invocation_id": "response-loss",
                "lease_epoch": 1,
            }
            int_descriptor = {"type_id": worker.INT64, "offset_width": 32}
            float_descriptor = {"type_id": worker.FLOAT64, "offset_width": 32}
            payload = complete_open_payload({
                "function_ref": {"account_id": 1, "database_id": 2, "function_id": 3, "revision": 1, "namespace_version": 1},
                "source": "def f(ctx, x): return float(x)",
                "handler": "f",
                "mode": worker.MODE_SCALAR,
                "null_policy": worker.NULL_CALL,
                "abi_contract": worker.ABI_CONTRACT,
                "adapter_version": worker.ADAPTER_VERSION,
                "sdk_version": worker.SDK_VERSION,
                "args": [int_descriptor],
                "return": float_descriptor,
                "max_batch_bytes": 1 << 20,
                "max_batch_rows": 8,
                "handler_timeout_seconds": 2,
            })

            def control(kind, **fields):
                return worker._encode_control({"kind": kind, "tuple": fence, **fields})

            writer, reader = client.do_exchange(
                flight.FlightDescriptor.for_command(
                    control("OpenInvocation", payload=payload)
                ),
                options=flight.FlightCallOptions(timeout=5),
            )
            schema = pa.schema([worker._field("arg_0", int_descriptor)])
            writer.begin(schema)
            batch = pa.RecordBatch.from_arrays(
                [pa.array([7], type=pa.int64())], schema=schema
            )
            writer.write_with_metadata(batch, control("InputBatch", sequence=1))
            writer.write_metadata(control("EndInput", last_sequence=1))
            writer.done_writing()

            while True:
                chunk = reader.read_chunk()
                self.assertIsNotNone(chunk)
                if not chunk.app_metadata:
                    continue
                metadata = worker._decode_control(chunk.app_metadata)
                if metadata["kind"] == "ResultBatch":
                    self.assertEqual([7.0], chunk.data.column(0).to_pylist())
                    list(
                        client.do_action(
                            flight.Action(
                                "AcknowledgeResults",
                                control("AcknowledgeResults", ack_sequence=1),
                            )
                        )
                    )
                    continue
                if metadata["kind"] != "Finish":
                    continue
                finish_id = metadata["finish_id"]
                with self.assertRaises(Exception):
                    list(
                        client.do_action(
                            flight.Action(
                                "AcknowledgeFinish",
                                control("AcknowledgeFinish", finish_id=finish_id),
                            )
                        )
                    )
                retry_client = flight.FlightClient(("127.0.0.1", server.port))
                retry_ack = list(
                    retry_client.do_action(
                        flight.Action(
                            "AcknowledgeFinish",
                            control("AcknowledgeFinish", finish_id=finish_id),
                        )
                    )
                )
                self.assertTrue(retry_ack)
                break

            key = worker._tuple_key(fence)
            self.assertTrue(server.terminalized.wait(2))
            self.assertEqual(worker._TERMINAL_SUCCESS, server._terminal[key].outcome)
        finally:
            if writer is not None:
                try:
                    writer.close()
                except Exception:
                    pass
            if retry_client is not None:
                retry_client.close()
            client.close()
            server.shutdown()
            server_thread.join(3)

    @unittest.skipUnless(os.name == "posix", "parent-death process-group test")
    def test_worker_death_reaps_handler_process_group(self):
        with tempfile.TemporaryDirectory(prefix="mo-udf-parent-death-") as artifact:
            marker = pathlib.Path(artifact) / "handler-and-descendant.json"
            relay_source = f'''
import importlib.util, pathlib, pyarrow as pa, sys, time
worker_path = pathlib.Path({str(WORKER_PATH)!r})
spec = importlib.util.spec_from_file_location("relay_worker", worker_path)
worker = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = worker
spec.loader.exec_module(worker)
descriptor = {{"type_id": worker.INT64, "offset_width": 32}}
batch = pa.RecordBatch.from_arrays([pa.array([1], type=pa.int64())], ["arg_0"])
request = {{
    "source": {(
        "import json, os, pathlib, subprocess, sys, time\n"
        "def f(ctx, x):\n"
        f"    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)\n"
        f"    pathlib.Path({str(marker)!r}).write_text(json.dumps(dict(handler_pid=os.getpid(), child_pid=child.pid, pgid=os.getpgrp())))\n"
        "    time.sleep(60)\n"
        "    return x\n"
    )!r},
    "handler": "f", "mode": worker.MODE_SCALAR, "null_policy": worker.NULL_CALL,
    "sdk_version": worker.SDK_VERSION, "context": None, "args": [descriptor],
    "return": descriptor, "max_batch_bytes": 1 << 20,
    "input": worker._serialize_record_batch(batch),
}}
worker._run_handler_process(None, request, 60)
'''
            relay = subprocess.Popen(
                [sys.executable, "-c", relay_source],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
            )
            observed = None

            def process_is_running(pid):
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    return False
                try:
                    status = subprocess.check_output(
                        ["ps", "-o", "stat=", "-p", str(pid)],
                        text=True,
                        stderr=subprocess.DEVNULL,
                    ).strip()
                except (OSError, subprocess.CalledProcessError):
                    return True
                return not status.startswith("Z")

            try:
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline and not marker.exists():
                    if relay.poll() is not None:
                        break
                    time.sleep(0.01)
                self.assertTrue(marker.exists(), "handler did not reach the death-watch barrier")
                observed = json.loads(marker.read_text())
                relay.kill()
                relay.wait(timeout=2)

                deadline = time.monotonic() + 3
                while time.monotonic() < deadline and any(
                    process_is_running(observed[key])
                    for key in ("handler_pid", "child_pid")
                ):
                    time.sleep(0.01)
                self.assertFalse(process_is_running(observed["handler_pid"]))
                self.assertFalse(process_is_running(observed["child_pid"]))
            finally:
                if relay.poll() is None:
                    relay.kill()
                    relay.wait(timeout=2)
                if observed is not None and any(
                    process_is_running(observed[key]) for key in ("handler_pid", "child_pid")
                ):
                    try:
                        os.killpg(observed["pgid"], signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    @unittest.skipUnless(os.name == "posix", "independent group watchdog test")
    def test_worker_death_reaps_descendant_after_handler_leader_exits(self):
        """The worker watchdog remains responsible after the leader is gone."""
        with tempfile.TemporaryDirectory(prefix="mo-udf-leader-exit-") as artifact:
            marker = pathlib.Path(artifact) / "handler-and-watchdog.json"
            relay_source = f'''
import importlib.util, json, pathlib, pyarrow as pa, sys, time
worker_path = pathlib.Path({str(WORKER_PATH)!r})
spec = importlib.util.spec_from_file_location("relay_worker_leader_exit", worker_path)
worker = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = worker
spec.loader.exec_module(worker)
descriptor = {{"type_id": worker.INT64, "offset_width": 32}}
batch = pa.RecordBatch.from_arrays([pa.array([1], type=pa.int64())], ["arg_0"])
request = {{
    "source": {(
        "import json, os, pathlib, subprocess, sys\n"
        "def f(ctx, x):\n"
        f"    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
        f"    pathlib.Path({str(marker)!r}).write_text(json.dumps(dict(handler_pid=os.getpid(), child_pid=child.pid, pgid=os.getpgrp())))\n"
        "    os._exit(7)\n"
    )!r},
    "handler": "f", "mode": worker.MODE_SCALAR, "null_policy": worker.NULL_CALL,
    "sdk_version": worker.SDK_VERSION, "context": None, "args": [descriptor],
    "return": descriptor, "max_batch_bytes": 1 << 20,
    "input": worker._serialize_record_batch(batch),
}}
session = worker._HandlerProcessSession()
try:
    session.run(None, request, 60)
except Exception:
    payload = json.loads(pathlib.Path({str(marker)!r}).read_text())
    payload["watchdog_pid"] = session._watchdog_process.pid
    pathlib.Path({str(marker)!r}).write_text(json.dumps(payload))
    time.sleep(60)
'''
            relay = subprocess.Popen(
                [sys.executable, "-c", relay_source],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
            )
            observed = None

            def process_is_running(pid):
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    return False
                try:
                    status = subprocess.check_output(
                        ["ps", "-o", "stat=", "-p", str(pid)],
                        text=True,
                        stderr=subprocess.DEVNULL,
                    ).strip()
                except (OSError, subprocess.CalledProcessError):
                    return True
                return not status.startswith("Z")

            try:
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline and not marker.exists():
                    if relay.poll() is not None:
                        break
                    time.sleep(0.01)
                self.assertTrue(marker.exists(), "handler did not exit after creating its descendant")
                observed = json.loads(marker.read_text())
                self.assertTrue(process_is_running(observed["child_pid"]))
                relay.kill()
                relay.wait(timeout=2)

                deadline = time.monotonic() + 3
                while time.monotonic() < deadline and "watchdog_pid" not in observed:
                    if marker.exists():
                        observed = json.loads(marker.read_text())
                    if "watchdog_pid" not in observed:
                        time.sleep(0.01)
                self.assertIn("watchdog_pid", observed, "watchdog did not register before leader exit")
                while time.monotonic() < deadline and any(
                    process_is_running(observed[key])
                    for key in ("child_pid", "watchdog_pid")
                ):
                    time.sleep(0.01)
                self.assertFalse(process_is_running(observed["child_pid"]))
                self.assertFalse(process_is_running(observed["watchdog_pid"]))
            finally:
                if relay.poll() is None:
                    relay.kill()
                    relay.wait(timeout=2)
                if observed is not None and process_is_running(observed["child_pid"]):
                    try:
                        os.killpg(observed["pgid"], signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    @unittest.skipUnless(os.name == "posix", "handler process-group cleanup")
    def test_parent_watch_read_error_kills_own_group_before_handler_exit(self):
        with (
            mock.patch.object(worker.os, "read", side_effect=OSError("closed")),
            mock.patch.object(worker.os, "getpgrp", return_value=23),
            mock.patch.object(worker.os, "killpg") as killpg,
            mock.patch.object(worker.os, "_exit", side_effect=SystemExit(137)),
        ):
            with self.assertRaises(SystemExit):
                worker._watch_parent_liveness(17)

        killpg.assert_called_once_with(23, worker.signal.SIGKILL)

    def test_handler_stdout_cannot_inject_parent_protocol(self):
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        batch = pa.RecordBatch.from_arrays([pa.array([1], type=pa.int64())], ["arg_0"])
        request = {
            "source": "import os\ndef f(ctx, x): os.write(1, b'\\x00' * 8); return x",
            "handler": "f",
            "mode": worker.MODE_SCALAR,
            "null_policy": worker.NULL_CALL,
            "sdk_version": worker.SDK_VERSION,
            "context": None,
            "args": [descriptor],
            "return": descriptor,
            "max_batch_bytes": 1 << 20,
            "input": worker._serialize_record_batch(batch),
        }
        result = worker._deserialize_record_batch(
            worker._run_handler_process(None, request, 3)
        )
        self.assertEqual([1], result.column(0).to_pylist())

    def test_handler_does_not_inherit_parent_environment(self):
        descriptor = {"type_id": worker.BOOL, "offset_width": 32}
        batch = pa.RecordBatch.from_arrays(
            [pa.array([True], type=pa.bool_())], ["arg_0"]
        )
        request = {
            "source": "import os;\ndef f(ctx, x): return 'MATRIXONE_TEST_SECRET' in os.environ",
            "handler": "f",
            "mode": worker.MODE_SCALAR,
            "null_policy": worker.NULL_CALL,
            "sdk_version": worker.SDK_VERSION,
            "context": None,
            "args": [descriptor],
            "return": descriptor,
            "max_batch_bytes": 1 << 20,
            "input": worker._serialize_record_batch(batch),
        }
        with mock.patch.dict(os.environ, {"MATRIXONE_TEST_SECRET": "must-not-leak"}, clear=False):
            result = worker._deserialize_record_batch(
                worker._run_handler_process(None, request, 3)
            )
        self.assertEqual([False], result.column(0).to_pylist())

    def test_handler_cannot_forge_completion_on_stdout(self):
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        batch = pa.RecordBatch.from_arrays([pa.array([77], type=pa.int64())], ["arg_0"])
        result_batch = pa.RecordBatch.from_arrays(
            [pa.array([77], type=pa.int64())], ["result"]
        )
        frame = struct.pack(">Q", len(bytes([worker._HANDLER_RESPONSE_OK]) + worker._serialize_record_batch(result_batch)))
        frame += bytes([worker._HANDLER_RESPONSE_OK]) + worker._serialize_record_batch(result_batch)
        request = {
            "source": (
                "import os,time\n"
                f"def f(ctx, x):\n    os.write(1, {frame!r})\n"
                "    time.sleep(5)\n"
                "    raise RuntimeError('handler never returned success')\n"
            ),
            "handler": "f",
            "mode": worker.MODE_SCALAR,
            "null_policy": worker.NULL_CALL,
            "sdk_version": worker.SDK_VERSION,
            "context": None,
            "args": [descriptor],
            "return": descriptor,
            "max_batch_bytes": 1 << 20,
            "input": worker._serialize_record_batch(batch),
        }
        with self.assertRaisesRegex(TimeoutError, "handler execution timeout"):
            worker._run_handler_process(None, request, 0.1)

    def test_end_input_rejects_late_batch_without_running_handler(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        fence = {
            "account_id": 1,
            "statement_id": "review",
            "group_id": "group",
            "group_epoch": 1,
            "invocation_id": "late-input",
            "lease_epoch": 1,
        }
        key = worker._tuple_key(fence)
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int64())],
            schema=pa.schema([worker._field("arg_0", descriptor)]),
        )
        payload = complete_open_payload({
            "function_ref": {"account_id": 1, "database_id": 2, "function_id": 3, "revision": 1, "namespace_version": 1},
            "mode": worker.MODE_SCALAR,
            "null_policy": worker.NULL_CALL,
            "abi_contract": worker.ABI_CONTRACT,
            "adapter_version": worker.ADAPTER_VERSION,
            "sdk_version": worker.SDK_VERSION,
            "args": [descriptor],
            "return": descriptor,
            "source": "def f(ctx, x): return x",
            "handler": "f",
            "max_batch_bytes": 1 << 20,
            "max_batch_rows": 1024,
            "handler_timeout_seconds": 2,
        })
        messages = []

        def control(kind, **fields):
            return worker._encode_control(dict(kind=kind, tuple=fence, **fields))

        chunks = iter(
            [
                types.SimpleNamespace(
                    data=None,
                    app_metadata=control("EndInput", last_sequence=0),
                ),
                types.SimpleNamespace(
                    data=batch,
                    app_metadata=control("InputBatch", sequence=1),
                ),
            ]
        )
        reader = types.SimpleNamespace(schema=batch.schema, read_chunk=lambda: next(chunks))

        class Writer:
            def begin(self, schema):
                pass

            def write_metadata(self, data):
                messages.append(worker._decode_control(data))

            def write_with_metadata(self, record, data):
                messages.append(worker._decode_control(data))

        handler = mock.Mock()
        try:
            with mock.patch.object(worker, "_run_handler_process", handler):
                with self.assertRaisesRegex(ValueError, "after EndInput"):
                    server.do_exchange(
                        types.SimpleNamespace(is_cancelled=lambda: False),
                        types.SimpleNamespace(
                            command=control("OpenInvocation", payload=payload)
                        ),
                        reader,
                        Writer(),
                    )
        finally:
            server.shutdown()
        self.assertEqual(0, handler.call_count)

    def test_cancel_interrupts_a_blocked_flight_input_reader(self):
        """Cancellation must release a native read before exchange cleanup."""
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        fence = {
            "account_id": 1,
            "statement_id": "blocked-reader",
            "group_id": "blocked-reader-group",
            "group_epoch": 1,
            "invocation_id": "blocked-reader-invocation",
            "lease_epoch": 1,
        }
        payload = complete_open_payload(
            {
                "function_ref": {
                    "account_id": 1,
                    "database_id": 2,
                    "function_id": 3,
                    "revision": 1,
                    "namespace_version": 1,
                },
                "source": "def f(ctx, x): return x",
                "handler": "f",
                "mode": worker.MODE_SCALAR,
                "null_policy": worker.NULL_CALL,
                "abi_contract": worker.ABI_CONTRACT,
                "adapter_version": worker.ADAPTER_VERSION,
                "sdk_version": worker.SDK_VERSION,
                "args": [descriptor],
                "return": descriptor,
                "max_batch_bytes": 1 << 20,
                "max_batch_rows": 1024,
                "handler_timeout_seconds": 2,
            }
        )

        class BlockingReader:
            schema = None

            def __init__(self):
                self.started = threading.Event()
                self.released = threading.Event()
                self.cancelled = threading.Event()

            def read_chunk(self):
                self.started.set()
                self.released.wait()
                raise StopIteration

            def cancel(self):
                self.cancelled.set()
                self.released.set()

        class Writer:
            def begin(self, schema):
                pass

            def write_metadata(self, data):
                pass

            def write_with_metadata(self, record, data):
                pass

        reader = BlockingReader()
        cancelled = threading.Event()
        context = types.SimpleNamespace(is_cancelled=cancelled.is_set)
        command = worker._encode_control(
            {"kind": "OpenInvocation", "tuple": fence, "payload": payload}
        )
        result = []

        def run_exchange():
            try:
                server.do_exchange(
                    context,
                    types.SimpleNamespace(command=command),
                    reader,
                    Writer(),
                )
            except Exception as exc:  # cancellation is the assertion
                result.append(exc)

        thread = threading.Thread(target=run_exchange)
        thread.start()
        try:
            self.assertTrue(reader.started.wait(2), "native reader did not block")
            cancelled.set()
            thread.join(2)
            self.assertFalse(thread.is_alive(), "cancelled exchange remained blocked")
            self.assertTrue(reader.cancelled.is_set(), "reader cancellation was not requested")
            self.assertEqual(1, len(result))
            self.assertIsInstance(result[0], TimeoutError)
            key = worker._tuple_key(fence)
            self.assertNotIn(key, server._active)
            self.assertEqual(worker._TERMINAL_CANCELLED, server._terminal[key].outcome)
        finally:
            if thread.is_alive():
                cancelled.set()
                reader.cancel()
                thread.join(2)
            server.shutdown()

    def test_cancelled_chunk_handoff_cannot_start_handler(self):
        """A chunk dequeued after cancel is rejected before handler admission."""
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        fence = {
            "account_id": 1,
            "statement_id": "cancelled-handoff",
            "group_id": "cancelled-handoff-group",
            "group_epoch": 1,
            "invocation_id": "cancelled-handoff-invocation",
            "lease_epoch": 1,
        }
        payload = complete_open_payload(
            {
                "function_ref": {
                    "account_id": 1,
                    "database_id": 2,
                    "function_id": 3,
                    "revision": 1,
                    "namespace_version": 1,
                },
                "source": "def f(ctx, x): return x",
                "handler": "f",
                "mode": worker.MODE_SCALAR,
                "null_policy": worker.NULL_CALL,
                "abi_contract": worker.ABI_CONTRACT,
                "adapter_version": worker.ADAPTER_VERSION,
                "sdk_version": worker.SDK_VERSION,
                "args": [descriptor],
                "return": descriptor,
                "max_batch_bytes": 1 << 20,
                "max_batch_rows": 1024,
                "handler_timeout_seconds": 2,
            }
        )
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.int64())],
            schema=pa.schema([worker._field("arg_0", descriptor)]),
        )
        control = worker._encode_control(
            {"kind": "InputBatch", "tuple": fence, "sequence": 1}
        )
        command = worker._encode_control(
            {"kind": "OpenInvocation", "tuple": fence, "payload": payload}
        )
        cancelled = threading.Event()
        next_started = threading.Event()
        release = threading.Event()
        handler = mock.Mock()

        class GatedInput:
            schema = batch.schema

            def __init__(self, _reader):
                pass

            def next(self, _context):
                next_started.set()
                self.assert_release()
                return types.SimpleNamespace(data=batch, app_metadata=control)

            def assert_release(self):
                if not release.wait(2):
                    raise AssertionError("test did not release the input handoff")

            def close(self):
                release.set()

        class Writer:
            def begin(self, schema):
                pass

            def write_metadata(self, data):
                pass

            def write_with_metadata(self, record, data):
                pass

        result = []
        thread = None

        def run_exchange():
            try:
                server.do_exchange(
                    types.SimpleNamespace(is_cancelled=cancelled.is_set),
                    types.SimpleNamespace(command=command),
                    object(),
                    Writer(),
                )
            except Exception as exc:
                result.append(exc)

        try:
            with mock.patch.object(worker, "_ExchangeInputReader", GatedInput), mock.patch.object(
                worker, "_HandlerProcessSession", handler
            ):
                thread = threading.Thread(target=run_exchange)
                thread.start()
                self.assertTrue(next_started.wait(2), "input handoff did not start")
                cancelled.set()
                release.set()
                thread.join(2)
                self.assertFalse(thread.is_alive(), "cancelled handoff remained blocked")
            self.assertEqual(1, len(result))
            self.assertIsInstance(result[0], TimeoutError)
            handler.assert_not_called()
        finally:
            release.set()
            if thread is not None and thread.is_alive():
                cancelled.set()
                thread.join(2)
            server.shutdown()

    def test_exchange_rejects_duplicate_open_control(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        fence = {
            "account_id": 1,
            "statement_id": "review",
            "group_id": "group",
            "group_epoch": 1,
            "invocation_id": "duplicate-open",
            "lease_epoch": 1,
        }
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        payload = complete_open_payload({
            "function_ref": {"account_id": 1, "database_id": 2, "function_id": 3, "revision": 1, "namespace_version": 1},
            "mode": worker.MODE_SCALAR,
            "null_policy": worker.NULL_CALL,
            "abi_contract": worker.ABI_CONTRACT,
            "adapter_version": worker.ADAPTER_VERSION,
            "sdk_version": worker.SDK_VERSION,
            "args": [descriptor],
            "return": descriptor,
            "source": "def f(ctx, x): return x",
            "handler": "f",
            "max_batch_bytes": 1 << 20,
            "max_batch_rows": 1024,
            "handler_timeout_seconds": 2,
        })

        def control(kind, **fields):
            return worker._encode_control(dict(kind=kind, tuple=fence, **fields))

        duplicate = control("OpenInvocation", payload=payload)
        chunks = iter(
            [
                types.SimpleNamespace(data=None, app_metadata=duplicate),
                types.SimpleNamespace(
                    data=None, app_metadata=control("EndInput", last_sequence=1)
                ),
            ]
        )
        reader = types.SimpleNamespace(
            schema=None, read_chunk=lambda: next(chunks)
        )

        class Writer:
            def begin(self, schema):
                pass

            def write_metadata(self, data):
                pass

            def write_with_metadata(self, record, data):
                pass

        try:
            with self.assertRaisesRegex(ValueError, "duplicate OpenInvocation"):
                server.do_exchange(
                    types.SimpleNamespace(is_cancelled=lambda: False),
                    types.SimpleNamespace(command=duplicate),
                    reader,
                    Writer(),
                )
        finally:
            server.shutdown()

    def test_invalid_open_does_not_consume_terminal_ledger(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        fence = {
            "account_id": 1,
            "statement_id": "review",
            "group_id": "group",
            "group_epoch": 1,
            "invocation_id": "invalid-open",
            "lease_epoch": 1,
        }
        payload = complete_open_payload({
            "function_ref": {"account_id": 1, "database_id": 2, "function_id": 3, "revision": 1, "namespace_version": 1},
            "mode": worker.MODE_SCALAR,
            "null_policy": worker.NULL_CALL,
            "abi_contract": worker.ABI_CONTRACT,
            "adapter_version": worker.ADAPTER_VERSION,
            "sdk_version": worker.SDK_VERSION,
            "args": [{"type_id": worker.INT64, "offset_width": 32}],
            "return": {"type_id": 999, "offset_width": 32},
            "source": "def f(ctx, x): return x",
            "handler": "f",
            "max_batch_bytes": 1 << 20,
            "max_batch_rows": 1024,
            "handler_timeout_seconds": 2,
        })
        command = worker._encode_control(
            {"kind": "OpenInvocation", "tuple": fence, "payload": payload}
        )
        reader = types.SimpleNamespace(schema=None, read_chunk=lambda: (_ for _ in ()).throw(StopIteration))
        writer = types.SimpleNamespace(
            begin=lambda schema: None,
            write_metadata=lambda data: None,
            write_with_metadata=lambda record, data: None,
        )
        key = worker._tuple_key(fence)
        try:
            with self.assertRaisesRegex(ValueError, "unsupported type id"):
                server.do_exchange(
                    types.SimpleNamespace(is_cancelled=lambda: False),
                    types.SimpleNamespace(command=command),
                    reader,
                    writer,
                )
            self.assertNotIn(key, server._active)
            self.assertNotIn(key, server._terminal)
        finally:
            server.shutdown()

    def test_exchange_rejects_empty_input_frame(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        fence = {
            "account_id": 1,
            "statement_id": "review",
            "group_id": "group",
            "group_epoch": 1,
            "invocation_id": "empty-frame",
            "lease_epoch": 1,
        }
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        payload = complete_open_payload({
            "function_ref": {"account_id": 1, "database_id": 2, "function_id": 3, "revision": 1, "namespace_version": 1},
            "mode": worker.MODE_SCALAR,
            "null_policy": worker.NULL_CALL,
            "abi_contract": worker.ABI_CONTRACT,
            "adapter_version": worker.ADAPTER_VERSION,
            "sdk_version": worker.SDK_VERSION,
            "args": [descriptor],
            "return": descriptor,
            "source": "def f(ctx, x): return x",
            "handler": "f",
            "max_batch_bytes": 1 << 20,
            "max_batch_rows": 1024,
            "handler_timeout_seconds": 2,
        })

        def control(kind, **fields):
            return worker._encode_control(dict(kind=kind, tuple=fence, **fields))

        chunks = iter(
            [
                types.SimpleNamespace(data=None, app_metadata=b""),
                types.SimpleNamespace(
                    data=None, app_metadata=control("EndInput", last_sequence=0)
                ),
            ]
        )
        reader = types.SimpleNamespace(schema=None, read_chunk=lambda: next(chunks))
        writer = types.SimpleNamespace(
            begin=lambda schema: None,
            write_metadata=lambda data: None,
            write_with_metadata=lambda record, data: None,
        )
        command = control("OpenInvocation", payload=payload)
        try:
            with mock.patch.object(worker._InvocationState, "wait_finish_ack"):
                with self.assertRaisesRegex(ValueError, "empty input frame"):
                    server.do_exchange(
                        types.SimpleNamespace(is_cancelled=lambda: False),
                        types.SimpleNamespace(command=command),
                        reader,
                        writer,
                    )
        finally:
            server.shutdown()

    def test_exchange_rejects_invalid_input_values_before_handler(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        fence = {
            "account_id": 1,
            "statement_id": "review",
            "group_id": "group",
            "group_epoch": 1,
            "invocation_id": "invalid-input-values",
            "lease_epoch": 1,
        }
        descriptor = {"type_id": worker.VECF32, "width": 2, "offset_width": 0}
        child = pa.array([1.0, None], type=pa.float32())
        invalid_vector = pa.FixedSizeListArray.from_arrays(child, 2)
        batch = pa.RecordBatch.from_arrays(
            [invalid_vector],
            schema=pa.schema([worker._field("arg_0", descriptor)]),
        )
        payload = complete_open_payload({
            "function_ref": {"account_id": 1, "database_id": 2, "function_id": 3, "revision": 1, "namespace_version": 1},
            "mode": worker.MODE_VECTOR,
            "null_policy": worker.NULL_CALL,
            "abi_contract": worker.ABI_CONTRACT,
            "adapter_version": worker.ADAPTER_VERSION,
            "sdk_version": worker.SDK_VERSION,
            "args": [descriptor],
            "return": descriptor,
            "source": "def f(ctx, x): return x",
            "handler": "f",
            "max_batch_bytes": 1 << 20,
            "max_batch_rows": 1024,
            "handler_timeout_seconds": 2,
        })

        def control(kind, **fields):
            return worker._encode_control(dict(kind=kind, tuple=fence, **fields))

        chunks = iter(
            [
                types.SimpleNamespace(
                    data=batch,
                    app_metadata=control("InputBatch", sequence=1),
                ),
            ]
        )
        reader = types.SimpleNamespace(schema=batch.schema, read_chunk=lambda: next(chunks))
        writer = types.SimpleNamespace(
            begin=lambda schema: None,
            write_metadata=lambda data: None,
            write_with_metadata=lambda record, data: None,
        )
        command = control("OpenInvocation", payload=payload)
        try:
            handler = mock.Mock()
            with mock.patch.object(worker, "_run_handler_process", handler):
                with self.assertRaisesRegex(ValueError, "child validity"):
                    server.do_exchange(
                        types.SimpleNamespace(is_cancelled=lambda: False),
                        types.SimpleNamespace(command=command),
                        reader,
                        writer,
                    )
            handler.assert_not_called()
        finally:
            server.shutdown()

    @unittest.skipUnless(os.name == "posix", "process-group test")
    def test_descendant_is_killed_after_handler_leader_exits(self):
        with tempfile.TemporaryDirectory(prefix="mo-udf-owned-child-") as artifact:
            witness = pathlib.Path(artifact) / "owned-child.json"
            descendant = "import os,time; time.sleep(0.15); os.close(1); time.sleep(10)"
            source = (
                "import json,os,pathlib,subprocess,sys\n"
                "def f(ctx,x):\n"
                f"    child=subprocess.Popen([sys.executable,'-c',{descendant!r}],stdin=subprocess.DEVNULL)\n"
                f"    pathlib.Path({str(witness)!r}).write_text(json.dumps(dict(pid=child.pid,pgid=os.getpgrp())))\n"
                "    os._exit(7)\n"
            )
            descriptor = {"type_id": worker.INT64, "offset_width": 32}
            batch = pa.RecordBatch.from_arrays([pa.array([1], type=pa.int64())], ["arg_0"])
            request = {
                "source": source,
                "handler": "f",
                "mode": worker.MODE_SCALAR,
                "null_policy": worker.NULL_CALL,
                "sdk_version": worker.SDK_VERSION,
                "context": None,
                "args": [descriptor],
                "return": descriptor,
                "max_batch_bytes": 1 << 20,
                "input": worker._serialize_record_batch(batch),
            }
            try:
                with self.assertRaisesRegex(ValueError, "handler process"):
                    worker._run_handler_process(None, request, 2)
                observed = json.loads(witness.read_text())
                try:
                    survived = os.getpgid(observed["pid"]) == observed["pgid"]
                except ProcessLookupError:
                    survived = False
                self.assertFalse(survived)
            finally:
                if witness.exists():
                    owned = json.loads(witness.read_text())
                    try:
                        if os.getpgid(owned["pid"]) == owned["pgid"]:
                            os.kill(owned["pid"], signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    @unittest.skipUnless(os.name == "posix", "non-blocking pipe test")
    def test_request_write_observes_handler_deadline(self):
        created = []
        entered = threading.Event()
        real_popen = subprocess.Popen

        def no_reader(argv, **kwargs):
            process = real_popen(
                [sys.executable, "-c", "import time; time.sleep(10)"], **kwargs
            )
            created.append(process)
            entered.set()
            return process

        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        batch = pa.RecordBatch.from_arrays([pa.array([1], type=pa.int64())], ["arg_0"])
        request = {
            "source": "def f(ctx, x): return x",
            "handler": "f",
            "mode": worker.MODE_SCALAR,
            "null_policy": worker.NULL_CALL,
            "sdk_version": worker.SDK_VERSION,
            "context": None,
            "args": [descriptor],
            "return": descriptor,
            "max_batch_bytes": 1 << 20,
            "input": worker._serialize_record_batch(batch),
            "probe_padding": b"x" * (1 << 20),
        }
        outcome = []

        def invoke():
            try:
                worker._run_handler_process(None, request, 0.05)
            except Exception as exc:
                outcome.append(type(exc).__name__)

        thread = threading.Thread(target=invoke, daemon=True)
        try:
            with mock.patch.object(worker.subprocess, "Popen", side_effect=no_reader):
                thread.start()
                self.assertTrue(entered.wait(1))
                thread.join(0.5)
                self.assertFalse(thread.is_alive())
            self.assertEqual(["TimeoutError"], outcome)
        finally:
            for process in created:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGKILL)
            thread.join(2)
            for process in created:
                process.wait(timeout=1)
                if process.stdin:
                    process.stdin.close()

    @unittest.skipUnless(os.name == "posix", "handler deadline test")
    def test_handler_deadline_includes_request_serialization(self):
        # The session owns one monotonic timestamp for the whole request. A
        # deterministic clock jump after request encoding proves that serialization
        # cannot consume time outside the handler budget.
        clock = iter((0.0, 0.0, 1.0))
        session = None
        try:
            with mock.patch.object(worker.time, "monotonic", side_effect=clock):
                session = worker._HandlerProcessSession()
                with self.assertRaisesRegex(TimeoutError, "handler execution timeout"):
                    session.run(None, {"input": b"x"}, 0.05)
        finally:
            if session is not None:
                session.close()

    @unittest.skipUnless(os.name == "posix", "non-blocking write budget test")
    def test_handler_request_write_rechecks_budget_within_one_write_event(self):
        class CancelAfterWrites:
            def __init__(self):
                self.writes = 0

            def is_cancelled(self):
                return self.writes >= 2

        class FakeStdin:
            def fileno(self):
                return 123

        class FakeProcess:
            stdin = FakeStdin()

            def poll(self):
                return None

        class FakeSelector:
            def register(self, *args):
                pass

            def unregister(self, *args):
                pass

            def select(self, _timeout):
                return [(types.SimpleNamespace(data="request"), None)]

        context = CancelAfterWrites()
        session = object.__new__(worker._HandlerProcessSession)
        session._closed = False
        session._process = FakeProcess()
        session._selector = FakeSelector()
        session._response_buffer = bytearray()
        session._burst_batches = 0

        request_parts = tuple(b"x" for _ in range(100))

        def write(_fd, data):
            context.writes += 1
            return len(data)

        with mock.patch.object(
            worker, "_encode_execution_request", return_value=(request_parts, 100)
        ), mock.patch.object(worker.os, "write", side_effect=write):
            with self.assertRaisesRegex(TimeoutError, "handler execution cancelled"):
                session.run(context, {"input": b"x"}, 10)

        self.assertLess(context.writes, len(request_parts))

    def test_zero_argument_vector_uses_context_rows(self):
        descriptor = {"type_id": worker.INT64, "offset_width": 32}
        output = pa.array([7, 7, 7], type=pa.int64())
        self.assertEqual(3, len(worker._output_array(output, descriptor, 3)))
        with self.assertRaisesRegex(ValueError, "result length"):
            worker._output_array(pa.array([7], type=pa.int64()), descriptor, 3)

    def test_statement_context_is_whitelisted_and_frozen(self):
        context = worker._statement_context(
            {
                "statement_id": "fence-only",
                "statement_timestamp_utc": "1704067200123456",
                "session_timezone_kind": "FIXED_OFFSET",
                "session_timezone_offset_minutes": "+510",
                "sql_mode": '["ANSI","STRICT"]',
                "current_database": "app",
                "current_user": "alice",
                "current_role": "writer",
                "connection_collation": "utf8mb4_bin",
            }
        )
        self.assertIsInstance(context, worker.StatementContext)
        self.assertEqual(510, context.session_timezone.offset_minutes)
        self.assertEqual(("ANSI", "STRICT"), context.sql_mode)
        self.assertEqual("alice", context.current_user)
        self.assertIsNone(worker._statement_context({"statement_id": "fence-only"}))
        with self.assertRaisesRegex(ValueError, "unsupported statement context"):
            worker._statement_context({"unexpected": "value"})
        with self.assertRaisesRegex(ValueError, "not canonical"):
            worker._statement_context(
                {
                    "statement_timestamp_utc": "1704067200000000",
                    "session_timezone_kind": "FIXED_OFFSET",
                    "session_timezone_offset_minutes": "0",
                    "sql_mode": '["STRICT", "ANSI"]',
                    "current_user": "alice",
                    "connection_collation": "utf8mb4_bin",
                }
            )

    def test_statement_context_rejects_unknown_iana_timezone(self):
        with self.assertRaisesRegex(ValueError, "not present in the local tzdb"):
            worker._statement_context(
                {
                    "statement_timestamp_utc": "1704067200000000",
                    "session_timezone_kind": "IANA",
                    "session_timezone_name": "NoSuch/Zone",
                    "session_timezone_tzdb_version": worker.TIMEZONE_DATABASE_VERSION,
                    "sql_mode": "[]",
                    "current_user": "alice",
                    "connection_collation": "utf8mb4_bin",
                }
            )

        context = worker._statement_context(
            {
                "statement_timestamp_utc": "1704067200000000",
                "session_timezone_kind": "IANA",
                "session_timezone_name": "UTC",
                "session_timezone_tzdb_version": worker.TIMEZONE_DATABASE_VERSION,
                "sql_mode": "[]",
                "current_user": "alice",
                "connection_collation": "utf8mb4_bin",
            }
        )
        self.assertEqual("UTC", context.session_timezone.name)

    def test_statement_context_rejects_stale_iana_timezone_database(self):
        with self.assertRaisesRegex(ValueError, "database version does not match"):
            worker._statement_context(
                {
                    "statement_timestamp_utc": "1704067200000000",
                    "session_timezone_kind": "IANA",
                    "session_timezone_name": "UTC",
                    "session_timezone_tzdb_version": "stale-version",
                    "sql_mode": "[]",
                    "current_user": "alice",
                    "connection_collation": "utf8mb4_bin",
                }
            )

    def test_terminal_admission_does_not_evict_live_tombstones(self):
        old_records = worker.MAX_LEDGER_ENTRIES
        old_bytes = worker.MAX_LEDGER_BYTES
        try:
            worker.MAX_LEDGER_ENTRIES = 1
            worker.MAX_LEDGER_BYTES = 1 << 20
            server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
            first = (1, "statement", "group", 1, "invocation", 1)
            state = server._admit(first)
            with server._lock:
                server._active.pop(first)
                server._active_bytes -= state.terminal_bytes
                server._remember_terminal_locked(first, state)
            with self.assertRaisesRegex(ValueError, "entries are full"):
                server._admit((1, "statement", "other-group", 1, "second", 1))
            self.assertIn(first, server._terminal)
            with server._lock:
                server._terminal[first] = worker._TerminalRecord(
                    0.0, state.terminal_bytes, state.last_result, state.finish_id,
                    worker._TERMINAL_FAILED,
                )
                server._close_group_epoch_locked(first)
                server._purge_terminal_locked(1.0)
            second = server._admit((1, "statement", "other-group", 1, "second", 1))
            self.assertIsNotNone(second)
        finally:
            worker.MAX_LEDGER_ENTRIES = old_records
            worker.MAX_LEDGER_BYTES = old_bytes

    def test_terminal_ledger_capacity_is_bounded_until_fake_clock_expiry(self):
        old_records = worker.MAX_LEDGER_ENTRIES
        old_bytes = worker.MAX_LEDGER_BYTES
        now = [100.0]
        try:
            worker.MAX_LEDGER_ENTRIES = 2
            worker.MAX_LEDGER_BYTES = 1 << 20
            server = worker.RoutineFlightServer(
                "grpc://127.0.0.1:0",
                clock=lambda: now[0],
                terminal_ttl_seconds=10.0,
            )
            first = (1, "statement", "group-first", 1, "first", 1)
            second = (1, "statement", "group-second", 1, "second", 1)
            third = (1, "statement", "group-third", 1, "third", 1)
            for key in (first, second):
                state = server._admit(key)
                state.mark_finish_sent("finish-" + key[4])
                state.ack_finish("finish-" + key[4])
                server._finish_invocation(key, state)

            with self.assertRaisesRegex(ValueError, "ledger entries are full"):
                server._admit(third)
            # Retention is part of the de-duplication contract.  Advancing the
            # test clock is the only operation that makes old identities
            # reclaimable; admission never evicts a live tombstone to make
            # room for a new short invocation.
            now[0] += 11.0
            self.assertIsNotNone(server._admit(third))
        finally:
            worker.MAX_LEDGER_ENTRIES = old_records
            worker.MAX_LEDGER_BYTES = old_bytes

    def test_expired_tombstone_keeps_closed_group_fence(self):
        now = [10.0]
        server = worker.RoutineFlightServer(
            "grpc://127.0.0.1:0", clock=lambda: now[0], terminal_ttl_seconds=1.0
        )
        key = (1, "statement", "closed-group", 1, "first", 1)
        state = server._admit(key)
        server._finish_invocation(key, state)
        now[0] += 2.0
        with self.assertRaisesRegex(ValueError, "group epoch is already closed"):
            server._admit((1, "statement", "closed-group", 1, "late", 1))
        with server._lock:
            server._purge_terminal_locked(now[0])
        self.assertNotIn(key, server._terminal)

    def test_active_groups_reserve_future_closed_fences(self):
        old_entries = worker.MAX_CLOSED_GROUP_ENTRIES
        old_bytes = worker.MAX_CLOSED_GROUP_BYTES
        try:
            worker.MAX_CLOSED_GROUP_ENTRIES = 1
            worker.MAX_CLOSED_GROUP_BYTES = 1 << 20
            server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
            first = (1, "statement", "reserved-first", 1, "first", 1)
            second = (1, "statement", "reserved-second", 1, "second", 1)
            first_state = server._admit(first)
            self.assertIn((first[0], first[2]), server._reserved_groups)
            with self.assertRaisesRegex(ValueError, "closed-group fence is full"):
                server._admit(second)

            server._finish_invocation(first, first_state)
            self.assertNotIn((first[0], first[2]), server._reserved_groups)
            self.assertIn((first[0], first[2]), server._closed_groups)
            with self.assertRaisesRegex(ValueError, "already closed"):
                server._admit((1, "statement", first[2], 1, "late", 1))
        finally:
            worker.MAX_CLOSED_GROUP_ENTRIES = old_entries
            worker.MAX_CLOSED_GROUP_BYTES = old_bytes

    def test_active_group_rejects_a_second_member_before_handler_creation(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        first = (1, "statement", "single-owner", 1, "first", 1)
        first_state = server._admit(first)
        try:
            with self.assertRaisesRegex(ValueError, "execution group epoch is already active"):
                server._admit((1, "statement", "single-owner", 1, "second", 1))
        finally:
            server._finish_invocation(first, first_state)

    def test_group_fence_is_scoped_by_account(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        first = (1, "statement-a", "same-name", 1, "first", 1)
        second = (2, "statement-b", "same-name", 1, "second", 1)
        first_state = server._admit(first)
        second_state = server._admit(second)
        try:
            self.assertIn((1, "same-name"), server._active_groups)
            self.assertIn((2, "same-name"), server._active_groups)
            server._finish_invocation(first, first_state)
            self.assertIn(second, server._active)
            self.assertIn((1, "same-name"), server._closed_groups)
            self.assertNotIn((2, "same-name"), server._closed_groups)
        finally:
            server._finish_invocation(second, second_state)

        # Each account can advance its own epoch independently after the
        # previous same-named group has closed.
        self.assertIsNotNone(
            server._admit((1, "statement-a-2", "same-name", 2, "third", 1))
        )
        self.assertIsNotNone(
            server._admit((2, "statement-b-2", "same-name", 2, "fourth", 1))
        )

    def test_terminal_ack_requires_the_completed_fence(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        key = (1, "statement", "group", 1, "completed", 1)
        state = server._admit(key)
        state.last_result = 2
        state.mark_finish_sent("finish-2")
        state.ack_finish("finish-2")
        server._finish_invocation(key, state)

        tuple_value = {
            "account_id": key[0],
            "statement_id": key[1],
            "group_id": key[2],
            "group_epoch": key[3],
            "invocation_id": key[4],
            "lease_epoch": key[5],
        }
        result_action = types.SimpleNamespace(
            type="AcknowledgeResults",
            body=worker._encode_control(
                {
                    "kind": "AcknowledgeResults",
                    "tuple": tuple_value,
                    "ack_sequence": 2,
                }
            ),
        )
        ack = worker._decode_control(next(server.do_action(None, result_action)))
        self.assertEqual(2, ack["ack_sequence"])

        wrong_action = types.SimpleNamespace(
            type="AcknowledgeResults",
            body=worker._encode_control(
                {
                    "kind": "AcknowledgeResults",
                    "tuple": tuple_value,
                    "ack_sequence": 1,
                }
            ),
        )
        with self.assertRaisesRegex(ValueError, "completed result"):
            next(server.do_action(None, wrong_action))

        finish_action = types.SimpleNamespace(
            type="AcknowledgeFinish",
            body=worker._encode_control(
                {
                    "kind": "AcknowledgeFinish",
                    "tuple": tuple_value,
                    "finish_id": "finish-2",
                }
            ),
        )
        finish_ack = worker._decode_control(next(server.do_action(None, finish_action)))
        self.assertEqual("finish-2", finish_ack["finish_id"])
        # The first Finish ACK may have reached the worker even if its RPC
        # response was lost. A retry is then an idempotent confirmation of
        # the frozen SUCCESS tombstone, never a new execution.
        repeat_ack = worker._decode_control(next(server.do_action(None, finish_action)))
        self.assertEqual("finish-2", repeat_ack["finish_id"])

    def test_late_finish_ack_cannot_promote_unconfirmed_terminal(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        key = (1, "statement", "group", 1, "unconfirmed", 1)
        state = server._admit(key)
        state.mark_finish_sent("finish-unconfirmed")
        server._finish_invocation(key, state)
        self.assertEqual(
            worker._TERMINAL_FINISH_UNCONFIRMED,
            server._terminal[key].outcome,
        )
        tuple_value = {
            "account_id": key[0], "statement_id": key[1], "group_id": key[2],
            "group_epoch": key[3], "invocation_id": key[4], "lease_epoch": key[5],
        }
        action = types.SimpleNamespace(
            type="AcknowledgeFinish",
            body=worker._encode_control({
                "kind": "AcknowledgeFinish", "tuple": tuple_value,
                "finish_id": "finish-unconfirmed",
            }),
        )
        with self.assertRaisesRegex(ValueError, "FINISH_UNCONFIRMED"):
            next(server.do_action(None, action))

    def test_cancellation_after_accepted_finish_ack_keeps_success_idempotent(self):
        state = worker._InvocationState({}, 128)
        state.mark_finish_sent("finish-race")
        state.ack_finish("finish-race")
        state.mark_cancelled()
        self.assertFalse(state.cancelled)
        state.ack_finish("finish-race")
        self.assertEqual(worker._TERMINAL_SUCCESS, state.freeze_terminal())

    def test_cancelled_terminal_is_frozen_and_rejects_late_ack(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        key = (1, "statement", "cancel-group", 1, "cancelled", 1)
        state = server._admit(key)
        state.mark_cancelled()
        with self.assertRaisesRegex(ValueError, "already terminal"):
            state.mark_finish_sent("finish-after-cancel")
        server._finish_invocation(key, state)
        self.assertEqual(worker._TERMINAL_CANCELLED, server._terminal[key].outcome)

        tuple_value = {
            "account_id": key[0], "statement_id": key[1], "group_id": key[2],
            "group_epoch": key[3], "invocation_id": key[4], "lease_epoch": key[5],
        }
        action = types.SimpleNamespace(
            type="AcknowledgeFinish",
            body=worker._encode_control({
                "kind": "AcknowledgeFinish", "tuple": tuple_value,
                "finish_id": "finish-after-cancel",
            }),
        )
        with self.assertRaisesRegex(ValueError, "CANCELLED"):
            next(server.do_action(None, action))
        self.assertEqual(worker._TERMINAL_CANCELLED, server._terminal[key].outcome)

    def test_cancelled_or_shutdown_action_cannot_ack_active_invocation(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        key = (1, "statement", "action-cancel", 1, "active", 1)
        state = server._admit(key)
        state.mark_finish_sent("finish-active")
        tuple_value = {
            "account_id": key[0], "statement_id": key[1], "group_id": key[2],
            "group_epoch": key[3], "invocation_id": key[4], "lease_epoch": key[5],
        }
        action = types.SimpleNamespace(
            type="AcknowledgeFinish",
            body=worker._encode_control({
                "kind": "AcknowledgeFinish", "tuple": tuple_value,
                "finish_id": "finish-active",
            }),
        )
        cancelled_context = types.SimpleNamespace(is_cancelled=lambda: True)
        with self.assertRaisesRegex(TimeoutError, "DEADLINE_EXCEEDED"):
            next(server.do_action(cancelled_context, action))
        self.assertFalse(state.finish_acked)

        server._shutdown_event.set()
        with self.assertRaisesRegex(TimeoutError, "DEADLINE_EXCEEDED"):
            next(server.do_action(None, action))
        self.assertFalse(state.finish_acked)
        state.mark_cancelled()
        server._finish_invocation(key, state)

    def test_active_fence_cannot_be_admitted_twice(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        key = (1, "statement", "group", 1, "invocation", 1)
        server._admit(key)
        with self.assertRaisesRegex(ValueError, "fence is active"):
            server._admit(key)
        with self.assertRaisesRegex(ValueError, "fencing tuple changed"):
            worker._require_tuple(
                {"account_id": 1, "statement_id": "other", "group_id": "group", "group_epoch": 1, "invocation_id": "invocation", "lease_epoch": 1},
                {"account_id": 1, "statement_id": "statement", "group_id": "group", "group_epoch": 1, "invocation_id": "invocation", "lease_epoch": 1},
            )

    def test_control_numeric_fields_are_strict(self):
        tuple_value = {
            "account_id": 0,
            "statement_id": "statement",
            "group_id": "group",
            "group_epoch": 1,
            "invocation_id": "invocation",
            "lease_epoch": 1,
        }
        self.assertEqual(
            (0, "statement", "group", 1, "invocation", 1),
            worker._tuple_key(tuple_value),
        )
        with self.assertRaisesRegex(ValueError, "unsupported control"):
            worker._decode_control(json.dumps({"version": True, "kind": "InputBatch", "tuple": tuple_value}).encode())
        with self.assertRaisesRegex(ValueError, "invalid control field sequence"):
            worker._required_uint64({"sequence": "1"}, "sequence")
        with self.assertRaisesRegex(ValueError, "invalid invocation field timeout"):
            worker._required_positive_float({"timeout": float("nan")}, "timeout", 60.0)

    def test_control_rejects_unknown_envelope_fields(self):
        tuple_value = {
            "account_id": 0,
            "statement_id": "statement",
            "group_id": "group",
            "group_epoch": 1,
            "invocation_id": "invocation",
            "lease_epoch": 1,
        }
        value = {
            "version": 1,
            "kind": "InputBatch",
            "tuple": tuple_value,
            "sequence": 1,
            "future_field": "must not be ignored",
        }
        with self.assertRaisesRegex(ValueError, "unsupported control field"):
            worker._decode_control(json.dumps(value).encode())
        with self.assertRaisesRegex(ValueError, "unsupported control field"):
            worker._encode_control(value)

    def test_control_fields_belong_to_their_kind(self):
        tuple_value = {
            "account_id": 1,
            "statement_id": "statement",
            "group_id": "group",
            "group_epoch": 1,
            "invocation_id": "invocation",
            "lease_epoch": 1,
        }
        input_batch = {
            "version": worker.PROTOCOL_VERSION,
            "kind": "InputBatch",
            "tuple": tuple_value,
            "sequence": 1,
        }
        with self.assertRaisesRegex(ValueError, "not valid for control kind"):
            worker._encode_control({**input_batch, "last_sequence": 1})
        with self.assertRaisesRegex(ValueError, "not valid for control kind"):
            worker._decode_control(
                json.dumps({**input_batch, "last_sequence": 1}).encode()
            )
        with self.assertRaisesRegex(ValueError, "unsupported control kind"):
            worker._decode_control(
                json.dumps(
                    {
                        "version": worker.PROTOCOL_VERSION,
                        "kind": "Unknown",
                        "tuple": tuple_value,
                    }
                ).encode()
            )

    def test_open_invocation_payload_must_be_object(self):
        tuple_value = {
            "account_id": 1,
            "statement_id": "statement",
            "group_id": "group",
            "group_epoch": 1,
            "invocation_id": "invocation",
            "lease_epoch": 1,
        }
        for payload in (None, [], "text"):
            value = {
                "version": worker.PROTOCOL_VERSION,
                "kind": "OpenInvocation",
                "tuple": tuple_value,
                "payload": payload,
            }
            with self.assertRaisesRegex(ValueError, "payload must be an object"):
                worker._decode_control(json.dumps(value).encode())
            with self.assertRaisesRegex(ValueError, "payload must be an object"):
                worker._encode_control(value)

    def test_control_encoder_rejects_nonstandard_json_numbers(self):
        tuple_value = {
            "account_id": 1,
            "statement_id": "statement",
            "group_id": "group",
            "group_epoch": 1,
            "invocation_id": "invocation",
            "lease_epoch": 1,
        }
        for number in (float("nan"), float("inf"), float("-inf")):
            with self.assertRaisesRegex(ValueError, "invalid control JSON"):
                worker._encode_control(
                    {
                        "kind": "OpenInvocation",
                        "tuple": tuple_value,
                        "payload": {"number": number},
                    }
                )

    def test_control_encoder_rejects_missing_required_fields(self):
        tuple_value = {
            "account_id": 1,
            "statement_id": "statement",
            "group_id": "group",
            "group_epoch": 1,
            "invocation_id": "invocation",
            "lease_epoch": 1,
        }
        cases = (
            ("InputBatch", {}, "sequence"),
            ("EndInput", {}, "last_sequence"),
            (
                "Finish",
                {"last_result_sequence": 0, "finish_id": "finish", "status": "OK"},
                "last_sequence",
            ),
            ("AcknowledgeFinish", {}, "finish_id"),
        )
        for kind, fields, field in cases:
            with self.assertRaisesRegex(ValueError, f"missing.*{field}"):
                worker._encode_control(
                    {"kind": kind, "tuple": tuple_value, **fields}
                )

    def test_control_rejects_unknown_fencing_tuple_fields(self):
        tuple_value = {
            "account_id": 0,
            "statement_id": "statement",
            "group_id": "group",
            "group_epoch": 1,
            "invocation_id": "invocation",
            "lease_epoch": 1,
            "future_epoch": 2,
        }
        with self.assertRaisesRegex(ValueError, "unsupported fencing tuple field"):
            worker._tuple_key(tuple_value)
        value = {"version": 1, "kind": "InputBatch", "tuple": tuple_value, "sequence": 1}
        with self.assertRaisesRegex(ValueError, "unsupported fencing tuple field"):
            worker._decode_control(json.dumps(value).encode())

    def test_fencing_tuple_rejects_invalid_utf8(self):
        tuple_value = {
            "account_id": 1,
            "statement_id": "\ud800",
            "group_id": "group",
            "group_epoch": 1,
            "invocation_id": "invocation",
            "lease_epoch": 1,
        }
        with self.assertRaisesRegex(ValueError, "invalid UTF-8"):
            worker._tuple_key(tuple_value)

    def test_control_rejects_duplicate_json_fields(self):
        wire = (
            b'{"version":1,"kind":"InputBatch","tuple":{"account_id":1,'
            b'"statement_id":"statement","group_id":"group","group_epoch":1,'
            b'"invocation_id":"invocation","lease_epoch":1},'
            b'"sequence":1,"sequence":2}'
        )
        with self.assertRaisesRegex(ValueError, "duplicate control JSON field"):
            worker._decode_control(wire)

    def test_result_ack_rejects_zero_sequence(self):
        state = worker._InvocationState({}, 1)
        state.last_result = 1
        with self.assertRaisesRegex(ValueError, "result ACK"):
            state.ack_result(0)

    def test_invocation_state_keeps_input_and_result_directions_independent(self):
        state = worker._InvocationState({}, 1)
        state.record_input(1)
        state.record_input(2)
        self.assertEqual((2, 0, 0), state.sequences())

        state.record_result(1)
        self.assertEqual((2, 1, 0), state.sequences())
        with self.assertRaisesRegex(ValueError, "result sequence"):
            state.record_result(3)
        state.record_result(2)
        self.assertEqual((2, 2, 0), state.sequences())

        state.ack_result(1)
        state.ack_result(2)
        self.assertEqual((2, 2, 2), state.sequences())


if __name__ == "__main__":
    unittest.main()
