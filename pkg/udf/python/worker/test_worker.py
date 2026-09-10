#!/usr/bin/env python3

import datetime
import importlib.util
import json
import os
import pathlib
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


WORKER_PATH = pathlib.Path(__file__).with_name("worker.py")
spec = importlib.util.spec_from_file_location("matrixone_python_worker", WORKER_PATH)
worker = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = worker
spec.loader.exec_module(worker)


class WorkerContractTest(unittest.TestCase):
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

    def test_zero_temporal_is_distinct_from_null(self):
        descriptor = {"type_id": worker.DATE, "offset_width": 32, "temporal_encoding": "sql_zero_struct"}
        zero = worker.SqlDate(True, None)
        array = worker._output_array([zero, None, worker.SqlDate(False, datetime.date(2024, 1, 2))], descriptor, 3)
        self.assertTrue(array[0].is_valid)
        self.assertFalse(array[1].is_valid)
        self.assertTrue(array[2].is_valid)
        self.assertEqual(True, array.field("is_zero")[0].as_py())
        self.assertEqual(datetime.date(1970, 1, 1), array.field("value")[0].as_py())

    def test_json_input_is_canonicalized_before_handler(self):
        descriptor = {"type_id": worker.JSON, "offset_width": 32, "json_encoding": "canonical_text"}
        array = pa.array(['{"b": [true, null, "中"], "a": 1}'], type=pa.string())
        self.assertEqual(
            '{"b":[true,null,"中"],"a":1}',
            worker._scalar_input(array, 0, descriptor),
        )

    def test_json_rejects_nonstandard_numbers(self):
        descriptor = {"type_id": worker.JSON, "offset_width": 32, "json_encoding": "canonical_text"}
        for value in ("NaN", "Infinity", "-Infinity", "1e9999"):
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
        payload = {
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
        payload = {
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
                server._admit((1, "statement", "group", 1, "second", 1))
            self.assertIn(first, server._terminal)
            with server._lock:
                server._terminal[first] = worker._TerminalRecord(
                    0.0, state.terminal_bytes, state.last_result, state.finish_id
                )
                server._purge_terminal_locked(1.0)
            second = server._admit((1, "statement", "group", 1, "second", 1))
            self.assertIsNotNone(second)
        finally:
            worker.MAX_LEDGER_ENTRIES = old_records
            worker.MAX_LEDGER_BYTES = old_bytes

    def test_terminal_ack_requires_the_completed_fence(self):
        server = worker.RoutineFlightServer("grpc://127.0.0.1:0")
        key = (1, "statement", "group", 1, "completed", 1)
        state = server._admit(key)
        state.last_result = 2
        state.finish_id = "finish-2"
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

    def test_result_ack_rejects_zero_sequence(self):
        state = worker._InvocationState({}, 1)
        state.last_result = 1
        with self.assertRaisesRegex(ValueError, "result ACK"):
            state.ack_result(0)


if __name__ == "__main__":
    unittest.main()
