#!/usr/bin/env python3

import datetime
import importlib.util
import json
import pathlib
import sys
import unittest

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

    def test_scalar_value_domain_is_checked_before_arrow_encoding(self):
        string_descriptor = {"type_id": worker.VARCHAR, "width": 3, "offset_width": 32}
        with self.assertRaisesRegex(ValueError, "string exceeds"):
            worker._output_array(["abcd"], string_descriptor, 1)
        time_descriptor = {"type_id": worker.TIME, "scale": 6, "offset_width": 32}
        with self.assertRaisesRegex(ValueError, "TIME is outside"):
            worker._output_array([datetime.timedelta(hours=839)], time_descriptor, 1)

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
        old_records = worker.MAX_TERMINAL_RECORDS
        old_bytes = worker.MAX_TERMINAL_BYTES
        try:
            worker.MAX_TERMINAL_RECORDS = 1
            worker.MAX_TERMINAL_BYTES = 1 << 20
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
                server._terminal[first] = (0.0, state.terminal_bytes)
                server._purge_terminal_locked(1.0)
            second = server._admit((1, "statement", "group", 1, "second", 1))
            self.assertIsNotNone(second)
        finally:
            worker.MAX_TERMINAL_RECORDS = old_records
            worker.MAX_TERMINAL_BYTES = old_bytes

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
            "account_id": 1,
            "statement_id": "statement",
            "group_id": "group",
            "group_epoch": 1,
            "invocation_id": "invocation",
            "lease_epoch": 1,
        }
        with self.assertRaisesRegex(ValueError, "unsupported control"):
            worker._decode_control(json.dumps({"version": True, "kind": "InputBatch", "tuple": tuple_value}).encode())
        with self.assertRaisesRegex(ValueError, "invalid control field sequence"):
            worker._required_uint64({"sequence": "1"}, "sequence")


if __name__ == "__main__":
    unittest.main()
