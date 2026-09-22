#!/usr/bin/env python3
# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Deterministic Sirius TPCH parity/performance campaign orchestration.

The runner command receives one canonical JSON RunSpec in
MO_SIRIUS_CAMPAIGN_RUN and must print one JSON object. Accelerated routes must
report the query-local Sirius execution-stats contract through
``execution_stats``. This interface mirrors the additive Sirius ABI exactly
and is kept explicit while that ABI is integrated into MatrixOne.

This module never supplies a TPCH command and never starts concurrent GPU
work. The real executor is deployment-owned and is injected here.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
from typing import Any, Callable, Iterable, Mapping, Sequence


SCHEMA_VERSION = 1
QUERIES = tuple(range(1, 23))
SCALE_FACTORS = (1, 10)
STREAMS = (1, 2, 4)
PERFORMANCE_STREAMS = 2
MEASURED_SUITES = 5
Q9_REPETITIONS = 10
# Reviewed design v2 section 5: one TAE staging unit is at most 8 MiB,
# immutable cached metadata is at most 32 MiB, and both the aggregate TAE host
# window and result window are 64 MiB.
TAE_HOST_WINDOW_BYTES = 64 << 20
TAE_STAGING_SLICE_BYTES = 8 << 20
TAE_METADATA_CACHE_BYTES = 32 << 20
RESULT_WINDOW_BYTES = 64 << 20
RESULT_SCHEMA_FIELDS = frozenset(("name", "type"))
RESULT_CELL_FIELDS = frozenset(("type", "value"))
FLOAT_SQL_TYPES = frozenset(("double", "float", "float32", "float64", "real"))
ROUTES = (
    {"id": "mo-native", "backend": "native", "scan_mode": "mo"},
    {"id": "flight-tae", "backend": "flight", "scan_mode": "tae"},
    {"id": "flight-mo", "backend": "flight", "scan_mode": "mo"},
    {"id": "embedded-tae", "backend": "embedded", "scan_mode": "tae"},
    {"id": "embedded-mo", "backend": "embedded", "scan_mode": "mo"},
)
REQUIRED_PROVENANCE = (
    "matrixone_revision",
    "sirius_revision",
    "sidecar_revision",
    "sdk_sha256",
    "query_set_sha256",
    "query_sha256",
    "datasets",
    "hardware",
    "config",
)
HARDWARE_FIELDS = ("cpu", "gpu", "host_memory_bytes", "gpu_memory_bytes")
CONFIG_FIELDS = ("sha256", "cuda_version", "driver_version")
REQUIRED_EXECUTION_STATS = (
    "source_mask",
    "terminal",
    "terminal_status",
    "fatal",
    "gpu_tasks_started",
    "gpu_tasks_completed",
    "mo_input_units",
    "mo_input_retained_charged_bytes",
    "mo_input_peak_charged_bytes",
    "mo_input_blocked_acquires",
    "tae_requests",
    "tae_work_issued",
    "tae_work_completed",
    "tae_active_work",
    "tae_peak_active_work",
    "tae_peak_queued_work",
    "tae_work_limit",
    "tae_slice_bytes",
    "tae_peak_cached_metadata_charged_bytes",
    "tae_peak_staging_charged_bytes",
    "tae_gpu_admission_waits",
    "tae_peak_gpu_reservation_admitted_bytes",
    "tae_payload_bytes",
    "result_rows",
    "result_payload_bytes",
    "result_retained_charged_bytes",
    "result_peak_charged_bytes",
    "result_blocked_publications",
    "result_parked_publications",
)


class CampaignError(ValueError):
    pass


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        allow_nan=False,
    )


def _run_spec(
    sequence: int,
    phase: str,
    route: Mapping[str, str],
    query: int,
    streams: int,
    suite: int,
    repetition: int,
    measured: bool,
    scale_factor: int,
    dataset_sha256: str,
    query_sha256: str,
) -> dict[str, Any]:
    return {
        "sequence": sequence,
        "phase": phase,
        "route": route["id"],
        "backend": route["backend"],
        "scan_mode": route["scan_mode"],
        "query": query,
        "query_sha256": query_sha256,
        "scale_factor": scale_factor,
        "dataset_sha256": dataset_sha256,
        "streams": streams,
        "suite": suite,
        "repetition": repetition,
        "measured": measured,
        "fallback_allowed": False,
    }


def build_schedule(provenance: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Build the fixed serial schedule; route rotation removes order bias."""
    schedule: list[dict[str, Any]] = []

    def append(phase: str, route: Mapping[str, str], query: int, streams: int,
               suite: int, repetition: int, measured: bool,
               scale_factor: int) -> None:
        scale = f"SF{scale_factor}"
        schedule.append(_run_spec(
            len(schedule) + 1, phase, route, query, streams, suite,
            repetition, measured, scale_factor,
            provenance["datasets"][scale],
            provenance["query_sha256"][f"Q{query}"],
        ))

    for scale_factor in SCALE_FACTORS:
        # Stream-count controls are correctness/stress evidence, not
        # performance samples. Performance is exclusively streams=2.
        for streams in (1, 4):
            for query in QUERIES:
                for route in ROUTES:
                    append("stream-validation", route, query, streams, 0, 1, False, scale_factor)

        # Suite zero is one excluded warm-up. Five subsequent suites are measured.
        for suite in range(MEASURED_SUITES + 1):
            for query in QUERIES:
                offset = (scale_factor + suite + query - 1) % len(ROUTES)
                ordered = ROUTES[offset:] + ROUTES[:offset]
                for route in ordered:
                    append(
                        "performance", route, query, PERFORMANCE_STREAMS,
                        suite, 1, suite > 0, scale_factor,
                    )

        # Q9's concurrency-sensitive evidence is independent of the five suites.
        for repetition in range(1, Q9_REPETITIONS + 1):
            offset = (scale_factor + repetition - 1) % len(ROUTES)
            for route in ROUTES[offset:] + ROUTES[:offset]:
                append(
                    "q9-stress", route, 9, PERFORMANCE_STREAMS, 0,
                    repetition, True, scale_factor,
                )
    return schedule


def canonical_query_set_digest(query_digests: Mapping[str, str]) -> str:
    return hashlib.sha256(canonical_json(dict(query_digests)).encode("utf-8")).hexdigest()


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _validate_provenance(provenance: Mapping[str, Any]) -> None:
    if not isinstance(provenance, dict):
        raise CampaignError("invalid campaign provenance object")
    if set(provenance) != set(REQUIRED_PROVENANCE):
        missing = sorted(set(REQUIRED_PROVENANCE) - set(provenance))
        unknown = sorted(set(provenance) - set(REQUIRED_PROVENANCE))
        raise CampaignError(f"invalid campaign provenance; missing={missing}, unknown={unknown}")
    for revision in ("matrixone_revision", "sirius_revision", "sidecar_revision"):
        value = provenance[revision]
        if not isinstance(value, str) or len(value) != 40 or any(c not in "0123456789abcdef" for c in value):
            raise CampaignError(f"invalid campaign provenance: {revision}")
    if not _is_sha256(provenance["sdk_sha256"]):
        raise CampaignError("invalid campaign provenance: sdk_sha256")
    queries = provenance["query_sha256"]
    expected_queries = {f"Q{query}" for query in QUERIES}
    if not isinstance(queries, dict) or set(queries) != expected_queries or not all(_is_sha256(value) for value in queries.values()):
        raise CampaignError("invalid campaign provenance: query_sha256")
    if provenance["query_set_sha256"] != canonical_query_set_digest(queries):
        raise CampaignError("invalid campaign provenance: query_set_sha256")
    datasets = provenance["datasets"]
    if not isinstance(datasets, dict) or set(datasets) != {f"SF{scale}" for scale in SCALE_FACTORS} or not all(_is_sha256(value) for value in datasets.values()):
        raise CampaignError("invalid campaign provenance: datasets")
    hardware = provenance["hardware"]
    if not isinstance(hardware, dict) or set(hardware) != set(HARDWARE_FIELDS):
        raise CampaignError("invalid campaign provenance: hardware")
    if (
        not isinstance(hardware["cpu"], str) or not hardware["cpu"]
        or not isinstance(hardware["gpu"], str) or not hardware["gpu"]
        or type(hardware["host_memory_bytes"]) is not int or hardware["host_memory_bytes"] <= 0
        or type(hardware["gpu_memory_bytes"]) is not int or hardware["gpu_memory_bytes"] <= 0
    ):
        raise CampaignError("invalid campaign provenance: hardware")
    config = provenance["config"]
    if not isinstance(config, dict) or set(config) != set(CONFIG_FIELDS):
        raise CampaignError("invalid campaign provenance: config")
    if not _is_sha256(config["sha256"]) or any(
        not isinstance(config[field], str) or not config[field]
        for field in ("cuda_version", "driver_version")
    ):
        raise CampaignError("invalid campaign provenance: config")


def build_campaign(
    provenance: Mapping[str, Any],
    float_tolerances: Mapping[str, Mapping[str, float]] | None = None,
) -> dict[str, Any]:
    _validate_provenance(provenance)
    tolerances = dict(float_tolerances or {})
    for query, tolerance in tolerances.items():
        if query not in {f"Q{number}" for number in QUERIES}:
            raise CampaignError(f"invalid tolerance query {query}")
        if set(tolerance) != {"absolute", "relative"}:
            raise CampaignError(f"invalid tolerance for {query}")
        if any(
            not isinstance(tolerance[field], (int, float))
            or not math.isfinite(tolerance[field])
            or tolerance[field] < 0
            for field in ("absolute", "relative")
        ):
            raise CampaignError(f"negative tolerance for {query}")
    return {
        "schema_version": SCHEMA_VERSION,
        "queries": list(QUERIES),
        "scale_factors": list(SCALE_FACTORS),
        "routes": list(ROUTES),
        "streams": list(STREAMS),
        "performance_streams": PERFORMANCE_STREAMS,
        "warmup_suites": 1,
        "measured_suites": MEASURED_SUITES,
        "q9_repetitions": Q9_REPETITIONS,
        "gpu_concurrency": 1,
        "fallback_allowed": False,
        "result_policy": {
            "default": "typed-exact",
            "float_tolerances": tolerances,
            "decimal": "typed-exact",
            "schema": "exact",
        },
        "performance_gates": {
            "embedded_mo_to_embedded_tae": 2.0,
            "embedded_tae_to_flight_tae": 1.0,
            "embedded_mo_to_flight_mo": 1.0,
        },
        "required_execution_stats": list(REQUIRED_EXECUTION_STATS),
        "provenance": json.loads(canonical_json(provenance)),
        "schedule": build_schedule(provenance),
    }


Executor = Callable[[Mapping[str, Any]], Mapping[str, Any]]


def execute_campaign(campaign: Mapping[str, Any], executor: Executor) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for spec in campaign["schedule"]:
        output = dict(executor(spec))
        record = dict(spec)
        record["output"] = output
        runs.append(record)
    return runs


class SubprocessExecutor:
    def __init__(self, command: Sequence[str], timeout_seconds: float):
        if not command:
            raise CampaignError("runner command is required")
        self.command = tuple(command)
        self.timeout_seconds = timeout_seconds

    def __call__(self, spec: Mapping[str, Any]) -> Mapping[str, Any]:
        if os.name != "posix":
            raise CampaignError("campaign runner process-group isolation requires POSIX")
        environment = os.environ.copy()
        environment["MO_SIRIUS_CAMPAIGN_RUN"] = canonical_json(spec)
        process = subprocess.Popen(
            self.command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=environment,
            start_new_session=True,
        )
        try:
            stdout, stderr = process.communicate(timeout=self.timeout_seconds)
        except subprocess.TimeoutExpired as error:
            self._kill_process_group(process)
            process.communicate()
            raise CampaignError(
                f"runner timed out for sequence {spec['sequence']}"
            ) from error
        except BaseException:
            self._kill_process_group(process)
            process.communicate()
            raise
        # The direct runner is reaped by communicate. Kill any descendant that
        # deliberately closed the inherited pipes and outlived its parent so a
        # completed run cannot contaminate the next serial campaign cell.
        self._kill_process_group(process)
        if process.returncode != 0:
            raise CampaignError(
                f"runner failed for sequence {spec['sequence']}: exit {process.returncode}: "
                f"{stderr.strip()}"
            )
        try:
            result = json.loads(stdout)
        except json.JSONDecodeError as error:
            raise CampaignError(
                f"runner returned invalid JSON for sequence {spec['sequence']}: {error}"
            ) from error
        if not isinstance(result, dict):
            raise CampaignError(f"runner result for sequence {spec['sequence']} is not an object")
        return result

    @staticmethod
    def _kill_process_group(process: subprocess.Popen[str]) -> None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def _sql_type_base(value: str) -> str:
    return value.strip().lower().split("(", 1)[0].strip()


def _validated_result_schema(schema: Any, sequence: int | None = None) -> list[dict[str, str]]:
    location = "" if sequence is None else f" at sequence {sequence}"
    if not isinstance(schema, list) or not schema:
        raise CampaignError(f"invalid result schema{location}")
    validated: list[dict[str, str]] = []
    for column in schema:
        if not isinstance(column, dict) or set(column) != RESULT_SCHEMA_FIELDS:
            raise CampaignError(f"invalid result schema{location}")
        name, sql_type = column["name"], column["type"]
        if (
            not isinstance(name, str)
            or not name
            or "\x00" in name
            or not isinstance(sql_type, str)
            or not sql_type
            or "\x00" in sql_type
        ):
            raise CampaignError(f"invalid result schema{location}")
        validated.append({"name": name, "type": sql_type})
    return validated


def _validate_typed_result(result: Mapping[str, Any], sequence: int) -> None:
    schema = _validated_result_schema(result.get("schema"), sequence)
    rows = result.get("rows")
    if not isinstance(rows, list) or not rows:
        raise CampaignError(f"canonical TPCH result is empty at sequence {sequence}")
    for row_index, row in enumerate(rows):
        if not isinstance(row, list) or len(row) != len(schema):
            raise CampaignError(f"invalid result row at sequence {sequence}/{row_index}")
        for column_index, (column, cell) in enumerate(zip(schema, row)):
            if (
                not isinstance(cell, dict)
                or set(cell) != RESULT_CELL_FIELDS
                or cell["type"] != column["type"]
                or (
                    cell["value"] is not None
                    and type(cell["value"]) not in (bool, int, float, str)
                )
            ):
                raise CampaignError(
                    f"invalid typed result cell at sequence {sequence}/{row_index}/{column_index}"
                )
            value = cell["value"]
            type_base = _sql_type_base(column["type"])
            if type_base in ("decimal", "numeric") and value is not None and not isinstance(value, str):
                raise CampaignError(
                    f"inexact decimal result encoding at sequence {sequence}/{row_index}/{column_index}"
                )
            if (
                type_base in FLOAT_SQL_TYPES
                and value is not None
                and (type(value) not in (int, float) or not math.isfinite(value))
            ):
                raise CampaignError(
                    f"invalid floating result encoding at sequence {sequence}/{row_index}/{column_index}"
                )


def _validate_output(spec: Mapping[str, Any], output: Mapping[str, Any], required_stats: Iterable[str]) -> None:
    evidence = output.get("evidence")
    expected_evidence = {
        "sequence": spec["sequence"],
        "scale_factor": spec["scale_factor"],
        "dataset_sha256": spec["dataset_sha256"],
        "query": spec["query"],
        "query_sha256": spec["query_sha256"],
        "gpu_streams": spec["streams"],
    }
    if not isinstance(evidence, dict) or evidence != expected_evidence:
        raise CampaignError(f"runner identity evidence mismatch at sequence {spec['sequence']}")
    if output.get("backend") != spec["backend"] or output.get("scan_mode") != spec["scan_mode"]:
        raise CampaignError(f"route evidence mismatch at sequence {spec['sequence']}")
    if output.get("fallback") is not False:
        raise CampaignError(f"fallback observed at sequence {spec['sequence']}")
    wall = output.get("wall_seconds")
    first_row = output.get("first_row_seconds")
    cpu = output.get("cpu_seconds")
    if not isinstance(wall, (int, float)) or not math.isfinite(wall) or wall <= 0:
        raise CampaignError(f"invalid wall time at sequence {spec['sequence']}")
    if (
        not isinstance(first_row, (int, float))
        or not math.isfinite(first_row)
        or first_row < 0
        or first_row > wall
    ):
        raise CampaignError(f"invalid first-row time at sequence {spec['sequence']}")
    if not isinstance(cpu, (int, float)) or not math.isfinite(cpu) or cpu < 0:
        raise CampaignError(f"invalid CPU time at sequence {spec['sequence']}")
    memory_peaks = output.get("memory_peaks")
    if not isinstance(memory_peaks, dict) or set(memory_peaks) != {
        "go_bytes", "native_host_bytes", "pinned_host_bytes", "gpu_bytes",
    }:
        raise CampaignError(f"invalid memory peak evidence at sequence {spec['sequence']}")
    if any(type(value) is not int or value < 0 for value in memory_peaks.values()):
        raise CampaignError(f"invalid memory peak evidence at sequence {spec['sequence']}")
    if output.get("cancellation_origin") != "none":
        raise CampaignError(f"unexpected cancellation at sequence {spec['sequence']}")
    expected_health = "not-applicable" if spec["route"] == "mo-native" else "accepting"
    if output.get("terminal_health") != expected_health:
        raise CampaignError(f"terminal health mismatch at sequence {spec['sequence']}")
    result = output.get("result")
    if not isinstance(result, dict) or "schema" not in result or "rows" not in result:
        raise CampaignError(f"missing typed result at sequence {spec['sequence']}")
    _validate_typed_result(result, spec["sequence"])
    if spec["route"] == "mo-native":
        return
    stats = output.get("execution_stats")
    if not isinstance(stats, dict):
        raise CampaignError(f"missing execution stats at sequence {spec['sequence']}")
    missing = [field for field in required_stats if field not in stats]
    if missing:
        raise CampaignError(
            f"incomplete execution stats at sequence {spec['sequence']}: {', '.join(missing)}"
        )
    for field in required_stats:
        if type(stats[field]) is not int or stats[field] < 0:
            raise CampaignError(f"invalid execution stat {field} at sequence {spec['sequence']}")
    started = stats["gpu_tasks_started"]
    completed = stats["gpu_tasks_completed"]
    expected_source = 1 if spec["scan_mode"] == "mo" else 2
    if (
        stats["terminal"] != 1
        or stats["terminal_status"] != 0
        or stats["fatal"] != 0
        or stats["source_mask"] != expected_source
        or started <= 0
        or completed != started
    ):
        raise CampaignError(f"missing successful GPU execution evidence at sequence {spec['sequence']}")
    rows = result["rows"]
    if not isinstance(rows, list) or stats["result_rows"] != len(rows):
        raise CampaignError(f"result-row execution evidence mismatch at sequence {spec['sequence']}")
    if (
        stats["result_rows"] <= 0
        or stats["result_payload_bytes"] <= 0
        or stats["result_peak_charged_bytes"] <= 0
        or stats["result_peak_charged_bytes"] > RESULT_WINDOW_BYTES
        or stats["result_retained_charged_bytes"] != 0
        or stats["result_parked_publications"] != 0
    ):
        raise CampaignError(f"invalid result retention evidence at sequence {spec['sequence']}")
    if spec["scan_mode"] == "mo":
        if (
            stats["mo_input_units"] <= 0
            or stats["mo_input_peak_charged_bytes"] <= 0
            or stats["mo_input_retained_charged_bytes"] != 0
            or any(stats[field] != 0 for field in (
                "tae_requests", "tae_work_issued", "tae_work_completed",
                "tae_active_work", "tae_peak_active_work", "tae_peak_queued_work",
                "tae_work_limit", "tae_slice_bytes",
                "tae_peak_cached_metadata_charged_bytes",
                "tae_peak_staging_charged_bytes", "tae_gpu_admission_waits",
                "tae_peak_gpu_reservation_admitted_bytes", "tae_payload_bytes",
            ))
        ):
            raise CampaignError(f"MO source execution evidence mismatch at sequence {spec['sequence']}")
    else:
        expected_limit = max(2, 2 * spec["streams"])
        maximum_slice = min(
            TAE_STAGING_SLICE_BYTES,
            TAE_HOST_WINDOW_BYTES // expected_limit,
        )
        if (
            stats["mo_input_units"] != 0
            or stats["mo_input_retained_charged_bytes"] != 0
            or stats["mo_input_peak_charged_bytes"] != 0
            or stats["mo_input_blocked_acquires"] != 0
            or stats["tae_requests"] <= 0
            or stats["tae_work_issued"] <= 0
            or stats["tae_work_completed"] != stats["tae_work_issued"]
            or stats["tae_active_work"] != 0
            or stats["tae_work_limit"] != expected_limit
            or stats["tae_peak_active_work"] < 1
            or stats["tae_peak_active_work"] > expected_limit
            or stats["tae_slice_bytes"] <= 0
            or stats["tae_slice_bytes"] > maximum_slice
            or stats["tae_peak_cached_metadata_charged_bytes"] <= 0
            or stats["tae_peak_cached_metadata_charged_bytes"] > TAE_METADATA_CACHE_BYTES
            or stats["tae_peak_staging_charged_bytes"] <= 0
            or stats["tae_peak_staging_charged_bytes"] > TAE_HOST_WINDOW_BYTES
            or stats["tae_peak_gpu_reservation_admitted_bytes"] <= 0
            or stats["tae_payload_bytes"] <= 0
        ):
            raise CampaignError(f"TAE source execution evidence mismatch at sequence {spec['sequence']}")


def _compare_typed(expected: Any, actual: Any, tolerance: Mapping[str, float] | None, path: str = "result") -> None:
    if type(expected) is not type(actual):  # bool must not compare equal to int.
        raise CampaignError(f"typed result mismatch at {path}")
    if isinstance(expected, float):
        if not math.isfinite(expected) or not math.isfinite(actual):
            raise CampaignError(f"non-finite result at {path}")
        if tolerance is None:
            if expected != actual:
                raise CampaignError(f"exact floating result mismatch at {path}")
            return
        if not math.isclose(
            expected,
            actual,
            rel_tol=tolerance["relative"],
            abs_tol=tolerance["absolute"],
        ):
            raise CampaignError(f"floating result mismatch at {path}")
        return
    if isinstance(expected, list):
        if len(expected) != len(actual):
            raise CampaignError(f"result length mismatch at {path}")
        for index, (left, right) in enumerate(zip(expected, actual)):
            _compare_typed(left, right, tolerance, f"{path}[{index}]")
        return
    if isinstance(expected, dict):
        if expected.keys() != actual.keys():
            raise CampaignError(f"result field mismatch at {path}")
        for key in expected:
            _compare_typed(expected[key], actual[key], tolerance, f"{path}.{key}")
        return
    if expected != actual:
        raise CampaignError(f"exact result mismatch at {path}")


def _compare_results(
    expected: Mapping[str, Any],
    actual: Mapping[str, Any],
    tolerance: Mapping[str, float] | None,
) -> None:
    _validate_typed_result(expected, 0)
    _validate_typed_result(actual, 0)
    expected_schema = _validated_result_schema(expected.get("schema"))
    actual_schema = _validated_result_schema(actual.get("schema"))
    _compare_typed(expected_schema, actual_schema, None, "result.schema")
    expected_rows, actual_rows = expected.get("rows"), actual.get("rows")
    if not isinstance(expected_rows, list) or not isinstance(actual_rows, list):
        raise CampaignError("invalid result rows")
    if len(expected_rows) != len(actual_rows):
        raise CampaignError("result length mismatch at result.rows")
    for row_index, (expected_row, actual_row) in enumerate(zip(expected_rows, actual_rows)):
        if not isinstance(expected_row, list) or not isinstance(actual_row, list):
            raise CampaignError(f"invalid result row at result.rows[{row_index}]")
        if len(expected_row) != len(expected_schema) or len(actual_row) != len(actual_schema):
            raise CampaignError(f"result length mismatch at result.rows[{row_index}]")
        for column_index, (column, expected_cell, actual_cell) in enumerate(
            zip(expected_schema, expected_row, actual_row)
        ):
            path = f"result.rows[{row_index}][{column_index}]"
            if not isinstance(expected_cell, dict) or not isinstance(actual_cell, dict):
                raise CampaignError(f"invalid typed result cell at {path}")
            _compare_typed(expected_cell.get("type"), actual_cell.get("type"), None, f"{path}.type")
            cell_tolerance = (
                tolerance
                if _sql_type_base(column["type"]) in FLOAT_SQL_TYPES
                else None
            )
            _compare_typed(
                expected_cell.get("value"), actual_cell.get("value"),
                cell_tolerance, f"{path}.value",
            )


def validate_runs(campaign: Mapping[str, Any], runs: Sequence[Mapping[str, Any]]) -> None:
    schedule = campaign["schedule"]
    if len(runs) != len(schedule):
        raise CampaignError(f"incomplete campaign: expected {len(schedule)} runs, got {len(runs)}")
    by_cell: dict[tuple[Any, ...], Mapping[str, Any]] = {}
    required_stats = campaign["required_execution_stats"]
    for expected, record in zip(schedule, runs):
        spec = {key: record.get(key) for key in expected}
        if spec != expected:
            raise CampaignError(f"campaign order/spec mismatch at sequence {expected['sequence']}")
        output = record.get("output")
        if not isinstance(output, dict):
            raise CampaignError(f"missing output at sequence {expected['sequence']}")
        _validate_output(expected, output, required_stats)
        cell = (
            expected["scale_factor"], expected["phase"], expected["streams"], expected["suite"],
            expected["repetition"], expected["query"], expected["route"],
        )
        if cell in by_cell:
            raise CampaignError(f"duplicate campaign cell at sequence {expected['sequence']}")
        by_cell[cell] = output

    tolerances = campaign["result_policy"]["float_tolerances"]
    for expected in schedule:
        if expected["route"] == "mo-native":
            continue
        base = (
            expected["scale_factor"], expected["phase"], expected["streams"], expected["suite"],
            expected["repetition"], expected["query"], "mo-native",
        )
        actual = (
            expected["scale_factor"], expected["phase"], expected["streams"], expected["suite"],
            expected["repetition"], expected["query"], expected["route"],
        )
        if base not in by_cell:
            raise CampaignError(f"missing native result oracle for sequence {expected['sequence']}")
        _compare_results(
            by_cell[base]["result"], by_cell[actual]["result"],
            tolerances.get(f"Q{expected['query']}"),
        )


def _median(values: Iterable[float]) -> float:
    materialized = list(values)
    if not materialized:
        raise CampaignError("missing measured timing samples")
    return float(statistics.median(materialized))


def summarize(campaign: Mapping[str, Any], runs: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    validate_runs(campaign, runs)
    routes = [route["id"] for route in ROUTES]
    query_samples: dict[tuple[int, str, int], list[float]] = {}
    suite_samples: dict[tuple[int, str, int], list[float]] = {}
    q9_samples: dict[tuple[int, str], list[float]] = {}
    for record in runs:
        wall = float(record["output"]["wall_seconds"])
        route = record["route"]
        scale = record["scale_factor"]
        if record["phase"] == "performance" and record["measured"]:
            query_samples.setdefault((scale, route, record["query"]), []).append(wall)
            suite_samples.setdefault((scale, route, record["suite"]), []).append(wall)
        elif record["phase"] == "q9-stress":
            q9_samples.setdefault((scale, route), []).append(wall)

    gates = campaign["performance_gates"]
    checks: list[dict[str, Any]] = []

    def ratio_gate(name: str, numerator: float, denominator: float, limit: float) -> None:
        if denominator <= 0:
            raise CampaignError(f"invalid gate denominator for {name}")
        ratio = numerator / denominator
        checks.append({"name": name, "ratio": ratio, "limit": limit, "passed": ratio <= limit})

    scales: dict[str, Any] = {}
    for scale in SCALE_FACTORS:
        summary_routes: dict[str, Any] = {}
        for route in routes:
            query_medians = {
                f"Q{query}": _median(query_samples.get((scale, route, query), []))
                for query in QUERIES
            }
            suite_totals = [
                sum(suite_samples.get((scale, route, suite), []))
                for suite in range(1, MEASURED_SUITES + 1)
            ]
            if any(len(suite_samples.get((scale, route, suite), [])) != len(QUERIES)
                   for suite in range(1, MEASURED_SUITES + 1)):
                raise CampaignError(f"incomplete measured suite for SF{scale}/{route}")
            summary_routes[route] = {
                "query_medians": query_medians,
                "sum_query_medians": sum(query_medians.values()),
                "median_full_suite": _median(suite_totals),
                "q9_stress_median": _median(q9_samples.get((scale, route), [])),
            }
        first_gate = len(checks)
        for metric in ("sum_query_medians", "median_full_suite", "q9_stress_median"):
            ratio_gate(
                f"SF{scale}/embedded-mo/{metric}/embedded-tae",
                summary_routes["embedded-mo"][metric],
                summary_routes["embedded-tae"][metric],
                gates["embedded_mo_to_embedded_tae"],
            )
            ratio_gate(
                f"SF{scale}/embedded-tae/{metric}/flight-tae",
                summary_routes["embedded-tae"][metric],
                summary_routes["flight-tae"][metric],
                gates["embedded_tae_to_flight_tae"],
            )
            ratio_gate(
                f"SF{scale}/embedded-mo/{metric}/flight-mo",
                summary_routes["embedded-mo"][metric],
                summary_routes["flight-mo"][metric],
                gates["embedded_mo_to_flight_mo"],
            )
        scale_checks = checks[first_gate:]
        scales[f"SF{scale}"] = {
            "routes": summary_routes,
            "gates": scale_checks,
            "passed": all(check["passed"] for check in scale_checks),
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "scales": scales,
        "gates": checks,
        "passed": all(check["passed"] for check in checks),
    }


def _format_seconds(value: float) -> str:
    return f"{value:.6f}"


def summary_csv(summary: Mapping[str, Any]) -> str:
    target = io.StringIO(newline="")
    headings = ["scale_factor", "route", "backend", "scan_mode"] + [f"Q{q}" for q in QUERIES] + [
        "sum_query_medians", "median_full_suite", "q9_stress_median", "gates_passed",
    ]
    writer = csv.DictWriter(target, fieldnames=headings, lineterminator="\n")
    writer.writeheader()
    by_id = {route["id"]: route for route in ROUTES}
    for scale in SCALE_FACTORS:
        for route in by_id:
            values = summary["scales"][f"SF{scale}"]["routes"][route]
            row: dict[str, Any] = {
                "scale_factor": scale,
                "route": route,
                "backend": by_id[route]["backend"],
                "scan_mode": by_id[route]["scan_mode"],
                "sum_query_medians": _format_seconds(values["sum_query_medians"]),
                "median_full_suite": _format_seconds(values["median_full_suite"]),
                "q9_stress_median": _format_seconds(values["q9_stress_median"]),
                "gates_passed": str(summary["scales"][f"SF{scale}"]["passed"]).lower(),
            }
            row.update({query: _format_seconds(value) for query, value in values["query_medians"].items()})
            writer.writerow(row)
    return target.getvalue()


def summary_markdown(summary: Mapping[str, Any]) -> str:
    columns = ["Route"] + [f"Q{query}" for query in QUERIES] + [
        "Sum of medians", "Median full suite", "Q9 x10 median",
    ]
    lines = ["# Sirius TPCH parity summary"]
    for scale in SCALE_FACTORS:
        lines.extend(["", f"## SF{scale}", "", " | ".join(columns), " | ".join(["---"] * len(columns))])
        for route in (item["id"] for item in ROUTES):
            values = summary["scales"][f"SF{scale}"]["routes"][route]
            line = [route]
            line.extend(_format_seconds(values["query_medians"][f"Q{query}"]) for query in QUERIES)
            line.extend([
                _format_seconds(values["sum_query_medians"]),
                _format_seconds(values["median_full_suite"]),
                _format_seconds(values["q9_stress_median"]),
            ])
            lines.append(" | ".join(line))
        lines.extend(["", f"### SF{scale} relative gates", ""])
        for gate in summary["scales"][f"SF{scale}"]["gates"]:
            state = "PASS" if gate["passed"] else "FAIL"
            lines.append(
                f"- {state}: {gate['name']} = {gate['ratio']:.6f}, limit {gate['limit']:.6f}"
            )
    lines.extend(["", f"Overall: {'PASS' if summary['passed'] else 'FAIL'}", ""])
    return "\n".join(lines)


def write_artifacts(
    campaign: Mapping[str, Any],
    runs: Sequence[Mapping[str, Any]],
    output_directory: Path,
) -> dict[str, Any]:
    summary = summarize(campaign, runs)
    output_directory.mkdir(parents=True, exist_ok=True)
    (output_directory / "campaign.json").write_text(canonical_json(campaign) + "\n", encoding="utf-8")
    raw = "".join(canonical_json(_artifact_run(campaign, record)) + "\n" for record in runs)
    (output_directory / "runs.jsonl").write_text(raw, encoding="utf-8")
    (output_directory / "summary.csv").write_text(summary_csv(summary), encoding="utf-8")
    (output_directory / "summary.md").write_text(summary_markdown(summary), encoding="utf-8")
    return summary


def _artifact_run(campaign: Mapping[str, Any], record: Mapping[str, Any]) -> dict[str, Any]:
    """Return the strict, redacted runs.jsonl record.

    Typed rows remain available to validate_runs in memory but are never
    persisted. Unknown runner fields are intentionally dropped so credentials,
    object paths, query text, or other deployment-local material cannot leak.
    """
    output = record["output"]
    result = output["result"]
    result_bytes = canonical_json(result).encode("utf-8")
    safe_output: dict[str, Any] = {
        "evidence": dict(output["evidence"]),
        "backend": output["backend"],
        "scan_mode": output["scan_mode"],
        "fallback": output["fallback"],
        "first_row_seconds": output["first_row_seconds"],
        "wall_seconds": output["wall_seconds"],
        "cpu_seconds": output["cpu_seconds"],
        "memory_peaks": dict(output["memory_peaks"]),
        "cancellation_origin": output["cancellation_origin"],
        "terminal_health": output["terminal_health"],
        "result": {
            "schema": _validated_result_schema(result["schema"]),
            "row_count": len(result["rows"]),
            "sha256": hashlib.sha256(result_bytes).hexdigest(),
        },
    }
    if record["route"] != "mo-native":
        stats = output["execution_stats"]
        safe_output["execution_stats"] = {
            field: stats[field] for field in campaign["required_execution_stats"]
        }
    safe = {key: record[key] for key in campaign["schedule"][0]}
    safe["output"] = safe_output
    return safe


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True, help="JSON object containing provenance and optional float_tolerances")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=3600)
    parser.add_argument("runner", nargs=argparse.REMAINDER, help="runner command after --")
    arguments = parser.parse_args(argv)
    runner = arguments.runner
    if runner and runner[0] == "--":
        runner = runner[1:]
    try:
        spec = json.loads(arguments.spec.read_text(encoding="utf-8"))
        campaign = build_campaign(spec["provenance"], spec.get("float_tolerances"))
        runs = execute_campaign(campaign, SubprocessExecutor(runner, arguments.timeout_seconds))
        summary = write_artifacts(campaign, runs, arguments.output)
    except (CampaignError, KeyError, OSError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        print(f"sirius parity campaign failed: {error}", file=sys.stderr)
        return 1
    return 0 if summary["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
