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

from pathlib import Path
import copy
import io
import json
import os
import select
import sys
import tempfile
import unittest
from unittest import mock

import sirius_parity


class SiriusParityTest(unittest.TestCase):
    def provenance(self):
        query_sha256 = {
            f"Q{query}": f"{query:064x}" for query in sirius_parity.QUERIES
        }
        provenance = {
            "matrixone_revision": "a" * 40,
            "sirius_revision": "b" * 40,
            "sidecar_revision": "c" * 40,
            "sdk_sha256": "d" * 64,
            "query_set_sha256": sirius_parity.canonical_query_set_digest(query_sha256),
            "query_sha256": query_sha256,
            "datasets": {"SF1": "e" * 64, "SF10": "f" * 64},
            "hardware": {
                "gpu": "fixture-gpu",
                "cpu": "fixture-cpu",
                "host_memory_bytes": 256 << 30,
                "gpu_memory_bytes": 80 << 30,
            },
            "config": {
                "sha256": "9" * 64,
                "cuda_version": "13.2",
                "driver_version": "fixture-driver",
            },
        }
        return provenance

    def output(self, spec):
        factor = {
            "mo-native": 1.0,
            "flight-tae": 2.0,
            "flight-mo": 3.0,
            "embedded-tae": 1.8,
            "embedded-mo": 2.5,
        }[spec["route"]]
        result = {
            "evidence": {
                "sequence": spec["sequence"],
                "scale_factor": spec["scale_factor"],
                "dataset_sha256": spec["dataset_sha256"],
                "query": spec["query"],
                "query_sha256": spec["query_sha256"],
                "gpu_streams": spec["streams"],
            },
            "backend": spec["backend"],
            "scan_mode": spec["scan_mode"],
            "fallback": False,
            "wall_seconds": factor + spec["query"] / 1000,
            "first_row_seconds": 0.1,
            "cpu_seconds": 0.25,
            "memory_peaks": {
                "go_bytes": 1024,
                "native_host_bytes": 2048,
                "pinned_host_bytes": 4096,
                "gpu_bytes": 8192,
            },
            "cancellation_origin": "none",
            "terminal_health": "not-applicable" if spec["route"] == "mo-native" else "accepting",
            "result": {
                "schema": [{"name": "value", "type": "decimal(15,2)"}],
                "rows": [[{"type": "decimal(15,2)", "value": "1.00"}]],
            },
        }
        if spec["route"] != "mo-native":
            stats = {field: 0 for field in sirius_parity.REQUIRED_EXECUTION_STATS}
            stats.update({
                "terminal": 1,
                "terminal_status": 0,
                "fatal": 0,
                "source_mask": 1 if spec["scan_mode"] == "mo" else 2,
                "gpu_tasks_started": 3,
                "gpu_tasks_completed": 3,
                "result_rows": 1,
                "result_payload_bytes": 16,
                "result_peak_charged_bytes": 64,
            })
            if spec["scan_mode"] == "mo":
                stats.update({
                    "mo_input_units": 2,
                    "mo_input_peak_charged_bytes": 128,
                })
            else:
                work_limit = max(2, 2 * spec["streams"])
                stats.update({
                    "tae_requests": 2,
                    "tae_work_issued": 2,
                    "tae_work_completed": 2,
                    "tae_peak_active_work": 1,
                    "tae_peak_queued_work": 1,
                    "tae_work_limit": work_limit,
                    "tae_slice_bytes": min(
                        sirius_parity.TAE_STAGING_SLICE_BYTES,
                        sirius_parity.TAE_HOST_WINDOW_BYTES // work_limit,
                    ),
                    "tae_peak_cached_metadata_charged_bytes": 1024,
                    "tae_peak_staging_charged_bytes": 2048,
                    "tae_peak_gpu_reservation_admitted_bytes": 4096,
                    "tae_payload_bytes": 8192,
                })
            result["execution_stats"] = stats
        return result

    def campaign_for(self, schedule):
        campaign = sirius_parity.build_campaign(self.provenance())
        campaign["schedule"] = copy.deepcopy(list(schedule))
        return campaign

    def test_schedule_is_complete_serial_and_rotated(self):
        schedule = sirius_parity.build_schedule(self.provenance())
        self.assertEqual(1860, len(schedule))
        self.assertEqual(list(range(1, 1861)), [run["sequence"] for run in schedule])
        self.assertEqual({1, 10}, {run["scale_factor"] for run in schedule})
        self.assertEqual({1, 2, 4}, {run["streams"] for run in schedule})
        measured = [run for run in schedule if run["phase"] == "performance" and run["measured"]]
        self.assertEqual(2 * 5 * 5 * 22, len(measured))
        warmup = [run for run in schedule if run["phase"] == "performance" and not run["measured"]]
        self.assertEqual(2 * 5 * 22, len(warmup))
        q9 = [run for run in schedule if run["phase"] == "q9-stress"]
        self.assertEqual(2 * 10 * 5, len(q9))
        self.assertTrue(all(not run["fallback_allowed"] for run in schedule))
        self.assertNotEqual(
            [run["route"] for run in schedule if run["phase"] == "performance" and run["suite"] == 0][:5],
            [run["route"] for run in schedule if run["phase"] == "performance" and run["suite"] == 1][:5],
        )

    def test_artifacts_are_deterministic_and_publish_all_queries(self):
        campaign = sirius_parity.build_campaign(self.provenance())
        runs = sirius_parity.execute_campaign(campaign, self.output)
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            summary = sirius_parity.write_artifacts(campaign, runs, Path(first))
            second_summary = sirius_parity.write_artifacts(campaign, runs, Path(second))
            self.assertTrue(summary["passed"])
            self.assertEqual({"SF1", "SF10"}, set(summary["scales"]))
            self.assertTrue(all(scale["passed"] for scale in summary["scales"].values()))
            self.assertTrue(all(len(scale["gates"]) == 9 for scale in summary["scales"].values()))
            self.assertEqual(summary, second_summary)
            for name in ("campaign.json", "runs.jsonl", "summary.csv", "summary.md"):
                self.assertEqual(
                    (Path(first) / name).read_bytes(),
                    (Path(second) / name).read_bytes(),
                )
            self.assertFalse((Path(first) / "failure.json").exists())
            headings = (Path(first) / "summary.csv").read_text().splitlines()[0]
            for query in range(1, 23):
                self.assertIn(f"Q{query}", headings.split(","))
            self.assertEqual(10, len((Path(first) / "summary.csv").read_text().splitlines()) - 1)
            markdown = (Path(first) / "summary.md").read_text()
            self.assertIn("## SF1", markdown)
            self.assertIn("## SF10", markdown)

    def test_runner_exception_and_malformed_output_are_bounded(self):
        schedule = sirius_parity.build_schedule(self.provenance())[:1]
        secret = "RUNNER-SECRET-29244"
        for runner, category in (
            (lambda spec: (_ for _ in ()).throw(RuntimeError(secret)), "runner-exception"),
            (lambda spec: (_ for _ in ()).throw(sirius_parity.CampaignFailure(
                secret, 1, secret, 1, 0, "invalid", secret)), "runner-exception"),
            (lambda spec: None, "malformed-output"),
        ):
            with self.assertRaises(sirius_parity.CampaignFailure) as raised:
                sirius_parity.execute_campaign(self.campaign_for(schedule), runner)
            self.assertEqual(category, raised.exception.diagnostic["category"])
            self.assertNotIn(secret, str(raised.exception))
            self.assertEqual(1, raised.exception.diagnostic["attempted"])
            self.assertEqual(0, raised.exception.diagnostic["validated"])

    def test_executor_receives_a_private_spec_copy(self):
        spec = sirius_parity.build_schedule(self.provenance())[0]

        def mutating_runner(executor_spec):
            executor_spec["route"] = "runner-secret"
            return self.output(spec)

        runs = sirius_parity.execute_campaign(self.campaign_for([spec]), mutating_runner)
        self.assertEqual(spec["route"], runs[0]["route"])

    def test_incremental_oracles_support_both_arrival_orders_and_pending_routes(self):
        specs = sirius_parity.build_schedule(self.provenance())[:5]
        for ordered in (specs, list(reversed(specs))):
            campaign = self.campaign_for(ordered)
            runs = sirius_parity.execute_campaign(campaign, self.output)
            self.assertEqual(5, len(runs))
            validator = sirius_parity.IncrementalValidator(campaign)
            for spec, record in zip(ordered, runs):
                self.assertEqual(spec, validator.start())
                validator.accept(record)
            validator.finish()
            self.assertEqual(5, validator.attempted)
            self.assertEqual(5, validator.validated)

    def test_incremental_result_mismatch_uses_native_as_expected_side(self):
        specs = sirius_parity.build_schedule(self.provenance())[:5]
        for ordered in (specs, list(reversed(specs))):
            target = next(spec for spec in ordered if spec["route"] != "mo-native")
            calls = []

            def runner(spec, target=target):
                calls.append(spec["route"])
                output = self.output(spec)
                if spec["route"] == target["route"]:
                    output["result"]["rows"][0][0]["value"] = "9.00"
                return output

            with self.assertRaises(sirius_parity.CampaignFailure) as raised:
                sirius_parity.execute_campaign(self.campaign_for(ordered), runner)
            self.assertEqual("result-mismatch", raised.exception.diagnostic["category"])
            current = ordered[1] if ordered[0]["route"] == "mo-native" else ordered[-1]
            self.assertEqual(current["sequence"], raised.exception.diagnostic["sequence"])
            self.assertEqual(current["route"], raised.exception.diagnostic["route"])
            if ordered[0]["route"] == "mo-native":
                self.assertEqual(2, len(calls))
            else:
                self.assertEqual(len(ordered), len(calls))
            self.assertEqual(1, raised.exception.diagnostic["validated"])

    def test_incremental_completion_duplicate_and_missing_native_categories(self):
        specs = sirius_parity.build_schedule(self.provenance())[:5]
        complete = self.campaign_for(specs)
        complete_runs = sirius_parity.execute_campaign(complete, self.output)
        with self.assertRaisesRegex(sirius_parity.CampaignFailure, "incomplete campaign"):
            sirius_parity.validate_runs(complete, complete_runs[:-1])
        with self.assertRaisesRegex(sirius_parity.CampaignFailure, "incomplete campaign"):
            sirius_parity.validate_runs(complete, complete_runs + complete_runs[:1])

        duplicate = self.campaign_for([specs[0], specs[0]])
        duplicate_runs = [
            {**specs[0], "output": self.output(specs[0])},
            {**specs[0], "output": self.output(specs[0])},
        ]
        with self.assertRaises(sirius_parity.CampaignFailure) as raised:
            sirius_parity.validate_runs(duplicate, duplicate_runs)
        self.assertEqual("duplicate", raised.exception.diagnostic["category"])

        missing_native = self.campaign_for([specs[1]])
        missing_run = {**specs[1], "output": self.output(specs[1])}
        with self.assertRaises(sirius_parity.CampaignFailure) as raised:
            sirius_parity.validate_runs(missing_native, [missing_run])
        self.assertEqual("missing-native", raised.exception.diagnostic["category"])
        self.assertEqual("incomplete", raised.exception.diagnostic["status"])

    def test_cli_rejects_stale_output_without_running_or_overwriting(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            output.mkdir()
            old = output / "summary.md"
            old.write_text("old PASS\n", encoding="utf-8")
            with (
                mock.patch.object(sirius_parity, "SubprocessExecutor") as executor,
                mock.patch.object(sys, "stderr", new_callable=io.StringIO) as stderr,
            ):
                result = sirius_parity.main([
                    "--spec", str(Path(directory) / "missing.json"),
                    "--output", str(output), "--", "runner",
                ])
            self.assertEqual(1, result)
            executor.assert_not_called()
            self.assertEqual("old PASS\n", old.read_text())
            self.assertFalse((output / "failure.json").exists())
            self.assertNotIn("missing.json", stderr.getvalue())

    def test_cli_failure_artifact_and_write_failure_keep_safe_original_category(self):
        secret = "FAILURE-DETAIL-29244"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            spec_path = root / "spec.json"
            spec_path.write_text(json.dumps({"provenance": self.provenance()}), encoding="utf-8")
            schedule = sirius_parity.build_schedule(self.provenance())
            for failure_at, invalid in ((1, "fallback"), (3, "fallback"), (1, "encoding")):
                calls = []

                def runner(spec, failure_at=failure_at):
                    calls.append(spec["sequence"])
                    output = self.output(spec)
                    if len(calls) == failure_at:
                        output["fallback"] = invalid == "fallback"
                        output["result"]["rows"][0][0]["value"] = secret + (
                            "\ud800" if invalid == "encoding" else ""
                        )
                        output["unknown_runner_field"] = secret
                    return output

                output_directory = root / f"failure-{failure_at}-{invalid}"
                with (
                    mock.patch.object(sirius_parity, "SubprocessExecutor", return_value=runner),
                    mock.patch.object(sys, "stderr", new_callable=io.StringIO) as stderr,
                ):
                    result = sirius_parity.main([
                        "--spec", str(spec_path), "--output", str(output_directory),
                        "--", "runner",
                    ])
                self.assertEqual(1, result)
                self.assertEqual(
                    [spec["sequence"] for spec in schedule[:failure_at]], calls
                )
                payload = json.loads((output_directory / "failure.json").read_text())
                self.assertEqual({
                    "category": "invalid-output",
                    "sequence": schedule[failure_at - 1]["sequence"],
                    "route": schedule[failure_at - 1]["route"],
                    "attempted": failure_at,
                    "validated": failure_at - 1,
                    "status": "invalid",
                }, payload)
                self.assertNotIn(secret, (output_directory / "failure.json").read_text())
                self.assertNotIn(secret, stderr.getvalue())
                self.assertIn("artifact_status=written", stderr.getvalue())

            # Even the owner cannot overwrite a prior diagnostic or follow a symlink.
            failure = sirius_parity.CampaignFailure("invalid-output", 1, "mo-native", 1, 0, "invalid")
            destination = output_directory / "failure.json"
            saved = destination.read_bytes()
            with self.assertRaises(FileExistsError):
                sirius_parity.write_failure(output_directory, failure, allow_partial=True)
            link_directory = root / "link"
            link_directory.mkdir()
            (link_directory / "failure.json").symlink_to(destination)
            with self.assertRaises(FileExistsError):
                sirius_parity.write_failure(link_directory, failure, allow_partial=True)
            self.assertEqual(saved, destination.read_bytes())

            failure = sirius_parity.CampaignFailure(
                "invalid-output", 1, "mo-native", 1, 0, "invalid", secret
            )

            with (
                mock.patch.object(sirius_parity, "execute_campaign", side_effect=failure),
                mock.patch.object(
                    sirius_parity, "write_failure", side_effect=OSError(secret)
                ),
                mock.patch.object(sys, "stderr", new_callable=io.StringIO) as stderr,
            ):
                result = sirius_parity.main([
                    "--spec", str(spec_path), "--output", str(root / "write-failure"),
                    "--", "runner",
                ])
            self.assertEqual(1, result)
            self.assertNotIn(secret, stderr.getvalue())
            self.assertIn('"category":"invalid-output"', stderr.getvalue())
            self.assertIn("artifact_status=write-failed", stderr.getvalue())

    def test_cli_partial_artifact_failure_retains_diagnostics_and_counts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            spec_path = root / "spec.json"
            spec_path.write_text(json.dumps({"provenance": self.provenance()}), encoding="utf-8")
            write = Path.write_text
            for exception in (OSError, UnicodeEncodeError):
                def fail_write(path, data, *args, **kwargs):
                    if path.name == "runs.jsonl":
                        if exception is UnicodeEncodeError:
                            raise UnicodeEncodeError("utf-8", "\ud800", 0, 1, "test")
                        raise OSError("WRITE-SECRET")
                    return write(path, data, *args, **kwargs)

                output = root / exception.__name__
                with (
                    mock.patch.object(sirius_parity, "SubprocessExecutor", return_value=self.output),
                    mock.patch.object(Path, "write_text", fail_write),
                    mock.patch.object(sys, "stderr", new_callable=io.StringIO) as stderr,
                ):
                    code = sirius_parity.main(["--spec", str(spec_path), "--output", str(output), "--", "runner"])
                self.assertEqual(1, code)
                self.assertEqual({"campaign.json", "failure.json"}, {p.name for p in output.iterdir()})
                self.assertEqual({
                    "category": "artifact-write", "sequence": None, "route": None,
                    "attempted": 1860, "validated": 1860, "status": "invalid",
                }, json.loads((output / "failure.json").read_text()))
                self.assertIn("artifact_status=written", stderr.getvalue())
                self.assertNotIn("WRITE-SECRET", stderr.getvalue())

    def test_cli_preserves_success_and_performance_exit_codes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            spec_path = root / "spec.json"
            spec_path.write_text(json.dumps({"provenance": self.provenance()}), encoding="utf-8")

            def regressed(spec):
                output = self.output(spec)
                if spec["route"] == "embedded-mo":
                    output["wall_seconds"] *= 10
                return output

            for name, runner, expected_exit, marker in (
                ("pass", self.output, 0, "Overall: PASS"),
                ("gate", regressed, 2, "Overall: FAIL"),
            ):
                output_directory = root / name
                with (
                    mock.patch.object(sirius_parity, "SubprocessExecutor", return_value=runner),
                    mock.patch.object(sirius_parity, "_validate_output", wraps=sirius_parity._validate_output) as validation,
                    mock.patch.object(sirius_parity, "_compare_results", wraps=sirius_parity._compare_results) as comparison,
                    mock.patch.object(sirius_parity, "_result_bytes", wraps=sirius_parity._result_bytes) as encoding,
                ):
                    result = sirius_parity.main([
                        "--spec", str(spec_path), "--output", str(output_directory),
                        "--", "runner",
                    ])
                self.assertEqual(expected_exit, result)
                self.assertEqual((1860, 1488, 3720),
                                 (validation.call_count, comparison.call_count, encoding.call_count))
                self.assertTrue((output_directory / "campaign.json").exists())
                self.assertTrue((output_directory / "runs.jsonl").exists())
                self.assertIn(marker, (output_directory / "summary.md").read_text())
                self.assertFalse((output_directory / "failure.json").exists())

    def test_completeness_and_execution_evidence_fail_closed(self):
        campaign = sirius_parity.build_campaign(self.provenance())
        runs = sirius_parity.execute_campaign(campaign, self.output)
        with self.assertRaisesRegex(sirius_parity.CampaignError, "incomplete campaign"):
            sirius_parity.validate_runs(campaign, runs[:-1])

        broken = [dict(record) for record in runs]
        broken[1] = dict(broken[1])
        broken[1]["output"] = dict(broken[1]["output"])
        broken[1]["output"]["fallback"] = True
        with self.assertRaisesRegex(sirius_parity.CampaignError, "fallback observed"):
            sirius_parity.validate_runs(campaign, broken)

        broken = [dict(record) for record in runs]
        broken[1] = dict(broken[1])
        broken[1]["output"] = dict(broken[1]["output"])
        broken[1]["output"]["execution_stats"] = dict(broken[1]["output"]["execution_stats"])
        broken[1]["output"]["execution_stats"]["gpu_tasks_started"] = 0
        broken[1]["output"]["execution_stats"]["gpu_tasks_completed"] = 0
        with self.assertRaisesRegex(sirius_parity.CampaignError, "GPU execution evidence"):
            sirius_parity.validate_runs(campaign, broken)

        for field, value in (
            ("sequence", -1),
            ("scale_factor", 100),
            ("dataset_sha256", "0" * 64),
            ("query", 22),
            ("query_sha256", "0" * 64),
            ("gpu_streams", 99),
        ):
            broken = copy.deepcopy(runs)
            broken[0]["output"]["evidence"][field] = value
            with self.assertRaisesRegex(sirius_parity.CampaignError, "runner identity evidence mismatch"):
                sirius_parity.validate_runs(campaign, broken)

        mo_index = next(index for index, run in enumerate(runs)
                        if run["route"] != "mo-native" and run["scan_mode"] == "mo")
        broken = copy.deepcopy(runs)
        broken[mo_index]["output"]["execution_stats"]["mo_input_peak_charged_bytes"] = 0
        with self.assertRaisesRegex(sirius_parity.CampaignError, "MO source execution evidence mismatch"):
            sirius_parity.validate_runs(campaign, broken)

        tae_index = next(index for index, run in enumerate(runs)
                         if run["route"] != "mo-native" and run["scan_mode"] == "tae")
        for field, value in (
            ("tae_work_limit", 99),
            ("tae_active_work", 1),
            ("tae_slice_bytes", sirius_parity.TAE_STAGING_SLICE_BYTES + 1),
            ("tae_peak_cached_metadata_charged_bytes", 1 << 40),
            ("tae_peak_staging_charged_bytes", 0),
            ("tae_peak_staging_charged_bytes", sirius_parity.TAE_HOST_WINDOW_BYTES + 1),
            ("tae_peak_gpu_reservation_admitted_bytes", 0),
            ("tae_payload_bytes", 0),
        ):
            broken = copy.deepcopy(runs)
            broken[tae_index]["output"]["execution_stats"][field] = value
            with self.assertRaisesRegex(sirius_parity.CampaignError, "TAE source execution evidence mismatch"):
                sirius_parity.validate_runs(campaign, broken)

        for field in ("result_payload_bytes", "result_peak_charged_bytes"):
            broken = copy.deepcopy(runs)
            broken[tae_index]["output"]["execution_stats"][field] = 0
            with self.assertRaisesRegex(sirius_parity.CampaignError, "invalid result retention evidence"):
                sirius_parity.validate_runs(campaign, broken)

        broken = copy.deepcopy(runs)
        broken[tae_index]["output"]["execution_stats"]["result_peak_charged_bytes"] = 1 << 40
        with self.assertRaisesRegex(sirius_parity.CampaignError, "invalid result retention evidence"):
            sirius_parity.validate_runs(campaign, broken)

    def test_result_policy_is_exact_except_explicit_float_tolerance(self):
        with self.assertRaisesRegex(sirius_parity.CampaignError, "exact floating"):
            sirius_parity._compare_typed(1.0, 1.0001, None)
        sirius_parity._compare_typed(
            1.0, 1.0001, {"absolute": 0.001, "relative": 0.0}
        )
        with self.assertRaisesRegex(sirius_parity.CampaignError, "typed result"):
            sirius_parity._compare_typed(1, 1.0, {"absolute": 1.0, "relative": 1.0})
        with self.assertRaisesRegex(sirius_parity.CampaignError, "exact result"):
            sirius_parity._compare_typed(
                {"type": "decimal", "value": "1.00"},
                {"type": "decimal", "value": "1.01"},
                {"absolute": 1.0, "relative": 1.0},
            )

        tolerance = {"absolute": 1.0, "relative": 0.0}
        decimal = {
            "schema": [{"name": "value", "type": "decimal(15,2)"}],
            "rows": [[{"type": "decimal(15,2)", "value": "1.00"}]],
        }
        wrong_decimal = copy.deepcopy(decimal)
        wrong_decimal["rows"][0][0]["value"] = "1.50"
        with self.assertRaisesRegex(sirius_parity.CampaignError, "exact result"):
            sirius_parity._compare_results(decimal, wrong_decimal, tolerance)

        integer = {
            "schema": [{"name": "value", "type": "bigint"}],
            "rows": [[{"type": "bigint", "value": 1}]],
        }
        wrong_integer = copy.deepcopy(integer)
        wrong_integer["rows"][0][0]["value"] = 2
        with self.assertRaisesRegex(sirius_parity.CampaignError, "exact result"):
            sirius_parity._compare_results(integer, wrong_integer, tolerance)

        large_integer = copy.deepcopy(integer)
        large_integer["rows"][0][0]["value"] = (1 << 53) + 1
        sirius_parity._validate_typed_result(large_integer, 1)
        lossy_integer = copy.deepcopy(large_integer)
        lossy_integer["rows"][0][0]["value"] = float((1 << 53) + 1)
        with self.assertRaisesRegex(sirius_parity.CampaignError, "inexact integer"):
            sirius_parity._validate_typed_result(lossy_integer, 1)
        boolean_integer = copy.deepcopy(integer)
        boolean_integer["rows"][0][0]["value"] = True
        with self.assertRaisesRegex(sirius_parity.CampaignError, "inexact integer"):
            sirius_parity._validate_typed_result(boolean_integer, 1)
        overflow_integer = copy.deepcopy(integer)
        overflow_integer["rows"][0][0]["value"] = 1 << 63
        with self.assertRaisesRegex(sirius_parity.CampaignError, "inexact integer"):
            sirius_parity._validate_typed_result(overflow_integer, 1)

        floating = {
            "schema": [{"name": "value", "type": "double"}],
            "rows": [[{"type": "double", "value": 1.0}]],
        }
        close_float = copy.deepcopy(floating)
        close_float["rows"][0][0]["value"] = 1.5
        sirius_parity._compare_results(floating, close_float, tolerance)

        inexact_decimal = copy.deepcopy(decimal)
        inexact_decimal["rows"][0][0]["value"] = 1.0
        with self.assertRaisesRegex(sirius_parity.CampaignError, "inexact decimal"):
            sirius_parity._validate_typed_result(inexact_decimal, 1)

    def test_relative_gate_failures_are_reported(self):
        campaign = sirius_parity.build_campaign(self.provenance())

        def regressed(spec):
            output = self.output(spec)
            if spec["route"] == "embedded-mo":
                output["wall_seconds"] *= 10
            return output

        summary = sirius_parity.summarize(
            campaign, sirius_parity.execute_campaign(campaign, regressed)
        )
        self.assertFalse(summary["passed"])
        failed = [gate["name"] for gate in summary["gates"] if not gate["passed"]]
        self.assertTrue(any("embedded-mo" in name for name in failed))

    def test_artifacts_redact_rows_paths_and_unknown_runner_fields(self):
        campaign = sirius_parity.build_campaign(self.provenance())
        secret = "SENTINEL-ROW-AND-OBJECT-PATH"

        def sensitive(spec):
            output = self.output(spec)
            output["result"] = {
                "schema": [{"name": "value", "type": "varchar"}],
                "rows": [[{"type": "varchar", "value": secret}]],
                "ignored": {"number": float("inf"), "text": "\ud800"},
            }
            output["unknown_runner_field"] = {"object_path": secret}
            if "execution_stats" in output:
                output["execution_stats"]["unknown_object_path"] = secret
            return output

        runs = sirius_parity.execute_campaign(campaign, sensitive)
        control = copy.deepcopy(runs[0])
        del control["output"]["result"]["ignored"]
        self.assertEqual(sirius_parity._artifact_run(campaign, control),
                         sirius_parity._artifact_run(campaign, runs[0]))
        with tempfile.TemporaryDirectory() as directory:
            sirius_parity.write_artifacts(campaign, runs, Path(directory))
            for name in ("campaign.json", "runs.jsonl", "summary.csv", "summary.md"):
                self.assertNotIn(secret, (Path(directory) / name).read_text())
            raw = (Path(directory) / "runs.jsonl").read_text()
            self.assertNotIn("unknown_runner_field", raw)
            self.assertNotIn("unknown_object_path", raw)
            self.assertIn('"row_count":1', raw)

        malicious = sirius_parity.execute_campaign(campaign, self.output)
        malicious[0]["output"]["result"]["schema"][0]["object_path"] = secret
        with self.assertRaisesRegex(sirius_parity.CampaignError, "invalid result schema"):
            sirius_parity.summarize(campaign, malicious)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(sirius_parity.CampaignError, "invalid result schema"):
                sirius_parity.write_artifacts(campaign, malicious, Path(directory))

    @unittest.skipUnless(
        sys.platform == "linux"
        and hasattr(os, "pidfd_open")
        and hasattr(sirius_parity.signal, "pidfd_send_signal"),
        "detached-descendant proof requires Linux subreapers and pidfds",
    )
    def test_subprocess_timeout_kills_descendants(self):
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            child_pid = directory_path / "child.pid"
            runner = directory_path / "runner.py"
            runner.write_text(
                """
import pathlib
import signal
import subprocess
import sys

child = subprocess.Popen([
    sys.executable,
    "-c",
    "import os, signal; os.setsid(); signal.pause()",
])
pathlib.Path(sys.argv[1]).write_text(str(child.pid), encoding="utf-8")
signal.pause()
""",
                encoding="utf-8",
            )
            executor = sirius_parity.SubprocessExecutor(
                [sys.executable, str(runner), str(child_pid)],
                timeout_seconds=2,
            )
            with self.assertRaisesRegex(sirius_parity.CampaignError, "runner timed out"):
                executor({"sequence": 1})
            pid = int(child_pid.read_text(encoding="utf-8"))
            try:
                descriptor = os.pidfd_open(pid)
            except ProcessLookupError:
                return
            try:
                poller = select.poll()
                poller.register(descriptor, select.POLLIN)
                self.assertTrue(poller.poll(1000), "runner descendant survived timeout cleanup")
            finally:
                os.close(descriptor)

    @unittest.skipUnless(
        sys.platform == "linux"
        and hasattr(os, "pidfd_open")
        and hasattr(sirius_parity.signal, "pidfd_send_signal"),
        "detached-descendant proof requires Linux subreapers and pidfds",
    )
    def test_successful_runner_cannot_leave_detached_descendant(self):
        with tempfile.TemporaryDirectory() as directory:
            directory_path = Path(directory)
            child_pid = directory_path / "child.pid"
            runner = directory_path / "runner.py"
            runner.write_text(
                """
import pathlib
import subprocess
import sys

child = subprocess.Popen(
    [sys.executable, "-c", "import os, signal; os.setsid(); signal.pause()"],
    stdin=subprocess.DEVNULL,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    close_fds=True,
)
pathlib.Path(sys.argv[1]).write_text(str(child.pid), encoding="utf-8")
print("{}")
""",
                encoding="utf-8",
            )
            executor = sirius_parity.SubprocessExecutor(
                [sys.executable, str(runner), str(child_pid)],
                timeout_seconds=2,
            )
            self.assertEqual({}, executor({"sequence": 1}))
            pid = int(child_pid.read_text(encoding="utf-8"))
            with self.assertRaises(ProcessLookupError):
                os.pidfd_open(pid)

    def test_subprocess_cancellation_kills_and_reaps_group(self):
        process = mock.Mock()
        process.pid = 123
        process.communicate.side_effect = [KeyboardInterrupt(), ("", "")]
        executor = sirius_parity.SubprocessExecutor(["runner"], timeout_seconds=1)
        with (
            mock.patch.object(sirius_parity.subprocess, "Popen", return_value=process) as popen,
            mock.patch.object(sirius_parity, "_enter_exclusive_subreaper", return_value=False),
            mock.patch.object(sirius_parity, "_cleanup_owned_processes") as cleanup,
            mock.patch.object(sirius_parity, "_direct_child_pids", return_value=set()),
            mock.patch.object(sirius_parity, "_set_child_subreaper") as restore,
            self.assertRaises(KeyboardInterrupt),
        ):
            executor({"sequence": 1})
        popen.assert_called_once()
        self.assertTrue(popen.call_args.kwargs["start_new_session"])
        cleanup.assert_called_once_with(process, close_pipes=True)
        restore.assert_called_once_with(False)

    @unittest.skipUnless(sys.platform == "linux", "exclusive child ownership requires procfs")
    def test_subprocess_rejects_preexisting_child(self):
        child = sirius_parity.subprocess.Popen(
            [sys.executable, "-c", "import signal; signal.pause()"]
        )
        self.addCleanup(child.wait)
        self.addCleanup(lambda: child.poll() is None and child.kill())
        executor = sirius_parity.SubprocessExecutor(["runner"], timeout_seconds=1)
        with self.assertRaisesRegex(sirius_parity.CampaignError, "exclusive subprocess ownership"):
            executor({"sequence": 1})

    def test_cleanup_failure_poisons_executor(self):
        process = mock.Mock()
        process.pid = 123
        process.returncode = 0
        process.communicate.return_value = ('{"ok":true}', "")
        executor = sirius_parity.SubprocessExecutor(["runner"], timeout_seconds=1)
        cleanup_error = sirius_parity.CampaignError("cleanup deadline")
        with (
            mock.patch.object(sirius_parity.subprocess, "Popen", return_value=process),
            mock.patch.object(sirius_parity, "_enter_exclusive_subreaper", return_value=False),
            mock.patch.object(
                sirius_parity, "_cleanup_owned_processes",
                side_effect=[cleanup_error, cleanup_error],
            ),
            mock.patch.object(sirius_parity, "_direct_child_pids", return_value=set()),
            mock.patch.object(sirius_parity, "_set_child_subreaper"),
            self.assertRaisesRegex(sirius_parity.CampaignError, "cleanup deadline"),
        ):
            executor({"sequence": 1})
        self.assertTrue(executor.poisoned)
        with self.assertRaisesRegex(sirius_parity.CampaignError, "poisoned"):
            executor({"sequence": 2})

    def test_provenance_is_mandatory(self):
        provenance = self.provenance()
        del provenance["sdk_sha256"]
        with self.assertRaisesRegex(sirius_parity.CampaignError, "missing=.*sdk_sha256"):
            sirius_parity.build_campaign(provenance)

        provenance = self.provenance()
        provenance["unknown_secret"] = "SENTINEL"
        with self.assertRaisesRegex(sirius_parity.CampaignError, "unknown=.*unknown_secret"):
            sirius_parity.build_campaign(provenance)

        provenance = self.provenance()
        provenance["hardware"]["object_path"] = "SENTINEL"
        with self.assertRaisesRegex(sirius_parity.CampaignError, "hardware"):
            sirius_parity.build_campaign(provenance)

        provenance = self.provenance()
        provenance["config"]["credential"] = "SENTINEL"
        with self.assertRaisesRegex(sirius_parity.CampaignError, "config"):
            sirius_parity.build_campaign(provenance)

        provenance = self.provenance()
        provenance["query_set_sha256"] = "0" * 64
        with self.assertRaisesRegex(sirius_parity.CampaignError, "query_set_sha256"):
            sirius_parity.build_campaign(provenance)


if __name__ == "__main__":
    unittest.main()
