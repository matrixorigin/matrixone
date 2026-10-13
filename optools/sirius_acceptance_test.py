# Copyright 2026 Matrix Origin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import contextlib
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import sirius_acceptance as acceptance
import sirius_campaign_v2 as campaign
from sirius_campaign_v2_test import provenance


def evidence():
    identity = provenance()
    timing = campaign.build_campaign(identity)
    resource = campaign.build_campaign(identity, resource=True)

    def records(manifest, offset):
        result = []
        for spec in manifest["schedule"]:
            row = {
                **spec,
                "query_id": f"{offset+spec['sequence']:032x}",
                "wall_seconds": 1.0,
                "first_row_seconds": 0.1,
                "row_count": 1,
                "result_sha256": "a" * 64,
                "output_schema_sha256": "b" * 64,
                "output_schema": [
                    {"oid": 23, "precision": 64, "scale": -1, "nullable": False}
                ],
                "observations": {
                    "host_process": {"samples": 2},
                    "go_heap": {"samples": 2},
                    "cpu": {"value": 1.0},
                    "gpu_residency": (
                        {"samples": 2}
                        if spec["backend"] == "embedded"
                        else {"status": "N/A"}
                    ),
                },
                "execution": None,
                "execution_stats": None,
            }
            if spec["backend"] == "embedded":
                row["execution"] = {
                    "backend": "embedded",
                    "scan_mode": "mo",
                    "fallback": False,
                    "native_capabilities": 31,
                    "numeric_profile": "mo-exact-decimal-v1",
                    "gpu_streams": spec["streams"],
                    "terminal_health": "healthy",
                }
                row["execution_stats"] = {
                    "terminal": True,
                    "fatal": False,
                    "gpu_tasks_completed": 1,
                    "mo_input_retained_charged_bytes": 0,
                    "result_retained_charged_bytes": 0,
                    "result_rows": 1,
                }
                row["native_pools"] = {
                    "source": "sirius-tagged-pools",
                    "scope": "query-window",
                    "pools": [
                        {
                            "tier": tier,
                            "end": {"allocated_bytes": 0, "reserved_bytes": 0},
                        }
                        for tier in ("host", "gpu")
                    ],
                }
            if manifest["resource_campaign"]:
                row["resource_span"] = {
                    "time_domain": "unix-epoch-ns",
                    "scope": "runner-lifetime",
                    "start_ns": spec["sequence"] * 100,
                    "end_ns": spec["sequence"] * 100 + 50,
                }
            result.append(row)
        return result

    timing_rows, resource_rows = records(timing, 0), records(resource, 10000)
    allocations = []
    for scale in (1, 10):
        for stream in (1, 2, 4):
            measured = [
                {
                    **row["resource_span"],
                    "phase": row["phase"],
                    "sequence": row["sequence"],
                    "query_id": row["query_id"],
                    "before_bytes": {"pinned": 1, "device": 1},
                }
                for row in resource_rows
                if row["scale_factor"] == scale
                and row["streams"] == stream
                and row["backend"] == "embedded"
            ]
            allocations.append(
                {
                    "scale_factor": scale,
                    "streams": stream,
                    "provenance": identity,
                    "source": "nsight-cupti-memory-events",
                    "scope": "cn-process-lifetime",
                    "allocation_counts": {"pinned": 1, "device": 1},
                    "unexplained_growth_bytes": 0,
                    "pool_baseline_reconciled": True,
                    "warmed_baseline_bytes": {"pinned": 1, "device": 1},
                    "post_cleanup_bytes": {"pinned": 1, "device": 1},
                    "runs": [
                        {
                            "phase": "resource-warmup",
                            "query_id": f"{100000 + len(allocations) * 1000 + index:032x}",
                            "sequence": 1000000 + len(allocations) * 1000 + index,
                            "time_domain": "unix-epoch-ns",
                            "scope": "runner-lifetime",
                            "start_ns": measured[0]["start_ns"] - 50 + 2 * index,
                            "end_ns": measured[0]["start_ns"] - 49 + 2 * index,
                            "before_bytes": {"pinned": 1, "device": 1},
                        }
                        for index in range(22)
                    ]
                    + measured
                    + [
                        {
                            "phase": "cleanup-probe",
                            "query_id": f"{200000 + len(allocations) * 1000:032x}",
                            "sequence": 2000000 + len(allocations) * 1000,
                            "time_domain": "unix-epoch-ns",
                            "scope": "runner-lifetime",
                            "start_ns": measured[-1]["end_ns"] + 1,
                            "end_ns": measured[-1]["end_ns"] + 2,
                            "before_bytes": {"pinned": 1, "device": 1},
                        }
                    ],
                }
            )
    lifecycle = {
        "provenance": identity,
        "cases": [
            {
                "id": case,
                "passed": True,
                "retained_input_bytes": 0,
                "retained_result_bytes": 0,
                "evidence_sha256": "c" * 64,
                "barrier": "controlled-test-event",
            }
            for case in acceptance.LIFECYCLE_CASES
        ],
    }
    quarantine = next(
        case for case in lifecycle["cases"] if case["id"] == "fatal-quarantine"
    )
    quarantine.update(
        scope="test-owned-process-death",
        admission_sealed=True,
        unsafe_owners_retained_before_exit=True,
        process_joined=True,
    )
    sanitizer = {
        "provenance": identity,
        "raw_exit_status": 0,
        "unexplained_errors": 0,
        "unexplained_leaks": 0,
        "cudf_pinned_pool_reconciled": True,
        "cub_diagnostic_reconciled": True,
        "evidence_sha256": "d" * 64,
        "raw_evidence_sha256": {"gpu.log": "1" * 64, "abi.log": "2" * 64},
        "runs": [
            {"id": name, "exit_status": 0, "raw_error_count": 0, "raw_leak_bytes": 0}
            for name in ("gpu.log", "abi.log")
        ],
    }
    baselines = {"provenance": identity, "cases": []}
    for case, kind in acceptance.BASELINE_CASES.items():
        reference, revision = (
            ("native-a", "c440013c89f1a54a6e5d934aabd0f9d3ef15ed3c")
            if kind == "scalar"
            else ("native-b", "5ea60cd31955d0dced2adcbcd3df0772207b79ef")
        )
        baselines["cases"].append(
            {
                "id": case,
                "kind": kind,
                "reference": reference,
                "reference_revision": revision,
                "candidate_revision": identity["sirius_revision"],
                "input_sha256": "e" * 64,
                "reference_input_sha256": "e" * 64,
                "rows": 262144,
                "timing_boundary_matched": True,
                "reference_seconds": [1.0] * 7,
                "candidate_seconds": [1.0] * 7,
            }
        )
    return [
        timing,
        timing_rows,
        resource,
        resource_rows,
        allocations,
        lifecycle,
        sanitizer,
        baselines,
    ]


class AcceptanceTest(unittest.TestCase):
    def test_cli_binds_consumed_inputs_to_archive_checksums(self):
        inputs = evidence()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            names = [
                "timing/campaign.json",
                "timing/runs.jsonl",
                "resource/campaign.json",
                "resource/runs.jsonl",
                "allocations.json",
                "lifecycle.json",
                "sanitizer.json",
                "baselines.json",
            ]
            for name, value in zip(names, inputs):
                file = root / name
                file.parent.mkdir(parents=True, exist_ok=True)
                if name.endswith("jsonl"):
                    file.write_text("".join(json.dumps(row) + "\n" for row in value))
                else:
                    file.write_text(json.dumps(value))
            hashes = {
                name: hashlib.sha256((root / name).read_bytes()).hexdigest()
                for name in names
            }
            manifest = root / "checksums.json"
            manifest.write_text(json.dumps(hashes))
            arguments = [
                "sirius_acceptance",
                "--timing",
                str(root / "timing"),
                "--resource",
                str(root / "resource"),
            ]
            for option in ("allocations", "lifecycle", "sanitizer", "baselines"):
                arguments += ["--" + option, str(root / (option + ".json"))]
            with patch.object(sys, "argv", arguments), contextlib.redirect_stdout(
                io.StringIO()
            ) as output:
                acceptance.main()
            self.assertTrue(json.loads(output.getvalue())["local_acceptance_complete"])
            rows = inputs[1]
            native = next(row for row in rows if row["route"] == "mo-native")
            native["row_count"] = 5
            target = root / "timing/runs.jsonl"
            target.write_text("".join(json.dumps(row) + "\n" for row in rows))
            with patch.object(sys, "argv", arguments):
                with self.assertRaisesRegex(
                    campaign.CampaignError, "checksum mismatch"
                ):
                    acceptance.main()
            # Rehashing a contradictory archive cannot replace cross-route validation.
            hashes["timing/runs.jsonl"] = hashlib.sha256(
                target.read_bytes()
            ).hexdigest()
            manifest.write_text(json.dumps(hashes))
            with patch.object(sys, "argv", arguments):
                with self.assertRaisesRegex(
                    campaign.CampaignError, "cardinality mismatch"
                ):
                    acceptance.main()
            del hashes["timing/runs.jsonl"]
            manifest.write_text(json.dumps(hashes))
            with patch.object(sys, "argv", arguments):
                with self.assertRaisesRegex(
                    campaign.CampaignError, "lacks an archive checksum"
                ):
                    acceptance.main()

    def test_declared_flight_retained_pairs_use_the_native_oracle(self):
        inputs = evidence()
        profile = {
            "id": "flight-mo",
            "coverage": [1],
            "provenance": {
                "matrixone_revision": "a" * 40,
                "sirius_revision": "b" * 40,
                "launcher_sha256": "1" * 64,
                "configuration_sha256": "2" * 64,
                "server_binary_sha256": "3" * 64,
                "tn_no_gc_verified": False,
            },
        }
        manifest = campaign.build_campaign(
            inputs[0]["provenance"], flight_profiles=[profile]
        )
        original = {
            (campaign.legacy._oracle_cell(row), row["route"]): row for row in inputs[1]
        }
        rows = []
        for spec in manifest["schedule"]:
            route = "mo-native" if spec["route"] == "flight-mo" else spec["route"]
            row = copy.deepcopy(original[(campaign.legacy._oracle_cell(spec), route)])
            row.update(spec, query_id=f"{50000+spec['sequence']:032x}")
            rows.append(row)
        campaign.summarize(manifest, rows)
        peer = next(row for row in rows if row["route"] == "flight-mo")
        peer["row_count"] = 5
        with self.assertRaisesRegex(campaign.CampaignError, "cardinality mismatch"):
            campaign.summarize(manifest, rows)

    def test_retained_oracle_pairs_reject_count_schema_and_identity_drift(self):
        for records_index in (1, 3):
            for mutation in (
                lambda row: row.update(row_count=5),
                lambda row: row["output_schema"][0].update(oid=24),
                lambda row: row.update(output_schema_sha256="c" * 64),
            ):
                with self.subTest(records=records_index, mutation=mutation):
                    inputs = evidence()
                    native = next(
                        row
                        for row in inputs[records_index]
                        if row["route"] == "mo-native"
                    )
                    mutation(native)
                    with self.assertRaises(campaign.CampaignError):
                        acceptance.validate(*inputs)

    def test_retained_pair_does_not_require_equal_ordered_spool_hashes(self):
        inputs = evidence()
        native = next(row for row in inputs[1] if row["route"] == "mo-native")
        native["result_sha256"] = "f" * 64
        self.assertTrue(acceptance.validate(*inputs)["local_acceptance_complete"])

    def test_matched_sanitizer_controls_preserve_raw_nonzero_status(self):
        inputs = evidence()
        report = inputs[6]
        report.update(
            raw_exit_status=99,
            raw_evidence_sha256={
                "pinned.log": "a" * 64,
                "pinned-control.log": "b" * 64,
                "cub.log": "e" * 64,
                "cub-control.log": "f" * 64,
            },
            runs=[
                {
                    "id": name,
                    "exit_status": 99,
                    "raw_error_count": 1,
                    "raw_leak_bytes": 41386248 if name.startswith("pinned") else 0,
                }
                for name in (
                    "pinned.log",
                    "pinned-control.log",
                    "cub.log",
                    "cub-control.log",
                )
            ],
            reconciliations=[
                {
                    "id": "cudf-default-pinned-pool",
                    "candidate_bytes": 41386248,
                    "control_bytes": 41386248,
                    "candidate_allocations": 1,
                    "control_allocations": 1,
                    "initialization_without_gpu_work": True,
                    "owner": "process-global-cudf-default-pinned-resource",
                    "query_credit_retained_bytes": 0,
                    "steady_profile_growth_bytes": [0] * 6,
                    "candidate_evidence_sha256": "a" * 64,
                    "control_evidence_sha256": "b" * 64,
                },
                {
                    "id": "cub-empty-kernel-registration",
                    "candidate_errors": 1,
                    "control_errors": 1,
                    "control_revision": "c440013c89f1a54a6e5d934aabd0f9d3ef15ed3c",
                    "symbol": "_ZN3cub6detail11EmptyKernelIvEEvv",
                    "source_unchanged_from_control": True,
                    "source_sha256": "c" * 64,
                    "no_op_header_sha256": "d" * 64,
                    "candidate_evidence_sha256": "e" * 64,
                    "control_evidence_sha256": "f" * 64,
                },
            ],
        )
        acceptance.validate(*inputs)
        self.assertEqual(99, report["raw_exit_status"])
        original = copy.deepcopy(report)
        for mutation in (
            lambda r: r["reconciliations"][0].update(candidate_bytes=41386249),
            lambda r: r["reconciliations"][0].update(owner="query-owned-leak"),
            lambda r: r["runs"][0].update(raw_leak_bytes=41386249),
            lambda r: r["runs"][2].update(raw_error_count=2),
            lambda r: r["runs"][2].update(exit_status=0),
            lambda r: r["runs"].pop(),
            lambda r: r["reconciliations"].append(
                copy.deepcopy(r["reconciliations"][0])
            ),
        ):
            inputs[6] = copy.deepcopy(original)
            mutation(inputs[6])
            with self.assertRaises(campaign.CampaignError):
                acceptance.validate(*inputs)

    def test_allocation_baselines_require_actual_ordered_trace_snapshots(self):
        for mutation in (
            lambda a: a["warmed_baseline_bytes"].update(pinned=2),
            lambda a: a["post_cleanup_bytes"].update(pinned=0),
            lambda a: a["runs"][-1].update(query_id=a["runs"][0]["query_id"]),
            lambda a: a["runs"][0].update(end_ns=a["runs"][1]["end_ns"]),
        ):
            inputs = evidence()
            mutation(inputs[4][0])
            with self.assertRaises(campaign.CampaignError):
                acceptance.validate(*inputs)

    def test_post_close_pool_proof_requires_the_actual_successor_begin(self):
        inputs = evidence()
        rows = [row for row in inputs[1] if row["backend"] == "embedded"]
        first, following = rows[:2]
        first["native_pools"]["window"] = 1
        following["native_pools"]["window"] = 2
        pool = first["native_pools"]["pools"][1]
        next_pool = following["native_pools"]["pools"][1]
        pool["space"] = next_pool["space"] = 0
        pool["end"]["allocated_bytes"] = 768
        next_pool["begin"] = {
            "allocated_bytes": 0,
            "reserved_bytes": 0,
            "lifetime_peak_bytes": 1024,
        }
        pool["after_close"] = {
            "source": "next-query-window-begin",
            "query_id": following["query_id"],
            "window": 2,
            **next_pool["begin"],
        }
        acceptance.validate(*inputs)
        next_pool["begin"]["allocated_bytes"] = 768
        with self.assertRaisesRegex(campaign.CampaignError, "retains"):
            acceptance.validate(*inputs)

    def test_complete_local_evidence_still_requires_ci_and_review(self):
        result = acceptance.validate(*evidence())
        self.assertTrue(result["local_acceptance_complete"])
        self.assertFalse(result["acceptance_complete"])
        self.assertEqual(
            ["required-CI", "reviewer-approval"], result["remaining_gates"]
        )

    def test_required_evidence_cannot_be_removed_or_misattributed(self):
        mutations = [
            lambda x: x[1].pop(),
            lambda x: x[3].pop(),
            lambda x: x[4].pop(),
            lambda x: x[4][0].update(scope="whole-process-lifetime"),
            lambda x: x[5]["cases"].pop(),
            lambda x: next(
                case for case in x[5]["cases"] if case["id"] == "fatal-quarantine"
            ).update(process_joined=False),
            lambda x: x[6].update(unexplained_leaks=1),
            lambda x: x[7]["cases"][0].update(candidate_seconds=[1.11] * 7),
            lambda x: x[0]["provenance"].update(sdk_build_mode="development"),
            lambda x: x[4][0]["runs"][0].update(query_id="0" * 32),
            lambda x: x[4][0]["post_cleanup_bytes"].update(pinned=2),
            lambda x: x[7]["cases"][0].update(input_sha256="invalid"),
            lambda x: x[7]["cases"].pop(),
            lambda x: x[1][0].update(query=22),
        ]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                inputs = evidence()
                mutation(inputs)
                with self.assertRaises(campaign.CampaignError):
                    acceptance.validate(*inputs)


if __name__ == "__main__":
    unittest.main()
