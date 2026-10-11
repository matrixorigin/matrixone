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
import unittest

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
                    "runs": measured
                    + [{"phase": "resource-warmup"} for _ in range(22)]
                    + [{"phase": "cleanup-probe"}],
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
        "exit_status": 0,
        "unexplained_errors": 0,
        "unexplained_leaks": 0,
        "cudf_pinned_pool_reconciled": True,
        "cub_diagnostic_reconciled": True,
        "evidence_sha256": "d" * 64,
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
