#!/usr/bin/env python3
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

"""Fail-closed final acceptance of complete, source-matched v2 evidence."""
import argparse
import json
import math
from pathlib import Path
import statistics

import sirius_campaign_v2 as campaign

LIFECYCLE_CASES = {
    "paused-consumption",
    "blocked-input-credit",
    "blocked-result-credit",
    "cancel-before-admission",
    "cancel-during-work",
    "writer-failure",
    "queue-saturation",
    "numeric-error-owner",
    "healthy-reuse",
    "fatal-quarantine",
}

# Shared IDs from the actual A/B/current harnesses. Scalar controls also keep
# their matched timing boundaries; aggregates first exist in Native B.
BASELINE_CASES = {
    "cudf_add64": "scalar",
    "cudf_widen_multiply64_to128": "scalar",
    "checked_add64": "scalar",
    "checked_multiply64_to128": "scalar",
    "checked_add256": "scalar",
    "checked_divide256": "scalar",
    "ordinary_expression_add64": "scalar",
    "exact_expression_add64": "scalar",
    "cudf_sum64_to128": "aggregate",
    "exact_sum64_to128": "aggregate",
}


def _record_identity(reference, actual):
    for key in (
        "matrixone_revision",
        "sirius_revision",
        "sdk_sha256",
        "query_sha256",
        "datasets",
        "hardware",
        "config",
        "toolchain",
        "service_binary_sha256",
        "runner_binary_sha256",
        "sdk_build_mode",
        "controller_sha256",
    ):
        if actual.get(key) != reference.get(key):
            raise campaign.CampaignError("acceptance evidence semantic inputs differ")


def validate(
    timing_campaign,
    timing_records,
    resource_campaign,
    resource_records,
    allocations,
    lifecycle,
    sanitizer,
    baselines,
):
    timing_summary = campaign.summarize(timing_campaign, timing_records)
    campaign.summarize(resource_campaign, resource_records)
    if (
        timing_campaign["resource_campaign"]
        or not resource_campaign["resource_campaign"]
    ):
        raise campaign.CampaignError("timing/resource evidence misclassified")
    identity = timing_campaign["provenance"]
    if identity["sdk_build_mode"] != "release":
        raise campaign.CampaignError("final acceptance requires a merged release SDK")
    _record_identity(identity, resource_campaign["provenance"])
    if len({row["query_id"] for row in timing_records + resource_records}) != len(
        timing_records
    ) + len(resource_records):
        raise campaign.CampaignError(
            "timing/resource campaigns reused a query identity"
        )
    by_query = {row["query_id"]: row for row in timing_records + resource_records}
    for spec, record in zip(
        timing_campaign["schedule"] + resource_campaign["schedule"],
        timing_records + resource_records,
    ):
        required = {"host_process", "go_heap", "cpu", "gpu_residency"}
        if not required <= set(record.get("observations", {})):
            raise campaign.CampaignError("timing measurement source missing")
        stats = record.get("execution_stats")
        if spec["backend"] == "embedded":
            if (
                not isinstance(stats, dict)
                or stats.get("terminal") is not True
                or stats.get("fatal") is not False
                or stats.get("gpu_tasks_completed", 0) <= 0
                or stats.get("mo_input_retained_charged_bytes") != 0
                or stats.get("result_retained_charged_bytes") != 0
            ):
                raise campaign.CampaignError("timing terminal ownership proof missing")
            if record["observations"]["gpu_residency"].get("samples", 0) <= 0:
                raise campaign.CampaignError("actual PID GPU measurement missing")
            pools = record.get("native_pools", {})
            if (
                pools.get("source") != "sirius-tagged-pools"
                or pools.get("scope") != "query-window"
                or {pool.get("tier") for pool in pools.get("pools", [])}
                != {"host", "gpu"}
            ):
                raise campaign.CampaignError("logical native pool measurement missing")
            for pool in pools["pools"]:
                if pool["tier"] != "gpu":
                    continue
                snapshot = pool.get("end", {})
                if (
                    snapshot.get("allocated_bytes")
                    == snapshot.get("reserved_bytes")
                    == 0
                ):
                    continue
                observed = pool.get("after_close", {})
                successor = by_query.get(observed.get("query_id"), {})
                next_pools = successor.get("native_pools", {})
                matching = [
                    item
                    for item in next_pools.get("pools", [])
                    if (item.get("tier"), item.get("space"))
                    == (pool["tier"], pool.get("space"))
                ]
                if (
                    observed.get("source") != "next-query-window-begin"
                    or type(pools.get("window")) is not int
                    or observed.get("window") != pools["window"] + 1
                    or next_pools.get("window") != observed["window"]
                    or successor.get("scale_factor") != record["scale_factor"]
                    or successor.get("streams") != record["streams"]
                    or len(matching) != 1
                    or any(
                        observed.get(key) != matching[0].get("begin", {}).get(key)
                        for key in (
                            "allocated_bytes",
                            "reserved_bytes",
                            "lifetime_peak_bytes",
                        )
                    )
                    or observed.get("allocated_bytes") != 0
                    or observed.get("reserved_bytes") != 0
                ):
                    raise campaign.CampaignError(
                        "native logical GPU pool retains query state"
                    )
        elif record["observations"]["gpu_residency"] != {"status": "N/A"}:
            raise campaign.CampaignError("native oracle GPU scope misrepresented")
    expected_profiles = {(scale, stream) for scale in (1, 10) for stream in (1, 2, 4)}
    if (
        set((entry.get("scale_factor"), entry.get("streams")) for entry in allocations)
        != expected_profiles
        or len(allocations) != 6
    ):
        raise campaign.CampaignError("allocation profile coverage incomplete")
    physical_query_ids = set()
    for allocation in allocations:
        _record_identity(identity, allocation.get("provenance", {}))
        if (
            allocation.get("source") != "nsight-cupti-memory-events"
            or allocation.get("scope") != "cn-process-lifetime"
            or any(
                allocation.get("allocation_counts", {}).get(kind, 0) <= 0
                for kind in ("pinned", "device")
            )
        ):
            raise campaign.CampaignError("required physical allocation source missing")
        if (
            allocation.get("unexplained_growth_bytes") != 0
            or allocation.get("pool_baseline_reconciled") is not True
        ):
            raise campaign.CampaignError("allocator/pool lifetime baseline unresolved")
        spans = allocation.get("runs", [])
        if (
            sum(span.get("phase") == "resource-validation" for span in spans) != 22
            or sum(span.get("phase") == "resource-warmup" for span in spans) != 22
            or sum(span.get("phase") == "cleanup-probe" for span in spans) != 1
        ):
            raise campaign.CampaignError(
                "allocation warmup/measured/cleanup coverage incomplete"
            )
        matched = {
            row["sequence"]: row
            for row in resource_records
            if row["backend"] == "embedded"
            and row["scale_factor"] == allocation["scale_factor"]
            and row["streams"] == allocation["streams"]
        }
        measured = [span for span in spans if span["phase"] == "resource-validation"]
        if [span.get("phase") for span in spans] != (
            ["resource-warmup"] * 22 + ["resource-validation"] * 22 + ["cleanup-probe"]
        ):
            raise campaign.CampaignError("allocation span phase order differs")
        previous_end = -1
        span_identities = set()
        for span in spans:
            query = campaign.query_uuid(span.get("query_id"))
            start, end = span.get("start_ns"), span.get("end_ns")
            if (
                query in span_identities
                or query in physical_query_ids
                or span.get("phase") != "resource-validation"
                and query in by_query
                or type(start) is not int
                or type(end) is not int
                or not previous_end <= start < end
                or span.get("time_domain") != "unix-epoch-ns"
                or span.get("scope") != "runner-lifetime"
            ):
                raise campaign.CampaignError(
                    "allocation span identity or interval differs"
                )
            span_identities.add(query)
            physical_query_ids.add(query)
            previous_end = end
        if {span.get("sequence") for span in measured} != set(matched) or any(
            span.get("query_id") != matched[span["sequence"]]["query_id"]
            or any(
                span.get(key)
                != matched[span["sequence"]].get("resource_span", {}).get(key)
                for key in ("start_ns", "end_ns", "time_domain", "scope")
            )
            for span in measured
        ):
            raise campaign.CampaignError(
                "allocation profile is not tied to actual resource queries"
            )
        before, after = allocation.get("warmed_baseline_bytes", {}), allocation.get(
            "post_cleanup_bytes", {}
        )
        if before != measured[0].get("before_bytes") or after != spans[-1].get(
            "before_bytes"
        ):
            raise campaign.CampaignError(
                "allocation baseline snapshot differs from trace"
            )
        if (
            set(before) != {"pinned", "device"}
            or set(after) != set(before)
            or any(
                type(value) is not int or value < 0
                for value in list(before.values()) + list(after.values())
            )
            or sum(max(0, after[kind] - before[kind]) for kind in before) != 0
        ):
            raise campaign.CampaignError("physical warmed allocation baseline grew")
    for artifact in (lifecycle, sanitizer, baselines):
        _record_identity(identity, artifact.get("provenance", {}))
    cases = lifecycle.get("cases", [])
    if {case.get("id") for case in cases} != LIFECYCLE_CASES or len(cases) != len(
        LIFECYCLE_CASES
    ):
        raise campaign.CampaignError("lifecycle case closure incomplete")
    if any(
        case.get("passed") is not True
        or case.get("retained_input_bytes") != 0
        or case.get("retained_result_bytes") != 0
        or not campaign.legacy._is_sha256(case.get("evidence_sha256"))
        or not case.get("barrier")
        for case in cases
    ):
        raise campaign.CampaignError("lifecycle ownership closure failed")
    quarantine = next(case for case in cases if case["id"] == "fatal-quarantine")
    if (
        quarantine.get("scope") != "test-owned-process-death"
        or quarantine.get("admission_sealed") is not True
        or quarantine.get("unsafe_owners_retained_before_exit") is not True
        or quarantine.get("process_joined") is not True
    ):
        raise campaign.CampaignError("fatal quarantine process closure unproved")
    if (
        type(sanitizer.get("raw_exit_status")) is not int
        or sanitizer.get("raw_exit_status") not in (0, 99)
        or sanitizer.get("unexplained_errors") != 0
        or sanitizer.get("unexplained_leaks") != 0
        or sanitizer.get("cudf_pinned_pool_reconciled") is not True
        or sanitizer.get("cub_diagnostic_reconciled") is not True
        or not campaign.legacy._is_sha256(sanitizer.get("evidence_sha256"))
    ):
        raise campaign.CampaignError("sanitizer/pinned-pool/CUB acceptance unresolved")
    hashes = sanitizer.get("raw_evidence_sha256", {})
    raw_runs = sanitizer.get("runs", [])
    if (
        not isinstance(hashes, dict)
        or len(hashes) not in (2, 4)
        or any(
            not isinstance(name, str)
            or not 0 < len(name) <= 128
            or Path(name).name != name
            for name in hashes
        )
        or any(not campaign.legacy._is_sha256(value) for value in hashes.values())
        or len(set(hashes.values())) != len(hashes)
        or not isinstance(raw_runs, list)
        or len(raw_runs) != len(hashes)
        or any(
            not isinstance(run, dict) or not isinstance(run.get("id"), str)
            for run in raw_runs
        )
        or {run.get("id") for run in raw_runs} != set(hashes)
    ):
        raise campaign.CampaignError("raw sanitizer evidence is missing or duplicated")
    if sanitizer["raw_exit_status"] == 0:
        if any(
            any(
                type(run.get(key)) is not int or run[key] != 0
                for key in ("exit_status", "raw_error_count", "raw_leak_bytes")
            )
            for run in raw_runs
        ):
            raise campaign.CampaignError(
                "raw sanitizer findings contradict zero status"
            )
    else:
        reconciliations = sanitizer.get("reconciliations", [])
        if (
            not isinstance(reconciliations, list)
            or len(reconciliations) != 2
            or any(
                not isinstance(item, dict) or not isinstance(item.get("id"), str)
                for item in reconciliations
            )
        ):
            raise campaign.CampaignError(
                "matched sanitizer controls are missing or duplicated"
            )
        reconciled = {item.get("id"): item for item in reconciliations}
        if len(reconciliations) != 2 or set(reconciled) != {
            "cudf-default-pinned-pool",
            "cub-empty-kernel-registration",
        }:
            raise campaign.CampaignError(
                "nonzero sanitizer outcome lacks matched controls"
            )
        pinned = reconciled["cudf-default-pinned-pool"]
        cub = reconciled["cub-empty-kernel-registration"]
        if (
            pinned.get("candidate_bytes") != 41386248
            or pinned.get("control_bytes") != pinned["candidate_bytes"]
            or pinned.get("candidate_allocations") != 1
            or pinned.get("control_allocations") != 1
            or pinned.get("initialization_without_gpu_work") is not True
            or pinned.get("owner") != "process-global-cudf-default-pinned-resource"
            or pinned.get("query_credit_retained_bytes") != 0
            or pinned.get("steady_profile_growth_bytes") != [0] * 6
            or cub.get("candidate_errors") != 1
            or cub.get("control_errors") != cub["candidate_errors"]
            or cub.get("control_revision") != "c440013c89f1a54a6e5d934aabd0f9d3ef15ed3c"
            or cub.get("symbol") != "_ZN3cub6detail11EmptyKernelIvEEvv"
            or cub.get("source_unchanged_from_control") is not True
            or not campaign.legacy._is_sha256(cub.get("source_sha256"))
            or not campaign.legacy._is_sha256(cub.get("no_op_header_sha256"))
            or any(
                not campaign.legacy._is_sha256(item.get(key))
                for item in (pinned, cub)
                for key in ("candidate_evidence_sha256", "control_evidence_sha256")
            )
        ):
            raise campaign.CampaignError(
                "matched sanitizer baseline reconciliation failed"
            )
        classified = {
            item[key]: (1, 41386248 if item is pinned else 0)
            for item in (pinned, cub)
            for key in ("candidate_evidence_sha256", "control_evidence_sha256")
        }
        if (
            len(classified) != 4
            or set(classified) != set(hashes.values())
            or any(
                type(run.get("exit_status")) is not int
                or run["exit_status"] != 99
                or type(run.get("raw_error_count")) is not int
                or type(run.get("raw_leak_bytes")) is not int
                or (run["raw_error_count"], run["raw_leak_bytes"])
                != classified[hashes[run["id"]]]
                for run in raw_runs
            )
        ):
            raise campaign.CampaignError(
                "raw sanitizer findings differ from matched controls"
            )
    baseline_cases = baselines.get("cases", [])
    if {case.get("id") for case in baseline_cases} != set(BASELINE_CASES) or len(
        baseline_cases
    ) != len(BASELINE_CASES):
        raise campaign.CampaignError("matched Native A/B baselines missing")
    checks = []
    identities = set()
    for case in baseline_cases:
        if (
            case.get("id") in identities
            or case.get("kind") != BASELINE_CASES.get(case.get("id"))
            or not campaign.legacy._is_sha256(case.get("input_sha256"))
            or case.get("input_sha256") != case.get("reference_input_sha256")
            or case.get("rows") != 262144
            or case.get("timing_boundary_matched") is not True
        ):
            raise campaign.CampaignError("baseline case identity mismatch")
        identities.add(case["id"])
        expected = "native-a" if case["kind"] == "scalar" else "native-b"
        if case.get("reference") != expected:
            raise campaign.CampaignError("numeric baseline owner mismatch")
        revision = {
            "native-a": "c440013c89f1a54a6e5d934aabd0f9d3ef15ed3c",
            "native-b": "5ea60cd31955d0dced2adcbcd3df0772207b79ef",
        }[expected]
        if (
            case.get("reference_revision") != revision
            or case.get("candidate_revision") != identity["sirius_revision"]
        ):
            raise campaign.CampaignError("numeric baseline revision mismatch")
        samples = case.get("reference_seconds", [])
        candidate = case.get("candidate_seconds", [])
        if (
            len(samples) < 7
            or len(candidate) < 7
            or any(
                type(value) not in (int, float)
                or not math.isfinite(value)
                or value <= 0
                for value in samples + candidate
            )
        ):
            raise campaign.CampaignError("numeric raw repetitions missing")
        ratio = statistics.median(candidate) / statistics.median(samples)
        checks.append(
            {"id": case["id"], "ratio": ratio, "limit": 1.1, "passed": ratio <= 1.1}
        )
    if not all(check["passed"] for check in checks):
        raise campaign.CampaignError("matched numeric regression exceeds 10 percent")
    flight_checks = []
    for profile in timing_campaign["flight_profiles"]:
        coverage = profile["coverage"]
        if not coverage:
            continue
        for scale in (1, 10):

            def medians(route):
                return {
                    q: statistics.median(
                        row["wall_seconds"]
                        for row in timing_records
                        if row["scale_factor"] == scale
                        and row["route"] == route
                        and row["phase"] == "performance"
                        and row["measured"]
                        and row["query"] == q
                    )
                    for q in coverage
                }

            embedded, flight = medians("embedded-mo"), medians(profile["id"])
            pairs = [
                (
                    "sum-covered-query-medians",
                    sum(embedded.values()),
                    sum(flight.values()),
                )
            ]

            def suite_median(route):
                return statistics.median(
                    sum(
                        row["wall_seconds"]
                        for row in timing_records
                        if row["scale_factor"] == scale
                        and row["route"] == route
                        and row["phase"] == "performance"
                        and row["suite"] == suite
                        and row["query"] in coverage
                    )
                    for suite in range(1, 6)
                )

            pairs.append(
                (
                    "median-covered-suite",
                    suite_median("embedded-mo"),
                    suite_median(profile["id"]),
                )
            )
            if 9 in coverage:

                def q9(route):
                    return statistics.median(
                        row["wall_seconds"]
                        for row in timing_records
                        if row["scale_factor"] == scale
                        and row["route"] == route
                        and row["phase"] == "q9-concurrency"
                    )

                pairs.append(("q9-stress-median", q9("embedded-mo"), q9(profile["id"])))
            for metric, numerator, denominator in pairs:
                ratio = numerator / denominator
                flight_checks.append(
                    {
                        "id": profile["id"],
                        "scale_factor": scale,
                        "coverage": coverage,
                        "metric": metric,
                        "ratio": ratio,
                        "limit": 1.0,
                        "passed": ratio <= 1.0,
                    }
                )
                if ratio > 1.0:
                    raise campaign.CampaignError(
                        "equivalent Flight comparison exceeds ratio 1.0"
                    )
    timing_summary["local_acceptance_complete"] = True
    timing_summary["remaining_gates"] = ["required-CI", "reviewer-approval"]
    timing_summary["numeric_gates"] = checks
    timing_summary["flight_gates"] = flight_checks
    return timing_summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in (
        "timing",
        "resource",
        "allocations",
        "lifecycle",
        "sanitizer",
        "baselines",
    ):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()

    def load(path):
        return json.loads(path.read_text())

    def records(root):
        return [
            json.loads(line) for line in (root / "runs.jsonl").read_text().splitlines()
        ]

    result = validate(
        load(args.timing / "campaign.json"),
        records(args.timing),
        load(args.resource / "campaign.json"),
        records(args.resource),
        load(args.allocations),
        load(args.lifecycle),
        load(args.sanitizer),
        load(args.baselines),
    )
    print(json.dumps(result, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
