# Single-CN key preaggregation validation

This records current, traceable evidence for PR #30982. The existing compiler optimization is unchanged; this follow-up rebases it and closes the execution/performance evidence gap. It does not close #27670 or implement the full proposal in #27720.

## Identity and configuration

- Dependency base: `f039b89fa923821fa0fa9219fe567e54a9d0f2ed`; tested optimization: `db0a75e38beb75451f124b0a84029c82422bdffe`.
- Linux amd64, Go 1.27.1, `GOMAXPROCS=4`, session `max_dop=4`, no statistics-overriding optimizer hints.
- Matching baseline/candidate binaries share the same native library and external one-CN SQL integration fixture. The baseline replaces only the compiler implementation with the target version.
- Both use 512MiB CN memory/disk caches, a 512MiB TN memory cache, the existing 1GiB TN disk cache, and a 1GiB fixture spill allowance. The default 32MiB feature-test caches caused repeated evictions; that I/O-dominated diagnostic is excluded from these measurements.
- No production configuration, allocation/spill ownership, runtime knob or execution state was added.

## Public execution and lifecycle

- A 6M-row table contains exactly 3M integer keys, each twice; nullable/empty-string VARCHAR count is independently derived as 2,935,713.
- Candidate physical pipelines replace producer `group(idx:1)` with its key projection before `shuffle(idx:2)`, retaining final key Group and outer count. Baseline retains that producer. Small, duplicate-heavy and padded CHAR controls retain it in both versions.
- Exact integer/VARCHAR/control results pass. One prepared frontend handle is reused with parameters `[3000000, 0, 1, 3000000]` and exact results.
- At `agg_spill_mem=33554432`, the final key Group records positive `GroupSpillSerializedBytes` and reloads, followed by exact ordinary-query results. This threshold is per operator, not a statement cap.
- After observing active Group and shuffle calls, `KILL QUERY` returns 1317 in 5.238ms; the same connection then returns the exact 3M count. This does not claim cancellation during spill I/O.
- Task databases were dropped and their absence verified; owned embedded services were closed.

## Sequential ABBA measurements

One warmup and three measured executions per phase produce six samples per version. All results are consumed and checked. Startup, loading, ANALYZE and EXPLAIN are excluded. No other task build/test ran during these phases.

| Synthetic workload | Baseline median (s) | Candidate median (s) | Baseline / candidate |
| --- | ---: | ---: | ---: |
| duplicate_control | 0.052821 | 0.045323 | 1.165x |
| eligible_integer | 0.422883 | 0.180913 | 2.337x |
| small_control | 0.052015 | 0.047817 | 1.088x |

These are bounded measurements, not a universal no-regression guarantee or the historical Apple M4 results. Full samples and ranges are retained in the evidence record.

## Validation and audit trail

- Full compiler UT: 1553 roots. Owning compiler race: 1553 roots; shuffle 46 and projection 6 roots. No failures or race reports.
- `go vet` passes. Pinned molint exits successfully with nine existing diagnostics in files verified byte-identical to the base. Configured incremental golangci-lint on `pkg/sql/compile` reports 0 issues; no new SCA finding is demonstrated.
- The [machine-readable evidence](single-cn-key-preaggregation-evidence.json) contains source/native/binary hashes, command and exit receipts, SQL data generation, raw phase samples, logical plans, physical pipeline excerpts and Group spill statistics.
- Full diagnostic source, overlays and runtime logs are retained at the recorded external paths with SHA-256 hashes. They are evidence artifacts, not million-row ordinary UT fixtures.
