# Role-rule freshness: authenticated TCP measurements

Historical measurements of the earlier request-only implementation. The current
statement-owned admission design and its expanded matched matrix are documented
in [Statement authorization admission](statement-authorization-admission.md).
The reproduction command below applies to the pinned historical harness; the
current harness requires the count-only instrumentation described in that report.

Measured on 2026-10-16 UTC to close PR 30400's request-admission review.
The candidate performs one successful role-catalog freshness proof per stable
prepared request, shared by the parse, computation-wrapper and parameter
lookups. A new request, principal change, cache generation/revision change,
error, cancellation or request completion prevents reuse. Prepared-plan
invalidation checks still run on every lookup.

## Revisions and environment

- Official main baseline: `1e844f26a491dcc0f9cd4a6979302def92051e81`.
- Candidate: parent `25d9a9612bfce5023adbff45436d11558d2eddb6` plus this
  change. The raw artifact records SHA-256 hashes of the three changed
  production files and the identical benchmark harness used in both builds.
- Go 1.27.1, Darwin/arm64, `CGO_ENABLED=1`, `GOMAXPROCS=10`, `matrixone_test`,
  release native libraries; no race instrumentation during measurement.
- One embedded CN, authenticated MySQL TCP, one sequential client, two rows,
  a non-admin role and a mandatory rule excluding the second row. Every
  request consumes its rows and asserts that only the permitted row is returned.
- Baseline source was a clean `git archive` export, with all 11,332 Git blobs
  verified before and after execution. Only this benchmark file was added.
  Both builds used identical native inputs; no baseline production patch.

The final delivery was subsequently rebased onto official main
`708a472f0116feb992672bf047cb464b5173e628`. That increment changes UT scheduling
and tests, plus Iceberg matched-delete row-ordinal overflow handling. The
frontend, role-catalog proof, ordinary-table SELECT, planner/evaluator,
transaction and native-input paths exercised here are unchanged. The direct
test commands do not use the changed UT scheduler. Their existing evidence is
reused, retaining the actual measured revisions and hashes above and in the
raw artifact.

## Protocol

`BenchmarkRoleRuleFreshnessTCP` owns its rounds, so run it with
`-benchtime=1x`. Each configuration starts a fresh cluster. Text and binary
protocols use separate authenticated connections; binary retains one prepared
statement. Connection setup, PREPARE and first execution are recorded separately
as `first-execution` samples. These are first-use measurements within the
started cluster, not a machine-cold cache or cluster-startup benchmark.

Each connection then runs one five-second warm-up and three separately recorded
five-second measured rounds. There are 60 raw records: 12 first-use samples,
12 warm-ups and 36 measured samples. All six runs passed with no request
errors or rule-filter violations. No concurrent benchmark or other test ran
while measuring. Run order was default base/candidate, preserve candidate/base,
then limited base/candidate; timestamps and log digests are retained.

| Configuration | Sacrificing freshness | Transaction limit/s |
| --- | ---: | ---: |
| default | 1 | unlimited (configured 0) |
| preserve | -1 | unlimited (configured 0) |
| limited | 1 | 100 |

The table below is the median of the three measured rounds for each metric;
latencies are milliseconds. Warm-ups and first-use samples are excluded.

| Configuration | Protocol | Revision | Requests/s | p50 ms | p95 ms | Snapshot observations/request |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| default | text | base | 7548.14 | 0.131 | 0.145 | 2.003 |
| default | text | candidate | 7215.96 | 0.136 | 0.150 | 3.007 |
| default | binary | base | 9751.27 | 0.102 | 0.111 | 2.002 |
| default | binary | candidate | 9466.47 | 0.105 | 0.114 | 3.005 |
| preserve | text | base | 134.51 | 7.894 | 8.541 | 2.165 |
| preserve | text | candidate | 91.39 | 11.634 | 12.494 | 3.492 |
| preserve | binary | base | 133.74 | 7.910 | 8.500 | 2.164 |
| preserve | binary | candidate | 91.46 | 11.658 | 12.418 | 3.441 |
| limited | text | base | 41.87 | 20.099 | 40.128 | 2.389 |
| limited | text | candidate | 24.36 | 30.144 | 61.139 | 4.106 |
| limited | binary | base | 41.32 | 20.087 | 40.221 | 2.420 |
| limited | binary | candidate | 24.21 | 30.205 | 60.503 | 4.131 |

The default configuration loses about 4.4% text / 2.9% binary throughput
against main. With freshness preserved, throughput falls about
32.1%/31.6%, and p95 rises from 8.54/8.50 ms to
12.49/12.42 ms (text/binary). At the 100/s admission limit,
throughput falls about 41.8%/41.4%, and p95 rises from
40.13/40.22 ms to 61.14/60.50 ms. This is a material cost
under those configurations. The baseline lacks cross-CN role freshness; the
comparison measures the complete freshness change, not the isolated speedup
from deduplicating prepared lookups.

The candidate adds a real current role-catalog proof instead of trusting a
warm session cache. Its snapshot determination shares transaction admission and,
with sacrificing freshness disabled, waits for current logtail progress.
Deduplication bounds repeated prepared lookups to one successful proof for an
unchanged request; it does not eliminate that proof or waive freshness under
rate limiting. Deployments using those configurations must account for this
additional work and the measured capacity/latency cost. These local one-client
measurements do not establish a production capacity guarantee or a mixed-load
SLA.

The snapshot column is the delta of the existing process-wide
`TxnDetermineSnapshotDurationHistogram` sample count, divided by requests.
It includes real transaction starts and background work; longer rate-limited
rounds can have more background observations per request. It is **not** an
isolated role-proof/admission count, nor proof that each request consumed that
many rate-limit tokens. `TestPreparedFreshnessAdmissionRequestScope` separately
asserts exactly one role-proof provider call across three stable prepared
lookups, plus new-request and invalidation boundaries.

## Reproduce and audit

Run serially from a checkout containing the harness and prepared native inputs:

```sh
MO_ROLE_FRESHNESS_BENCH_CONFIG=default GOWORK=off GOMAXPROCS=10 \
  .agents/skills/mo-dev/scripts/mo-cgo-test -short -tags matrixone_test \
  -run '^$' -bench '^BenchmarkRoleRuleFreshnessTCP$' -benchtime=1x \
  -count=1 -timeout=20m ./pkg/tests/issues
```

Repeat for `preserve` and `limited`, then repeat against the exact baseline
with the identical harness copied into an otherwise clean source export. The
original runs compiled separate binaries before measurement and ran one binary
at a time; the command above executes the same harness. Preserve platform-matched
native inputs and build flags when comparing. Do not run another cluster/test
concurrently with the measurements.

[Raw samples and provenance](role-rule-freshness-results.jsonl) retain every
round, first-use timing, requests, errors, elapsed duration, p50/p95, process-wide
snapshot delta and build identity. They can be regenerated from the
`ROLE_FRESHNESS` JSON lines emitted by the benchmark. The harness deliberately
stops on any SQL, protocol or row-filter failure instead of reporting successful
throughput for erroneous requests.
