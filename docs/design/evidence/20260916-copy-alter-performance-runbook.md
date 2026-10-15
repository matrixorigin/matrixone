# COPY ALTER performance evidence runbook

This runbook is the versioned driver contract for issue #28319. The driver is
in `pkg/tests/issues/issue_28319_perf_test.go` and is excluded from normal test
selection by the `issue28319_perf` build tag.

## Revision 4 execution preparation (2026-10-15)

**Not launch-ready.** Revision-4 maintainer approval and its implementation are
pending. The commands below describe the historical driver interface; they do
not prove a revision-4 runner exists. Do not launch the old fixed base/head
payload against current sources or infer optimization from fallback correctness.

Before the new-head run, freeze these changes as a new runner generation:

- Bind baseline to the approved integration main and candidate to the repaired
  full SHA, rather than old `8a4c84f4` / `5be67e36`; record actual source deltas,
  observer/driver hashes, binaries, native libraries and required Go 1.27.1.
- Observe actual narrow/broad admission and shared/exclusive lock attempts.
  Replace old base-before-copy/head-after-copy exclusive-gate assertions. Prove
  complete observer coverage before accepting zero exclusive attempts; missing
  counters remain UNAVAILABLE. Keep copy and transaction identity accounting.
- Reuse the successful personal Ubuntu end-acknowledgement protocol: sample
  live child I/O at ready and measurement end, require collector acknowledgement
  before post-workload validation/cluster close. Report boundary lag and collector
  CPU attribution. OS I/O is not a physical copy-row/byte counter.
- Use native ext4, the verified fixture range 21000–22000 outside ephemeral
  ports, 8 logical CPUs/quota, 14GiB memory, worker swap disabled, GOMAXPROCS=8
  and GOMEMLIMIT=10GiB. Keep the independent watchdog, exact owner/process-group
  cleanup, 120-minute unit deadline and 50GiB disk floor. Refresh live admission;
  a historical idle/cleanup receipt is not a reservation.
- First run both-arm same-generation smoke (2 workers, 8 rows, width 16, one
  ADD/DROP round). Then run 1/2/4/8 workers, 8192 rows per table, width 1024,
  20 rounds per worker, 30-second request deadlines. At each level execute
  B/H, H/B, B/H, giving 24 formal arms. Never drop failed attempts or retry
  individual requests. Freeze new inputs after any repair before a new series.
- Validate full byte contents, row/key/allocator state, absence of private
  relations, a follow-up ALTER and final cluster/resource cleanup. Record all
  paired ratios before judging thresholds in the validation matrix.

Race preparation must preserve separate top-level failure cases and collect
actual `go test -race -json` test terminals (not package aggregate time).
Every new/modified case must be <=45.55s both on Ubuntu and in exact-head CI.

Nightly uses the accepted fixed reconstructed mo-load input (source
`25af0236ff773f4f9b854a48b83d82c720c3a10a`, JAR SHA256
`c7b8fbea399efe34ef227425407e58a2e86589ffd8170482496163df038f7f47`),
JDBC 8.0.19 and the locked start.sh/JVM heap. Preserve 100 tables with seed
values 56/222/686/9985, ADD/DROP 50+50 users, mode 0, five minutes and only
1068/1091 allowed. Use three serial equivalent admitted TKE runs with
3CN/1TN/3Log/2Proxy and JVM Xms1024M/Xmx30720M; record actual effective timeout,
classpath, JAR/JDBC hashes, subject/image, deployment, owner and independent
namespace/PVC/PV/CBS cleanup. Do not resize this topology/heap to fit the
personal host or alter the accepted historical input lock's subject in place.

## Embedded diagnostic run

Run one entry point at a time from a clean checkout. The default shape is two
workers, 8192 rows per table, 20 ADD/DROP rounds per worker (80 ALTERs total):

```text
MO_ISSUE28319_SOURCE_SHA=$(git rev-parse HEAD) \
  ./.agents/skills/mo-dev/scripts/mo-cgo-test \
  -tags issue28319_perf -count=1 -timeout=1800s \
  -run '^TestIssue28319Performance$' ./pkg/tests/issues \
  -args -issue28319-run-mode=embedded -issue28319-mode=query \
  -issue28319-run-id=<run-id> -issue28319-workers=2 \
  -issue28319-rows=8192 -issue28319-width=1024 \
  -issue28319-rounds=20 -issue28319-timeout=30s \
  -issue28319-output=<output-directory>
```

Use `query`, `text`, `binary` and `executor` for the four production entry
points. The text and binary modes keep a connection-local prepared ADD
statement and use the direct DROP statement to restore the schema before the
next prepared ADD, matching the existing prepared ALTER regression. The
executor mode is embedded-only.

The hook records `copy-started` and `data-copied`, transaction IDs, and the
per-request result. `copy_rows` and `copy_bytes` are `UNAVAILABLE` unless a
future observer can obtain actual physical counters; configured table size is
never used as a substitute. A successful normal run must contain 80 records,
80 copy starts, 80 completed copies, zero errors and one transaction attempt
per request. The final checks also verify row counts, distinct IDs, payload
length, primary-key state, temporary relation cleanup and a follow-up ALTER.

## External service run

Set `MO_ISSUE28319_DSN` to an isolated deployment connection and use
`-issue28319-run-mode=service`. The same database is created from the run ID
and dropped at the end. This mode reports client timing and errors; internal
copy counters remain `UNAVAILABLE` because no in-process observer is installed.
It must therefore be paired with service-side transaction or lock tracing when
gate wait and hold intervals are required.

Every run writes `requests.jsonl`, `summary.csv` and `manifest.json`. The
manifest contains the source SHA, complete command, tool revision, workload
parameters, database name, status and output timestamps. Failed requests are
written before the test returns; no client retry is performed.

The driver is a diagnostic instrument, not a substitute for the paired
baseline/head experiment. Formal performance status remains `NOT_RUN` or
`BLOCKED_ENVIRONMENT` until both versions use the same tool revision, data,
configuration and isolated environment.
