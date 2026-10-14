# COPY ALTER performance evidence runbook

This runbook is the versioned driver contract for issue #28319. The driver is
in `pkg/tests/issues/issue_28319_perf_test.go` and is excluded from normal test
selection by the `issue28319_perf` build tag.

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
