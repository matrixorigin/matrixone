# Catalog dependency watermark attribution

Status: experimental instrumentation only (`MEASUREMENT_PENDING`)

Base: `344d852ac3288e391a3ae6be3e3c7108caf2b75d`

This document describes the opt-in attribution surface for issue #27235. It
does not propose changing the production invalidation decision. The exact
BTree remains the authority; bucket and precise-shadow decisions are recorded
for comparison only.

## Scope

The experiment compares three decisions at the two real consumers:

- exact BTree lookup, which is returned to the caller;
- the existing 4096-account bucket high-watermark, with the exact database
  identity check retained;
- a precise shadow keyed by account, database identity/name, and table name,
  carrying object identity, version, timestamp, and deletion state.

The shadow state accepts only newer timestamps. Older replay and GC events do
not lower it. Equal-timestamp identity conflicts are marked ambiguous and
force a conservative shadow decision. State is bounded to one latest entry per
database/table key plus bounded latency histograms; the report includes an
estimated retained-byte value rather than claiming allocator accounting.

Attribution is disabled unless `MO_CATALOG_INVALIDATION_ATTRIBUTION=1` is read
during catalog construction, or a test harness explicitly enables it before
using the cache. The disabled path does not read the environment, take a
timestamp, update counters, or allocate shadow state from the hot decision
path.

## Report contract

`CatalogCache.WriteCatalogInvalidationReport` emits the per-CN JSON fragment
with schema version 2:

- exact decision/check/invalidation counts per prepared-plan and RC table-cache
  consumer, split into stable and inconclusive checks;
- bucket and precise false-positive/false-negative counts separately;
- catalog event counts;
- prepared rebuild and RC reload bounded p50/p95/p99 latency plus the raw
  bounded histogram buckets;
- shadow account/entry counts and estimated retained bytes;
- explicit MatrixOne SHA, config, collection window, and integrity metadata.

The two-CN harness wraps the fragments in a schema-v2 envelope containing the
CN service identities, scenario/DDL/consumer mapping, merged decision totals,
merged histograms, and `window_start_utc`/`window_end_utc`. It writes the
report to `catalog-invalidation-report.json` through a same-directory
temporary file and atomic rename. Missing artifacts or a non-`complete`
integrity value are measurement failures, not zero results.

## Required measurement matrix

Before any production proposal, run the differential correctness matrix for
no-change, alter, truncate, rename, drop/recreate, empty-database
drop/recreate, same-account unrelated DDL, forced account collision,
equal/older/newer timestamps, replay, checkpoint restore, GC, text/binary
prepared execution, session reuse, and RC table-cache reuse. Add randomized
event sequences and deterministic update/check barriers under race.

For exact, bucket, and precise microbenchmarks, use history lengths 1, 16, 256,
and 4096, measure changed and warmed-negative paths for five runs, and retain
the raw output plus `benchstat`. Also compare the attribution-disabled wrapper
with the exact implementation. A warmed negative precise path must remain
`0 allocs/op`, and the disabled wrapper must not add more than 3% median
latency. These measurements are directional and must not be converted into
TPCC TPS claims.

## Candidate gate

Every false negative permanently rejects that candidate. Precise shadow may
advance to a separate production Draft PR only after it matches the exact
oracle across the full matrix, proves replay/GC monotonicity, and has an
accepted retained-memory bound. Bucket may advance only after Catalog,
frontend, and RC-cache owners record explicit false-positive and rebuild/reload
budgets in #27235. If neither candidate meets those gates, retain the exact
BTree implementation and publish the report without opening a production
implementation PR.

No claim is made here about isolated TPS, transaction P95/P99, or the historical
`+20%` target. The previous A+C result (`+0.7026%`) remains combination-only
evidence and is not reused as a C-only result.
