# Publish table-wide ANALYZE NDVs to optimizer statistics

- Superseded collection path: `docs/rfcs/20260904_manual_analyze_sampled_stats.md`
- Status: implemented and validated
- Tracking issue: [matrixorigin/matrixone#27728](https://github.com/matrixorigin/matrixone/issues/27728)
- Implementation PR: [matrixorigin/matrixone#28067](https://github.com/matrixorigin/matrixone/pull/28067)
- Builds on: `docs/design/analyze_stats_publication.md`
- Last updated: 2026-09-04

The schema-version and cache-publication fences described here remain in use.
The later sampled-ANALYZE design replaces the derived full-table aggregate and
metadata-overlay path with bounded storage sampling and coherent-generation
replacement.

## 1. Problem

`ANALYZE TABLE` performs a table-wide `approx_count_distinct` aggregation for
every selected column, but discards those results after returning them to the
client. It then asks disttae to rebuild optimizer statistics from object
metadata. Object metadata contains only one scalar NDV per object, not a
mergeable distinct-value sketch, so the refresh cannot in general recover a
table-wide NDV.

The loss is material for strings whose distinguishing bytes occur after the
eight-byte prefix retained by `ShuffleRange.UpdateString`. On a 10-million-row
table of distinct URL-shaped values with a common prefix, ANALYZE reports
9,967,970 while metadata refresh reports 1,931,682. That estimate selects a
single aggregation owner and takes 7.6 seconds. Patching only the optimizer NDV
to the value already computed by ANALYZE selects 16-way hash shuffle and takes
0.99 seconds on the same table, objects, process, and query. The complete
implementation, including synchronous statistics refresh and publication,
takes 1.18 seconds on the same 10-million-row shape, about 6.4 times faster
than the 7.54-second metadata-only baseline.

This cannot be fixed generically by changing an optimizer threshold or by
summing object NDVs. Two tables may have identical object row counts, zonemaps,
and object-local NDVs while one repeats the same values in every object and the
other has disjoint values. Their global NDVs differ by the object count. The
missing information must come from a table-wide observation or a mergeable
sketch.

## 2. Decision

Reuse the table-wide NDVs that ANALYZE already computes, add `count(*)` to the
same internal aggregate scan, and pass both as typed inputs to the existing
synchronous statistics refresh. The internal count column is removed before
response handling, so ANALYZE's SQL-visible columns and rows do not change.
Disttae continues to collect object count, block count, sizes, null counts,
zonemaps, and shuffle ranges from current object metadata. The table-wide count
replaces the metadata row count, including when committed data is still
represented by in-memory blocks rather than persisted objects. After
collection succeeds and before the atomic cache publication, disttae validates
and applies the count and the table-wide NDVs for only the columns selected by
ANALYZE.

The local cache stores the table-definition version beside only those entries
that contain a table-wide observation. Planner statistics reads carry the
`TableDef` used by the scan, frontend session-cache entries carry the same
version, and disttae returns a schema-bound entry only to a matching planner.
This metadata is process-local: no protobuf, wire, catalog, or persisted format
changes. A remote reader cannot export a schema-bound entry and falls back to
its existing local metadata refresh; local diagnostic readers may inspect the
published process-local value without using it for a versioned plan.

This adds no extra scan, storage, or network work to ANALYZE; one scalar count
state piggybacks on the existing aggregate. It changes neither automatic
metadata refresh nor planning for tables that have not been explicitly
analyzed.

## 3. Required invariants

1. A successful publish contains one complete metadata refresh plus the valid
   table-wide row count and NDV overrides. Readers never observe the
   intermediate metadata estimate. A completed table-wide observation is
   usable even when the persisted-object count or the exact row count is zero;
   its table name distinguishes that state from an uninitialized statistic.
2. Any invalid override or schema-version mismatch rejects the whole refresh
   before cache replacement. Applying a map must not partially mutate the
   candidate statistics object.
3. Override names are canonical physical column names. User spelling and
   original case are resolved against the current table definition before the
   engine boundary.
4. The internal derived result has exactly one row, one value per requested
   column, and one final row-count value; missing, NULL, non-integral, or extra
   values are errors. The final value is never exposed to the client.
5. Row count is an unsigned integer; NDV is finite and non-negative. Every NDV
   and NULL count is capped by the table-wide row count because neither may
   exceed that mathematical upper bound. `approx_count_distinct` may
   legitimately estimate above it before the cap.
6. Partial-column ANALYZE overrides only those columns. Metadata estimates for
   every other column remain intact except for restoring the universal
   `NDV <= rows` and `NULLs <= rows` bounds when the exact row count decreases.
7. Publication ownership, table identity, transaction visibility, cancellation,
   cleanup generation, and plan-cache invalidation remain the contracts from
   `analyze_stats_publication.md`.
8. Engines that do not implement synchronous statistics refresh preserve the
   legacy SQL-visible ANALYZE behavior.
9. A database-only remap may publish the observation for the resolved physical
   table. Any relation rewrite may filter or transform that table, so its
   derived count and NDVs are not treated as whole-table statistics; the
   existing metadata-only refresh remains the safe fallback.
10. A non-system account scan for which the planner injects an implicit account
    filter does not refresh the shared physical-table cache at all. Plan
    construction and ANALYZE admission use the same filter-classification
    function, so adding a new tenant-filtered system table cannot silently
    create a statistics-publication gap.
11. A schema-bound cache entry is consumable by a plan only when it was built
    from the same table-definition version. An unversioned remote reader cannot
    export it; a local diagnostic reader may inspect it. A later metadata-only
    refresh atomically replaces the entry and removes the local version binding.
    Cross-CN lookup routes by the advertised physical `(database ID, table ID)`
    identity, sends the complete statistics key in the RPC payload, and releases
    every successful pooled response even when its optional stats payload is
    absent.
12. Current-table lookup treats the newest row for each table name as
    authoritative. A DROP tombstone hides historical live rows, while
    TRUNCATE exposes only the replacement table identity.

## 4. Visibility and concurrency

ANALYZE publication remains limited to a current physical table when the
statement did not begin inside a user transaction. The derived aggregation and
the frontend's physical table resolution share the outer ANALYZE transaction
snapshot; the metadata refresh starts after the aggregation completes and
reads process-global committed state independently. The derived row count and
NDVs therefore share one snapshot, while a concurrent commit may make the
object metadata slightly newer. This is normal statistics staleness, not a
mixed uncommitted visibility domain. The physical table ID prevents a
drop/recreate of the same name from receiving the old observation. Current
identity resolution is tombstone-aware, so a dropped or truncated historical
row cannot satisfy that check. The schema version carried with the observation
also makes a concurrent ALTER or drop/re-add of a same-named column fail closed
before cache publication. The final schema-version comparison and
`statsInfoMap` plus version-tag replacement execute while the catalog
table-change read lock is held. Publication therefore linearizes entirely
before a concurrent ALTER, or observes the newer version and rejects the stale
observation; there is no validate-then-ALTER-then-publish window. If publication
wins first and ALTER completes afterward, the old entry may remain available
to a matching old snapshot, but a new plan's versioned frontend and engine
reads reject it.

The existing frontend and engine table stripes serialize explicit same-table
publications. Disttae applies the options while holding the exact refresh
generation's admission token and publishes only if the subscription lifetime
is still current. Cancellation, subscription replacement, object-read failure,
or validation failure leaves the previous cache value and plan generation
unchanged. The final request/owner cancellation check and cache swap execute as
one bounded critical section after schema validation, with no intervening wait:
cancellation visible at that point wins and preserves the last-good value and
schema binding; a cache swap that wins first is already committed and is not
rolled back by later cancellation.

Later automatic refreshes may replace an explicitly analyzed NDV after table
metadata changes. Retaining a table-wide NDV across arbitrary UPDATE/DELETE and
compaction events requires either a logical-change provenance signal or
persistent mergeable sketches; scalar object NDVs are insufficient. That is a
separate storage/statistics design and is not approximated here.

## 5. Counterexamples and validation

The acceptance matrix covers:

- high-NDV VARCHAR with a long common prefix: table-wide NDV must replace the
  metadata underestimate and enable hash shuffle;
- high-NDV VARCHAR varying within the first eight bytes: existing range stats
  and range shuffle must remain available;
- low-NDV VARCHAR with the same long prefix: a small table-wide NDV must remain
  small and must not force shuffle;
- empty/all-NULL, one-value, two-value, and committed-but-unflushed tables;
- numeric, date/time, decimal, CHAR, VARCHAR, TEXT, and nullable columns;
- explicit subsets, all visible columns, original-case identifiers, reserved
  identifiers, and duplicate requested identifiers;
- NDV above row count, retained NDV/NULL counts above a newly exact row count,
  unknown columns, negative/fractional/non-finite result cells, schema-version
  replacement both before and after publication, unversioned remote reads,
  complete remote request encoding, physical-key routing, empty-response release,
  DROP tombstones, TRUNCATE replacement identities, malformed result shape,
  cancellation at the final publication fence, refresh failure, and
  subscription cleanup races;
- multiple tables in one ANALYZE statement and unrelated-table parallelism;
- pre-existing user transactions, snapshots, views, temporary tables,
  publications, legacy refresh implementations, database remaps, and
  filtering/transforming relation rewrites.

Unit tests own result decoding and SQL-visible trimming, canonical-name mapping,
validation atomicity, row-count replacement, clamping, partial overrides,
option propagation, legacy-interface fallback, and existing publication
failure/lifecycle contracts. A 55-machine black-box test owns the long-prefix
plan transition and performance comparison on a real 10-million-row table.
