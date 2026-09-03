# Publish table-wide ANALYZE NDVs to optimizer statistics

- Status: implemented and validated
- Builds on: `docs/design/analyze_stats_publication.md`
- Last updated: 2026-09-03

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

This adds no extra scan, storage, or network work to ANALYZE; one scalar count
state piggybacks on the existing aggregate. It changes neither automatic
metadata refresh nor planning for tables that have not been explicitly
analyzed.

## 3. Required invariants

1. A successful publish contains one complete metadata refresh plus the valid
   table-wide row count and NDV overrides. Readers never observe the
   intermediate metadata estimate.
2. Any invalid override rejects the whole refresh before cache replacement.
   Applying a map must not partially mutate the candidate statistics object.
3. Override names are canonical physical column names. User spelling and
   original case are resolved against the current table definition before the
   engine boundary.
4. The internal derived result has exactly one row, one value per requested
   column, and one final row-count value; missing, NULL, non-integral, or extra
   values are errors. The final value is never exposed to the client.
5. Row count and NDV are finite and non-negative, and NDV is capped by the
   table-wide row count. `approx_count_distinct` may legitimately estimate above that
   mathematical upper bound.
6. Partial-column ANALYZE overrides only those columns. Metadata estimates for
   every other column remain intact.
7. Publication ownership, table identity, transaction visibility, cancellation,
   cleanup generation, and plan-cache invalidation remain the contracts from
   `analyze_stats_publication.md`.
8. Engines that do not implement synchronous statistics refresh preserve the
   legacy SQL-visible ANALYZE behavior.
9. A database-only remap may publish the observation for the resolved physical
   table. Any relation rewrite may filter or transform that table, so its
   derived count and NDVs are not treated as whole-table statistics; the
   existing metadata-only refresh remains the safe fallback.

## 4. Visibility and concurrency

ANALYZE publication remains limited to a current physical table when the
statement did not begin inside a user transaction. The derived aggregation
commits before metadata refresh starts. Its row count and NDVs share one
snapshot, while a concurrent commit may make the object metadata slightly
newer. This is normal statistics staleness, not a mixed uncommitted visibility
domain. Publishing the internally consistent table-wide count and NDVs avoids
mixing two snapshots within cardinality statistics.

The existing frontend and engine table stripes serialize explicit same-table
publications. Disttae applies the options while holding the exact refresh
generation's admission token and publishes only if the subscription lifetime
is still current. Cancellation, subscription replacement, object-read failure,
or validation failure leaves the previous cache value and plan generation
unchanged.

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
- NDV above row count, unknown columns, NULL result cells, malformed result
  shape, cancellation, refresh failure, and subscription cleanup races;
- multiple tables in one ANALYZE statement and unrelated-table parallelism;
- pre-existing user transactions, snapshots, views, temporary tables,
  publications, legacy refresh implementations, database remaps, and
  filtering/transforming relation rewrites.

Unit tests own result decoding and SQL-visible trimming, canonical-name mapping,
validation atomicity, row-count replacement, clamping, partial overrides,
option propagation, legacy-interface fallback, and existing publication
failure/lifecycle contracts. A 55-machine black-box test owns the long-prefix
plan transition and performance comparison on a real 10-million-row table.
