- Status: in-progress
- Start Date: 2026-09-04
- Owners: frontend, disttae, statistics, and optimizer maintainers
- Implementation PR: [#28097](https://github.com/matrixorigin/matrixone/pull/28097)
- Issue: [#27728](https://github.com/matrixorigin/matrixone/issues/27728)
- Design-review decision: pending independent approval on the implementation candidate

# Manual `ANALYZE TABLE` from sampled values

## Summary

Replace the legacy no-op `ANALYZE TABLE` path with a synchronous maintenance
operation that reads visible column values, computes bounded statistics, and
publishes one coherent `StatsInfo` generation to the local CN optimizer cache.

The supported forms are:

```sql
ANALYZE TABLE db.table;
ANALYZE TABLE db.table(col1, col2);
ANALYZE TABLE db.table FULLSCAN;
```

`AUTO` is the default. It reads a deterministic, spatially stratified block
sample. `FULLSCAN` reads every visible row but still uses a fixed-memory NDV
sketch; it promises complete coverage, not exact NDV.

This first version is deliberately operator-controlled. It does not add an
automatic scheduler, a durable statistics catalog, or cross-CN publication.
Those can be added behind the same engine collection and publication
interfaces without changing the SQL contract.

## Motivation and invariants

Object metadata is cheap but cannot estimate correlations, duplicates spanning
objects, or value distributions reliably. Optimizer mistakes on large analytic
tables therefore survive even when object metadata is internally consistent.
Manual analysis supplies a higher-quality input by reading values while keeping
the work bounded for multi-terabyte tables.

The implementation maintains these invariants:

1. Collection observes one transaction snapshot, including persisted and
   in-memory rows and visible tombstones.
2. SQL equality has one owner. NDV hashes keycodec's grouping-canonical value,
   so `+0/-0`, scaled `FLOAT32`, canonical-equivalent JSON, and vector floating
   zero variants do not become false distinct values.
3. A failed or cancelled collection publishes nothing; the previous generation
   remains usable.
4. Publication replaces the complete manual generation atomically. It never
   combines a new table row count with old per-column maps.
5. All user-controlled collectors have explicit bounds. `FULLSCAN` coverage is
   allowed to increase I/O, not NDV memory with cardinality.

## End-to-end flow

```text
SQL / authorization
        |
        v
admission + table/column binding
        |
        v
snapshot range inventory ----> AUTO: stratified block selection
        |                       FULLSCAN: every visible range
        v
table readers + tombstone visibility
        |
        v
per-column aggregation (one column per pass)
  - null count and logical vector bytes
  - grouping-canonical typed hash
  - AUTO: row-frequency + block-incidence COLLAPSE estimator
  - FULLSCAN: fixed p=14 HyperLogLog
        |
        v
all columns succeeded?
   no --> return error, preserve last-good generation
   yes
        |
        v
generation-checked atomic publication to local GlobalStats
        |
        v
ordinary planner StatsInfo consumers
```

The frontend owns statement binding, SELECT-compatible authorization, the
single-table publication mutex, and the public result row. Disttae owns
snapshot visibility, physical range selection, table readers, aggregation, and
the generation-checked cache transition. `pkg/statistics/analyze` owns the
storage-independent sample and NDV algorithms. `keycodec` remains the sole
owner of SQL grouping canonicalization.

## Admission and SQL behavior

ANALYZE is rejected inside an active user transaction because collection and
publication form their own synchronous boundary. Historical snapshots, views,
remote shards, partitions, and engines that do not implement the optional
analysis capabilities fail explicitly. Duplicate targets and missing or
duplicate columns fail before data I/O.

A statement may contain multiple tables. They are processed in bound order and
each table is independently all-or-nothing. The result reports table, mode,
coverage, columns, population rows, sampled rows/blocks/bytes, status, and the
effective row inclusion fraction.

There is no background queue in this version. The operator admits work by
issuing the statement and can bound its duration through the normal statement
context. Cancellation is checked by the storage readers and prevents
publication.

## AUTO selection

The collector first inventories visible physical ranges. Persisted blocks are
divided into at most 64 equal contiguous strata. At least one block per stratum
is selected by deterministic hash-ranked sampling; selection then grows toward
the target while respecting all bounds.

Defaults are:

| Policy | Default |
|---|---:|
| target retained rows | 300,000 |
| minimum selected persisted blocks | 512 |
| maximum selected persisted blocks | 4,096 |
| maximum strata | 64 |
| columns per scan pass | 1 |

The selection seed is stable for an account/database/table identity. Equal
marginal row inclusion is obtained by combining physical block probability
with a conditional row hash threshold. A separate equal-marginal block frame
feeds the COLLAPSE estimator. In-memory rows use the same final inclusion
fractions; persisted tombstones are attached to readers so only snapshot-visible
values contribute.

AUTO scopes tombstone materialization to the selected persisted block IDs.
In-memory Rowid deletes are retained only on an exact block match. Persisted
tombstone objects and blocks are retained or prefetched only when their Rowid
zone maps may cover a selected block; missing or legacy zone maps fail open so
this pruning can introduce false positives but never hide a visible delete.
Consequently retained tombstone memory and tombstone data I/O are proportional
to deletes that may affect the sample, rather than to the table's complete
delete history. Enumerating visible tombstone object metadata is still part of
the inventory. FULLSCAN keeps the unscoped behavior because every persisted
block is admitted.

The inventory itself uses the existing relation range API and is not a claim
that metadata enumeration is free. This version bounds sampled data reads, not
the size of the storage engine's range list.

## Aggregation

### NDV

For `AUTO`, each selected block contributes a set of distinct value hashes to a
block-incidence frame. Repeated occurrences inside one block collapse to one
observation. A Haas-Stokes Duj1 estimate over this frame is compatible with
block sampling and is bounded by the visible non-null population. The ordinary
row-frequency frame is diagnostic; if it overflows independently, a complete
incidence frame can still be published.

The exact AUTO maps have a hard default limit of 4,194,304 distinct hashes per
frame. Incidence overflow fails the table analysis rather than publishing a
biased partial estimate. With one column per pass, peak map memory is
independent of table width, though high-cardinality AUTO samples can still
reach this explicit bound.

For `FULLSCAN`, every non-null visible value is inserted into a dense p=14
HyperLogLog from the outset. Its register memory is about 16 KiB per active
column and does not grow with NDV. The estimate is capped by the exact visible
non-null row population. The mode name denotes complete I/O coverage, not an
exact distinct counter.

### Nulls and size

Nulls and logical bytes are measured on the equal-probability retained row
sample and scaled by the exact visible population. Logical bytes match the
uncompressed vector model: every row owns the type's fixed slot; a non-null
varlen value whose payload exceeds the 23-byte inline capacity additionally
owns its out-of-line payload. Inline payload is not double counted.

### Canonical equality

The collector never hashes raw storage bytes as a substitute for SQL equality.
`keycodec.CanonicalBytesAt` supplies the same representation used by grouping
hashes. The typed NDV hash then adds the logical type ID, width, and scale so
partial states of different types cannot be merged accidentally.

## Publication and consumption

Collection constructs a fresh `StatsInfo` with the exact table row count and
the newly analyzed columns' NDV, null count, logical size, and type. After all
requested columns succeed, GlobalStats clones that object and replaces the
local cache entry under the table generation captured after subscription and
the schema version used to bind the analyzed relation. Cleanup, resubscription,
or ALTER crossing either boundary rejects the publication. The schema check and
cache swap share the catalog change lock, and the frontend session cache records
the same version.
The completed generation carries its table identity and observed row count, so
the shared optimizer validity check recognizes it without misrepresenting the
metadata-derived accurate-object count.

The previous cache entry is not an input to the new generation. In particular,
unselected column maps and metadata-derived min/max or shuffle maps are not
retained beside the new row count. Their absence invokes existing optimizer
fallback behavior instead of presenting mixed-epoch values as one coherent
observation.

When the retained sample covers every visible row, ANALYZE also publishes exact
min/max values for supported ordered types. A partial sample deliberately omits
sample extrema: treating inward-biased sample bounds as the table domain would
make tail predicates appear impossibly selective. Compatible lower and upper
bounds on a regular index are still combined into one bounded lookup without
requiring range metadata.

This version is CN-local and memory-resident. A restart, cache refresh, or later
automatic metadata refresh may replace it. There is no catalog schema, upgrade
step, backup/restore behavior, or mixed-version persistent format in this PR.
A schema-bound entry is not exported over the existing unversioned CN stats
wire contract. ANALYZE is rejected when the caller's table scan has an implicit
account filter, because that subset cannot replace statistics for the shared
physical table.
A future durable provider must add explicit table/snapshot identity, atomic
catalog publication, cache invalidation, and compatibility handling rather than
silently extending this cache record.

## Failure, concurrency, and lifecycle

Per-table frontend serialization prevents two manual publishers on the same CN
from interleaving. Existing GlobalStats refresh ownership and table-generation
checks prevent a late analysis from resurrecting a cleaned-up table. Readers,
batches, and publication locks are released on success, error, and
cancellation.

Errors from range inventory, tombstone collection, readers, arithmetic
overflow, estimator state, or resource bounds abort the current table before
publication. A multi-table statement can therefore have earlier successful
tables followed by a reported error; it does not provide cross-table atomicity.

## Alternatives and tradeoffs

1. Keep deriving all statistics from object metadata. This avoids data I/O but
   cannot repair the estimation errors that motivate the feature.
2. Full-scan every analysis. This is simple but operationally unacceptable for
   terabyte tables and makes routine analysis proportional to table size.
3. Store exact distinct hashes during FULLSCAN. This gives exact NDV but creates
   unbounded memory and predictable OOM risk; fixed-memory HLL is selected.
4. Merge analyzed columns into the previous cache entry. This looks convenient
   for column lists but combines values from different table populations and is
   rejected as a false coherence guarantee.
5. Add durable catalog and automatic scheduling now. Both require distributed
   freshness, upgrade, and policy design. Keeping them out of v1 makes the
   manual accuracy path independently reviewable.

## Validation and acceptance

| Contract | Owner-level evidence | Public evidence |
|---|---|---|
| deterministic bounded selection | sampling unit tests, including boundary strata | AUTO result reports bounded blocks and fraction |
| SQL-equality NDV domain | keycodec canonical tests for scaled floats, signed zero, JSON, and arrays | repeated equivalent values do not increase NDV |
| bounded FULLSCAN NDV | 100k-cardinality HLL/merge test | FULLSCAN completes without exact-map limit |
| varlen logical size | inline, out-of-line, null, and fixed-width unit tests | result publishes size through StatsInfo |
| coherent generation | clone/replacement unit test and generation concurrency tests | failed ANALYZE preserves last-good stats |
| syntax, authorization, cleanup, prepare, and errors | parser/frontend tests | `analyze/analyze_stmt.sql` BVT |

Acceptance requires focused unit tests, affected package tests under the
repository CGO environment, static checks, the analyze BVT on a test-owned
service, and independent approval of this exact design/implementation
candidate. Any future durable or automatic phase requires a design revision.

## Unresolved follow-ups

- durable, account-scoped storage and cross-CN invalidation;
- automatic admission and freshness policy;
- bounded metadata inventory for extremely fragmented tables;
- MCV, histogram, correlation, and uncertainty-aware optimizer consumers;
- replacing AUTO's exact incidence maps with a bounded estimator that retains
  acceptable accuracy under block clustering.
