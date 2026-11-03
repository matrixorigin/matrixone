# Rows an index cannot store

Status: proposed; revised 2026-10-29 after review (consistency, recovery and bounds),
2026-10-30 (limit on the rows left out, error codes, restore), and 2026-11-02 (CAGRA and IVF-PQ).
Issue: #31583 (fixes #31493).

## Problem

An index can be given a source row whose value it cannot store, for example a `vecf32` element
above 65504 in an HNSW index that stores `float16`. The source table accepted the row, so the
write cannot be undone, and an asynchronous index learns about the row only after it is
committed. Retrying the change cannot succeed: the same value fails the same way on every
replay. The index stops at that row, every later change of the table is missing from it, and
queries keep using the stale index without any warning.

## Decision

Decided by @cpegeric, 2026-10-28.

A source row whose value an index cannot store is left out of that index and recorded, with
the reason, in the index's ignored rows. The index keeps building and syncing. This follows
Elasticsearch's `ignore_malformed`: the document is indexed without the malformed field, and
the field name is recorded in the document's `_ignored` metadata.

- **The ignored rows hold exactly the source rows that exist and are not in the index** because
  their value cannot be stored. A row is recorded when it is inserted or updated with such a
  value. It leaves the ignored rows when it is deleted, or updated to a value the index stores.
  Updated to another value the index cannot store, its reason changes.
- **An index leaves out at most `MAX_BAD_RECORDS` rows** (decided by @cpegeric, 2026-10-30).
  It is an option of the index, given at CREATE INDEX and changed by ALTER REINDEX, and is 0
  unless given, as BigQuery's `maxBadRecords` defaults to 0. Every build and sync applies it:
  one row more than the limit fails a synchronous build, which rolls back as a whole, and
  stops an asynchronous build or sync with a permanent error. A stopped index is not retried;
  REINDEX rebuilds it and starts it again, so the user fixes the data or raises the limit and
  runs REINDEX. A rejected alternative was a session variable: the limit belongs to the
  index, the background sync has no session, and it must be visible and changeable.
- **The limit counts the rows the index leaves out**, not the rows of one statement or sync:
  it bounds what the index is missing. A partitioned table keeps an index on each partition,
  and each has the limit.
- **A build that can store none of the rows it reads fails**, within the limit too: an index
  with no row of a table that has rows is a wrong index, not a partial one. An incremental
  sync is not a build: its changes may all be rows the index cannot store.
- **A restore follows the limit of the index it restores** (@cpegeric, 2026-10-30). It
  restores the index with its `MAX_BAD_RECORDS` and rebuilds it from the restored rows, as
  REINDEX does: the rows are those the index covered, so an index within its limit is
  restored within it, with the same ignored rows, and an index stopped by the limit is
  restored stopped, with the same error. A rejected alternative left out every row without a
  limit, a rule of its own. REINDEX and TRUNCATE start the ignored rows over.
- **Each reason is an error code.** The ignored rows hold the code; a message is optional and
  absent unless a code needs more detail. SHOW INDEX IGNORED shows the message of the code.
- **Only the value of one row is ignored.** An error that does not belong to one row's value,
  such as a lost connection, a failed write of the index or a missing model, keeps its existing
  handling.
- **A search vector is not a row.** A query vector the index cannot store is rejected.
- Each build or sync step that ignores rows writes one log line with the index or its table,
  the count, and a primary key with its reason.

## Consistency, recovery and bounds

- **One commit.** An incremental sync of an HNSW, CAGRA or IVF-PQ index writes the index, its
  ignored rows and the advanced watermark in one transaction. If the sync fails or the CN stops before that
  commit, nothing of it is visible and the next sync replays the same changes from the old
  watermark, recomputing the same ignored rows. A build writes its ignored rows in the
  statement that builds the index.
- **Restart.** The ignored rows are only in the table; a sync keeps no state of them between
  iterations, so a restart needs nothing beyond the watermark.
- **Clone and restore.** The ignored table is one of the index's hidden tables. A clone, and a
  restore, copies the ignored rows with the index as of the same snapshot; the rebuild of the
  index that follows records the same rows again.
- **Retry.** A row is ignored only for a value the index cannot store, which fails the same way
  on every retry, so no automatic retry is made, and an index stopped by the limit is not
  retried either. If a row was ignored because of a defect of MatrixOne that is later fixed,
  REINDEX retries every row: it reads all source rows again and starts the ignored rows over.
- **Memory and work.** A sync never reads the ignored rows. It reads only whether the table has
  one, and then records the primary keys of the rows it changes, so its memory follows the
  changes of the iteration, which it holds anyway, never the number of ignored rows; after
  recording rows it counts the table against the limit. A build, and a sync that rebuilds the
  index, keep the rows they ignore in a local file, as they spill the index itself, and write
  them in the statement that commits the index: their memory does not grow with the limit,
  nor with the partitions of a table, whose indexes are built at the same time and each have
  the limit (@cpegeric, 2026-10-30; a rejected alternative capped the limit to bound that
  memory). Writes to the table go in statements of at most 1024 rows.

## Interface

```sql
CREATE INDEX ... USING hnsw|cagra|ivfpq ... MAX_BAD_RECORDS n
ALTER TABLE tbl ALTER REINDEX idx hnsw|cagra|ivfpq MAX_BAD_RECORDS = n
SHOW INDEX IGNORED FROM [db.]tbl [WHERE expr]
SHOW INDEX STATUS FROM [db.]tbl [WHERE expr]
```

`MAX_BAD_RECORDS` is the number of rows each index may leave out; of a partitioned table, each
partition's index may leave out that many.

It returns one row per ignored source row of every index of the table:

| Column | Content |
|---|---|
| `Key_name` | the index |
| `Primary_key` | the primary key of the source row |
| `Error_code` | the error code of why the index cannot store the row |
| `Reason` | the message of the row, or of its code |
| `Ignored_at` | when the row was recorded |

`WHERE` filters on these columns, as in `SHOW INDEX`. For a partitioned table it lists the
ignored rows of the indexes of every partition. It shows rows of the table, so it requires the
`SELECT` privilege on the table.

`SHOW INDEX STATUS` returns one row per index of the table, or of each partition, that is kept
in sync or has ignored rows. The sync jobs are kept by the system account, so an account cannot
read them itself; this statement shows those of the account, under the same privilege:

| Column | Content |
|---|---|
| `Key_name` | the index |
| `Table` | the table, or the partition, of the index |
| `State` | `running`, `retrying` after an error that is retried, or `stopped` by a permanent error |
| `Error_message` | the error of the sync, if any |
| `Ignored_rows` | the number of ignored rows |
| `Max_bad_records` | the limit of the index, of each partition for a partitioned table |
| `Synced_at` | the time the index is synced to |

## CAGRA and IVF-PQ

Measured on a GPU before this change, with one source row a cuVS index cannot store:

| Case | CAGRA | IVF-PQ |
|---|---|---|
| vecf32 with `QUANTIZATION 'float16'`, an element of 100000 | the build fails (`RAFT failure ... Could not generate an intermediate CAGRA graph`) | the build succeeds, and a search of any vector returns no row |
| vecf32 without quantization, an element of 1e20 | the build fails (`RAFT failure`) | the build succeeds |
| the row arrives by incremental sync | the sync succeeds (the row is searched in the brute-force tail); every later build from the source (REINDEX, restore, ALTER COPY, the scheduled reindex) fails | as the build |

**What the index cannot store** (decided by @cpegeric, 2026-11-02), for both indexes:

- with `QUANTIZATION 'float16'` on a vecf32 column, an element at or beyond the float16 range,
  the rule HNSW applies;
- a vector whose squared L2 norm exceeds float32 max / 4. The index computes L2 distances in
  float32, and the squared distance of two such vectors is at most (‖a‖ + ‖b‖)² ≤ float32 max,
  so no distance overflows. HNSW has no such rule. IVF-PQ built in the case above, but its
  k-means also computes distances in float32.

Both are `ErrIndexVectorOutOfRange`. A vecf16 column holds no value either rule rejects, int8 and
uint8 storage saturate rather than overflow, and a source column holds no NaN or infinite
element. `CheckStorable` in `pkg/vectorindex/cuvs` is the one test, used by the incremental sync
(the cuvs CDC writer) and by the builds (`cagra_create`, `ivfpq_create`).

**Builds** (@cpegeric, 2026-11-02). A build plans its GPU sub-indexes from the count of the
non-NULL rows before it reads them, and cuVS builds no sub-index of fewer rows than its minimum
(CAGRA: more than `intermediate_graph_degree`; IVF-PQ: `lists`). A left-out row, like a NULL
vector, does not take a place in the plan, so it shortens the last sub-index. The build therefore
holds the first minimum rows of each sub-index until all of them arrive, and the rows still held
at the end go to the brute-force tail with the rows the plan puts there, where they are
searched. A sharded build is one sub-index split across its devices, each shard
`(rows / devices) &^ 31` rows, so it holds devices × the minimum rounded up to a multiple of 32.
The held rows are charged to the host memory the build plans with. Without an ignored table a build holds no rows and checks none. Rejected: a new SQL
function making the count exclude the rows (a second home of the rule), and failing such a build.

**Search vectors.** A query vector either rule rejects is rejected with the same error, by every
CAGRA and IVF-PQ index, with an ignored table or without one; before this change the search
returned no row.

**Incremental sync cost** (decision log). The sync records a removal for every row it indexes
while the ignored table has a row, so a large insert into such a table issues one `DELETE ... IN`
per 1024 rows in its commit. This is the tracker HNSW uses (`pkg/iscp/index_ignored.go`); a
change that skips plain inserts would apply to every index and is not made here.

## Scope

HNSW, CAGRA and IVF-PQ record their ignored rows. IVF-Flat and FULLTEXT2 follow under #31583.
The ignored table is created with the index. An index created before this change has none and
keeps its previous behavior, without SHOW INDEX IGNORED rows, until it is dropped and created
again; REINDEX does not add the table.

## Claims

Each claim heads a section of the black-box suite
`test/distributed/cases/pessimistic_transaction/vector/vector_hnsw_ignored.sql` (`-- I<n>`); the
CAGRA and IVF-PQ claims, and I9 for them, head sections of
`test/distributed/gpu_cases/vector/vector_cuvs_ignored.sql`, which runs every section for each.

| # | Claim | Unit proof |
|---|---|---|
| I1 | A row an HNSW index cannot store, of any column type, does not stop later rows from being indexed while the index is within its limit. | `TestHnswWriterIgnoresUnstorableVectors`, `TestCheckStorableMatchesToStorage` |
| I2 | The ignored rows follow the source rows: a delete, or an update to a storable value, removes a row; an update to another value the index cannot store changes its code. | `TestIgnoredRowsTransitions`, `TestIgnoredRowsLoadIsBounded` |
| I3 | Every build and sync records the rows it leaves out, and one row more than `MAX_BAD_RECORDS` (0 unless given), or a build that can store none of its rows, fails a synchronous build and leaves no new index, or stops an asynchronous build or sync with a permanent error; REINDEX and TRUNCATE start the rows over. | `TestHnswCreateIgnoresUnstorableRows`, `TestIgnoredRowsLimit`, `TestPermanentErrorCause` |
| I4 | ALTER REINDEX with `MAX_BAD_RECORDS` changes the limit, rebuilds the index and restarts a stopped one. | `TestValidateReindexParams_MaxBadRecords`, `TestReindexSpecifiedParams_MaxBadRecords` |
| I5 | A search vector the index cannot store is rejected. | `TestToStorage` |
| I6 | `SHOW INDEX IGNORED` lists the ignored rows of every index and partition of a table with their code and message, and `WHERE` filters them. | `TestShowIndexIgnored`, `TestIgnoredReasonSql` |
| I7 | `SHOW INDEX IGNORED` requires `SELECT` on the table. | `TestShowIndexIgnored` |
| I8 | An index without an ignored table builds and syncs as before. | `TestNewIgnoredRowsWithoutTable`, `TestHnswCreateIgnoresUnstorableRows`, `TestShowIndexIgnored`, `TestCuvsWriterWithoutIgnoredTable`, `TestCuvsIgnoredRowsWithoutTable` |
| I9 | A restore follows the limit of the index it restores: an index within its limit is restored with the same ignored rows, and one stopped by the limit is restored stopped. | `TestPermanentErrorCause`, `TestHnswCreateIgnoresUnstorableRows` |
| I10 | `SHOW INDEX STATUS` shows, in any account, whether each index of a table is in sync or stopped, with its error, its ignored rows and its limit; it requires `SELECT` on the table. | `TestShowIndexStatus`, `TestShowIndexStatusSql`, `TestStoppedJobState` |
| I11 | A CAGRA or IVF-PQ index cannot store a vecf32 element beyond the float16 range when it stores float16, or a vector whose squared L2 norm exceeds float32 max / 4; float32 and int8 storage, and a vecf16 column, store the float16 case. | `TestCheckStorable`, `TestCuvsWriterIgnoresUnstorableVectors`, `TestCuvsWriterFloat16OnlyNarrowsVecf32` |
| I12 | CAGRA and IVF-PQ apply the limit as HNSW does (I1–I4): a synchronous build fails past it, or when it can store none of its rows; an asynchronous build or incremental sync stops past it; ALTER REINDEX with a higher limit restarts it; an update to a storable value, or a delete, removes a row. | `TestCuvsIgnoredRowsLimit`, `TestCuvsIgnoredRowsAllBad`, `TestCagraValidateReindexParams_MaxBadRecords`, `TestIvfpqValidateReindexParams_MaxBadRecords`, `TestCagraGenBuildSQL_Ignored`, `TestIvfpqGenBuildSQL_Ignored` |
| I13 | Left-out rows never give cuVS a sub-index, or a shard of a sharded build, smaller than it builds; the rows of such a sub-index go to the brute-force tail and are found by a search, with their INCLUDE values. | `TestCuvsIgnoredRowsHoldsShortSubIndex`, `TestAppendFilterRowFromInclude`, `TestMinSubIndexRows`, `TestReserveHeldRows` |
| I14 | A search vector a CAGRA or IVF-PQ index cannot store is rejected. | `TestCheckQuery`, `TestNewSearchRejectsUnstorableQuery` |
