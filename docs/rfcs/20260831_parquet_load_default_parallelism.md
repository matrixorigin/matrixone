- Status: drafted
- Start Date: 2026-08-31
- Authors: iamlinjunhong
- Implementation PR: [#27903](https://github.com/matrixorigin/matrixone/pull/27903)
- Issue for this RFC: [#27899](https://github.com/matrixorigin/matrixone/issues/27899)

# Default parallel Parquet `LOAD DATA`

## Summary

For Parquet `LOAD DATA`, an omitted `PARALLEL` clause selects bounded parallel
execution when the resolved input is at least `LoadParallelMinSize` (128 MiB).
`PARALLEL 'false'` remains a serial opt-out.  The planner first uses whole-file
fanout when the number of files already fills the bounded execution DOP; it
opens Parquet footers only when row-group fanout can add useful concurrency.

## Problem and invariant

The previous default made a large valid Parquet load serial unless a caller knew
to add `PARALLEL 'true'`.  On high-latency object storage, the resulting
row-group and column-chunk range reads leave available load scopes idle.

For an admitted load, either all rows become visible at the normal transaction
commit point or none do.  A cancellation, read/decode error, or downstream
write error terminates every scope through the existing LOAD pipeline and the
transaction rolls back; independent file or row-group scopes never commit
individually.  Explicit `PARALLEL 'false'` continues to choose the serial
pipeline.

## Design

The parser records whether `PARALLEL` was specified, preserving the distinction
between omitted and explicitly false.  LOAD binding defaults only omitted
Parquet clauses to parallel, then applies the existing 128 MiB admission guard.

The compiler has two bounded fanout choices:

1. If matched file count is at least the bounded load DOP, scopes receive
   whole files.  This path performs no Parquet footer reads during planning.
2. Otherwise the compiler reads metadata and creates contiguous row-group
   shards only if row groups provide more fanout than files.  A one-file or
   under-filled file set therefore still receives row-group parallelism.

Every scope has `Mcpu=1`; S3 DOP is the sum of stage-node CPUs capped by
`external.S3ParallelMaxnum` per node, and local DOP is `ncpu`.  The execution
is therefore bounded by the existing scheduler capacity rather than by file or
row-group count.

For S3 row-group shards, one ReaderAt retains at most 1 MiB and fetches no more
than four times a small request (requests above 256 KiB bypass it).  The cache
is owned by one active external operator and is released with that operator;
there is no query-global cache, background worker, or cross-query state.

## Failure, cancellation, and compatibility

This changes no persisted, catalog, wire, or mixed-version format.  It uses the
existing distributed LOAD scope and transaction ownership: planning failure
creates no scope, and any running-scope failure/cancellation flows through the
existing statement cancellation and transaction rollback path.  `PARALLEL
'false'` is the immediate operational rollback switch; the size guard keeps
small/local loads serial.

## Alternatives

1. Enumerate every footer before choosing file fanout. Rejected: for many
   one-row-group objects it adds file-count times object-store RTT without
   increasing useful DOP.
2. Concurrent footer enumeration. Rejected for this change: it still creates
   unnecessary object-store requests and adds a planning worker/lifecycle.
3. Always use file fanout. Rejected: a large single file needs row-group shards
   to use available capacity.

## Verification and evidence

Focused planner tests cover omitted/default state, explicit serial opt-out,
row-group scope construction, contiguous shard assignment, and the
many-small-file admission decision.  The new DOP test proves that a
threshold-admitted omitted default with four one-row-group files takes the
whole-file path before any footer enumeration.  The existing distributed LOAD
transaction path remains the atomic-failure and cancellation owner; before this
RFC advances, its exact default-parallel failure/cancellation acceptance case
must assert zero visible rows after an injected shard failure.

`BenchmarkParquetRangeReadAheadSequential` reports range calls per operation,
fetched bytes per operation, peak cache bytes, and simulated range latency for
direct and bounded read-ahead modes.  It is a deterministic local benchmark,
not a claim about a particular object-store endpoint.  Endpoint wall time and
resource measurements are required as rollout evidence before enabling this in
a release benchmark environment.

## Open questions

No implementation-blocking question remains.  Independent design approval and
endpoint benchmark evidence are required before this RFC can move to
`in-progress`.
