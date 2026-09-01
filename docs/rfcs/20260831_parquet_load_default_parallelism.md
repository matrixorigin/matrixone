- Status: draft — independent design approval pending
- Revision: 3
- Start Date: 2026-08-31
- Authors: iamlinjunhong
- Implementation PR: [#27903](https://github.com/matrixorigin/matrixone/pull/27903)
- Issue for this RFC: [#27899](https://github.com/matrixorigin/matrixone/issues/27899)

# Default parallel Parquet `LOAD DATA`

## Summary

For Parquet `LOAD DATA`, an omitted `PARALLEL` clause remains serial by default.
A session can opt into the bounded experimental rollout with
`experimental_parquet_load_parallel = 1`; its default admission threshold is
`LoadParallelMinSize` (128 MiB). `PARALLEL 'false'` remains a serial opt-out.
The planner first uses whole-file fanout when the number of files already fills
the bounded execution DOP; it opens Parquet footers only when row-group fanout
can add useful concurrency.

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
between omitted and explicitly false. LOAD binding enables only omitted Parquet
clauses after the session opts into `experimental_parquet_load_parallel`; the
default remains disabled until independent design approval and endpoint evidence
are available. `experimental_parquet_load_parallel_min_size` is session-only,
defaults to 128 MiB, and permits a bounded canary/test threshold between 1 byte
and 128 MiB. The resolved threshold is captured in the plan before admission.

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

### State and ownership

```text
parse -> bind default -> size admission -> compile fanout -> LOAD scopes
                  |             |                |
                  |             |                +-- file count fills DOP: no footer reads
                  |             +-- below 128 MiB: serial path
                  +-- explicit false: serial path
```

The binder owns the omitted-versus-explicit distinction and writes the admitted
parameter into the plan. The compiler owns choosing the bounded fanout shape;
it does not re-admit based on the per-file listing. Every resulting scope is
owned by the existing statement transaction. A scope can produce batches, but
cannot independently commit. The transaction owner alone publishes rows at
commit; it rolls back on any scope error or statement cancellation.

| Resource | Effective owner | Bound | Terminal path |
|---|---|---:|---|
| Load scopes | existing scheduler | bounded DOP | normal scope completion, statement error, or context cancellation |
| Footer planning | compiler | at most one sequential open per file, only below file-fanout saturation | compile return |
| ReaderAt cache | one active Parquet external operator | 1 MiB retained; at most 4x a small request; requests above 256 KiB bypass | operator close / scope cleanup |
| Transaction visibility | existing LOAD transaction | one statement transaction | commit publishes all rows; any error/cancel rolls back all rows |

There is no added goroutine, retry loop, query-global cache, or persistent
state. Cancellation follows the existing statement context into every scope;
the new code has no separate wait or cleanup protocol.

## Failure, cancellation, and compatibility

This changes no persisted, catalog, wire, or mixed-version format.  It uses the
existing distributed LOAD scope and transaction ownership: planning failure
creates no scope, and any running-scope failure/cancellation flows through the
existing statement cancellation and transaction rollback path.  `PARALLEL
'false'` is the immediate operational rollback switch; the size guard keeps
small/local loads serial.

This change has no catalog, storage, wire, or mixed-version surface. It is safe
to roll back by reverting the code or by using explicit `PARALLEL 'false'` for
an affected statement. It adds debug-only footer planning counters (file count,
row groups, calls, bytes, duration) and does not add a metric with unbounded
labels.

## Alternatives

1. Enumerate every footer before choosing file fanout. Rejected: for many
   one-row-group objects it adds file-count times object-store RTT without
   increasing useful DOP.
2. Concurrent footer enumeration. Rejected for this change: it still creates
   unnecessary object-store requests and adds a planning worker/lifecycle.
3. Always use file fanout. Rejected: a large single file needs row-group shards
   to use available capacity.

## Verification and evidence

| Invariant | Deterministic witness | Oracle |
|---|---|---|
| Disabled-by-default gate leaves omitted clauses serial; enabled session gate admits at its captured threshold; explicit false remains serial | `TestDefaultParquetLoadParallel`, `TestDefaultParquetLoadParallelExperimentalRollout`, `TestDefaultParquetLoadParallelAdmission` | bound parameter flags |
| A threshold-admitted omitted default reaches whole-file fanout before metadata I/O when files fill DOP | `TestCompileExternScanParquetLoadDefaultAtThresholdUsesFileFanoutWithoutFooterReads` | deliberately invalid `.parquet` files compile into one-file scopes, proving no footer open occurred |
| Row-group selection preserves rows and nullable values | `TestParquet_RowGroupSelection_SerialVsShards_Nulls` | serial/sharded result equality |
| A selected shard reports a NOT NULL failure | `TestParquet_RowGroupSelection_NotNullViolation` | constraint error class |
| Threshold-admitted omitted default cancels sibling file scopes on a shard failure and keeps the seed row only | distributed `load_data_parquet` rollback case with the session gate and test threshold | post-failure row count and aggregates |
| Client cancellation after every admitted file shard has begun terminates every shard | `TestCompileExternScanParquetLoadDefaultFanoutContextCancellationTerminatesAllShards` | synchronization barrier proves both scopes are in flight before cancellation; every scope process observes `context.Canceled` |

The threshold fanout test uses the post-bind parameter as the test seam, so it
does not need a 128 MiB fixture or a timing assertion. The distributed rollback
case uses the session-only experimental gate and a one-byte test threshold to
exercise the omitted-clause file-fanout transaction path with the existing tiny
multi-file fixture; a schema failure asserts that no partial row is visible.
The cancellation witness uses the same admitted whole-file scope construction,
then blocks each test shard until the client/query context is canceled after
both have started. It proves the cancellation graph terminates every admitted
scope; the distributed rollback case remains the SQL-visible zero-partial-row
oracle for the statement transaction.
The release-default acceptance proof still requires endpoint rollout
measurements before this RFC can move from draft to in-progress.

`BenchmarkParquetRangeReadAheadSequential` reports range calls per operation,
fetched bytes per operation, peak cache bytes, and simulated range latency for
direct and bounded read-ahead modes.  It is a deterministic local benchmark,
not a claim about a particular object-store endpoint.  Endpoint wall time and
resource measurements are required as rollout evidence before enabling this in
a release benchmark environment.

The terminal local benchmark record is in
[`docs/design/evidence/27903_parquet_range_read_ahead_benchmark.md`](../design/evidence/27903_parquet_range_read_ahead_benchmark.md): direct/read-ahead
range calls are 128/32, fetched bytes are both 8 MiB, and retained cache is
0/256 KiB. These values are local synthetic evidence only; the linked issue's
object-store observations are problem evidence, not before/after rollout proof.

## Open questions

No implementation decision is deferred. Independent design approval and
endpoint benchmark evidence are required before the disabled rollout gate can
be enabled by default.
