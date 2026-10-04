# Cost-based hash partitioning for ordinary window functions

- Status: draft; automatic selection disabled pending independent review and acceptance evidence
- Tracking issue: [matrixorigin/matrixone#27943](https://github.com/matrixorigin/matrixone/issues/27943)
- Owner: iamlinjunhong
- Base commit: `c46d897e9645b80178568ef0783dd8e99e527222`
- Implementation PR: [matrixorigin/matrixone#27972](https://github.com/matrixorigin/matrixone/pull/27972)
- Design revision: `window-hash-partition-2026-09-02-r4`
- Last updated: 2026-09-02

## 1. Decision

This draft defines a cost-selected `HASH` implementation for the existing `Node_PARTITION`
operator used by ordinary SQL window functions. `SORT` remains the zero-value,
wire-compatible default and the mandatory fallback. `HASH` is not automatically
selected in this revision: the optimizer fail-closes to `SORT` until an
independent design decision and the complete real-Window performance/resource
acceptance matrix are recorded. Once enabled, the planner makes one explicit
choice; compile and execution follow that choice without independently
replanning it.

The first revision implements coordinator hash partitioning. All upstream CN
streams are merged before the selected partition operator, exactly as they are
for the existing ordinary partition path. The operator buffers the input, hashes
the `PARTITION BY` expressions, stably groups row indexes by hash group, and emits
one complete equality partition per batch to `Window`. A subsequent revision may
add distributed hash shuffle, but it is not required for correctness or for the
first performance win.

This design concerns ordinary analytic windows such as
`sum(v) over (partition by k)`. MatrixOne's timestamp-based TIME WINDOW operators
already use grouping machinery and are outside scope.

## 2. Current path and problem

The binder creates a `Node_PARTITION` below each `Node_WINDOW` that has
`PARTITION BY`. Compile currently sorts every input scope on the partition keys,
merges the streams, and runs the partition operator to identify equal adjacent
keys. Even when the window has no ordering requirement, this performs
`O(N log N)` key comparisons and moves all rows through sort.

Hash partitioning can replace only that partition-key sort:

```text
current:  input scopes -> local Sort(partition keys) -> merge -> Partition -> Window
new:      input scopes -> merge -> HashPartition(partition keys) -> Window
```

If the window also has `ORDER BY`, `Window` still sorts rows inside each complete
partition. Hash partitioning does not remove or change that ordering step.

## 3. First-principles invariants

For input multiset `R` and SQL partition-key equality `=`:

1. every input row is assigned to exactly one partition;
2. two rows are assigned to the same partition iff all partition-key components
   compare equal under the established window partition semantics;
3. every partition is complete and is emitted to exactly one `Window` evaluator;
4. all `N` input rows are emitted exactly once; the optimization never has GROUP
   BY cardinality `G`;
5. row order within a hash partition is stable relative to upstream arrival, so
   unordered windows do not acquire avoidable nondeterminism;
6. no output is emitted before end-of-input because an unseen row can belong to
   any existing partition;
7. error, cancellation, reset, and free paths release every retained batch,
   evaluated key vector, hash table, index buffer, and borrowed output view once.

These invariants apply equally when the merged inputs originated on multiple CNs.
Coordinator execution makes partition ownership explicit: the single
`HashPartition` owns every key.

## 4. Plan and wire contract

Add a `PartitionAlgorithm` enum with values `SORT = 0` and `HASH = 1`, plus a
field on plan `Node`, pipeline `Instruction`, and execution `Partition`.
`SORT = 0` is intentional:

- old serialized plans read by new CNs retain the established sort behavior;
- new sort plans read by old CNs are unchanged;
- compile gates HASH on the cluster protocol version. During a rolling upgrade,
  it restores both the prerequisite local sorts and coordinator SORT algorithm;
  HASH is never sent to a CN that can ignore the new field.
- this revision assigns the capability to `MORPCVersion47`; v46 is the immediate
  predecessor and therefore retains the legacy SORT topology.

The field is copied by plan deep-copy, operator duplication, and remote pipeline
serialization. Text and JSON EXPLAIN identify `algorithm: hash` only when HASH is
selected; existing sort output remains stable. Partition Top-N continues to use
its existing `Limit != nil` path and ignores this ordinary-window choice.

## 5. Eligibility and equality boundary

HASH is considered only when all conditions hold:

- the node is an ordinary `Node_PARTITION` with no partition Top-N limit;
- at least one partition expression exists;
- planner row-count and NDV estimates are finite and positive;
- every expression type is supported by the existing group hash table and has
  equality identical to the current partition comparator.

The first allowlist accepts boolean, signed/unsigned integer, decimal, date/time,
timestamp, UUID, enum, binary, varbinary, varchar, text, blob, and supported
fixed-width opaque identifiers after type-specific tests prove equality. FLOAT,
DOUBLE, JSON, arrays, tuples, and other composite or newly introduced types fail
closed to SORT. CHAR is also rejected in this revision because its padding
semantics have not yet been proven equivalent to the group hash table. In
particular, signed zero, NaN, JSON normalization, collation, padding, and NULL
equality must not silently change partition membership. Partition Top-N retains
its broader, independent eligibility predicate because it has a different
physical contract; this ordinary-window HASH predicate is intentionally
conservative.

NULL partition keys remain equal to NULL as in the established window behavior;
the hash table is configured for nullable keys and tests cover single and
composite nullable keys.

## 6. Cost and memory decision

Let:

- `N` be child output rows;
- `K` be the number of partition expressions;
- `G` be the estimated composite NDV, computed as the capped product of
  per-expression NDVs: `1 <= G <= N`;
- `W` be the summed fixed key width, with a conservative variable-width charge;
- `B` be the coordinator hash-memory budget.

The planner compares deterministic relative work:

```text
sort_work = N * log2(max(N, 2)) * K
hash_work = N * K + N + 32*G*K
hash_aux  = 16*N + G*(W + hash-entry-overhead)
```

`32*G*K` is a conservative per-partition downstream/equality-state cost. It
scales with the composite key width so near-unique multi-key inputs do not look
artificially cheap despite emitting `N` separate batches. `16*N` accounts for
the group-id and stable-selection arrays.
Retained input batches are common to both blocking paths and are not used to make
HASH look artificially worse. Overflow, missing statistics, invalid NDV, or an
unbounded width estimate rejects HASH.

`B` uses the configured aggregate spill threshold when one is present; otherwise
it uses MatrixOne's existing per-worker default memory threshold. HASH is selected
only when `N` is at least one vector batch, `hash_work < sort_work`, and
`hash_aux <= B`. This deliberately favors SORT for small inputs and for high-NDV,
wide keys whose auxiliary hash state approaches the memory limit. Constants and
the crossover are covered by table-driven planner tests and calibrated by the
operator benchmark matrix before merge.

Already ordered physical-property propagation is not currently available at
this boundary. This revision therefore does not claim to recognize it. Once such
a property exists, zero incremental partition-sort cost must force SORT. Until
then, no-order and ordered-window controls in the benchmark gate detect
regressions from choosing HASH.

## 7. Execution design

`Partition.Prepare` chooses one of three mutually exclusive containers:

1. partition Top-N when `Limit != nil`;
2. hash partition when `Algorithm == HASH`;
3. the existing sort/merge partition path otherwise.

The hash container owns expression executors, a `group.ResHashRelated` hash
table, retained copied input batches, one group id per row, prefix counts, stable
selection indexes, and at most one borrowed output window.

### 7.1 State machine

```text
receive -> finalize -> emit -> end
   |          |         |
   +----------+---------+-> error/cancel -> caller cleanup
```

- `receive`: call the child, copy each non-empty batch, evaluate partition keys,
  insert them into the group hash table in `hashmap.UnitLimit` chunks, and append
  returned group ids. Poll cancellation between chunks.
- `finalize`: after EOF, count group sizes, build prefix offsets, fill a stable
  selection array in arrival order, and materialize one replacement batch into
  group-contiguous order. The materialization copies bounded selection units and
  polls cancellation between units; it publishes the replacement only after the
  full copy succeeds, so cancellation cannot expose a half-reordered batch.
- `emit`: return a borrowed `Batch.Window(start, end)` for exactly one complete
  group per call. Release the previous borrowed view before returning the next.
- `end`: return `ExecStop`; repeated calls do not produce rows.

Empty input transitions directly to `end`. A batch or expression error is
returned immediately. `Reset` clears data and hash contents but retains reusable
executors; `Free` releases executors and all owned memory. Tests exercise reuse
after success, cancellation during receive/finalize, empty batches, and exact
fallback.

### 7.2 Resource bound and fallback

Bulk hash allocations, including group ids, prefix offsets, stable-selection
indexes, and the overlapping final materialization batch, are charged through
the process mpool and reported through the operator analyzer as one peak working
set; retained batch growth is also reported there. The planner's estimate is admission
control, not a hard runtime proof: NDV and variable-width statistics can be
wrong. During receive, the operator checks actual hash size plus current and
required row-index capacity against its `SpillMem` threshold. If the threshold
is crossed before any output, it destroys and immediately frees hash-only state,
then finishes through an exact stable sort of the retained input. Direct-column
partition keys are re-evaluated over that retained batch. This is a one-way
fallback; it never mixes hash and sort output. The triggering input batch is the
only permitted admission overshoot: after fallback starts, the operator rejects
with the established OOM error before copying any later batch that would exceed
the same byte or row threshold. Consequently stale estimates cannot turn a
pre-output fallback into an unbounded coordinator buffer.

A zero configured threshold resolves through the same default-threshold helper
used by hash aggregation. If fallback cannot allocate its sort state, the
established mpool error is returned; it must not retry or leak the retained input.

## 8. Compile topology

For SORT, compile remains unchanged: construct local Order operators, merge
scopes, then construct Partition.

For HASH, compile omits all local partition-key Order operators, merges the same
scopes, and constructs one coordinator Partition with `Algorithm == HASH`.
Window remains downstream and receives one complete partition batch per call.
Tests inspect typed operator trees rather than relying only on EXPLAIN strings.

No distributed receiver protocol, hash-shuffle message, remote spill ownership,
or cross-CN replay state is added in this revision. A future ShuffleHashPartition
must retain the same invariants and add a topology proof that each hash bucket has
one owner before it can become a third optimizer candidate.

## 9. Alternatives considered

- **Always sort:** lowest implementation risk, but preserves the avoidable
  `N log N` partition-key work that motivated the issue.
- **Always hash:** rejects useful small-input behavior and can amplify memory for
  high-NDV or wide keys.
- **Rewrite Window as Group plus join-back:** Group returns `G` rows while Window
  must return `N`; reconstructing rows and frames adds more work and changes
  ordering/error ownership.
- **Implement distributed HashShuffle first:** potentially removes the
  coordinator bottleneck, but expands the first change into exchange routing,
  skew, spill, retry, and mixed-version protocols. It is separable from the local
  algorithm and is deferred.
- **Runtime race between sort and hash:** duplicates blocking work and memory and
  complicates cancellation. Planner choice plus one-way pre-output fallback is
  simpler and deterministic.

## 10. Validation plan

### Planner and serialization

- HASH selected for large, narrow, well-estimated eligible inputs;
- SORT selected for small, unknown, wide, memory-unsafe, or incompatible inputs;
- composite NDV is capped and arithmetic overflow fails closed;
- partition Top-N remains on its existing path;
- plan deep-copy and pipeline round-trip preserve HASH;
- absent/zero enum decodes as SORT;
- text and JSON EXPLAIN expose the selected algorithm.

### Operator correctness and lifecycle

- single/composite keys, NULLs, varlen values, multiple input batches;
- aggregate, ranking, value, ROWS and RANGE window consumers, with and without
  window `ORDER BY`;
- every row appears once and each key appears in one output batch;
- stable row order within a partition;
- empty input and empty intermediate batches;
- cancellation and injected expression/allocation failures;
- Reset/reuse and Free leave the test mpool at zero;
- actual-memory threshold triggers exact sort fallback before output, and a
  multi-batch wide-row regression proves that post-trigger input is rejected
  rather than retained without bound.

### Compile and distributed shape

- SORT plan contains local Order operators and coordinator Partition;
- HASH plan contains no partition-key Order and exactly one coordinator
  HashPartition after Merge;
- mocked multi-scope input proves one logical key is never split across Window
  calls.
- a public SQL regression populates analyzed low-NDV input above one vector
  batch, asserts `Hash Partition` in EXPLAIN, and checks aggregate, ranking,
  value, ROWS/RANGE, ordered, and unordered window result oracles against
  nearby SORT controls.

### Performance evidence

Benchmarks cover `N` at 1K, 64K, and 1M; NDV at 1, 1%, and 100% of `N`; one and
three keys; fixed and variable-width keys; and windows with/without `ORDER BY`.
Report wall time, allocations, bytes, and crossover. Merge requires a material
win for the activating large-input cases and no material regression for small,
ordered, or unsupported controls chosen as SORT.

Initial single-scope operator calibration on Apple M4 (`-benchtime=3x`, 65,536
INT32-key rows) produced:

| NDV | Sort | Hash | Hash effect |
| ---: | ---: | ---: | ---: |
| 64 | 9.39 ms | 1.15 ms | 8.2x faster |
| 1,024 | 10.61 ms | 1.29 ms | 8.2x faster |
| 16,384 | 13.59 ms | 5.36 ms | 2.5x faster |
| 65,536 | 17.38 ms | 16.96 ms | only 2.5% faster; rejected by cost model |

The near-unique result motivated the explicit per-group work term rather than an
`N log N` versus `N` comparison alone.

The historical r2 operator matrix used `-benchtime=1x -benchmem` across 1K,
64K, and 1M rows; NDV 1, 1%, and 100%; one/three fixed or varlen keys. It
informed the cost-model shape but is not r3 end-to-end acceptance evidence. On
the exact r3 head, a repeated local 1M, three-fixed-key HASH check (`-count=3
-benchtime=3x`) measured 137--153 ms/op and 48.1 MB `peak-mpool-B` at 1%-NDV,
versus 1.216--1.222 s/op and 139.1 MB `peak-mpool-B` at 100%-NDV. The latter
counterexample remains on SORT through the key-count-scaled per-group cost.

This is deliberately an operator microbenchmark, not a claim about end-to-end
SQL latency. Automatic HASH selection remains disabled until the performance
gate records repeated selected-HASH-versus-SORT measurements through a real
Window consumer for the ordered/unordered, activating/rejected, fallback, and
multi-scope cases, including peak memory. The public SQL BVT now asserts the
fail-closed SORT default while preserving aggregate, ranking, value, ROWS,
RANGE, ordered, and unordered Window result oracles. Re-enablement must add an
exact selected-HASH public-path oracle in addition to those semantic controls.

## 11. Rollout and observability

The plan enum makes rollout reversible: forcing the optimizer eligibility
predicate false restores the complete old path without changing SQL or wire
contracts. EXPLAIN records the selected algorithm. Operator analyzer statistics
account retained batches, hash-table growth, group-id capacity, output-boundary
indexes, and fallback selection scratch; benchmark evidence records peak bytes
and fallback activation. No user-visible session switch is introduced in the
first revision.

## 12. Design verification and review status

- Proposed semantic invariants: complete; HASH changes physical grouping only and preserves
  complete `N`-row window partitions.
- Proposed failure and lifecycle closure: complete with required pre-output one-way sort
  fallback, cancellable final materialization, and explicit Reset/Free tests.
- Proposed distributed topology: complete for the coordinator implementation; all streams
  merge before one partition owner.
- Proposed compatibility: complete; zero-value SORT plus the protocol-version compile gate is
  safe across persisted and mixed-version protobuf readers.
- Proposed cost/resource implementation: draft; benchmark calibration, mpool-owned
  index buffers, and a bounded post-trigger fallback contract are implemented,
  while real-Window acceptance measurements remain required before automatic
  selection can be enabled.
- Independent review decision: pending. This document is versioned with the
  implementation PR above; an independent reviewer must record approval of its
  exact revision, and the acceptance evidence above must be recorded, before
  automatic HASH selection can be enabled.
