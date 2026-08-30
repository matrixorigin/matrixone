# Grouped-aggregate finalization into bounded TopK

- Status: implemented (design review passed; validation evidence below)
- Tracking issue: [matrixorigin/matrixone#27730](https://github.com/matrixorigin/matrixone/issues/27730)
- Parent performance issue: [matrixorigin/matrixone#27685](https://github.com/matrixorigin/matrixone/issues/27685)
- Design and implementation PR: [matrixorigin/matrixone#27850](https://github.com/matrixorigin/matrixone/pull/27850)
- Last updated: 2026-08-30

## 1. Decision summary

For an eligible physical `Group` or `MergeGroup -> Top` edge, the existing
`Top` operator becomes the aggregate owner's finalization consumer. The
aggregate finalizes one existing aggregate chunk at a time, applies its existing
projection exactly once, and synchronously admits that batch into the existing
`Top` implementation. `Top` copies only retained winners into its own allocation
domain. The aggregate then releases the complete finalized input chunk before
finalizing the next one.

The physical operator tree remains visible as `Group -> Top -> MergeTop` and the
wire representation does not change. A prepare-time handshake on the direct
operator edge activates fusion independently in each execution owner. If any
eligibility proof is absent, the handshake fails without changing the tree and
the current pull-based path executes.

This design deliberately does not add a second heap, comparator, ordering
implementation, queue, goroutine, scheduler, or aggregate-specific TopK rule.
It adds one bounded synchronous ownership transfer at the existing operator
boundary and an optional chunk-finalization capability to aggregate executors.

## 2. Problem and evidence

The current final group owner retains group keys and aggregate states while it
builds the grouping hash table. On the first final-output call,
`getNextFinalResult` calls `Flush` on every aggregate executor. `Flush` returns
the result vector for every aggregate-state chunk, and the group operator
attaches all those result vectors to all `groupByBatches` before it returns the
first batch. A downstream bounded `Top` eventually retains only K rows, but the
full aggregate result has already been materialized.

The controlled #27685 reproduction had:

- 10,000,000 input rows;
- 10,000,000 finalized groups;
- approximately 685.59 MiB of aggregate output;
- `LIMIT 10`;
- a bounded Top state measured in KiB, after consuming all 10,000,000 rows.

The observed cost is not caused by an inexact heap. It is caused by the
materialization boundary between exact aggregate finalization and an already
bounded exact consumer.

### 2.1 Violated invariant and its negation

Required invariant:

> After every logical group has reached its complete final aggregate state,
> each eligible group is considered exactly once by the same exact bounded TopK
> semantics as the unfused plan, while non-winning finalized rows do not remain
> live beyond one bounded finalization chunk.

Negation:

- comparing or evicting a group before all its input and partial states merge;
- evaluating HAVING, projection, or ordering in a different order or count;
- admitting one complete group zero or multiple times;
- retaining all finalized aggregate vectors while K is bounded;
- changing the existing Top comparator, NULL/collation behavior, tie behavior,
  offset arithmetic, spill behavior, or MergeTop contract;
- activating when ownership, expression, aggregate, or lifecycle support is
  uncertain.

## 3. Scope, goals, and non-goals

### 3.1 Goals

1. Bound additional finalized-output memory to the existing Top retention for
   `K + OFFSET` plus one aggregate chunk and expression scratch.
2. Preserve byte-for-byte result values and SQL-equivalent row selection relative
   to the existing `Aggregate -> projection -> Top -> MergeTop` path.
3. Emit at most `K + OFFSET` rows from each final aggregate owner across the
   operator boundary into `MergeTop`.
4. Reuse existing Top comparison, spill, allocation accounting, and cleanup.
5. Make activation a strict capability proof with a zero-semantic-change
   fallback.
6. Close success, error, cancellation, reset, free, prepared reuse, local
   parallel cloning, and remote reconstruction.

### 3.2 Non-goals

- reducing the memory of the grouping hash table, retained group keys, or
  aggregate states before they are final;
- early termination of aggregate input;
- TopK pushdown into partial aggregation or hash-table admission;
- supporting DISTINCT, ordered aggregates, grouping sets, or HAVING in the
  first implementation;
- changing logical optimizer rules, SQL syntax, protobuf, MORPC version, or
  explain-plan topology;
- parallel spill-partition finalization, which remains #27729;
- reusing grouping hashes across operators, which remains #27753;
- creating a performance unit test with large input or elapsed-time assertions.

## 4. Current topology and first owners

### 4.1 No-shuffle aggregation

```text
input scopes
  -> local Group(NeedEval=false) emits partial state
  -> MergeGroup owns complete groups
  -> local Top owns bounded winners
  -> caller
```

`MergeGroup` is the first owner that can prove every group is final. Local Group
must not compare its partial states.

### 4.2 Hash-shuffle aggregation

```text
input scopes
  -> hash shuffle by complete grouping identity
  -> one Group(NeedEval=true) per owner
  -> one local Top per owner
  -> MergeTop owns the global bounded merge
```

The shuffled `Group` is a final owner only after the existing shuffle contract
has assigned every equal complete grouping key to exactly one owner. Fusion does
not alter that ownership mapping.

### 4.3 Single-scope aggregation

```text
input -> Group(NeedEval=true) -> Top -> caller
```

The single `Group` is the complete-group owner. No `MergeTop` is necessary.

## 5. Alternatives considered

### 5.1 Keep separate operators and only make aggregate Flush chunked

This removes the all-chunks-at-once peak but still publishes every group through
the operator boundary and makes the standalone Top consume every output row. It
is a useful prerequisite but does not satisfy the bounded local-owner emission
contract.

### 5.2 Evict groups from the aggregation hash table using partial values

Rejected. SUM, COUNT, MIN/MAX, and especially general aggregates can change
their ordering as more rows or remote partials arrive. An early loser can become
a final winner. Correctness would require aggregate-specific monotonicity proofs
and different behavior for each ordering expression, which is not the general
contract.

### 5.3 Add a GroupTop operator with a new heap/comparator

Rejected. It duplicates NULL ordering, collation, multi-key comparison, variable
payload copying, offset handling, spill, allocation accounting, and cleanup
already implemented by Top. The duplicate would drift and enlarge both
correctness and performance risk.

### 5.4 Serialize finalized groups into an intermediate queue consumed by Top

Rejected. A queue adds ownership, backpressure, cancellation, and capacity state
without exposing concurrency: finalization and heap admission are both local CPU
work in the same pipeline. Synchronous consumption has the smaller wait and
resource graph.

### 5.5 Selected: existing Top as synchronous finalization consumer

This preserves one ordering authority and one retained-winner owner. The only
new aggregate capability is transferring one already-final chunk instead of all
chunks. It produces the required bound with no asynchronous machinery.

## 6. Eligibility proof

Activation requires all rows below. A failed row is a normal fallback, not an
execution error.

| Proof | First implementation rule |
| --- | --- |
| Direct ownership edge | Top has exactly one direct child implementing the final-group-source handshake. No Filter, Projection operator, exchange, connector, or fan-out lies between them. |
| Complete groups | Child is `Group` with `NeedEval=true` or `MergeGroup`. Partial `Group(NeedEval=false)` is rejected. |
| Grouped shape | At least one active grouping key exists. Scalar H0 aggregation is rejected because it has at most one row and no materialization problem. |
| Grouping sets | Any inactive `GroupingFlag` rejects fusion. |
| Aggregate capability | Every aggregate is non-DISTINCT and its concrete executor advertises exact chunk finalization. Initial coverage is COUNT, SUM, MIN, and MAX for their supported input types. A mixed aggregate list is all-or-nothing. A zero-aggregate grouped DISTINCT shape is eligible because it has no aggregate result vector to finalize. |
| Ordered/configured state | Ordered aggregates and aggregate configurations without the chunk contract reject fusion. |
| HAVING/filter | A physical Filter between final group and Top prevents the direct handshake. No filter is crossed. |
| Projection | An embedded Group projection is allowed only when every expression is non-volatile and non-real-time. It is evaluated exactly once per finalized chunk before admission. |
| Ordering expressions | Every Top ordering expression is non-volatile and non-real-time and consumes only the projected finalized batch, as already required by the physical Top. |
| Downstream consumers | The physical child edge and operator tree establish one consumer. Shared materialized/CTE/fan-out paths have another operator boundary and cannot handshake. |
| Limit | Runtime Top limit is non-zero and no greater than the existing in-memory bounded-Top threshold (16,384). Larger limits keep the existing Top/spill path. |
| Runtime benefit | If finalization has no pending spilled partition and the complete resident result fits in one aggregate chunk, the attached source uses the ordinary pull path. It is already O(B), so fusion would add a callback without reducing peak output memory. |
| Offset | Existing compilation may convert constant safe `LIMIT + OFFSET` to TopN only after checked addition and only within 16,384. Fusion sees that already-proved TopN. Other offset shapes retain order/offset/limit. |
| SQL_CALC_FOUND_ROWS | Existing compilation does not create Top for this path, so it is unreachable. |
| Expression/order semantics | Existing expression executors and Top comparator are reused; there is no simplified fusion evaluator. |

Function expressions must resolve to a concrete built-in overload ID. Missing,
unknown, or out-of-range overload metadata rejects fusion; it is never treated
as stable merely because an unknown function name is absent from the volatile
registry.

The aggregate-executor capability must be queried from the constructed executor,
not inferred from a function name alone. This prevents a newly added type-
specific implementation from accidentally inheriting unsupported activation.
`AggFuncExecExpression` provides only an early rejection for
DISTINCT/configured/unsupported families.

`Group` constructs its aggregate executors during Prepare and can complete the
concrete check before attachment. `MergeGroup` cannot: its concrete executors
depend on metadata decoded from the first partial batch. Its initial handshake
is therefore provisional. Immediately before the first finalization chunk, it
checks every constructed executor. If any capability is absent, and before any
chunk or Top candidate has been consumed, it disables the provisional path and
returns batches through the unchanged pull path. Late fallback after one fused
admission is forbidden. This two-stage rule makes capability drift a safe
fallback instead of an execution error.

The same before-first-admission rule applies to the runtime benefit gate. A
single resident chunk with no spill state falls back. More than one resident
chunk or any pending spill partition activates fusion; after the first
admission, every later spill partition remains on the same fused consumer.

## 7. Interfaces and data flow

### 7.1 Optional aggregate chunk finalization

Do not expand the mandatory `AggFuncExec` interface. Add a narrow optional
interface implemented only by proven executors:

```go
type ChunkFinalizer interface {
    FinalizeChunk(ctx context.Context, chunk int) (*vector.Vector, error)
}
```

Contract:

1. `chunk` identifies the same aggregate-state chunk as the matching
   `groupByBatches[chunk]`.
2. A successful call transfers one result vector to the caller and clears only
   that chunk's aggregate ownership.
3. Each chunk is finalized at most once and only after its owner has received
   all input/partial state.
4. Failure returns no caller-owned live vector. If construction transferred
   storage before failing, the implementation frees it or returns it under an
   explicit caller-owned error contract; the selected implementation uses the
   former to keep ownership singular.
5. Cancellation is checked before expensive per-row finalization and within
   implementations whose finalization loops can be material.
6. Ordinary `Flush` remains unchanged and is the fallback path.
7. `Free` accepts a prefix of finalized chunks and frees every remaining state;
   it never frees successfully transferred vectors.

COUNT, non-DISTINCT SUM, MIN, and MAX already keep state in aggregate-sized
chunks. Their implementations should factor one-chunk logic out of `Flush`, so
`Flush` becomes a loop over the same helper. This gives fused and unfused paths
one result algorithm rather than two.

### 7.2 Source-consumer handshake

Add a small interface at the common execution boundary:

```go
type FinalizedBatchConsumer interface {
    ConsumeFinalizedBatch(*process.Process, *batch.Batch) error
}

type FinalizedBatchConsumerToken uint64

type FinalizedBatchSource interface {
    TryAttachFinalizedBatchConsumer(FinalizedBatchConsumer) (FinalizedBatchConsumerToken, bool)
    DetachFinalizedBatchConsumer(FinalizedBatchConsumerToken)
}
```

The concrete token is generation-specific and comparable. A detach for a stale
token is a no-op and cannot detach a newer execution generation.

`Top.Prepare` runs after its child has prepared. It first completes every
fallible Top initialization step, evaluates the runtime limit, and checks its
own ordering/limit requirements. Attachment is the last Prepare step: Top asks
the direct child to attach and stores the returned token, after which Prepare
cannot fail. No compile-time pointer is serialized or copied.
This ordering has four benefits:

- prepared executions re-evaluate dynamic limits per generation;
- parallel operator cloning starts with no captured pointer;
- remote reconstruction discovers the same structural edge locally without a
  protobuf or MORPC change;
- mixed capable/incapable owners may choose fused/unfused execution while
  emitting the identical existing schema into MergeTop.

`Top.Reset` and `Top.Free` detach their exact token. Source Reset/Free also
clears any attachment defensively. A second live attachment in the same source
generation is rejected and never silently replaces the existing consumer.

### 7.3 Finalization loop

After the aggregate input is complete and any spill reload has produced a
complete resident partition, an attached source executes:

```text
for each resident aggregate chunk in stable chunk order:
  check cancellation
  finalize each aggregate column for this chunk
  attach finalized columns to the matching group-key batch
  evaluate the existing embedded projection once
  synchronously call Top.ConsumeFinalizedBatch(projected batch)
  release projection scratch for reuse
  release the complete finalized group-key/result batch
  clear its groupByBatches slot
release the aggregate-executor prefix and resident hash state
load the next spilled partition, if any, and repeat
signal EOF to Top through the ordinary child return
```

`Top.ConsumeFinalizedBatch` factors the same shallow input wrapper and `build`
logic used by `Top.Call`. Top ordering executors evaluate the same expressions,
and retained rows are copied into the existing Top-owned batch or existing Top
spill representation. Input vectors are never retained by reference after the
synchronous call returns.

The source returns no data batch to `vm.ChildrenCall` while attached. Top then
enters its existing Eval state and emits its bounded result. Unfused sources
continue returning one batch at a time through `vm.Exec`, including ordinary
projection evaluation.

## 8. Correctness reasoning

### 8.1 Exactly-once group admission

The existing final owner maps group ID ranges to aligned key and aggregate-state
chunks. The finalization cursor advances only after all aggregate columns,
projection, and Top admission for the current chunk succeed. No retry is made
after admission. On error, execution terminates and Reset/Free owns the
remaining state. Therefore a successful execution admits every chunk once and
every row in each chunk once.

Spilled partitions retain their existing disjoint ownership. The Top consumer
lives across sequential partition reloads, so every restored complete group is
admitted to the same owner-local heap exactly once. Fusion neither changes spill
partitioning nor permits two reload owners.

### 8.2 Projection and HAVING order

The first implementation does not cross a Filter. HAVING therefore falls back.
An embedded projection is evaluated on the same finalized batch that the normal
`vm.Exec(Group)` path would project before returning to Top. The expression
executor objects are the existing Group-owned objects and are invoked once. The
only changed fact is that the projected batch is synchronously consumed rather
than returned through one pull frame.

Volatile and real-time expressions are rejected even though the intended call
count is unchanged. This conservative boundary avoids making evaluation order
part of the first rollout proof.

### 8.3 Ordering, NULL, collation, and ties

The existing Top evaluates every `OrderBySpec` and uses its existing compare
objects and flags. Fusion does not inspect aggregate values or implement an
ordering shortcut. ASC/DESC, multiple keys, NULL placement, type comparison,
string collation, binary data, and grouping metadata therefore retain the same
authority.

The current local Top plus MergeTop decomposition may select any legal K rows at
an incomplete tie boundary. Fusion sends the same candidates through the same
decomposition and makes no deterministic-tie claim. Tests compare fully ordered
results only when the SQL order is complete; otherwise they compare allowed
multisets and boundary values.

### 8.4 Offset

The physical compiler already changes eligible constant `LIMIT K OFFSET O` to
local Top `K+O`, checked for uint64 overflow, followed by Offset. The selected
limit cap is the same 16,384 boundary. Fusion retains `K+O` locally; MergeTop
retains the global `K+O`; Offset then discards O. Unsupported/dynamic/overflowing
offset shapes never present an eligible Top edge.

## 9. Ownership, lifecycle, and unhappy paths

### 9.1 Ownership table

| Resource | Creation/initial owner | Transfer | Terminal owner |
| --- | --- | --- | --- |
| grouping key/state chunks | Group or MergeGroup | none before finalization | source frees each consumed chunk or remaining state in Reset/Free |
| finalized aggregate vector | aggregate executor chunk | move to current source output batch | source cleans after synchronous admission; Top never borrows it after return |
| projected expression vector | Group projection executor | borrowed by synchronous Top call | projection executor Reset/Free; never batch-cleaned as independently owned input |
| winning fixed/variable payload | Top retained allocation | copied during admission | Top Reset/Free or output transfer under existing contract |
| displaced Top entry | Top | existing overwrite/heap path | existing Top cleanup/accounting |
| Top spill file/tokens | Top | existing spill lifecycle | existing Top close/release paths |
| attachment | Top Prepare generation | token registered in source | exact Top detach or source Reset/Free |

There is no ownership transfer merely because a vector pointer is passed to the
consumer. The synchronous method's contract is “borrow during call, copy any
retained winner.” This is required for variable-width values and is validated
with post-consumption source cleanup before result evaluation.

### 9.2 State transitions

```text
source Prepare: Detached -> Attachable
Top Prepare eligible handshake: Attachable -> Attached(generation token)
source Build complete: Attached -> Finalizing(chunk=0)
successful consume: Finalizing(i) -> Finalizing(i+1)
last partition/chunk: Finalizing -> Drained
Top child EOF: Top Build -> Top Eval -> Top End
any error/cancel: Build|Finalizing -> Failed -> Reset/Free
Reset/Free: any source state -> Detached; any Top state -> no token
next Prepare: fresh generation, never reuses prior token/cursor/heap
```

### 9.3 Q1: exactly one effective cleanup owner

- A finalized input vector is attached to the current source batch before any
  later operation can fail. Source cleanup owns it on projection error, Top
  admission error, cancellation after finalization, and success.
- Top copies winners; it does not share input storage. Existing Top owns all
  copied rows and displaced entries.
- An executor whose chunk has not been finalized remains owned by executor
  `Free`. A successfully transferred chunk is cleared in the executor and is
  not freed twice.
- Attachments have one exact generation token. Both sides may attempt defensive
  detach, but token matching makes only one effective transition.

### 9.4 Q2: every wait dependency terminates

The new path adds no channel, condition variable, mutex wait, RPC, I/O wait,
goroutine, or retry. `ConsumeFinalizedBatch` is a synchronous function call.
Limits that require Top spill do not fuse and retain their current error and
cancellation behavior. Cancellation is checked before each finalization chunk
and by expensive chunk finalizers, so cancellation does not wait for all
remaining groups.

### 9.5 Q3: every accumulation is bounded

Additional live output is bounded by:

```text
Top retained candidates <= K + OFFSET <= 16,384
+ one aggregate chunk <= aggexec.AggBatchSize (currently 8,192 rows)
+ bounded Group and Top expression scratch for that chunk
+ existing aggregation state
```

No per-group allocation, goroutine, lock, log, metric label, queue item, or
callback registration is introduced. Attachment state is one token per
eligible physical source/Top edge and is removed every generation.

### 9.6 Failure matrix

| Failure point | Required behavior |
| --- | --- |
| capability/eligibility missing | no attachment; execute unchanged pull path |
| provisional MergeGroup concrete capability missing | before the first admission, disable fusion and return the ordinary first batch to Top; no late fallback is permitted |
| chunk finalizer allocation/capacity error | clean any provisional output, return typed error, leave remaining chunks for Reset/Free |
| chunk finalizer cancellation | stop before next chunk, clean current provisional output, preserve caller cancellation |
| projection error | clean finalized source batch; Top has no ownership from that chunk |
| Top admission error before copy | source cleans input; Top Reset cleans earlier winners |
| Top admission error after partial retained mutation | existing Top Reset/Free cleans all Top-owned state; source independently cleans input |
| Top spill write/close error | phase-one fusion cannot activate above the in-memory threshold; the unchanged fallback preserves existing Top error propagation and cleanup |
| downstream stops after Top begins Eval | aggregation has already drained into bounded Top state; existing Top cleanup handles unconsumed result |
| panic | ordinary pipeline deferred cleanup remains the terminal owner; no new goroutine or external resource can outlive it |
| Reset after success/error | detach exact token, clear cursor, free remaining source/Top state, reset projection executors |
| repeated Prepare/execute | attach a fresh token and start cursors/heaps empty |

## 10. Compatibility, rollout, and fallback

- SQL, logical plan, physical operator kinds, result schema, protobuf, and MORPC
  remain unchanged.
- Remote pipeline reconstruction uses the same Group/Top tree. Each remote
  process performs its own prepare-time handshake.
- An incapable process executes the existing path and still emits at most local
  Top K to MergeTop. A capable process emits the same schema and cardinality.
  No cross-process capability agreement is required for correctness.
- There is no persisted/catalog state, migration, upgrade, downgrade, backup,
  restore, or restart contract.
- No authentication, authorization, tenant identity, or data-exposure boundary
  changes. All retained bytes remain charged to the same query execution
  generation. A tenant can still create high-cardinality grouping work, but
  fusion lowers rather than raises its additional finalized-output footprint.
- Rollback is code-only: removing the optional handshake restores the existing
  pull path without data conversion.
- Activation is an allowlist of concrete capabilities. New aggregates and
  shapes remain fallback until their result and lifecycle matrix is proven.
- A bounded counter records `eligible`, `fused`, and coarse fallback reasons.
  It must not contain query text, key values, table IDs, group IDs, or other
  high-cardinality labels.

## 11. Performance and capacity model

Let G be finalized groups owned by one final owner, B the aggregate chunk size,
and T = K + OFFSET.

Current additional final-output behavior is approximately:

- materialized aggregate outputs: O(G);
- Top retained candidates: O(T), or existing Top spill for large T;
- Top comparisons: O(G log T).

Eligible fused behavior is:

- live finalized input: O(B);
- Top retained candidates: O(T), with T <= 16,384 in phase one;
- Top comparisons: unchanged O(G log T);
- aggregate finalization arithmetic: unchanged;
- copied retained payload: O(T), using existing Top allocation accounting;
- callbacks: one synchronous call per non-empty chunk, O(G/B);
- new goroutines/locks/queues: zero.

The design reduces bytes crossing the child-return boundary and peak finalized
vectors. It does not claim to avoid considering G candidates or eliminate the
grouping hash/state. Low-cardinality work with only one resident final chunk
falls back after the handshake because its output is already bounded by B and a
callback cannot improve that bound. T above the cap falls back to remove
uncertainty around interaction with Top spill and cases where most groups
survive.

## 12. Observability

Use bounded operator statistics, not logs per chunk or group:

- `GroupTopKFusionEligible` / `GroupTopKFusionUsed` counts;
- coarse fallback reason: shape, partial, grouping-sets, filter-boundary,
  volatile-expression, unsupported-aggregate, or large-limit;
- finalized groups considered;
- finalized input chunks and bytes;
- rows/bytes emitted across the ordinary Group-to-Top batch boundary (zero for
  fused candidate batches);
- local Top retained/emitted rows;
- existing Top spill bytes, allocations, memory, and wall time.

EXPLAIN keeps the existing operators. Analyze output may mark the Group and Top
as one fused boundary while retaining their individual CPU/memory attribution.
The callback executes inside Top's `ChildrenCall` frame, so the existing
exclusive-time calculation attributes that synchronous wall interval to the
source boundary. Dedicated bounded Top admission chunk/row/nanosecond counters
make comparison/copy cost visible without double-counting ordinary operator
time; Top retains its existing memory/allocation accounting. The callback does
not fabricate child input rows; groups considered are reported explicitly by
the fused-boundary counters.

## 13. Validation plan

### 13.1 Deterministic unit tests

All tests use small batches, direct operator seams, injected failures/capacity,
and exact selection. No sleeps, large fixtures, retry loops, or elapsed-time
oracles.

| Contract | Focused proof |
| --- | --- |
| chunk API equivalence | For every supported COUNT/SUM/MIN/MAX type family, concatenate `FinalizeChunk` results and compare with ordinary `Flush`; cover empty/one/multiple chunks, NULL, variable-width MIN/MAX, overflow/error, cancellation, and partial-prefix Free. |
| exact Top semantics | Run the same deterministic input through fused and forced-unfused operator trees; compare fully ordered output for ASC/DESC, multi-key, group-key plus aggregate, fixed/VARCHAR/composite keys, NULL ordering, and complete tie-breakers. |
| legal ties | Use all-equal and boundary-tie data; compare result cardinality and allowed multiset/boundary rather than incidental row identity unless a tie-breaker is specified. |
| K/offset | 0, 1, 10, 16,384, 16,385 fallback, checked non-zero offset TopN, and prepared execution whose runtime limit crosses the activation cap. |
| projection | direct aggregate, deterministic arithmetic projection, multiple aggregate outputs, and volatile/real-time projection fallback. Assert one evaluation per finalized row with an injected deterministic counting executor where available. |
| shape fallback | partial Group, Filter/HAVING boundary, explicit Projection operator, grouping sets, DISTINCT, ordered/configured/unsupported aggregate, scalar aggregate, and non-direct consumer. Cover provisional MergeGroup concrete rejection before first admission. Assert unchanged tree behavior and result. |
| zero aggregates | Grouped DISTINCT with only grouping keys uses the same chunk ownership path without aggregate finalizers and remains equivalent to unfused execution. |
| ownership | Clean each source chunk immediately after synchronous consumption, then evaluate Top output to prove winners own fixed and variable payloads. Allocation ledger must return to terminal zero. |
| unhappy paths | Inject failure/cancel at first/middle/last chunk, second aggregate column, projection, and Top comparison/copy. Keep the existing unfused Top-spill error tests as the large-limit fallback control. Assert one cleanup, no retained vector, and typed error/cancel. |
| lifecycle | success/error/cancel followed by Reset, second Prepare, and reuse with different K/input. Assert fresh token, cursor, expressions, heap, and no prior winner. |
| spill composition | Force small deterministic Group spill and verify all restored complete groups are considered exactly once by one persistent local Top consumer. This does not test #27729 parallelism. |
| execution topology | Compile DOP=1, local multi-DOP, no-shuffle MergeGroup, shuffle final Group, and remote-reconstructed trees. Assert only final owners attach and local output is <= T. |

Because the changed behavior is SQL-visible, extend an existing coherent GROUP
BY/ORDER BY/LIMIT BVT with minimum rows for COUNT/SUM/MIN/MAX, NULL/multi-key,
offset, HAVING fallback, and ties. Run it twice on the same ready test-owned
instance and verify teardown. BVT proves public equivalence, not activation;
focused compile/operator tests prove the fused boundary.

### 13.2 Race and lifecycle evidence

There is no new parallel state, but attachment/reset/reuse is lifecycle state
and local scopes are cloned. Run focused tests under race mode, first once and
then repeated if runtime permits. A full package race run is required only if
the focused run or closure map exposes shared-state behavior beyond per-operator
ownership.

### 13.3 Benchmarks and integration evidence

Benchmarks report both fused and forced-unfused controls on the same binary:

- group cardinality: 0, 1, low, and high;
- K: 1, 10, 1,024, 16,384, plus 16,385 fallback;
- keys: fixed, VARCHAR, and composite;
- aggregates: COUNT, SUM, MIN/MAX, and mixed supported output;
- DOP: 1 and multi-DOP;
- resident and forced-spill Group controls.

Record aggregate-output rows/bytes, groups considered, Top boundary input,
local candidates emitted, CPU, allocations, peak accounted memory, spill I/O,
and wall time. The 10M-group #27685 reproduction is an integration/performance
gate, not a UT. Acceptance requires that it no longer publishes approximately
685.59 MiB / 10M rows through the standalone boundary and that each local owner
emits at most T rows to MergeTop.

Low-cardinality, large-K fallback, and unfused unsupported shapes must show no
material regression. The implementation PR must state the measured threshold
used for “material”; it may not infer success from wall time alone.

### 13.4 Implementation evidence

The implementation keeps the decisions above intact and adds deterministic
evidence for:

- per-chunk COUNT/SUM/MIN/MAX equivalence with ordinary Flush across the 8,192
  row chunk boundary, including NULL and variable-width results;
- cancellation before transfer, repeated/out-of-range finalization, and
  partial-prefix cleanup;
- exact fused versus forced-unfused Top output with COUNT/SUM/MIN/MAX,
  variable-width MIN, NULLs, multiple order keys, and a complete tie-breaker;
- deterministic embedded projection, zero-aggregate grouped DISTINCT,
  generation-safe attach/detach reuse, and the 16,385 large-limit fallback;
- MergeGroup's provisional handshake before concrete executor construction;
- one persistent consumer across deterministic Group spill/reload;
- terminal-zero Group and Top allocation accounts; and
- SQL-visible COUNT/SUM/MIN/MAX, NULL ordering, offset, and HAVING fallback in
  the existing qexec GROUP BY BVT.

The focused same-binary benchmark reports wall time, allocations,
`top-input-rows/op`, and `fused-finalized-bytes/op` for low-cardinality and
8,193-group controls. The 10M-group reproduction remains an integration gate;
it is intentionally not a unit-test fixture.

### 13.5 Validation evidence (2026-08-30)

- Full CGo owning-package suites passed for `pkg/sql/plan/function`,
  `pkg/sql/colexec/aggexec`, `pkg/sql/colexec/group`, `pkg/sql/colexec/top`, and
  `pkg/sql/compile` using `mo-cgo-test`.
- The focused race suite passed for chunk equivalence/cancellation, ownership,
  provisional fallback, spill/reload, generation reuse, exact fused/unfused
  output, allocation failure, and malformed function metadata.
- The same-binary benchmark used `-benchtime=100x -count=5`. The material-
  regression threshold was 5% on median wall time or allocated bytes. For 128
  groups, the direct edge selected the single-chunk fallback (128 ordinary Top
  input rows, zero fused bytes), with 13.721 microseconds median versus 15.282
  microseconds for the capability-hiding control and one fewer allocation. For
  8,193 groups, fusion admitted two chunks directly (zero ordinary Top input
  rows) with 259.778 microseconds median versus 260.372 microseconds, 17,833
  versus 18,008 B/op, and 100 versus 99 allocs/op. Timing is treated only as a
  no-regression control; the semantic result and bounded-boundary counters are
  the primary proof.
- The qexec GROUP BY BVT was extended, but was not run locally because no ready
  test-owned MatrixOne instance was listening on port 6001. CI remains the
  public SQL gate. The 10M-group #27685 reproduction remains an explicit
  integration/performance gate rather than a UT.

## 14. Implementation sequence and review gates

1. Add the optional chunk-finalizer contract and factor supported executors so
   ordinary Flush and chunk Flush use one implementation. Land equivalence,
   failure, cancellation, and partial-prefix cleanup tests first.
2. Add the generation-token source/consumer handshake and refactor Top's input
   admission into one method shared by pull and fused calls.
3. Add Group and MergeGroup finalization drains, immediate input cleanup, and
   static plus concrete eligibility checks.
4. Add compile/remote clone/reconstruction, fallback, lifecycle, race, and
   allocation-ledger evidence.
5. Add the minimum public BVT and focused benchmarks. Run the 10M integration
   control only after deterministic correctness and ownership evidence passes.

The design gate passed at revision `9c91e8ca3f`. Any material deviation in
ownership, supported shapes, activation threshold, wire/topology, or cleanup
still requires this document to be updated and the affected decision reviewed
again.

## 15. Decision log

| Decision | Rationale |
| --- | --- |
| Reuse Top as the only ordering authority | Prevent comparator, NULL/collation, tie, spill, and allocation drift. |
| Optional aggregate capability | Unsupported/new aggregate executors remain safe fallback without expanding a broad mandatory interface. |
| Prepare-time structural handshake | Handles dynamic prepared limits, cloning, remote reconstruction, and rollback without a wire field. |
| Synchronous borrowed input | Smallest ownership/wait graph; Top copies only retained winners. |
| Reject HAVING and volatile expressions initially | Keeps evaluation-order proof narrow; these can be enabled only with independent evidence. |
| Cap phase-one fusion at 16,384 | Aligns with existing bounded in-memory Top/offset conversion and avoids coupling first rollout to Top spill. |
| Preserve visible operator tree | No plan/protocol compatibility change; observability marks the fused edge instead. |

## 16. Open decisions

None. The first implementation includes both final `Group` and `MergeGroup`
owners and uses the 16,384 activation cap. Review feedback that changes either
decision must update this document before implementation approval; it may not
weaken exactness, fallback, ownership, or lifecycle gates.
