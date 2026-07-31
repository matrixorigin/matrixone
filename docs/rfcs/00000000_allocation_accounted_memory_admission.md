- Status: implementation validation
- Start Date: 2026-07-30
- Authors: aptend
- Implementation candidate: `feature/26459-statement-lifecycle` at
  `656e254fe6`
- Issue for this RFC:
  [#26459](https://github.com/matrixorigin/matrixone/issues/26459)
- Implementation plan:
  [Allocation-Accounted Memory Admission Implementation Plan](../design/allocation_accounted_memory_admission_impl.md)

# Allocation-Accounted Memory Admission for Spillable SQL Execution

## Summary

MatrixOne currently protects HashBuild with finite query and CN budgets and
bounded spill. That protection is necessary: before it existed, a large join
could continue allocating until the CN was killed by OOM. The remaining
problem is that several hard admission decisions are based on predictions of
an operation's future memory rather than the capacity of memory that is
actually allocated and retained.

Predictions such as SQL type maximums, payload multipliers, recursive
expression-tree peaks, or logical batch sizes are useful for deciding to spill
early. They are not reliable enough to decide that a valid statement cannot
run. The same prediction can over-count aliases and retained buffers, or
under-count an allocate-copy-free overlap.

This RFC replaces estimator-driven hard rejection with allocation-accounted
ownership:

```text
hard admission = capacity of real live allocations
               + named, bounded non-allocator scratch

prediction     = scheduling hint only
```

An account-aware allocation reserves the exact physical capacity before it is
allocated. The resulting charge follows the allocation across reuse and owner
handoff, and is released by the same physical `Free`. Growth accounts for the
real replacement overlap by keeping the old allocation charged while
admitting the complete replacement capacity.

When a real allocation cannot be admitted, execution treats it as typed memory
pressure: reclaim retained state, spill, retry, reduce the processing batch
where possible, degrade optional structures, and only then return a controlled
error for a minimum indivisible allocation that cannot fit.

The first consumer is HashBuild and the joins that share its spill lifecycle.
The accounting primitive is deliberately defined below the SQL operator layer
so other spillable operators can adopt the same model later.

The current activation is deliberately owner-closed. Build and probe trees
from the audited COL/literal/CONCAT/CASE/varchar-EQUAL/string-CAST set activate
exact accounting for the whole local HashBuild/join owner set. A tree
containing an unclosed generic function family keeps all participating owners
in that local attempt on the legacy path; exact map/batch ownership is never
mixed with an estimator-gated expression. The remaining generic-function
migration is therefore explicit rather than being misreported as RFC
completion.

## Motivation

### Confirmed failures

The incidents below include both false rejection and real under-accounting.
They are opposite results of using predictions as allocation facts.

| Incident | Observed behavior | Accounting mismatch |
| --- | --- | --- |
| #25782 / #25837 | HashBuild could OOM a CN instead of spilling | hash-table growth was not admitted at its physical allocation boundary |
| #26174 / #26178 | fulltext INSERT requested 18.72 GiB with about 1.29 GiB used | current ingress, retained tail, const materialization, and future drain were charged as if simultaneously owned |
| #26192 / #26231 / #26318 | LOAD DATA was rejected | runtime-filter and payload multipliers duplicated already retained owners |
| #26413 / #26438 | a Hive external-table self-join was rejected | a 50K-row logical ingress estimate did not match the real 8192-row copy segmentation and allocator rounding |
| #26454 | a string-expression join requested exactly 551,368,048,640 bytes while observed memory was about 6--7 GiB | TEXT maximum size was multiplied by row count and recursively summed through CAST/CONCAT nodes |
| #26433 / #26455 | expression result capacity could be under-counted or double-charged across reuse | the budget lease lifetime did not match retained `ExpressionExecutor` capacity |
| #26186 | a spill transition could under-count ingress overlap | independent estimates omitted a simultaneously live ownership state |

Fixing one multiplier or expression kind does not close the defect class. A
more conservative estimate prevents one OOM shape but rejects more valid
queries. Relaxing the estimate restores those queries but can miss a different
physical overlap.

### Root problem

Three concerns are currently mixed:

1. **Ownership accounting**: which allocations are live, which finite account
   owns them, and when their charges end.
2. **Operation prediction**: how much an upcoming expression, batch copy,
   marshal, or spill transition might allocate.
3. **Pressure response**: what execution does when the next allocation cannot
   fit.

Only the first concern can support a hard capacity invariant. Prediction and
pressure response remain necessary, but they cannot be the source of truth for
live memory.

### Relationship to SQL Resource Accounting

This RFC is separate from
[`SQL Resource Accounting`](./00000000_sql_resource_accounting.md).

SQL Resource Accounting defines observational facts used by statement trace,
CU, and physical-plan diagnostics. Its memory fields describe MPool domain
peaks after execution; it explicitly does not provide allocation-site
attribution or admission control.

This RFC defines an execution-time control mechanism:

| Concern | SQL Resource Accounting | This RFC |
| --- | --- | --- |
| Primary purpose | observe and persist usage | prevent an allocation from exceeding a finite execution account |
| Time of decision | execution summary / statement completion | immediately before allocation or growth |
| Unit | domain usage and peak | live physical allocation capacity |
| Missing data | quality flag | the path is not considered fully protected |
| Failure behavior | report incomplete facts | reclaim, spill, retry, or controlled pressure error |

The two systems may share metrics and consistency checks, but one must not be
derived from the other. A terminal MPool peak cannot authorize an allocation
that has already happened, and a HashBuild account is not a complete statement
memory measurement.

## Goals

1. Make every hard memory charge correspond to a live physical allocation or
   a named, bounded scratch owner.
2. Admit memory before allocation and leave allocator/account state unchanged
   on failure.
3. Account for the real capacity chosen by MPool, including replacement
   overlap during `Grow` and `Grow2`.
4. Make allocation provenance survive vector reuse, cross-owner handoff, and
   cross-MPool `Free`.
5. Make Reset, Free, cancellation, retry, and generation turnover obey one
   ownership contract.
6. Treat real capacity rejection as recoverable pressure where execution can
   make progress.
7. Prevent a prediction alone from rejecting a query.
8. Add no per-row reservation or shared budget lock in the steady-state reuse
   path.
9. Make the primitive reusable by spillable operators outside HashBuild.
10. Retain the no-OOM safety objective introduced for #25782.

## Non-goals

This RFC does not:

- make the account equal to CN RSS or total Go runtime memory;
- remove process/CN headroom for caches, RPC, logs, goroutine stacks, or other
  memory outside the controlled allocation domain;
- guarantee that every SQL statement completes under every finite cap;
- make an indivisible value smaller than its real representation;
- change SQL syntax, catalog metadata, or a persisted/wire format;
- replace spill disk and file-descriptor accounting;
- use type-specific exemptions for TEXT, CONCAT, CASE, LOAD DATA, or a
  particular benchmark;
- raise the existing cap to hide false estimates;
- add a mutable process-wide "current memory account";
- require all MPool users to become accounted in the first implementation.

## Terminology

### Allocation account

A finite ledger that admits and releases bytes for one execution ownership
domain. `HashBuildBudgetGeneration` is the initial policy implementation, but
the allocator contract is not HashBuild-specific.

### Accounted allocation

An allocator-owned allocation whose metadata records:

- its actual allocated capacity;
- an opaque reference to the account charge;
- bounded diagnostic classification such as owner class and allocation site.

The charge belongs to the allocation, not to the operator field that currently
references it.

The first implementation makes data-sized accounted allocations off-heap.
`MPool.Alloc(..., false)` uses a Go allocation: its requested size is not the
runtime size class, and removing MPool metadata does not make the GC reclaim it.
Such memory cannot be described as exact physical ownership at `Free`.

### Allocation lease

An exactly-once charge returned after successful admission. It is attached to
the allocation before the allocation is published to its caller. `Free`
releases it. Copying a lease value must not create a second release owner.

### Explicit scratch lease

A lease for bounded memory that cannot yet be allocated through MPool. It must
have one named owner, a finite size, and an explicit release point. It is an
exception used during migration, not a substitute for accounting ordinary
buffers. A data- or row-scaled Go allocation is not accepted merely by adding
such a lease: it must move off-heap or retain a conservative charge through a
proved GC-reclamation boundary. The initial implementation permits only small,
statically bounded Go metadata under explicit CN headroom.

### Prediction

A non-authoritative estimate used to select an execution strategy, start spill
before a hard limit, or choose an initial batch size. A prediction does not
create a retained allocation lease and cannot directly produce a terminal
budget error.

### Generation

One query-CN execution ownership epoch. The generation fixes the account
identity used by allocations that can outlive one operator call or move
between producer and consumer operators.

## Required invariants

### I1. Conservation

For a live account at every observable transition:

```text
account.used
  = sum(capacity of live off-heap allocations charged to the account)
  + sum(size of live named explicit scratch leases)
```

Predictions, logical vector length, source batch size, and SQL type maximum do
not appear in this equation. Small allocation/runtime metadata classified as
headroom is outside `account.used`; I9 separately requires a finite aggregate
bound for it.

### I2. Admission precedes allocation

The complete capacity of a new physical allocation is admitted before MPool or
another allocator changes state.

### I3. Failure atomicity

If admission or allocation fails:

- no new allocation is published;
- the old allocation remains valid;
- any provisional lease is released;
- account usage and MPool usage return to their pre-operation values.

### I4. Reclaimable ownership defines charge lifetime

- shrinking a logical length does not release capacity;
- Reset retains both reusable capacity and its charge;
- reuse within existing capacity performs no new admission;
- off-heap `Free` releases physical memory and the charge together;
- a Go reference becoming unreachable is not treated as physical release;
- replacing a buffer transfers publication only after the replacement is
  complete.

### I5. One physical allocation has one charge

Aliases, vector windows, const views, and shared areas do not create a second
charge. A deep copy creates a new allocation and therefore a new charge.

### I6. Provenance survives handoff

The account identity and charge remain correct when:

- a build-side buffer is published to a `JoinMap`;
- a vector or batch moves to another operator;
- an allocation is freed through a different MPool;
- the producing operator has already Reset;
- a retained buffer is reused in a later call of the same execution
  generation.

### I7. Generation closure is not implicit memory release

Sealing a generation prevents new admission but does not pretend that live
allocations disappeared. Existing allocation leases remain releasable. Normal
owner cleanup must bring usage to zero; a nonzero terminal value is an
invariant failure and a leak signal.

### I8. Prediction cannot hard-reject

An estimate may trigger early reclaim or spill. A terminal memory pressure
error must name a real allocation or explicit scratch request that failed
admission after applicable pressure responses.

### I9. Metadata headroom is finite

Every accounted owner has a proved maximum number of simultaneously live
allocations. Pointer headers, account-ID side records, registry entries, and
other per-allocation Go metadata have measured per-entry bounds. Their
aggregate bound is reserved as CN headroom:

```text
metadata headroom
  >= maximum live allocation count * measured metadata bytes per allocation
     + maximum live generation count * measured registry bytes per generation
```

The implementation enforces both counts with finite CN-local metadata slots.
Opening a generation consumes a generation slot; publishing an accounted
allocation or replacement consumes an allocation-metadata slot; physical Free
returns that slot. Replacement growth temporarily consumes slots for both old
and new allocations. Slot exhaustion is exact metadata pressure, not a
payload estimate. An owner with no supported bound cannot be activated merely
because each payload allocation is charged.

The first hash-table activation's concrete slot counts, conservative
per-entry bytes, and startup headroom formula are fixed in the implementation
plan. A later owner with smaller allocations must re-derive and provision its
own count before activation; the generic API does not silently widen the
proved domain.

## Technical design

### 1. Separate accounting mechanism from cap policy

The low-level allocator must not import the SQL operator or `process` package.
It depends on a small generic contract, conceptually:

```go
// Names are illustrative; this RFC does not freeze the Go API.
type AllocationAccount interface {
    Acquire(AllocationRequest) (AllocationLease, error)
}

type AllocationRequest struct {
    Capacity uint64
    Class    AllocationClass // bounded enum
    Site     AllocationSite  // bounded enum
}

type AllocationLease interface {
    Capacity() uint64
    Release()
}
```

The contract is synchronous and does not wait for memory or reclamation; it may
briefly contend on account synchronization. It is called only when a physical
allocation or growth is required, not for every row or append.
It returns non-overlapping typed reasons: finite capacity pressure,
sealed generation, account mismatch, allocator size limit, and invariant
corruption. Only finite capacity pressure may enter reclaim/spill/retry.

The policy layer remains responsible for:

- query and CN caps;
- cap refresh;
- concurrency and linearization;
- spill disk/FD ledgers;
- metrics;
- typed pressure errors.

The allocator layer is responsible for:

- requesting the actual allocation capacity;
- failure rollback;
- associating the lease with allocation metadata;
- releasing the lease exactly once with physical memory.

### 2. Allocation metadata and provenance

MPool already tracks each allocation in pointer metadata so it can identify
the original pool, allocation size, off-heap status, double free, and
cross-pool free. Account provenance belongs at this same boundary.

An accounted allocation adds an optional opaque charge handle to that
metadata. Unaccounted allocations retain current behavior. The handle's exact
representation must be benchmarked: adding a Go interface to every metadata
entry is not assumed acceptable. A compact pointer or account-local lease
record is preferred if it preserves exactly-once release and diagnostic
identity.

The metadata is authoritative. Operator-maintained byte totals may remain as
diagnostics during migration, but they cannot independently release or
reconstruct the charge.

Cross-MPool `Free` already delegates physical release to the original MPool.
The same terminal path releases the account charge, so the freeing caller does
not need to recover the producing operator or generation.

An existing allocation's metadata also decides the account used by growth.
Growing an account-A allocation under account B is an invariant error.
Unaccounted-to-accounted conversion is never implicit: a migrated owner creates
an accounted destination and copies from the unaccounted source, or creates its
own buffers as accounted from the beginning. This prevents a caller from
changing only a vector field while the retained physical buffer still has
different provenance.

For the compact-handle design, the registry is a finite set of reusable slots.
An opaque handle contains the slot and its generation counter. A slot is
reused only after sealing and exact zero; incrementing its generation makes
every older handle stale. Generation counters never wrap: a slot whose counter
is exhausted is retired. A missing or generation-mismatched registry entry
during `Free` is an invariant failure, not permission to drop the charge.

Pointer headers, account-ID side records, and registry entries are `H`
metadata, not accounted payload. Their backing stores are sized from finite
CN-local slot limits and charged to explicit CN headroom. Before activating an
owner, supported live-allocation and generation counts must fit those limits;
the limits remain the safety backstop if an ownership assumption is wrong.

### 3. New allocation protocol

Initial allocation and growth share one authoritative
`AllocationCapacity(request, allocatorMode)` calculation. It includes
allocator rounding and the actual maximum accepted by `Alloc`; callers do not
infer capacity from a logical size or from `GrowCapacity` alone.

For an account-aware allocation of capacity `C`:

```text
calculate actual allocator capacity C
  -> acquire lease(C)
  -> reserve one allocation-metadata slot
  -> allocate C
     -> on failure: return metadata slot, release lease(C), return error
  -> attach lease to allocation metadata
  -> publish allocation to caller
```

If metadata attachment can fail, it is part of the unpublished transaction:
free the new allocation, release the lease, and return an error.

MPool cap failure, global cap failure, and underlying allocator failure all
follow the same rollback contract.

Metadata publication is checked. An allocator panic before publication must
run provisional-lease cleanup. Cross-pool `Free` and deleted-owner-pool
fallback release the lease when they physically deallocate the allocation.

Pool teardown is not itself a universal release event. A `noLock` teardown
that physically deallocates pool-local allocations releases each matching
lease. Unregistering a normal pool retains global pointer metadata and the
charge until later physical `Free`; live accounted allocations at teardown are
reported as an invariant violation, never bulk-released.

### 4. Growth and replacement protocol

MPool `Grow` and `Grow2` currently allocate a replacement, copy the old bytes,
then free the old allocation. Hard accounting must represent that real
overlap.

For old capacity `O` and required logical size `R`:

1. calculate `N = AllocationCapacity(GrowCapacity(O, R), allocatorMode)`;
2. keep the old allocation and its `O` charge live;
3. acquire a complete `N` lease, not `N - O`;
4. allocate `N`;
5. copy old data and any second source;
6. attach the new lease and publish the replacement;
7. free the old allocation, releasing its `O` lease.

Peak account usage during replacement is therefore:

```text
other live allocations + O + N
```

After publication and old-buffer release it is:

```text
other live allocations + N
```

Reserving only the delta would under-count the actual allocate-copy-free peak.
Using a multiplier would be an estimate of the same fact even though the
allocator already knows `O` and `N`.

If `R <= O`, no physical growth occurs and no account call is made.

#### Growth failure table

| Failure point | Old buffer | New buffer | Account result |
| --- | --- | --- | --- |
| capacity calculation | unchanged | none | unchanged |
| new lease admission | unchanged | none | unchanged |
| physical allocation | unchanged | freed/not published | new lease released |
| copy before publication | unchanged | freed/not published | new lease released |
| publication succeeds | released afterward | live | old lease released; new lease retained |

The implementation must preserve this table under panic-safe cleanup where
MPool currently permits a recoverable error.

### 5. Vector and batch propagation

An allocation's metadata owns the retained charge. A vector additionally
needs an optional account selection for the first allocation of a currently
nil buffer. Subsequent growth inherits or verifies the existing allocation's
account.

The following rules apply:

| Operation | Rule |
| --- | --- |
| append within capacity | no admission |
| append causing data growth | admit exact replacement capacity |
| append causing varlen-area growth | independently admit exact replacement capacity |
| logical reset / set length to zero | retain allocation and charge |
| vector Free | free every owning buffer; each allocation releases its own charge |
| window/view/const alias | no new allocation and no new charge |
| deep Dup/copy | destination allocations use the destination account |
| batch handoff | allocation metadata preserves charges; no sum-and-rereserve |
| cross-pool Free | original allocation metadata releases the original charge |

Data and varlen area are separate physical allocations and therefore separate
charges. Null bitmap and other auxiliary buffers follow their actual
allocation ownership rather than a synthetic per-vector total.

The first production migration accounts only off-heap vector buffers.
Row-scaled null/group bitmaps currently allocate Go `[]uint64`; they must move
to an off-heap owner or remain an explicit activation blocker. Switching a
vector field to an account while its existing backing remains on-heap is not a
valid migration.

A shared area must retain one physical release owner. If current vector sharing
permits multiple logical owners, account integration must use the same
reference/ownership mechanism that prevents physical double free; it must not
introduce a second budget-only reference count.

### 6. Expression execution

`FunctionResult` and `ExpressionExecutor` must create result vectors with the
execution account selected for the owning HashBuild path.

Fixed-width results allocate from actual row count and element width through
normal vector growth. Varlen results allocate from actual appended payload.
Neither uses the maximum SQL type width multiplied by row count.

Intermediate expression results are charged only while their buffers are
physically live. Reuse keeps the charge. Reset does not release it unless Reset
also frees the buffer.

Expression implementations that create unbounded temporary Go strings or byte
slices bypass MPool and must be changed by one of these methods:

1. write directly into an account-aware result buffer;
2. use an account-aware MPool scratch buffer;
3. for small metadata only, use a named explicit scratch lease with a proved
   finite bound and one cleanup owner.

For example, CONCAT should not construct an unaccounted complete Go string and
then copy it into an accounted result vector. The temporary and final buffers
can be simultaneously live, so omitting the temporary would violate I1.

Recursive expression peak calculation may remain temporarily as an early-spill
hint. Before deleting it as a hard gate, an allocation-site ledger must cover
every reachable data-scaled allocation in generic evaluation and built-ins,
including selection arrays, conversion slices, nested result vectors, and
function-specific scratch. Migrating only `FunctionResult` is not closure.

### 7. Non-vector HashBuild allocations

The migration inventory must include all memory whose lifetime is owned by the
HashBuild execution domain:

- copied build and probe batches;
- integer and string hash-table blocks;
- selection lists and group mappings;
- join-map auxiliary storage;
- expression keys and intermediate results;
- spill scatter buffers;
- spill/re-spill read and decode buffers;
- runtime-filter buffers;
- marshal/unmarshal scratch;
- retained emergency scratch.

An ownership closure is not complete merely because its largest vector is
accounted. Every data-scaled allocation reachable from an activated owner must
use account-aware, synchronously reclaimable allocation. Unactivated owners
retain their complete legacy charge until their own closure; only small,
statically bounded metadata may remain under named headroom.

Hash-table callbacks that already reserve from `ResizePlan` are an intermediate
bridge. The final charge should be owned by the physical hash-table allocation
metadata rather than a parallel slice of reservation tokens in the operator.

### 8. Pressure response state machine

A failed exact allocation admission returns typed memory pressure, not
`ErrHashBuildBudgetInvalid`.

The owner handling the request proceeds through a bounded state machine:

```text
need allocation
  -> exact admission succeeds
       -> allocate and continue
  -> exact admission rejected
       -> release reclaimable retained capacity
       -> retry exact allocation
       -> start/advance spill
       -> retry exact allocation
       -> reduce processing batch where semantics permit
       -> retry exact allocation
       -> degrade optional structure where permitted
       -> retry/continue
       -> return controlled minimum-unit pressure error
```

Not every allocation supports every response. The request carries an owner
class that maps to a policy:

| Owner class | Permitted response |
| --- | --- |
| retained reusable result | release retained capacity, then retry |
| spillable build/probe input | spill/re-spill, then retry |
| splittable expression batch | reduce batch, then retry |
| runtime filter | degrade to PASS when correctness is unchanged |
| indivisible single value / minimum hash block | controlled error with actual capacity |
| invariant corruption | fail immediately as invalid state |

The state machine records which responses were attempted so it cannot loop
without progress. A retry is justified only after account usage decreased,
the input unit became smaller, spill state advanced, or an optional owner was
disabled.

`ErrHashBuildBudgetInvalid` remains reserved for arithmetic overflow, corrupted
ownership, double release, account mismatch, or an impossible lifecycle
transition. Ordinary finite pressure is not an invariant failure.

A sealed or stale generation is a lifecycle result distinct from finite
pressure. It cannot enter the retry state machine.

#### Retry transaction boundary

Allocation rollback alone does not make an operator operation retryable. A
vector or expression may successfully grow one retained buffer, fail on the
next allocation, and leave reusable capacity or partially written output.
Each retry-capable owner therefore defines:

- the unpublished operation checkpoint;
- which retained capacity may survive failure;
- how row/output publication is rolled back;
- the Reset/Free actions required before retry;
- how a smaller batch resumes without duplicate work.

Until the shared controller and these checkpoints exist, an exact rejection
returns a controlled terminal pressure error. An earlier migration must not
claim spill/reduce/retry merely because it has allocation-level rollback.

#### Forward-progress memory

Spill and reclaim paths do not bypass admission. Encoding, decoding, scatter,
and IO buffers are allocations too. Allowing normal work to consume the last
byte and then allocating uncharged "emergency" scratch would reproduce the
original safety hole at a different site.

Before retaining a unit of work, an operator must preserve one bounded way to
make progress:

1. reuse an already allocated and accounted spill buffer;
2. keep a finite progress sub-cap that normal work cannot consume and charge
   actual spill/cleanup allocations against it; or
3. reduce the retained or spill chunk until the minimum real progress
   allocation fits.

Progress headroom is cap policy, not a fabricated live-memory charge:
`account.used` still contains only actual allocations and explicit live
scratch. If a sub-cap is used, normal and progress allocations remain bounded
by the same total query/CN cap, with the progress portion unavailable to normal
growth.

The minimum progress unit is derived from the concrete buffer/chunk layout, not
from SQL type maximums. If retained state plus that minimum real unit cannot
fit, admission must stop earlier or return a controlled pressure error; it
must not wait until spill itself is unable to start.

### 9. Concurrency and generation lifecycle

Child pipelines may share a `BaseProcess` and execute concurrently. Therefore
neither Process nor MPool may contain a mutable "current account" used
implicitly by all allocations.

Account selection is explicit at the owner/allocation boundary and immutable
for a published allocation. Concurrent allocations linearize in the account's
existing query/CN admission operation. Reuse within capacity does not enter
that lock.

Normal generation lifecycle is:

```text
open
  -> allocations and explicit scratch may acquire leases
  -> seal: no new leases
  -> owner cleanup frees all live allocations/scratch
  -> used reaches zero
  -> finalize
```

One `Compile` execution attempt on each CN is the sole seal/finalize owner for
that CN's generation. HashBuild Reset cannot seal: `JoinMap`, spill payload,
broadcast, remote scope, and message-board consumers may outlive the producing
operator and still own or create controlled work. The attempt coordinator
seals only after all scopes and remote notifiers it owns have stopped
publishing work and its MessageBoard consumers have reached a terminal state.
Cleanup may then release old leases; exact zero produces one immutable
terminal snapshot and permits registry-slot reuse. The statement
`ResourceRoot` aggregates these immutable CN-attempt snapshots but does not own
allocation release.

A nonzero terminal generation does not disappear and does not wait forever:

```text
seal after execution/message quiescence
  -> run terminal owner cleanup
  -> zero:
       export one valid immutable snapshot
       remove the registry entry
  -> nonzero at terminal-cleanup completion:
       export one immutable invariant-failure snapshot
       retain a release-capable tombstone until late Free reaches zero
       suspend admission of new accounted generations on that CN
```

Suspension bounds tombstone growth to generations already active when the
first invariant failure is detected. It is lifted only after every tombstone
reaches zero; an operational deadline escalates with owner/site diagnostics
and permits a controlled CN restart rather than deleting provenance. Late
release may update bounded health counters, but it cannot rewrite or duplicate
the exported snapshot.

Generation open and nonzero-terminal suspension linearize through the same
CN-local generation gate. Opening publishes a generation only if the
suspension check succeeds in that transaction. Once suspension publication
linearizes, no later open may publish; opens that linearized earlier are the
finite active set allowed to finish or become tombstones.

`SetStmtProfile` turnover, frontend `StatementInfo.EndStatement`, and
`HashBuildBudgetGeneration.Close` alone do not prove this per-CN quiescence or
validate zero. The implementation therefore gives the `Compile` attempt an
explicit post-pipeline/MessageBoard-close transition for success, failure,
panic, cancellation, retry, broadcast, prepared reuse, and remote execution.
A forced close never silently zeros accounting while allocations remain live.

Ownership transfer does not change generations. If a transfer would cross to a
different generation, it must either:

- retain the original generation until the allocation is freed; or
- perform one explicit atomic charge transfer before publication.

The initial implementation should prefer retaining original provenance; charge
transfer adds a second failure and rollback boundary and is unnecessary for
normal HashBuild producer-to-consumer handoff.

### 10. Observability

Admission exposes bounded owner/site, actual capacity, used/cap, attempted
pressure response, and terminal result. Exact allocation events, prediction
hints, pressure responses, and invariant failures remain distinguishable.

Owner/site values are bounded enums. Metrics and logs aggregate at
operator/generation or terminal-pressure boundaries; there is no
per-allocation log or unbounded SQL/stack label.

The controlled-domain snapshot has a stable generation identity and is
exported exactly once by its CN attempt coordinator. The statement resource
root may aggregate those snapshots. It is separate from SQL Resource
Accounting's current off-heap MPool domain; consumers must not sum duplicate
operator references to one generation or claim the domains match before owner
coverage does.

## Migration plan

Migration is incremental by complete physical owner. Exact accounting and
removal of that owner's legacy hard charge happen atomically; one buffer is
never charged by both models.

The implementation order is:

1. measured metadata/API decisions and a reference model;
2. generic MPool allocation transaction;
3. dormant Vector/Batch propagation;
4. allocation-site closure and dormant expression/spill propagation;
5. statement generation lifecycle, typed pressure, and retry checkpoints;
6. hash-table cell/descriptor activation with only its legacy charge removed;
7. copied-batch and JoinMap activation;
8. expression-owner activation;
9. spill and runtime-filter closures;
10. unified join pressure control and remaining legacy estimator deletion;
11. workload and performance acceptance.

The allocation-site ledger, PR gates, rollback rules, and test commands live in
the
[implementation plan](../design/allocation_accounted_memory_admission_impl.md).

## Rollout and compatibility

The change is internal to one CN binary and does not change SQL, catalog, disk,
or RPC formats. Unmigrated MPool users retain current behavior. A HashBuild
owner enables exact accounting only after its full alloc-to-Free closure is
covered and its legacy hard charge is removed. The final design has no
permanent legacy/exact switch.

## Testing strategy

Testing is derived from the invariants:

- a randomized reference model checks conservation after alloc, grow, failure,
  reuse, handoff, Free, seal, and cancellation;
- boundary and fault tests cover exact cap, allocator rounding, unpublished
  rollback, cross-pool Free, and generation turnover;
- container/operator tests cover aliases, varlen data, Reset, broadcast,
  spill/re-spill, pressure progress, and optional degradation;
- workload regressions cover #25782, #26174, #26192, #26413, #26454, and TPCH
  spill/non-spill paths;
- performance gates verify no per-row account operation, no budget lock on
  within-capacity reuse, bounded metadata cost, concurrent-generation P50/P99
  admission latency, release storms, and separately measured resident/spill
  behavior.

Exact matrices and per-PR gates are maintained in the
[implementation plan](../design/allocation_accounted_memory_admission_impl.md).

## Drawbacks

### Allocator and container changes are invasive

Correct ownership crosses MPool, vector, batch, expression, hash table, and
operator handoff boundaries. A partial implementation can create a more
convincing but still incomplete safety claim.

Mitigation: migrate by complete owner class, maintain the ownership ledger, and
gate each phase on conservation properties rather than workload success alone.

### Allocation metadata has a cost

An account handle can increase pointer-map memory and alloc/free work even when
only some allocations are accounted.

Mitigation: keep it optional and compact, benchmark representation choices,
and avoid a Go interface stored inline in every metadata record unless
measurement supports it.

### Exact replacement overlap can reject earlier than steady-state size

If a 6 GiB buffer grows to 8 GiB, the current allocator may need 14 GiB live
during copy even though the final buffer is 8 GiB. Charging only 2 GiB would
look friendlier but would not protect the real peak.

Mitigation: reclaim or spill before growth, reduce the processing unit, or
introduce a genuinely lower-overlap allocator operation. Do not hide the peak
with delta accounting.

### Go-heap reclamation is not allocator-controlled

Dropping an MPool pointer record for `Alloc(..., false)` does not synchronously
return its backing bytes. Treating that event as exact physical release would
allow new work while the old Go object remains resident.

Mitigation: keep data-scaled controlled owners off-heap. Audit and migrate
row/payload-scaled Go slices before activation; reserve separate CN headroom
only for small, proved-bounded runtime metadata.

### This is not complete RSS accounting

Go runtime, goroutine stacks, caches, and unrelated subsystems remain outside
the HashBuild account.

Mitigation: preserve explicit CN headroom and state coverage boundaries. A
future broader account may reuse the primitive but requires its own ownership
inventory.

## Rationale and alternatives

### Tune multipliers and type rules

Rejected. The confirmed incidents demonstrate that no fixed multiplier
represents aliasing, reuse, variable payloads, segmentation, and replacement
overlap simultaneously.

### Use predictions only to choose spill, then remove hard budgets

Rejected. Prediction errors in the other direction can again allow #25782 to
OOM the CN. Real allocations still need finite admission.

### Charge only `newCapacity - oldCapacity` on growth

Rejected for the current allocate-copy-free implementation. It under-counts
the period where both allocations are live.

### Use MPool current bytes as the HashBuild budget

Rejected. A shared MPool contains other owners and does not preserve
HashBuild/query provenance across child pipelines. Sampling after allocation
also cannot provide pre-allocation safety.

### Store a mutable current account on Process or MPool

Rejected. Concurrent child pipelines can share the same base process and MPool.
The wrong goroutine could charge or free against another generation.

### Poll RSS and spill near the cgroup limit

Rejected as the primary control. RSS is delayed, includes unrelated memory,
and cannot make a specific allocation failure-atomic. It remains useful as a
coarse pressure signal and validation metric.

### Wrap MPool only at operator call sites

Insufficient by itself. A wrapper can select the account for initial
allocation, but provenance must still survive vector growth, physical handoff,
and cross-pool Free. The terminal charge belongs in allocation metadata.

### Raise or disable the cap

Rejected. It hides false rejection while weakening the original no-OOM
requirement.

## Unresolved questions

1. Does the provisional 16-byte-header plus side-map representation retain its
   advantage in real MPool, cross-pool, and high-concurrency benchmarks?
2. What explicit MessageBoard close-and-drain primitive and tests prove
   quiescence at the selected local and remote `Compile` attempt hooks?
3. Which remaining data-scaled Go-heap sites can write directly to off-heap
   output, and which need a new off-heap scratch abstraction?
4. What is the minimum semantically safe batch and operation checkpoint for
   each expression and spill phase?
5. What cap/headroom policy is appropriate once exact HashBuild ownership
   replaces conservative estimates? This is policy work and must not change the
   accounting invariant.
6. What measured metadata and hot-path overhead is acceptable for enabling the
   primitive beyond HashBuild?

These questions affect implementation shape, not the core decision that hard
admission must be tied to owned physical capacity.

## Acceptance criteria

The RFC is implemented only when:

- I1--I9 hold under property tests, fault injection, cancellation, and race
  execution;
- account-aware alloc/grow/free use the same real capacity calculation as
  MPool;
- every HashBuild-owned data-scaled allocation is off-heap and accounted;
- every excluded Go/runtime metadata allocation is statically bounded and
  covered by explicit CN headroom;
- live-allocation and generation counts prove the aggregate I9 metadata
  headroom;
- each CN `Compile` attempt seals, validates exact zero, and exports one
  generation snapshot after execution/MessageBoard quiescence;
- a nonzero terminal generation exports one failure snapshot, preserves
  release provenance, and cannot accumulate unbounded tombstones;
- Reset retains reusable allocation charges and Free releases them exactly
  once;
- aliases and handoffs do not duplicate charges;
- estimator-only false rejection is structurally impossible on migrated
  paths;
- exact pressure triggers bounded reclaim/spill/retry/reduce behavior only
  across proved operation checkpoints;
- real minimum-unit over-cap errors report actual allocation capacity and
  owner/site;
- #25782 cannot exceed the finite account or OOM the CN;
- #26174, #26192, #26413, and #26454 pass durable workload regressions;
- TPCH spill and non-spill performance gates pass;
- superseded hard estimators and parallel reservation owners are removed;
- the implementation documentation states the remaining memory outside the
  account and preserves corresponding CN headroom.
