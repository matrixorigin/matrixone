- Status: implementation validation
- Start Date: 2026-07-30
- Authors: aptend
- Issue: [#26459](https://github.com/matrixorigin/matrixone/issues/26459)
- Implementation plan:
  [Allocation-accounted HashBuild memory admission](../design/allocation_accounted_memory_admission_impl.md)

# Allocation-accounted memory admission for spillable SQL execution

## Summary

HashBuild needs a finite query/CN memory boundary, but a predicted future size
is not a reliable hard-admission fact. SQL maximum widths, payload multipliers,
recursive expression sums, and logical batch sizes can reject valid work by
orders of magnitude. Relaxing those estimates can instead miss a real
allocate-copy-free overlap and allow OOM.

This RFC makes the capacity of live physical allocations the source of truth:

```text
hard HashBuild admission = live retained physical allocation capacity
prediction               = planning and early-spill hint only
```

MPool admits an accounted allocation before allocating it. Immutable account,
owner, and site provenance follows the allocation across reuse and ownership
handoff. The same physical Free releases the charge. Growth admits the actual
replacement capacity while the old allocation is still charged, so overlap is
represented without a multiplier.

The first consumer is retained HashBuild and join storage. The design has one
production path: no activation switch, estimated-memory reservation, or
compatibility ledger remains beside physical accounting.

## Motivation

The related incidents show both sides of the same modeling error:

| Incidents | Failure | Mismatch |
| --- | --- | --- |
| #25782 / #25837 | CN OOM instead of bounded spill | hash-table growth was not admitted at allocation time |
| #26174 / #26178 | false fulltext INSERT rejection | non-simultaneous ingress/tail/drain states were summed |
| #26192 / #26231 / #26318 | false LOAD DATA rejection | runtime-filter/payload multipliers duplicated live owners |
| #26413 / #26438 | false external-table join rejection | logical ingress did not match physical batch segmentation |
| #26454 | request reported hundreds of GiB with single-digit-GiB usage | TEXT maximum width was multiplied through CAST/CONCAT |
| #26186 | spill overlap could be under-counted | independent estimates omitted a live transition state |

Fixing individual multipliers cannot close this class. A stricter estimate
rejects more valid queries; a looser estimate misses a different overlap.

Three concerns must remain separate:

1. ownership accounting: which retained allocations are live;
2. prediction: which strategy or capacity may be useful;
3. pressure response: spill, reclaim, reduce, degrade, or error.

Only ownership accounting can support a hard live-memory invariant.

## Goals

1. Make each HashBuild hard memory charge correspond to live physical retained
   storage.
2. Admit before allocation and leave allocator/account state unchanged on
   rejection.
3. Represent real replacement overlap during growth.
4. Preserve provenance through vector reuse, copies, and owner transfer.
5. Give Reset, Free, cancellation, retry, and prepared reuse one lifecycle.
6. Treat only typed memory-capacity rejection as reclaimable pressure.
7. Prevent a prediction alone from rejecting a statement.
8. Add no per-row reservation or Go allocation to steady-state reuse paths.
9. Keep spill disk and FD accounting distinct from memory.
10. Preserve the no-OOM objective introduced for #25782.

## Non-goals

This RFC does not:

- equate the HashBuild account with total query RSS;
- account all Go runtime/library allocations;
- guarantee completion under every finite cap;
- change optimizer join selection;
- make disk spill as fast as sufficient-memory execution;
- add type- or workload-specific exemptions;
- raise a cap to hide a false estimate;
- introduce a mutable process-wide current account.

## Controlled domain

The first implementation accounts physical storage retained by HashBuild and
its join consumers: hash tables, copied build batches, JoinMap state, retained
keys, join bitmaps/capture/result state, Product result state,
runtime-filter payloads, and spill encode/decode/rebuild buffers.

ExpressionExecutor temporary results, caches, and library-internal Go heap
remain in the existing MPool/Go runtime domain. They cannot truthfully be put
in an exact terminal-zero account while regexp, JSON, JQ, and similar libraries
do not expose allocator/free hooks. An expression value becomes accounted when
it is physically copied into retained HashBuild/join storage.

This is a static ownership boundary, not a runtime fallback. A future general
expression-memory design must either use allocator-aware implementations or a
separate explicitly non-exact transient policy. Estimates must not be inserted
into this exact ledger.

ProductL2's accounted input remains the producer-owned JoinMap. Its additional
CPU/GPU index and scratch storage is outside the first controlled domain because
the native GPU allocator does not expose an admission/free capacity contract.
Partially charging only Go-visible buffers would be another ledger, not exact
physical ownership. ProductL2 can join this domain only when both platform
implementations provide the same allocation contract.

## Allocation contract

An accounted allocation records:

- actual allocator capacity;
- allocation account and generation;
- bounded owner class and allocation site.

The contract is transactional:

1. compute the allocator capacity using checked arithmetic;
2. acquire account/controller capacity;
3. allocate physical storage;
4. publish metadata and ownership;
5. on failure, undo the acquisition before returning;
6. on Free, deallocate and release exactly once.

For growth, old and replacement allocations are both charged until the copy
succeeds and the old allocation is physically freed. Views borrow storage and
do not charge again. Copies charge their destination allocation.

The query generation and CN aggregate are checked at the same controller
boundary. There is no operator-owned memory reservation token parallel to the
allocator lease.

## Lifecycle

Compile opens one generation for each local statement attempt, configures all
physical-plan owners, and records them for reverse-order cleanup. Parallel
scan/load workers created during `runOnce` join the same generation before
worker `Prepare` starts. Configuration is atomic: a failure clears newly
configured owners in reverse order.

At completion, the message board is drained, owners are cleared, and the
account is sealed. A valid terminal state requires zero live bytes and zero
live metadata. A late allocation or release mismatch is a lifecycle invariant,
not capacity pressure. Prepared statements and retries use new generations;
generation-bound state cannot survive Reset.

## Pressure protocol

Typed reasons keep control flow honest:

- memory capacity may reclaim, spill, reduce an unpublished input, or disable
  an optional optimization;
- sealed, suspended, owner/site mismatch, or allocator invariant is terminal;
- spill disk and spill FD rejection are resource-specific terminal results;
- minimum-unit pressure ends retries when no smaller valid input exists.

A retry requires monotonic evidence: fewer live bytes, a new spill epoch, a
smaller input unit, or an optional structure disabled. Publication and spill
I/O cannot be replayed merely because the same capacity error recurred.

Runtime filters are optional and degrade to PASS if their retained payload
cannot be admitted. Required join state never bypasses admission.

## Spill ownership

SpillEngine must receive the producer's live budget generation. Memory, disk,
and FD tokens describe different physical resources. One file owns one
growable disk token and one FD token; file handoff moves both. Close releases
them once. Recursive spill validates framing, schema, row conservation, file
metadata, and bounded queue progress.

## Relationship to SQL Resource Accounting

[`SQL Resource Accounting`](./00000000_sql_resource_accounting.md) observes and
persists statement resource facts. This RFC controls an allocation before it
happens.

| Concern | SQL Resource Accounting | This RFC |
| --- | --- | --- |
| Purpose | observation and diagnostics | admission and pressure control |
| Time | during/after execution | immediately before allocation |
| Unit | domain usage/peak | live physical retained capacity |
| Missing data | quality flag | storage is outside this controlled domain |
| Failure action | report | reclaim/spill/reduce/degrade/error |

The systems may cross-check terminal facts, but neither authorizes the other.

## Performance constraints

- no per-row account object;
- no per-allocation Go object in steady state;
- fixed-size provenance in allocator metadata;
- controller calls only when physical capacity changes;
- views and Reset reuse do not re-admit unchanged storage;
- spill tokens scale with open files, not records;
- no extra expression-account bookkeeping in hot predicates.

Benchmarks must compare unaccounted MPool behavior, accounted acquire/release,
vector growth/reuse, hash build/lookup, and spill scatter. Correctness
requires exact terminal zero; performance acceptance requires no material
regression outside measurement noise for unaccounted paths and no new
data-scaled Go allocation in accounted paths.

## Alternatives rejected

### Patch each multiplier

It cannot close both false-positive and false-negative estimate errors.

### Reserve an estimated amount, reconcile later

The estimate can reject before real allocation or under-count live overlap. It
also creates a second release owner beside MPool.

### Sample RSS or heap counters

Sampling is observational, process-wide, and too late to authorize a specific
allocation.

### Account only selected expression buffers

This reports false exactness while opaque library Go heap remains untracked.
The implementation therefore accounts retained copies, not partial expression
internals.

### Keep old and new production paths behind a switch

Two admission semantics double ownership states, tests, and failure modes. The
final implementation removes the old path instead.

## Rollout and validation

The merge unit is one PR with reviewable commits, but the final branch must
present a single production path. Local validation includes unit tests,
prepared/retry and parallel-clone lifecycle tests, cancellation/transfer tests,
spill resource tests, race tests, build, vet, and allocation benchmarks.

Remote benchmark workflows are not required for correctness convergence in
this implementation cycle.

## Completion invariant

The RFC is complete when every retained HashBuild/join allocation in scope has
one physical owner, one admission path, and one terminal release; estimated
hard memory reservations and activation gates are absent; runtime clones share
the statement generation; all pressure/resource types remain disjoint; and
independent review plus fresh local validation reports no blocker or major
regression.
