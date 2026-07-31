# Allocation-accounted HashBuild memory admission

Issue: #26459

## Goal

Within the controlled domain, HashBuild and its join consumers must admit the
bytes they physically retain, not a predicted multiple of logical payload size.
A query may spill or degrade an optional optimization when the retained
allocation cannot be admitted, but must not fail because an estimator guessed
a larger, non-existent allocation.

The implementation has one production path:

1. the MPool performs the physical allocation;
2. the allocation carries immutable account, owner, and site provenance;
3. the account charges the query generation and the CN aggregate controller;
4. the same allocation releases the charge when MPool frees it.

There is no feature switch, activation gate, estimated-memory reservation, or
parallel compatibility ledger. Cardinality estimates may still select a hash
table capacity, but they never create a separately releasable memory charge.

## Scope

The account covers retained physical storage owned by the HashBuild execution
family:

- hash table cells, descriptors, iterator keys, and selection lists;
- copied build batches and retained unique keys;
- JoinMap-owned batches and grouping metadata;
- join matched/capture/result state;
- Product result state;
- runtime-filter payloads until ownership transfer;
- spill encode/decode/scatter buffers and rebuilt retained state.

ExpressionExecutor results, caches, and library-internal Go heap objects are
not HashBuild-retained storage. They remain in the existing MPool/Go runtime
domain. When an expression result is copied into a retained batch, key, or
result vector, that destination allocation is charged. This boundary avoids a
misleading partial "exact expression memory" account for regexp, JSON, JQ, and
other libraries that do not expose allocator/free hooks.

The account is therefore not advertised as total query RSS. General transient
expression admission is a separate problem and must not be represented by an
estimated charge inside this exact, terminal-zero ledger.

ProductL2 scratch and native CPU/GPU index storage are also outside this first
controlled domain. ProductL2 still consumes an accounted JoinMap: those source
allocations keep their original provenance through transfer and are released
by JoinMap `Free`. Its additional search index and scratch remain under the
existing implementation until their CPU and GPU allocators expose one common
physical capacity contract. Adding only the visible Go buffers would claim
false exactness and create a partial second path.

## Ownership model

Each physical allocation has one owner and one release path. Provenance is
attached before the first owned allocation and cannot change while storage is
live. Views borrow storage and do not create another charge. Copies allocate
new storage in their destination account.

The main transfer boundaries are:

| Storage | Initial owner | Transfer | Terminal release |
| --- | --- | --- | --- |
| retained build batch | HashmapBuilder | JoinMap | JoinMap `Free` |
| hash cells/descriptors | HashmapBuilder | JoinMap | JoinMap `Free` |
| grouping selections | HashmapBuilder | JoinMap | JoinMap `Free` |
| spill file + disk/FD tokens | HashBuild | SpillBuildPayload | SpillEngine/file close |
| matched bitmap | parallel worker | BitmapMailbox/merger | merger or mailbox drain |
| Product build batches | producer JoinMap | Product | Product reset/free |
| runtime-filter payload | HashBuild | message board | message destruction |

Transfers are move-only. A successful send clears the sender's ownership; a
failed send leaves ownership with the sender. Cancellation seals mailboxes and
drains queued accounted objects before terminal validation.

## Execution lifecycle

Compile opens one allocation generation for each local statement attempt.
Every operator implementing `SetAllocationAccount` / `ClearAllocationAccount`
is an owner in that generation.

The sequence is:

1. collect owners from physical scope templates;
2. open the account with the live HashBuild capacity controller;
3. configure all owners atomically, rolling back in reverse order on failure;
4. attach parallel scan/load clones created during `runOnce` to the same
   generation before worker `Prepare` runs;
5. execute and drain the message board;
6. clear owners in reverse order;
7. seal and finalize the account;
8. export exactly one terminal snapshot.

Prepared statements and retries create a new generation. Reset frees all
generation-bound state; it does not carry an executor, bitmap, mailbox payload,
or allocation selection into the next attempt. Runtime parallel clones are
also registered as owners and are cleared before their reuse-pool release.

A valid terminal snapshot requires zero live bytes and zero live allocation
metadata. A mismatch suspends new admission until late physical frees drain the
tombstone; it is never converted into retryable capacity pressure.

## Capacity and pressure

The controller enforces both the query-generation cap and the CN aggregate
cap. Admission uses checked arithmetic and charges the physical MPool capacity
requested by the allocator.

Pressure reasons are typed and disjoint:

- memory capacity: reclaim, spill, reduce an unpublished input unit, or degrade
  an optional runtime filter;
- account sealed/suspended: terminal lifecycle error;
- owner/site mismatch or allocator invariant: terminal correctness error;
- spill disk cap: spill-resource error, never a memory-reduction retry;
- spill FD cap: spill-resource error, never a memory-reduction retry;
- minimum input unit: terminal capacity error after monotonic progress is no
  longer possible.

Retry is allowed only when progress is observable: retained bytes decrease,
spill epoch advances, input units shrink, or optional state is disabled. This
prevents a capacity loop from replaying the same publication or I/O.

Runtime filters are optional. If their retained payload cannot be admitted,
HashBuild publishes PASS and releases unpublished scratch. Required hash/join
state does not silently bypass admission.

## Spill resources

SpillEngine requires the same live budget generation as its producer. There is
no nil-budget file path.

Memory, disk bytes, and open file descriptors are separate physical resources:

- memory is charged by MPool allocations;
- each spill file owns one growable disk token;
- each open spill file owns one FD token;
- a file handoff moves both tokens with the file;
- close releases all three exactly once.

Recursive spill validates schema, framing, row conservation, queue bounds, and
file metadata. Repartitioning keeps only bounded control arrays plus admitted
scatter buffers. Test fixtures use the same builder copy and budget paths as
production.

## Grouping semantics

Grouping sentinels are a distinct key domain from ordinary zero/empty values.
HashBuild selects grouping-aware key encoding whenever input contains grouping
bits. Hashing preserves this distinction for partitioning, equality preserves
it in resident maps, copies preserve the bitmap, and ordering treats the
sentinel as SQL NULL for NULLS FIRST/LAST behavior.

Sample keeps its ordinary batched hash path unchanged. If grouping bits appear
after ordinary groups were already inserted, it lazily opens a grouping-aware
key domain and translates both maps' local IDs into one sample-pool ID space.
Its iterators are reused per batch, so alternating grouping bits do not create
one iterator allocation per row.

## Non-goals

- estimating or limiting total query RSS;
- charging regexp/JQ/JSON/library-internal Go heap as if it were exact;
- changing optimizer join selection;
- making spill as fast as a sufficient-memory in-memory join;
- using remote benchmark workflows as a correctness oracle.

## Completion criteria

The implementation is complete only when:

- production has no estimated HashBuild memory reservation or activation gate;
- every retained HashBuild/join allocation in the controlled domain has
  immutable provenance;
- runtime parallel clones join the current attempt before `Prepare`;
- every transfer has exactly one owner after success and on cancellation;
- memory, disk, and FD rejection remain distinct;
- prepared/retry generations terminate independently at zero;
- local unit, race, build, vet, lifecycle, spill, and performance checks pass;
- independent reviews report no blocker or major correctness/performance issue.
