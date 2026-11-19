# Bounded import memory and uploads

Owner: aptend. Related issue: [#26992](https://github.com/matrixorigin/matrixone/issues/26992). Implementation context:
[PR #31166](https://github.com/matrixorigin/matrixone/pull/31166).
This document describes the combined port of
[PR #31166](https://github.com/matrixorigin/matrixone/pull/31166) and
[PR #31810](https://github.com/matrixorigin/matrixone/pull/31810) to `4.2-dev`.
The port retains 4.2's PipelineEdge terminal protocol, allocation-account
provenance, hash execution, spill engine, and native mmap allocator/cache.

## Problem and scope

A completed import can leave reusable buffers and allocator-resident free pages.
The next import must reserve its working set before allocation, while allowing
encoding and upload to overlap. Repeated imports must preserve rows, indexes and
cleanup within the process memory limit.

Controlled admission requires a complete, bounded physical graph with a CSV
source executing at the origin CN. Prepared execution, unknown variable-length
expressions, JSONLINE, retaining/unbounded operators, partition wrappers,
remote scopes/receivers, geometry normalization, tolerant/error-row sources and unproven channel layouts use whole-statement legacy
fallback before ticket allocation. Local nested compilation may borrow a process
view; it must not replace or close the parent's root. The local manifest adds
no wire contract or independent admission of partial remote graphs.

## Admission and physical accounting

One process coordinator manages allocated capacity, including Go and native
buffers, through three commitments:

| Commitment | Owner and release |
| --- | --- |
| Input/output (I) | Complete graph input, output and producer scratch slots; charged until consumption or destruction. |
| Retained data (D) | Private writer/build allocations; reserved before growth and retired by flush/spill or destruction. |
| IO workspace (W) | Shared byte budget for encoding and upload; each job owns its complete peak before publication. |

Admission atomically reserves graph I and any increase in the shared W maximum.
The sum of live W leases stays within that shared promise. Pool grants and import
admission serialize at the same physical-accounting owner, including pools used
without an import ticket. Otherwise independently valid grants can overcommit
memory before their allocations appear in RSS.

Reserve append, copy, resize and serialization peaks before mutation, including
old and new backing simultaneously; reconcile to retained capacity afterwards.
Partial failure and cross-pool frees retain exactly one effective owner. Returning
a grant does not imply that the allocator returned resident pages to the OS.

Physical pressure is `max(RSS, max(0, cgroup.current - inactive_file))`, with
outstanding commitments included and no subtraction of unproven resident
coverage. Missing samples preserve known container limits. Resolve Linux cgroup
membership relative to the exposed controller mount root. Shared idle-arena
budgets cover cached capacity across size tiers; checked-out arenas belong to
jobs. Recovery yields headroom only after a fresh physical sample. Native
trimming is best effort on Linux/glibc; online limit resizing and other libc
platforms are outside this change.
Existing admission metrics expose I/D/W and sampled physical pressure; promises,
RSS/cgroup usage and cumulative profile allocations are distinct measurements.

Build descending legal unit candidates from the existing throttler target,
merging only identical complete I/W envelopes. Recheck shared commitments and
physical pressure after release/recovery. An already admitted graph may claim
physical D before a later admission waiter, within the same physical bound:
that waiter may depend on the graph completing and releasing its root. FIFO
applies between new admissions. If even an empty process cannot fit a
parallel target, recompile a smaller DOP before permanently reducing the unit.
At final DOP, wait cancellably for transient pressure. Explicit or uncertain
transactions use immediate admission to avoid waiting on their own prior work.
Release notifications drive FIFO progress; coalesced one-second sampling runs
only while waiters exist.

Closing the statement root rejects new claims and wakes acquisition waiters.
Existing leases finish under their owners. Retire promises after root closure
and the final input/IO owner; derive owner counts from these authoritative owners
rather than a duplicate total counter.

## CSV records and conversion

Raw records and typed batches share an immutable admitted source unit and row
budget. Account separately for parser scratch, sparse retained fields, headers,
variable payload, conversion scratch and backing growth, without double counting
inline values. Preserve configured record limits, stale-file bounds and pending
crossing-row storage, including `LOCAL` input. The physical schema follows 4.2: parallel reads
keep ordinary columns as VARCHAR and directly decode supported vector columns.
Reserve record capacity through the candidate batch unit, between the read-block
minimum and legal record maximum; shrink the row budget with that unit.

Precompute geometric record budgets for the selected unit. Before a wider record
exceeds its reservation, grow the complete graph's I/W envelope atomically and
monotonically. Growth succeeds immediately or fails before record/conversion
allocation; it never queues behind downstream resources held by that reader.
Cancellation, denial or close publishes no partial increase. Active IO jobs keep
their original capacity; later jobs can use the larger promise. File size may
tighten a legal bound but must not force every short row to reserve the maximum.

Within one CSV fill or merge output, reuse the backing-capacity frontier until
growth, replacement or oversized-buffer cleanup. Keep exact bitmap forecasts and
calculate single-row variable payload once. Writer window selection grows its
search prefix geometrically instead of rescanning the remaining batch.

Array literals count dimensions and scan token views instead of copying the
whole normalized string and allocating a split slice. Preserve the existing
ASCII-space rules, dimension-error precedence, float32/float64 conversion, exact
result bits and error text. The caller owns the result; no cache or shared
parser state is introduced.

## Writer batching and upload ownership

The writer target is separate from source framing. Choose the largest target
whose complete IO envelope fits W, starting from the existing throttler target
and never below the source unit. Cap rows using the source-unit row bound and
minimum flat row width so metadata frames still fit. Charge all index
projections against the target and acquire actual retained backing from D.
Bounds also include bitmaps, old/new vectors, delete preparation, dedup state
and output metadata; committed-delete work stays charged until materialized or
destroyed. Preserve LOAD FAIL/IGNORE, duplicate filtering and final-row UPDATE
index semantics. DOP recompilation must not mutate caller routing labels.

Eligible ordinary CSV writers use the existing bounded SinkPool; other writers
keep their existing pipeline policy. Lack of optional D buffering flushes owned
sources or uses the admitted borrowed-input path instead of failing the import.
Neither path enlarges source units or the per-object metadata frame bound.

1. Reserve complete W for actual payload, block limit, merge backing, encoding,
   compression, arenas, file-service storage and final metadata before queueing.
   A published job must finish without another W/D grant or producer progress.
2. Sorted jobs transfer merged input, private pool and W to SinkPool, invalidating
   the submitter's handle. Failed publication destroys those owners locally.
3. Once Sink has consumed and destroyed source vectors, shrink only their part
   of W. Keep every remaining buffer and its effective pool owner alive through
   Sync and Close; an upload worker cannot borrow an uncharged merge output.
4. Collect successful object stats into retained transaction metadata. Transfer
   output into reserved I or an existing D owner before releasing its W.

A borrowed write clones/sorts/encodes under its caller's W lease. Its private
copy does not inherit the source allocation account; the complete W lease owns
that copy while the source retains its original provenance. It may detach
its encoded sinker only after a separate nonblocking W claim covers the full
remaining upload peak. If that claim fails, Sync completes under the original
lease. Never wait for another lease while retaining W.

After a borrowed write, release the caller's W before pulling more input and
collect only already completed stats. Ready uploads own no borrowed source and
continue independently. An empty D writer must not drain them just to retry D.
Pressure that must retire actual D sources, final Sync, error/cancellation and
Reset/Free join pending jobs before returning their owners.

For admitted INSERT-only MultiUpdate, create persistent per-projection sinkers
under the stable operator pool before entering a borrowed window's temporary W
pool. Each detached upload owns a private pool and complete W. Destroy window
scratch before releasing parent W; restore the operator pool before failure
cleanup. Persistent sinkers must never retain the temporary pool. Updates,
deletes and unadmitted writers keep their existing retirement policy.

Empty reusable source buffers retain their D grant until destruction. Completed
stats are collected at subsequent boundaries; residual work is bounded by the
existing queues/workers and W. Insert and INSERT-only MultiUpdate materialize one
complete object's metadata per frame in their output pool; a completed-object
backlog never enlarges the frame bound. Async counters stay separate from
per-call analyzer resets and are reported after uploads join. Transaction output
becomes visible only after upload success.

Serial upload boundaries prevent overlap. Reserving a worst-case W maximum per
worker wastes capacity or overcommits it. The selected mechanism charges actual
peaks within one shared maximum and reuses the existing coordinator, envelopes
and SinkPool, without a new worker, timer, configuration or workload threshold.

## Transaction metadata ownership

Admitted INSERT and insert-only MultiUpdate keep compact object/block metadata
in operator memory until Build completes, then emit the existing bounded
MergeBlock/MultiUpdate protocol during Eval. INSERT retains ObjectStats and
expands one object at a time; insert-only MultiUpdate retains its existing
five-column output. Object payloads remain in object storage.

In the representative replay, each 39-table round generated about 2,000 objects and
14,000 blocks including indexes, or approximately 3 MB of encoded metadata
across the statements. That is an encoding estimate, not a measured simultaneous
peak. It does not describe uncommitted-delete output, which contains raw RowIDs
and primary keys proportional to the deleted rows. There is no evidence that
compact metadata required file storage for this load.
The memory fast path avoids temporary IO for this ordinary workload; this
sample is not a capacity bound for arbitrarily many objects or raw delete keys.

For admitted imports, retained metadata and the current output share the existing
output input-slot lease. Before vector backing grows, extend that lease through WorkingSet's
nonblocking growth admission using the existing four-copy metadata allowance.
This charges cumulative capacity, preserves the fixed per-frame protocol limit,
and adds no workload threshold, configuration, queue owner or worker. Capacity
denial switches to bounded temporary storage instead of waiting behind retained
output. INSERT streams fixed ObjectStats records; insert-only MultiUpdate
migrates one already-encoded row at a time into the existing frame spool. Its old
four-copy claim covers retained input plus old/new one-row backing, and direct
serialization adds no whole-frame encoding copy. The old vectors are retired
only after migration succeeds. Cancellation, closed owners and malformed frames
remain errors rather than triggering fallback. Eval can switch after partial
consumption and resumes at the unread object/row; it never replays emitted rows.
A single frame must still fit its admitted envelope.
Legacy and delete-capable MultiUpdate preserve the existing per-frame temporary
storage and delete-before-insert replay. Retire each encoded frame before the
next window, so raw-key output need not fit in D for the whole statement.
Eval acquires only the current decoded frame from D after retiring the previous
frame. It holds that claim until the consumer advances or cleanup destroys it.
At partition Build EOF, finalize all legacy writers to their existing frame
storage and retire their source/future D before replaying any partition. A later
partition cannot be expected to release D while the current partition waits.
Independent owners retain their cancellable release/wait behavior.
EOF preflush keeps unhanded objects under the existing writer/metadata owner.
Object-stat frames carry compact cleanup names in their private temporary-file
trailer; raw-delete frames do not. Frame creation transfers ownership from
block-info to the file, and complete replay transfers it to the existing
transaction consumer. Failed replay cannot advance that watermark. Free/Reset
remove unread objects without materializing raw-key payloads, using a bounded
cleanup context independent of query cancellation, with one deadline shared by
all partition writers. Partially successful uploads remain in their existing
sinker until discarded or handed off. A fully drained
writer skips the cleanup scan. This closes the added EOF preflush window; it
does not introduce a second transaction-object registry or change persistence.
Metadata files use the configured LOCAL spill service rather than OS TMPDIR.
Existing query/CN spill-disk and FD reservations cover both INSERT
and MultiUpdate files; each full append is admitted before writing, and file
close retires its reservations. Transaction-local delete-key deduplication uses
the same owner: admit its bucket table before sizing the file, then admit each
distinct RowID record before appending; duplicate lookups spend no new storage.
Reset/Free closes all partition/table files and returns both byte and FD claims.
This reuses the native spill policy and adds no new setting.
Known-autocommit writers retain cancellable D acquisition and release notifications;
explicit transactions retain 4.2's nonblocking guard against their own workspace/locks.
There is no new cumulative D-memory claim or blanket rejection while output is
pending. Reset/Free close the unlinked frame file, including partial writes and
cancelled replay. Admitted imports open temporary storage only after capacity
denial; the same temporary-volume requirements and IO-error behavior apply.

Eval retires each metadata batch after it is fully consumed. Error, cancellation,
Reset and Free destroy remaining vectors before releasing the output lease. Verify multiple objects,
delete-before-insert order, growth denial, unchanged prior output and reuse.

## Publication object buffers

ApplyObjects resolves the existing CN coordinator from Engine.GetService at
its common entry, so direct callers and ExecuteIteration share the same owner.
Explicitly supplied owners remain effective; a missing owner still fails before
source allocation. Service test fixtures install the same real coordinator.

Every FilterObject attempt reserves its source-buffer peak through the CN memory
coordinator before fetching data, for ordinary data, appendable and tombstone
objects alike. The promise covers class-rounded final storage, returned chunks
and driver decoding backing. Fixed-name tombstone rewriting starts with the existing
IO envelope as a conservative source-buffer floor. The object-format size ceiling is unchanged. Empty or
initially unknown sizes start with a nonzero input slot and grow from GETOBJECT
metadata before allocating the source buffer.

The root attempt retains that owner through decoding/copying and the final
fileservice write; returning from fetch does not return the credit. If the
reported size needs more memory and nonblocking growth fails, retire every
resource of that attempt before waiting for a larger reservation and refetching.
Capacity denial alone takes this path; cancellation and closed owners propagate.
Failed submission and cancellation join accepted jobs before root retirement.
No workload threshold, extra queue or configuration is introduced.

The protected Apply phase owns one expiration timer and cancellation context.
The worker returns the authoritative TTL and its change notification under one
lock. Registration updates, renewal replies and removal wake the observer, so
earlier and later deadlines both rearm the timer without a lost notification.
A reply from a removed registration cannot change a replacement generation.
Expiry cancels queued admission and IO.
This uses no polling or per-object timer. Parent cancellation and phase exit
stop and join the observer, while existing job retirement retains live buffers
until consumers exit. Preserve both the primary failure and protection cause.

## Build, spill and ordinary execution

HashBuild and HashJoin retain 4.2's allocation-owned admission and its existing
query/CN memory, spill-disk and file-descriptor policies. The import does not
restore 4.1's broadcast spill registry, skew executor, key estimator admission,
or post-allocation accounting. Ordinary query ceilings and spill policy stay
with their existing owners.

Open import admission before the execution allocation account. A controlled
HashBuild generation captures the admitted WorkingSet before publication.
Every physical allocation and recovery-capacity increase first acquires D from
that WorkingSet without waiting, then applies the existing hash budget policy.
If the latter rejects, return the complete D claim. The existing physical
allocation/recovery destructor releases both charges exactly once, including
cross-pool Free and release after root closure. No additional reservation token
or estimator owns those bytes. Uncontrolled generations retain the 4.2 path.

CSV and writer I/W commitments serialize with those D acquisitions at the same
coordinator. Controlled join output uses the admitted byte unit; its allocation
provenance and dependency/terminal semantics remain those of 4.2. Unknown
expression contracts, MARK joins and unsupported execution layouts take whole
statement fallback before admission.

Keeping only cache limits cannot bound live input/writer peaks. Copying the
4.1 hash/spill engine would replace 4.2's ownership and semantic fixes. Integrating
the existing allocation owner with the shared coordinator preserves both and
provides a single physical admission boundary.

Reject graphs without a controlled CSV source before expression/producer
analysis. Build the producer proof once over immutable local expressions rather
than serializing trees for each candidate. Normal bytes.Buffer encoding keeps
its direct scalar writes; bounded spill uses streaming encoding with the same
persistent format. No spill thresholds, catalog/object formats, authentication
or tenant boundaries change.

## Completion, cancellation and reuse

Writer cleanup destroys active and retired projections after partial
initialization and on legacy-fallback paths as well as controlled execution.
Queued acquisition wakes on cancellation/close. Submitted jobs own their cleanup;
failed merge, publication, Sink or Sync reaches one destructor for every source,
arena, pool and lease. Shrink, transfer and release serialize on the lease
handle. Cancel siblings before joining them and join generation N before reuse
in generation N+1. Network errors retain the statement's existing error policy.

PipelineEdge's typed terminal is authoritative. Reset publishes End/Error
independently of data-channel capacity; Call does not publish terminal signals.
Preserve primary errors and consumer Stop settlement. Failed publication discards
only unsent ownership. Register receivers/spools immediately after construction,
so a later Prepare failure cannot strand a saved current batch.

After local scopes, notifications and cleanup join, the attempt owner releases
saved currents on every BaseProcess, drains edges including directly accepted
frames while retaining terminal causes, performs final spool-slot cleanup and
clears registries, then restores process views and closes the working-set root.
Cache cleanup and final slot cleanup remain separate and idempotent. Active
borrowers remain valid until release or proven quiescence. Admission includes
abort bookkeeping, receiver/channel capacity, registry arrays and final graph
metadata; unsupported final layouts fall back before admission.

## Validation and limits

Use deterministic minimal fixtures for allocation/growth denial, FIFO and lease
transfer/shrink/close, partial initialization and exactly-once retirement. Block
real FS uploads with barriers to prove that the next input advances, parent W is
released and metadata stays private until success. Cover zero-D and ordinary-D
paths, all projections, cancellation, Reset/reuse and the synchronous fallback
when a second upload claim cannot fit. Preserve public LOAD/index/DOP and legacy
fallback controls, cross-pool exclusion, encoding compatibility and bitwise/error
array-parser differential checks. Run owning packages, affected consumers and
incremental static checks; use race/stress for changed lifecycle contracts.

Workload acceptance uses the original inputs, hashes, schema/indexes and order:
five continuous local rounds at 22 GiB/no swap without retries, restarts, forced
GC or inter-round pauses, with row/index/temp-table and I/D/W retirement checks.
Compare four-round TKE total import time under the same frozen 2-CN,
20-GiB/10-CPU configuration, excluding DDL/preparation time. Check IO/profile and
memory evidence alongside timing; local speedup alone is insufficient.

An irreducible row/minimum working set can exceed the memory limit, and unrelated
cgroup users can consume headroom. Hash partition recovery can repeat probe IO. These are
limits of bounded execution. Raw measurements, manifests and review records stay
outside source; results and tested revisions belong in the PR body or linked
evidence.

## Persisted block layout and recovery retirement

Persisted objects require every non-final block to contain `BlockMaxRows` rows. Byte windows bound source admission and independently publishable runs, not the blocks emitted inside a sorted object. Sorted and borrowed output therefore packs full row blocks; an unsorted writer finishes an object before staging another block after a partial one. The IO claim covers the complete run's materialized payload, including constant/aliased expansion. Legacy single-object callers retain the row-block memory unit; this does not add a new byte cap to those unadmitted paths.

A fallback worker keeps generation ownership through recovery, timer cleanup and actual retirement. Closing its stop channel only requests retirement. `workerDone` prevents replacement while recovery is blocked; under the coordinator mutex, retirement closes that completion and starts a replacement only when requests remain queued. Request cancellation removes its waiter without joining recovery. This reuses the existing worker and channels; no new timer, pool or setting is introduced.
