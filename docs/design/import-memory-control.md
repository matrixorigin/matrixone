# Bounded import memory and uploads

Owner: aptend. Implementation context:
[PR #31166](https://github.com/matrixorigin/matrixone/pull/31166).
This document describes the active import-memory contracts on `4.1.6-hotfix`
and the bounded writer pipeline built on them.

## Problem and scope

A completed import can leave reusable buffers and allocator-resident free pages.
The next import must reserve its working set before allocation, while allowing
encoding and upload to overlap. Repeated imports must preserve rows, indexes and
cleanup within the process memory limit.

Controlled admission requires a complete, bounded physical graph with a CSV
source executing at the origin CN. Prepared execution, unknown variable-length
expressions, JSONLINE, retaining/unbounded operators, partition wrappers,
remote scopes/receivers and unproven channel layouts use whole-statement legacy
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
physical pressure after release/recovery. If even an empty process cannot fit a
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
crossing-row storage, including `LOCAL` input.
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
Neither path enlarges source units or metadata/output slots.

1. Reserve complete W for actual payload, block limit, merge backing, encoding,
   compression, arenas, file-service storage and final metadata before queueing.
   A published job must finish without another W/D grant or producer progress.
2. Sorted jobs transfer merged input, private pool and W to SinkPool, invalidating
   the submitter's handle. Failed publication destroys those owners locally.
3. Once Sink has consumed and destroyed source vectors, shrink only their part
   of W. Keep every remaining buffer and its effective pool owner alive through
   Sync and Close; an upload worker cannot borrow an uncharged merge output.
4. Collect successful object stats into the existing metadata spool. Transfer
   output into reserved I or an existing D owner before releasing its W.

A borrowed write clones/sorts/encodes under its caller's W lease. It may detach
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

## Build, spill and ordinary execution

Controlled HashBuild reserves backing and hash-map growth before allocation.
Key-expression bounds include live intermediates/results, selection, iterators
and resize peaks. Scatter, spill frames and controlled HashJoin output have byte
bounds. Unknown expression contracts use the legacy path. Ordinary shuffle
builds retain their existing post-copy row/byte spill threshold; uncontrolled
broadcast builds retain their existing in-memory path.

Local broadcast spill seals actual participants, including empty probes, using
stable scope/parallel identities. One finalizer receives build/probe files
atomically; claim and abort establish exactly one terminal owner. Deep skew uses
bounded build chunks, probe replay and sidecar match state while preserving join
semantics. File IO/close runs outside the group lock; independent readers use
ReadAt/SectionReader rather than shared Seek. No file descriptor crosses a CN.

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
cgroup users can consume headroom. Skew replay can repeat probe IO. These are
limits of bounded execution. Raw measurements, manifests and review records stay
outside source; results and tested revisions belong in the PR body or linked
evidence.
