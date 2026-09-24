# Unpublished CN S3 object ownership and remote handoff

Issue: [#29257](https://github.com/matrixorigin/matrixone/issues/29257).
Implementation: [#29290](https://github.com/matrixorigin/matrixone/pull/29290).
Status: **proposed for independent design review; not approved**.

This document records the design embodied by an already-open implementation PR.
It is retrospective and does not establish that the earlier commits passed a
design-first gate. Link the exact reviewed document revision and an independent
design decision in the PR before treating that gate as satisfied. A material
change to the ownership or wire contract needs a new design revision.

## Problem and acceptance boundary

CN writers can persist an S3 object before its metadata enters `txn.writes`.
`SyncAndTakeResults` can detach the only stats from the sinker, and `Sinker.Close`
discards remaining persisted/unpublished names without deleting them. A later
serialization, forwarding, or workspace-append error then leaves an object that
ordinary rollback GC cannot find. The same boundary occurs in Insert,
MultiUpdate, remote DELETE, workspace dumps/rewrite, clone readers, and tombstone
transfer. It is not a request to make every S3 object globally discoverable.

The registration boundary is the successful append of the corresponding
metadata entry to the transaction workspace. Before that append, exactly one
effective cleanup responsibility must remain; after it, normal transaction
rollback/GC owns the object. Duplicate name-only owners may exist during
handoff, but the transaction deduplicates by object name and never lets a later
owner delete a name already accepted by the first owner.

Required invariants:

1. A failed Sync, metadata copy, or append cannot silently discard persisted
   names. Successful metadata copy alone is not registration.
2. A producer may release a completed writer's buffers only after a lightweight
   name owner has been retained by its workspace. A failed transfer leaves the
   writer responsible and retryable.
3. A remote worker may retire its owner only after the coordinator has retained
   each exported batch's names and sent an explicit receipt for every batch.
   An ordinary credit ACK, a missing ACK, or an empty stream is not proof.
4. Aborted, stopped, canceled, or failed streams keep the worker in cleanup
   mode. Accepted names are never deleted by the unpublished-object cleaner.
5. Failed deletion retains the exact writer/name owner for a later in-process
   retry and returns a cleanup error; it is not interpreted as success. A
   remote mirror whose handler has exited transfers that retry obligation to
   its CN server before the stream is removed. A local transaction does the
   same before terminal rollback retires its workspace.

## Ownership transitions

| Boundary | Before | Successful transition | Failure |
|---|---|---|---|
| Writer Sync / result extraction | Writer/sinker owns persisted names | Writer tracks detached names alongside sinker results | Writer keeps both sets and abort cleanup deletes them |
| Metadata copied to producer output | Writer owns names | Workspace retains a copied name-only owner; writer may reset/close | Writer remains cleanup-pending and cannot close as though published |
| Local workspace append | Workspace name owner | Append accepts only names in that entry; transaction rollback/GC takes over | Unaccepted names remain for workspace cleanup |
| Remote output decode | Worker workspace owns names | Coordinator parses and retains names before forwarding/discarding output | No ownership receipt; worker cleans on terminal failure |
| Batch ACK | Worker and coordinator have temporary overlapping responsibility | Coordinator sends `batch_ack_s3_ownership_retained=true` only after retention; worker records contiguous receipts | Missing/unmarked receipt cannot retire worker ownership |
| Remote terminal drain | Worker still owns unproven names | All sent batches have contiguous receipts, no pending credits, no abort/stop; worker accepts its owners and completes | Worker cleans; cleanup error is returned with the original execution error |

`CNS3Writer.TransferPersistedObjects` reads results already produced by Sync; it
does not Sync again. Persistent Insert/MultiUpdate/remote DELETE producers call
it only after their output metadata has been copied. One-shot MultiUpdate
writers close after successful Sync and copy; only object names survive until
workspace append. A writer whose deletion fails remains open for retry, so an
outage can temporarily retain its buffers. Normal successful spills do not.

The transaction workspace keeps both name owners and fallback cleanup
callbacks for writers/flows whose immediate cleanup failed. It detaches the
ledger while performing file-service I/O, then reattaches failed entries for
the next lifecycle callback. The name index prevents duplicate owners from
deleting an accepted object. Cleanup is independent of operator reuse. If the
remote terminal cleanup fails, the handler detaches the workspace's cleanup
records into a callback and registers it with the owning CN server before
returning its error. The callback does not capture the transaction itself.
One CN worker
retries failed callbacks after the stream has exited; a successful callback
releases the reference, including the consumed slot in the queue backing array.
The server stops transaction producers before closing cleanup admission, joins
the worker, and retries tasks in rounds within one cleanup deadline before CN
I/O dependencies close. Individual attempts have a thirty-second bound. A
failed task does not prevent healthy tasks from being attempted in that round.
If the total deadline expires, CN shutdown remains fail-stop and preserves its
dependencies, but the cleanup worker resumes so later storage recovery can
still delete the objects. The CN-close error remains visible; the existing
one-shot service shutdown contract does not automatically resume its tail.

Terminal local rollback transfers detached unpublished cleanup records to
that CN worker before `delTransaction`, because frontend rollback invalidates
the transaction handle even when cleanup reports an error. A clone reader's
retained callback uses the current retry-attempt context rather than the
expired request context captured when the reader was created.

## Remote receipt protocol and ordering

The existing batch-credit ACK has a cumulative sequence and may be sent by an
older coordinator without retaining S3 names. Field 19 of `pipeline.Message`
adds an optional boolean `batch_ack_s3_ownership_retained`. Absence decodes to
false. It is a per-batch ownership receipt, not a new credit or a claim that
the transaction append succeeded.

The coordinator classifies the actual producer output, including grouped
`PreScopes`, ordinary `UpdateWriteS3` where coordinator `IsRemote` is false,
Insert S3 output, and remote DELETE tombstone metadata. Traversal stops at
operators that have already consumed/registered the metadata. For a classified
batch, it parses names and retains them in the coordinator's transaction
workspace **before** forwarding and ACKing. A parse/retention failure sends no
ownership receipt and fails the stream. A no-output stream rejects unexpected
S3 output rather than acknowledging it as a successful handoff.

The worker tracks two frontiers under the batch-flow mutex: cumulative
`ackedSeq` frees flow-control credits, while contiguous `ownershipAckedSeq`
proves explicit receipts. Only the latter can authorize S3 lease release. CN
ingress handles ACKs synchronously in the per-session receive loop, preserving
wire order before the next ACK handler runs; non-ACK pipeline work remains
asynchronous. Without this serialization, ACK 2 could consume cumulative
credits before ACK 1 updates the ownership frontier. The worker's finalization
predicate requires actual output (`nextSeq > 0`), every receipt, no pending
credits, and no abort or receiver StopSending. An invalid ACK closes the
connection; late ACKs after lifecycle removal or abort do not create ownership.

The negotiated credit window is at most eight batches and 64 MiB of pending
wire payload; a single oversized batch can occupy an otherwise empty window.
ACK processing adds constant state, no receipt map, blocked ACK goroutines, or
new wait barrier. Object-name ownership is **not** bounded by that credit
window: it grows with the unpublished objects in a transaction and is released
by registration or cleanup. S3 deletion outages can prolong writer retention.
The CN retry queue holds one callback per failed stream or local rollback.
Each callback owns the remaining name owners and fallback cleanup callbacks,
not a bound method holding the entire transaction. Fallback writer cleanup can
still retain writer buffers and references until deletion succeeds. Memory
therefore grows with concurrent failed cleanups during a storage outage; this
is an explicit limit
of the in-memory repair, not a fixed-capacity queue that could drop ownership.
The single retry worker bounds retry concurrency.

## Failure, rollout, and limits

- A lost/failed ACK, connection loss, invalid sequence, cancellation, or
  StopSending cannot prove takeover. The worker keeps its owner and attempts
  cleanup; the coordinator's overlapping owner is reduced only if its local
  transaction actually appends the corresponding metadata. Cleanup retries are
  in-process, not a distributed durable handoff log.
- Pipeline teardown starts one execution-scoped cleanup deadline before Reset
  or Free. All pipeline contexts share the budget without changing execution
  cancellation; nested writer, MultiUpdate, transfer and remote finalizer
  cleanup preserve it instead of starting another ten-minute timer per writer.
  The first pipeline entering teardown starts the clock; later pipelines may
  defer expired cleanup to the CN queue even if their own execution ran longer.
  Prepared execution/query retry creates a fresh budget. Failed Reset transfers
  cleanup to the workspace immediately, including Insert, Delete, MultiUpdate,
  partition writers and clone readers, since prepared execution skips Free.
  The CN
  retry worker uses a fresh bounded attempt after the handler exits. A failed
  S3 delete is returned and its in-memory owner stays queued; a process crash
  or persistent storage outage can still leave an orphan. Durable recovery is
  a separate design and is not claimed here. The budget bounds context-aware
  object deletion, not an uncooperative file service or arbitrary CPU teardown.
- A new worker with an old coordinator sees an unmarked ACK and fails closed
  rather than releasing its owner. An old worker with a new coordinator still
  has its old pre-receipt gap; this PR cannot retrofit it. A mixed-version
  cluster therefore does not have the full guarantee. Roll out coordinators
  before workers, drain old workers before claiming the guarantee, and do not
  treat wire-field tolerance as safe ownership interoperability.
- Retaining before downstream append can temporarily duplicate name ledgers
  on worker and coordinator. The object itself is not copied. The extra ACK
  field is emitted only when true; normal non-S3 streams keep credit behavior.
  No new global registry, background sweeper, or S3 listing is introduced.

Alternatives rejected: accepting any cumulative ACK as takeover (old
coordinator can ACK without retention); retaining every completed writer until
transaction end (unaccounted sinker memory across repeated spills); and a
receipt map or waiting ACK goroutines (additional unbounded/terminal-wakeup
state when per-session ingress order can be preserved).

## Validation and review gate

Deterministic regressions cover post-persistence/pre-append failures for local
producers and transaction callers, repeated spills and MPool release,
coordinator classification/retention for connector and dispatch output,
wire-field round trip and absent-field compatibility, marked/unmarked/gapped
ACKs, ACK ingress ordering, no-output and StopSending behavior, accepted versus
unaccepted names, and failed cleanup retries after stream exit. Deadline tests
check that nested cleanup preserves the outer bound. Focused race tests cover the
ownership and ingress paths. The PR's CI runs UT, coverage and multi-CN BVT;
passing them does not prove every mixed-version/cancel combination.

Design review must explicitly decide whether the mixed-version fail-closed
behavior and in-memory-only deletion retry are acceptable for this bug fix,
whether an unbounded outage backlog requires a separate durable-recovery or
backpressure design before rollout, and verify that no terminal path accepts
ownership without all receipts.
Until an independent review links an exact document revision with a decision,
the design gate identified in PR review remains open.

## Proposed revision: bounded in-process cleanup debt

**Status: design proposal, not implemented or approved.** The implementation
above currently accepts an unbounded number of failed-cleanup callbacks. The
following revision is required before claiming a finite CN memory bound. The
proposal supersedes the current document's unbounded-queue limit, heavy-writer
fallback, and cleanup-error reporting; other ownership and receipt invariants
remain in force. The
product choice is to stop admitting new CN S3 uploads when cleanup debt reaches
its limit; an already-uploaded object must never be abandoned because a queue is
full. Read-only queries and writes that do not upload new S3 objects remain
eligible to run. The existing ten-minute cleanup deadline is a limit on an
attempt, not on the lifetime of an ownership obligation.

### Budget and admission point

The CN owns a fixed-capacity ledger of **65,536 object-name tickets**. A ticket
is reserved immediately before each CN file sinker's `Sync` can persist its
named object, including synchronous and pipelined sinkers. The reservation uses
the name already available from `FSinkerImpl.ActiveObjectName`; an unavailable
name or missing CN admission service fails closed before `Sync`. A canceled
reservation releases its ticket only if no write started. Once `Sync` starts,
both success and commit-ambiguous failure retain the ticket until either
metadata registration is confirmed or deletion succeeds. No failed-cleanup
queue append can be the first admission decision.

The bound covers object-name tickets, not object bytes on S3. Each retained
record must contain a validated bounded-length name, one file-service handle,
and a small fixed-size ownership state. Callbacks handed to the CN retry worker
must close/drain their sinkers and release `mpool` buffers first, then retain
only file-service/name records. The queue must not retain a transaction,
`CNS3Writer`, `TransferFlow`, reader, batch, or process. A callback with no
object-name tickets is not queued. The worker may group tickets per failed
stream or rollback, but its queue entry count cannot exceed the ticket count.
The 65,536 default is a concrete admission ceiling; benchmarked retained bytes
per ticket and operational headroom must be recorded before rollout. In-flight
writer buffers remain outside this ticket budget; their existing controls are
path-specific, and this proposal does not claim a global bound for them. The
ticket budget specifically covers debt that outlives those writers. Changing
the ceiling requires a separately reviewed configuration/capacity decision.

All production CN S3 upload paths use this admission hook: Insert, MultiUpdate,
remote DELETE, transaction dumps and rewrites, clone readers, and tombstone
transfer. The existing shared sinker remains usable by TN and other callers
without CN admission. CN constructors supply the service-specific hook; direct
`TransferFlow` sinker creation supplies it explicitly. A path with no hook must
not silently fall back to unmetered CN persistence. The name/ticket moves with
the first effective cleanup owner; duplicate ownership references share the
same ticket, rather than consuming or releasing it twice.

For a remote handoff, the worker retains its ticket while the coordinator
reserves its own ticket **before** recording each received name and emitting
field-19 ownership receipt. The two CNs may briefly each charge the same
object; the worker releases its charge only after contiguous receipts prove
takeover. If the coordinator has no capacity, it sends no ownership receipt,
fails the stream, and the worker remains responsible for deletion. An old
coordinator cannot issue a valid receipt. Admission is local to each CN; no
cross-CN shared counter or new wire field is required.

Tickets are released exactly once after a successful workspace append accepts
the corresponding object name, or after `DeleteUnpublishedObjects` succeeds or
reports the object absent. A failed Delete, timed-out cleanup, stream removal,
transaction retirement, or CN shutdown timeout retains the ticket and its
name-only owner. A permit exhausted during a transaction returns an explicit
resource-exhausted error **before the next object upload**; the transaction
rolls back and retries deletion of objects it already owns. While S3 deletion
remains unavailable, later CN uploads remain backpressured. Admission must not
wait while holding a workspace, sinker, or batch-flow lock; a nonblocking
reservation failure avoids a capacity/deletion deadlock.

This does not make ownership durable. A CN process crash can still lose its
in-memory ledger and leave an object behind. The issue acceptance for this PR
therefore covers live-CN rollback/stream/teardown failures, not process-crash
recovery. Durable journaling of names before upload is a separate design if
crash recovery is required. Operators must not treat the finite ledger as a
substitute for S3 availability or durable GC.

### Rollout and observable behavior

Roll out ACK-capable coordinators before workers that require receipts. Drain
old workers before claiming the full ownership guarantee. New workers must
reject S3 handoff to an old coordinator before releasing ownership, and the
release plan must state that those remote writes can fail during an unordered
upgrade. A capability/version gate at scheduling time is preferable if mixed
versions must continue serving all writes; that is a separate compatibility
decision and must be implemented and validated before making that promise.

Expose current and high-water ticket use, failed reservation count, pending
cleanup names/tasks, retry age, and Delete failures per CN. Alert before the
admission ceiling and on sustained cleanup debt. Do not log individual object
names in a high-cardinality metric. During a storage outage, the system trades
write availability for bounded memory and retained ownership; successful
cleanup releases tickets and automatically resumes uploads.

### Primary errors and transaction terminal behavior

Cleanup ownership transfer is separate from the SQL result. When a statement
already has a business error, and its remaining cleanup records were accepted
by the workspace or CN retry worker, return that original error unchanged
through RPC; log and count the Delete failure as cleanup debt. Do not
`errors.Join` the two into a new error that loses the original MySQL code.
Apply the same rule to remote finalization, tombstone transfer, and remote
DELETE writers. With no primary error, report a cleanup error only if no live
owner accepted the obligation. A failed queue admission is an ownership
invariant failure, not an ordinary successful cleanup handoff; CN shutdown must
stop transaction producers before closing retry admission, and the transaction
must not silently drop the owner if that invariant is violated.

At Commit and Rollback, delete unpublished objects with one bounded teardown
budget detached from a canceled request context. Nested writer and transfer
cleanup share that budget; an already-expired execution deadline must not make
the first rollback cleanup attempt instantly fail. If deletion fails but the CN
worker accepts the name-only record, Commit/Rollback proceed according to their
business outcome and the worker owns the remaining garbage. If transfer fails,
surface that failure and preserve the owner before any transaction handle is
retired. This changes only garbage cleanup reporting, not whether registered
transaction writes commit or roll back.

### Alternatives and proof required

An unbounded in-memory queue preserves local throughput during an outage but
can exhaust CN memory, so it is rejected. A cap enforced only when enqueueing
after an upload can drop the sole owner and is rejected. A durable ownership
journal would allow continued admission until its disk budget is reached and
would improve crash recovery, but adds atomic record-before-upload and
record-after-registration transitions, restart replay, disk-pressure handling,
and upgrade/migration semantics; it is out of scope for this revision. A
single-worker retry rate, larger timeout, or queue-slice compaction cannot
prove a capacity bound.

Before implementation approval, deterministic tests must prove: reservation
precedes every real CN object Sync; no-capacity fails before upload; ambiguous
Sync retains its ticket; successful registration and deletion each release once;
failed deletion and terminal handoff retain the name/ticket without writer
buffers; 65,536 occupied tickets reject the next upload; concurrent reserve/
release and retry survive race testing; remote coordinator capacity failure
withholds the receipt and leaves worker cleanup-owned; both mixed-version
directions behave as documented. Benchmarks must report retained bytes per
ticket and a repeated-spill hot-path cost. The existing package, race, and
multi-CN CI evidence is reusable only for behavior whose implementation and
test inputs have not changed.
