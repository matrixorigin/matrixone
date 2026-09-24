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
remote terminal cleanup fails, the handler registers that workspace's cleanup
callback with the owning CN server before returning its error. One CN worker
retries failed callbacks after the stream has exited; a successful callback
releases the reference. The server joins the worker and makes a final bounded
attempt before CN I/O dependencies close. Persistent failure remains visible
as a CN-close error, not a silently successful cleanup.

Terminal local rollback transfers a failed unpublished cleanup callback to
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
The CN retry queue holds one callback per failed stream or local rollback and
may retain its mirror workspace or retired local transaction until deletion
succeeds. Its memory therefore grows with concurrent failed cleanups during a
storage outage; this is an explicit limit
of the in-memory repair, not a fixed-capacity queue that could drop ownership.
The single retry worker bounds retry concurrency.

## Failure, rollout, and limits

- A lost/failed ACK, connection loss, invalid sequence, cancellation, or
  StopSending cannot prove takeover. The worker keeps its owner and attempts
  cleanup; the coordinator's overlapping owner is reduced only if its local
  transaction actually appends the corresponding metadata. Cleanup retries are
  in-process, not a distributed durable handoff log.
- The remote finalizer removes request cancellation once and starts one cleanup
  deadline. Nested writer, MultiUpdate, and transfer cleanup preserve that
  deadline instead of starting another ten-minute timer per writer. The CN
  retry worker uses a fresh bounded attempt after the handler exits. A failed
  S3 delete is returned and its in-memory owner stays queued; a process crash
  or persistent storage outage can still leave an orphan. Durable recovery is
  a separate design and is not claimed here. Reset/Free attempts that run
  before the remote finalizer are not yet charged to its later deadline.
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
