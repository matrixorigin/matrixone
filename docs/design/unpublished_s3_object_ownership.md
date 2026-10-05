# Unpublished CN S3 object ownership

## Goal and scope

CN writers can persist objects before their metadata reaches `txn.writes`.
After `SyncAndTakeResults` detaches stats, a metadata-copy, forwarding or
workspace-append failure can leave ordinary rollback GC unable to discover
those objects. This design closes that live-CN gap for Insert, MultiUpdate,
remote DELETE, transaction dumps/rewrite, clone readers and tombstone transfer.

The goal is useful cleanup coverage at small, measured normal-path cost and
proportionate complexity, not unconditional elimination of every orphan.
Existing writer, workspace, transaction and CN lifecycle owners remain the
boundaries. No durable journal, restart-recovery protocol, full S3 listing or
generic cleanup framework is added.

Upgrades are **stop-the-cluster upgrades**: drain/stop the old services, then
start all services on the new version before admitting work. Mixed-version
write availability and rolling-upgrade interoperability are not requirements.
Stored object formats and committed metadata remain unchanged and readable;
in-memory cleanup debt is not durable across a process exit.

## Ownership contract

Before metadata registration, one effective cleanup responsibility must remain
until confirmed deletion or takeover. This is continuity of responsibility,
not physical deletion by a deadline regardless of storage availability.
Successful metadata copy alone is not registration. After workspace append,
ordinary transaction rollback/GC owns the object.

| Boundary | Successful transition | Failure |
|---|---|---|
| Before object Sync | Reserve a CN object-name ticket | Reject before upload; never drop an already-owned name to make room |
| Sync/result extraction | Writer tracks sinker results and detached names | Ambiguous Sync keeps its name and ticket |
| Producer copies output metadata | Workspace retains a lightweight name owner before writer Reset/Close | Writer remains responsible and retryable |
| Workspace append | Accept only names in that entry; normal rollback/GC takes over | Unaccepted names remain cleanup-owned |
| Remote receive | Coordinator reserves/retains names before forwarding and ACK | No ownership receipt; stream fails |
| Remote terminal drain | Every exported batch has a contiguous receipt and no abort/stop | Worker remains cleanup-owned |
| Failed terminal cleanup | Transfer lightweight records to the CN retry worker | Surface failed handoff; do not report successful deletion |

The workspace deduplicates by object name. A duplicate received owner does not
release the first owner's ticket or delete a name already accepted by it.
The owner's map records pending names and whether it holds their admission
charge; no second per-owner charge map is needed.

`TransferPersistedObjects` reads existing Sync results; it does not Sync again.
CN writers flush their entire tail. A successful transfer permits prompt Reset
or Close. Failed deletion closes/drains the sinker and releases batches, arenas,
free lists and session mpools. Retry retains names, a file-service reference and
a lightweight owner/writer/flow shell. Prepared Reset transfers debt immediately
because prepared execution skips Free.

Clone readers accept newly written data/tombstone objects only after destination
metadata is accepted. Failed reader cleanup detaches the table, snapshot and
request context; subsequent attempts use the fresh retry context. Shared
pre-existing clone objects are not unpublished uploads owned by this cleaner.

## Remote receipts

`pipeline.Message.batch_ack_s3_ownership_retained` is boolean field **20**.
Field 19 independently carries `protocol_version`. An ordinary credit ACK is
not an ownership receipt, even within one version. A receipt confirms cleanup
retention, not transaction commit.

The coordinator classifies S3 producers through metadata-preserving
Connector/Dispatch/Merge scopes, stopping at operators that already register
metadata. Insert, MultiUpdate and DELETE retain names before forwarding or
discarding a batch and before ACK. Parse/retention failures send no receipt.
No-output streams reject unexpected S3 output. Current producer metadata
formats remain supported; these are not mixed-upgrade compatibility branches.

The worker keeps cumulative `ackedSeq` for credits and contiguous
`ownershipAckedSeq` for receipts under the flow mutex. Owners release only
when `nextSeq > 0`, every receipt arrived, no credits remain, and no execution
error, abort or receiver StopSending occurred. Empty streams, unmarked/gapped
ACKs, cancellation and connection loss are not evidence of takeover.

ACK ingress preserves per-session wire order. Valid ACK handling is an
in-memory flow-state update with no I/O or blocking wait; other pipeline work
remains asynchronous. This avoids a receipt map or waiting ACK goroutines.
The existing credit window bounds pending wire data, not object-name debt.

`PartitionMultiUpdate` has no remote instruction codec, so partition writers
stay on the coordinator. Group remote source scopes by CN before wrapping
them, preserving shuffle receiver ownership and writer parallelism.

## Admission, retries and shutdown

Each CN caps live unpublished names at **65,536**, with names at most **256
bytes**. The shared sinker reserves immediately before each real file Sync,
including pipelined Sync. Missing CN admission or an unavailable name fails
before writing. TN/non-CN sinkers do not acquire CN tickets.

After Sync begins, success and ambiguous failure retain the ticket until
workspace acceptance or confirmed deletion/absence. The coordinator reserves
its own ticket before a receipt; overlapping CN ownership does not copy the
object. Admission is nonblocking: it cannot wait on cleanup while holding a
workspace, sinker or flow lock.

Confirmed Delete batches retire only their own names/tickets. An uncertain
batch remains wholly owned even if storage applied part of it. Retry starts
with remaining names. The TN sinker API keeps its full-snapshot count/error
contract.

Terminal Commit/Rollback or remote cleanup detaches records into a CN callback,
not a bound method retaining the transaction. No empty debt is queued. A single
worker immediately drains successful tasks; failed tasks rotate and wait one
second. Each attempt has a 30-second context. Queue metrics update count
immediately and sample oldest age at most once per second, without scanning the
whole backlog on every append/completion.

Pipeline teardown starts one shared ten-minute cleanup budget for Reset, Free
and finalization. Nested cleanups preserve it. Request cancellation does not
cancel terminal cleanup; prepared executions/retries get a fresh budget.
The bound applies to context-aware deletion, not an uncooperative file service
or arbitrary CPU teardown.

CN shutdown stops transaction producers before closing retry admission, joins
the worker and drains debt in bounded rounds before closing I/O dependencies.
If cleanup expires, shutdown reports failure and retains dependencies; the
worker resumes so storage recovery can make progress. The top-level Close is
one-shot and does not automatically resume its remaining shutdown steps.

Once a live owner accepts debt, a Delete failure must not replace the original
SQL error across RPC or turn a valid commit into a garbage-cleanup failure.
A failed ownership handoff is an invariant failure, not best-effort success.

The cap bounds retained names, not S3 bytes or all in-flight writer buffers.
Sustained Delete failure eventually rejects related new S3 uploads; read-only
work and writes without new uploads remain eligible. This trades some write
availability for bounded debt. Recovery releases tickets and reopens admission.
A process crash can still lose names; historical orphans and durable recovery
are outside the contract.

## Observability and validation

Metrics expose `mo_unpublished_s3_tickets{state="used|high_water"}`,
`mo_unpublished_s3_admission_failures_total`,
`mo_unpublished_s3_pending_cleanup_tasks`, and
`mo_unpublished_s3_oldest_cleanup_age_seconds` by CN, plus process-level
`mo_unpublished_s3_delete_failures_total`. Never label by object/transaction.
Initial alerts are 80% of tickets for five minutes, oldest debt above five
minutes, and sustained Delete failures. Plan capacity using measured full
retained state plus GC/in-flight-work headroom, not just map size.

Required evidence:

- Deterministic post-persistence/pre-registration failures and accepted-object
  controls: physically verify deletion/preservation and ticket release.
  Include ambiguous Sync/Delete, partial Delete progress and recovery.
- Prepared Reset/Free, same-instance reuse, failed rollback/stream cleanup and
  CN shutdown: ownership survives without retaining writer buffers.
- Actual receive-before-ACK, absent/gapped receipts, malformed output,
  StopSending/cancel and admission exhaustion. Pin independent wire field
  numbers as well as generated-code round trips.
- Focused races for ownership/admission, ACK ingress, retry rotation and
  enqueue/close; full affected packages and incremental static checks.
- Multi-CN partitioned writes through public SQL with independent row-count/
  value oracles, not just plan-placement assertions.
- `BenchmarkUnpublishedS3RetainedCapacity`: at 65,536 names, post-GC live bytes
  include names, owners, real transaction-detached callbacks, queue/timestamps
  and FS references. Vary 42/256-byte names and 1/64 objects per task. Shared
  FS caches and object payload are separate costs.
- `BenchmarkCNS3RepeatedSpill`: identical workload on verified main and PR,
  reporting timing and allocations for repeated real Write/Sync/metadata/
  ownership cycles at 1 MiB and 128 MiB. A retry-ticker microbenchmark or green
  CI does not replace these capacity/hot-path proofs.

Rejected alternatives: unbounded outage queues; admission only after upload;
full-writer retention until transaction end; cumulative credits as ownership
receipts; durable journaling solely to claim every orphan is covered.
