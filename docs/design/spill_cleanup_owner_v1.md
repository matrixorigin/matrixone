# Bounded spill cleanup ownership, revision 2

Status: **proposed; design approval pending**. This is a documentation-only
revision of the proposal, not authorization to deliver the implementation.
The filename stays stable for existing links. Revision 1 is Git blob
`5168b1c70cee7ad29aff87af7b4102a423743c0b`; each implementation PR must link the
commit containing the exact revision accepted by its design reviewer.

Implementation series: [#30099](https://github.com/matrixorigin/matrixone/pull/30099)
and [#30102](https://github.com/matrixorigin/matrixone/pull/30102).
Owning issues: [#30097](https://github.com/matrixorigin/matrixone/issues/30097),
[#30098](https://github.com/matrixorigin/matrixone/issues/30098),
[#30100](https://github.com/matrixorigin/matrixone/issues/30100), and
[#30103](https://github.com/matrixorigin/matrixone/issues/30103).

This revision responds to
[review 5542718835](https://github.com/matrixorigin/matrixone/pull/30099#pullrequestreview-5542718835),
[comment 4290864401](https://github.com/matrixorigin/matrixone/pull/30102#discussion_r4290864401),
and [review 5551035539](https://github.com/matrixorigin/matrixone/pull/30102#pullrequestreview-5551035539).
The gate applies to the whole series: a process-global owner, admission state
machine, retry workers, and startup rejection change ownership, concurrency,
capacity, and availability. Earlier implementation approvals do not approve
these later design decisions.

## Problem and invariants

A removal failure must not make a named spill file ownerless or refund its disk
charge. Query cancellation, Reset/Free, rejected handoff, and operator-pool
release can otherwise destroy the last cleanup owner. Conversely, retaining
whole operators or admitting unlimited cleanup callbacks moves the failure to
heap exhaustion. The review's zero-byte cancellation counterexample is important:
disk-byte and closed-FD ledgers can both be zero while named files accumulate.

The proposal must preserve all of these invariants:

1. Every successfully created named file has one effective destruction owner
   until removal succeeds or confirms the file is already absent.
2. Admission happens before file creation. Active files and failed removals share
   a finite count budget, including zero-byte files.
3. Closing a writer or execution generation is not deletion. FD, memory, disk,
   and metadata reservations have separate release conditions.
4. Handoff moves ownership; it neither duplicates a reservation nor refunds it.
   Detached cleanup cannot retain query batches, operators, or query callbacks.
5. A failed or non-returning cleanup cannot create unlimited tasks or workers.
   Eventual removal is conditional on the filesystem returning and recovering,
   not a guaranteed deadline.

Goals are bounded retained cleanup state and recovery after transient removal
errors. Non-goals are a generic task service, durable query recovery, per-tenant
fair scheduling, arbitrary filesystem cancellation, or a new spill format.

## Scope and ownership transitions

Reuse `pkg/common/spillcleanup` as the process-scoped terminal owner, not a new
generic framework. The first owner is the producer creating the named file:
GROUP/DISTINCT buckets, JOIN writers, or HashBuild entries. Successful payload
handoff moves the file service, generated name, and reservations to the receiver.
A failed handoff leaves or returns ownership to a reachable sender. Terminal
teardown transfers unresolved removals to detached per-file callbacks before
discarding the operator; it clears the former owner's references.

Reservation states below describe metadata admission, not whether a file is
open. Reopening an existing named file needs FD admission but no new metadata
reservation.

| Event | State transition / commit point | Effective owner and release rule |
| --- | --- | --- |
| Reserve before CreateFile | unadmitted -> active; successful admission-counter CAS | Producer owns the slot even before a file exists. |
| Admission rejected | remains unadmitted | Return ErrCapacity; do not create a file or enqueue work. |
| CreateFile fails without creating a file | active -> released; release CAS | Producer returns the unused slot and any other acquired reservations. |
| CreateFile succeeds; later init/write/cancel fails | remains active | Producer keeps the filename and reservations until deletion or terminal handoff. |
| Payload handoff / reopen | active -> active | Move the same token; retain one effective owner. This is not a second Reserve. |
| Terminal enqueue | active -> queued; token CAS wins once, then queue insertion under mutex | Detached owner takes over; caller must stop mutating or deleting that file. |
| Removal error | active stays active, or queued stays queued | Preserve metadata and disk charge; queued retry retains one task. |
| Removal succeeds / already absent | active or queued -> released; release CAS wins once | Release applicable disk/cache accounting and the metadata slot; retire callback. |
| Duplicate enqueue / release | no second transition | Token operations are idempotent; this alone does not make arbitrary file-owner methods concurrent-safe. |
| Process exit | in-memory state disappears | Startup cleanup, not the old token, owns previous-incarnation scratch files. |

The callback contract is stronger than `func() error`: nil means cleanup is
complete and its reservation has been released. A pre-deletion error keeps the
token live. A close/flush error returned after successful removal must not retain
an uncharged retry. The queue wrapper checks released state for that case.
These are internal APIs; callers must enforce the owner-transfer and callback
contract rather than treating atomic tokens as a substitute for synchronization.

## Admission and retained-resource bounds

The proposed hard limit is **16,384 active plus pending file reservations per
process**, shared by all queries and CN instances in that process. It is separate
from disk-byte and FD limits; budget-less compatibility paths must also reserve.
The arithmetic permits 128 simultaneous 128-file footprints, but that is not
workload sizing evidence or a completion guarantee. Approving this constant
requires accepting admission failures and the shared blast radius below.

When full, `Reserve` returns `ErrCapacity` before CreateFile. It does not wait
for cleanup, evict old callbacks, or retry as memory reclamation. The error is an
internal error with text `spill file metadata capacity exhausted`, not a new
wire error code. A surrounding producer operation may still hold other locks:
this is not a claim that every caller has bounded end-to-end rejection latency.

| Resource | Bound and lifetime |
| --- | --- |
| File reservations / retry tasks | At most 16,384 admitted owners; at most one queued/running task per queued token. |
| Completed callback frames | Up to four workers can briefly retain released metadata while slots are reused. Pending slice slots are cleared on removal. |
| File names / callback captures | Generated names and detached per-file metadata; no operator, batch, query callback, or whole payload bundle capture. |
| Disk/cache accounting | Keep applicable charges while the file remains; do not refund on query/generation close. Independent existing budgets still apply. |
| FD / writer buffers | Close/release during ordinary teardown before detached retries; metadata admission is not FD admission. |
| Accounting referents | A disk token can retain its execution generation and shared budget. Count their reachability; do not describe the token as a standalone fixed-size heap bound. |
| File service | Borrowed service reference, not a newly created service per task. May remain reachable until removal. No byte bound on arbitrary custom service implementations is asserted. |
| Retry goroutines | At most four process-wide workers; no per-attempt goroutine or replacement of a stuck worker. |

Thus the proposal bounds file-associated owner count, not all transitively
reachable Go heap or physical filesystem storage. Review must retain this
distinction. HashBuild/message payloads enqueue per-file callbacks so a stuck
file does not keep already-deleted siblings in a bundle callback.

## Retry, cancellation, and service lifecycle

Workers call cleanup without holding the queue mutex. Errors become eligible
again after one second; ready work may bypass delayed tasks. Workers exit on an
empty queue and start again on new work. Queue scanning and slice compaction are
O(n), n bounded by admitted tasks; enqueue is amortized O(1). Reserve contends
on one process-wide atomic counter. No throughput benchmark is claimed.

An individual callback can use the existing RemoveFileWithRetry helper: up to
three synchronous removal attempts with two 5 ms delays. Its attempt count does
not bound a syscall's duration. A callback still failing afterward is requeued;
there is no total retry deadline or expiry that silently drops ownership.

The delay is a minimum retry interval, not an SLA. Sleeping workers are not
explicitly woken by new work; execution also depends on scheduling, queue depth,
and callback duration. A non-returning callback consumes one worker. Four such
calls stall all cleanup; ownership is retained and admission eventually fails
closed. Recovery requires the filesystem call to return or an operator-controlled
process restart. Existing synchronous teardown attempts can also block before
terminal enqueue, including while an owner lock is held; worker isolation does
not establish bounded query teardown latency.

The applicable Go API contracts are
[`os.Remove`](https://pkg.go.dev/os#Remove), which has no context parameter,
and [`context.CancelFunc`](https://pkg.go.dev/context#CancelFunc), which does
not wait for the operation to stop. MatrixOne's LocalFS/LocalETLFS RemoveFile
checks context before calling os.Remove; a timeout wrapper cannot guarantee
interruption after that call begins. This is an API constraint, not an optional
queue-design precedent or a new network interoperability protocol.

Query/generation close does not stop the process-wide queue. A retained file
service pointer prevents collection, but does **not** prevent another owner from
calling its Close. Current LocalFS/LocalETLFS removal uses the retained path and
os.Remove, rather than their closed cache/directory handles. Service replacement
or reuse of the same scratch root while old callbacks are live still needs an
explicit lifecycle contract; pointer reachability is not proof of safe reuse.
Callbacks must not panic; the queue adds no panic recovery. An unrecovered panic
is process failure, not a successfully completed cleanup.

## Restart, compatibility, and rollback

There is no durable callback journal and no drain wait that can hold process
exit hostage to an unavailable filesystem. Exit loses reservations/callbacks;
named files remain under the existing `LOCAL/__spill` scratch root. Queries
are not resumed from those files.

Before publishing a newly initialized service, createFileService runs the
existing clearSpillFiles. The proposal propagates listing/deletion errors instead
of accepting new spill work over uncleared previous-incarnation files. Partial
startup cleanup may have deleted some files before failing; retry lists what
remains. Successful cleanup is the recovery boundary. A failing startup must
not be presented as healthy.

Each live service incarnation must exclusively own its scratch root. Concurrent
roots shared by different processes, or in-process replacement overlapping old
callbacks, cannot be treated as restart recovery. The latter is an open blocker
below, not silently assumed safe.

No catalog, spill encoding, wire format, persistent journal, or configuration key
is added. Separate processes keep independent limits and workers; mixed versions
do not coordinate admission. Upgrade must stop the old owner before reusing its
root. Downgrade/rollback restores the old cleanup/admission behavior and may
restore the original leak: it is not a safe way to clear a filesystem outage.
Repair the filesystem and verify the old owner has stopped before retrying
startup or performing separately authorized scratch cleanup. Do not delete a
live service's spill files. Backup/restore does not gain query-resume support or
any new cleanup journal.

## Alternatives and proposed choice

| Option | Correctness / liveness | Cost, compatibility, and operations |
| --- | --- | --- |
| Original teardown / discard on removal failure | Loses the only owner and may refund bytes too early. | Smallest code path, but cannot satisfy the recovery invariant. |
| Keep all ownership in query/operator objects | Survives errors only while that owner remains reachable; cached or released operators complicate retry. | Avoids a global queue but retains query graphs and needs teardown changes across every consumer; difficult to bound over repeated failures. |
| Durable per-root journal and sweeper | Can survive exit and isolate service incarnations if journal ordering/replay is specified. | Additional persistent format, migration/recovery protocol, I/O, and compatibility tests; disproportionate without a requirement to resume spill queries. |
| Bounded detached owner with fixed workers (proposed) | Retains ownership after query teardown; limits files/workers, but can fail admission or stall on four hung calls. | Reuses the cleanup package and scratch format; deterministic injection tests are possible. Shares capacity across tenants/CNs and needs lifecycle/operational approval. |

A timeout plus a replacement goroutine per attempt is not a valid alternative:
a callback that never returns would accumulate goroutines. A larger cap or more
workers only changes the failure threshold. Neither proves termination or tenant
fairness.

## Operations, isolation, and rollout gate

This proposal has no feature flag or runtime tuning knob. Any later cap,
worker-count, or startup-policy change needs its own decision and evidence.

A single tenant can consume the shared file slots, including zero-byte files
under repeated cancellation during a removal outage. Other queries/CNs in that
process may then fail spill admission despite available disk/FD budget.
Reservations prevent unbounded file-owner growth; they do not provide tenant
fairness or a tenant quota. No new privilege or user-supplied deletion path is
introduced. Diagnostics must not add SQL, tenant IDs, or filenames as unbounded
metric labels.

Current diagnostics are the capacity error, existing cleanup/startup errors,
and read-only filesystem/process inspection. `spillcleanup.Used()` is a local
snapshot API used by tests, not an exported production metric. Pending count,
oldest retry age, and blocked-worker duration have no dedicated production
metrics in this proposal. Do not claim alerting coverage that does not exist.

Before production delivery, the design reviewer must accept that diagnostic
surface or require a bounded observability change; the implementation owner must
then supply its deterministic validation. Rollout, when separately authorized,
should begin with an isolated CN/process and exclusive root, observe admission
rejections, cleanup recovery and restart failure behavior, then expand only with
the deployment owner's acceptance. Stop expansion on new admission failures,
non-draining cleanup, or startup rejection. This document does not authorize a
deployment, CI dispatch, shared-environment test, or destructive cleanup.

## Verification contract and remaining evidence

The following are acceptance oracles, not claims of a fresh successful run.
Reuse evidence only for unchanged relevant code, fixture, mode, and base
contracts; stale, failed, skipped, or zero-test runs are not passing evidence.

| Invariant / failure | Cheapest proof and existing test anchor |
| --- | --- |
| One token before create, including empty files | Exhaust admission; assert CreateFile is not called, names do not grow, and capacity is restored after real deletion. GROUP `TestGroupSpillMetadataAdmissionBeforeCreate`; JOIN `TestJoinSpillMetadataAdmissionAndHandoff`; HashBuild `TestHashBuildSpillMetadataAdmissionAndHandoff`. |
| Duplicate terminal publication / release | Concurrent Enqueue/Release controls; exactly one callback and one refund, including close error after deletion. `TestReservationBoundsAndRestoresAdmission` in spillcleanup. |
| Independent cleanup and finite workers | Barrier-block one callback while a healthy one completes; block all four with pending work, assert no replacement workers, then unblock and drain. `TestQueueIsolatesBlockedCallback` / `TestQueueNeverReplacesBlockedWorkers`. |
| No query retention or premature accounting refund | Real small files plus remove-error injection through Reset/Free, failed handoff and pool release; GC/reachability controls before recovery; physical absence and exact disk/FD/cache/metadata release afterward. Existing spillutil, message, GROUP and HashBuild cleanup tests. |
| Failed restart cannot publish new service | Private local scratch files, injected deletion/listing error, startup error and retry after repair. `TestClearSpillFilesRejectsUndeletedPreviousOwners`; also verify service publication remains after cleanup. |
| Service/root reuse while old callbacks survive | **Missing design closure:** decide the supported lifecycle first, then add a barrier-controlled Close/replacement test if supported. No shared cluster is needed. |
| Operational recovery at the chosen capacity | Accept the current diagnostic limits or define low-cardinality signals; supply minimum workload/cardinality evidence for the chosen limit before rollout. The 128 x 128 calculation alone is insufficient. |

After approval, implementation review must map every producer and terminal
consumer to these contracts, run affected normal/race tests where justified, and
resolve CI/base compatibility blockers. This revision changes only Markdown;
document checks cannot establish runtime correctness or repair a failed UT.

## Design decision record

| Decision requiring closure | Proposed position / unresolved part | Decision owner and deadline |
| --- | --- | --- |
| Capacity and tenant blast radius | 16,384 shared active+pending files; no tenant fairness. Sizing is not workload-validated. | Design reviewer with execution-resource maintainer; before approval. |
| Liveness / availability | Four workers; no replacement for hung I/O; synchronous teardown may block. | Design reviewer with file-service maintainer; before approval. |
| Service replacement / root reuse | Exclusive process restart is defined; in-process old-owner overlap is not closed. Specify prevention or a generation-safe protocol. | Service lifecycle maintainer and design reviewer; before approval. |
| Startup and rollback | Fail startup if old scratch cannot be cleared; rollback does not repair the original defect. | Service lifecycle maintainer and design reviewer; before approval. |
| Observability and rollout | Current errors lack queue age/worker metrics; accept this limitation or require bounded signals and their tests. | Design reviewer with deployment owner; before approval. |

No acceptance is recorded on behalf of these owners. The PR author owns collecting
the decisions and linking an exact approved document revision. Material changes
require a new revision and re-review.

```text
Change scope: #30099 + #30102, shared spill cleanup lifecycle
Trigger: process-global resource controller, concurrency and restart semantics
Design: spill_cleanup_owner_v1.md revision 2, proposed
Blocking findings: approval absent; decisions in the table above remain open
Decision log: proposed alternatives/tradeoffs documented; none self-approved
Decision: REQUEST_CHANGES for implementation delivery
Implementation deviations: not fully audited while the design gate is blocked
```
