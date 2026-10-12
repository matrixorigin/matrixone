# Clone checkpoint subscription isolation

Revision 3, issue #29383, implementation PR #29769.
Scope: PushClient latest-checkpoint replay ownership, not parallel clone/DDL.
Design trigger: subscription state, cancellation, reconnect and capacity.

## Evidence and Contract

An actual deployed v4.2.5 source with 135 logical tables and 394 hidden index
tables took 72.327s to clone. One FK edge does not explain this workload's
dominant cost. 478 complete cold-subscription send/response/ready spans summed
to 38.969s; response-to-ready summed to 26.921s. These sums do not establish
an exclusive causal allocation of the total elapsed time. An empty table took
6.356s, with its source and hidden-index subscriptions occurring sequentially.
Only three data rows required new object materialization.

The current global subscribed-table RW lock is held through
`Engine.LazyLoadLatestCkp` and checkpoint consumption. A deterministic test with
a nonempty checkpoint blocked at the real `Partition.Lock` demonstrates that
an unrelated subscription-state read cannot progress. Cancellation terminates
the loader while the partition remains locked. This proves the convoy mechanism,
not its exclusive contribution to production latency.

Invariant: independent subscription-state operations must not wait for another
table's checkpoint I/O. Preserve checkpoint readiness, pending-update visibility,
entry/partition generation, caller cancellation and existing replay capacity.
Keep snapshot fences, current authorization, FK ordering, clone transactions,
atomic rollback, object/index selection and target writes unchanged.

## Ownership and Transitions

PushClient owns a private capacity-one replay gate. Initialize it under
`subscribed.rw`, retain it across transport reconnects, release the RW lock
before admission, and acquire with caller cancellation. Admission occurs before
capturing any Partition/checkpoint objects; queued callers pin none. Defer gate
release on all successful, error, stale and cancelled terminal paths.

This replaces the I/O ownership of the old global critical section. It retains
one active latest-checkpoint materialization in this changed PushClient path,
including inherited prefetch per replay, so it does not amplify replay resources.
Pre-existing direct engine checkpoint callers are outside this bound. No worker,
job queue, configuration, wire state or new prefetch operation is introduced.
`Partition.ConsumeCheckpoints` retains per-table state mutation ownership.

Under the subscriber lock, capture the exact subEntry and latest Partition in
the existing subscriber-to-engine lock order. Consume that captured Partition
outside the global lock through the existing engine checkpoint helper. The
public `LazyLoadLatestCkp` entry point continues using the same helper.

After I/O, reacquire the subscriber lock. Publish `Subscribed` and update lastTs
only for the same entry, matching database ID, and eligible
`SubRspReceived`/`Subscribed` state. Never overwrite pendingTo at publication.

Completion transitions:

- Current eligible entry: propagate its checkpoint error or publish readiness.
- Replacement/reconnect/clear: discard stale success/error and re-enter the
  existing state branches. Never replay old-generation checkpoints into a
  replacement Partition or use an old completion to certify its readiness.
- Missing entry: use the existing subscribe-start branch. Do not return absent
  `Unsubscribed` with nil error into the caller's impossible-path panic.
- Unsubscribing entry: return that existing state and wait via its current owner.
- Caller cancellation: terminal before each iteration and after I/O, even if
  the entry was replaced. Queued cancellation does not wait for active replay.
- Database identity mismatch: error without publishing readiness or looping.

Pending logtail markers are accepted in `SubRspReceived` as well as
`Subscribed`. Replay now runs without the global lock, so incoming logtail must
mark pending before application can wait behind the replay's partition lock.
The existing clear-after-apply and snapshot/pending capture order are retained.

## Alternatives and Boundaries

FK metadata reuse is independently useful but cannot remove this source's cold
subscription cost. Parallel whole-table SQL/DDL would violate transaction/FK
ownership. Parallel source-reader initialization is not selected: shared
workspace/snapshot ownership and aggregate resource bounds need separate proof.
An unbounded unlock would permit simultaneous materialization across partitions;
retain the original active replay capacity instead.

Source flushes, stale-cache reads, skipped indexes and relaxed snapshot fences
are not changes to the violated ownership boundary. No such behavior is added.

There is no API, catalog, persisted-format or configuration migration. Upgrade
and rollback use the ordinary binary lifecycle; reconnect retains the gate but
replaces subscription/partition generations through existing owners. Security
and tenant selection are unchanged. Existing logs and metrics remain the
diagnostic surfaces. Normal rollout is required before evaluating production
latency; no live deployment is part of this PR.

## Verification and Decision

Deterministic barriers cover unrelated progress, same-table readiness, pending
markers across publication, replacement with stale success/error, current
failure, cancellation before/after replacement, unsubscribe/clear transitions,
and capacity-one queued cancellation before partition capture. Use real nonempty
checkpoint state; empty checkpoints return before the partition lock.

Run scoped normal/race owning packages and individually repeat the four new
tests under race. Public SQL current/snapshot clone, FK rebinding, independent
target writes, rollback and existing authenticated restore tests cover consumers.
The actual 135/394 source schema without business data is a local functional
control, not a production latency reproduction or matched performance A/B.

Distinct design-first review: revisions 1/2 requested changes for pending-update,
stale completion and capacity closures; revision 3 received independent local
design PASS before implementation. Implementation approval is a separate gate.
This review record is not a GitHub human approval.

Acceptance: unrelated state operations progress during blocked checkpoint work,
all terminal paths release the gate, no stale readiness or lost pending fence,
and active replay capacity remains one. This change does not promise parallel
checkpoint materialization or removal of the serial 39-second subscription span.
Issue #29383 remains open pending a validated production latency closure.
