# Account lifecycle shared-gate admission

- Status: approved
- Design revision: 8 (2026-09-23)
- Tracking issue: [#28079](https://github.com/matrixorigin/matrixone/issues/28079)
- Implementation PR: [#29234](https://github.com/matrixorigin/matrixone/pull/29234)
- Design base: `66da3877e9e77f01083c4aac0cb9f94ab6e11fe6`

## 1. Problem and evidence

`CREATE ACCOUNT` inherits the system account's View-metadata revalidation
marker. The original implementation protected that read with `FOR UPDATE` on
the single `mo_catalog.mo_view_refresh` catalog row. Every account creation held
that global Exclusive row lock for its surrounding transaction, so unrelated
accounts formed one serial queue. Under the workload in #28079 this queue grew
until concurrent account operations reached their client timeout.

The View identity cannot be read without a lifecycle fence. DROP, restore, and
metadata-recovery owners may replace the catalog objects from which the marker
is inherited. Account creation must observe one stable lifecycle generation
while keeping its account-local catalog writes atomic.

## 2. Invariants and non-goals

The design maintains these invariants:

1. Same-version account creations may hold the lifecycle read generation
   concurrently.
2. Lifecycle writers remain mutually exclusive with readers and preserve the
   existing `SNAPSHOT -> View` order.
3. Once an Exclusive lifecycle writer is queued, a later opted-in reader cannot
   join the current Shared generation ahead of it.
4. A transaction that already holds a Shared generation may re-enter its own
   gate while a writer is queued; it must not deadlock behind itself.
5. A peer that cannot prove writer-fair owner admission is never sent an
   ordinary Shared first acquisition. The request falls back to Exclusive.
6. An old remote pipeline can only re-enter a lifecycle gate already acquired
   on the initiating CN; dropping new statement metadata cannot weaken the
   effective lock.
7. Cancellation removes only the cancelling waiter/subscriber and advances the
   next eligible writer or leading Shared cohort without leaking notifications.

This change does not alter Range-lock merge semantics, general Shared-lock
throughput policy, catalog formats, persisted state, or the transactional scope
of `CREATE ACCOUNT`.

## 3. Control flow and ownership

The initiating frontend acquires each physical lifecycle row directly through
its local lockservice before executing the existing verification SELECT. The
CREATE ACCOUNT entry point unconditionally creates its background executor
with `forcePessimisticRC`; this overrides an optimistic/SI deployment default
for this control-plane transaction only. The direct helper reuses that exact
transaction and rejects a non-pessimistic caller instead of allowing
`LockRows` to silently become a no-op.

```text
CREATE ACCOUNT transaction
  -> initiating-CN lockservice: SNAPSHOT Shared(writer-fair)
  -> SELECT SNAPSHOT FOR SHARE (same-txn re-entry; pipeline may be old)
  -> account-name lock and account-local catalog work
  -> initiating-CN lockservice: SNAPSHOT Shared re-entry
  -> initiating-CN lockservice: View Shared(writer-fair)
  -> SELECT SNAPSHOT, View FOR SHARE (same-txn re-entry)
  -> inherit marker and commit
```

The direct View key is the exact `mo_tables` composite key for
`(account_id=0, reldatabase='mo_catalog', relname='mo_view_refresh')`. The
SNAPSHOT key is the exact `mo_feature_registry('SNAPSHOT')` primary key. The
verification SELECTs remain in place so missing/partially upgraded catalog
objects keep their existing typed-error behavior.

Writer fairness is carried only from the initiating process context to
`LockOptions.WriterFair`; it is not a pipeline wire contract. A local owner
applies the queue rule directly. A remote origin sends the new
`LockWriterFair` lock RPC method (MORPC v94), which is registered only by a
capable owner and accepts only Shared Row requests with `WriterFair=true`.

The owner checks for an Exclusive waiter before the requesting waiter.
Consecutive eligible Shared waiters form one cohort and notification stops at
the next Exclusive waiter. Existing admitted or pending proxy membership is
resolved before the fairness bypass, so same-transaction re-entry reuses its
generation.

## 4. Compatibility and failure transitions

The compatibility decision is atomic at the authoritative owner; there is no
cluster-wide version-floor assumption.

| Event | Result |
|---|---|
| local new owner | apply writer-fair Shared admission |
| remote new owner with v94 active | `LockWriterFair` enters the normal lock handler |
| old owner has no method handler | server returns `ErrNotSupported` before admission; origin retries Exclusive |
| remote new owner is locally gated below v94 | capability handler returns `ErrNotSupported` before admission; origin retries Exclusive |
| initiating runtime is below v94 and the owner is remote | client-side method gate returns `ErrNotSupported`; origin retries Exclusive |
| capable owner is local while its runtime gate is below v94 | apply writer-fair Shared locally; no wire capability is needed |
| binding changes during either attempt | existing bind retry/fencing runs; any compatibility retry is Exclusive |
| old pipeline executes verification SELECT | it sends an ordinary Shared re-entry for a transaction that already holds Shared or fallback Exclusive |
| missing lifecycle catalog object | retain the existing typed-error/feature-disabled behavior |

`ErrNotSupported` is an application response proving the capability-bearing
request published no ownership. Transport errors are not used for fallback,
because their admission outcome is indeterminate and existing cleanup/fencing
must remain authoritative.

During rolling upgrade, an old frontend continues using the pre-change
Exclusive lifecycle SQL only when that old CN is configured for
pessimistic/RC transactions. Pre-v94 frontend binaries do not force that mode;
under an optimistic default their nominal `FOR UPDATE` gate is a no-op and no
owner-side protocol can recover a lock request that was never sent. Therefore
mixed-frontend rollout has one explicit prerequisite: before a pre-v94 CN and a
v94 CN can both accept account-lifecycle traffic, every pre-v94 CN must use
pessimistic/RC defaults, and those defaults must remain until the old CNs are
drained. An optimistic deployment may switch only after all account-lifecycle
frontends run this revision. This is an operator rollout gate, not a claim that
the local MORPC runtime value is a cluster membership floor.

Subject to that prerequisite, a new frontend is safe against both an old
pipeline and an old lock owner because its first physical acquisition occurs
locally and an old owner can only receive the Exclusive fallback. During
downgrade, producers must restore pessimistic/RC defaults before a pre-v94
frontend accepts traffic. A lowered protocol gate produces Exclusive fallback
for a remote owner, while a local capable owner continues using its in-process
writer-fair implementation; an old replacement owner rejects the capability
method and also falls back. Lock-owner correctness needs no membership
snapshot, HAKeeper floor, or probe/admission race.

Every lifecycle writer paired with these readers must also take a real
pessimistic lock under optimistic deployment defaults. CREATE/DROP ACCOUNT,
CREATE/DROP/restore SNAPSHOT, CREATE/DROP/ALTER/restore PITR, and ALTER/DROP
PUBLICATION therefore create their owning background transaction with
`forcePessimisticRC` (or use the equivalent foreground lifecycle enforcement).
View-metadata revalidation creates its internal SQL transaction with explicit
pessimistic/RC options, including each normal recovery `mo_ctl` invocation;
the command handler also rejects a transaction that is not pessimistic/RC.
Publication snapshot GC creates its lifecycle transaction as pessimistic/RC.
ALTER-lineage GC retains fixed SI discovery but explicitly uses pessimistic
mode, so its fast-fail lifecycle write is a real lock. Foreground catalog DDL
has two admission classes. Lifecycle operations that need a fresh control
snapshot, including TRUNCATE, PITR, DROP/REPLACE, and DATA BRANCH
DIFF/MERGE/PICK, force pessimistic/RC or reject an incompatible existing
transaction. Fixed-snapshot-compatible ALTER/RENAME, CREATE DATA BRANCH, and
public CLONE force pessimistic mode for a newly created transaction but preserve
its selected isolation; an existing pessimistic SI transaction is accepted and
an optimistic transaction is rejected. This matches DATA BRANCH's independent
RC quota-control transaction without changing its outer fixed SI snapshot. The
compiler lifecycle helper independently rejects optimistic transactions,
covering internal CREATE/DROP PITR plans that do not pass through frontend
dispatch. Explicit public CLONE now installs the same commit-time lifecycle
validation as explicit DATA BRANCH operations. Each mode is selected before
`begin` and before any catalog mutation; it does not change the user's later
transactions.

Concurrent account initialization also builds the tenant's metadata queries in
parallel. Planner-generated boolean subquery sentinels must therefore be
query-local: statistics calculation annotates `Expr.Ndv`, so sharing package
level `true`/`false` expressions races and can leak one builder's annotations
into another. Each use allocates a fresh expression, with a focused identity
test and the two-CN regression run under the race detector.

## 5. Cancellation and queue state

The change adds no new goroutine or retry loop. Existing owners remain:

- the physical-RPC initiator owns the owner admission callback and publishes
  one terminal result to its proxy generation;
- an independent pending follower owns only its subscriber and cannot cancel
  the physical initiator;
- a same-transaction re-entry owns only its re-entry waiter and cannot fail
  other subscribers;
- a detached completed generation retains notified followers until each
  commits its local ledger or retries uncached;
- cancelling an owner waiter removes that waiter; a cancelled leading writer
  wakes the next eligible Shared cohort, while a notified Shared head remains
  ordered until it consumes or cancels its notification.

An `ErrNotSupported` capability attempt creates none of these states. The
Exclusive retry is a normal physical admission and inherits existing timeout,
deadlock, bind-change, remote-unlock, and transaction-generation ownership.

## 6. Alternatives

1. **Cluster-wide `MOProtocolVersion` floor.** Rejected. The runtime value is
   local to each service and is not an automatically maintained deployment
   minimum, so it cannot close a membership/admission race.
2. **Probe every owner before Shared admission.** Rejected. A binding can move
   between probe and admission.
3. **Transport WriterFair through the pipeline.** Rejected as the correctness
   boundary. An old execution node drops the field. Direct acquisition makes
   the pipeline operation a harmless same-transaction re-entry instead.
4. **Capability-bearing owner method plus Exclusive fallback.** Selected. An
   old owner rejects the new method in the RPC dispatcher before lock state is
   touched, making fallback linearizable and fail-closed.
5. **Keep the original Exclusive gate.** Safe but retains the reported global
   serialization and timeout behavior.

## 7. Performance and resource budget

The same-version path adds at most three authoritative direct admissions per
`CREATE ACCOUNT`: initial SNAPSHOT, SNAPSHOT re-entry, and View. Re-entry does
not add a physical holder or another transaction-ledger key. The existing SQL
verification may issue the same three re-entries if executed remotely.

Each transaction retains at most two lifecycle row keys, creates no goroutine,
and adds no persistent or process-wide per-request container. Queue storage is
the existing bounded waiter state per contended row. Owner notification scans
only the leading eligible Shared cohort and stops at the next Exclusive
waiter; it is linear in the cohort it wakes and does not scan unrelated locks.

Fair Shared requests bypass the origin proxy cache so the authoritative owner
can observe queued writers. This applies only to the two account-lifecycle
keys. The acceptance criteria are: two account creations are simultaneously
observable as View Shared holders; a queued View writer is admitted before
later readers; and every fallback request is observable as one rejected
capability attempt followed by one Exclusive attempt. If the fallback is
active, concurrency temporarily returns to the legacy serialized behavior
without weakening correctness.

## 8. Verification contract

Deterministic unit coverage must prove:

- v93 rejects `LockWriterFair` and v94 accepts it;
- an `ErrNotSupported` fair attempt is retried as Exclusive, while transport
  errors retain the existing indeterminate-outcome path;
- the direct frontend acquisition happens before its verification SQL and the
  SQL context itself does not depend on WriterFair transport;
- owner ordering is `Exclusive -> Shared cohort -> Exclusive`, cancellation of
  the leading writer advances the cohort, and notified readers are not
  overtaken;
- a proxy sends a new fair reader to the owner but reuses admitted/pending
  same-transaction membership.
- CREATE ACCOUNT and each paired background lifecycle writer request a forced
  pessimistic/RC owner transaction before `begin`; the direct helper rejects an
  accidentally optimistic transaction.
- view-metadata revalidation supplies explicit pessimistic/RC options when its
  internal SQL executor creates every gate-owning transaction, and the command
  handler fails closed if a manual caller supplies another mode.
- publication snapshot GC supplies pessimistic/RC options to the exact
  transaction that deletes a snapshot; ALTER-lineage GC supplies pessimistic
  mode while retaining its fixed SI discovery snapshot.
- frontend admission forces fresh-snapshot lifecycle participants to
  pessimistic/RC; fixed-snapshot-compatible ALTER/RENAME, DATA BRANCH CREATE,
  and CLONE preserve SI while forcing pessimistic mode. Both classes reject an
  incompatible existing transaction, and the compiler gate fails closed for an
  optimistic internal PITR caller.
- mixed-version rollout documentation rejects optimistic defaults on pre-v94
  frontends, because those binaries can omit the lock request entirely.
- planner-generated subquery boolean sentinels are distinct per query, so the
  newly concurrent account initialization is race-free under statistics
  annotation.

The two-CN integration regression must prove with observable barriers:

1. two CREATE transactions hold the real SNAPSHOT Shared gate concurrently;
2. a SNAPSHOT writer remains queued while both transactions re-enter SNAPSHOT;
3. a View writer queued before the CREATE readers is admitted first;
4. after that writer releases, both CREATE transactions hold the real View
   Shared gate concurrently;
5. every connection/transaction owns immediate idempotent cleanup, DROP errors
   are surfaced, and final account and lock-state queries prove no residue.
6. with both CN runtime defaults set to optimistic/SI, CREATE ACCOUNT still
   exposes real SNAPSHOT and View Shared holders because its background owner
   transaction is locally forced to pessimistic/RC.

The integration writer barriers use exact physical lifecycle keys through the
real lock services. This isolates owner admission ordering from SQL-session
transaction defaults; separate frontend tests prove every SQL lifecycle writer
forces its owning transaction to pessimistic/RC before `begin`.

Final validation requires focused tests, owning-package race tests, the two-CN
integration test under `-race`, incremental vet/lint, generated-protobuf
consistency, and rebase validation against current `main`.

## 9. Decision log

- Revision 1 was rejected because it incorrectly treated local
  `MOProtocolVersion` as a HAKeeper-maintained deployment floor.
- Revision 2 moves compatibility to an owner-atomic RPC capability and moves
  the first physical lock acquisition ahead of any remote pipeline boundary.
- Revision 3 makes pessimistic/RC ownership explicit for readers and paired
  writers, fails closed for an accidental optimistic direct caller, and fixes
  the local-owner protocol-gate truth table.
- Revision 4 closes the paired-writer audit for PITR and records the pre-v94
  pessimistic/RC rollout prerequisite.
- Revision 5 covers normal/manual View-metadata recovery, publication snapshot
  GC, and ALTER-lineage GC with real lifecycle locks under optimistic runtime
  defaults.
- Revision 6 closes foreground ALTER/RENAME/TRUNCATE, internal PITR, explicit
  DATA BRANCH, and explicit CLONE admission gaps found by the all-writer audit.
- Revision 7 splits fixed-snapshot lifecycle statements from the RC-only
  admission class, preserving pessimistic SI semantics for ALTER/RENAME, DATA
  BRANCH CREATE, and CLONE while continuing to reject optimistic execution.
- Revision 8 records the query-local planner sentinel requirement exposed by
  concurrent account initialization and verified by the integration race run.
- Revision 8 is reviewed together with the implementation in PR #29234; PR
  approval covers this design. Material changes to the fallback,
  direct-acquisition order, queue semantics, or cleanup ownership require a
  new revision in the same PR.
