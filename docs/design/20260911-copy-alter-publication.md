# COPY ALTER publication protocol

- Issue: [#28319](https://github.com/matrixorigin/matrixone/issues/28319)
- Implementation PR: [#28418](https://github.com/matrixorigin/matrixone/pull/28418)
- Revision: 4 (2026-10-15)
- Status: PROPOSED — explicit maintainer decision required before production repair.
- Proposed decision owners: @cpegeric and @LeftHandCold, the current requested reviewers.
- Inspected implementation head: `f7903a01aa528bb05b5662d6b2bdd859cf4212f5`.
- Inspected target main: `8795f3699b40504376343fa1acda8b4619f0c76e`.

This revision supersedes revisions 1–3. Their late exclusive-publication
protocol and proposed reduction to 1/2-worker acceptance are not the proposed
implementation. The [previous document](https://github.com/matrixorigin/matrixone/blob/f7903a01aa528bb05b5662d6b2bdd859cf4212f5/docs/design/20260911-copy-alter-publication.md)
remains an immutable historical record. This is a design-only phase in the
existing Draft PR; it is not approval or an implementation-complete claim.

## Problem, evidence and minimal scope

The present PR admits ordinary pessimistic RC COPY through canonical broad
C/G/D lifecycle admission before checking delayed-publication eligibility.
That admission retains locks, so the old split path rejects the transaction.
Existing sequential peer tests and green correctness checks do not prove
independent-table preparation. Merely moving that eligibility check earlier
would permit late global admission while holding source locks and can reverse
the lock order used by DROP, restore and recovery.

The proposed first optimization is ordinary permanent-table ADD/DROP PRIMARY
KEY through the existing COPY algorithm. Both live source and planned target
must satisfy main's scalar replacement shape: ordinary relation, no temporary
or partitioned storage, secondary index, FK/ref-child, feature flag, parent or
owned index table. Published physical/logical identity must be revalidated.
The existing scalar protection checks must exclude Snapshot/PITR, branch
membership/protection and reclaim descendants in actor and SYS scopes.
Missing metadata or read errors are not evidence of absence.

Only a fresh, statement-owned pessimistic RC transaction is eligible, with no
prior retained locks, workspace history or external transaction owner.
An independent executor is eligible only if it owns the whole fresh transaction.
Explicit transactions, SI, optimistic transactions, transaction-local tables,
temporary/system/restore operations, unsupported alterations and complex tables
retain the main safety path. Privilege checks, allocator state, publication
restrictions and existing validation errors remain in force.

Main's persistent View refresh layer is currently inactive. The narrow path
requires that same layer to be disabled; enabled View refresh selects the broad
path before narrow admission. There is no new unreliable scan asserting that
legacy View dependencies do not exist, and no activation of that subsystem.

## Lock protocol and lifecycle ownership

Reuse `admitScalarReplacementRC` and the existing root-scoped replacement
receipt, rather than adding a distributed lock, catalog key or global fence.
A COPY-specific pre-copy result must distinguish an unsupported candidate,
successful narrow admission and a candidate that needs a fresh broad transaction.
Do not change other scalar replacement callers' contracts incidentally.

| Order | Protection | Narrow mode | Lifetime |
| --- | --- | --- | --- |
| C | Physical catalog identity of feature registry | Shared | Through commit/rollback |
| G | SNAPSHOT lifecycle row | Shared | Through commit/rollback |
| D | Database name | Shared | Through commit/rollback |
| K | Source relation catalog name | Exclusive | Through commit/rollback |
| T | Source data/definition | Exclusive | Through commit/rollback |

Use main's canonical acquisition, applied-logtail frontier and identity checks.
After K/T, repeat shape, physical/logical/database identity and protection
checks before workspace writes. The final negative protection result relies
on that full admission, not on the preliminary unlocked probe.

Expensive row copying runs outside the **global exclusive** critical section.
Shared C/G/D protection remains held during copying. Different-table owners
can share C/G/D and prepare independently; same-table owners wait at K/T.
Lifecycle operations requiring C/G/D exclusion wait before touching the source.
There is no late G acquisition, no G shared-to-exclusive upgrade while holding
source locks, and no new reverse wait edge. Ordinary compatible contention
waits under existing cancellation/deadline semantics without discarding copied data.

The broad path retains main's complete C → exclusive G → D protocol, View
ordering where enabled, and SNAPSHOT no-op UPDATE/write barrier. The proposed
narrow path does not execute an UPDATE that would upgrade shared G. Approval
must explicitly accept this omission for the certified scalar domain: main's
narrow replacement admission is the precedent, not a claim that broad-path
optimistic validation or GC barriers are generally unnecessary.

## Copy, replacement and failure terminals

The outer statement owns one receipt bound to the exact transaction, attempt,
actor, source database/physical/logical identity and phase. Its authorized
CREATE names contain only the exact source/final name and generated private
replica name; broad wildcard authorization is prohibited. Nested CREATE/DROP
borrow that receipt and existing locks, without late broad admission, snapshot
advancement after writes, or publication of temporary View/task metadata.
A mismatched, expired or foreign receipt is rejected, not treated as broad
permission. End the binding on every success, error, rollback and cancellation.

```text
Candidate -> NarrowAdmission -> Revalidated -> Copying -> Replacing -> Commit
    |              |                |            |           |          |
  Broad       RollbackBeforeCopy   abort        abort       abort    outcome
                   |
           FreshBroadAttempt (at most once)
```

`T_data` is the admitted, source-locked snapshot before preparation writes.
Copy once, preserve the existing logical identity and allocator semantics,
then atomically replace within the same transaction. No post-copy catalog
snapshot advance or rewind is needed for this locked, protected-negative domain.
Commit remains the visibility point; it is not the source-read timestamp.
There are no secondary-index rebuilds or asynchronous index tasks in this fast
path. Complex COPY uses main's complete index/task preparation and publication;
the old split's mismatched `NewPluginIndexes` preparation predicate must not be
activated. Remove orphaned split machinery only after inventorying consumers;
retain independent fixture fixes and distinct correctness oracles.

- Unsupported preliminary candidate: choose broad admission before source locks.
- Narrow admission discovers changed identity/shape/protection: request a typed,
  transaction-generation-bound **pre-copy** fallback. Fully roll back and clean
  the old transaction before at most one fresh broad attempt, under the original
  deadline and prepared database/parameter binding. Never wait for G-X or copy
  before returning that decision. The existing helper's late-upgrade retry is
  not a substitute for this COPY contract.
- Preparation/replacement failure or cancellation: roll back the whole operation;
  the original relation remains usable and no private relation/task survives.
- Once copying begins, no automatic whole-statement recopy. Normal lock
  contention does not request fallback. Preserve original errors; report cleanup
  failures separately. An uncertain commit result is never replayed.
- Caller-owned transactions are never replaced or replayed by this mechanism.

No SQL, catalog, persistence, wire or user configuration interface is added.
Reverting the optimization restores broad admission without a migration. Old
CNs retain their own path and existing C/G compatibility; multi-CN and supported
mixed-version interleavings remain an explicit validation obligation, not an
unverified deployment claim. Existing privilege and tenant identity checks apply
to every nested operation and to the actor/SYS protection probes.

## Alternatives and expected cost

1. Keep broad admission for every COPY: safest status quo, but unrelated tables
   remain serialized during copying and the issue's optimization is undelivered.
2. Copy first, acquire exclusive G later (old revisions): requires split data/
   catalog snapshots and retry/task machinery; normal late waits can invert
   current main's source/lifecycle order. Not selected.
3. Introduce another publication fence or global coordination key: adds protocol
   and mixed-version consumers without being necessary for the accepted narrow
   workload. Not selected.
4. Reuse main's scalar shared admission (proposed): narrower capability, one copy,
   no new global synchronization primitive, and direct deterministic proof.

CPU/I/O remain proportional to rows copied; private storage is one replacement
per in-flight operation. Added protection/identity reads use main's scoped
queries, not a full catalog scan. Shared G can still delay exclusive lifecycle
work for the duration of a copy; this is a deliberate tradeoff for decision.
Cancellation, the request deadline, watchdog and exact-owner cleanup bound each
run. No throughput benefit is claimed before the new-head measurements.

## Verification and delivery contract

The [validation matrix](evidence/20260916-copy-alter-validation-matrix.md)
and [runbook](evidence/20260916-copy-alter-performance-runbook.md) define the
prepared execution contract. Every result must identify source, observer,
binary/native/toolchain and workload revisions; absent observations remain
`UNAVAILABLE`, never inferred zero or estimated physical copied bytes.

1. Instrument the real entry path to prove narrow admission, one completed
   copy, one transaction, shared G and zero attempted exclusive admission with
   a complete observer. Before release, pause A and prove unrelated B prepares
   and commits; separately rendezvous both preparations. Same-table B must wait.
   Use bounded barriers with unconditional release/cleanup, not timing sleeps.
2. Cover pre-copy fallback after identity/protection change, strict retry bound,
   rollback/cancel/commit terminals, restore/GC and historical owner exclusion,
   View-layer enablement fallback and wrong-receipt rejection. Preserve the
   existing Snapshot/View correctness case separately from fast-path proof.
3. Exercise query, text/binary prepared and owned executor entry points,
   fallback transaction modes, affected package tests, BVT and multi-CN paths.
   Each new/modified top-level race test must report at most **45.55 seconds**
   in exact-head Ubuntu CI; a green aggregate job is insufficient.
4. On personal Ubuntu, rebuild both frozen main and repaired head with required
   Go 1.27.1 and matching native artifacts. Reuse the verified runner's lifecycle,
   ext4, safe fixture-port range, metrics end acknowledgement and resource limits;
   update old gate-position assertions for actual shared/exclusive admission.
   Same-generation smoke precedes formal 1/2/4/8-worker paired runs: 8192 rows,
   width 1024, 20 ADD/DROP rounds per worker, 30-second request deadlines,
   B/H–H/B–B/H at each concurrency (24 arms), no client retry or selective rerun.
   For each pair compute head/base TPS and latency ratios. Require the median
   paired TPS ratio >=0.90 and P50 ratio <=1.10 at one worker; at each of 2/4/8
   require median paired TPS >1 and P95 <1. Retain all individual ratios, failures,
   lock/copy/commit timings, CPU/I/O and post-run data/schema/cleanup oracles.
5. Run three serial equivalent nightly replays with the already accepted fixed
   reconstruction: 100 tables, four seeded rows, 50 ADD plus 50 DROP users,
   five minutes, only ADD 1068 / DROP 1091 allowed. Preserve pinned mo-load/JAR/
   JDBC/JVM identity and 3CN/1TN/3Log/2Proxy topology; record effective timeout,
   immutable image, rendered manifest, ownership and storage cleanup receipts.
   Admission-gated TKE supplies that topology; personal Ubuntu does not replace
   it. Label historical missing receipts honestly; reconstruction acceptance is
   already given and does not need another user decision.

Freeze the repaired implementation/test/runner subjects before acceptance;
subsequent semantic changes invalidate affected evidence. Refresh main before
merging, preserve its lifecycle/restore semantics and this Draft's history, and
resolve conflicts only after this design is approved. Do not alter the primary
working tree or restart the paused resource-wait automation. QA remains required.

## Requested maintainer decision

Please approve **revision 4 as a complete protocol**, or identify the specific
change required, including these coupled decisions:

- ordinary unprotected key-only COPY scope and existing scalar shared C/G/D
  protection held throughout copy, with source K/T exclusion;
- omission of exclusive G/no-op SNAPSHOT write only in that admitted domain,
  enabled View refresh and complex-table broad fallback;
- at most one pre-copy fresh broad transaction, no ordinary-contention recopy,
  and no replay after copy starts or commit becomes uncertain;
- unchanged full 1/2/4/8-worker, per-test race, three-nightly and QA gates.

An authoritative approval must identify this revision or its immutable commit
and have no unresolved conflicting protocol decision. Silence, existing review
requests, historical conditional handoff, local smoke and this author's checks
are not approval. After approval, repair production/conflicts, validate the
exact new head, update each blocking thread and request implementation re-review.
