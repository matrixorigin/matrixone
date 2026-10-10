# ALTER COPY pre-publication design

- Status: proposed; independent design approval required before implementation approval.
- Issue: #29208.
- Implementation PR: #29798.
- Design trigger: materially changes the COPY ALTER concurrency, retry, and lifecycle model.
- Supersedes: none; extends the RC lifecycle contract in [20260929-rc-lifecycle-protocol.md](20260929-rc-lifecycle-protocol.md).

## Problem

`ALTER TABLE ... COPY` currently acquires the global SNAPSHOT lineage gate before physical preparation. The gate is transaction-scoped and intentionally broad, so unrelated branch ALTERs serialize behind each other and can eventually return lock-wait timeouts or connection losses under high concurrency.

The required correctness invariant is unchanged: publication must not expose a stale or inconsistent source snapshot, and every source or lineage mutation that could invalidate the copy must be detected before publication.

## Goals

- Let independent ALTER COPY operations prepare physical work without waiting on each other’s global lineage gate.
- Preserve atomic publication and the existing global ordering for genuinely conflicting lifecycle owners.
- Retain the exact source and lineage invariants that the old gate-before-copy path enforced.
- Convert all detected preparation races into controlled, retryable definition-change failures.
- Keep the design bounded to ALTER COPY and the existing RC lifecycle protocol.

## Non-goals

- Remove or weaken the global SNAPSHOT lineage gate.
- Change DATA BRANCH, Snapshot, PITR, restore, or GC ownership.
- Introduce a new distributed protocol or persistence format.
- Improve unrelated DDL paths beyond the lock-order invariants they already share.

## Selected design

### Pre-publication phase

The transaction resolves the source table and records a copy timestamp, but does not take source locks or the global lineage gate. It then performs the physical work that does not require authoritative publication:

1. create the temporary target relation;
2. copy the source rows at the recorded timestamp;
3. reconcile auto-increment state;
4. clone unaffected indexes.

Unlocked lineage inspection may choose a fixed historical snapshot, but it is advisory only. Late lineage discovery is re-resolved during publication.

### Publication phase

The transaction then acquires the global lineage gate in the existing `C → G` order, advances the RC snapshot, and re-resolves the authoritative source relation and definition. Before publication, it rejects:

- source relation replacement;
- same-ID table-definition-version changes;
- committed source row changes since the copy timestamp;
- foreign-key state changes;
- stale or late lineage participation;
- same-statement lineage column replacement.

Only after all checks pass does the transaction publish the prepared target, drop the original source, and rename the target into place. If any check fails, the statement returns `ErrTxnNeedRetryWithDefChanged`; the surrounding transaction owns rollback and cleanup.

### Source-change detection

For a relation that existed before the current transaction, the authoritative check uses the engine CDC interface from the copy timestamp to the maximum timestamp. The check is performed under the global gate and therefore observes a stable publication frontier. Transaction-local relations cannot use committed CDC, so they are detected through the new `RelationCreatedInCurrentTxn` capability and skip the committed-change probe.

### Lock and retry model

The design deliberately separates speculative physical preparation from authoritative publication:

- pre-publication holds no source lock and no global lineage gate;
- publication acquires the global lineage gate before source database, catalog-table, and physical-table locks;
- source-lock contention is converted to `ErrTxnNeedRetryWithDefChanged`;
- optimistic ALTER uses no source locks during preparation;
- late source or lineage mutation causes a controlled whole-transaction rollback.

This preserves the existing gate-before-source-lock ordering while removing unrelated physical work from the serialized section.

## Alternatives considered

1. **Keep gate-before-copy.** Correct, but independent ALTERs continue to serialize and can time out under high concurrency.
2. **Lock only the source table.** Insufficient because the global lineage gate also orders branch, Snapshot, PITR, restore, and GC owners that can affect the source’s lineage.
3. **Speculative preparation plus postgate revalidation.** Correctness-preserving and materially improves independent concurrency; selected.

## Compatibility and rollout

This is an internal lifecycle refinement, not a persistence or wire-protocol change. Existing pessimistic RC transactions retain their transaction and rollback semantics. SI and optimistic behavior remain outside the new fast path. Mixed-version operation follows the existing RC lifecycle protocol’s maintenance-window requirement.

## Resource and performance model

The serialized publication section is reduced from physical copy plus validation to validation and publication. Validation cost is bounded by the committed change range since the copy timestamp, not by the source table size. Under source churn, a retry may repeat physical preparation; this is accepted because correctness requires a fresh copy. Independent operations no longer hold the global gate through physical copy, which is the reported scalability bottleneck.

## Verification

- Deterministic barrier tests prove independent physical work starts while the global gate is held elsewhere.
- Lock-order tests prove the gate is acquired before source locks.
- Source-churn tests reject replacement, row changes, and same-ID definition changes after physical preparation.
- Transaction-local relation tests skip committed CDC safely.
- Index-disappearance tests convert stale metadata into `ErrTxnNeedRetryWithDefChanged`.
- A 500-session workload proves independent ALTERs complete without lock-wait or connection-loss failures.

## Open decision

Independent design review and approval of this exact revision are required before implementation approval.
