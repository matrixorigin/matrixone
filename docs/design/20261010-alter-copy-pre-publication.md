# ALTER COPY pre-publication design

- Status: proposed; independent design approval required before implementation approval.
- Issue: #29208.
- Implementation PR: #29798.
- Design trigger: materially changes the COPY ALTER concurrency, retry, and lifecycle model.
- Supersedes: none; extends the RC lifecycle contract in [20260929-rc-lifecycle-protocol.md](20260929-rc-lifecycle-protocol.md).

## Problem

`ALTER TABLE ... COPY` currently acquires the global SNAPSHOT lineage gate before physical preparation. The gate is transaction-scoped and intentionally broad, so unrelated branch ALTERs serialize behind each other and can eventually return lock-wait timeouts or connection losses under high concurrency.

The correctness invariant is unchanged: publication must not expose a stale or inconsistent source snapshot, and every source or lineage mutation that could invalidate the copy must be detected before publication.

## Goals

- Let independent ALTER COPY operations prepare physical work without waiting on each other’s global lineage gate.
- Preserve atomic publication and global ordering for genuinely conflicting lifecycle owners.
- Keep speculative preparation from retaining source database or source catalog locks.
- Use an existing authoritative source-commit frontier instead of constructing a full CDC replay pipeline.
- Make post-physical source races terminal so RC statement retry cannot repeat a full copy while retaining publication locks.
- Bound the redesign to ALTER COPY and existing lifecycle protocols.

## Non-goals

- Remove or weaken the global SNAPSHOT lineage gate.
- Change DATA BRANCH, Snapshot, PITR, restore, or GC ownership.
- Introduce a new distributed protocol or persistence format.
- Change unrelated DDL paths.

## Selected design

### Transaction and mode coverage

For a non-temporary relation, all transaction modes use the same ordering: physical preparation first, then publication admission and validation. Mode-specific admission is:

- pessimistic RC uses broad-table lifecycle admission;
- other pessimistic modes use the pessimistic lineage-owner gate;
- optimistic transactions perform no prepublication source locks and still validate the resolved source before publication.

Autocommit owns transaction cleanup after a terminal conflict. An explicit transaction remains failed and must be rolled back by its user; the ALTER statement never silently retries it.

### Pre-publication phase

The transaction resolves the source and records the copy timestamp, then performs transaction-local preparation:

1. create the temporary target relation;
2. copy source rows at the recorded timestamp;
3. reconcile auto-increment state;
4. clone unaffected indexes.

The temporary CREATE uses `WithDisableLock`. It therefore does not acquire the source database shared lock or temporary target table exclusive lock. Row copy, allocator reconciliation, and index cloning operate through the current transaction and can consume engine/workspace resources, but do not retain source database/catalog locks. No global lineage gate is held.

### Publication phase

The transaction acquires mode-specific lifecycle admission, locks the source relation, and re-resolves the authoritative source. Before publication it rejects:

- planned or resolved source relation replacement;
- same-ID table-definition version changes;
- committed source changes after the copy timestamp;
- foreign-key state changes;
- stale or late lineage participation;
- same-statement lineage column replacement.

These post-physical source races return `ErrTxnWriteConflict`, not `ErrTxnNeedRetryWithDefChanged`. They are terminal for the transaction, so the RC statement-retry transition cannot retain the global gate and repeat physical preparation. Lock contention and errors that occur before speculative effects remain under their existing retry semantics.

### Source-change detection

For a relation that existed before the current transaction, publication calls `SourceCommitTS(ctx, copyTS)`. This is an existing authoritative lower-bound capability, not a CDC handle:

- transaction-local source writes fail closed;
- each partition member supplies its source commit frontier and fails closed on unavailable state;
- a combined relation aggregates member maxima and short-circuits as soon as one member exceeds the copy timestamp;
- a result greater than the copy timestamp means the copy is stale.

Transaction-local relations are detected through `RelationCreatedInCurrentTxn` and skip the committed-source probe because their whole life is owned by this transaction.

## Lock and retry model

- Pre-publication holds no source database lock, source catalog lock, source physical-table lock, temporary-target catalog lock, or global lineage gate.
- Publication order remains mode-specific lifecycle admission, source locks, then final lineage publication. It preserves existing `G -> D` ownership for genuinely conflicting owners.
- The speculative CREATE no longer owns `D S`, so it cannot participate in the prior `ALTER(D S) waits for G X` versus `DROP DATABASE(G S) waits for D X` reciprocal wait.
- Nested errors, cancellation, or a terminal conflict return through the existing transaction cleanup owner: autocommit rolls back; explicit transactions fail and wait for user rollback.
- A terminal post-physical conflict is deliberately not statement-retryable, preventing a second physical copy under retained locks.

## Resource and performance model

The serialized source probe costs `O(P)` `SourceCommitTS` calls for a partitioned source, where `P` is the number of visible members. A member’s worst case is source-state dependent:

- one workspace scan under the transaction mutex, fail-closed on a local write;
- one in-memory replay-row scan, `R` entries;
- one object-index iteration, `O` visible objects;
- appendable objects may read commit-ts and abort columns for each visible block;
- non-appendable objects may read per-block commit-ts zonemaps.

The copy timestamp is a short-circuit threshold, not an upper bound on rows or objects. A state-start boundary, in-memory row, or object candidate greater than the threshold stops that member before later object I/O. An unchanged large source can still scan in-memory rows and object metadata; this is not a changes-only cost.

The remaining gate-held budget is therefore bounded by visible partition state and storage I/O, not by copy size. The focused unchanged benchmark `BenchmarkSourceCommitTSAtUnchanged250KObject` models one 250,000-row non-appendable object with a threshold after all row commits. On Apple M5 (`darwin/arm64`, 10 GOMAXPROCS), 20 iterations measured `44075 ns/op`, `1125 B/op`, and `4 allocs/op` through the commit-ts zonemap path. This is object-layout evidence, not a full multi-object production benchmark; additional storage layouts should be measured if reviewers require a broader envelope.

## Alternatives considered

1. **Keep gate-before-copy.** Correct, but independent ALTERs serialize and can time out under high concurrency.
2. **Lock only the source table.** Insufficient because the global lineage gate also orders branch, Snapshot, PITR, restore, and GC owners.
3. **CDC replay after copy.** Rejects committed changes, but `CollectChanges` constructs full replay-row/object handlers and is not a bounded existence probe.
4. **Speculative preparation plus `SourceCommitTS` publication validation.** Uses the existing authoritative frontier and selected.

## Compatibility and rollout

This is an internal lifecycle refinement, not a persistence or wire-protocol change. Pessimistic RC, pessimistic non-RC, and optimistic transactions follow the mode-specific admission described above. Mixed-version operation follows the existing lifecycle protocol’s maintenance-window requirement.

## Verification

- Deterministic barrier tests prove independent physical work starts while the global gate is held elsewhere.
- Unit tests prove the temporary CREATE options disable nested locking.
- Unit tests prove unchanged, changed, provider-failure, and current-transaction source behavior.
- Unit tests prove partitioned frontiers return the exact maximum, short-circuit, and fail closed for missing member providers.
- Unit tests prove post-physical replacement, definition, and data races return a terminal write conflict and are not classified as RC statement retries.
- A 500-session one-row workload proves independent ALTERs complete without lock-wait or connection-loss failures.
- `BenchmarkSourceCommitTSAtUnchanged250KObject` measures the unchanged 250,000-row object frontier at `44075 ns/op`, `1125 B/op`, and `4 allocs/op`.

## Open decision

Independent design review and approval of this exact revision are required before implementation approval.
