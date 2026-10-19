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

`ErrTxnWriteConflict` is transaction-fatal in the frontend rollback policy. Autocommit and explicit transactions therefore roll back through the frontend transaction owner; the ALTER statement never silently retries the conflict.

### Pre-publication phase

The transaction resolves the source and records the copy timestamp, then performs transaction-local preparation:

1. create the temporary target relation;
2. copy source rows at the recorded timestamp;
3. reconcile auto-increment state;
4. clone unaffected indexes, including lock-disabled cleanup of transaction-created replacement hidden tables.

The temporary CREATE, row copy, and nested replacement-index cleanup use `WithDisableLock`, so nested execution acquires neither pre-pipeline metadata/table locks nor row-lock operators. The lock-free policy is a property of the whole statement tree, not only the statements the compiler itself issues: the internal SQL executor carries it on the nested statement context, so service-owned catalog SQL triggered by the same statement — partition-service metadata writes and auto-increment allocator column writes, both of which run on service executors with fresh statement options — inherits `WithDisableLock` too. Without that inheritance a partitioned or AUTO_INCREMENT preparation would retain public catalog locks that publication admission cannot certify as private, reintroducing FastFail failures for unrelated publishers; the rows those services write address keys unique to the in-flight private preparation, so the same isolation argument as the private CREATE applies. The temporary CREATE also skips transient index CDC and idxcron task registration; because no tasks are registered, ALTER skips their catalog cleanup deletes, while post-rename publication retains the existing final task-registration path. The registration opt-out is a property of that one nested compiler generation only: `Compile.clear` resets the flag with the other per-statement flags before the object returns to the global reuse pool, and `NewCompile` re-initializes it, so the suppression cannot reach an unrelated later `CREATE TABLE`. Row copy, allocator reconciliation, and index cloning operate through the current transaction and can consume engine/workspace resources; they may retain table locks on transaction-created replacement or index tables, but not source database/catalog/name locks. No global lineage gate is held.

In pessimistic RC the copy timestamp is recorded by advancing the transaction snapshot through `databranchutils.AdvanceLineageSnapshot` before preparation begins. The workspace owns that advancement — `Workspace.AdvanceSnapshot` relocates transaction-local tombstones to their post-flush row locations before the new snapshot is used — so the advanced snapshot is retained on every ALTER return path (success, error, and cleanup boundaries). ALTER never rewinds the transaction timestamp alone after the workspace has transferred deletes; that would leave the workspace and the snapshot in different generations and resurrect relocated deletes belonging to earlier uncommitted DML in the same transaction (the ownership contract established by #29539, whose ALTER-side timestamp-only rewind was removed on main).

### Publication phase

The transaction acquires mode-specific lifecycle admission, pins replacement endpoint names, locks the source relation, and re-resolves the authoritative source. In pessimistic RC the broad SNAPSHOT gate is therefore acquired before the source and temporary-target catalog-name pins. ALTER records the retained lock-table set before preparation and permits gate waiting only when every retained lock table was acquired after that baseline and is certified by the workspace as transaction-created private preparation. Any pre-existing, source, catalog, or otherwise unprovable lock keeps the fast-fail gate policy. Post-gate replacement-name admission waits, but cannot form a `G -> name -> G` cycle: a transaction that already retained those name locks is required to fast-fail rather than wait for `G`. Independent ALTERs on different tables can therefore physically prepare and wait at that gate without creating a reverse edge. Before publication it rejects:

- planned or resolved source relation replacement;
- same-ID table-definition version changes;
- committed source changes after the copy timestamp;
- foreign-key state changes;
- stale or late lineage participation;
- same-statement lineage column replacement.

These post-physical source races return `ErrTxnWriteConflict`, not `ErrTxnNeedRetryWithDefChanged`. Post-copy lifecycle admission, source-lock refresh, and final validation errors that would otherwise request an RC statement retry are converted to the same terminal conflict. They are terminal for the transaction, so the RC statement-retry transition cannot retain the global gate and repeat physical preparation. Lock contention and errors that occur before speculative effects remain under their existing retry semantics.

### Source-change detection

For a relation that existed before the current transaction, publication calls `SourceCommitTSAfterCopy(ctx, copyTS)`. This uses the existing authoritative lower-bound frontier while deliberately excluding only the current transaction's workspace writes; local writes to one row or partition never imply that another transaction did not change another row or partition after the copy:

- current-transaction workspace writes were visible to the copy and are excluded from ALTER's committed-change comparison;
- each partition member supplies its source commit frontier and fails closed on unavailable state;
- a combined relation aggregates member maxima and short-circuits as soon as one member exceeds the copy timestamp;
- in-memory rows and visible data objects contribute their user-data commit timestamps;
- retained tombstone entries contribute row-level non-aborted DELETE commit timestamps. Appendable tombstones read commit-ts and abort columns with the same snapshot semantics as the delete reader; non-appendable tombstones read their commit-ts zonemaps. Soft-deleted tombstones remain proof because logical pruning is not retention truncation, while actual partition-state truncation is represented by the state-start floor;
- unavailable tombstone files or commit-ts metadata fail closed;
- a result greater than the copy timestamp means the copy is stale.

Transaction-local relations are detected through `RelationCreatedInCurrentTxn` and skip the committed-source probe because their whole life is owned by this transaction. The original `SourceCommitTS` capability continues to fail closed for local writes so its async-index callers do not mistake transaction-visible state for a committed coverage frontier; ALTER's after-copy capability shares the partition-state reader but bypasses only that local-write failure.

## Lock and retry model

- Pre-publication holds no source database lock, source catalog lock, source physical-table lock, temporary-target catalog lock, or global lineage gate. It may retain only locks on transaction-created replacement or index tables.
- Publication order remains mode-specific lifecycle admission, replacement-name admission, source locks, then final lineage publication. It preserves existing `G -> D` ownership for genuinely conflicting owners.
- The speculative CREATE no longer owns `D S`, so it cannot participate in the prior `ALTER(D S) waits for G X` versus `DROP DATABASE(G S) waits for D X` reciprocal wait.
- Nested errors, cancellation, or a terminal conflict return through the existing frontend transaction cleanup owner. A terminal write conflict rolls back the whole transaction in both autocommit and explicit-transaction mode.
- A terminal post-physical conflict is deliberately not statement-retryable, preventing a second physical copy under retained locks.

## Resource and performance model

The serialized source probe costs `O(P)` `SourceCommitTS` calls for a partitioned source, where `P` is the number of visible members. A member’s worst case is source-state dependent:

- no workspace scan is needed to exclude writes owned by the copying transaction; the original frontier method still fails closed for async-index callers;
- one in-memory replay-row scan, `R` entries;
- one data-object index iteration, `O` visible data objects;
- one tombstone-object index iteration, `T` retained tombstone objects (including soft-deleted entries);
- appendable data and tombstone objects may read commit-ts and abort columns for each block visible at the snapshot;
- non-appendable data and tombstone objects may read per-block commit-ts zonemaps.

The copy timestamp is a short-circuit threshold, not an upper bound on rows or objects. A state-start boundary, in-memory row, or object candidate greater than the threshold stops that member before later object I/O. An unchanged large source can still scan in-memory rows and retained data/tombstone object metadata; this is not a changes-only cost.

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
- Pessimistic-RC integration tests prove both a held explicit publisher and an overlapping autocommit publisher let an independent table wait at the gate and finish after release. The held-publisher regression covers three victim layouts — an unaffected IVF-FLAT index, a HASH-partitioned source, and an AUTO_INCREMENT source — proving the lock-free policy reaches partition-service and allocator catalog SQL.
- Unit tests prove the temporary CREATE options disable nested locking.
- Unit tests prove unchanged, changed, provider-failure, current-transaction, and committed-change-despite-local-write source behavior.
- Unit tests prove partitioned frontiers return the exact maximum, short-circuit, check every member after copy, and fail closed for missing member providers.
- Unit tests prove in-memory, persisted appendable, and pruned persisted tombstone delete frontiers detect a post-copy source change, and that unavailable tombstone proof fails closed.
- Unit tests prove post-physical replacement, definition, data, admission-retry, and source-lock-retry races return a terminal write conflict and are not classified as RC statement retries.
- Unit tests prove ALTER COPY never rewinds the transaction timestamp after the workspace has advanced: the workspace-owned advanced snapshot is retained on the workspace-advance-failure and post-advance physical-copy-failure return paths, and any `SetSnapshotTS` rewind fails the test.
- A pessimistic-RC integration test with real storage proves uncommitted in-memory DELETEs whose committed rows a forced DN flush relocated stay deleted across the ALTER COPY return boundary, inside the same transaction and after commit.
- Frontend tests prove `ErrTxnWriteConflict` is classified as a whole-transaction rollback error.
- A 500-session one-row workload proves independent ALTERs complete without lock-wait or connection-loss failures.
- `BenchmarkSourceCommitTSAtUnchanged250KObject` measures the unchanged 250,000-row object frontier at `44075 ns/op`, `1125 B/op`, and `4 allocs/op`.

## Open decision

Independent design review and approval of this exact revision are required before implementation approval.
