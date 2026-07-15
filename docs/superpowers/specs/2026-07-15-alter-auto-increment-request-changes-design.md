# ALTER TABLE AUTO_INCREMENT Request-Changes Design

## Goal

Make `ALTER TABLE t AUTO_INCREMENT = N` satisfy the invariant that, after the ALTER serializes successfully, no later automatically generated value can commit below `max(N, max-existing+1)`, including multi-CN, optimistic transactions, explicit transactions, rollback, cancellation, and COPY ALTER paths.

## Existing foundation

The current PR head already replaces best-effort CN broadcasts with a durable table-definition version fence. CN writes carry `(table_def_version, table_def_version_known)` and TAE validates known versions at `PrepareCommit`. CN increment caches are version-scoped, and COPY planning plus post-copy reconciliation applies `max(requestedOffset, copiedMax)`.

This foundation is retained. Broadcast callbacks remain cleanup/optimization only and are never correctness barriers.

## Remaining correctness gap

In one explicit transaction, `ALTER AUTO_INCREMENT` stages the new offset through its `TxnOperator`, but a subsequent INSERT rebuilds its new-version increment cache through `GetColumns(..., nil)`. It therefore sees the old committed offset. TAE accepts the transaction-local schema version, allowing the ALTER and a too-low generated value to commit together.

## Architecture

Add transaction-private reset caches to incrservice.

- Committed caches remain keyed by table ID and table-definition version.
- A successful `SetOffset` builds a private cache using the ALTER transaction's visibility and stores it under `(txnID, tableID)`.
- `GetLastAllocateTS` and `InsertValues` receive the current `TxnOperator`. When that transaction owns a reset cache, they use it exclusively; they must never fall back to a committed cache.
- Private caches are never published globally. They are retired when the transaction closes, regardless of commit, rollback, or unknown outcome. A later transaction lazily rebuilds a committed cache from durable metadata.
- If an ALTER statement fails after installing a reset cache, compile explicitly discards it before the explicit transaction can execute another statement.
- The existing TN table-version fence remains the cross-CN and optimistic-mode serialization barrier.

The allocator continues to serialize old local allocations, force-reset, and private-cache allocation through its existing FIFO. Private-cache construction occurs only after the force-reset action completes.

## COPY behavior

COPY keeps the current implementation:

1. Discard inherited source preallocation metadata.
2. Apply the requested internal offset unconditionally.
3. Copy rows.
4. Reconcile the destination with `max(requestedOffset, copiedMax)` in the ALTER transaction.
5. Use the same transaction-private cache lifecycle as inplace ALTER.

## Failure semantics

- Pre-canceled context: no store mutation or private cache is installed.
- Cache construction failure: close any partially constructed cache, fail the ALTER, and rely on statement rollback for the staged store mutation.
- Failed ALTER after `SetOffset`: synchronously discard the private cache.
- Statement/transaction rollback: no private cache survives; committed cache state is rebuilt from committed metadata.
- Commit unknown: private cache is retired, never promoted.
- Partial or unreachable CN: no acknowledgement is required. A stale known-version write is rejected at TN prepare and must retry with the new definition.
- Old writers with `table_def_version_known=false` remain a rolling-upgrade limitation. The PR must document that strict fencing requires version-aware CN writers; it must not claim mixed-version safety.

## Compatibility

The public incrservice Go interface changes to pass `TxnOperator` into allocation timestamp lookup and value generation, and gains an explicit reset-discard operation. All mocks and call sites must be regenerated or updated. Wire fields remain additive and are regenerated from `proto/api.proto` after rebasing.

## Required verification

- RED/GREEN regression for same-transaction ALTER then INSERT.
- Failed ALTER statement followed by INSERT in the same transaction.
- Commit and rollback cleanup of private caches.
- Two incrservice instances with stale ranges and TN rejection/retry behavior.
- Pessimistic and optimistic commit fencing.
- Cancellation before and after staged reset.
- Current COPY plan, compile, and distributed BVT cases.
- Version propagation through memory batch, S3/flush, and compaction paths.
- Race tests for incrservice lifecycle and allocator changes.
- Rebase conflict resolution against current `upstream/main`, followed by protobuf regeneration.

## Decision log

- Use `TableDef.Version`, not a new auto-increment epoch: the durable fence already exists and a dedicated epoch would not remove the transaction-local visibility problem.
- Do not implement freeze/ack/update/unfreeze: correctness must not depend on all CNs being reachable.
- Do not promote private caches on commit: lazy durable rebuild is simpler and prevents callback ordering from affecting correctness.
- Do not expand this PR into a cluster capability framework; document and test the known/unknown compatibility boundary instead.
