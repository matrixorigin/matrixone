# ALTER TABLE AUTO_INCREMENT Request-Changes Design

## Goal

Make `ALTER TABLE t AUTO_INCREMENT = N` satisfy the invariant that, after the ALTER serializes successfully, no later automatically generated value can commit below `max(N, max-existing+1)`, including multi-CN, optimistic transactions, explicit transactions, rollback, cancellation, and COPY ALTER paths.

## Existing foundation

The current PR head already replaces best-effort CN broadcasts with a durable table-definition version fence. CN writes carry `(table_def_version, table_def_version_known)` and TAE validates known versions at `PrepareCommit`. CN increment caches are version-scoped, and COPY planning plus post-copy reconciliation applies `max(requestedOffset, copiedMax)`.

This foundation is retained. Broadcast callbacks remain cleanup/optimization only and are never correctness barriers.

## Remaining correctness gap

In one explicit transaction, `ALTER AUTO_INCREMENT` stages the new offset through its `TxnOperator`, but a subsequent INSERT rebuilds its new-version increment cache through `GetColumns(..., nil)`. It therefore sees the old committed offset. TAE accepts the transaction-local schema version, allowing the ALTER and a too-low generated value to commit together.

## COPY `ReplaceDef` version propagation

The TN fence compares the known `TableDef.Version` carried by a CN write with
TAE's `Schema.Version`. COPY-style ALTER updates the existing table in place:
disttae applies `ReplaceDef`, increments `mo_tables.rel_version`, and rewrites
the catalog rows. The old path explicitly removed `ReplaceDef` from the ALTER
payload sent to TN. When no other physical ALTER request remained, TN created no
new table MVCC node and kept version zero. Every later write then presented the
committed catalog version (for example 1) against TAE version zero and was
rejected with `ErrTxnNeedRetryWithDefChanged`.

Keep `ReplaceDef` in the existing ALTER payload. TN treats it as a no-op schema
mutation because executable-definition replacement remains CN-owned, while the
normal TAE alter path creates one MVCC node and performs its existing single
version increment. If the COPY ALTER also carries rename or constraint actions,
they reuse that node and do not increment again. This aligns the two version
domains without weakening the fence or adding a protobuf field. The CN strips
the full executable definition from the TN copy of `ReplaceDef`, since TN needs
only the existing kind and table identity; this avoids redundant wire and log
amplification.

For rolling upgrades, checkpoint replay already rebuilds TAE schema versions
from `mo_tables`. A legacy `ReplaceDef` can also remain only in the WAL tail,
however. WAL replay therefore captures committed, non-tombstone `mo_tables`
appends and reconciles each existing table to the maximum replayed
`rel_version`. The reconciliation runs only after that replay transaction
commits, is discarded on rollback, and never lowers a schema version. This
repairs pre-fix state without weakening the runtime DML fence.

This recovery repair is not a live mixed-version compatibility protocol. During
a rolling upgrade, COPY/ALTER DDL must be quiesced while any pre-fix CN can
still issue it, or all pre-fix CNs must be removed before version-aware CN
traffic resumes. If an old CN issues a `ReplaceDef`-only ALTER after the TN has
already upgraded, the resulting legacy drift is repaired only by the final TN
restart and WAL replay; known-version DML must not bypass the fence before that
restart. Consequently this PR does not claim zero-downtime mixed-version safety.

## Architecture

Add transaction-private reset caches to incrservice.

- Committed caches remain keyed by table ID and table-definition version.
- A successful `SetOffset` builds a private cache using the ALTER transaction's visibility and stores it under `(txnID, tableID)`.
- `GetLastAllocateTS` and `InsertValues` receive the current `TxnOperator`. When that transaction owns a reset cache, they use it exclusively; they must never fall back to a committed cache.
- Private caches are never published globally. They are retired when the transaction closes, regardless of commit, rollback, or unknown outcome. A later transaction lazily rebuilds a committed cache from durable metadata.
- If an ALTER statement fails after installing a reset cache, compile explicitly discards it before the explicit transaction can execute another statement.
- The existing TN table-version fence remains the cross-CN and optimistic-mode serialization barrier.

### DML-first serialization

The version fence must cover both prepare orders. When ALTER prepares first,
the existing fence rejects a later old-version DML. When a known-version DML
prepares first, TAE records its accepted prepare timestamp on the table. An
AUTO_INCREMENT ALTER whose snapshot start precedes that watermark retries, so
the CN repeats the MAX query from a snapshot that includes the earlier DML.

This watermark is a TN-local serialization fact, not persisted allocator
metadata. TAE's pre-WAL prepare stage is ordered, so an accepted DML publishes
before a later ALTER checks; an ALTER ordered first checks before the DML and
the schema fence rejects that DML. A rejected stale DML never publishes. A
same-transaction DML plus AUTO_INCREMENT ALTER is one serialization unit and
does not fence against itself.

The watermark advances monotonically. If an accepted/prepared DML later
aborts, it can conservatively retry each older ALTER snapshot, but cannot admit
an unsafe lowering.

Prepared 2PC transactions are a separate recovery case: their prepare records
survive a TAE restart and their final commit or rollback may arrive later. WAL
replay therefore reconstructs a table-scoped set of unresolved prepared DML
transaction IDs from the serialized `TxnMemo` dirty-table tree. The replay
format does not retain `table_def_version_known`, so all replayed prepared DML
tables are registered conservatively. Registration is idempotent and the set is
bounded by the unresolved prepared transactions touching that table.

Commit or rollback removes the transaction only after its replay command has
successfully applied. A committed decision first records its commit visibility
timestamp in the ordinary DML watermark while holding the same lock that
protects the unresolved set, then removes the transaction. A rollback only
removes the transaction. At AUTO_INCREMENT ALTER prepare, any unresolved
prepared DML forces retry; after resolution, a committed watermark newer than
the ALTER snapshot also forces retry.

This protocol does not compare clocks from separate domains. MatrixOne's TN
transaction manager assigns snapshot and visibility timestamps from the same
monotonic HLC timeline. Ordered replay-logtail application guarantees that when
an ALTER snapshot starts at or after the committed visibility timestamp, the
resolved rows are already visible to its MAX reconciliation. Publishing the
commit timestamp and removing the unresolved transaction under one lock closes
the decision race without a pending bit or an ALTER-owned timestamp.

Legacy live writers with `table_def_version_known=false` remain the documented
rolling-upgrade compatibility boundary. Recovery is deliberately more
conservative because the known bit is absent from durable replay context.

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
- Rolling upgrades must quiesce COPY/ALTER DDL until old CNs are gone, and a TN restart/replay is required after any old-CN `ReplaceDef` tail; runtime forward-version mismatches remain retry errors.

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
- RED/GREEN coverage that COPY `ReplaceDef` reaches TN and advances the TAE table version exactly once.
- Recovery coverage for a legacy `mo_tables` WAL tail, `Schema.Extra` preservation, and atomic recovery snapshots under `-race`.
- Real-MO regression for COPY ALTER followed immediately by INSERT, UPDATE, and DELETE on a table whose only AUTO_INCREMENT column is the hidden fake primary key.

## Decision log

- Use `TableDef.Version`, not a new auto-increment epoch: the durable fence already exists and a dedicated epoch would not remove the transaction-local visibility problem.
- Do not implement freeze/ack/update/unfreeze: correctness must not depend on all CNs being reachable.
- Do not promote private caches on commit: lazy durable rebuild is simpler and prevents callback ordering from affecting correctness.
- Do not expand this PR into a cluster capability framework; document and test the known/unknown compatibility boundary instead.
- Do not bypass or scope down the TN version fence to hide COPY mismatches; the existing TAE relation must consume the version-advancing ALTER that CN already records.
- Do not accept a forward DML version mismatch as an upgrade shortcut; reconcile only from committed durable catalog rows during recovery.
