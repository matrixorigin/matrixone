# Temporary-table DDL transaction boundary

- Issue: https://github.com/matrixorigin/matrixone/issues/28255
- Status: Design reviewed; implementation and validation pending.
- Revision: 2 (2026-09-07)
- Inspected base/head: `6eee64625e7e2cefd0f3dfeb61606f637111e057`, freshly fetched `up/main`.
- Workspace: `m-28255`; implementation PR: none.

## Contract

A successful temporary-table CREATE or DROP remains effective after rollback.
Neither statement commits the surrounding transaction. Temporary-table DML,
including DML after CREATE, remains transactional. Permanent-table DML in the
same transaction retains its existing isolation and atomicity.

MySQL documents this distinction in
[Statements That Cause an Implicit Commit](https://dev.mysql.com/doc/refman/8.4/en/implicit-commit.html).
The issue supplies observations from MySQL 8.3.0 and MatrixOne; these have not
been independently rerun in this worktree.

## Verified source evidence

1. `pkg/sql/compile/ddl.go`, `Scope.createTable`: the temporary flag changes the
   physical name, table type, index names, and session alias. Creation and CTAS
   follow-up work still execute through the compile process's transaction.
2. `pkg/vm/engine/disttae/txn_database.go`, `txnDatabase.Create` and
   `createWithID`: physical table creation and catalog/index writes belong to
   the current transaction workspace. `deleteTable` uses the same workspace
   and reads catalog row IDs through its transaction.
3. `pkg/frontend/session.go`, `recordTempTableMutationLocked`: alias changes
   have both statement and transaction undo maps. `rollbackTempTableTransaction`
   restores the transaction's original aliases.
4. `pkg/frontend/txn.go`, `rollbackUnsafe`: storage rollback is followed by
   alias rollback. Discarded DDL also advances the prepared-plan version.
5. `txnDatabase.getTableItem` explicitly looks up metadata at
   `db.op.SnapshotTS()`. Independently committing CREATE does not by itself
   make it visible to an already running SI transaction.
6. `Scope.DropTable` already omits `lockDroppedRelation` for temporary tables.
   A new design must nevertheless handle buffered/spilled DML and catalog
   lifetime; an ordinary persistent-table lock deadlock is not a proven root
   cause here.
7. `pkg/frontend/back_exec.go` forwards temporary aliases and the physical
   session ID to background sessions. There is an existing independent
   transaction execution seam, but it does not solve metadata visibility,
   CTAS source reads, or outstanding user writes by itself.
8. Session reset/close and connection migration consume the same alias-owned
   physical identities (`resetTempTables`, `dropSessionTempTables`,
   `snapshotTempTablesForMigration`, `migrateTempTables`).

The failure is therefore not confined to alias undo bookkeeping. A change
that only deletes the transaction journal would retain dangling aliases after
CREATE rollback and orphan physical tables after DROP rollback.

## Approved design, revision 2

The user requested completion of the design and its implementation on 2026-09-07.
This revision resolves the ownership decisions before production editing. Design
review: PASS for the architecture below; implementation evidence remains required.
The review is local, not an external GitHub approval.

### CREATE and CTAS

Only a top-level user compile with the optional session temporary-DDL capability
uses the new path. Internal/shared background SQL keeps its transactional behavior.
The compiler deep-copies the CREATE plan, removes CTAS data population from that
copy, and executes its existing schema/index/auto-increment creation code under
one independent transaction. A statement-local staging session retains aliases;
it does not publish changes into the real session during physical creation.
The internal executor owns rollback/commit and a committed-logtail barrier.
The original compile plan, process transaction, session and internal flag are
restored on every return, including panic. No SQL text replay is involved.

After confirmed commit and visibility, the session publishes the root alias
outside the parent transaction's undo journal. CTAS then executes the existing
INSERT helper in the original user transaction, including its source visibility,
prepared parameters, privilege checks and affected-row reporting. CTAS data
failure retires the new table; ordinary subsequent DML rollback leaves its empty
definition. A failed/ambiguous physical creation retains the exact generated root
name for cleanup without publishing an alias. Cleanup is DROP IF EXISTS, never
re-execution of CREATE or CTAS.

The compiler checks that the database ID seen by the independent transaction is
the same as in the parent before taking DDL locks. An uncommitted database or a
transaction-local replacement is rejected before physical side effects. This is
an explicit boundary of MO's transactional CREATE DATABASE extension; MySQL's
implicit-commit CREATE DATABASE cannot reach it. No parent transaction is committed
or advanced to make the independent DDL work.

### Schema visibility, not data snapshot advancement

CN normally resolves metadata at the transaction snapshot. Only when that lookup
has no table does it accept a currently committed temporary-table catalog entry
for the same account and physical identity, by name or ID. Existing visible
schemas, persistent tables, tombstones and ordinary storage fallback retain their
current behavior. CREATE waits for committed logtail publication before returning.
The parent is marked HaveDDL so its new-table work uses existing local DDL routing;
subsequent transactions use the normal catalog/logtail path.

TN likewise permits a committed, live temporary-table schema when the ordinary
snapshot lookup finds none. The same helper supplies visibility and the data and
tombstone schema. The durable temporary marker and matching tenant are mandatory;
an uncommitted creation or a committed deletion never qualifies. Row/object reads,
DML workspace ownership, write conflict checks and commit remain unchanged.

This changes CN/TN semantics, so protocol version 54 gates user CREATE and DROP.
Until the deployment gate reaches 54, user temporary DDL keeps its existing
transactional behavior. No protobuf or on-disk schema changes are introduced.
Upgrade TN and CN before activating the gate; drain active sessions before a
protocol downgrade. Old physical temporary names and migrated tables remain valid.

### DROP, generations and cleanup

DROP resolves and validates the real relation first. A successful temporary DROP
removes its root and hidden aliases outside transaction undo and retires the root
physical identity. It does not execute physical DROP in the parent workspace.
The parent may finish its buffered/spilled writes normally; the retired object is
inaccessible by the session name. Cleanup runs after transaction finalization,
outside the transaction-handler mutex. Both COMMIT and ROLLBACK then reclaim the
same retired root using the existing physical DROP path, which owns its indexes.

A new CREATE uses the existing session-prefixed physical name format with a fresh
UUID suffix on the alias portion. GC can still extract the same owning session.
No name is reused by another generation. Retryable cleanup cannot delete a
replacement or reinsert an old alias. Retired roots are omitted from migration,
but reset and disconnect retain their cleanup ownership. Reset failure must not
restore a retired root as a live alias.

The session admits at most 1024 live plus retired user-created roots on the new
path, checked before CREATE; inherited legacy excess can drain but cannot grow.
Thus an arbitrarily long transaction cannot accumulate an unbounded retirement
queue. Each root's data continues to use existing transaction resource controls.
No new background worker or retry loop is added. Failed cleanup retains identity
for the next idle boundary/reset/close and returns an error rather than claiming
successful reclamation. Existing disconnect GC is the final orphan owner after
process failure. Successful physical creation is never replayed.

### Publication, rollback and exceptional outcomes

Publishing or retiring an alias invalidates older undo entries for that alias
across the session journals. Later rollback cannot overwrite the independently
completed DDL. Unrelated/internal aliases keep their existing journals. The
session mutex protects map operations only; no storage I/O runs under it.

A committed schema followed by a visibility timeout is retained for cleanup, with
no usable alias. Unknown parent commit leaves physical retirement to the bounded
cleanup/close path; it never reconstructs an alias or retries user DML. Physical
cleanup executes in a later transaction and uses the existing storage conflict
and cancellation mechanisms. The request/cleanup deadline bounds all new I/O.

### Scope and alternatives

This is an R3 ownership refactor across frontend, compile, CN and TN catalog
visibility; the feature-design gate applies. Alternatives rejected: deleting only
alias undo (dangling names), advancing the parent snapshot (persistent-table
isolation change), committing all DDL/data independently (CTAS and DML mismatch),
and recreating schemas at rollback (late failure and replay semantics).

ALTER, TRUNCATE, internal data-branch temp tables, durable catalog formats and
index algorithm dispatch are not redesigned. Their existing paths and tests are
retained. Concurrent mutation of one session is already serialized by frontend;
metadata and session helpers additionally preserve their existing lock discipline.

## Verification and acceptance

- Focused compile tests: staged publication, original plan/process restoration,
  late schema failure, CTAS source/DML ownership, IF guards and rollout gate.
- Session tests: durable CREATE/DROP versus legacy journal rollback, generation
  replacement, capacity, retryable cleanup, reset and migration snapshots.
- CN/TN tests: newer committed temporary schema versus persistent, other tenant,
  uncommitted and deleted controls; no data snapshot advancement.
- Owning-package normal tests, focused lifecycle race checks and owning-package
  race evidence where the new shared-state closure applies.
- Real-service BVT: both issue sequences, existing-table DML rollback, CREATE
  COMMIT, DROP after DML then COMMIT, same-name recreation, indexed tables, CTAS,
  prepared execution, SI snapshot control and session isolation. Use one or two
  rows, normal comparison, teardown verification and same-instance repetition.
- Reuse existing temporary-table BVT fixtures and compile/session test seams.
  Keep internal temporary-table tests transactional; do not weaken their oracles.
- Final self-review must inspect every changed hunk, owner and reverse consumer,
  including retired identity cleanup, all terminal outcomes and delivery scope.

## Evidence status

Source inspection and existing-test inventory completed at the recorded base.
Design revision 2 closes the implementation direction; runtime acceptance is
pending. No production implementation or test pass is claimed by this document.
