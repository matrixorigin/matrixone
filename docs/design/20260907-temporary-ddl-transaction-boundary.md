# Temporary-table DDL transaction boundary

- Issue: https://github.com/matrixorigin/matrixone/issues/28255
- Status: Implemented; final acceptance evidence recorded below.
- Revision: 3 (2026-09-07)
- Implementation base: `6eee64625e7e2cefd0f3dfeb61606f637111e057`, the
  freshly fetched `up/main` when the worktree was created. Third-round review
  fetched `up/main` at `3d67664696e48cec0efc84308fb7de1cfda2f7db`;
  the two newer commits do not change this feature's ownership contracts.
- Workspace: `m-28255`; implementation PR: none.

## Contract

A successful temporary-table CREATE or DROP remains effective after rollback.
Neither statement commits the surrounding transaction. Temporary-table DML,
including DML after CREATE, remains transactional. Permanent-table DML in the
same transaction retains its existing isolation and atomicity.

MySQL documents this distinction in
[Statements That Cause an Implicit Commit](https://dev.mysql.com/doc/refman/8.4/en/implicit-commit.html).
The issue supplies observations from MySQL 8.3.0 and MatrixOne. The fixed MO
behavior was independently exercised in this worktree; no local MySQL comparison
server was started.

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

## Approved design, revision 3

The user requested completion of the design and its implementation on 2026-09-07.
This revision resolves the ownership decisions before production editing. Design
review: PASS for the architecture below; implementation evidence is recorded below.
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
restored on every return, including panic. The schema callback converts panic
to an error because ExecTxn rolls back callback errors, but does not recover
callback panics itself. No SQL text replay is involved.

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

Revision 3 review: the physical name is
`__mo_tmp_<session>_<database>_<generation>_<alias>`. Keeping the original
hidden-index alias at the end preserves existing UUID-based index classification
and nullable UNIQUE semantics. GC still extracts only the owning session prefix.
The frontend resolves the current session's exact physical root identity as
temporary, including CTAS INSERT targets. Such references and their hidden-index
references use the existing NotLockMeta capability: their schema is already
committed and their name cannot be accessed by another user session. Permanent
source references retain normal metadata locks. This avoids retrying CTAS against
its own independently committed catalog row without advancing the data snapshot.

### DROP, generations and cleanup

DROP resolves and validates the real relation first. A successful temporary DROP
removes its root and hidden aliases outside transaction undo and retires the root
physical identity. It does not execute physical DROP in the parent workspace.
The parent may finish its buffered/spilled writes normally; the retired object is
inaccessible by the session name. Cleanup runs after transaction finalization,
outside the transaction-handler mutex. Both COMMIT and ROLLBACK then reclaim the
same retired root using the existing physical DROP path, which owns its indexes.

A new CREATE uses the existing session-prefixed physical name format with a fresh
UUID prefix on the alias portion. GC can still extract the same owning session.
No name is reused by another generation. Retryable cleanup cannot delete a
replacement or reinsert an old alias. Retired roots are omitted from migration,
but reset and disconnect retain their cleanup ownership. Reset failure must not
restore a retired root as a live alias.

The session admits at most 1024 live aliases plus retired roots on the new
path, checked before CREATE; inherited legacy excess can drain but cannot grow.
Thus an arbitrarily long transaction cannot accumulate an unbounded retirement
queue. Each root's data continues to use existing transaction resource controls.
No new background worker or retry loop is added. Failed cleanup retains identity
for the next idle boundary/reset/close and logs the deferred reclamation. It must
not return a retryable SQL error after an already successful data COMMIT. Existing disconnect GC is the final orphan owner after
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

ALTER, TRUNCATE, CLONE/data-branch extensions, durable catalog formats and
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

## Change map and final review

| Closure | Risk / owners and reverse consumers | Evidence |
|---|---|---|
| Independent CREATE and CTAS | R3: compile stages schema, executor finalizes its transaction, Session owns the published root; cloned plans/process state are restored on success/error/panic | Compile failure/commit/CTAS/panic tests; SQL rollback, CTAS source workspace, CTAS duplicate failure + immediate reuse |
| Alias retirement and physical reclamation | R3: Session removes logical identity, existing physical DROP owns root + children; idle transaction boundary, reset, migration and disconnect consume distinct live/retired state | Session rollback/replacement/capacity/reset/retry tests; DROP after pending DML then COMMIT and ROLLBACK |
| Schema visibility and binding | R3: CN name/ID cache and TN schema access admit only newer committed temporary schema for the same tenant; physical roots bind through exact session ownership, hidden references inherit metadata-lock exemption | CN marker/name/tenant controls, TN uncommitted/dropped/tenant/persistent controls; SI two-session SQL and nullable UNIQUE regression |
| Rollout and delivery | R2: protocol 54 enables top-level behavior; old gate and internal sessions keep transactional DDL; native/protobuf formats unchanged | Protocol 53/54 unit control, owning-package normal/race, native build, existing temporary-table suite |

All changed hunks, including untracked new source/tests, were reviewed against
base `6eee64625e7e2cefd0f3dfeb61606f637111e057` and design commit `4a8b609ba6`.
The delivery scope includes the design and all local changes, not only the commit.
No PR or push is part of this request.

Q1: schema transaction owns all uncommitted catalog/index/autoincrement writes;
a committed or ambiguous root transfers to the Session. Parent DML retains its
original workspace owner. Retirement never makes a physical generation name
reusable. Existing DROP owns child-index cleanup, including partial retry.

Q2: no storage operation holds the session mutex or transaction-handler mutex.
Independent schema work uses the request context and executor transaction
finalization. Schema callback panic becomes a rollback error; CTAS panic retires
the root before frontend recovery finalizes the parent. Idle reclamation has a
one-minute independent deadline and preserves ownership on failure. An
uncommitted parent database is rejected before independent DDL lock acquisition.

Q3: admission is bounded by 1024 live aliases plus retired roots. The new path
publishes only roots; existing internal/legacy hidden aliases also count against
admission. There is no worker, unbounded retry or per-row state addition. CREATE
adds one independent schema transaction and a logtail barrier; DROP moves its
physical work to transaction completion. Catalog fallback runs only on misses.
Physical-ownership lookup adds one existing session-mutex critical section per
resolved table. Data memory/disk usage remains under existing transaction controls.

Operator signal/pipeline, persistent formats, protobuf, allocator and index-plugin
registration interfaces are unchanged. The index-name compatibility counterexample
was fixed at the naming producer; no new algorithm dispatch was added. Session
serialization plus existing map/catalog locks remain the concurrency model.

## Validation record (2026-09-07)

Environment: Go 1.26.4, macOS arm64. `make cgo` rebuilt worktree-native artifacts;
all Go evidence uses `.agents/skills/mo-dev/scripts/mo-cgo-test`. `make build`
produced the final service binary. Duplicate native library/rpath warnings were
non-fatal. Tests use an isolated CN/TN/logservice at SQL port 28455 and an isolated
mo-tester directory; other local services were not modified.

- Owning normal: `-count=1 ./pkg/sql/compile ./pkg/frontend ./pkg/vm/engine/disttae ./pkg/vm/engine/tae/catalog`.
- Owning race: same four packages with `-race -count=1`; compile/frontend rerun after final failure-path edits.
- Exact lifecycle stress: `TestSessionTemporaryDDLLifetime` and `TestSessionResetTempTablesIsSynchronousAndRetryable`, each separately under `-race -count=100`. Individual JSON measurement was below timer resolution, selecting the capped count 100.
- Real-service regression: full `test/distributed/cases/table/temporary_table` directory in normal comparison mode. Existing tests and expected results are unchanged. New CTAS failure expectations were derived from duplicate-PK semantics, not accepted from result generation.
- Initial full regression exposed a real nullable-UNIQUE failure caused by the generation suffix. Generation-prefix correction passed the unchanged case. Initial CTAS regression exposed metadata-lock retries; exact physical ownership binding fixed it.
- The focused new SQL covers permanent DML atomicity, definition survival/removal, same-name generations, prepared CREATE, inline indexes, CTAS data ownership/failure, and SI snapshot preservation. Existing fixtures cover session isolation, aliases, fulltext and IVF indexes.

Three requested branch-review rounds were completed after the implementation
commit. Round 1 made an invalid internal-executor capability a diagnostic error
instead of a type-assertion panic. Round 2 aligned TN with CN by requiring both
the temporary catalog marker and physical-name identity. Round 3 made the CTAS
failure BVT assert the stable duplicate-entry category rather than full error
rendering. Each finding was fixed in its review round and committed separately.

Final review decision: PASS, with the documented protocol and uncommitted-database
boundaries. Owning normal and race commands completed successfully. Before the
three reviews, the service passed the full temporary-table suite twice on the
same instance; after all review fixes, the final binary passed it once more:
281/281 SQL statements each time, zero failures/ignored/abnormal. Catalog checks
confirmed zero fixture tables after teardown and zero root/hidden-index tables
immediately after DROP + ROLLBACK while the fixture database was still present.
The test-owned service was stopped after verification.

Local logs
are under `/tmp/m-28255-*.log`; service logs/config/data are under
`/tmp/m-28255-service`, and tester reports under `/tmp/m-28255-tester`.
