# Object privilege lifecycle and DDL transaction protocol

- Status: **in review; implementation approval remains blocked until this document is approved**
- Owning issue: [#27723](https://github.com/matrixorigin/matrixone/issues/27723)
- Implementation PR: [#27745](https://github.com/matrixorigin/matrixone/pull/27745)
- Historical cleanup follow-up: [#27836](https://github.com/matrixorigin/matrixone/issues/27836)
- Scope: authorization metadata, persistent-object DDL, transaction generation/admission, prepared execution, and private restore/clone owners

## 1. Classification and evidence

This change requires design review independently of line count because it changes an authorization boundary, a distributed catalog locking protocol, transaction lifecycle/admission, and restore/clone ownership. It also crosses frontend, SQL compile, lockservice, and catalog/storage boundaries.

The observed defect is that successful `DROP TABLE`, `DROP VIEW`, and `DROP DATABASE` remove catalog objects while leaving `mo_catalog.mo_role_privs` rows keyed by those objects' logical IDs. Repeated churn creates unreachable authorization metadata. Name reuse does not currently inherit the stale grant because a new logical ID is allocated, but the metadata is unbounded and cannot be treated as harmless.

A second correctness requirement follows from fixing the first: a concurrent GRANT must not publish a privilege for an object generation after DROP has selected that generation for deletion. Conversely, DROP must remove any GRANT that linearized before it.

## 2. Definitions and invariants

- **Logical object identity**: the stable catalog ID used by authorization metadata (`obj_id`). Physical replacement during ALTER/TRUNCATE does not create a new authorization object when the logical ID is preserved.
- **Object generation**: one committed existence interval of a database/relation logical identity.
- **Lifecycle owner transaction**: the transaction that creates, replaces, or drops the persistent object and owns all catalog side effects until commit/rollback.
- **Private owner transaction**: a frontend background transaction created before a multi-step restore/clone/data-branch operation and used by nested persistent DDL.

Required invariants:

1. For every committed privilege row whose scope names one concrete database, table, or view, the referenced logical object generation exists at the privilege publication point.
2. After a persistent object's DROP commits, no privilege row for that database/relation logical ID remains.
3. GRANT and DROP for the same object generation have one serial order:
   - GRANT commits first -> DROP observes and deletes the row;
   - DROP commits first -> waiting GRANT refreshes under RC, observes absence, and publishes nothing.
4. All participants acquire lifecycle locks in `database -> relation` order. No path may acquire a hidden child relation lock while retaining shared locks for sibling index write targets.
5. A failed lifecycle cleanup aborts the owning DDL transaction; object removal and privilege cleanup never commit independently.
6. Prepared execution uses the database binding captured for the prepared statement rather than a later session `USE`, for both text and binary execution.
7. Existing user transactions are never silently replaced or upgraded. A persistent lifecycle statement either runs in a compatible generation or is rejected before mutation.

Non-goals:

- Automatically deleting rows orphaned before rollout; #27836 owns bounded migration/maintenance cleanup.
- Making hidden index relations independent GRANT targets.
- Changing temporary-table lifecycle or privileges.
- Changing partition, collation, stored-procedure, UDF, or warning compatibility.

## 3. Lock protocol and linearization

### 3.1 Lock keys and order

Lifecycle synchronization uses catalog-row locks derived from account ID and object names:

```text
database key: serial(account_id, database_name) in mo_database
relation key: serial(account_id, database_name, relation_name) in mo_tables
```

For a concrete relation, GRANT and DROP acquire:

```text
database lifecycle lock -> relation lifecycle lock -> resolve logical ID -> mutate catalog -> commit
```

Database-scoped operations acquire the database key. Database DROP owns recursive child cleanup and deletes privileges for the database ID plus all child relation logical IDs in the same transaction. Recursive child DROP suppresses redundant privilege cleanup and lock acquisition through the existing internal lifecycle context.

The lock modes must conflict between GRANT and DROP. GRANT resolves the object while retaining shared lifecycle locks through privilege publication. DROP retains exclusive lifecycle locks through object deletion and privilege cleanup. Waiting transactions refresh under pessimistic RC before resolving IDs.

### 3.2 Linearization points

- GRANT linearizes at commit of the `mo_role_privs` mutation while holding lifecycle locks.
- DROP linearizes at commit of object deletion plus `mo_role_privs` cleanup while holding lifecycle locks.
- A lock wait is not itself a commit point. After waiting, object identity is resolved from the refreshed RC view.

### 3.3 Hidden index relations

Hidden index relations are implementation details owned by the parent table/index definition and are rejected as direct GRANT targets. DROP captures their logical IDs only to remove possible legacy rows. It does **not** upgrade a hidden-child metadata lock: DML may already hold shared locks on several index targets, and sibling child upgrades create a wait cycle. Parent lifecycle serialization is the first owner.

### 3.4 Failure and cancellation

- Lock acquisition error, timeout, deadlock-victim selection, privilege-cleanup error, or object-resolution error returns through the owner transaction and causes rollback.
- Waiters are owned by lockservice; cancellation or victim selection must detach the waiter and release all locks held by the aborted transaction through existing transaction rollback.
- Test barriers are process-local validation instrumentation only. Their release is registered immediately with idempotent cleanup, so assertion failure cannot retain a blocked request.

## 4. Transaction generation and admission

### 4.1 New autocommit generation

Persistent `DROP DATABASE/TABLE/VIEW` creates a pessimistic RC transaction generation even when deployment defaults are optimistic/SI. Object-scoped GRANT uses a private pessimistic RC transaction. This makes lifecycle locks effective and ensures post-wait visibility refresh.

### 4.2 Existing user transaction

An existing transaction is reused; it is never replaced because replacement would split earlier user writes from the DDL and violate atomicity. Admission rules are:

- pessimistic RC: admit persistent lifecycle DDL;
- pessimistic non-RC or optimistic transaction: reject before lifecycle mutation with a stable unsupported error;
- temporary-object DROP: exempt because it does not participate in persistent catalog lifecycle;
- internal recursion owned by DROP ACCOUNT/database replacement: preserve the outer owner's generation and avoid nested ownership.

A rejection leaves prior statements in the user transaction intact and leaves the target object unchanged. The user can commit or roll back the existing transaction explicitly.

### 4.3 Creation, reuse, and terminal paths

```text
no active txn -> select effective statement -> create required generation -> execute -> commit/rollback -> release
active txn    -> select effective statement -> validate generation -> execute or reject -> caller commit/rollback
private owner -> create required generation before BEGIN -> nested operations reuse owner -> owner commit/rollback
```

No nested statement may upgrade or substitute the owner after `BEGIN`. Every owner has exactly one commit/rollback/close path, including BEGIN failure and partial initialization.

## 5. Prepared text and binary binding

The effective statement used by authorization and transaction admission is the prepared AST, not the transport wrapper (`EXECUTE`, binary execute command, or a later session statement). Preparation captures the default database needed to resolve unqualified object names. Execution restores that binding for authorization, planning, lifecycle locking, and cleanup even if `USE` changed the session default afterward.

This applies equally to:

- text protocol `PREPARE` / `EXECUTE`;
- binary protocol prepared execution;
- direct statements, which use their current execution database.

Missing or stale prepared handles fail before transaction generation selection. The binding is request/session state only; no wire, catalog, or on-disk format changes are introduced.

## 6. Restore, clone, data branch, snapshot, and PITR ownership

Multi-step workflows may invoke persistent DROP/replace internally. The component that starts the private background transaction is the owner and must request pessimistic RC **before** `BEGIN`.

- clone and data-branch create/delete: their private background executor owns nested restore/replacement DDL;
- snapshot/PITR restore: restore's private executor owns cleanup and replacement;
- snapshot/PITR metadata creation: private owner participates in the same object publication barrier;
- ordinary clone snapshot reads retain the frontend snapshot transaction unless the explicit data-branch option assigns snapshot reads to the background owner.

The options distinguishing transaction mode from snapshot-read ownership are separate. Reusing one boolean for both contracts is forbidden because it can silently change snapshot provenance.

On any nested failure, the owner rolls back all catalog/object/privilege changes and closes the background executor. Nested code does not independently commit or clean up the owner's transaction.

## 7. DDL identity and replacement rules

- DROP cleanup uses logical IDs captured from the object selected by the statement.
- ALTER/TRUNCATE physical replacement that preserves logical identity must not delete authorization metadata.
- Internal replacement DROP statements use fully qualified, quoted identifiers. They must target the statement database, not the session's current database, including reserved-word names.
- DROP INDEX removes legacy grants for hidden child logical IDs without treating those children as public authorization objects.
- Object ID zero is rejected for concrete cleanup to prevent accidental deletion of global-scope grants.

## 8. Compatibility, rollout, and rollback

No SQL syntax, wire protocol, persisted schema, or catalog format changes. New writes and drops are compatible with existing rows.

- **Upgrade / mixed version**: nodes without the protocol can still create new orphans during rollout. Therefore correctness requires normal binary rollout completion before claiming forward prevention cluster-wide. Existing orphan rows remain inert and are handled by #27836.
- **Downgrade**: code rollback requires no data conversion. It reintroduces the possibility of new orphan rows; rows already cleaned by the new code remain validly absent.
- **Backup/restore/restart**: privilege rows and logical IDs retain their existing formats. Restored workflows use the same private-owner protocol; restart releases uncommitted transactions through existing recovery.
- **Feature gate**: none. Partial behavioral gating would permit protocol participants to disagree and is less safe than one rollout unit.
- **Failure containment**: cleanup errors abort DDL rather than committing a potentially inconsistent object lifecycle.
- **Observability**: existing transaction/lock/deadlock logs and SQL errors identify wait, rejection, or rollback. No unbounded metric labels or new background workers are added.

## 9. Performance and bounds

The protocol adds a constant number of catalog-row locks and bounded cleanup SQL per DDL/GRANT, not per data row. These are metadata paths, not row DML hot paths. Database DROP scans/deletes only privilege rows belonging to the database and its cataloged children inside the existing DDL transaction. No queue, cache, retry loop, goroutine, or retained state is introduced.

Lock waits remain bounded by transaction cancellation/lock wait timeout and deadlock detection. Test hooks store at most one function pointer and are unset by cleanup; callbacks must not block inside lockservice.

## 10. Standards and architectural precedent

SQL authorization semantics require privileges to name database objects, but neither ISO SQL nor the MySQL client protocol prescribes an engine-internal catalog-row lock encoding. MySQL-compatible behavior provides the relevant external precedent: DROP removes the object's effective grants, prepared execution keeps statement meaning stable across execution, and failed DDL must not partially publish authorization metadata. These are compatibility goals, not a requirement to copy MySQL's data dictionary implementation.

The internal precedent is MatrixOne's existing pessimistic lockservice protocol: deterministic serialized row keys, transaction-owned locks, RC refresh after waits, deadlock-victim rollback, and catalog mutation committed by one transaction. Reusing that protocol avoids a second distributed state machine. No wire/API interoperability extension, new SQL syntax, or persistent format is proposed.

## 11. Alternatives

### A. Cleanup only during DROP, without GRANT locking

Rejected. It fixes sequential accumulation but allows GRANT to resolve an object before DROP and publish after DROP cleanup, recreating the orphan.

### B. Foreign-key/cascade constraint from privileges to object catalogs

Rejected. Object types span multiple catalogs and global scopes, and MatrixOne's system-catalog bootstrap/upgrade paths do not currently provide one polymorphic FK contract. It adds migration and write-path complexity while not defining cross-object lock order.

### C. Asynchronous orphan sweeper

Rejected for forward correctness. It permits inconsistency windows, requires an unbounded scan/worker/retry/observability lifecycle, and cannot establish whether a concurrent object generation is safe to delete. A bounded operator-triggered cleanup remains appropriate only for historical rows (#27836).

### D. Upgrade every hidden child relation lock

Rejected. Hidden children are not authorization objects, and DML can retain shared sibling locks; sequential upgrades create cross-index cycles. Parent ownership plus captured logical-ID cleanup is sufficient.

### E. Replace incompatible existing user transactions

Rejected. It would commit, detach, or lose earlier user work and violate explicit transaction atomicity. Early rejection is observable and safe.

## 12. Deterministic verification matrix

| Contract | Cheapest evidence |
|---|---|
| Sequential table/view/database cleanup | compile UT plus public SQL BVT querying `mo_role_privs` |
| Logical identity preserved across ALTER/TRUNCATE | UT and BVT asserting grants remain on preserved logical ID |
| GRANT-first serialization | real two-session embedded test: GRANT pauses after locks; lockservice callback proves DROP is enqueued on lifecycle catalog lock; then release and assert zero orphan |
| DROP-first serialization and RC refresh | explicit DROP transaction; lockservice callback proves GRANT waiter is enqueued; commit; assert GRANT fails and zero orphan |
| Barrier failure cleanup | idempotent release registered immediately with `t.Cleanup`; focused race/stress in one process detects leaked global state |
| Prepared text/binary binding | frontend UT and BVT with `USE` changed after prepare |
| Existing transaction admission/failure atomicity | frontend UT plus SQL controls for prior writes and unchanged target |
| Private owner transaction | focused owner-option UTs and restore/clone/data-branch BVT |
| Hidden-child deadlock regression | exact fulltext snapshot restore test repeated under its real topology |
| Cross-database replacement quoting | same-instance BVT with equal names and reserved identifiers |
| Package integration | frontend/compile owning-package tests, vet, lint; race where shared-state closure applies |

Acceptance requires non-empty exact test selection, terminal PASS, no orphan rows, no leaked global hook, and no package-level timeout. Repetition is stress evidence only after deterministic barriers establish the protocol.

## 13. Security analysis

The change narrows authorization metadata validity. It does not broaden who may grant or drop. Tenant/account ID remains part of every lifecycle key and catalog query. Concrete cleanup rejects zero IDs, and hidden internal relations cannot become independent grant targets. Fully qualified internal DDL prevents a session database change from redirecting destructive operations to another object.

The primary denial-of-service risk is longer metadata lock retention. The bounded database-before-relation order, existing timeouts/deadlock detection, and absence of sibling hidden-child upgrades constrain that risk.

## 14. Decision log and open review

Resolved decisions:

1. One `database -> relation` protocol for GRANT and persistent DROP.
2. Pessimistic RC for newly owned lifecycle generations; reject incompatible existing generations.
3. Logical ID, not physical relation identity or name, owns privileges.
4. Parent lifecycle owns hidden index children.
5. Prepared default-database binding is part of statement identity.
6. Private workflow transaction is selected before BEGIN; snapshot-read ownership is a separate option.
7. Forward prevention ships independently from historical cleanup.

Blocking review item: an implementation reviewer must approve this exact design revision before PR #27745 can be approved. Material deviations in lock order, transaction admission, identity, or private ownership require updating this document and re-reviewing the affected sections.

### Design review record

```text
Change scope: complete issue #27723 implementation in PR #27745
Trigger: authorization boundary + distributed lock/lifecycle protocol + restore/rollback ownership
Design: this versioned document; status IN REVIEW
Blocking findings: independent reviewer approval is pending; no unresolved technical question is deferred
Decision log: sections 2-13 and resolved decisions 1-7 above
Decision: REQUEST_CHANGES until design approval is recorded
Implementation deviations: none currently identified; reviewer must verify against this revision
```
