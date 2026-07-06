# Data Branch Privilege Model Design

## 1. Overview

Issue #24840 exposes a systemic privilege problem in `DATA BRANCH`
statements. The current frontend privilege map treats the data branch family as
coarse `TableAll/TableOwnership` or `DatabaseAll/AccountAll` checks. That does
not match the SQL operations performed by each statement, and it lets a user
with only one side of the required permission create or modify branch objects.

This document defines a complete privilege model for all user-facing
`DATA BRANCH` statements and a concrete implementation plan.

The central rule is:

> A data branch statement must require the same privileges as the ordinary SQL
> reads and writes that it logically performs.

## 2. Current State

### 2.1 Syntax and handlers

Parser grammar in `pkg/sql/parsers/dialect/mysql/mysql_sql.y` supports:

- `DATA BRANCH CREATE TABLE <dst> FROM <src> [TO ACCOUNT ...]`
- `DATA BRANCH CREATE DATABASE <dst> FROM <src> [snapshot] [TO ACCOUNT ...]`
- `DATA BRANCH DELETE TABLE <table>`
- `DATA BRANCH DELETE DATABASE <db>`
- `DATA BRANCH DIFF <target> AGAINST <base> ...`
- `DATA BRANCH MERGE <src> INTO <dst> ...`
- `DATA BRANCH PICK <src> INTO <dst> ...`

`pkg/frontend/data_branch.go` routes all of them through `handleDataBranch`.
These are self-handled frontend statements. They do not naturally go through
normal plan-based table privilege extraction.

### 2.2 Existing privilege mapping

`determinePrivilegeSetOfStatement` currently maps:

- `DataBranchCreateTable`, `DataBranchDeleteTable`, `DataBranchDiff`,
  `DataBranchMerge`, `DataBranchPick` to object type `table` with only
  `TableAll` and `TableOwnership`.
- `DataBranchCreateDatabase`, `DataBranchDeleteDatabase` to object type
  `database` with only `DatabaseAll` and `AccountAll`.

`authenticateUserCanExecuteStatementWithObjectTypeAccountAndDatabase` also has
an owner fallback branch that directly returns `true` for Clone/DataBranch
statements after the initial privilege check fails.

`DATA BRANCH PICK` additionally has `authenticatePickTablePrivileges`, but it
also checks only `TableAll/TableOwnership` on source and destination. It is
more restrictive than SQL semantics in some cases and still not expressive
enough for read/write separation.

### 2.3 Why this is not fixable case-by-case

The issue is not limited to `CREATE TABLE` or `CREATE DATABASE`.

- `CREATE` statements have a source side and a destination side.
- `DIFF` reads both sides.
- `MERGE` reads both sides and mutates the destination.
- `PICK` reads both sides, may execute a user subquery, and mutates the
  destination.
- `DELETE` statements should follow normal `DROP` privileges and must not use a
  weaker data branch-specific permission.

The fix must therefore introduce one central data branch privilege resolver,
not scattered checks inside each branch handler.

## 3. Goals

- Define exact required privileges for every `DATA BRANCH` statement.
- Reuse existing MatrixOne privilege semantics and privilege types.
- Express cross-side requirements as logical AND, and alternatives as logical
  OR.
- Keep role semantics compatible with ordinary statements: different required
  permissions may be satisfied by different active roles.
- Ensure background executor / restore paths are never the user privilege
  boundary.
- Preserve existing sys-only restrictions for `TO ACCOUNT`.
- Add tests that reproduce both "source-only" and "destination-only" bypasses.

## 4. Non-goals

- This design does not change the data branch diff/merge algorithms.
- This design does not introduce new privilege types.
- This design does not redesign the full frontend privilege engine.
- This design does not grant delegated cross-account branch privileges. Cross
  account `TO ACCOUNT` remains sys-only.

## 5. Permission Vocabulary

The implementation should use explicit requirement objects instead of encoding
the whole data branch statement as one `privilege` with multiple unrelated
entries.

### 5.1 Requirement semantics

Use:

- A list of requirements as logical AND.
- The privilege types inside one requirement as logical OR.

Example:

```text
DATA BRANCH CREATE TABLE dst FROM src

require all:
  1. source read on src:
       SELECT OR TableAll OR TableOwnership
  2. target create table on dst database:
       CreateTable OR DatabaseAll OR DatabaseOwnership
```

Do not use one `compoundEntry` for cross-side requirements. The current
compound evaluation checks all items under one role during one
`determineRoleSetHasPrivilegeSet` pass. That can accidentally reject a user who
has the required permissions through multiple active roles. A list of
independent requirements evaluated against the active role set preserves the
normal SQL behavior.

### 5.2 Base requirement constructors

Implement these constructors in a new file:

`pkg/frontend/data_branch_privilege.go`

```go
type branchPrivilegeRequirement struct {
    objType                       objectType
    databaseName                  string
    tableName                     string
    privilegeTypes                []PrivilegeType
    writeDatabaseAndTableDirectly bool
    isClusterTable                bool
    clusterTableOperation         clusterTableOperationType
}
```

Helper constructors:

- `branchCreateTableRequirement(db)`
  - `CreateTable`, `DatabaseAll`, `DatabaseOwnership`
  - `writeDatabaseAndTableDirectly = true`
- `branchCreateDatabaseRequirement()`
  - `CreateDatabase`, `AccountAll`
  - `writeDatabaseAndTableDirectly = true`
- `branchWriteTableRequirement(db, tbl, typ, clusterOp)`
  - one of `Insert`, `Update`, `Delete`
  - plus `TableAll`, `TableOwnership`
  - `writeDatabaseAndTableDirectly = true`
- `branchDropTableRequirement(db, tbl)`
  - same as `DROP TABLE`: `DropTable`, `DropObject`, `DatabaseAll`,
    `DatabaseOwnership`
  - these alternatives are database-level privileges in the current MO model;
    `tbl` is used for owner fallback and metadata validation, not for a
    table-level `DropTable` grant.
  - pass `tbl` into the requirement so `verifyLightPrivilege` can apply the
    same cluster-table drop checks as normal `DROP TABLE`.
  - if those fail, reuse existing table owner logic.
- `branchDropDatabaseRequirement(db)`
  - same as `DROP DATABASE`: `DropDatabase`, `AccountAll`
  - if those fail, reuse existing database owner logic.

Evaluation helper:

```go
func requireAllBranchPrivileges(
    ctx context.Context,
    ses *Session,
    requirements []branchPrivilegeRequirement,
) (statistic.StatsArray, error)

func requireBranchDropTablePrivilege(
    ctx context.Context,
    ses *Session,
    dbName string,
    tableName string,
) (statistic.StatsArray, error)

func requireBranchDropDatabasePrivilege(
    ctx context.Context,
    ses *Session,
    dbName string,
) (statistic.StatsArray, error)
```

For each requirement, build a temporary `privilege` whose `entries` are the OR
alternatives in that requirement, then call `determineUserHasPrivilegeSet`.
Return `moerr.NewInternalError(ctx, "do not have privilege to execute the statement")`
when any requirement is not satisfied.

When building each temporary `privilege`, copy the requirement's
`writeDatabaseAndTableDirectly`, `isClusterTable`, and
`clusterTableOperation` values into the `privilege`. These fields are required
by `verifyLightPrivilege`; omitting them would accidentally bypass the existing
system-database and cluster-table checks.

The drop-specific helpers first evaluate the normal DROP privilege
requirements. If they fail and the caller is a real user, call the existing
`checkRoleWhetherTableOwner` or `checkRoleWhetherDatabaseOwner` helpers
explicitly. Do not preserve the current generic DataBranch owner fallback that
returns `true` for the whole statement family.

### 5.3 Source read resolution

Source-side read checks must be resolved through existing MO object semantics,
not by ad hoc catalog name checks.

Implement:

```go
func requireBranchReadOnTableName(
    ctx context.Context,
    ses *Session,
    name tree.TableName,
) (statistic.StatsArray, error)
```

The helper should:

- resolve omitted schema names with the session default database;
- preserve `{SNAPSHOT=...}` and `{MO_TS=...}` on the source name;
- honor subscription metadata the same way `TxnCompilerContext.Resolve` and
  normal SELECT planning do;
- check privileges against the user-visible subscription database name when the
  source is a subscription, not the publisher's physical database name;
- distinguish tables from views through the normal SELECT privilege path;
- preserve view-chain, definer/invoker, cluster-table, and snapshot behavior
  through the existing plan-based authorization code.

Implement this by directly constructing a synthetic `SELECT * FROM <source>`
AST, building its plan with `buildPlan`, and calling
`authenticateUserCanExecuteStatementWithObjectTypeDatabaseAndTable`.

Do not parse or split the original `DATA BRANCH` SQL string. Do not duplicate
the view or subscription privilege resolver inside the data branch module; the
synthetic SELECT path is the reuse boundary.

Because `DATA BRANCH` statements are self-handled frontend statements, this
helper must run from the frontend execution path after the transaction compiler
context has an active transaction operator. Calling it from the earlier generic
statement-auth phase can fail before plan building with a nil transaction
operator.

## 6. Required Privilege Matrix

### 6.1 `DATA BRANCH CREATE TABLE dst FROM src`

Logical SQL equivalent:

```sql
CREATE TABLE dst AS SELECT * FROM src AT <snapshot>;
```

Required privileges:

1. Source read on `src`:
   - `SELECT` on source table/view, or
   - `TableAll`, or
   - `TableOwnership`.
2. Destination table creation on `dst` database:
   - `CREATE TABLE` on destination database, or
   - `DatabaseAll`, or
   - `DatabaseOwnership`.

Notes:

- If `src` has `{SNAPSHOT=...}` or `{MO_TS=...}`, the privilege is still checked
  on the source object name. A snapshot does not weaken authorization.
- If `dst` omits schema, use the session default database.
- If `src` omits schema, use the session default database.
- Existing `TO ACCOUNT` behavior remains: non-sys users cannot branch to
  another account.
- Check target `CREATE TABLE` before resolving the source table. This keeps the
  failure order consistent with `CREATE TABLE ... AS SELECT` and avoids exposing
  source object details to a user that cannot create the destination table.
- Do not use the current raw-SQL regexp rewrite in `dataBranchCreateTable` for
  authorization. The privilege resolver must use `stmt.SrcTable` and
  `stmt.CreateTable.Table`.
- If the source is a view, use the same read semantics as a normal SELECT from
  that view, including view security metadata.

### 6.2 `DATA BRANCH CREATE DATABASE dst FROM src`

Logical SQL equivalent:

```text
CREATE DATABASE dst;
for each table/view in src:
    DATA BRANCH CREATE TABLE dst.<object> FROM src.<object>;
```

Required privileges:

1. Destination database creation:
   - `CREATE DATABASE`, or
   - `AccountAll`.
2. Source read on every cloned source table/view:
   - `SELECT` on the source object, or
   - `TableAll`, or
   - `TableOwnership`.

Important correction:

- Do not require `CREATE TABLE` on `dst` before `dst` exists. The target table
  creation happens inside a newly created database. The authorization boundary
  is `CREATE DATABASE`; after that, the created database's ownership model
  covers internal table creation.
- Check `CREATE DATABASE` before enumerating source objects. This avoids
  exposing source database details to a user that cannot create the destination
  database.

Implementation detail:

- Use the same source-object resolver as clone database:
  - resolve `TO ACCOUNT` and snapshot with `getOpAndToAccountId`;
  - resolve subscription metadata with `GetSubscriptionMeta`;
  - list source objects with the same `getTableInfos` / `showFullTables`,
    foreign-key topo sort, and view collection logic used by
    `handleCloneDatabase`.
- Do not duplicate the SQL that enumerates source objects. Factor the current
  clone-database source-object collection into a shared helper if needed.
- Check source read privilege for every user-visible source table returned by
  that resolver.
- For source views, check SELECT on the view using `objectTypeView`. If the
  implementation can derive view-chain privilege tips through a synthetic
  SELECT plan, preserve them; otherwise do not parse view SQL manually.
- Internal objects created as part of table storage or restore mechanics are
  not separate user privilege targets. Follow the same filters as
  `buildTableInfoListSQL`: skip index tables, temporary tables, partitions,
  sequences, and other restore-internal objects.

### 6.3 `DATA BRANCH DELETE TABLE t`

Logical SQL equivalent:

```sql
DROP TABLE t;
-- plus branch metadata/snapshot cleanup
```

Required privileges:

- Same as `DROP TABLE t`:
  - `DropTable`, or
  - `DropObject`, or
  - `DatabaseAll`, or
  - `DatabaseOwnership`, or
  - existing table owner fallback.

Execution safety:

- Resolve `t` to a table id before executing `DROP TABLE`.
- Verify that the table id has an active row in `mo_catalog.mo_branch_metadata`
  before executing `DROP TABLE`.
- If no active branch metadata row exists, return an error before any DDL.
- If the table cannot be resolved, return the normal no-such-table error before
  any DDL. `DATA BRANCH DELETE TABLE` has no `IF EXISTS` syntax and should not
  silently succeed.
- The metadata row must satisfy `table_id = <target rel_id>` and
  `table_deleted = false`. It is a child-row check; `p_table_id` alone is not
  enough.

This prevents `DATA BRANCH DELETE TABLE` from becoming a second, weaker spelling
of `DROP TABLE`.

### 6.4 `DATA BRANCH DELETE DATABASE db`

Logical SQL equivalent:

```sql
DROP DATABASE db;
-- plus branch metadata/snapshot cleanup for contained branch tables
```

Required privileges:

- Same as `DROP DATABASE db`:
  - `DropDatabase`, or
  - `AccountAll`, or
  - existing database owner fallback.

Execution safety:

- Resolve contained tables before executing `DROP DATABASE`.
- If the database does not exist, return the normal bad-database error before
  any DDL. `DATA BRANCH DELETE DATABASE` has no `IF EXISTS` syntax and should
  not silently succeed.
- If the database contains user data tables, every contained user data table
  must have an active `mo_branch_metadata` child row.
- Fail before DDL if any contained user data table is not a branch child.
- If validation finds no active branch child tables, fail before DDL. Current
  branch metadata is table-level only and cannot distinguish an empty
  branch-created database from an ordinary empty database, so `DATA BRANCH
  DELETE DATABASE` must fail closed for empty, view-only, or sequence-only
  databases until a database-level branch identity exists.
- Views are not recorded in `mo_branch_metadata` today. Do not require branch
  metadata for views during database-delete validation; they are covered by the
  required `DROP DATABASE` privilege.
- Ignore restore/internal objects that do not get their own branch metadata:
  index tables, temporary tables, partitions, sequences, `mo_account_lock`, and
  other objects skipped by the clone/restore object filters.
- Internal object filters must be shared with clone/list-table logic. SQL
  `LIKE` predicates for literal prefixes such as `__mo_index_` and
  `__mo_tmp_` must escape `_`, `%`, and `\` and use an explicit `ESCAPE`
  clause. Do not use unescaped `LIKE '__mo_tmp_%'` style predicates.
- Run validation, `DROP DATABASE`, `markBranchTablesDeleted`, and
  `reclaimBranchSnapshotsWithBH` in the same background-executor transaction.

### 6.5 `DATA BRANCH DIFF target AGAINST base`

Logical SQL equivalent:

```sql
SELECT ... FROM target;
SELECT ... FROM base;
```

Required privileges:

1. Read on `target`:
   - `SELECT`, or `TableAll`, or `TableOwnership`.
2. Read on `base`:
   - `SELECT`, or `TableAll`, or `TableOwnership`.

Output modes:

- Default rows, `OUTPUT LIMIT`, `OUTPUT COUNT`, and `OUTPUT SUMMARY` are read
  only.
- `OUTPUT FILE` produces a SQL file and must keep using
  `validateOutputDirPath`; it does not require table write privileges.
- `OUTPUT AS table_name` is parsed today but not implemented by
  `satisfyDiffOutputOpt`. The privilege fix must not leave this silent.
  Return `not supported` explicitly in this change.
  Implementation can check `stmt.OutputOpt != nil &&
  stmt.OutputOpt.As.ObjectName != ""` in `validate` or at the start of
  `satisfyDiffOutputOpt`.

### 6.6 `DATA BRANCH MERGE src INTO dst`

Logical SQL equivalent:

```text
read src;
read dst;
apply inserts/updates/deletes to dst;
```

Required privileges:

1. Read on `src`:
   - `SELECT`, or `TableAll`, or `TableOwnership`.
2. Read on `dst`:
   - `SELECT`, or `TableAll`, or `TableOwnership`.
3. Write on `dst`:
   - `INSERT`, or `TableAll`, or `TableOwnership`.
4. Write on `dst`:
   - `UPDATE`, or `TableAll`, or `TableOwnership`.
5. Write on `dst`:
   - `DELETE`, or `TableAll`, or `TableOwnership`.

Rationale:

- The implementation currently applies rows with generated `DELETE` and
  `INSERT` SQL, but the statement semantics can modify existing rows. Requiring
  `UPDATE` as well avoids tying authorization to one physical implementation.

### 6.7 `DATA BRANCH PICK src INTO dst ...`

Logical SQL equivalent:

```text
read src;
read dst;
apply selected inserts/updates/deletes to dst;
optionally execute KEYS subquery;
```

Required privileges:

1. Read on `src`:
   - `SELECT`, or `TableAll`, or `TableOwnership`.
2. Read on `dst`:
   - `SELECT`, or `TableAll`, or `TableOwnership`.
3. Write on `dst`:
   - `INSERT`, or `TableAll`, or `TableOwnership`.
4. Write on `dst`:
   - `UPDATE`, or `TableAll`, or `TableOwnership`.
5. Write on `dst`:
   - `DELETE`, or `TableAll`, or `TableOwnership`.
6. If `KEYS (SELECT ...)` is used, the key subquery must continue to use
   `buildPlanWithAuthorization`, so ordinary query privileges are enforced on
   every table read by the subquery.

Notes:

- `WHEN CONFLICT SKIP` or `FAIL` does not reduce the required destination write
  set. The exact row operations are data-dependent.
- `BETWEEN SNAPSHOT` does not weaken source or destination read privileges.

## 7. Integration Design

### 7.1 Central entry point

Add:

```go
func isDataBranchStatement(stmt tree.Statement) bool

func authenticateDataBranchStatement(
    ctx context.Context,
    ses *Session,
    stmt tree.Statement,
) (statistic.StatsArray, error)
```

Wire it in the `execInFrontend` DataBranch case before `handleDataBranch`:

- `SkipCheckPrivilege`,
- `skipAuthForSpecialUser`,
- tenant nil check.

The authenticator itself should perform those early exits so the entry point is
safe for direct tests and future callers:

```go
authStats, authErr := authenticateDataBranchStatement(execCtx.reqCtx, ses, st)
stats.Add(&authStats)
if authErr != nil {
    err = authErr
    return
}
handleStats, handleErr := handleDataBranch(execCtx, ses, st)
stats.Add(&handleStats)
if handleErr != nil {
    err = handleErr
    return
}
```

Then update `determinePrivilegeSetOfStatement` so DataBranch statements no
longer carry the old coarse privilege entries. They can use
`objectTypeNone/kindNone` there because their real authorization is handled by
`authenticateDataBranchStatement`.

Remove all DataBranch cases from the owner fallback that currently returns
`true`.

Do not call the DataBranch authenticator from individual branch handlers. It
belongs in the single `execInFrontend` DataBranch dispatch branch so source
plan building has the right transaction context while all DataBranch statements
still share one authorization path.

### 7.2 Remove handler-level privilege checks

Remove `authenticatePickTablePrivileges` and `requirePickTablePrivilege` from
`handleBranchPick` after the central auth path is in place.

Keep `buildPlanWithAuthorization` in `materializeSubqueryUnified`; that check
belongs to the user-provided `KEYS` subquery, not to the branch source/target
table model.

### 7.3 Object resolution helpers

Add this helper in a small shared clone helper file:

```go
func collectCloneDatabaseSource(
    ctx context.Context,
    ses *Session,
    bh BackgroundExec,
    stmt *tree.CloneDatabase,
) (cloneDatabaseSource, error)

type cloneDatabaseSource struct {
    srcResolveDBName   string
    srcPrivilegeDBName string
    srcTblInfos        []*tableInfo
    viewMap            map[string]*tableInfo
    sortedFkTbls       []string
    fkTableMap         map[string]*tableInfo
    snapshot           *plan.Snapshot
    opAccountId        uint32
    toAccountId        uint32
}
```

`collectCloneDatabaseSource` must be factored from the current
`handleCloneDatabase` source side and reused by both authorization and clone
execution. It must preserve the clone database account, snapshot,
subscription, and source-object logic:

- `getOpAndToAccountId`
- `GetSubscriptionMeta`
- `getTableInfos` / `showFullTables`
- `fkTablesTopoSort`
- `getTableInfoMap`
- view collection before `sortedViewInfos` / `restoreViews`

Do not duplicate SQL string construction for listing source objects. Keep the
physical source database used for catalog reads separate from the database name
used for privilege checks; subscription sources must be checked against the
user-visible subscription database.

### 7.4 Background executor handling

The privilege resolver may need a background executor for metadata reads. Use a
read-only helper:

```go
func withBranchPrivilegeBackgroundExec(
    ctx context.Context,
    ses *Session,
    fn func(BackgroundExec) error,
) (statistic.StatsArray, error)
```

It should:

- use `getBackExecutor` to participate correctly in explicit/share txn cases;
- close or finish the executor exactly once;
- accumulate stats into the caller's `StatsArray`.

The background executor is only for metadata/object resolution during
authorization. It is not the privilege boundary.

### 7.5 Branch metadata validation for delete

Add helpers near the branch metadata code:

```go
func getActiveBranchChildByTableID(
    ctx context.Context,
    ses *Session,
    bh BackgroundExec,
    tableID uint64,
) (bool, error)

func validateDataBranchDeleteTableTarget(
    ctx context.Context,
    ses *Session,
    bh BackgroundExec,
    dbName string,
    tableName string,
) (tableID uint64, error)

func validateDataBranchDeleteDatabaseTarget(
    ctx context.Context,
    ses *Session,
    bh BackgroundExec,
    dbName string,
) ([]uint64, error)
```

These helpers read `mo_branch_metadata` under the sys account, matching
`constructBranchDAG` and `updateBranchMetaTable`.

Call validation before `DROP TABLE` or `DROP DATABASE`.

Validation details:

- Use `defines.GetAccountId(ctx)` for the current tenant and resolve target
  rel ids from `mo_catalog.mo_tables` in that tenant.
- Quote string literals with existing helpers such as `quoteSQLStringLiteral`;
  do not interpolate database/table names into catalog SQL with bare `%s`.
- For table delete, require exactly one target relation and one active metadata
  row for that relation.
- For database delete, collect only user data table rel ids. Exclude views and
  restore/internal objects that never receive branch metadata.
- Query active metadata as sys:

```sql
select table_id
from mo_catalog.mo_branch_metadata
where table_deleted = false and table_id in (...)
```

- Query active metadata in bounded batches using the same batch size as branch
  metadata update paths. Do not build a single unbounded `IN (...)` list for a
  whole database.
- Return an error listing the first missing non-branch table when validation
  fails. Do not execute the DDL on validation failure.

## 8. Implementation Steps

1. Add `pkg/frontend/data_branch_privilege.go`.
2. Implement `branchPrivilegeRequirement` and requirement constructors.
3. Implement `requireAllBranchPrivileges`.
4. Implement source/target object resolution helpers.
5. Factor clone-database source object collection out of `handleCloneDatabase`
   and reuse it from the privilege resolver.
6. Implement `authenticateDataBranchStatement` switch:
   - `DataBranchCreateTable`
   - `DataBranchDeleteTable`
   - `DataBranchDeleteDatabase`
   - `DataBranchDiff`
   - `DataBranchMerge`
   - `DataBranchPick`
7. Implement `DataBranchCreateDatabase` authorization inside
   `dataBranchCreateDatabase`, not in the pre-handler switch. That path must
   check `CREATE DATABASE` before source enumeration, reuse the collected
   `cloneDatabaseSource` for source read authorization and execution, and
   return its `StatsArray` through `handleDataBranch`.
8. Wire the central auth entry in the `execInFrontend` DataBranch dispatch
   branch before `handleDataBranch`, and add handler stats into the same
   statement stats array.
9. Change `determinePrivilegeSetOfStatement` so DataBranch statements no longer
   use coarse privilege entries.
10. Remove DataBranch statements from the owner fallback allow-list.
11. Remove `authenticatePickTablePrivileges` and `requirePickTablePrivilege`.
12. Add delete-target branch metadata validation before executing DDL.
13. Return `not supported` for `DATA BRANCH DIFF ... OUTPUT AS ...`.
14. Add unit tests and BVT cases listed below.

## 9. Test Plan

### 9.1 Unit tests

Add focused tests in `pkg/frontend/authenticate_test.go` or a new
`pkg/frontend/data_branch_privilege_test.go`.

Required unit tests:

- `CREATE TABLE`:
  - only source `SELECT` -> fail;
  - only destination `CREATE TABLE` -> fail;
  - both source `SELECT` and destination `CREATE TABLE` -> pass;
  - `TableAll` or `TableOwnership` on source can satisfy source read;
  - `DatabaseAll` or `DatabaseOwnership` on destination can satisfy create.
  - source view uses view SELECT semantics.
  - source subscription checks privileges on the subscription database name.
- `CREATE DATABASE`:
  - only `CREATE DATABASE` -> fail if any source object lacks read permission;
  - only source reads -> fail without `CREATE DATABASE`;
  - `CREATE DATABASE` plus read on every source table -> pass;
  - missing read on one table among many -> fail.
  - source database containing views can be authorized and deleted later even
    though views are not recorded in `mo_branch_metadata`.
  - source database with indexes/FKs uses the same object list as
    `handleCloneDatabase`.
- `DIFF`:
  - read on only one side -> fail;
  - read on both sides -> pass.
  - `OUTPUT AS` returns not supported explicitly.
- `MERGE`:
  - missing source read -> fail;
  - missing destination read -> fail;
  - missing any of destination `INSERT`, `UPDATE`, `DELETE` -> fail;
  - all requirements -> pass.
- `PICK`:
  - same table read/write matrix as `MERGE`;
  - `KEYS (SELECT ...)` still calls `buildPlanWithAuthorization`.
- `DELETE TABLE`:
  - normal `DROP TABLE` privilege -> pass only if active branch metadata exists;
  - table owner fallback -> pass only if active branch metadata exists;
  - no active branch metadata -> fail before drop.
  - missing table -> fail before DDL.
  - failed validation leaves any existing non-branch table untouched.
- `DELETE DATABASE`:
  - normal `DROP DATABASE` privilege -> pass only if contained user tables are
    branch children;
  - database owner fallback -> same;
  - database with non-branch user table -> fail before drop.
  - failed validation leaves the database and all contained objects untouched.
  - empty database with normal `DROP DATABASE` privilege -> fail before drop.
  - database containing only views/sequences and no active branch child tables
    -> fail before drop.
  - database containing only branch tables plus restored views -> pass.

### 9.2 BVT regression cases

Add SQL cases under:

`test/distributed/cases/git4data/branch/privilege/`

Required cases may be split by operation or kept in one focused regression file,
as long as the matrix is covered:

- reproduce issue #24840 `CREATE TABLE` case 1 and case 2;
- reproduce issue #24840 `CREATE DATABASE` case 3 and case 4;
- grant both required create-side privileges and verify success;
- after failed create attempts, verify destination tables/databases were not
  created;
- grant read on only one `DIFF` side and verify fail;
- grant read on both `DIFF` sides and verify success;
- verify unsupported `DIFF OUTPUT AS table_name` fails explicitly and does not
  create the output table;
- test `MERGE` missing source read and missing destination write;
- after failed `MERGE`, verify destination data is unchanged;
- grant all `MERGE` privileges and verify success with no data conflict;
- test `PICK` with a `KEYS (SELECT ...)` subquery that reads a third table and
  fails without `SELECT` on that third table;
- after failed `PICK`, verify destination data is unchanged;
- verify `DELETE TABLE` and `DELETE DATABASE` use normal DROP privilege;
- verify non-branch delete targets are rejected before DDL;
- verify validation failure leaves target table/database present;
- verify a normal table whose name looks similar to an internal object prefix
  is not skipped by SQL wildcard matching, for example a table name matching
  unescaped `__mo_tmp_%` semantics but not the literal `__mo_tmp_` prefix;
- verify a mixed database containing both branch children and normal user tables
  cannot be dropped through `DATA BRANCH DELETE DATABASE`.

Run each privilege case with:

```sql
SET GLOBAL enable_privilege_cache = off;
```

At least one case should run with cache on to confirm the new checks populate
and consult the cache correctly.

## 10. Compatibility and Risk

This change intentionally tightens authorization. Some existing workloads may
have relied on the loose `TableAll/Ownership` or `DatabaseAll/AccountAll`
behavior. Those workloads should fail until the user is granted the ordinary SQL
privileges that match the statement.

Expected behavior changes:

- Users with only source read can no longer create branch objects.
- Users with only destination create can no longer copy unreadable source data.
- Users with only `TableAll/Ownership` on one branch side can no longer diff,
  merge, or pick without the matching side's read/write permissions.
- `DATA BRANCH DELETE TABLE` can no longer drop a non-branch table.
- `DATA BRANCH DELETE DATABASE` can no longer drop a database containing
  non-branch user tables through the branch path.

## 11. Fixed Implementation Decisions

Implement these decisions in this change:

1. `DATA BRANCH DIFF OUTPUT AS table_name`
   - Return `not supported` explicitly. The parser accepts it, but execution
     does not implement it today.
2. Empty database created by `DATA BRANCH CREATE DATABASE`
   - Reject `DATA BRANCH DELETE DATABASE` if validation finds no active branch
     child tables. Current metadata is table-level only and cannot distinguish
     an empty branch-created database from an ordinary empty database, so the
     secure behavior is to fail closed until database-level branch identity is
     implemented.
3. View handling in `CREATE DATABASE`
   - Require `SELECT` on each source view returned by `showFullTables`.
   - Preserve existing view security behavior through synthetic SELECT planning
     or the existing view privilege helpers. Do not parse view definitions by
     hand in the data branch privilege resolver.
   - Do not require `mo_branch_metadata` rows for restored views during
     `DATA BRANCH DELETE DATABASE`; current metadata is table-level only.
4. Database-level branch identity
   - Keep this fix table-metadata based. Adding database-level branch metadata
     is the clean long-term model for empty, view-only, or sequence-only branch
     databases, but it requires a metadata schema/upgrade change and should be
     done as a separate design and implementation step.
5. Metadata validation batching
   - Validate branch child metadata in bounded batches using the shared
     `dataBranchMetadataIDBatchSize` constant. This keeps validation and update
     paths aligned and prevents large databases from constructing one very long
     `IN (...)` SQL statement.
6. Internal identifier formatting
   - Every identifier in internally generated SQL, including database names,
     table names, and `TO ACCOUNT` account names, must use the shared
     identifier quoting helper. Do not combine user/catalog identifiers with
     raw backticks or bare `%s` formatting.
