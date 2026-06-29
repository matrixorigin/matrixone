# Data Branch Protect Snapshot Design Document

## 1. Overview

Data Branch relies on time-travel reads against parent (LCA) tables to classify
tombstones produced on child/derived tables as either `UPDATE` or pure `DELETE`.
When the storage GC reclaims objects that hold LCA-side history, the time-travel
read returns zero rows and the classifier silently downgrades an `UPDATE` into
an `INSERT`, producing incorrect diff output.

This document describes **Branch Protect Snapshot** — a system-managed snapshot
mechanism that pins every object version required by live branch diffs, and
reclaims that protection only when it is no longer needed.

### 1.1 Design Goals

- **Correctness under GC**: guarantee that any `diff <tar> against <base>`
  between live branch tables keeps working regardless of background compaction
  and GC.
- **Zero new storage format**: reuse the existing `mo_snapshots` table and its
  `kind` column, so no schema migration is required.
- **Invisible to users**: branch snapshots never appear in `SHOW SNAPSHOTS`,
  cannot be dropped via `DROP SNAPSHOT`, and do not consume per-account
  snapshot quota.
- **Precise reclamation**: release protection as soon as the branch subtree
  that depends on it becomes fully deleted, so long-lived branches do not
  accumulate unbounded storage.
- **Cross-account safe**: when `DATA BRANCH CREATE ... TO ACCOUNT <x>`
  spans two accounts, the snapshot is anchored on the parent's account, which
  is where GC retention actually applies.

### 1.2 Non-goals

- Does not change the diff algorithm itself.
- Does not protect against "user removes the snapshot manually from system
  tables" — manual tampering with `mo_catalog.mo_snapshots` is out of scope.
- Does not attempt to backfill protection for branches that were created
  before this feature shipped (see §8 Upgrade).

---

## 2. Background

### 2.1 LCA Probe Requirement

For every tombstone produced on one side of a diff, `handleDelsOnLCA`
(`pkg/frontend/data_branch_hashdiff.go`) runs a time-travel probe on the LCA
table at the branch timestamp:

```sql
SELECT pks.__idx_, lca.* FROM <lca_db>.<lca_tbl>{MO_TS = <branchTS>} AS lca
RIGHT JOIN (values ...) AS pks(__idx_, <pk_cols>) ON ...
```

If the probe finds the row, the tombstone is paired with a post-branch insert
and emitted as `UPDATE`. Otherwise it is emitted as a plain `DELETE`.

When the SQL path fails (e.g. object files were physically removed by GC after
compaction), `runLCAProbeWithReaderFallback` retries using a snapshot-scan
reader. That reader still depends on the parent table's object files being
present on storage; once GC deletes them, the fallback also returns zero rows.

### 2.2 Observed Bug

With a single-level branch `t1 ──s──► t2`, the following sequence reproduces a
silent mis-classification:

1. Populate `t1`, branch `t2` from `t1` at ts `s`, commit updates on `t1`.
2. Trigger a compaction + GC round that deletes the pre-`s` object of row
   `a=1`.
3. `DATA BRANCH DIFF t2 AGAINST t1` reports `t1 | INSERT | a=1` instead of
   `t1 | UPDATE | a=1`.

Root cause: the LCA probe for the tombstone on row `a=1` needs the pre-`s`
version of `t1.a=1`. Both the SQL path and the reader fallback lose access to
that version once GC has reclaimed the backing object.

### 2.3 Why Snapshots Solve It

`pkg/vm/engine/tae/logtail/snapshot.go` already feeds every entry in
`mo_snapshots` into `SnapshotInfo`, and `AccountToTableSnapshots` turns those
timestamps into per-table retention hints that GC respects. If a snapshot with
`ts = clone_ts(child)` exists on the parent table, GC will refuse to remove any
object version visible at that timestamp. The LCA probe therefore keeps
working.

---

## 3. Concepts

### 3.1 Branch DAG Terminology

```
t0 ──s1──► t1 ──s2──► t2
           └──s3──► t3
```

- **edge**: the `(parent, child, clone_ts)` triple produced by
  `DATA BRANCH CREATE`.
- **branchTS(edge)**: `clone_ts(child)`, stored in
  `mo_branch_metadata.clone_ts`.
- **subtree(node)**: the node together with every descendant reachable through
  any number of forward edges.

### 3.2 Dependency Rule

For any two live tables `x` and `y`, let `LCA(x, y) = p` and let `e_x` (resp.
`e_y`) be the edge on the unique path from `p` to `x` (resp. `y`) that is
incident to `p`. Then
`DATA BRANCH DIFF x AGAINST y` requires an LCA probe on `p` at
`min(branchTS(e_x), branchTS(e_y))` (§2.1).

Consequence: an edge `e = (p, c, s)` is **in use** iff at least one node in
`subtree(c)` is alive. Equivalently, `e` is reclaimable iff every node in
`subtree(c)` has `table_deleted = true`.

This is the ownership model the protection mechanism enforces.

---

## 4. Snapshot Layout

### 4.1 Table

Reuses the existing `mo_catalog.mo_snapshots`. No DDL change.

```
snapshot_id      : uuid
sname            : '__mo_branch_<child_table_id>'
ts               : clone_ts of the edge (ns)
level            : 'table'
account_name     : parent's account name
database_name    : parent's database name
table_name       : parent's table name
obj_id           : parent's table_id
kind             : 'branch'
```

One row per branch edge. `child_table_id` is the edge's unique key because a
child may have at most one parent.

### 4.2 Why These Fields

- `ts = clone_ts(child)` is exactly the timestamp required by the LCA probe.
- `level = 'table'` means GC retention is scoped to the parent table only;
  snapshots on unrelated tables in the same account are unaffected.
- `obj_id = parent_table_id` follows the convention for table-level snapshots
  and lets reclaim paths look up the edge cheaply without joining
  `mo_branch_metadata`.
- `account_name = parent_account` is mandatory: GC consults retention lists
  scoped by account, so the snapshot must be anchored on the account that
  owns the parent.
- `kind = 'branch'` distinguishes these rows from user snapshots. It is the
  source of truth for filtering and quota decisions.

### 4.3 Naming

`__mo_branch_<child_table_id>`

- `<child_table_id>` is the decimal `mo_tables.rel_id` of the child.
- The `__mo_` prefix matches existing internal-namespace conventions used
  elsewhere in MO (e.g. `__mo_diff_*`, `__mo_fake_pk_col`).
- Guaranteed globally unique because `rel_id` is cluster-unique, so no
  additional cluster uuid is needed.

### 4.4 Invariants

- **I1**: every row in `mo_branch_metadata` with `table_deleted = false` has
  exactly one matching `mo_snapshots` row with `sname =
  '__mo_branch_' || table_id` and `kind = 'branch'`.
- **I2**: every `mo_snapshots` row with `kind = 'branch'` has a matching
  `mo_branch_metadata` row.

I1 can be weakened after a crash between `INSERT mo_branch_metadata` and
`INSERT mo_snapshots` (see §6 for atomicity). I2 is preserved by routing all
deletes through `reclaimBranchSnapshots`.

---

## 5. Lifecycle

### 5.1 Creation

`dataBranchCreateTable` / `dataBranchCreateDatabase` currently:

1. Run `handleCloneTable` / `handleCloneDatabase`, which performs the
   `CREATE TABLE ... CLONE ... {MO_TS = <cts>}` and returns `cloneReceipt`
   values (including `snapshotTS`, `srcAccount`, `toAccount`).
2. Call `updateBranchMetaTable(receipt)` to insert a row into
   `mo_branch_metadata`.

After step 2, a new step is added:

3. `createBranchProtectSnapshot(receipt)` — insert the branch snapshot row
   into `mo_snapshots`.

Pseudocode:

```
createBranchProtectSnapshot(receipt):
    sname      := fmt("__mo_branch_%d", receipt.dstTableID)
    ts         := receipt.snapshotTS
    parentAcc  := accountName(receipt.srcAccount)
    parentDB   := receipt.srcDb
    parentTbl  := receipt.srcTbl
    parentTid  := receipt.srcTableID

    // Execute as sys so the row can be written into the parent's account
    // regardless of caller tenant.
    ctx := defines.AttachAccountId(ctx, sysAccountID)
    bh.Exec(ctx, `INSERT INTO mo_catalog.mo_snapshots VALUES (
        '<uuidv7>', '<sname>', <ts>, 'table',
        '<parentAcc>', '<parentDB>', '<parentTbl>',
        <parentTid>, 'branch'
    )`)
```

### 5.2 Atomicity

Steps 1–3 run inside the same background executor session (`bh`) that
`dataBranchCreate*` already owns. The existing deferred `finishTxn(bh, err)`
wraps all three inserts as a single transaction, so any failure after the
clone succeeds but before the snapshot insert rolls back both the
`mo_branch_metadata` row and the clone DDL.

If step 3 is skipped for any reason (bug, partial rollback), the branch is
already represented in `mo_branch_metadata`, so `data branch diff` still works
against a fresh clone. It merely loses GC protection. This soft-failure mode
is strictly better than the current state, where no protection exists at all.

### 5.3 Reclamation

Triggered whenever any node transitions to `table_deleted = true`. Two entry
points call the **same** `reclaimBranchSnapshots` helper:

- `dataBranchDeleteTable` / `dataBranchDeleteDatabase`: right after
  `markBranchTablesDeleted`.
- `plain DROP TABLE` / `DROP DATABASE` / `DROP ACCOUNT`: after
  `ddl.go` runs `UPDATE mo_branch_metadata SET table_deleted = true`.

Sharing a single helper guarantees identical semantics on both paths and
keeps the DAG walk in one place. `ddl.go` calls into the helper exposed by
the frontend package via a system-tenant executor (see §9.2 for the exact
wiring).

Reclamation algorithm `reclaimBranchSnapshots(deadTIDs)`:

```
1. Load the DAG:
     SELECT table_id, p_table_id, clone_ts, table_deleted
     FROM mo_catalog.mo_branch_metadata
   Build children[p] = [c, ...] and info[tid] = {p, cts, deleted}.

2. candidates := emptySet
   For each tid in deadTIDs:
       // Walk up to the root, marking every ancestor edge for re-check.
       cursor := tid
       while cursor != 0 and cursor is in info:
           candidates.add(cursor)
           cursor := info[cursor].p_table_id

3. For each candidate in topological order (leaves first):
       if subtreeAllDeleted(candidate, info, children):
           emit "__mo_branch_<candidate>" into drop_list

4. DELETE FROM mo_catalog.mo_snapshots
   WHERE kind = 'branch' AND sname IN (drop_list)
   (executed as sys account)
```

`subtreeAllDeleted` is a DFS:

```
subtreeAllDeleted(root):
    if not info[root].deleted:
        return false
    for c in children.get(root, []):
        if not subtreeAllDeleted(c):
            return false
    return true
```

Complexity per reclaim: `O(|DAG|)` in the worst case, but typical DAG sizes
are small. The DAG read is one SQL call; the delete is one SQL call. DAG walk
happens entirely in memory.

### 5.4 Worked Example

DAG state after several creates:

```
t1 ──s1──► t2 ──s2──► t3
           └──s4──► t4
```

`mo_snapshots` has three branch rows: `__mo_branch_<t2>`, `__mo_branch_<t3>`,
`__mo_branch_<t4>`, all with `obj_id` pointing at the respective parent.

Scenario A — user drops t3:
- `info[t3].deleted = true`. Candidates = {t3, t2, t1}.
- `subtreeAllDeleted(t3) = true` → drop `__mo_branch_<t3>`.
- `subtreeAllDeleted(t2)`: t2 is still alive → false → keep
  `__mo_branch_<t2>`.
- `subtreeAllDeleted(t1)`: t1 is still alive → false.

Scenario B — user then drops t2 and t4:
- Candidates = {t2, t4, t1}.
- `subtreeAllDeleted(t4) = true` → drop `__mo_branch_<t4>`.
- `subtreeAllDeleted(t2)`: t2 deleted, its only child t3 already deleted →
  true → drop `__mo_branch_<t2>`.
- `subtreeAllDeleted(t1)`: t1 alive → keep (there is no `__mo_branch_<t1>`
  anyway because t1 is a DAG root).

After scenario B the `mo_snapshots` branch rows for this DAG are fully
cleared.

---

## 6. Cross-Account Semantics

`DATA BRANCH CREATE TABLE b.db2.t2 FROM a.db1.t1 TO ACCOUNT b` (assuming
`a` is the caller, and `b` is the destination) already populates
`cloneReceipt` with:

- `srcAccount = a`, `srcDb = db1`, `srcTbl = t1`
- `toAccount = b`, `dstDb = db2`, `dstTbl = t2`
- `snapshotTS = <cts>`

The snapshot row is inserted with `account_name = a` (the *source* account).
GC on account `a` scans `mo_snapshots` filtered by `account_name = 'a'`, so
the protection is effective exactly where the parent's objects live.

Deletion is symmetric: even though the drop happens on account `b`, the
reclaim path executes `DELETE FROM mo_catalog.mo_snapshots ...` under sys, so
it can remove rows in account `a`'s namespace.

Edge case: if account `a` is dropped while account `b` still exists, the
parent table no longer exists, so no LCA probe will ever target it. The
`mo_snapshots` rows owned by `a` are reclaimed by the usual account-drop
cascade. No branch-specific action is required.

---

## 7. User-Facing Surface

### 7.1 SHOW SNAPSHOTS

`pkg/sql/plan/build_show.go:975` currently filters out `ccpr_%` snapshots.
Augment the filter:

```sql
... WHERE sname NOT LIKE 'ccpr_%' AND kind != 'branch' ORDER BY ts DESC
```

### 7.2 DROP SNAPSHOT

`doDropSnapshot` rejects branch snapshots:

```
if snapshot.kind == 'branch':
    return moerr.NewInternalErrorf(ctx,
        "snapshot %q is managed by data branch and cannot be dropped directly",
        snapshot.name)
```

Matching on `kind` (not on the sname prefix) is preferred because it keeps
the sname format internal and allows future renames.

### 7.3 Quota

`checkSnapshotQuota` is only called on user-initiated `CREATE SNAPSHOT`. The
branch code path calls `createBranchProtectSnapshot` directly without going
through `doCreateSnapshot`, so the quota check is naturally bypassed.

### 7.4 Restore

Cluster / account restore operates on a snapshot taken at TS `T`. If the
snapshot includes `mo_branch_metadata` rows, it also includes the matching
`__mo_branch_*` rows (both tables live in `mo_catalog`). Restore therefore
preserves invariants I1 and I2 automatically, with no branch-specific logic.

---

## 8. Upgrade

### 8.1 Schema Migration

None. `mo_snapshots.kind` already exists and defaults to `'user'`. Existing
rows remain valid.

### 8.2 Pre-existing Branches

Branches created before this feature shipped are **not** backfilled. The
rationale is that any pre-existing branch has already passed through at
least one GC window without protection, so the parent-side history needed
by its LCA probe is almost certainly already gone. Inserting a snapshot
row now would not bring those objects back; it would only add a stale row
that pins nothing useful.

Operators who require correct `DATA BRANCH DIFF` output against a
pre-existing branch should drop and recreate the branch after upgrade. The
new branch will be protected from its creation timestamp onward.

### 8.3 Rollback

If the feature must be rolled back, execute
`DELETE FROM mo_catalog.mo_snapshots WHERE kind = 'branch'` under sys and
ship the previous binary. No data loss because branch snapshots are purely
protective; removing them returns the system to the pre-feature behavior
(which has the known bug in §2.2, but is otherwise functionally complete).

---

## 9. Implementation Notes

### 9.1 File Touch List

| File | Change |
|------|--------|
| `pkg/frontend/data_branch.go` | Call `createBranchProtectSnapshot` after `updateBranchMetaTable`; call `reclaimBranchSnapshots` after `markBranchTablesDeleted`. |
| `pkg/frontend/snapshot.go` | New helpers `createBranchProtectSnapshot`, `reclaimBranchSnapshots`; reject branch kind in `doDropSnapshot`. |
| `pkg/frontend/clone.go` | Extend `cloneReceipt` to carry `srcTableID` and `dstTableID` if not already present. |
| `pkg/sql/compile/ddl.go` | After `UPDATE mo_branch_metadata SET table_deleted = true`, invoke the shared reclaim helper (see §9.2). |
| `pkg/sql/plan/build_show.go:975` | Extend filter to exclude `kind = 'branch'`. |

### 9.2 Shared Reclaim Helper

Both `dataBranchDelete*` (frontend, has `Session`+`BackgroundExec`) and the
plain `DROP TABLE/DATABASE/ACCOUNT` path (compile layer, only has
`Compile.runSqlWithSystemTenant`) call into one helper. The helper exposes
two entry points that share the same core algorithm:

- `reclaimBranchSnapshotsWithBH(ctx, bh, deadTIDs) error` — used by the
  frontend path; reuses the caller's background executor.
- `reclaimBranchSnapshotsBySQL(runSQL func(sql string) error, deadTIDs)` —
  used by `ddl.go`; takes a closure bound to
  `c.runSqlWithSystemTenant` so the compile layer does not need to pull in
  a `Session`.

Both wrappers delegate to the same internal `reclaimBranchSnapshotsCore`
that issues (at most) two SQL statements: one `SELECT` to load
`mo_branch_metadata` and one batched `DELETE` on `mo_snapshots`. The DAG
walk runs in Go.

This keeps drop-path reclaim **synchronous with the transition** — no cron
lag, no double bookkeeping, and identical observability between the two
entry points.

### 9.3 Testing

Coverage combines **BVT cases** (end-to-end behaviour, including cross-account
and negative paths) with **Go unit tests** (helper-level invariants). GC
interaction is verified in dedicated integration tests that drive a real
TAE instance and can trigger flush+GC synchronously.

#### 9.3.1 Unit tests — `pkg/frontend/data_branch_snapshot_test.go`

Mock-based tests that do not need a running MO. All mocks reuse the
`BackgroundExec` / `Session` fakes already used by
`pkg/frontend/data_branch_hashdiff_test.go`.

- **UT-U1 `TestBranchSnapshotName`** — `branchSnapshotName(tid)` returns
  `"__mo_branch_<tid>"`.
- **UT-U2 `TestBuildDAG`** — feed a synthetic `mo_branch_metadata` result
  (flat list of rows) into the DAG builder and assert:
  `children[p] = [c...]` correctness, `info[c].deleted` propagation,
  detached node handling.
- **UT-U3 `TestSubtreeAllDeleted_Linear`** — on `t1 → t2 → t3`:
  - All alive → all predicates false.
  - Only t3 deleted → `subtreeAllDeleted(t3)` true, others false.
  - t3 and t2 deleted → `subtreeAllDeleted(t2)` and `t3` true, `t1` false.
- **UT-U4 `TestSubtreeAllDeleted_Branching`** — on
  `t1 → {t2, t3}, t2 → t4`:
  - Only t4 deleted → `subtreeAllDeleted(t4)` true only.
  - t3 deleted → `t3` true; `t1` still false (t2 alive).
  - t3, t2, t4 deleted → `t3, t2, t4` true; `t1` false unless t1 also
    deleted.
- **UT-U5 `TestReclaimCore_DropList`** — driver-level test: given a
  pre-populated DAG snapshot and `deadTIDs`, assert the computed drop
  list matches the expected `__mo_branch_<tid>` set for each drop
  scenario in §5.4 (scenario A, scenario B).
- **UT-U6 `TestReclaimCore_AncestorWalk`** — deep DAG (`t1 → t2 → t3 → t4`);
  drop only t4. Assert candidate set is `{t4, t3, t2, t1}` (walk reaches
  root) and only `__mo_branch_<t4>` is emitted.
- **UT-U7 `TestReclaimCore_DanglingChildMetadata`** — simulate a corrupt
  state where `mo_branch_metadata` references a parent id that has no
  entry. Assert the walk terminates cleanly and the alive subtree check
  treats the orphan parent as absent rather than panicking.
- **UT-U8 `TestDropSnapshotRejectBranchKind`** — call `doDropSnapshot`
  with a mocked snapshot row having `kind='branch'`, assert it returns
  a `moerr.InternalError` containing "managed by data branch".
- **UT-U9 `TestShowSnapshotsExcludesBranch`** — assert the SQL produced by
  `buildShowSnapShots` contains `kind != 'branch'` (regex match on the
  generated SQL string).

#### 9.3.2 Engine-level tests — `pkg/vm/engine/test/branch_protect_snapshot_test.go`

Uses the in-process disttae+TAE harness that existing branch tests already
rely on (see `pkg/vm/engine/test/branch_*_test.go`). These drive the
server-side paths that the engine harness can reach: lifecycle bookkeeping
in `mo_branch_metadata` + `mo_snapshots`, reclaim via the shared DAG walk,
and cross-account isolation. The classifier round-trip
(§2.2 bug) is **not** retested here — `diff_9.sql` in `branch/diff/`
already covers GC → diff correctness at the full-stack level.

- **ET-G1 `TestBranchProtectSnapshot_Created`** — after simulating a branch
  create, assert exactly one row exists in `mo_catalog.mo_snapshots` with
  `sname='__mo_branch_<child_tid>'`, `ts` equal to the branch's
  `clone_ts`, `obj_id = parent_tid`, `kind='branch'`, `level='table'`.
- **ET-G3 `TestBranchProtectSnapshot_ReclaimOnDataBranchDelete`** —
  create chain `t1 → t2 → t3`; simulate `DATA BRANCH DELETE TABLE t3`;
  assert `__mo_branch_<t3>` was deleted **and** `__mo_branch_<t2>` is
  retained.
- **ET-G4 `TestBranchProtectSnapshot_ReclaimOnPlainDropTable`** — same
  topology but exercise the plain `DROP TABLE` code path via the shared
  helper; assert reclaim still fires.
- **ET-G5 `TestBranchProtectSnapshot_ReclaimCascaded`** — drop t2 while
  t3 is alive; assert **neither** `__mo_branch_<t2>` nor
  `__mo_branch_<t3>` is released (t3 keeps both alive). Then drop t3;
  assert **both** are released.
- **ET-G6 `TestBranchProtectSnapshot_CrossAccount`** — two accounts `a`
  and `b`: simulate `DATA BRANCH CREATE TABLE b.t2 FROM a.t1 TO ACCOUNT b`;
  assert snapshot row has `account_name='a'` and `obj_id=t1.rel_id`;
  simulate `DROP TABLE b.t2`; assert the row is reclaimed via sys.
- **ET-G7 `TestBranchProtectSnapshot_CrossAccount_DropSourceFirst`** —
  variant of ET-G6 where `a.t1` is marked deleted first. Assert
  `__mo_branch_<t2>` is **not** reclaimed (child t2 in account `b` is
  still alive) and no panic occurs.
- **ET-G8 `TestBranchProtectSnapshot_CreateFailedRollsBack`** — inject a
  failure into `createBranchProtectSnapshot` (forced SQL error) and
  verify that the enclosing txn rolls back the `mo_branch_metadata` row,
  so the final state is the pre-create baseline.

#### 9.3.3 BVT cases — `test/distributed/cases/git4data/branch/protect/`

Mirrors the directory layout already used for `diff/`, `merge/`,
`pick/`, `metadata/`. Each case pair is `protect_<n>.sql` +
`protect_<n>.result`. Scope is **snapshot lifecycle only** — creation,
reclamation, and user-facing surface. GC → diff correctness is already
covered by `branch/diff/diff_9.sql`, which exercises the exact
post-flush-+-checkpoint-+-GC sequence the feature exists to protect.

- **BVT-1 `protect_1.sql` — creation + visibility**
  - Create `t1`, branch `t2` from `t1`.
  - `SELECT sname, kind, level, table_name FROM mo_catalog.mo_snapshots
    WHERE sname LIKE '__mo_branch_%';` — expect exactly one row whose
    `table_name = 't1'`, `kind = 'branch'`, `level = 'table'`.
  - `SHOW SNAPSHOTS;` — expect the branch snapshot **not** to appear.
  - After creating a regular user snapshot `usersp1`, `SHOW SNAPSHOTS;`
    returns only `usersp1`.
  - `DROP SNAPSHOT __mo_branch_<t2>;` — expect error "managed by data
    branch". Discover the sname via a subquery on `mo_branch_metadata`.
- **BVT-2 `protect_2.sql` — reclaim on `DATA BRANCH DELETE`**
  - Chain `t1 → t2 → t3`.
  - Verify 2 branch snapshots present.
  - `DATA BRANCH DELETE TABLE t3;` — assert only `__mo_branch_<t3>` is
    gone; `__mo_branch_<t2>` remains.
  - `DATA BRANCH DELETE TABLE t2;` — assert both are gone.
- **BVT-3 `protect_3.sql` — reclaim on plain `DROP TABLE`**
  - Chain `t1 → t2`.
  - `DROP TABLE t2;`
  - Assert `mo_branch_metadata.table_deleted = true` for t2 and
    `__mo_branch_<t2>` row is gone.
- **BVT-4 `protect_4.sql` — subtree semantics**
  - Chain `t1 → t2 → t3`.
  - `DROP TABLE t2;` (without touching t3) — assert `__mo_branch_<t2>`
    is **retained** because t3 is alive.
  - `DROP TABLE t3;` — assert both branch snapshots are released.
- **BVT-5 `protect_5.sql` — fan-out**
  - `t1 → {t2, t3, t4}` (three siblings).
  - Assert three branch snapshots present, all on t1.
  - `DROP TABLE t3;` — assert only `__mo_branch_<t3>` gone.
  - `DROP TABLE t2;` — assert `__mo_branch_<t2>` gone,
    `__mo_branch_<t4>` still present.
- **BVT-6 `protect_6.sql` — SHOW SNAPSHOTS excludes branch rows**
  - Create `t1`, branch `t2`, branch `t3` from `t2`.
  - Create a regular user snapshot `usersp1`.
  - `SHOW SNAPSHOTS;` — returns only `usersp1`.
  - Direct query over `mo_catalog.mo_snapshots` with
    `kind='branch'` shows 2 rows (the two branch protection rows).
- **BVT-7 `protect_7.sql` — cross-account**
  - Create `acc_a` under sys.
  - Under `acc_a`, create `acc_a.dbA.t1`.
  - Under `acc_a`, `DATA BRANCH CREATE TABLE dbA.t2 FROM dbA.t1` (same
    account, simplest cross-account shape: snapshot row is anchored on
    the parent's account which in this case is also `acc_a`).
  - Verify branch snapshot row is queryable under `acc_a` with
    `account_name = 'acc_a'`.
  - `DROP TABLE acc_a.dbA.t2` — assert the branch snapshot row is gone.
- **BVT-8 `protect_8.sql` — `data branch create database` batch insert**
  - Create src db with three tables.
  - `DATA BRANCH CREATE DATABASE dst FROM src` — assert three
    `kind='branch'` rows are produced, one per cloned table, each with
    `obj_id` matching the corresponding src table id.
  - `DATA BRANCH DELETE DATABASE dst` — assert all three branch rows
    are reclaimed in one shot.
- **BVT-9 `protect_9.sql` — plain `drop database` cascade reclaim**
  - Create src db, branch-create a dst db (two tables), then add one
    extra table-level branch edge into dst.
  - Plain `DROP DATABASE dst` — ddl.go iterates the contained tables
    and flips `table_deleted=true` for each. Assert both branch rows
    are reclaimed synchronously and both metadata rows carry
    `table_deleted=true`.
- **BVT-10 `protect_10.sql` — full cross-account via `TO ACCOUNT`**
  - Create two accounts: `sys` (parent) and `acc_protect_child`
    (child).
  - Under child, pre-create the destination database.
  - Under sys, `DATA BRANCH CREATE TABLE dst.t2 FROM src.t1
    {snapshot=...} TO ACCOUNT acc_protect_child`.
  - Assert the branch snapshot row is anchored on the **parent's**
    account (`account_name = 'sys'`, `obj_id = parent_tid`).
  - Under child, `DROP TABLE dst.t2` — assert reclaim crosses the
    account boundary (runs as sys) and wipes the snapshot row.

The `.result` files are generated via `mo-tester` against a reference run
following `docs/ai-skills/testing-guide.md`.

#### 9.3.4 Manual verification checklist (pre-merge)

For each cascaded-diff path (`lcaRight`, `lcaLeft`, `lcaOther`,
`lcaEmpty`), confirm:

1. Create the matching DAG topology.
2. Force a GC cycle.
3. Run the diff; compare output against a gold file.
4. Query `mo_snapshots` and assert the expected branch rows.

### 9.4 Observability

Emit info-level logs at every lifecycle transition:

- `DataBranch-ProtectSnapshot-Create` with `child_tid`, `parent_tid`,
  `parent_account`, `clone_ts`.
- `DataBranch-ProtectSnapshot-Reclaim-Start` with input `dead_tids`.
- `DataBranch-ProtectSnapshot-Reclaim-Done` with list of released snames
  and remaining count.

Metrics (counter):

- `mo_branch_protect_snapshot_total` labelled by `action = create | reclaim`.

---

## 10. Decisions

- **Reclaim cadence**: synchronous. Frontend and compile-layer drop paths
  share one helper (§9.2).
- **Pre-existing branches**: no backfill. Existing branches have already
  lost their LCA history to prior GC cycles; recreate them to gain
  protection.
- **PITR overlap**: no deduplication. PITR lifecycles are user-managed and
  cannot be reliably tracked by this feature, so branch snapshots are
  always created, even when redundant with a covering PITR.

---

## 11. Summary

Branch Protect Snapshot pins LCA-side history for the exact duration a branch
subtree is alive, using the pre-existing `mo_snapshots` machinery with a new
`kind = 'branch'` marker. Creation piggybacks on `DATA BRANCH CREATE` within
the same transaction; reclamation triggers synchronously on
`table_deleted = true` transitions (shared helper between frontend and
compile-layer drop paths) and releases only when the entire dependent
subtree has been dropped. Cross-account branches anchor the snapshot on the
parent's account so GC retention applies in the right place. No schema
change, no user-visible surface change, no backfill for pre-existing
branches.
