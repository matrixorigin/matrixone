# Snapshot-aware ANALYZE Column Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `ANALYZE TABLE table{snapshot=...}` without an explicit column list expand columns from the historical table definition instead of the current schema.

**Architecture:** Reuse frontend's canonical `resolveSnapshot` wrapper around `QueryBuilder.ResolveTsHint`, then pass the returned `*plan.Snapshot` into `TxnCompilerContext.Resolve`. Keep generated SQL and explicit-column behavior unchanged, and prove the fix with a schema-evolution snapshot regression in the existing distributed ANALYZE case.

**Tech Stack:** Go, MatrixOne frontend/compiler context, MatrixOne SQL BVT, mo-tester, MySQL protocol client

## Global Constraints

- Work only in `/Users/yanghaoyang/repo/matrixone/.worktrees/pr-24659-ci-fix` on `codex/pr-24659-ci-fix`.
- Preserve the untracked `thirdparties/install` dependency directory and never stage it.
- Use the existing `resolveSnapshot`/`QueryBuilder.ResolveTsHint` semantics; do not duplicate timestamp parsing.
- Do not mutate `TableName.AtTsExpr`, session snapshot state, or explicit ANALYZE column lists.
- Add the regression before production code and observe it fail for the expected current-schema column mismatch.

---

### Task 1: Add and prove the schema-evolution regression

**Files:**
- Modify: `test/distributed/cases/analyze/analyze_stmt.sql`
- Modify: `test/distributed/cases/analyze/analyze_stmt.result`

**Interfaces:**
- Consumes: Existing `ANALYZE TABLE` BVT case and named account snapshot SQL syntax.
- Produces: An end-to-end regression requiring implicit columns to come from the named snapshot.

- [ ] **Step 1: Add the failing SQL scenario before the CHECK TABLE section**

Insert this block after the existing nonexistent-table ANALYZE case:

```sql
-- implicit columns must be resolved from the same historical schema
drop snapshot if exists analyze_schema_snapshot;
drop table if exists snapshot_cols;
create table snapshot_cols(old_col int);
insert into snapshot_cols values (1), (2);
create snapshot analyze_schema_snapshot for account;
alter table snapshot_cols add column current_only int;
analyze table snapshot_cols {snapshot = 'analyze_schema_snapshot'};
select 'AFTER_SNAPSHOT_ANALYZE';
drop snapshot analyze_schema_snapshot;
drop table snapshot_cols;
```

Mirror the successful statements in `analyze_stmt.result`, including:

```text
analyze table snapshot_cols {snapshot = 'analyze_schema_snapshot'};
select 'AFTER_SNAPSHOT_ANALYZE';
AFTER_SNAPSHOT_ANALYZE
AFTER_SNAPSHOT_ANALYZE
```

- [ ] **Step 2: Build and start the unmodified PR head**

Run:

```bash
make build
./mo-service -launch etc/launch/launch.toml
```

Expected: `make build` exits 0 and MatrixOne listens on `127.0.0.1:6001`. Keep the service in a separate PTY session.

- [ ] **Step 3: Run the focused SQL before changing production code**

Run the snapshot sequence through one persistent MySQL client:

```bash
mysql --protocol=tcp -h127.0.0.1 -P6001 -uroot -p111 --force <<'SQL'
drop database if exists analyze_snapshot_red;
create database analyze_snapshot_red;
use analyze_snapshot_red;
drop snapshot if exists analyze_schema_snapshot;
create table snapshot_cols(old_col int);
insert into snapshot_cols values (1), (2);
create snapshot analyze_schema_snapshot for account;
alter table snapshot_cols add column current_only int;
analyze table snapshot_cols {snapshot = 'analyze_schema_snapshot'};
select 'AFTER_SNAPSHOT_ANALYZE';
drop snapshot analyze_schema_snapshot;
drop database analyze_snapshot_red;
SQL
```

Expected before the fix: ANALYZE fails because `current_only` was expanded from the current schema but is absent from the snapshot. The marker SELECT still executes, proving the failure is schema mismatch rather than a broken connection.

- [ ] **Step 4: Stop the service and retain the RED evidence**

Send Ctrl-C to the MatrixOne PTY and retain the exact ANALYZE error for the final verification report.

### Task 2: Resolve implicit columns against the timestamp hint

**Files:**
- Modify: `pkg/frontend/mysql_cmd_executor.go:1320`
- Test: `test/distributed/cases/analyze/analyze_stmt.sql`
- Test: `test/distributed/cases/analyze/analyze_stmt.result`

**Interfaces:**
- Consumes: `resolveSnapshot(ses *Session, atTsExpr *tree.AtTimeStamp) (*plan.Snapshot, error)` from `pkg/frontend/clone.go`.
- Produces: `resolveTableVisibleColumns` passes the resolved `*plan.Snapshot` to `TxnCompilerContext.Resolve`.

- [ ] **Step 1: Implement the minimal snapshot-aware lookup**

Replace the nil-snapshot lookup with:

```go
snapshot, err := resolveSnapshot(ses, tbl.AtTsExpr)
if err != nil {
	return nil, err
}

tcc := ses.GetTxnCompileCtx()
_, tableDef, err := tcc.Resolve(dbName, tblName, snapshot)
```

Do not change `buildAnalyzeDerivedSQL`; it must continue formatting the original timestamp hint for normal planning and execution.

- [ ] **Step 2: Format and run focused Go checks**

Run:

```bash
gofmt -w pkg/frontend/mysql_cmd_executor.go
go test ./pkg/frontend -run 'Test(HandleAnalyze|BuildAnalyze|ExecuteAnalyze)' -count=1
go test ./pkg/sql/plan -run 'Test.*Snapshot' -count=1
```

Expected: both `go test` commands exit 0.

- [ ] **Step 3: Rebuild and rerun the exact RED SQL**

Run:

```bash
make build
./mo-service -launch etc/launch/launch.toml
```

Then rerun the persistent-client SQL from Task 1 Step 3.

Expected after the fix: ANALYZE produces no error, `AFTER_SNAPSHOT_ANALYZE` is returned, and cleanup succeeds.

- [ ] **Step 4: Run the full focused BVT**

If mo-tester is not already available, install it outside the repository:

```bash
git clone https://github.com/matrixorigin/mo-tester.git /tmp/mo-tester
```

With the locally built MatrixOne service running, run:

```bash
cd /tmp/mo-tester
./run.sh -n -g -p /Users/yanghaoyang/repo/matrixone/.worktrees/pr-24659-ci-fix/test/distributed/cases/analyze/analyze_stmt.sql
```

Expected: the analyze case reports zero failed and zero abnormal tests.

- [ ] **Step 5: Commit the tested fix and regression**

Run:

```bash
git add pkg/frontend/mysql_cmd_executor.go test/distributed/cases/analyze/analyze_stmt.sql test/distributed/cases/analyze/analyze_stmt.result
git commit -m "fix: resolve analyze columns at table snapshot"
```

Expected: only the three named files are included; `thirdparties/install` remains untracked.

### Task 3: Complete local confidence gates and prepare the PR update

**Files:**
- Review: `pkg/frontend/mysql_cmd_executor.go`
- Review: `test/distributed/cases/analyze/analyze_stmt.sql`
- Review: `test/distributed/cases/analyze/analyze_stmt.result`
- Review: `docs/superpowers/specs/2026-07-13-analyze-snapshot-column-resolution-design.md`

**Interfaces:**
- Consumes: The committed snapshot-aware lookup and passing focused regression.
- Produces: Evidence-backed, self-reviewed commits ready to push to the existing PR branch.

- [ ] **Step 1: Run broad frontend and parser gates**

Run:

```bash
go test ./pkg/frontend -count=1
go vet ./pkg/frontend
go test ./pkg/sql/parsers/... ./pkg/sql/plan -count=1
```

Expected: every command exits 0.

- [ ] **Step 2: Repeat the complete snapshot scenario**

Run the persistent-client sequence from Task 1 Step 3 twenty times against the rebuilt service, using unique snapshot/database names or complete cleanup between iterations.

Expected: 20/20 iterations complete without ANALYZE errors or connection/protocol failures.

- [ ] **Step 3: Perform MatrixOne pre-push self-review**

Apply the `mo-self-review` skill to the complete diff from the PR merge base through HEAD. Check timestamp-hint errors, nil snapshots, explicit columns, mixed table entries, schema qualification, cleanup, protocol state, and unintended files.

Run:

```bash
git diff --check origin/fix/issue-23122-analyze-check-show-profile...HEAD
git status --short
git log --oneline origin/fix/issue-23122-analyze-check-show-profile..HEAD
```

Expected: no whitespace errors; only intentional commits are ahead; `thirdparties/install` is the only unrelated untracked path.

- [ ] **Step 4: Rebase or fast-forward safety check and push**

Run:

```bash
git fetch origin fix/issue-23122-analyze-check-show-profile
git merge-base --is-ancestor origin/fix/issue-23122-analyze-check-show-profile HEAD
git push origin HEAD:fix/issue-23122-analyze-check-show-profile
```

Expected: the ancestry check and push both exit 0 without force, updating PR #24659.
