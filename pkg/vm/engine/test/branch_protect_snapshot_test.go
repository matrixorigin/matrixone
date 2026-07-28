// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file provides in-process TAE/disttae coverage for the Branch Protect
// Snapshot feature described in docs/design/data_branch_protect_snapshot.md.
//
// The engine-level test harness does not wire the frontend Session that
// `DATA BRANCH CREATE/DELETE` would normally execute under; that flow is
// exercised by the BVT cases in test/distributed/cases/git4data/branch/protect.
// These tests instead stress-test the **invariants** the feature relies on
// when the catalog state it produces is driven through the real disttae +
// TAE stack: branch rows in `mo_branch_metadata` paired with
// `kind='branch'` rows in `mo_snapshots`, reclaimed synchronously by the
// shared DAG walk in pkg/frontend/databranchutils.

package test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
)

// bpsEnv bundles everything the branch-protect snapshot tests need, so each
// subtest can reuse the same engine harness without duplicating the
// 30-line boilerplate that `CreateEngines` already asks for elsewhere.
type bpsEnv struct {
	disttae  *testutil.TestDisttaeEngine
	tae      *testutil.TestTxnStorage
	ctx      context.Context
	cancel   context.CancelFunc
	exec     executor.SQLExecutor
	sysCtx   context.Context
	rpcAgent *testutil.MockRPCAgent
}

func setupBranchProtectSnapshotEnv(t *testing.T) *bpsEnv {
	catalog.SetupDefines("")

	ctx, cancel := context.WithCancel(context.Background())
	sysCtx := context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	sysCtxTimeout, cancelTimeout := context.WithTimeout(sysCtx, time.Minute*5)
	_ = cancelTimeout

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(sysCtx, testutil.TestOptions{}, t)
	// taeHandler's SyncProtectionValidator check is unrelated to this
	// feature and fails loudly when the test skips sync protection
	// registration; disable it defensively.
	taeHandler.GetDB().Runtime.SyncProtectionValidator = nil

	// Bring up the catalog tables the tests touch. `mo_indexes` is a
	// dependency of `CREATE TABLE` (indexed tables insert rows into it)
	// and must be created before any other mo_catalog table the tests
	// rely on. Missing tables would surface as "table does not exist"
	// during the first SQL.
	require.NoError(t, exec_sql(disttaeEngine, sysCtxTimeout, frontend.MoCatalogMoIndexesDDL))
	require.NoError(t, exec_sql(disttaeEngine, sysCtxTimeout, frontend.MoCatalogMoSnapshotsDDL))
	require.NoError(t, exec_sql(disttaeEngine, sysCtxTimeout, frontend.MoCatalogBranchMetadataDDL))

	// Plain `exec.Exec` handle shared by the tests below so they don't
	// need to reach into moruntime every time.
	runtimeVar, ok := lookupInternalSQLExecutor()
	require.True(t, ok, "internal SQL executor must be registered")
	return &bpsEnv{
		disttae:  disttaeEngine,
		tae:      taeHandler,
		ctx:      ctx,
		cancel:   cancel,
		exec:     runtimeVar,
		sysCtx:   sysCtxTimeout,
		rpcAgent: rpcAgent,
	}
}

func (e *bpsEnv) close(t *testing.T) {
	t.Helper()
	if e == nil {
		return
	}
	if e.disttae != nil {
		e.disttae.Close(e.ctx)
	}
	if e.tae != nil {
		e.tae.Close(true)
	}
	if e.rpcAgent != nil {
		e.rpcAgent.Close()
	}
	if e.cancel != nil {
		e.cancel()
	}
}

// execSQL runs `sql` under `ctx` inside a fresh test-side txn that is
// committed on success; used for DDL and DML driven by the test itself.
// It intentionally does NOT reuse the harness helper (execSql in
// cdc_testutil.go) because that helper returns an `executor.Result` that
// must always be Close()'d even when ignored, and we want a simpler shape
// here.
func (e *bpsEnv) execSQL(ctx context.Context, sql string) (executor.Result, error) {
	txn, err := e.disttae.NewTxnOperator(ctx, e.disttae.Now())
	if err != nil {
		return executor.Result{}, err
	}
	opts := executor.Options{}.WithDisableIncrStatement().WithTxn(txn)
	res, err := e.exec.Exec(ctx, sql, opts)
	if err != nil {
		return res, err
	}
	if cerr := txn.Commit(ctx); cerr != nil {
		return res, cerr
	}
	return res, nil
}

// querySnapshotsByPrefix returns the set of `sname` values present in
// mo_snapshots that start with the branch protect snapshot prefix. Sorted
// for determinism.
func (e *bpsEnv) querySnapshotsByPrefix(t *testing.T, prefix string) []string {
	t.Helper()
	sql := fmt.Sprintf(
		"select sname from %s.%s where sname like '%s%%' order by sname",
		catalog.MO_CATALOG, catalog.MO_SNAPSHOTS, prefix,
	)
	res, err := e.execSQL(e.sysCtx, sql)
	require.NoError(t, err)
	defer res.Close()
	var out []string
	res.ReadRows(func(n int, cols []*vector.Vector) bool {
		if n == 0 {
			return true
		}
		data, area := vector.MustVarlenaRawData(cols[0])
		for i := 0; i < n; i++ {
			out = append(out, data[i].GetString(area))
		}
		return true
	})
	sort.Strings(out)
	return out
}

// loadBranchDAG queries mo_branch_metadata and builds a DAG via
// databranchutils. Matches exactly what the compile-layer reclaim path
// does.
func (e *bpsEnv) loadBranchDAG(t *testing.T) databranchutils.BranchReclaimDag {
	t.Helper()
	sql := fmt.Sprintf(
		"select table_id, p_table_id, clone_ts, table_deleted from %s.%s",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA,
	)
	res, err := e.execSQL(e.sysCtx, sql)
	require.NoError(t, err)
	defer res.Close()
	var rows []databranchutils.DataBranchMetadata
	res.ReadRows(func(n int, cols []*vector.Vector) bool {
		if n == 0 {
			return true
		}
		tids := vector.MustFixedColWithTypeCheck[uint64](cols[0])
		pids := vector.MustFixedColWithTypeCheck[uint64](cols[1])
		cts := vector.MustFixedColWithTypeCheck[int64](cols[2])
		for i := 0; i < n; i++ {
			deleted := !cols[3].IsNull(uint64(i)) &&
				vector.GetFixedAtWithTypeCheck[bool](cols[3], i)
			rows = append(rows, databranchutils.DataBranchMetadata{
				TableID:      tids[i],
				CloneTS:      cts[i],
				PTableID:     pids[i],
				TableDeleted: deleted,
			})
		}
		return true
	})
	return databranchutils.NewBranchReclaimDag(rows)
}

// simulateBranchCreate inserts the (mo_branch_metadata, mo_snapshots) pair
// that `DATA BRANCH CREATE` would produce when a child table of the given
// tid is cloned from a parent table. This mirrors the two writes
// `updateBranchMetaTable` + `createBranchProtectSnapshot` perform inside
// the same txn in the real flow (§5.1).
func (e *bpsEnv) simulateBranchCreate(
	t *testing.T,
	childTID, parentTID uint64,
	cloneTS int64,
	parentAccount, parentDB, parentTbl string,
	parentTableID uint64,
) {
	t.Helper()
	require.NoError(t, exec_sql(e.disttae, e.sysCtx,
		fmt.Sprintf(
			"insert into %s.%s values(%d, %d, %d, %d, 'table', false)",
			catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA,
			childTID, cloneTS, parentTID, 0,
		),
	))

	// Mint an arbitrary-but-deterministic snapshot id. The real flow
	// uses uuid.NewV7(); in the test any syntactically valid UUID will do.
	snapshotID := fmt.Sprintf("019e06ae-0000-7000-8000-%012d", childTID)
	sname := databranchutils.BranchSnapshotName(childTID)
	require.NoError(t, exec_sql(e.disttae, e.sysCtx,
		fmt.Sprintf(
			"insert into %s.%s(snapshot_id, sname, ts, level, account_name, database_name, table_name, obj_id, kind) "+
				"values('%s','%s',%d,'table','%s','%s','%s',%d,'%s')",
			catalog.MO_CATALOG, catalog.MO_SNAPSHOTS,
			snapshotID, sname, cloneTS,
			parentAccount, parentDB, parentTbl,
			parentTableID, databranchutils.BranchSnapshotKind,
		),
	))
}

// markBranchDeleted flips `table_deleted=true` for a given child tid.
// Matches the effect of the UPDATE issued by ddl.go before the reclaim
// hook fires.
func (e *bpsEnv) markBranchDeleted(t *testing.T, childTID uint64) {
	t.Helper()
	require.NoError(t, exec_sql(e.disttae, e.sysCtx,
		fmt.Sprintf(
			"update %s.%s set table_deleted = true where table_id = %d",
			catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA, childTID,
		),
	))
}

// runReclaim wires the engine-level plumbing the compile layer uses and
// drives the shared core exactly as `(*Compile).reclaimBranchProtectSnapshots`
// does.
func (e *bpsEnv) runReclaim(t *testing.T, deadTIDs []uint64) []string {
	t.Helper()
	loadDAG := func() (databranchutils.BranchReclaimDag, error) {
		return e.loadBranchDAG(t), nil
	}
	var executedSQL string
	execDelete := func(snames []string) error {
		executedSQL = databranchutils.BuildBranchSnapshotDeleteSQL(snames)
		return exec_sql(e.disttae, e.sysCtx, executedSQL)
	}
	err := databranchutils.ReclaimBranchSnapshotsCore(deadTIDs, loadDAG, execDelete)
	require.NoError(t, err)
	_ = executedSQL
	// Re-query for the surviving branch rows so callers can assert state.
	return e.querySnapshotsByPrefix(t, databranchutils.BranchSnapshotSnamePrefix)
}

// lookupInternalSQLExecutor indirects through the moruntime registry to
// grab the SQL executor the test services register at startup. It is
// local to this file to avoid hard-coding moruntime paths in callers.
func lookupInternalSQLExecutor() (executor.SQLExecutor, bool) {
	rt := moruntime.ServiceRuntime("")
	if rt == nil {
		return nil, false
	}
	v, ok := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return nil, false
	}
	sqlExec, ok := v.(executor.SQLExecutor)
	return sqlExec, ok
}

// ---------------------------------------------------------------------------
// ET-G1 — Created: branch protect snapshot row exists with the right shape.
// ---------------------------------------------------------------------------

func TestBranchProtectSnapshot_Created(t *testing.T) {
	env := setupBranchProtectSnapshotEnv(t)
	defer env.close(t)

	const (
		parentTID = uint64(1001)
		childTID  = uint64(2001)
		cloneTS   = int64(10_000_000_000)
	)
	env.simulateBranchCreate(t, childTID, parentTID, cloneTS, "sys", "db1", "t1", parentTID)

	rows := env.querySnapshotsByPrefix(t, databranchutils.BranchSnapshotSnamePrefix)
	require.Equal(t, []string{databranchutils.BranchSnapshotName(childTID)}, rows,
		"created branch should surface exactly one `__mo_branch_<child>` row")

	// ts, level, obj_id and kind must match the invariants from §4.1.
	sel := fmt.Sprintf(
		"select ts, level, obj_id, kind from %s.%s where sname='%s'",
		catalog.MO_CATALOG, catalog.MO_SNAPSHOTS,
		databranchutils.BranchSnapshotName(childTID),
	)
	res, err := env.execSQL(env.sysCtx, sel)
	require.NoError(t, err)
	defer res.Close()
	var (
		sawTS    int64
		sawLevel string
		sawObjID uint64
		sawKind  string
	)
	res.ReadRows(func(n int, cols []*vector.Vector) bool {
		if n == 0 {
			return true
		}
		sawTS = vector.MustFixedColWithTypeCheck[int64](cols[0])[0]
		levelData, levelArea := vector.MustVarlenaRawData(cols[1])
		sawLevel = levelData[0].GetString(levelArea)
		sawObjID = vector.MustFixedColWithTypeCheck[uint64](cols[2])[0]
		kindData, kindArea := vector.MustVarlenaRawData(cols[3])
		sawKind = kindData[0].GetString(kindArea)
		return false
	})
	require.Equal(t, cloneTS, sawTS, "ts must match clone_ts")
	require.Equal(t, "table", sawLevel, "level must be 'table'")
	require.Equal(t, parentTID, sawObjID, "obj_id must point at parent")
	require.Equal(t, databranchutils.BranchSnapshotKind, sawKind, "kind must be 'branch'")
}

// ---------------------------------------------------------------------------
// ET-G3 — ReclaimOnDataBranchDelete: only the leaf edge is reclaimed.
// ---------------------------------------------------------------------------

func TestBranchProtectSnapshot_ReclaimOnDataBranchDelete(t *testing.T) {
	env := setupBranchProtectSnapshotEnv(t)
	defer env.close(t)

	const (
		t1 = uint64(3001) // root in test-space
		t2 = uint64(3002)
		t3 = uint64(3003)
	)
	env.simulateBranchCreate(t, t2, t1, 100_000, "sys", "db", "tbl1", t1)
	env.simulateBranchCreate(t, t3, t2, 200_000, "sys", "db", "tbl2", t2)

	env.markBranchDeleted(t, t3)
	remaining := env.runReclaim(t, []uint64{t3})
	require.Equal(t, []string{databranchutils.BranchSnapshotName(t2)}, remaining,
		"only __mo_branch_<t3> is reclaimable because t2 is still alive")
}

// ---------------------------------------------------------------------------
// ET-G4 — ReclaimOnPlainDropTable: shared helper behaves identically.
// ---------------------------------------------------------------------------

func TestBranchProtectSnapshot_ReclaimOnPlainDropTable(t *testing.T) {
	env := setupBranchProtectSnapshotEnv(t)
	defer env.close(t)

	const (
		t1 = uint64(4001)
		t2 = uint64(4002)
	)
	env.simulateBranchCreate(t, t2, t1, 300_000, "sys", "db", "tbl1", t1)

	// Simulate ddl.go's first SQL in the drop-table chain: flip
	// table_deleted=true for the child.
	env.markBranchDeleted(t, t2)
	// Then drive the same reclaim core the compile path uses.
	remaining := env.runReclaim(t, []uint64{t2})
	require.Empty(t, remaining, "plain DROP TABLE must release the child's branch snapshot")
}

// ---------------------------------------------------------------------------
// ET-G5 — ReclaimCascaded: drop intermediate then leaf.
// ---------------------------------------------------------------------------

func TestBranchProtectSnapshot_ReclaimCascaded(t *testing.T) {
	env := setupBranchProtectSnapshotEnv(t)
	defer env.close(t)

	const (
		t1 = uint64(5001)
		t2 = uint64(5002)
		t3 = uint64(5003)
	)
	env.simulateBranchCreate(t, t2, t1, 500_001, "sys", "db", "tbl1", t1)
	env.simulateBranchCreate(t, t3, t2, 500_002, "sys", "db", "tbl2", t2)

	// Drop t2 first — t3 is still alive, so NEITHER branch snapshot is
	// reclaimable. Candidates = {t2, t1}.
	env.markBranchDeleted(t, t2)
	remaining := env.runReclaim(t, []uint64{t2})
	require.ElementsMatch(t,
		[]string{
			databranchutils.BranchSnapshotName(t2),
			databranchutils.BranchSnapshotName(t3),
		},
		remaining,
		"t3 is alive; no branch snapshot must be released yet")

	// Now drop t3 — both snapshots must be released.
	env.markBranchDeleted(t, t3)
	remaining = env.runReclaim(t, []uint64{t3})
	require.Empty(t, remaining, "all snapshots must be released once the whole subtree is gone")
}

// ---------------------------------------------------------------------------
// ET-G6 — CrossAccount: snapshot is anchored on the parent's account, and
// reclaim (as sys) clears it regardless of the dropping account.
// ---------------------------------------------------------------------------

func TestBranchProtectSnapshot_CrossAccount(t *testing.T) {
	env := setupBranchProtectSnapshotEnv(t)
	defer env.close(t)

	const (
		parentTID = uint64(6001)
		childTID  = uint64(6002)
	)
	// Parent lives in account `acc_a` (id 999); child in acc_b (not
	// relevant for the assertion because the snapshot row carries the
	// parent's account_name as §6 dictates).
	env.simulateBranchCreate(t, childTID, parentTID, 600_000, "acc_a", "db", "t1", parentTID)

	sel := fmt.Sprintf(
		"select account_name from %s.%s where sname='%s'",
		catalog.MO_CATALOG, catalog.MO_SNAPSHOTS,
		databranchutils.BranchSnapshotName(childTID),
	)
	res, err := env.execSQL(env.sysCtx, sel)
	require.NoError(t, err)
	defer res.Close()
	var sawAcc string
	res.ReadRows(func(n int, cols []*vector.Vector) bool {
		if n == 0 {
			return true
		}
		data, area := vector.MustVarlenaRawData(cols[0])
		sawAcc = data[0].GetString(area)
		return false
	})
	require.Equal(t, "acc_a", sawAcc, "snapshot must be anchored on the parent's account")

	// Dropping as sys must still be able to reclaim the cross-account
	// row (the DELETE runs under sys per design §6).
	env.markBranchDeleted(t, childTID)
	remaining := env.runReclaim(t, []uint64{childTID})
	require.Empty(t, remaining)
}

// ---------------------------------------------------------------------------
// ET-G7 — CrossAccount drop of the *source* leaves the branch snapshot
// alive because the child (in account b) is still referenced.
// ---------------------------------------------------------------------------

func TestBranchProtectSnapshot_CrossAccount_DropSourceFirst(t *testing.T) {
	env := setupBranchProtectSnapshotEnv(t)
	defer env.close(t)

	const (
		parentTID = uint64(7001)
		childTID  = uint64(7002)
	)
	env.simulateBranchCreate(t, childTID, parentTID, 700_000, "acc_a", "db", "t1", parentTID)

	// The "parent dropped" flow does not update mo_branch_metadata for
	// the parent (the parent is not a branch). The reclaim hook only
	// runs against the child tid. So if the operator only drops the
	// parent, the child row stays.
	remaining := env.runReclaim(t, []uint64{parentTID})
	require.Equal(t,
		[]string{databranchutils.BranchSnapshotName(childTID)},
		remaining,
		"dropping only the parent must NOT reclaim the child's branch snapshot",
	)
}

// ---------------------------------------------------------------------------
// ET-G8 — CreateFailedRollsBack (shared-txn semantics).
// ---------------------------------------------------------------------------

// TestBranchProtectSnapshot_CreateFailedRollsBack demonstrates the §5.2
// atomicity guarantee at the SQL level: if a txn inserts a
// `mo_branch_metadata` row and then fails before inserting the matching
// `mo_snapshots` row, an outer rollback makes BOTH disappear. The real
// frontend flow wraps all three steps in `bh`'s deferred finishTxn which
// issues the rollback on error; at the engine harness we drive the same
// invariant through the raw SQL executor.
func TestBranchProtectSnapshot_CreateFailedRollsBack(t *testing.T) {
	env := setupBranchProtectSnapshotEnv(t)
	defer env.close(t)

	const (
		parentTID = uint64(8001)
		childTID  = uint64(8002)
	)

	txn, err := env.disttae.NewTxnOperator(env.sysCtx, env.disttae.Now())
	require.NoError(t, err)
	opts := executor.Options{}.WithDisableIncrStatement().WithTxn(txn)

	// Step 1: insert mo_branch_metadata successfully.
	_, err = env.exec.Exec(env.sysCtx, fmt.Sprintf(
		"insert into %s.%s values(%d, %d, %d, %d, 'table', false)",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA,
		childTID, 800_000, parentTID, 0,
	), opts)
	require.NoError(t, err)

	// Step 2: simulate a failure at the snapshot-insert step by issuing
	// an intentionally broken SQL within the same txn. The outer test
	// then rolls back the txn.
	_, err = env.exec.Exec(env.sysCtx, "select * from mo_catalog.__does_not_exist__", opts)
	require.Error(t, err)

	require.NoError(t, txn.Rollback(env.sysCtx))

	// The rolled-back txn must have left no row behind.
	sel := fmt.Sprintf(
		"select count(*) from %s.%s where table_id=%d",
		catalog.MO_CATALOG, catalog.MO_BRANCH_METADATA, childTID,
	)
	res, err := env.execSQL(env.sysCtx, sel)
	require.NoError(t, err)
	defer res.Close()
	var cnt int64
	res.ReadRows(func(n int, cols []*vector.Vector) bool {
		if n == 0 {
			return true
		}
		cnt = vector.MustFixedColWithTypeCheck[int64](cols[0])[0]
		return false
	})
	require.Zero(t, cnt, "rollback must erase the orphan mo_branch_metadata row")

	// And nothing shows up in mo_snapshots under the child's sname.
	remaining := env.querySnapshotsByPrefix(t, databranchutils.BranchSnapshotSnamePrefix)
	for _, s := range remaining {
		require.False(t,
			strings.HasSuffix(s, fmt.Sprintf("%d", childTID)),
			"no branch-snapshot row must remain for the rolled-back child")
	}
}
