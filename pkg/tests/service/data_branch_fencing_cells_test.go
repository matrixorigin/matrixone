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

package service

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// (H.10)(p)/(r)/(s) cells of the pk-move fencing design (D186(B.3) generation
// scoping, D186(B.5) D155-C' transaction-backed seam cells). Statements run
// through REAL user sessions on the target CN's SQL port, mirroring the
// drop path the production owner-removal reap rides (the internal executor
// cannot issue CREATE ACCOUNT).
// ---------------------------------------------------------------------------

func dbrOpenDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(2)
	return db
}

func dbrSysDSN(addr string) string {
	return fmt.Sprintf("dump:111@tcp(%s)/", addr)
}

func dbrTenantDSN(addr, account string) string {
	return fmt.Sprintf("%s#admin#accountadmin:111@tcp(%s)/", account, addr)
}

func dbrSQL(t *testing.T, ctx context.Context, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.ExecContext(ctx, query); err != nil {
		t.Fatalf("sql failed: %s: %v", query, err)
	}
}

func dbrSQLStr(t *testing.T, ctx context.Context, db *sql.DB, query string) string {
	t.Helper()
	var s string
	if err := db.QueryRowContext(ctx, query).Scan(&s); err != nil {
		t.Fatalf("sql failed: %s: %v", query, err)
	}
	return s
}

func dbrWaitCond(t *testing.T, timeout time.Duration, label string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition %q not met within %s", label, timeout)
}

// dbrErrTransientClass reports the sibling-CN-close turbulence class:
// the statement's transaction reaches a shared lock table living on a
// co-CN that is mid-close (bind migration in flight / lockservice
// leaving the cluster inventory). Production clients retry this class;
// the cells' oracles must observe the completion a retrying client sees
// while a sibling drains — a FAILED statement rolls back atomically, so
// a retried DDL restarts from a clean slate. Every error outside this
// class stays terminal.
func dbrErrTransientClass(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "cannot connect to backend") ||
		strings.Contains(msg, "lock table bind changed") ||
		strings.Contains(msg, "is absent from cluster inventory")
}

// dbrRetryWindow bounds the per-statement retry: a harness CN's full
// close drains in tens of seconds, so 90s is generous without being
// unbounded.
const dbrRetryWindow = 90 * time.Second

func dbrSleepRetry(err error, deadline time.Time) bool {
	if !dbrErrTransientClass(err) || time.Now().After(deadline) {
		return false
	}
	time.Sleep(200 * time.Millisecond)
	return true
}

func dbrSQLRetry(t *testing.T, ctx context.Context, db *sql.DB, query string) {
	t.Helper()
	deadline := time.Now().Add(dbrRetryWindow)
	for {
		if _, err := db.ExecContext(ctx, query); err != nil {
			if dbrSleepRetry(err, deadline) {
				continue
			}
			t.Fatalf("sql failed: %s: %v", query, err)
		}
		return
	}
}

func dbrSQLStrRetry(t *testing.T, ctx context.Context, db *sql.DB, query string) string {
	t.Helper()
	deadline := time.Now().Add(dbrRetryWindow)
	for {
		var s string
		if err := db.QueryRowContext(ctx, query).Scan(&s); err != nil {
			if dbrSleepRetry(err, deadline) {
				continue
			}
			t.Fatalf("sql failed: %s: %v", query, err)
		}
		return s
	}
}

// dbrCellRetainedShape builds one retained dead-branch-table shape in the
// account's own database - parent, branch, pk move, drop - and returns the
// dropped branch table's table_id as a string. A covering owner created
// AFTER this call grounds the retention rows; removing that owner runs the
// certified reap for exactly this address.
func dbrCellRetainedShape(
	t *testing.T, ctx context.Context, db *sql.DB, dbName, tag string,
) string {
	t.Helper()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	exec := func(query string) {
		t.Helper()
		deadline := time.Now().Add(dbrRetryWindow)
		for {
			if _, err := conn.ExecContext(ctx, query); err != nil {
				if dbrSleepRetry(err, deadline) {
					continue
				}
				t.Fatalf("sql failed: %s: %v", query, err)
			}
			return
		}
	}
	exec(fmt.Sprintf("create database if not exists %s", dbName))
	exec(fmt.Sprintf("use %s", dbName))
	exec(fmt.Sprintf("create table %s_p (id int primary key, v varchar(32))", tag))
	exec(fmt.Sprintf("insert into %s_p values (2, 'two')", tag))
	exec(fmt.Sprintf("data branch create table %s_b from %s_p", tag, tag))
	exec(fmt.Sprintf("update %s_b set id = 3 where id = 2", tag))
	var tid string
	{
		tidQuery := fmt.Sprintf("select cast(rel_id as char) from mo_catalog.mo_tables where relname = '%s_b' and reldatabase = '%s'", tag, dbName)
		deadline := time.Now().Add(dbrRetryWindow)
		for {
			if err := conn.QueryRowContext(ctx, tidQuery).Scan(&tid); err != nil {
				if dbrSleepRetry(err, deadline) {
					continue
				}
				require.NoError(t, err)
			}
			break
		}
	}
	exec(fmt.Sprintf("drop table %s_b", tag))
	return tid
}

// dbrCellLinkFingerprint is one retained address's links/ledger fingerprint
// (count + numeric content sums per catalog), read under the OWNING account
// (both catalogs are per-account physical relations). The summed columns
// cover every numeric identity the rows carry.
func dbrCellLinkFingerprint(
	t *testing.T, ctx context.Context, db *sql.DB, tid string,
) string {
	t.Helper()
	upd := dbrSQLStrRetry(t, ctx, db,
		fmt.Sprintf("select concat_ws('|', cast(count(*) as char), coalesce(cast(sum(table_id) as char), 'x'), coalesce(cast(sum(write_ts) as char), 'x')) from mo_catalog.mo_data_branch_update_link where table_id = %s", tid))
	ret := dbrSQLStrRetry(t, ctx, db,
		fmt.Sprintf("select concat_ws('|', cast(count(*) as char), coalesce(cast(sum(row_kind) as char), 'x'), coalesce(cast(sum(obj_id + created_ts + logical_id) as char), 'x')) from mo_catalog.mo_data_branch_link_retention where table_id = %s", tid))
	return upd + "||" + ret
}

// dbrSeamObserver is the ordered witness recorder (D186(B.5)(ii)): account
// steps COMPLETE in order; the seam-armed attempt must show exactly the
// prefix that ran before the named account's step failed.
type dbrSeamObserver struct {
	mu   sync.Mutex
	seen []uint32
}

func (o *dbrSeamObserver) fire(id uint32) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.seen = append(o.seen, id)
}

func (o *dbrSeamObserver) reset() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.seen = nil
}

func (o *dbrSeamObserver) snapshot() []uint32 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]uint32(nil), o.seen...)
}

// dbrOwnerFlavor binds the two owner kinds the (r)/(s) cells twin over;
// every fingerprint is one deterministic varchar (byte-compare).
type dbrOwnerFlavor struct {
	createSQL func(name string) string
	dropSQL   func(name string) string
	rowSQL    func(name string) string
}

func dbrPitrFlavor() dbrOwnerFlavor {
	return dbrOwnerFlavor{
		createSQL: func(name string) string {
			return fmt.Sprintf("create pitr %s for cluster range 1 'h'", name)
		},
		dropSQL: func(name string) string { return fmt.Sprintf("drop pitr %s", name) },
		// String fingerprint: the cluster level writes obj_id =
		// math.MaxUint64 (pitr.go), so any arithmetic sum of the live
		// row overflows uint64. A per-column concat is strictly stronger
		// than a sum (no aliasing) and never overflows.
		rowSQL: func(name string) string {
			return fmt.Sprintf("select concat_ws('|', cast(count(*) as char), coalesce(group_concat(concat_ws('~', cast(pitr_id as char), cast(create_account as char), cast(create_time as char), cast(modified_time as char), cast(obj_id as char), cast(pitr_status as char), cast(pitr_length as char))), 'none')) from mo_catalog.mo_pitr where pitr_name = '%s'", name)
		},
	}
}

func dbrSnapshotFlavor() dbrOwnerFlavor {
	return dbrOwnerFlavor{
		createSQL: func(name string) string {
			return fmt.Sprintf("create snapshot %s for cluster", name)
		},
		dropSQL: func(name string) string { return fmt.Sprintf("drop snapshot %s", name) },
		rowSQL: func(name string) string {
			return fmt.Sprintf("select concat_ws('|', cast(count(*) as char), coalesce(group_concat(concat_ws('~', cast(snapshot_id as char), cast(sname as char), cast(ts as char), cast(obj_id as char))), 'none')) from mo_catalog.mo_snapshots where sname = '%s'", name)
		},
	}
}

// dbrAccountSnapshotFlavor is the owner flavor for the generation cell's
// retirement cycles: account-scoped, so its create/drop execute on the
// session's own CN (the cell's whole point is running B's rounds while a
// SIBLING CN proceeds through its full close; cluster-scoped DDL fans out
// to every CN backend and is transiently refused during the sibling's
// drain — tolerated production behavior outside this cell's assertions).
// Account- and cluster-level owners ground historical branches alike under
// the structural coverage predicate, so the cycle's reap and fencing
// rounds are unchanged.
func dbrAccountSnapshotFlavor() dbrOwnerFlavor {
	return dbrOwnerFlavor{
		createSQL: func(name string) string {
			return fmt.Sprintf("create snapshot %s for account sys", name)
		},
		dropSQL: func(name string) string { return fmt.Sprintf("drop snapshot %s", name) },
		rowSQL: func(name string) string {
			return fmt.Sprintf("select concat_ws('|', cast(count(*) as char), coalesce(group_concat(concat_ws('~', cast(snapshot_id as char), cast(sname as char), cast(ts as char), cast(obj_id as char))), 'none')) from mo_catalog.mo_snapshots where sname = '%s'", name)
		},
	}
}

// TestDataBranchFencingReapSeamTransactionBacked is (H.10)(r)/(s): the
// D155-C' transaction-backed fail-atomicity cells. The seam (armed for the
// SECOND of three accounts) fails the owner-removal reap strictly after
// account A1's certified step; the drop's own transaction aborts; the
// owner row and every account's links/ledger byte-compare EQUAL to the
// pre-drop state; the identical retry with the seam cleared reaps all
// three to zero. The seam-silent (RED twin) landing-zone control runs
// first: with no seam the reap completes on the first attempt.
// Certification stays enforced end to end (the fault framework is never
// enabled).
func TestDataBranchFencingReapSeamTransactionBacked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	observer := &dbrSeamObserver{}
	restoreObserver := frontend.SetDataBranchOwnerRemovalReapObserverForTest(observer.fire)
	t.Cleanup(restoreObserver)

	run := func(t *testing.T, tag string, flavor dbrOwnerFlavor) {
		// One cluster PER FLAVOR. Owner coverage is decided structurally
		// (scope/level: a live cluster pitr or snapshot grounds EVERY
		// account's historical branches, pk_move_owner_coverage.go), so
		// a leftover cluster owner from the sibling flavor would silently
		// KEEP every shape through the reap and void the cell. Each
		// flavor therefore proves its story in a clean environment.
		c, err := NewCluster(ctx, t,
			DefaultOptions().WithRootDataDir(t.TempDir()).WithLogLevel(zap.WarnLevel))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Close()) })
		require.NoError(t, c.Start())
		c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
		cnSvc, err := c.GetCNServiceIndexed(0)
		require.NoError(t, err)
		pool := databranchutils.DataBranchFencingPoolFor(cnSvc.ID())

		sysDB := dbrOpenDB(t, dbrSysDSN(cnSvc.SQLAddress()))
		defer sysDB.Close()

		// The cell premise, made explicit: the run's owners are the only
		// owners alive in this cluster.
		require.Equal(t, "0", dbrSQLStr(t, ctx, sysDB,
			"select cast(count(*) as char) from mo_catalog.mo_pitr"),
			"fresh cluster: no pitr owners")
		require.Equal(t, "0", dbrSQLStr(t, ctx, sysDB,
			"select cast(count(*) as char) from mo_catalog.mo_snapshots"),
			"fresh cluster: no snapshot owners")

		// The enumerated three accounts; ids are issued increasingly,
		// so the classification's sorted order is [acc1 acc2 acc3] and
		// acc2 is the seam's named A2.
		names := []string{"genr_acc1", "genr_acc2", "genr_acc3"}
		for _, name := range names {
			dbrSQL(t, ctx, sysDB,
				fmt.Sprintf("create account %s admin_name='admin' identified by '111'", name))
		}
		ids := make([]uint32, 3)
		accDBs := make([]*sql.DB, 3)
		for i, name := range names {
			s := dbrSQLStr(t, ctx, sysDB,
				fmt.Sprintf("select cast(account_id as char) from mo_catalog.mo_account where account_name = '%s'", name))
			id, perr := strconv.ParseUint(s, 10, 32)
			require.NoError(t, perr)
			ids[i] = uint32(id)
			accDBs[i] = dbrOpenDB(t, dbrTenantDSN(cnSvc.SQLAddress(), name))
			defer accDBs[i].Close()
		}
		require.Less(t, ids[0], ids[1], "sorted classification order follows id order")
		require.Less(t, ids[1], ids[2], "sorted classification order follows id order")

		// Ordered witnesses of the certified walk. For these CLUSTER-level
		// owners, classifyRemovedDataBranchOwnerAccounts' target set is
		// the caller plus every mo_account id, walked in ascending id
		// order — so the sys account's certified step (id 0 by
		// bootstrap) LEADS the witness, and the three harness accounts
		// follow in their id order. The seam's per-account guard sits at
		// the head of a step, so the armed witness sees exactly the
		// steps that CERTIFIED before the guard fired on names[1]:
		// [sys, A1].
		expectedAll := append([]uint32{0}, ids...)
		expectedArmed := []uint32{0, ids[0]}

		dbName := "branch_" + tag

		// RED twin / landing-zone control: seam NEVER armed on this
		// owner; the identical drop reaps all three accounts on the
		// first attempt. The owner is created BEFORE the shapes: with
		// coverage decided structurally, the cluster-scoped owner
		// grounds every subsequent branch regardless of order, and this
		// order additionally guarantees the retention writer runs under
		// a live owner (authoritative grounding).
		redOwner := tag + "_red"
		dbrSQL(t, ctx, sysDB, flavor.createSQL(redOwner))
		redTids := make([]string, 3)
		for i, accDB := range accDBs {
			redTids[i] = dbrCellRetainedShape(t, ctx, accDB, dbName,
				fmt.Sprintf("%s_r%d", tag, i))
			require.NotEqual(t, "0|x|x||0|x|x",
				dbrCellLinkFingerprint(t, ctx, accDB, redTids[i]),
				"red shape retained nonzero links")
		}
		observer.reset()
		dbrSQL(t, ctx, sysDB, flavor.dropSQL(redOwner))
		require.Equal(t, expectedAll, observer.snapshot(),
			"seam-silent: every account's certified step completed in one attempt, sys leading")
		for i, accDB := range accDBs {
			require.Equal(t, "0|x|x||0|x|x",
				dbrCellLinkFingerprint(t, ctx, accDB, redTids[i]),
				"account fully reaped on the first attempt")
		}
		require.True(t, strings.HasPrefix(dbrSQLStr(t, ctx, sysDB, flavor.rowSQL(redOwner)), "0|"),
			"red owner row dropped on the first attempt")

		// Armed attempt: the seam fails A2's step strictly after A1's.
		// Owner created before the shapes, same as the RED control.
		greenOwner := tag + "_grn"
		dbrSQL(t, ctx, sysDB, flavor.createSQL(greenOwner))
		greenTids := make([]string, 3)
		preLinks := make([]string, 3)
		for i, accDB := range accDBs {
			greenTids[i] = dbrCellRetainedShape(t, ctx, accDB, dbName,
				fmt.Sprintf("%s_g%d", tag, i))
			preLinks[i] = dbrCellLinkFingerprint(t, ctx, accDB, greenTids[i])
			require.NotEqual(t, "0|x|x||0|x|x", preLinks[i],
				"green shape retained nonzero links")
		}
		preOwnerRow := dbrSQLStr(t, ctx, sysDB, flavor.rowSQL(greenOwner))
		require.True(t, strings.HasPrefix(preOwnerRow, "1|"), "owner row live pre-drop")

		spawnedBefore := pool.Dump().Spawned
		restoreSeam := frontend.FailDataBranchOwnerRemovalReapAccountForTest(names[1])
		observer.reset()
		_, dropErr := sysDB.ExecContext(ctx, flavor.dropSQL(greenOwner))
		restoreSeam()
		require.Error(t, dropErr, "armed seam fails the drop at A2")
		require.Contains(t, dropErr.Error(),
			fmt.Sprintf("data branch owner-removal reap seam: account %s step failed", names[1]),
			"the typed seam error propagates")

		// Fail-atomic: the owner row (uuid instance included) and EVERY
		// account's links/ledger byte-compare EQUAL to the pre-drop state.
		require.Equal(t, preOwnerRow, dbrSQLStr(t, ctx, sysDB, flavor.rowSQL(greenOwner)),
			"owner row byte-equal to the pre-drop snapshot after the abort")
		for i, accDB := range accDBs {
			require.Equal(t, preLinks[i], dbrCellLinkFingerprint(t, ctx, accDB, greenTids[i]),
				"account links/ledger byte-equal to pre-drop")
		}
		// Ordered witness: the sys step and A1's certified step ran
		// (fencing rounds observed on the real certified lane); A2's,
		// A3's and every later step never began.
		require.Equal(t, expectedArmed, observer.snapshot(),
			"exactly sys's and A1's certified reaps completed before the seam fired at A2")
		require.Greater(t, pool.Dump().Spawned, spawnedBefore,
			"A1's certified fencing rounds executed through the certified lane")

		// Identical retry with the seam cleared: all accounts reap to zero.
		observer.reset()
		dbrSQL(t, ctx, sysDB, flavor.dropSQL(greenOwner))
		require.Equal(t, expectedAll, observer.snapshot(),
			"retry reaps every account in sorted order, sys leading")
		for i, accDB := range accDBs {
			require.Equal(t, "0|x|x||0|x|x",
				dbrCellLinkFingerprint(t, ctx, accDB, greenTids[i]),
				"account reaped to zero on the retry")
		}
		require.True(t, strings.HasPrefix(dbrSQLStr(t, ctx, sysDB, flavor.rowSQL(greenOwner)), "0|"),
			"owner row dropped on the cleared retry")
	}

	t.Run("cell-r-pitr-owner", func(t *testing.T) { run(t, "genrp", dbrPitrFlavor()) })
	t.Run("cell-s-snapshot-owner", func(t *testing.T) { run(t, "genrs", dbrSnapshotFlavor()) })
}

// dbrRetirementCycle drives one full retained-shape + owner + owner-drop
// cycle on the given CN session (sys account) and asserts the certified
// reap completed: rounds ran on the session's generation, links reaped to
// zero, and the pool settles to no charges and no retained entries.
func dbrRetirementCycle(
	t *testing.T, ctx context.Context, sysDB *sql.DB, tag, dbName string,
	pool *databranchutils.DataBranchFencingPool,
) {
	t.Helper()
	flavor := dbrAccountSnapshotFlavor()
	ownerName := tag + "_snap"
	dbrSQLRetry(t, ctx, sysDB, flavor.createSQL(ownerName))
	tid := dbrCellRetainedShape(t, ctx, sysDB, dbName, tag)
	spawnedBefore := pool.Dump().Spawned
	dbrSQLRetry(t, ctx, sysDB, flavor.dropSQL(ownerName))
	require.Equal(t, "0|x|x||0|x|x",
		dbrCellLinkFingerprint(t, ctx, sysDB, tid),
		"retirement reaped the address to zero")
	require.Greater(t, pool.Dump().Spawned, spawnedBefore,
		"the generation admitted, ran and completed certified rounds")
	dbrWaitCond(t, 10*time.Second, "generation settles account-free", func() bool {
		dump := pool.Dump()
		return dump.Charges == 0 && len(dump.Retained) == 0 && !dump.Sealed
	})
}

// TestDataBranchFencingGenerationCellP is (H.10)(p): the generation cell
// (D186(B.3), C.24). CN B's retirements run through B's certified lane
// while CN A proceeds through its FULL close (recorded close-step and
// final-barrier chain oracles); B's rounds admit, complete and account
// throughout and B's pool dump shows no A-generation residue. A newly
// constructed CN C receives a fresh, independent, open generation: its
// first retirement admits, completes and drains at C's own close. The
// stock arm asserts single-instance multiplicity of the "" generation.
func TestDataBranchFencingGenerationCellP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	c, err := NewCluster(ctx, t,
		DefaultOptions().WithRootDataDir(t.TempDir()).WithLogLevel(zap.WarnLevel).WithCNServiceNum(2))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	require.NoError(t, c.Start())
	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 1)
	cnA, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	cnB, err := c.GetCNServiceIndexed(1)
	require.NoError(t, err)
	uuidA, uuidB := cnA.ID(), cnB.ID()
	require.NotEqual(t, uuidA, uuidB)
	sysA := dbrOpenDB(t, dbrSysDSN(cnA.SQLAddress()))
	defer sysA.Close()
	sysB := dbrOpenDB(t, dbrSysDSN(cnB.SQLAddress()))
	defer sysB.Close()

	// STOCK arm: the "" generation resolves to the sole stock instance
	// (single-instance multiplicity is a no-op for the stock schedules).
	require.Same(t, databranchutils.DataBranchFencingGlobalPool(),
		databranchutils.DataBranchFencingPoolFor(""))

	// A runs one retirement so its generation holds accounting history.
	poolA := databranchutils.DataBranchFencingPoolFor(uuidA)
	dbrRetirementCycle(t, ctx, sysA, "genpa", "branch_genpa", poolA)
	poolB := databranchutils.DataBranchFencingPoolFor(uuidB)
	require.NotSame(t, poolA, poolB, "one pool per CN-service generation")

	// B admits, completes and accounts BEFORE A's close.
	dbrRetirementCycle(t, ctx, sysB, "genpb1", "branch_genpb", poolB)

	// CN A through its FULL CLOSE (close step + final barrier inside the
	// recorded stopRPCs chain slots) while B keeps retiring.
	closeDone := make(chan error, 1)
	go func() { closeDone <- c.CloseCNService(uuidA) }()
	for _, tag := range []string{"genpb2", "genpb3"} {
		dbrRetirementCycle(t, ctx, sysB, tag, "branch_genpb", poolB)
		// B's own generation is addressed throughout A's close.
		require.Same(t, poolB, databranchutils.DataBranchFencingPoolFor(uuidB),
			"B's generation untouched by A's close chain")
	}
	require.NoError(t, <-closeDone,
		"A's recorded close-step and final-barrier oracles completed")

	// B after A's close: still its own unsealed generation with zero
	// residue from A (no retained entries at all).
	dbrRetirementCycle(t, ctx, sysB, "genpb4", "branch_genpb", poolB)
	require.Same(t, poolB, databranchutils.DataBranchFencingPoolFor(uuidB))

	// A's drained generation left the registry: a later same-identity
	// construction is FRESH, INDEPENDENT, OPEN - no inherited seal or
	// drained charge (C.24).
	poolAAfter := databranchutils.DataBranchFencingPoolFor(uuidA)
	require.NotSame(t, poolA, poolAAfter,
		"A's verified-drained generation was removed at its barrier")
	require.Zero(t, poolAAfter.Dump().Spawned, "fresh generation has no round history")
	require.False(t, poolAAfter.Dump().Sealed, "fresh generation is born unsealed")

	// CN C, constructed AFTER A's close: fresh open admission; its first
	// retirement admits, completes, and its generation drains at C's own
	// close.
	require.NoError(t, c.StartCNServices(1))
	cnC, err := c.GetCNServiceIndexed(2)
	require.NoError(t, err)
	uuidC := cnC.ID()
	require.NotEqual(t, uuidA, uuidC, "C is a new service identity, not A restarted")
	require.NotEqual(t, uuidB, uuidC)
	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 2)
	sysC := dbrOpenDB(t, dbrSysDSN(cnC.SQLAddress()))
	defer sysC.Close()
	poolC := databranchutils.DataBranchFencingPoolFor(uuidC)
	dbrRetirementCycle(t, ctx, sysC, "genpc1", "branch_genpc", poolC)
	require.NoError(t, c.CloseCNService(uuidC),
		"C's own close drains C's generation")
	poolCAfter := databranchutils.DataBranchFencingPoolFor(uuidC)
	require.NotSame(t, poolC, poolCAfter,
		"C's verified-drained generation was removed at its own barrier")

	// B outlives both closes, sealed by neither.
	require.Same(t, poolB, databranchutils.DataBranchFencingPoolFor(uuidB))
	require.False(t, poolB.Dump().Sealed, "B's generation was never sealed by another close")
}
