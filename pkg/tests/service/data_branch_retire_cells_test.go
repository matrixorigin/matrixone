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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	multiupdate "github.com/matrixorigin/matrixone/pkg/sql/colexec/multi_update"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// The r82-r92 live cell family of the pk-move update-provenance design
// (D206/D208/D210/D211/D212/D213): the expected-conflict mark contract,
// the fenced retire contention outcomes, and the skip-path conflict cells.
// Every statement runs through REAL user sessions on the target CN's SQL
// port of an in-process multi-service cluster with the RC-invalid checker
// ARMED (enable-check-rc-invalid-error), and every defect flag rides the
// design's D155-C harness-only installers. The unmarked + checker-armed
// dispositions are process-Fatal by construction and run as subprocess
// children whose death the parent asserts.
// ---------------------------------------------------------------------------

// dbrArmChecker arms the RC-invalid checker on the given CN's service
// runtime and records the ARMED witness (D206(C.1): runtime/types.go:53-54).
func dbrArmChecker(t *testing.T, uuid string) {
	t.Helper()
	moruntime.ServiceRuntime(uuid).SetGlobalVariables(
		moruntime.EnableCheckInvalidRCErrors, true)
	v, ok := moruntime.ServiceRuntime(uuid).GetGlobalVariables(
		moruntime.EnableCheckInvalidRCErrors)
	require.True(t, ok && v.(bool),
		"checker ARMED witness: enable-check-rc-invalid-error resolves true for the whole cell")
}

// dbrErrClassLocked classifies a raw driver error for the conflict cells:
// ww for the ErrTxnWWConflict surface, dup for the duplicate surface,
// lockwait for the bounded lock-wait deadline, bind for the bind-change
// bounded class, or "" for anything else.
func dbrErrClassLocked(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "w-w conflict") ||
		strings.Contains(msg, "ww conflict") ||
		strings.Contains(msg, "20619"):
		return "ww"
	case strings.Contains(msg, "duplicate entry"):
		return "dup"
	case strings.Contains(msg, "lock wait timeout"):
		return "lockwait"
	case strings.Contains(msg, "lock table bind changed"):
		return "bind"
	case strings.Contains(msg, "deadlock"):
		return "deadlock"
	default:
		return ""
	}
}

// dbrTableRows is one user table's byte-exact row fingerprint.
func dbrTableRows(t *testing.T, ctx context.Context, db *sql.DB, dbName, table string) string {
	t.Helper()
	return dbrSQLStr(t, ctx, db, fmt.Sprintf(
		"select concat_ws('|', cast(count(*) as char), coalesce(group_concat(concat_ws('~', cast(id as char), payload) order by id), 'none')) from %s.%s",
		dbName, table))
}

// dbrLedgerRowFingerprint is one address's retention identity row content
// fingerprint (the seal is a byte-identity self-update, so the fingerprint
// is stable across sealed versions; names/logical id DO move on refresh).
func dbrLedgerRowFingerprint(
	t *testing.T, ctx context.Context, db *sql.DB, tid string,
) string {
	t.Helper()
	return dbrSQLStrRetry(t, ctx, db, fmt.Sprintf(
		"select concat_ws('|', cast(count(*) as char), coalesce(group_concat(concat_ws('~', cast(row_kind as char), cast(table_id as char), database_name, table_name, cast(obj_id as char), cast(created_ts as char), cast(logical_id as char))), 'none')) from mo_catalog.mo_data_branch_link_retention where table_id = %s", tid))
}

// dbrRelID resolves a table's physical id under the given session.
func dbrRelID(t *testing.T, ctx context.Context, db *sql.DB, dbName, table string) string {
	t.Helper()
	return dbrSQLStr(t, ctx, db, fmt.Sprintf(
		"select cast(rel_id as char) from mo_catalog.mo_tables where relname = '%s' and reldatabase = '%s'",
		table, dbName))
}

// dbrPlanTruth captures the drop-flow's durable identity for one table: its
// physical id and the raw rel_logical_id the plan's TableDef carries
// (ddl.go:5778's droppedLogicalID), so the KEEP fixtures assert the funnel's
// inserted/enriched identity byte-for-byte against production truth.
func dbrPlanTruth(t *testing.T, ctx context.Context, db *sql.DB, dbName, table string) (tid, lgid string) {
	t.Helper()
	tid = dbrRelID(t, ctx, db, dbName, table)
	lgid = dbrSQLStr(t, ctx, db, fmt.Sprintf(
		"select cast(rel_logical_id as char) from mo_catalog.mo_tables where relname = '%s' and reldatabase = '%s'",
		table, dbName))
	return tid, lgid
}

// dbrLedgerKindRow returns the retention identity rows of ONE row_kind at
// the given table id ("none" when absent). The KEEP fixtures co-seed a
// kind-1 decoy, so kind-filtered queries are the only honest counts: a
// table-wide fingerprint would fold the decoy into the sums.
func dbrLedgerKindRow(
	t *testing.T, ctx context.Context, db *sql.DB, tid string, kind int,
) string {
	t.Helper()
	return dbrSQLStrRetry(t, ctx, db, fmt.Sprintf(
		"select concat_ws('|', cast(count(*) as char), coalesce(group_concat(concat_ws('~', cast(row_kind as char), cast(table_id as char), database_name, table_name, cast(obj_id as char), cast(created_ts as char), cast(logical_id as char))), 'none')) from mo_catalog.mo_data_branch_link_retention where table_id = %s and row_kind = %d",
		tid, kind))
}

// dbrSeedViaReapSpy stages catalog fixture rows through the reap driver's
// own background executor: ordinary user SQL cannot write the retention
// catalog (privilege-blocked; PROBE-D, /tmp/e114_coldprobe2.log), so the
// D217(B.3)(ii) incomplete-row seed and the optional kind-1 decoy ride the
// EXISTING D155-C resolver spy (D210) inside a throwaway cluster-owner
// removal, whose bh transaction commits every seed atomically. User-statement
// privilege, production admission and every funnel predicate are untouched;
// only the fixture seed transport differs from a user statement.
func dbrSeedViaReapSpy(
	t *testing.T, ctx context.Context, sys *sql.DB, seedSQLs []string,
) {
	t.Helper()
	flavor := dbrSnapshotFlavor()
	name := fmt.Sprintf("dbr_seed_%d", time.Now().UnixNano())
	dbrSQL(t, ctx, sys, flavor.createSQL(name))
	var seedMu sync.Mutex
	seedFired := false
	var seedErr error
	restoreSpy := frontend.SetReapTxnOperatorResolverForTest(
		func(bh frontend.BackgroundExec,
			next func(frontend.BackgroundExec) client.TxnOperator,
		) client.TxnOperator {
			// The deferred restore below MUST survive a require.FailNow in
			// the following drop: an unrestored spy stays chained as the
			// next test's prev resolver and poisons every later drop with a
			// completed test's *testing.T (observed as cascade FAILs).
			// The callback runs in the CN's serving goroutine
			// (doDropSnapshot -> reap -> resolver), never in the test
			// goroutine, so failures are recorded, not t.Fatal'd. The bh
			// executor resolves the account from the ctx: every production
			// write in data_branch_link_retention.go uses
			// defines.AttachAccountId, and the fixture seeds target the
			// SYS account's retention catalog.
			seedCtx := defines.AttachAccountId(
				context.Background(), catalog.System_Account)
			seedMu.Lock()
			seedFired = true
			for _, stmt := range seedSQLs {
				bh.ClearExecResultSet()
				if err := bh.Exec(seedCtx, stmt); err != nil {
					seedErr = err
					break
				}
			}
			seedMu.Unlock()
			return next(bh)
		})
	defer restoreSpy()
	// The drop runs on this goroutine; the bh transaction commits the
	// seeds with the owner removal (atomic: both or neither). The
	// recorded seed failure below is the callback's error channel.
	dbrSQL(t, ctx, sys, flavor.dropSQL(name))
	seedMu.Lock()
	firedNow, recorded := seedFired, seedErr
	seedMu.Unlock()
	require.True(t, firedNow,
		"the removed cluster owner's reap reached the resolver, committing the seeded fixture rows")
	require.NoError(t, recorded,
		"reap-spy catalog seed failed in the bh transaction")
}

// dbrAdmissionRefusalControl executes D215(B.4)/D216(C)'s admission-witness
// control shape from user SQL: an RR transaction plus the given statement
// must be refused with wantText, with rollback cleanup.
func dbrAdmissionRefusalControl(
	t *testing.T, ctx context.Context, db *sql.DB, stmt, wantText string,
) {
	t.Helper()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.ExecContext(ctx,
		"set session transaction_isolation='REPEATABLE-READ'")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "begin")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, stmt)
	require.Error(t, err,
		"the admission-witness control statement must be refused: %s", stmt)
	require.Contains(t, err.Error(), wantText,
		"the admission refusal carries the pinned text: %v", err)
	_, _ = conn.ExecContext(ctx, "rollback")
}

// dbrBoundedClass reports whether cls belongs to the D213-class bounded
// lattice (deadlock victim / lock-wait deadline / bind change).
func dbrBoundedClass(cls string) bool {
	switch cls {
	case "lockwait", "bind", "deadlock":
		return true
	}
	return false
}

// dbrNewRetireCluster boots a 1-CN cluster with the checker armed and
// returns the CN service and a session func factory.
func dbrNewRetireCluster(t *testing.T, ctx context.Context) (CNService, func() *sql.DB) {
	t.Helper()
	c, err := NewCluster(ctx, t,
		DefaultOptions().WithRootDataDir(t.TempDir()).WithLogLevel(zap.WarnLevel))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	require.NoError(t, c.Start())
	c.WaitCNStoreTaskServiceCreatedIndexed(ctx, 0)
	cn, err := c.GetCNServiceIndexed(0)
	require.NoError(t, err)
	dbrArmChecker(t, cn.ID())
	return cn, func() *sql.DB {
		db := dbrOpenDB(t, dbrSysDSN(cn.SQLAddress()))
		db.SetMaxOpenConns(4)
		return db
	}
}

// dbrPinnedConn returns one pinned session with the fixture database
// selected. A pooled *sql.DB silently hops sessions, and a session with
// no selected database refuses the cells' unqualified fixture names.
func dbrPinnedConn(t *testing.T, ctx context.Context, db *sql.DB, dbName string) *sql.Conn {
	t.Helper()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, fmt.Sprintf("use %s", dbName))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

// dbrConnSQL executes one must-succeed statement on a pinned session.
func dbrConnSQL(t *testing.T, ctx context.Context, conn *sql.Conn, query string) {
	t.Helper()
	if _, err := conn.ExecContext(ctx, query); err != nil {
		t.Fatalf("sql failed: %s: %v", query, err)
	}
}

// dbrRetireFixture builds one retained live-branch shape in dbName: parent
// and branch tables, seed rows, and ONE committed pk move, so the address
// carries visible links plus the (0,T) retention ledger row. It returns
// the branch table's physical id. All fixture statements run on a pinned
// session with the database selected; catalog fingerprints can be read
// from any session.
func dbrRetireFixture(
	t *testing.T, ctx context.Context, db *sql.DB, dbName, tag string,
) string {
	t.Helper()
	dbrSQL(t, ctx, db, fmt.Sprintf("drop database if exists %s", dbName))
	dbrSQL(t, ctx, db, fmt.Sprintf("create database %s", dbName))
	conn := dbrPinnedConn(t, ctx, db, dbName)
	dbrConnSQL(t, ctx, conn, fmt.Sprintf(
		"create table %s_t (id int primary key, payload varchar(32))", tag))
	dbrConnSQL(t, ctx, conn, fmt.Sprintf(
		"insert into %s_t values (2, 'two'), (4, 'four')", tag))
	dbrConnSQL(t, ctx, conn, fmt.Sprintf(
		"data branch create table %s_s from %s_t", tag, tag))
	// The committed genesis: one link row plus the (0,T) ledger row stand.
	dbrConnSQL(t, ctx, conn, fmt.Sprintf(
		"update %s_s set id = 3 where id = 2", tag))
	return dbrRelID(t, ctx, db, dbName, tag+"_s")
}

// ---------------------------------------------------------------------------
// D213(C.1) THE FENCED RETIRE CONTENTION-OUTCOME CELL (SUPERSEDES
// D212(C.1)): a retiring DROP reaching AcquireLedgerGrant while a producer
// seal's transaction concurrently HOLDS (0,T) WAITS (no refusal, no state
// change); on the seal's close the grant ACQUIRES and the drop commits with
// exact finals. ATOMICITY ARM: the wait's deadline variant returns the
// determinate bounded error with the retiring transaction rolled back
// atomically and a fresh re-drive committing. NO Fatal twin is claimed on
// this path.
// ---------------------------------------------------------------------------
func TestDataBranchFencedContentionOutcomeCell(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	_, open := dbrNewRetireCluster(t, ctx)
	sys := open()
	defer sys.Close()

	// setup takes the runtime (subtest) t: the fixture's pinned session
	// must close when the SUBTEST ends. Binding it to the outer t keeps
	// one of the four pool slots checked out across subtests and starves
	// every pooled fingerprint read in the next subtest.
	setup := func(t *testing.T, dbName, tag string) string {
		return dbrRetireFixture(t, ctx, sys, dbName, tag)
	}

	t.Run("wait-then-grant-then-commit", func(t *testing.T) {
		tid := setup(t, "branch_d213a", "d213a")

		// The producer seal's transaction holds (0,T)'s lock open.
		holder, err := sys.Conn(ctx)
		require.NoError(t, err)
		defer holder.Close()
		_, err = holder.ExecContext(ctx, "use branch_d213a")
		require.NoError(t, err)
		_, err = holder.ExecContext(ctx, "begin")
		require.NoError(t, err)
		_, err = holder.ExecContext(ctx, "update d213a_s set id = 5 where id = 4")
		require.NoError(t, err)
		preLinks := dbrCellLinkFingerprint(t, ctx, sys, tid)
		require.NotEqual(t, "0|x|x||0|x|x", preLinks, "the staged shape retained links")
		preLedger := dbrLedgerRowFingerprint(t, ctx, sys, tid)

		// The retiring drop must WAIT at the grant: no refusal, no state
		// change, for a witnessed window.
		dropDone := make(chan error, 1)
		dropper, err := sys.Conn(ctx)
		require.NoError(t, err)
		defer dropper.Close()
		go func() {
			_, derr := dropper.ExecContext(ctx, "drop table branch_d213a.d213a_s")
			dropDone <- derr
		}()
		select {
		case derr := <-dropDone:
			t.Fatalf("the fenced grant did not wait: drop returned early with %v", derr)
		case <-time.After(6 * time.Second):
		}
		require.Equal(t, preLinks, dbrCellLinkFingerprint(t, ctx, sys, tid),
			"witnessed wait: no link state change while the grant waits")
		require.Equal(t, preLedger, dbrLedgerRowFingerprint(t, ctx, sys, tid),
			"witnessed wait: no ledger state change while the grant waits")
		require.Equal(t, tid, dbrRelID(t, ctx, sys, "branch_d213a", "d213a_s"),
			"witnessed wait: the user table still stands")

		// The seal's close lets the grant acquire; the drop commits with
		// exact finals.
		_, err = holder.ExecContext(ctx, "commit")
		require.NoError(t, err)
		select {
		case derr := <-dropDone:
			require.NoError(t, derr, "wait-then-grant: the drop must commit")
		case <-time.After(90 * time.Second):
			t.Fatal("the drop did not finish after the holder's close")
		}
		require.Equal(t, "0|x|x||0|x|x", dbrCellLinkFingerprint(t, ctx, sys, tid),
			"exact finals: links and ledger row gone")
		require.True(t, strings.HasPrefix(
			dbrLedgerRowFingerprint(t, ctx, sys, tid), "0|"),
			"exact finals: the ledger row is gone")
	})

	t.Run("atomicity-arm-wait-deadline", func(t *testing.T) {
		tid := setup(t, "branch_d213b", "d213b")

		holder, err := sys.Conn(ctx)
		require.NoError(t, err)
		defer holder.Close()
		_, err = holder.ExecContext(ctx, "use branch_d213b")
		require.NoError(t, err)
		_, err = holder.ExecContext(ctx, "begin")
		require.NoError(t, err)
		_, err = holder.ExecContext(ctx, "update d213b_s set id = 5 where id = 4")
		require.NoError(t, err)
		preLinks := dbrCellLinkFingerprint(t, ctx, sys, tid)
		require.NotEqual(t, "0|x|x||0|x|x", preLinks)
		preLedger := dbrLedgerRowFingerprint(t, ctx, sys, tid)
		preRows := dbrTableRows(t, ctx, sys, "branch_d213b", "d213b_t")

		// The wait's deadline variant: a bounded determinate error, the
		// retiring transaction rolled back atomically, the process alive.
		dropDone := make(chan error, 1)
		dropper, err := sys.Conn(ctx)
		require.NoError(t, err)
		defer dropper.Close()
		_, err = dropper.ExecContext(ctx, "set session lock_wait_timeout = 2")
		require.NoError(t, err)
		go func() {
			_, derr := dropper.ExecContext(ctx, "drop table branch_d213b.d213b_s")
			dropDone <- derr
		}()
		select {
		case derr := <-dropDone:
			require.Error(t, derr, "the contended wait must fail at its deadline")
			require.Equal(t, "lockwait", dbrErrClassLocked(derr),
				"the bounded wait-deadline class surfaces as the determinate error: %v", derr)
		case <-time.After(90 * time.Second):
			t.Fatal("the deadline-bounded wait never returned")
		}
		require.Equal(t, preLinks, dbrCellLinkFingerprint(t, ctx, sys, tid),
			"atomic rollback: links byte-exact")
		require.Equal(t, preLedger, dbrLedgerRowFingerprint(t, ctx, sys, tid),
			"atomic rollback: ledger row byte-exact")
		require.Equal(t, preRows, dbrTableRows(t, ctx, sys, "branch_d213b", "d213b_t"),
			"atomic rollback: user table byte-exact")

		// The fresh re-drive after the holder's close commits with exact
		// finals.
		_, err = holder.ExecContext(ctx, "commit")
		require.NoError(t, err)
		dbrSQL(t, ctx, sys, "drop table branch_d213b.d213b_s")
		require.Equal(t, "0|x|x||0|x|x", dbrCellLinkFingerprint(t, ctx, sys, tid),
			"exact finals after the fresh re-drive")
	})
}

// ---------------------------------------------------------------------------
// D212(C.2)/(C.3) THE SKIP-PATH RETIRE CELLS under D215's tier (iii): the
// retiring DROP rides ONLY the admitted pessimistic RC shape (lifecycle
// admission, txn.go:572-579/:837-848), so NO checker-class conflict is
// schedulable through admission. With the guard/degrade fixture (the D155-C
// harness installer) taking done=false, each arm carries (a) a
// DETERMINISTIC producer-first CLEAN-COMMIT (the genesis committed before
// the drop executes; the cells3 observation re-classified as the
// availability witness) with exact finals, and (b) a concurrent-race
// BOUNDED arm (the producer seal's transaction holds (0,T) open; the
// retiring DELETE waits and returns the D213-class bounded error with
// ATOMIC rollback and a FRESH re-drive committing with exact finals), plus
// the (B.4)/D216(C) admission-witness control (DropTable IS whitelisted,
// stmt_kind.go:334-337). The names-only legacy arm reruns both schedules.
// The checker stays ARMED; NO drop-carried Fatal twin (D215(B.3)).
// ---------------------------------------------------------------------------
func TestDataBranchRetireSkipPathConflictCell(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	_, open := dbrNewRetireCluster(t, ctx)
	sys := open()
	defer sys.Close()

	forcePath := func(legacy bool) func() {
		restoreDegrade := compile.ForceDataBranchRetireCSDegradeForTest(true)
		if legacy {
			restoreLegacy := compile.ForcePkMoveRetentionLegacyDeleteForTest(true)
			return func() { restoreLegacy(); restoreDegrade() }
		}
		return restoreDegrade
	}

	// (a) DETERMINISTIC PRODUCER-FIRST CLEAN-COMMIT: the committed genesis
	// precedes the drop; the admitted drop commits with exact finals.
	runCleanCommit := func(t *testing.T, dbName, tag string, legacy bool) {
		tid := dbrRetireFixture(t, ctx, sys, dbName, tag)
		require.NotEqual(t, "0|x|x||0|x|x", dbrCellLinkFingerprint(t, ctx, sys, tid),
			"the committed genesis staged the retained shape")
		defer forcePath(legacy)()

		// The availability witness: CLEAN COMMIT with the checker ARMED.
		dbrSQL(t, ctx, sys, fmt.Sprintf("drop table %s.%s_s", dbName, tag))
		require.Equal(t, "0|x|x||0|x|x", dbrCellLinkFingerprint(t, ctx, sys, tid),
			"exact finals: links and ledger row gone")
		require.True(t, strings.HasPrefix(
			dbrLedgerRowFingerprint(t, ctx, sys, tid), "0|"),
			"exact finals: the ledger row is gone")
	}

	// (b) CONCURRENT RACE: the producer seal's transaction HOLDS (0,T)'s
	// row lock open; the retiring skip-path DELETE waits on it and returns
	// the bounded lock-wait deadline error (the observed shape in the
	// D213-class lattice) with ATOMIC rollback; the seal's close plus a
	// fresh re-drive commits with exact finals.
	runBoundedRace := func(t *testing.T, dbName, tag string, legacy bool) {
		tid := dbrRetireFixture(t, ctx, sys, dbName, tag)
		preLinks := dbrCellLinkFingerprint(t, ctx, sys, tid)
		require.NotEqual(t, "0|x|x||0|x|x", preLinks)
		preLedger := dbrLedgerRowFingerprint(t, ctx, sys, tid)
		preRows := dbrTableRows(t, ctx, sys, dbName, tag+"_t")
		defer forcePath(legacy)()

		// The producer stages the seal and HOLDS (0,T)'s lock open.
		holder := dbrPinnedConn(t, ctx, sys, dbName)
		dbrConnSQL(t, ctx, holder, "begin")
		dbrConnSQL(t, ctx, holder, fmt.Sprintf(
			"update %s_s set id = 5 where id = 4", tag))

		dropper, err := sys.Conn(ctx)
		require.NoError(t, err)
		defer dropper.Close()
		_, err = dropper.ExecContext(ctx, "set session lock_wait_timeout = 2")
		require.NoError(t, err)
		dropDone := make(chan error, 1)
		go func() {
			_, derr := dropper.ExecContext(ctx,
				fmt.Sprintf("drop table %s.%s_s", dbName, tag))
			dropDone <- derr
		}()
		select {
		case derr := <-dropDone:
			require.Error(t, derr,
				"the contended skip-path drop must fail at the bounded wait")
			require.True(t, dbrBoundedClass(dbrErrClassLocked(derr)),
				"the D213-class bounded error surfaces as the determinate error: %v", derr)
		case <-time.After(90 * time.Second):
			t.Fatal("the deadline-bounded wait never returned")
		}
		// ATOMIC rollback: links, ledger row, user table byte-exact to the
		// sealed baseline; the held seal stays uncommitted throughout.
		require.Equal(t, preLinks, dbrCellLinkFingerprint(t, ctx, sys, tid),
			"atomic rollback: links byte-exact")
		require.Equal(t, preLedger, dbrLedgerRowFingerprint(t, ctx, sys, tid),
			"atomic rollback: ledger row byte-exact")
		require.Equal(t, preRows, dbrTableRows(t, ctx, sys, dbName, tag+"_t"),
			"atomic rollback: user table byte-exact")
		require.Equal(t, tid, dbrRelID(t, ctx, sys, dbName, tag+"_s"),
			"atomic rollback: the user table stands")

		// The seal's close lets the fresh re-drive commit with exact
		// finals.
		dbrConnSQL(t, ctx, holder, "commit")
		dbrSQL(t, ctx, sys, fmt.Sprintf("drop table %s.%s_s", dbName, tag))
		require.Equal(t, "0|x|x||0|x|x", dbrCellLinkFingerprint(t, ctx, sys, tid),
			"exact finals after the fresh re-drive")
	}

	t.Run("wide-shape-plain-delete/clean-commit", func(t *testing.T) {
		runCleanCommit(t, "branch_d212wa", "d212wa", false)
	})
	t.Run("wide-shape-plain-delete/bounded-race", func(t *testing.T) {
		runBoundedRace(t, "branch_d212wb", "d212wb", false)
	})
	t.Run("legacy-names-only-arm/clean-commit", func(t *testing.T) {
		runCleanCommit(t, "branch_d212la", "d212la", true)
	})
	t.Run("legacy-names-only-arm/bounded-race", func(t *testing.T) {
		runBoundedRace(t, "branch_d212lb", "d212lb", true)
	})

	// D215(B.4)/D216(C) ADMISSION-WITNESS CONTROL: DropTable IS whitelisted
	// (stmt_kind.go:334-337), so the RR transaction's drop reaches
	// TxnHandler.Create's lifecycle refusal.
	t.Run("admission-witness-control", func(t *testing.T) {
		tid := dbrRetireFixture(t, ctx, sys, "branch_d212c", "d212c")
		dbrAdmissionRefusalControl(t, ctx, sys,
			"drop table branch_d212c.d212c_s",
			"object lifecycle statements require an existing pessimistic RC transaction")
		// The refused transaction changed nothing; the admitted retry
		// commits with exact finals.
		require.NotEqual(t, "0|x|x||0|x|x", dbrCellLinkFingerprint(t, ctx, sys, tid),
			"the refusal left every surface untouched")
		defer forcePath(false)()
		dbrSQL(t, ctx, sys, "drop table branch_d212c.d212c_s")
		require.Equal(t, "0|x|x||0|x|x", dbrCellLinkFingerprint(t, ctx, sys, tid),
			"exact finals after the admitted retry")
	})
}

// ---------------------------------------------------------------------------
// D211(C.1) THE KEEP-ONLY DROP CELL under D215's tier (iii), D216(B.3)'s
// THREE fixture states and D217(B)'s real-retention namespace: the admitted
// KEEP-verdict DROP COMMITS CLEAN in the deterministic producer-first
// ordering, and every fixture/probe/assertion identifies retained rows by
// (row_kind=0, table_id) - the real rows the drop-lifecycle funnels and
// genesis admission own (pkg/catalog/types.go:418-431), with the funnel's
// kind-0-scoped probe (:108-115), enrichment UPDATE (:202-207) and INSERT
// (:259-271); a kind-1 decoy is co-seeded ONLY as the disjointness witness
// (byte-untouched, never counted). ATOMICITY/RACE ARM (b): the producer
// seal's transaction holds (0,T) open; the KEEP write waits and returns the
// bounded error with ATOMIC rollback, and the fresh re-drive's finals adopt
// branch (iii) (the funnel's fresh RC probe observes the sealed row). Plus
// the :572-579 admission-witness control (DropTable IS whitelisted). The
// checker stays ARMED; NO Fatal twin (D215(B.3)). The fixtures' catalog
// seeds ride the EXISTING reap resolver spy (bh.Exec): user SQL is
// privilege-blocked from this catalog (PROBE-D, /tmp/e114_coldprobe2.log);
// the funnel's branch SQL shapes and the :185-199 no-op seam stay UT-pinned
// in pkg/sql/compile (D216(C)'s funnel-branch pins).
// ---------------------------------------------------------------------------
func TestDataBranchKeepOnlyDropConflictCell(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Minute)
	defer cancel()
	_, open := dbrNewRetireCluster(t, ctx)
	sys := open()
	defer sys.Close()
	flavor := dbrAccountSnapshotFlavor()

	// setup builds one unbranched covered table with no pk-move history.
	setupUnbranched := func(t *testing.T, dbName, table string) (tid, lgid string) {
		dbrSQL(t, ctx, sys, fmt.Sprintf("drop database if exists %s", dbName))
		dbrSQL(t, ctx, sys, fmt.Sprintf("create database %s", dbName))
		boot := dbrPinnedConn(t, ctx, sys, dbName)
		dbrConnSQL(t, ctx, boot, fmt.Sprintf(
			"create table %s (id int primary key, payload varchar(32))", table))
		dbrConnSQL(t, ctx, boot, fmt.Sprintf("insert into %s values (2, 'two')", table))
		return dbrPlanTruth(t, ctx, sys, dbName, table)
	}
	// setupMoved builds one branch table with one committed genesis move.
	setupMoved := func(t *testing.T, dbName, tag string) (tid string) {
		dbrSQL(t, ctx, sys, fmt.Sprintf("drop database if exists %s", dbName))
		dbrSQL(t, ctx, sys, fmt.Sprintf("create database %s", dbName))
		boot := dbrPinnedConn(t, ctx, sys, dbName)
		dbrConnSQL(t, ctx, boot, fmt.Sprintf(
			"create table %s_t (id int primary key, payload varchar(32))", tag))
		dbrConnSQL(t, ctx, boot, fmt.Sprintf("insert into %s_t values (2, 'two')", tag))
		dbrConnSQL(t, ctx, boot, fmt.Sprintf(
			"data branch create table %s_s from %s_t", tag, tag))
		dbrConnSQL(t, ctx, boot, fmt.Sprintf(
			"update %s_s set id = 3 where id = 2", tag))
		return dbrRelID(t, ctx, sys, dbName, tag+"_s")
	}
	retentionInsert := func(kind int, tid, dbName, tblName string, createdTS int64, logical string) string {
		return fmt.Sprintf(
			"insert into mo_catalog.mo_data_branch_link_retention (row_kind, table_id, database_name, table_name, obj_id, created_ts, logical_id) values (%d, %s, '%s', '%s', %s, %d, %s)",
			kind, tid, dbName, tblName, tid, createdTS, logical)
	}

	// (i) MISSING-row INSERT fixture: the covered table has no retention row
	// at the drop; the funnel's INSERT carries the captured identity.
	t.Run("missing-row-insert", func(t *testing.T) {
		tid, lgid := setupUnbranched(t, "branch_d211i", "t_i")

		decoyTS := time.Now().UnixNano()
		dbrSeedViaReapSpy(t, ctx, sys, []string{
			retentionInsert(1, tid, "zzdecoydb", "zzdecoytbl", decoyTS, "0"),
		})

		ownerName := "d211i_cover"
		dbrSQL(t, ctx, sys, flavor.createSQL(ownerName))
		t.Cleanup(func() { dbrSQL(t, ctx, sys, flavor.dropSQL(ownerName)) })

		// Deterministic producer-first ordering: the admitted drop COMMITS
		// CLEAN and the funnel INSERTs the identity row (D216(B.3)(i)).
		dbrSQL(t, ctx, sys, "drop table branch_d211i.t_i")

		row0 := dbrLedgerKindRow(t, ctx, sys, tid, 0)
		require.True(t, strings.HasPrefix(row0, "1|"),
			"(i) ONE kind-0 identity row total: %s", row0)
		require.Contains(t, row0,
			fmt.Sprintf("0~%s~branch_d211i~t_i~%s~", tid, tid),
			"(i) the inserted row carries row_kind=0 and the exact re-probed identity: %s", row0)
		require.True(t, strings.HasSuffix(row0, "~"+lgid),
			"(i) the inserted row carries the captured durable logical id: %s", row0)
		require.NotContains(t, row0, "~~",
			"(i) created_ts is present (no empty segment): %s", row0)
		require.Equal(t, fmt.Sprintf("1|1~%s~zzdecoydb~zzdecoytbl~%s~%d~0", tid, tid, decoyTS),
			dbrLedgerKindRow(t, ctx, sys, tid, 1),
			"(i) the kind-1 disjointness decoy is byte-untouched and never counted")
		require.Equal(t, "0", dbrSQLStr(t, ctx, sys,
			"select cast(count(*) as char) from mo_catalog.mo_tables where relname = 't_i' and reldatabase = 'branch_d211i'"),
			"(i) the user table is gone")
	})

	// (ii) INCOMPLETE-row ENRICHMENT fixture: a seeded INCOMPLETE KIND-0 row
	// (blank names, 0 logical marker, fixed created_ts) is enriched by the
	// funnel's UPDATE while created_ts stays byte-identical. The enriching
	// drop is the SEED TRANSPORT's own throwaway owner removal: it covers
	// the fixture table (cluster snapshot created after it), so the reap's
	// KEEP refresh heals the incomplete row at seed time through the same
	// production enrichment UPDATE. created_ts byte-identity plus
	// kind/table stability is exactly the in-place UPDATE witness - a
	// delete+reinsert could not preserve the seeded created_ts. The
	// fixture table's own covered drop then exercises complete-row
	// stability: the healed row is byte-untouched by it.
	t.Run("incomplete-row-enrichment", func(t *testing.T) {
		tid, lgid := setupUnbranched(t, "branch_d211ii", "t_ii")

		seedTS := time.Now().UnixNano()
		dbrSeedViaReapSpy(t, ctx, sys, []string{
			retentionInsert(0, tid, "", "", seedTS, "0"),
			retentionInsert(1, tid, "zzdecoydb", "zzdecoytbl", seedTS+1, "0"),
		})
		healed := fmt.Sprintf("1|0~%s~branch_d211ii~t_ii~%s~%d~%s",
			tid, tid, seedTS, lgid)
		require.Equal(t, healed,
			dbrLedgerKindRow(t, ctx, sys, tid, 0),
			"(ii) ENRICHMENT at the seed transport's removal: names/logical id exact, created_ts byte-identical, kind/table stable")
		require.Equal(t, fmt.Sprintf("1|1~%s~zzdecoydb~zzdecoytbl~%s~%d~0", tid, tid, seedTS+1),
			dbrLedgerKindRow(t, ctx, sys, tid, 1),
			"(ii) the kind-1 decoy is byte-untouched by the kind-0-scoped refresh")

		ownerName := "d211ii_cover"
		dbrSQL(t, ctx, sys, flavor.createSQL(ownerName))
		t.Cleanup(func() { dbrSQL(t, ctx, sys, flavor.dropSQL(ownerName)) })

		dbrSQL(t, ctx, sys, "drop table branch_d211ii.t_ii")

		require.Equal(t, healed,
			dbrLedgerKindRow(t, ctx, sys, tid, 0),
			"(ii) the covered drop leaves the healed complete row byte-untouched")
		require.Equal(t, fmt.Sprintf("1|1~%s~zzdecoydb~zzdecoytbl~%s~%d~0", tid, tid, seedTS+1),
			dbrLedgerKindRow(t, ctx, sys, tid, 1),
			"(ii) the kind-1 decoy is byte-untouched and never counted")
		require.Equal(t, "0", dbrSQLStr(t, ctx, sys,
			"select cast(count(*) as char) from mo_catalog.mo_tables where relname = 't_ii' and reldatabase = 'branch_d211ii'"),
			"(ii) the user table is gone")
	})

	// (iii) EXISTING-complete-row NO-OP fixture: the producer-first sealed
	// row is complete at the funnel's fresh RC probe, so found=true with
	// empty setClauses yields the :198-199 early return - FULL byte equality
	// plus the code-shaped branch witness (UT-pinned, pkg/sql/compile).
	t.Run("existing-complete-row-no-op", func(t *testing.T) {
		tid := setupMoved(t, "branch_d211iii", "d211iii")
		decoyTS := time.Now().UnixNano()
		dbrSeedViaReapSpy(t, ctx, sys, []string{
			retentionInsert(1, tid, "zzdecoydb", "zzdecoytbl", decoyTS, "0"),
		})
		preLedger := dbrLedgerKindRow(t, ctx, sys, tid, 0)
		require.True(t, strings.HasPrefix(preLedger, "1|"),
			"(iii) the complete sealed row stands pre-drop: %s", preLedger)
		preLinks := dbrCellLinkFingerprint(t, ctx, sys, tid)

		ownerName := "d211iii_cover"
		dbrSQL(t, ctx, sys, flavor.createSQL(ownerName))
		t.Cleanup(func() { dbrSQL(t, ctx, sys, flavor.dropSQL(ownerName)) })

		dbrSQL(t, ctx, sys, "drop table branch_d211iii.d211iii_s")

		require.Equal(t, preLedger, dbrLedgerKindRow(t, ctx, sys, tid, 0),
			"(iii) NO-OP: FULL byte equality - the equal-value rewrite is unreachable "+
				"(the :185-199 seam); byte equality is reserved for the no-op and rollback arms")
		require.Equal(t, preLinks, dbrCellLinkFingerprint(t, ctx, sys, tid),
			"(iii) links KEPT byte-exact")
		require.Equal(t, fmt.Sprintf("1|1~%s~zzdecoydb~zzdecoytbl~%s~%d~0", tid, tid, decoyTS),
			dbrLedgerKindRow(t, ctx, sys, tid, 1),
			"(iii) the kind-1 decoy is byte-untouched and never counted")
		require.Equal(t, "0", dbrSQLStr(t, ctx, sys,
			"select cast(count(*) as char) from mo_catalog.mo_tables where relname = 'd211iii_s' and reldatabase = 'branch_d211iii'"),
			"(iii) the user table is gone")
	})

	// (b) THE CONCURRENT-RACE BOUNDED ARM: the producer seal's transaction
	// HOLDS (0,T) open across the KEEP drop's window; the retention write
	// waits and returns the D213-class bounded error with ATOMIC rollback;
	// the fresh re-drive commits with finals adopting branch (iii).
	t.Run("race-bounded-adaptive-finals", func(t *testing.T) {
		tid := setupMoved(t, "branch_d211r", "d211r")
		preLedger := dbrLedgerKindRow(t, ctx, sys, tid, 0)
		require.True(t, strings.HasPrefix(preLedger, "1|"))
		preLinks := dbrCellLinkFingerprint(t, ctx, sys, tid)

		ownerName := "d211r_cover"
		dbrSQL(t, ctx, sys, flavor.createSQL(ownerName))
		t.Cleanup(func() { dbrSQL(t, ctx, sys, flavor.dropSQL(ownerName)) })

		holder := dbrPinnedConn(t, ctx, sys, "branch_d211r")
		dbrConnSQL(t, ctx, holder, "begin")
		dbrConnSQL(t, ctx, holder, "update d211r_s set id = 5 where id = 4")

		dropper, err := sys.Conn(ctx)
		require.NoError(t, err)
		defer dropper.Close()
		_, err = dropper.ExecContext(ctx, "set session lock_wait_timeout = 2")
		require.NoError(t, err)
		dropDone := make(chan error, 1)
		go func() {
			_, derr := dropper.ExecContext(ctx, "drop table branch_d211r.d211r_s")
			dropDone <- derr
		}()
		select {
		case derr := <-dropDone:
			require.Error(t, derr, "the contended KEEP drop must fail at the bounded wait")
			require.True(t, dbrBoundedClass(dbrErrClassLocked(derr)),
				"the bounded error surfaces: %v", derr)
		case <-time.After(90 * time.Second):
			t.Fatal("the bounded wait never returned")
		}
		// ATOMIC rollback: links, ledger row, user table byte-exact.
		require.Equal(t, preLinks, dbrCellLinkFingerprint(t, ctx, sys, tid),
			"atomic rollback: links byte-exact")
		require.Equal(t, preLedger, dbrLedgerKindRow(t, ctx, sys, tid, 0),
			"atomic rollback: ledger row byte-exact")
		require.Equal(t, tid, dbrRelID(t, ctx, sys, "branch_d211r", "d211r_s"),
			"atomic rollback: the user table stands")

		// The seal's close plus a fresh re-drive commits; the funnel's
		// fresh RC probe observes the complete sealed row -> branch (iii).
		dbrConnSQL(t, ctx, holder, "commit")
		dbrSQL(t, ctx, sys, "drop table branch_d211r.d211r_s")
		require.Equal(t, preLedger, dbrLedgerKindRow(t, ctx, sys, tid, 0),
			"race finals adopt branch (iii): FULL byte equality (the sealed version, byte-identity seal)")
		require.Equal(t, preLinks, dbrCellLinkFingerprint(t, ctx, sys, tid),
			"race finals: links KEPT byte-exact")
		require.Equal(t, "0", dbrSQLStr(t, ctx, sys,
			"select cast(count(*) as char) from mo_catalog.mo_tables where relname = 'd211r_s' and reldatabase = 'branch_d211r'"),
			"race finals: the user table is gone")
	})

	// D215(B.4)/D216(C) ADMISSION-WITNESS CONTROL: DropTable is whitelisted,
	// so the RR transaction's drop reaches the lifecycle refusal; the
	// refused transaction changed nothing and the admitted retry commits
	// with branch-(i) finals.
	t.Run("admission-witness-control", func(t *testing.T) {
		tid, lgid := setupUnbranched(t, "branch_d211c", "t_c")
		ownerName := "d211c_cover"
		dbrSQL(t, ctx, sys, flavor.createSQL(ownerName))
		t.Cleanup(func() { dbrSQL(t, ctx, sys, flavor.dropSQL(ownerName)) })

		dbrAdmissionRefusalControl(t, ctx, sys,
			"drop table branch_d211c.t_c",
			"object lifecycle statements require an existing pessimistic RC transaction")
		require.Equal(t, "0|none", dbrLedgerKindRow(t, ctx, sys, tid, 0),
			"the refused drop staged no ledger write")

		dbrSQL(t, ctx, sys, "drop table branch_d211c.t_c")
		row0 := dbrLedgerKindRow(t, ctx, sys, tid, 0)
		require.True(t, strings.HasPrefix(row0, "1|"), "admitted retry: branch (i): %s", row0)
		require.True(t, strings.HasSuffix(row0, "~"+lgid), "admitted retry: logical id: %s", row0)
	})
}

// ---------------------------------------------------------------------------
// D208(C.1) THE REFRESH-ONLY DROP CELL under D214/D215/D216 (checker ARMED;
// DROP SNAPSHOT as the main schedule with the DROP PITR admitted twin and
// BOTH refusal twins): a kept ledger row grounded only by the dropped owner
// while its table stays LIVE. The refresh conflict executes in the
// production-fixed RC BACKGROUND operator (txn.go:837-848), whose
// per-statement snapshot is fresh by construction - NO conflict is
// schedulable there, so the oracle is AVAILABILITY (cells3-observed): the
// drop COMMITS CLEAN across a certified window whose mid-window producer
// seal commits at C > S; finals - owner row GONE, ledger row KEPT and
// refresh-healed (names/logical id exact, created_ts immutable), links
// byte-exact, both user moves applied. D216(C)'s SPLIT controls: (a) the
// uncommitted-txn refusal for EACH of DROP SNAPSHOT and DROP PITR
// (RR-begin refused with the :5367 'unclassified statement appears in
// uncommitted transaction'; rollback), and (b) the admitted-path
// background-operator witness (MODE pessimistic AND ISOLATION RC via the
// forcePessimisticRC chain, Txn().ID DISTINCT per drop and never a
// user-tagged transaction, the lineage-owner lifecycle lock consulted -
// witnessed STRICTLY by a concurrent second drop blocking on the gate row
// until the first drop's window closes). NO Fatal twin (D215(B.3)).
// ---------------------------------------------------------------------------
func TestDataBranchRefreshOnlyDropConflictCell(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	cnSvc, open := dbrNewRetireCluster(t, ctx)
	sys := open()
	defer sys.Close()

	// (a) THE UNCOMMITTED-TXN REFUSAL TWINS (D216(A.1)/(C)): the family's
	// statements have NO whitelist entry (stmt_kind.go:219-360), so an
	// active transaction's drop hits the :5367 unclassified error before
	// any lifecycle gate.
	t.Run("uncommitted-txn-refusal-snapshot", func(t *testing.T) {
		name := "d208_refuse_s"
		dbrSQL(t, ctx, sys, dbrSnapshotFlavor().createSQL(name))
		t.Cleanup(func() { dbrSQL(t, ctx, sys, dbrSnapshotFlavor().dropSQL(name)) })
		dbrAdmissionRefusalControl(t, ctx, sys,
			dbrSnapshotFlavor().dropSQL(name),
			"unclassified statement appears in uncommitted transaction")
	})
	t.Run("uncommitted-txn-refusal-pitr", func(t *testing.T) {
		name := "d208_refuse_p"
		dbrSQL(t, ctx, sys, dbrPitrFlavor().createSQL(name))
		t.Cleanup(func() { dbrSQL(t, ctx, sys, dbrPitrFlavor().dropSQL(name)) })
		dbrAdmissionRefusalControl(t, ctx, sys,
			dbrPitrFlavor().dropSQL(name),
			"unclassified statement appears in uncommitted transaction")
	})

	// D210(C)/(b) witnesses: the spy captures each removal's background
	// operator and txn identity; assertions run after the drops.
	type spyState struct {
		mu     sync.Mutex
		txnIDs []string
		ops    []client.TxnOperator
	}
	spy := &spyState{}
	restoreSpy := frontend.SetReapTxnOperatorResolverForTest(
		func(bh frontend.BackgroundExec,
			next func(frontend.BackgroundExec) client.TxnOperator,
		) client.TxnOperator {
			op := next(bh)
			spy.mu.Lock()
			if op != nil {
				spy.ops = append(spy.ops, op)
				spy.txnIDs = append(spy.txnIDs,
					fmt.Sprintf("%x", op.Txn().ID))
			}
			spy.mu.Unlock()
			return op
		})
	defer restoreSpy()

	// (b) THE ADMITTED DROP PITR TWIN: the autocommit drop COMMITS CLEAN
	// while the spy records its background operator (pitr.go:779-797's
	// forcePessimisticRC chain), asserted below with the snapshot drop's.
	t.Run("admitted-pitr-background-operator", func(t *testing.T) {
		name := "d208_admit_p"
		dbrSQL(t, ctx, sys, dbrPitrFlavor().createSQL(name))
		dbrSQL(t, ctx, sys, dbrPitrFlavor().dropSQL(name))
	})

	// Two accounts: shapes live in the SECOND account so the sorted reap
	// order completes the sys step first and the blocking observer opens
	// the certified window BEFORE that account's step.
	dbrSQL(t, ctx, sys, "create account d208acc admin_name='admin' identified by '111'")
	acc := dbrOpenDB(t, dbrTenantDSN(cnSvc.SQLAddress(), "d208acc"))
	defer acc.Close()

	dbrSQL(t, ctx, acc, "create database branch_d208")
	accBoot := dbrPinnedConn(t, ctx, acc, "branch_d208")
	dbrConnSQL(t, ctx, accBoot, "create table branch_d208_t (id int primary key, payload varchar(32))")
	dbrConnSQL(t, ctx, accBoot, "insert into branch_d208_t values (2, 'two'), (4, 'four')")
	dbrConnSQL(t, ctx, accBoot, "data branch create table branch_d208_s from branch_d208_t")
	// The committed genesis, then a RENAME drifts the stored name so the
	// self-live refresh has a heal write to stage.
	dbrConnSQL(t, ctx, accBoot, "update branch_d208_s set id = 3 where id = 2")
	dbrConnSQL(t, ctx, accBoot, "rename table branch_d208_s to branch_d208_slive")
	tid := dbrRelID(t, ctx, acc, "branch_d208", "branch_d208_slive")
	require.NotEqual(t, "0|x|x||0|x|x", dbrCellLinkFingerprint(t, ctx, acc, tid),
		"the committed genesis staged the retained shape")
	driftLedger := dbrLedgerRowFingerprint(t, ctx, acc, tid)
	require.NotContains(t, driftLedger, "branch_d208_slive",
		"identity drift witness: the stored name still carries the pre-rename identity")

	// The covers: the main cluster snapshot about to be dropped, and a
	// SECOND cluster snapshot whose drop must BLOCK on the lineage-owner
	// lifecycle gate row while the main drop's window holds it (the strict
	// (b) lock-consulted witness, snapshot.go:703).
	flavor := dbrSnapshotFlavor()
	ownerName := "d208_cover"
	dbrSQL(t, ctx, sys, flavor.createSQL(ownerName))
	blockerName := "d208_blocker"
	dbrSQL(t, ctx, sys, flavor.createSQL(blockerName))
	ownerRow := dbrSQLStr(t, ctx, sys, flavor.rowSQL(ownerName))
	require.True(t, strings.HasPrefix(ownerRow, "1|"), "owner row live pre-drop")

	// The certified window: the observer sleeps inside the FIRST account's
	// completed step (sys, empty ledger) while the producer commits its
	// seal in the SECOND account; windowDone closes when the sleep ends.
	windowOpened := make(chan struct{})
	windowDone := make(chan struct{})
	var once sync.Once
	restoreObserver := frontend.SetDataBranchOwnerRemovalReapObserverForTest(
		func(accountID uint32) {
			once.Do(func() {
				close(windowOpened)
				time.Sleep(3 * time.Second)
				close(windowDone)
			})
		})
	defer restoreObserver()

	dropResult := make(chan error, 1)
	go func() {
		_, derr := sys.ExecContext(ctx, flavor.dropSQL(ownerName))
		dropResult <- derr
	}()
	select {
	case <-windowOpened:
	case <-time.After(120 * time.Second):
		t.Fatal("the reap never reached its first certified step")
	}

	// (b) STRICT LOCK-CONSULTED WITNESS: a concurrent second drop must
	// BLOCK on the feature-registry SNAPSHOT gate row the main drop's
	// background transaction holds through its reap window.
	blockerDone := make(chan error, 1)
	go func() {
		_, berr := sys.ExecContext(ctx, flavor.dropSQL(blockerName))
		blockerDone <- berr
	}()

	// The producer seals the same shared row at C > S inside the certified
	// window; the availability oracle says this changes NOTHING about the
	// drop's clean commit (the background RC operator re-probes fresh).
	dbrConnSQL(t, ctx, accBoot, "update branch_d208_slive set id = 5 where id = 4")
	userRows := dbrTableRows(t, ctx, acc, "branch_d208", "branch_d208_slive")
	sealLinks := dbrCellLinkFingerprint(t, ctx, acc, tid)
	sealLedger := dbrLedgerRowFingerprint(t, ctx, acc, tid)
	require.Equal(t, driftLedger, sealLedger,
		"the seal is a byte-identity self-update and keeps the drifted name")

	// The second drop must NOT complete while the main drop's window is
	// open; it commits after the window closes and the gate releases.
	select {
	case berr := <-blockerDone:
		t.Fatalf("the concurrent second drop was NOT blocked on the lineage-owner lifecycle gate: %v", berr)
	case <-windowDone:
	}
	select {
	case berr := <-blockerDone:
		require.NoError(t, berr,
			"the blocked second drop commits clean after the gate releases")
	case <-time.After(120 * time.Second):
		t.Fatal("the blocked second drop never committed")
	}

	// (b) ADMITTED-PATH OPERATOR WITNESSES (D210(C) folded, D216(C)):
	// every resolved removal operator is the drop's OWN background
	// transaction - marked at reap-driver entry (site (4)), pessimistic and
	// RC per the forced chain, with per-drop DISTINCT Txn().IDs that match
	// NO user-tagged transaction.
	dropErr := <-dropResult
	require.NoError(t, dropErr,
		"the AVAILABILITY oracle (cells3-observed): the drop commits clean across the closed window")
	spy.mu.Lock()
	require.GreaterOrEqual(t, len(spy.txnIDs), 3,
		"three removals resolved their own background operators (pitr twin, blocked cover, blocker): %v", spy.txnIDs)
	seen := map[string]bool{}
	for i, id := range spy.txnIDs {
		require.False(t, seen[id], "each removal's background Txn().ID is DISTINCT")
		seen[id] = true
		op := spy.ops[i]
		require.True(t, op.Txn().IsPessimistic(),
			"background operator witness: MODE pessimistic (forcePessimisticRC chain)")
		require.True(t, op.Txn().IsRCIsolation(),
			"background operator witness: ISOLATION RC (txn.go:837-848)")
		marked, mok := op.(interface{ ExpectedConflictMarked() bool })
		require.True(t, mok, "the background operator carries the optional mark interface")
		require.True(t, marked.ExpectedConflictMarked(),
			"mark-ownership witness: the BACKGROUND operator is marked")
		require.Equal(t, "0", dbrSQLStr(t, ctx, sys, fmt.Sprintf(
			"select cast(count(*) as char) from mo_catalog.mo_transactions where txn_id = '%s' and txn_id is not null and user_txn = true", id)),
			"the background removal operator is never the user statement's transaction")
	}
	spy.mu.Unlock()

	// EXACT FINALS: owner row GONE, the ledger row KEPT and refresh-healed
	// (names + logical id exact, created_ts immutable), links byte-exact,
	// both user moves applied.
	require.True(t, strings.HasPrefix(
		dbrSQLStr(t, ctx, sys, flavor.rowSQL(ownerName)), "0|"),
		"exact finals: the owner row is gone")
	require.Equal(t, sealLinks, dbrCellLinkFingerprint(t, ctx, acc, tid),
		"exact finals: links byte-exact equal to the sealed version")
	finalLedger := dbrLedgerRowFingerprint(t, ctx, acc, tid)
	require.True(t, strings.HasPrefix(finalLedger, "1|"),
		"exact finals: the ledger row is KEPT")
	require.Contains(t, finalLedger, "branch_d208_slive",
		"exact finals: the identity is refresh-healed to the live name")
	require.NotContains(t, finalLedger, "branch_d208_s~",
		"exact finals: no stale name remains")
	require.Equal(t, userRows, dbrTableRows(t, ctx, acc, "branch_d208", "branch_d208_slive"),
		"exact finals: both user moves applied")
}

// ---------------------------------------------------------------------------
// D206(C.1) THE REVIEW-REQUESTED DISJOINT CONTROL and the fabric seals'
// conflict cells (checker ON): (a) SUPERSEDED by D218(C.2) - the disjoint
// payload twins now run as two-session before-COMMIT cells (ABSENT and
// EXISTING retention rows): T2 must complete while T1 stays open, with
// ZERO ledger rows staged (ABSENT) or the pre-existing row byte-unchanged
// (EXISTING); the autocommit twins could not see the serialization; (b)
// the SEAL-REMOVED arm reruns disjoint moves with the seal-staging defect
// flag forced OFF and both commit clean; (c) D206(C.2) the marked alpha:
// a marked mover's HIT-lane seal conflicts WW at C > S and returns
// SURVIVABLY, the fresh re-drive commits with ONE physical ledger row;
// (d) D206(C.4) MISS-MISS: two disjoint first moves race the genesis
// insert, the duplicate returns SURVIVABLY to one mover, the retry lands,
// ONE identity row exists. D218(C.2)(iii) retains update-then-move
// continuity in the one-statement and split-statement shapes.
// ---------------------------------------------------------------------------
func TestDataBranchFabricConflictCells(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	_, open := dbrNewRetireCluster(t, ctx)
	sys := open()
	defer sys.Close()

	// setup takes the runtime (subtest) t so the fixture's pinned session
	// closes at subtest end; see TestDataBranchFencedContentionOutcomeCell.
	setup := func(t *testing.T, dbName, tag string, moved bool) string {
		dbrSQL(t, ctx, sys, fmt.Sprintf("drop database if exists %s", dbName))
		dbrSQL(t, ctx, sys, fmt.Sprintf("create database %s", dbName))
		conn := dbrPinnedConn(t, ctx, sys, dbName)
		dbrConnSQL(t, ctx, conn, fmt.Sprintf(
			"create table %s_t (id int primary key, payload varchar(32))", dbName))
		dbrConnSQL(t, ctx, conn, fmt.Sprintf(
			"insert into %s_t values (2, 'two'), (4, 'four')", dbName))
		dbrConnSQL(t, ctx, conn, fmt.Sprintf(
			"data branch create table %s_s from %s_t", dbName, dbName))
		if moved {
			dbrConnSQL(t, ctx, conn, fmt.Sprintf(
				"update %s_s set id = 6 where id = 4", dbName))
		}
		return dbrRelID(t, ctx, sys, dbName, dbName+"_s")
	}

	t.Run("disjoint-control", func(t *testing.T) {
		// D218(C.2)(i) ABSENT retention row, the two-session before-COMMIT
		// shape that SUPERSEDES the r96-era autocommit payload twins: T1's
		// payload-only update stays OPEN; the disjoint T2 payload update
		// must complete while T1 is open - channel-ordered, the T2-done
		// signal precedes T1's COMMIT, and the failure witness is T2
		// incomplete at the deadline (the ungated r96 code serialized T2 on
		// T1's exclusive genesis identity lock, MISS shape). Finals: exact
		// rows, ZERO kind-0 ledger rows, ZERO committed links - the
		// admission-level proof, replacing reliance on the prune.
		tid := setup(t, "branch_d206a", "d206a", false)
		t1, err := sys.Conn(ctx)
		require.NoError(t, err)
		_, err = t1.ExecContext(ctx, "use branch_d206a")
		require.NoError(t, err)
		_, err = t1.ExecContext(ctx, "begin")
		require.NoError(t, err)
		_, err = t1.ExecContext(ctx,
			"update branch_d206a_s set payload = 'two-x' where id = 2")
		require.NoError(t, err)

		t2Done := make(chan error, 1)
		go func() {
			_, terr := sys.ExecContext(ctx,
				"update branch_d206a.branch_d206a_s set payload = 'four-x' where id = 4")
			t2Done <- terr
		}()
		select {
		case terr := <-t2Done:
			require.NoError(t, terr,
				"the disjoint payload update completes while the first transaction stays open")
		case <-time.After(60 * time.Second):
			t.Fatal("the disjoint payload update blocked on the open transaction: " +
				"the actual-move admission gate failed (D218, ABSENT shape)")
		}
		_, err = t1.ExecContext(ctx, "commit")
		require.NoError(t, err)
		// Release the session before the seal-removed arm: its twins need two
		// CONCURRENT pooled conns alongside the setups' pinned sessions
		// (MaxOpenConns 4).
		require.NoError(t, t1.Close())
		require.Equal(t, "2|2~two-x,4~four-x",
			dbrTableRows(t, ctx, sys, "branch_d206a", "branch_d206a_s"),
			"exact finals: both disjoint payload updates applied")
		require.Equal(t, "0|none", dbrLedgerKindRow(t, ctx, sys, tid, 0),
			"admission-level proof: continuity-only transactions stage ZERO ledger rows")
		require.Equal(t, "0|x|x||0|x|x", dbrCellLinkFingerprint(t, ctx, sys, tid),
			"exact finals: no links and no ledger row (markers never commit)")

		// SEAL-REMOVED arm: the disjoint moves themselves commit clean
		// with the seal-staging defect flag forced ON.
		setup(t, "branch_d206b", "d206b", false)
		restoreSeal := multiupdate.DisablePkMoveRetentionSealForTest(true)
		defer restoreSeal()
		done2 := make(chan error, 2)
		go func() {
			_, err := sys.ExecContext(ctx,
				"update branch_d206b.branch_d206b_s set id = 3 where id = 2")
			done2 <- err
		}()
		go func() {
			_, err := sys.ExecContext(ctx,
				"update branch_d206b.branch_d206b_s set id = 5 where id = 4")
			done2 <- err
		}()
		require.NoError(t, <-done2)
		require.NoError(t, <-done2)
		require.Equal(t, "2|3~two,5~four",
			dbrTableRows(t, ctx, sys, "branch_d206b", "branch_d206b_s"),
			"both disjoint moves committed clean with the seal removed")
	})

	t.Run("disjoint-control-existing-row", func(t *testing.T) {
		// D218(C.2)(ii) EXISTING retention row (one committed move
		// pre-seeds the address): the same two-session before-COMMIT shape
		// completes disjointly - the ungated HIT lane serialized T2 on
		// T1's staged seal row lock - PLUS the pre-existing ledger row
		// stays BYTE-UNCHANGED afterwards (no seal staged by either
		// session) and the links/ledger sums stay frozen.
		tid := setup(t, "branch_d206e", "d206e", true)
		preLedger0 := dbrLedgerKindRow(t, ctx, sys, tid, 0)
		require.True(t, strings.HasPrefix(preLedger0, "1|"),
			"the seeding move registered the shared identity row (HIT lane): %s", preLedger0)
		preShape := dbrCellLinkFingerprint(t, ctx, sys, tid)

		t1, err := sys.Conn(ctx)
		require.NoError(t, err)
		// Release the session at subtest end, not before the queries below.
		defer t1.Close()
		_, err = t1.ExecContext(ctx, "use branch_d206e")
		require.NoError(t, err)
		_, err = t1.ExecContext(ctx, "begin")
		require.NoError(t, err)
		_, err = t1.ExecContext(ctx,
			"update branch_d206e_s set payload = 'two-x' where id = 2")
		require.NoError(t, err)

		t2Done := make(chan error, 1)
		go func() {
			_, terr := sys.ExecContext(ctx,
				"update branch_d206e.branch_d206e_s set payload = 'four-x' where id = 6")
			t2Done <- terr
		}()
		select {
		case terr := <-t2Done:
			require.NoError(t, terr,
				"the disjoint payload update completes while the first transaction stays open")
		case <-time.After(60 * time.Second):
			t.Fatal("the disjoint payload update blocked on the open transaction's seal (D218, EXISTING shape)")
		}
		_, err = t1.ExecContext(ctx, "commit")
		require.NoError(t, err)
		require.Equal(t, "2|2~two-x,6~four-x",
			dbrTableRows(t, ctx, sys, "branch_d206e", "branch_d206e_s"),
			"exact finals: both disjoint payload updates applied")
		require.Equal(t, preLedger0, dbrLedgerKindRow(t, ctx, sys, tid, 0),
			"the pre-existing ledger row is BYTE-UNCHANGED: neither session staged a seal")
		require.Equal(t, preShape, dbrCellLinkFingerprint(t, ctx, sys, tid),
			"exact finals: links and ledger sums frozen across the payload sessions")
	})

	t.Run("continuity-retained", func(t *testing.T) {
		// D218(C.2)(iii) CONTINUITY RETAINED: a payload edit then a pk
		// move issued as ONE statement, and the same shape split across
		// two statements of one transaction (payload-only first, move
		// second): both commit the move's link plus exactly one ledger
		// row, and the split shape's committed link names the SAME
		// old->new pk bytes the one-statement shape produced - the
		// continuity marker carried the lineage through the gate.
		runShape := func(t *testing.T, dbName string, oneStatement bool) string {
			tid := setup(t, dbName, "x", false)
			// A subtest-scoped dbrPinnedConn would hold its pool slot to
			// subtest end: two shapes x (setup + shape session) exhaust
			// MaxOpenConns 4 and starve the pooled fingerprint queries.
			conn, cerr := sys.Conn(ctx)
			require.NoError(t, cerr)
			_, cerr = conn.ExecContext(ctx, fmt.Sprintf("use %s", dbName))
			require.NoError(t, cerr)
			dbrConnSQL(t, ctx, conn, "begin")
			if oneStatement {
				dbrConnSQL(t, ctx, conn, fmt.Sprintf(
					"update %s_s set payload = 'p', id = 3 where id = 2", dbName))
			} else {
				dbrConnSQL(t, ctx, conn, fmt.Sprintf(
					"update %s_s set payload = 'p' where id = 2", dbName))
				dbrConnSQL(t, ctx, conn, fmt.Sprintf(
					"update %s_s set id = 3 where id = 2", dbName))
			}
			dbrConnSQL(t, ctx, conn, "commit")
			require.NoError(t, conn.Close())
			require.Equal(t, "2|3~p,4~four",
				dbrTableRows(t, ctx, sys, dbName, dbName+"_s"),
				"exact finals: the payload and the move both applied")
			require.True(t, strings.HasPrefix(
				dbrLedgerKindRow(t, ctx, sys, tid, 0), "1|"),
				"exactly one kind-0 ledger row accompanies the committed move")
			linkPair := dbrSQLStr(t, ctx, sys, fmt.Sprintf(
				"select concat_ws('|', cast(count(*) as char), coalesce(group_concat(concat(hex(old_pk), '>', hex(new_pk))), 'none')) from mo_catalog.mo_data_branch_update_link where table_id = %s", tid))
			require.True(t, strings.HasPrefix(linkPair, "1|"),
				"exactly one committed link accompanies the move")
			return linkPair
		}
		oneLink := runShape(t, "branch_d206f", true)
		splitLink := runShape(t, "branch_d206g", false)
		require.Equal(t, oneLink, splitLink,
			"the split shape's marker carried the lineage: identical old->new pk bytes; a broken gate or dropped marker loses or corrupts this row")
	})

	t.Run("alpha-hit-seal-ww", func(t *testing.T) {
		tid := setup(t, "branch_d206c", "d206c", true)
		preLedger := dbrLedgerRowFingerprint(t, ctx, sys, tid)
		require.True(t, strings.HasPrefix(preLedger, "1|"),
			"the identity row already existed (HIT lane) pre-schedule")
		// D214(B) VENUE (D215(B.2)(ii)'s verified carrier record): the loser
		// mover's SESSION transaction pins its snapshot REPEATABLE-READ; the
		// producer/controls stay READ-COMMITTED. Conflict statements return
		// NIL and TN adjudication refuses 20619 AT THE EXPLICIT COMMIT
		// (PROBE-RR-WW3, /tmp/e114_rrprobe1.log RC=0).
		s1, err := sys.Conn(ctx)
		require.NoError(t, err)
		defer s1.Close()
		_, err = s1.ExecContext(ctx, "set session transaction_isolation='REPEATABLE-READ'")
		require.NoError(t, err)
		_, err = s1.ExecContext(ctx, "use branch_d206c")
		require.NoError(t, err)
		_, err = s1.ExecContext(ctx, "begin")
		require.NoError(t, err)
		_, err = s1.ExecContext(ctx, "select count(*) from branch_d206c_t")
		require.NoError(t, err)
		// The sibling's pk move seals the same identity row at C > S.
		dbrSQL(t, ctx, sys, "update branch_d206c.branch_d206c_s set id = 7 where id = 6")
		_, mvErr := s1.ExecContext(ctx, "update branch_d206c_s set id = 3 where id = 2")
		require.NoError(t, mvErr,
			"under the RR pin the marked mover's statements return nil")
		_, cmErr := s1.ExecContext(ctx, "commit")
		require.Error(t, cmErr,
			"the sibling-seal schedule must adjudicate 20619 at the explicit commit")
		require.Equal(t, "ww", dbrErrClassLocked(cmErr),
			"the marked mover's 20619 returns SURVIVABLY at commit, whole rollback: %v", cmErr)
		// Cleanup tolerates the auto-rolled-back post-commit-failure state.
		_, _ = s1.ExecContext(ctx, "rollback")
		// The fresh re-drive per D201(B.5)/D205 commits; finals: ONE
		// physical ledger row byte-exact, every committed move's link
		// addressed to it, both user moves applied.
		dbrSQL(t, ctx, sys, "update branch_d206c.branch_d206c_s set id = 3 where id = 2")
		require.True(t, strings.HasPrefix(
			dbrLedgerRowFingerprint(t, ctx, sys, tid), "1|"),
			"ONE physical ledger row")
		require.Equal(t, "1", dbrSQLStr(t, ctx, sys, fmt.Sprintf(
			"select cast(count(*) as char) from mo_catalog.mo_data_branch_link_retention where table_id = %s", tid)),
			"ONE physical ledger row total")
		require.Equal(t, "3", dbrSQLStr(t, ctx, sys, fmt.Sprintf(
			"select cast(count(*) as char) from mo_catalog.mo_data_branch_update_link where table_id = %s", tid)),
			"the three committed moves' links addressed to the one identity row")
		require.Equal(t, "2|3~two,7~four",
			dbrTableRows(t, ctx, sys, "branch_d206c", "branch_d206c_s"),
			"both user moves applied")
	})

	t.Run("miss-miss-first-touch", func(t *testing.T) {
		tid := setup(t, "branch_d206d", "d206d", false)
		// D214(B) VENUE: the racing mover's session pins REPEATABLE-READ;
		// the pinned duplicate insert returns nil at the statement and
		// adjudicates 20619 WW-class AT THE EXPLICIT COMMIT (PROBE-RR-DUP2;
		// the 1062 fresh-read class stands only on the CN surface).
		s1, err := sys.Conn(ctx)
		require.NoError(t, err)
		defer s1.Close()
		_, err = s1.ExecContext(ctx, "set session transaction_isolation='REPEATABLE-READ'")
		require.NoError(t, err)
		_, err = s1.ExecContext(ctx, "use branch_d206d")
		require.NoError(t, err)
		_, err = s1.ExecContext(ctx, "begin")
		require.NoError(t, err)
		_, err = s1.ExecContext(ctx, "select count(*) from branch_d206d_t")
		require.NoError(t, err)
		// The first mover registers the identity row at C > S_d.
		dbrSQL(t, ctx, sys, "update branch_d206d.branch_d206d_s set id = 3 where id = 2")
		// The second disjoint first move races the genesis insert.
		_, mvErr := s1.ExecContext(ctx, "update branch_d206d_s set id = 5 where id = 4")
		require.NoError(t, mvErr,
			"under the RR pin the racing mover's statements return nil")
		_, cmErr := s1.ExecContext(ctx, "commit")
		require.Error(t, cmErr,
			"the miss-miss schedule must adjudicate 20619 at the explicit commit")
		require.Equal(t, "ww", dbrErrClassLocked(cmErr),
			"the pinned duplicate adjudicates WW-class at commit, SURVIVABLY: %v", cmErr)
		_, _ = s1.ExecContext(ctx, "rollback")
		// The retry lands; ONE identity row exists; both moves applied.
		dbrSQL(t, ctx, sys, "update branch_d206d.branch_d206d_s set id = 5 where id = 4")
		require.True(t, strings.HasPrefix(
			dbrLedgerRowFingerprint(t, ctx, sys, tid), "1|"),
			"ONE identity row exists")
		require.Equal(t, "2|3~two,5~four",
			dbrTableRows(t, ctx, sys, "branch_d206d", "branch_d206d_s"),
			"both moves applied after the retry")
	})
}

// ---------------------------------------------------------------------------
// THE MARK-DISABLED FATAL RED TWINS (deterministic necessity): with the
// mark-disabled defect flag forced ON, the marked-cell schedule's verifier
// FATALS in the cell-owned service. Per D215(B.3) the necessity
// demonstration consolidates at D206(C.2)'s FABRICSEAL twin (a
// session-carried marked mover on the D214(B) RR venue) plus the CONTENDED
// same-row parity control (the verifier STILL armed on plain contended user
// rows - the disjoint control is not vacuous); the drop-carried RETIRE and
// KEEP twins are VACATED - lifecycle admission forces every public DROP to
// pessimistic RC (txn.go:572-579/:837-848), whose transactional conflicts
// hot-heal or return bounded non-Fatal errors through any admission-
// preserving schedule, so no checker-Fatal class is reachable there. Every
// disposition fires the verifier's zap Fatal, which is a recovered panic
// per logutil/api.go:161, so each variant runs as a subprocess child whose
// failure propagates past the recovered connection; the parent asserts the
// child's non-zero exit and the fatal signature (the zap FATAL entry plus
// the child's sentinel, never RED-TWIN-DID-NOT-FATAL).
// ---------------------------------------------------------------------------
const dbrFatalChildEnv = "MO_DBRANCH_FATAL_CHILD"

func TestDataBranchExpectedConflictFatalRedTwins(t *testing.T) {
	if os.Getenv(dbrFatalChildEnv) != "" {
		dbrRunFatalChild(t, os.Getenv(dbrFatalChildEnv))
		return
	}
	for _, variant := range []string{"fabricseal", "contended"} {
		t.Run(variant, func(t *testing.T) {
			cmd := exec.Command(os.Args[0],
				"-test.run=^TestDataBranchExpectedConflictFatalRedTwins$")
			cmd.Env = append(os.Environ(), dbrFatalChildEnv+"="+variant)
			out, err := cmd.CombinedOutput()
			require.Error(t, err,
				"the child must exit non-zero on the unmarked covered conflict")
			require.True(t,
				bytes.Contains(bytes.ToLower(out), []byte("fatal")),
				"the verifier's Fatal signature is present in the child log: %s", out)
			require.False(t,
				bytes.Contains(out, []byte("RED-TWIN-DID-NOT-FATAL")),
				"the schedule genuinely reached the covered conflict: %s", out)
		})
	}
}

// dbrRunFatalChild reruns one marked-cell schedule with the mark-disabled
// defect flag forced ON and REQUIRES the verifier's Fatal (delivered as
// the WriteThenPanic 20101 signature, operator.go:1889 via
// logutil/api.go:161); a survivable return or a clean commit is a failed
// RED twin, reported distinctly so the parent can tell it apart from the
// verifier's own signature.
func dbrRunFatalChild(t *testing.T, variant string) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	_, open := dbrNewRetireCluster(t, ctx)
	sys := open()
	defer sys.Close()
	restore := client.DisableExpectedConflictMarkForTest(true)
	defer restore()

	trigger := func(schedule func() error) {
		err := schedule()
		if err == nil {
			fmt.Fprintf(os.Stderr, "RED-TWIN-DID-NOT-FATAL: committed cleanly\n")
			t.Fatal("RED-TWIN-DID-NOT-FATAL")
			return
		}
		// The verifier's Fatal IS the oracle, but its transport is not
		// process death: operator.go:1884-1892's unmarked consult fires
		// tc.logger.Fatal("failed"), and logutil/api.go:161 pins
		// zapcore.WriteThenPanic for EVERY MatrixOne logger, so the Fatal
		// panics, the CN recovers it per-connection, and the client
		// observes Error 20101 "internal error: panic failed" while the
		// FATAL log entry lands in this process's output. The `panic
		// failed` needle is unique: operator.go:1889 is the repository's
		// only logger.Fatal("failed") call site.
		if strings.Contains(err.Error(), "panic failed") {
			fmt.Fprintf(os.Stderr,
				"RED-TWIN-FATAL-SIGNATURE-OBSERVED: %v\n", err)
			t.Fatal("RED-TWIN-FATAL-SIGNATURE-OBSERVED")
			return
		}
		fmt.Fprintf(os.Stderr, "RED-TWIN-DID-NOT-FATAL: survivable %v\n", err)
		t.Fatal("RED-TWIN-DID-NOT-FATAL")
	}

	dbName := "branch_red_" + variant
	dbrSQL(t, ctx, sys, fmt.Sprintf("create database %s", dbName))
	boot := dbrPinnedConn(t, ctx, sys, dbName)
	dbrConnSQL(t, ctx, boot, fmt.Sprintf(
		"create table %s_t (id int primary key, payload varchar(32))", dbName))
	dbrConnSQL(t, ctx, boot, fmt.Sprintf(
		"insert into %s_t values (2, 'two'), (4, 'four')", dbName))

	switch variant {
	case "fabricseal":
		dbrConnSQL(t, ctx, boot, fmt.Sprintf(
			"data branch create table %s_s from %s_t", dbName, dbName))
		dbrConnSQL(t, ctx, boot, fmt.Sprintf("update %s_s set id = 6 where id = 4", dbName))
		trigger(func() error {
			s1, err := sys.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer s1.Close()
			stmts := []string{
				"set session transaction_isolation='REPEATABLE-READ'",
				fmt.Sprintf("use %s", dbName),
				"begin",
				fmt.Sprintf("select count(*) from %s_t", dbName),
			}
			for _, q := range stmts {
				if _, err = s1.ExecContext(ctx, q); err != nil {
					t.Fatal(err)
				}
			}
			// The sibling commits its seal at C > S; the mover's statement
			// returns nil under the RR pin and the 20619 adjudicates at the
			// explicit commit - where the unmarked watchdog FATALS
			// (operator.go:1884-1892 via the WriteThenPanic transport).
			dbrConnSQL(t, ctx, boot, fmt.Sprintf(
				"update %s_s set id = 7 where id = 6", dbName))
			if _, err = s1.ExecContext(ctx,
				fmt.Sprintf("update %s_s set id = 3 where id = 2", dbName)); err != nil {
				return err
			}
			_, err = s1.ExecContext(ctx, "commit")
			return err
		})
	case "contended":
		trigger(func() error {
			s1, err := sys.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer s1.Close()
			stmts := []string{
				"set session transaction_isolation='REPEATABLE-READ'",
				fmt.Sprintf("use %s", dbName),
				"begin",
				fmt.Sprintf("select payload from %s_t where id = 2", dbName),
			}
			for _, q := range stmts {
				if _, err = s1.ExecContext(ctx, q); err != nil {
					t.Fatal(err)
				}
			}
			dbrConnSQL(t, ctx, boot, fmt.Sprintf(
				"update %s_t set payload = 'a' where id = 2", dbName))
			if _, err = s1.ExecContext(ctx,
				fmt.Sprintf("update %s_t set payload = 'b' where id = 2", dbName)); err != nil {
				return err
			}
			_, err = s1.ExecContext(ctx, "commit")
			return err
		})
	default:
		t.Fatalf("unknown RED twin variant %q", variant)
	}
}
