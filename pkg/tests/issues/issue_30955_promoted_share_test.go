// Copyright 2026 MatrixOne
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// #30955, pre-PR review round 8, finding 2: a SELECT ... LOCK IN SHARE MODE
// whose Shared lock target exceeds the MaxLockRowCount budget is promoted by
// applyLockTableFallback at bind, staged by compileLock into the pre-pipeline
// disposition, and acquired by lockTable BEFORE source admission. Under RR a
// writer that commits while the promoted shared table lock waits must be
// served by the reader's current read: the acquisition carries the admitted
// locking-current-read contract, the read frontier resolves after the grant,
// and the statement returns the COMMITTED value — not today's
// "SI not support retry" terminal conflict, and not the stale pre-commit
// snapshot. This member executes the exact reproduction on a real embedded
// cluster, single CN, with a forced promotion (100k rows over the embedded
// cluster's 10k MaxLockRowCount budget), an observed wait (the lock-service
// waiter poll), a mid-wait commit, the exact current-value oracle, and the
// subsequent ordinary RR snapshot read inside the same reader transaction.
func TestPromotedShareCurrentReadAfterMidWaitCommitUnderRR(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true",
			cn.GetServiceConfig().CN.Frontend.Port)
		sqlDB, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer sqlDB.Close()

		writer, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer writer.Close()
		reader, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer reader.Close()

		exec := func(conn *sql.Conn, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoErrorf(t, err, "SQL: %s", statement)
		}

		dbName := strings.ToLower(testutils.GetDatabaseName(t))
		exec(writer, "create database "+dbName)
		defer func() {
			dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer dropCancel()
			_, _ = sqlDB.ExecContext(dropCtx, "drop database if exists "+dbName)
		}()
		exec(writer, "use "+dbName)
		exec(reader, "use "+dbName)
		exec(writer, "create table f(id int primary key, v int)")

		// The forced promotion fixture: 100000 rows over the embedded
		// cluster's MaxLockRowCount budget of 10000 (adjustBasicClusterService).
		const rows = 100000
		exec(writer,
			fmt.Sprintf("insert into f select result, result from generate_series(1,%d) g;", rows))
		snapshotSum := int64(rows) * int64(rows+1) / 2
		const touchedID = 7
		const bumpedValue = int64(1000000000)
		currentSum := snapshotSum - touchedID + bumpedValue

		var tableID uint64
		require.NoError(t, writer.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = ? and relname = 'f'",
			dbName).Scan(&tableID))

		// The writer holds an exclusive row lock inside an open transaction.
		exec(writer, "begin")
		writerCommitted := false
		defer func() {
			if !writerCommitted {
				rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer rollbackCancel()
				_, _ = writer.ExecContext(rollbackCtx, "rollback")
			}
		}()
		exec(writer, fmt.Sprintf("update f set v = %d where id = %d", bumpedValue, touchedID))

		// The reader runs under explicit REPEATABLE READ (MatrixOne's SI):
		// the defect's contract is a FIXED transaction snapshot, so the
		// subsequent ordinary read must keep the pre-commit value while the
		// locking read current-reads the committed one.
		exec(reader, "set session transaction isolation level repeatable read")

		// The reader opens its RR transaction and freezes its gauge BEFORE
		// the locking read: this ordinary snapshot read must keep observing
		// the pre-commit value forever, before AND after the locking read.
		exec(reader, "begin")
		var gauge int64
		require.NoError(t, reader.QueryRowContext(ctx,
			"select v from f where id = ?", touchedID).Scan(&gauge))
		require.Equal(t, int64(touchedID), gauge, "the reader's snapshot predates the writer commit")

		// The promoted shared locking read runs in the reader's transaction;
		// its table lock waits on the writer's row lock.
		// SUM over an int column returns a MySQL DECIMAL, so the value is
		// scanned as float64: every oracle value of this member is an exact
		// integral below 2^33 and exactly representable.
		done := make(chan float64, 1)
		readErr := make(chan error, 1)
		go func() {
			var sum sql.NullFloat64
			err := reader.QueryRowContext(ctx, "select sum(v) from f lock in share mode").Scan(&sum)
			if err != nil {
				readErr <- err
				return
			}
			done <- sum.Float64
		}()
		readerDone := false
		defer func() {
			if !readerDone {
				_, _ = reader.ExecContext(context.WithoutCancel(ctx), "rollback")
			}
		}()

		// Observed wait: the promoted shared request must really be waiting
		// on the writer's row lock before the commit releases it.
		require.Eventually(t, func() bool {
			return issueLockWaiters30955(c, tableID) > 0
		}, 30*time.Second, 10*time.Millisecond,
			"the promoted shared table lock never waited on the writer")

		// The writer commits WHILE the promoted shared lock waits: the
		// committed value must serve the reader's current read.
		exec(writer, "commit")
		writerCommitted = true

		select {
		case sum := <-done:
			require.Equal(t, float64(currentSum), sum,
				"the promoted LOCK IN SHARE MODE current-reads the mid-wait committed value")
		case err := <-readErr:
			t.Fatalf("the promoted locking read failed instead of reading the committed value: %v", err)
		case <-ctx.Done():
			t.Fatal("the promoted locking read did not finish")
		}

		// The subsequent ordinary RR read inside the same open reader
		// transaction stays on the fixed transaction snapshot.
		require.NoError(t, reader.QueryRowContext(ctx,
			"select v from f where id = ?", touchedID).Scan(&gauge))
		require.Equal(t, int64(touchedID), gauge,
			"the ordinary RR read keeps the transaction snapshot after the locking current read")
		exec(reader, "commit")
		readerDone = true

		// A fresh transaction sees the committed value.
		var fresh int64
		require.NoError(t, sqlDB.QueryRowContext(ctx,
			fmt.Sprintf("select v from `%s`.f where id = ?", dbName), touchedID).Scan(&fresh))
		require.Equal(t, bumpedValue, fresh)
	})
}

// #30955, pre-PR review round 9: the whole-statement admission decision must
// be FINAL before the promoted pre-pipeline lock options are chosen. A
// promoted Shared locking read that joins an ORDINARY table created inside
// the same transaction passes every plan-level admission check, and its
// relation binding refuses only at source initialization (the createdInTxn
// soft refusal, txn_table_delegate.go) — AFTER the pre-pipeline lock
// acquisition has already run. The round-8 marked acquisition suppressed
// exactly the mid-wait committed conflict (lock_op.go's marked
// wait-resolved branch) before the same statement restored itself to
// snapshot semantics through the binding-refusal abandonment, silently
// returning the STALE committed state where the legacy acquisition fails
// with today's terminal "SI not support retry" (D-10 clause 3/G4). The
// round-9 prelock relation-support resolution declines the whole statement
// up front — sharing the admission decision with source initialization — so
// the promoted shared table lock acquires today's LEGACY options and the
// mid-wait commit surfaces today's conflict. This member executes the
// deterministic promotion-plus-binding-refusal reproduction the review
// demanded: forced promotion, observed wait, mid-wait commit, the declined
// statement's legacy conflict, and the admitted current-read control stays
// with the preceding member.
func TestPromotedShareBindingRefusalDeclinesToLegacyConflictUnderRR(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true",
			cn.GetServiceConfig().CN.Frontend.Port)
		sqlDB, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer sqlDB.Close()

		writer, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer writer.Close()
		reader, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer reader.Close()

		exec := func(conn *sql.Conn, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoErrorf(t, err, "SQL: %s", statement)
		}

		dbName := strings.ToLower(testutils.GetDatabaseName(t))
		exec(writer, "create database "+dbName)
		defer func() {
			dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer dropCancel()
			_, _ = sqlDB.ExecContext(dropCtx, "drop database if exists "+dbName)
		}()
		exec(writer, "use "+dbName)
		exec(reader, "use "+dbName)
		exec(writer, "create table f(id int primary key, v int)")

		// The forced promotion fixture: 100000 rows over the embedded
		// cluster's MaxLockRowCount budget of 10000.
		const rows = 100000
		exec(writer,
			fmt.Sprintf("insert into f select result, result from generate_series(1,%d) g;", rows))
		const touchedID = 7
		const bumpedValue = int64(1000000000)

		var tableID uint64
		require.NoError(t, writer.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = ? and relname = 'f'",
			dbName).Scan(&tableID))

		// The writer holds an exclusive row lock inside an open transaction.
		exec(writer, "begin")
		writerCommitted := false
		defer func() {
			if !writerCommitted {
				rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer rollbackCancel()
				_, _ = writer.ExecContext(rollbackCtx, "rollback")
			}
		}()
		exec(writer, fmt.Sprintf("update f set v = %d where id = %d", bumpedValue, touchedID))

		// The reader runs under explicit REPEATABLE READ and joins a relation
		// created inside its own open transaction — the reachable form of the
		// reviewer's transaction-created-relation counterexample. The two
		// alternative constructions are front-gated by today's tree and are
		// recorded as red-fixture iterations (evidence/rev22/logs/
		// red-probe-off-embedded.log.attempt*): a plain CREATE TABLE inside
		// an active SI transaction is refused by frontend/txn.go's
		// object-lifecycle gate (it demands a pessimistic RC transaction —
		// attempt1, Error 20105), and an in-transaction CREATE INDEX on the
		// promoted table serializes against the concurrent writer through
		// the lock service (attempt3, Error 1205, DDL waits out the
		// writer's row lock), while an index created on a small joined
		// table de-promotes the lock target (applyLockTableFallback promotes
		// from the LOCK_OP node's join-narrowed Outcnt) and the fabricated
		// covered scan binds the owning BASE handle anyway (rev-20 provenance
		// contract). The SESSION TEMPORARY table joins the promoted Shared
		// locking read under SI, is transaction-created for the binding, and
		// whichever admission layer declines it — round 9's prelock
		// relation-support resolution or the plan-admission family before
		// it — the USER-VISIBLE contract this member pins is the same:
		// today's whole-statement legacy outcome, the terminal mid-wait
		// conflict, never a suppressed conflict restored onto snapshot
		// semantics.
		exec(reader, "set session transaction isolation level repeatable read")
		exec(reader, "begin")
		exec(reader, "create temporary table g(id int primary key, v int)")
		exec(reader, fmt.Sprintf("insert into g values (%d, 1)", touchedID))

		var gauge int64
		require.NoError(t, reader.QueryRowContext(ctx,
			"select v from f where id = ?", touchedID).Scan(&gauge))
		require.Equal(t, int64(touchedID), gauge, "the reader's snapshot predates the writer commit")

		done := make(chan float64, 1)
		readErr := make(chan error, 1)
		go func() {
			var sum sql.NullFloat64
			err := reader.QueryRowContext(ctx,
				"select sum(f.v) from f join g on f.id = g.id lock in share mode").Scan(&sum)
			if err != nil {
				readErr <- err
				return
			}
			done <- sum.Float64
		}()
		readerDone := false
		defer func() {
			if !readerDone {
				_, _ = reader.ExecContext(context.WithoutCancel(ctx), "rollback")
			}
		}()

		// Observed wait: the locking read's shared lock on f must really be
		// waiting on the writer's row lock before the commit releases it.
		require.Eventually(t, func() bool {
			return issueLockWaiters30955(c, tableID) > 0
		}, 30*time.Second, 10*time.Millisecond,
			"the promoted shared table lock never waited on the writer")

		// The writer commits WHILE the locking read waits.
		exec(writer, "commit")
		writerCommitted = true

		select {
		case sum := <-done:
			t.Fatalf("the declined statement silently returned %v — the suppressed conflict was restored onto snapshot semantics", sum)
		case err := <-readErr:
			require.Error(t, err)
			require.Contains(t, err.Error(), "SI not support retry",
				"the declined statement keeps today's terminal legacy conflict")
			require.Contains(t, err.Error(), "w-w conflict")
		case <-ctx.Done():
			t.Fatal("the declined locking read did not finish")
		}
		// The failed statement never lost the reader's transaction state:
		// a best-effort rollback clears it regardless of the statement's
		// post-error transaction mode, and a fresh transaction still sees
		// the writer's committed value.
		_, _ = reader.ExecContext(context.WithoutCancel(ctx), "rollback")
		readerDone = true

		// A fresh transaction sees the committed value: the failing
		// statement never lost the writer's commit.
		var fresh int64
		require.NoError(t, sqlDB.QueryRowContext(ctx,
			fmt.Sprintf("select v from `%s`.f where id = ?", dbName), touchedID).Scan(&fresh))
		require.Equal(t, bumpedValue, fresh)
	})
}

// #30955, pre-PR review round 10, finding 2: lock marks must stay per
// OCCURRENCE. The supported LEFT-lowered same-table scalar-subquery shape
// puts TWO occurrences of ONE table into a single LockOp — the outer locked
// occurrence (marked current-read, admitted) and a nested subquery
// occurrence of the same table that stays snapshot-bound (unmarked presence
// by construction, design D-1's per-target representation + R4-F2/R5-F1's
// same-table identity). The bind-time collector collapses nothing: each
// plan.LockTarget carries its own LockingRead — but the operator-side marks
// expanded by TABLE ID, so the nested occurrence's row lock inherited the
// marked current-read conflict suppression/retry contract and a writer
// committing while the NESTED occurrence's shared lock waits was silently
// served instead of surfacing today's terminal "SI not support retry"
// legacy conflict. This member pins the nested target's legacy conflict
// behavior at row scale (no promotion): the writer's row is inside the
// nested occurrence's scan only, the outer occurrence's row stays
// uncontended, the wait is observed on the real lock service, and the commit
// lands mid-wait. The eligible current-read control stays with the rev-8
// member above: only a marked occurrence's own acquisition may suppress.
func TestSharedScalarSubqueryNestedOccurrenceKeepsLegacyConflictUnderRR(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true",
			cn.GetServiceConfig().CN.Frontend.Port)
		sqlDB, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer sqlDB.Close()

		writer, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer writer.Close()
		reader, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer reader.Close()

		exec := func(conn *sql.Conn, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoErrorf(t, err, "SQL: %s", statement)
		}

		dbName := strings.ToLower(testutils.GetDatabaseName(t))
		exec(writer, "create database "+dbName)
		defer func() {
			dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer dropCancel()
			_, _ = sqlDB.ExecContext(dropCtx, "drop database if exists "+dbName)
		}()
		exec(writer, "use "+dbName)
		exec(reader, "use "+dbName)

		// Row scale on purpose: no promotion, so each occurrence takes its
		// own row locks and the nested occurrence's acquisition is directly
		// observable.
		exec(writer, "create table f(id int primary key, v int)")
		exec(writer, "insert into f values (1, 100), (2, 100), (3, 100)")

		const outerID = 2
		const innerID = 3
		const bumpedValue = int64(999)

		var tableID uint64
		require.NoError(t, writer.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = ? and relname = 'f'",
			dbName).Scan(&tableID))

		// The writer holds an exclusive row lock on the NESTED occurrence's
		// row; the outer occurrence's row stays uncontended.
		exec(writer, "begin")
		writerCommitted := false
		defer func() {
			if !writerCommitted {
				rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer rollbackCancel()
				_, _ = writer.ExecContext(rollbackCtx, "rollback")
			}
		}()
		exec(writer, fmt.Sprintf("update f set v = %d where id = %d", bumpedValue, innerID))

		exec(reader, "set session transaction isolation level repeatable read")
		exec(reader, "begin")

		// The same-table scalar-subquery locking read: the outer occurrence
		// of f scans and locks row outerID, the LEFT-lowered nested
		// occurrence of the SAME table scans its snapshot and locks row
		// innerID inside one LockOp. LIMIT 1 pins the plain LEFT lowering
		// (query_builder.go:13195-13205/flatten_subquery.go:797 — the same
		// supported shape design R4-F2/R5-F1 identified).
		done := make(chan int64, 1)
		readErr := make(chan error, 1)
		go func() {
			var v sql.NullInt64
			err := reader.QueryRowContext(ctx,
				fmt.Sprintf("select v from f where id = %d and v = (select v from f where id = %d limit 1) lock in share mode",
					outerID, innerID)).Scan(&v)
			if err != nil {
				readErr <- err
				return
			}
			done <- v.Int64
		}()
		readerDone := false
		defer func() {
			if !readerDone {
				_, _ = reader.ExecContext(context.WithoutCancel(ctx), "rollback")
			}
		}()

		// Observed wait: the nested occurrence's shared row lock really waits
		// on the writer's exclusive row lock before the commit releases it.
		require.Eventually(t, func() bool {
			return issueLockWaiters30955(c, tableID) > 0
		}, 30*time.Second, 10*time.Millisecond,
			"the nested occurrence's shared row lock never waited on the writer")

		// The writer commits WHILE the nested occurrence's lock waits.
		exec(writer, "commit")
		writerCommitted = true

		select {
		case v := <-done:
			t.Fatalf("the nested (snapshot-bound) occurrence's lock inherited the marked suppression and the statement silently returned %v", v)
		case err := <-readErr:
			require.Error(t, err)
			require.Contains(t, err.Error(), "SI not support retry",
				"the nested target keeps today's terminal legacy conflict behavior")
			require.Contains(t, err.Error(), "w-w conflict")
		case <-ctx.Done():
			t.Fatal("the same-table scalar-subquery locking read did not finish")
		}

		// A best-effort rollback clears the reader, and a fresh transaction
		// sees the committed value: the legacy conflict never lost the
		// writer's commit.
		_, _ = reader.ExecContext(context.WithoutCancel(ctx), "rollback")
		readerDone = true

		var fresh int64
		require.NoError(t, sqlDB.QueryRowContext(ctx,
			fmt.Sprintf("select v from `%s`.f where id = ?", dbName), innerID).Scan(&fresh))
		require.Equal(t, bumpedValue, fresh)
	})
}

// #30955, PR review round 26 (mo-bot P2, head 13a19202): the historical
// exemption member must OBSERVE the lock result it grades. The bounded
// fresh-database trial loop previously accepted any successful trial, so a
// silently completed execution followed by a legacy-conflict trial passed —
// masking intermittent regressions — and the promoted aggregate was logged
// without being validated. This member now runs the reproduction ONCE and
// synchronizes on the acquisition's RESOLVED lock result through the
// existing lock-result hook of doLock (lock_op.go:1276, fired on the
// promoted pre-pipeline table path too): the observed flags decide the
// strict per-execution oracle. A wait-resolved conflict (HasConflict AND
// HasPrevCommit — the writer committed while this lock waited) MUST
// surface today's terminal legacy conflict; ANY silently returned
// aggregate on such an execution is exactly the suppression this member
// exists to catch, and it fails this single execution. Any OTHER observed
// flags are the pre-existing stale-flag window of the wait resolution
// (present at this PR's base; CI observed it once on the mixed sibling:
// run 41067443301, job 124102560211) — an acquisition that observably
// resolved that way can ONLY complete silently on the pre-commit
// historical view, with exactly that aggregate and no error. The graded
// contract itself is round 10, finding 1, unchanged: promoted lock options
// derive from EFFECTIVE per-execution occurrence eligibility, so a
// promoted (100000 rows over the embedded 10000 MaxLockRowCount budget)
// locking read with an ENGAGING explicit snapshot (mo_ts = H, H strictly
// earlier than the executing SnapshotTS) is withheld from this execution's
// current read, its promoted Shared table lock keeps today's legacy
// options, and a mid-wait commit surfaces the legacy conflict — never a
// silently current-read committed value; the historical view stays intact
// and the eligible (no mo_ts) current-read control keeps the round-8
// contract.
func TestPromotedShareHistoricalStaysLegacyOnMidWaitCommitUnderRR(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true",
			cn.GetServiceConfig().CN.Frontend.Port)
		sqlDB, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer sqlDB.Close()

		writer, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer writer.Close()
		reader, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer reader.Close()

		exec := func(conn *sql.Conn, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoErrorf(t, err, "SQL: %s", statement)
		}

		dbName := strings.ToLower(testutils.GetDatabaseName(t))
		exec(writer, "create database "+dbName)
		defer func() {
			dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer dropCancel()
			_, _ = sqlDB.ExecContext(dropCtx, "drop database if exists "+dbName)
		}()
		exec(writer, "use "+dbName)
		exec(reader, "use "+dbName)
		exec(writer, "create table f(id int primary key, v int)")

		// The forced promotion fixture: 100000 rows over the embedded
		// cluster's MaxLockRowCount budget of 10000 (adjustBasicClusterService).
		const rows = 100000
		exec(writer,
			fmt.Sprintf("insert into f select result, result from generate_series(1,%d) g;", rows))
		const touchedID = 7
		const bumpedValue = int64(1000000000)
		snapshotSum := int64(rows) * int64(rows+1) / 2

		// The explicit historical point: taken AFTER the bulk commit has
		// settled and strictly BEFORE the reader transaction opens, so the
		// locked occurrence engages the historical cohort for that
		// execution (H nonzero, H < executing SnapshotTS).
		time.Sleep(3 * time.Second)
		hPoint := time.Now().UTC().Format("2006-01-02 15:04:05.999999999")
		histTable := fmt.Sprintf("{ mo_ts = '%s' }", hPoint)

		var tableID uint64
		require.NoError(t, writer.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = ? and relname = 'f'",
			dbName).Scan(&tableID))

		// The writer holds an exclusive row lock inside an open transaction.
		exec(writer, "begin")
		writerCommitted := false
		defer func() {
			if !writerCommitted {
				rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer rollbackCancel()
				_, _ = writer.ExecContext(rollbackCtx, "rollback")
			}
		}()
		exec(writer, fmt.Sprintf("update f set v = %d where id = %d", bumpedValue, touchedID))

		// Lock-result synchronization (round 26): the CN's existing
		// lock-result testing hook publishes the resolved flags of every
		// successful lock acquisition on the promoted table — the same
		// flags doLock's wait resolution uses to choose today's terminal
		// legacy conflict against the defensive early-proceed — scoped to
		// the promoted table only and installed after the writer's own row
		// locks, so the first captured result IS the reader's promoted
		// historical acquisition.
		promotedResults := make(chan lockpb.Result, 8)
		testingContext := moruntime.MustGetTestingContext(cn.ServiceID())
		// The runtime's GetBeforeLockFunc returns beforeLockFunc but guards
		// on the adjustLockResultFunc state (pkg/common/runtime/testing.go),
		// so a non-nil adjust hook must be paired with a non-nil before hook
		// or every doLock entry calls a nil function value. Every other hook
		// consumer in the repository pairs the two the same way.
		testingContext.SetBeforeLockFunc(func(_ []byte, _ uint64) {})
		defer testingContext.SetBeforeLockFunc(nil)
		testingContext.SetAdjustLockResultFunc(func(_ []byte, lockedTable uint64, result *lockpb.Result) {
			if lockedTable == tableID {
				select {
				case promotedResults <- *result:
				default:
				}
			}
		})
		defer testingContext.SetAdjustLockResultFunc(nil)

		// The reader opens its RR transaction here: its SnapshotTS is later
		// than H by the sleep margins above, so the mo_ts occurrence below
		// stays historical for this whole execution.
		exec(reader, "set session transaction isolation level repeatable read")
		exec(reader, "begin")

		// The promoted historical LOCK IN SHARE MODE runs in the reader's
		// transaction; its promoted shared table lock waits on the writer's
		// row lock. SUM over an int column returns a MySQL DECIMAL, so the
		// value is scanned as float64: the aggregates below are exact
		// integrals below 2^53 and exactly representable.
		done := make(chan float64, 1)
		readErr := make(chan error, 1)
		go func() {
			var sum sql.NullFloat64
			err := reader.QueryRowContext(ctx,
				fmt.Sprintf("select sum(v) from f %s lock in share mode", histTable)).Scan(&sum)
			if err != nil {
				readErr <- err
				return
			}
			done <- sum.Float64
		}()
		readerDone := false
		defer func() {
			if !readerDone {
				_, _ = reader.ExecContext(context.WithoutCancel(ctx), "rollback")
			}
		}()

		// Observed wait: the promoted historical shared table lock really
		// waits on the writer's row lock before the commit releases it.
		require.Eventually(t, func() bool {
			return issueLockWaiters30955(c, tableID) > 0
		}, 30*time.Second, 10*time.Millisecond,
			"the promoted historical shared table lock never waited on the writer")

		// The writer commits WHILE the promoted historical lock waits: the
		// historical statement's acquisition keeps today's options because
		// no occurrence of the statement current-reads.
		exec(writer, "commit")
		writerCommitted = true

		// The synchronized lock result: what the reader's own promoted
		// historical acquisition resolved to, observed before the
		// statement can report anything.
		var resolved lockpb.Result
		select {
		case resolved = <-promotedResults:
		case <-ctx.Done():
			t.Fatal("the promoted historical acquisition never resolved")
		}

		if resolved.HasConflict && resolved.HasPrevCommit {
			// The mid-wait commit, observed on this execution's lock
			// result: today's terminal legacy conflict is the ONLY
			// admissible outcome. Any silently returned aggregate here —
			// a suppressed conflict restored onto snapshot semantics or a
			// current-read retry — is exactly the regression this member
			// pins, and it fails this execution with no later trial to
			// hide behind.
			select {
			case sum := <-done:
				t.Fatalf("the promoted historical locking read silently returned %v after a wait-resolved conflict (HasConflict=true, HasPrevCommit=true, commit ts %v): the historical exemption was lowered and this acquisition suppressed the observable mid-wait conflict",
					sum, resolved.Timestamp)
			case err := <-readErr:
				require.Error(t, err)
				if strings.Contains(err.Error(), "invalid locking-read retry frontier") {
					t.Fatalf("the marked machinery engaged the historical target and its strict-progress retry fence terminated the promoted historical statement: %v", err)
				}
				require.Contains(t, err.Error(), "SI not support retry",
					"the historical locked statement keeps today's terminal legacy conflict")
				require.Contains(t, err.Error(), "w-w conflict")
			case <-ctx.Done():
				t.Fatal("the promoted historical locking read did not finish")
			}
		} else {
			// The pre-existing stale-flag window of the wait resolution,
			// OBSERVED on this execution's lock result rather than merely
			// claimed (round 26): the acquisition resolved without a
			// confirmed committed conflict, doLock's defensive
			// early-proceed serves the statement, and the ONLY admissible
			// outcome is the pre-commit historical view — exactly the
			// historical aggregate, and no error.
			select {
			case sum := <-done:
				require.Equal(t, float64(snapshotSum), sum,
					"the observed stale-flag window can only complete silently on the pre-commit historical view")
			case err := <-readErr:
				t.Fatalf("the promoted historical locking read failed even though its acquisition resolved without a confirmed committed conflict (lock result: HasConflict=%v, HasPrevCommit=%v): %v",
					resolved.HasConflict, resolved.HasPrevCommit, err)
			case <-ctx.Done():
				t.Fatal("the promoted historical locking read did not finish")
			}
		}

		// A best-effort rollback clears the reader.
		_, _ = reader.ExecContext(context.WithoutCancel(ctx), "rollback")
		readerDone = true

		// The historical view itself stays intact, and the eligible shape
		// keeps the current-read contract, in one fresh RR transaction.
		exec(reader, "begin")
		var histV int64
		require.NoError(t, reader.QueryRowContext(ctx,
			fmt.Sprintf("select v from f %s where id = %d lock in share mode",
				histTable, touchedID)).Scan(&histV))
		require.Equal(t, int64(touchedID), histV,
			"the locked historical read still returns the H view with its row lock")
		var currentV int64
		require.NoError(t, reader.QueryRowContext(ctx,
			"select v from f where id = ? lock in share mode", touchedID).Scan(&currentV))
		require.Equal(t, bumpedValue, currentV,
			"the eligible (no mo_ts) locked read keeps the admitted current-read contract")
		exec(reader, "commit")
		readerDone = true

		// A fresh transaction sees the committed value.
		var fresh int64
		require.NoError(t, sqlDB.QueryRowContext(ctx,
			fmt.Sprintf("select v from `%s`.f where id = ?", dbName), touchedID).Scan(&fresh))
		require.Equal(t, bumpedValue, fresh)
	})
}

// #30955, PR review round 26 (mo-bot P2, head 13a19202): the mixed member
// takes the same oracle repair as its promoted sibling. The bounded
// fresh-database trial loop let a silently completed execution pass behind
// a later legacy-conflict trial; this member now runs the scored mixed
// reproduction ONCE and synchronizes on the HISTORICAL target's resolved
// lock result through the existing lock-result hook of doLock
// (lock_op.go:1276, row path), grading every execution strictly on the
// observed flags: a wait-resolved conflict (HasConflict AND HasPrevCommit
// — the writer committed while the historical row lock waited) MUST
// surface today's terminal legacy conflict, and any other observed flags —
// the pre-existing stale-flag window of the wait resolution (CI observed
// it here: run 41067443301, job 124102560211) — MUST complete silently
// with EXACTLY the pre-commit join view. The graded contract is round 11,
// unchanged: the historical exemption must reach the ROW-LOCK activation
// of a mixed statement. A row-scale LOCK IN SHARE MODE joins a HISTORICAL
// table (its own { mo_ts = H } clause, H strictly earlier than the
// executing SnapshotTS) with an ORDINARY current-read table; the ordinary
// occurrence is admitted and stamps this attempt's frontier, the
// historical occurrence is withheld from this execution's current read,
// and a writer committing while the HISTORICAL target's shared row lock
// waits must surface today's legacy conflict — the exact occurrence whose
// read stays historical — never a value served through the marked
// suppression/retry branches; the eligible (no mo_ts) current-read
// control is retained in a fresh transaction afterwards.
func TestMixedHistoricalShareKeepsLegacyConflictOnHistoricalTargetUnderRR(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?multiStatements=true",
			cn.GetServiceConfig().CN.Frontend.Port)
		sqlDB, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer sqlDB.Close()

		writer, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer writer.Close()
		reader, err := sqlDB.Conn(ctx)
		require.NoError(t, err)
		defer reader.Close()

		exec := func(conn *sql.Conn, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoErrorf(t, err, "SQL: %s", statement)
		}

		dbName := strings.ToLower(testutils.GetDatabaseName(t))
		exec(writer, "create database "+dbName)
		defer func() {
			dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer dropCancel()
			_, _ = sqlDB.ExecContext(dropCtx, "drop database if exists "+dbName)
		}()
		exec(writer, "use "+dbName)
		exec(reader, "use "+dbName)

		// Row scale on purpose: no promotion, so the historical target's
		// own row-lock acquisition is directly observable.
		exec(writer, "create table h(id int primary key, v int)")
		exec(writer, "insert into h values (1, 100), (2, 100)")
		exec(writer, "create table g(id int primary key, v int)")
		exec(writer, "insert into g values (1, 100), (2, 100)")

		const touchedID = 2
		const bumpedValue = int64(999)

		// The explicit historical point: after both tables' commits have
		// settled and strictly before the reader transaction opens, so the
		// historical occurrence ENGAGES the cohort for that execution.
		time.Sleep(3 * time.Second)
		hPoint := time.Now().UTC().Format("2006-01-02 15:04:05.999999999")
		histTable := fmt.Sprintf("{ mo_ts = '%s' }", hPoint)

		var historicalTableID uint64
		require.NoError(t, writer.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase = ? and relname = 'h'",
			dbName).Scan(&historicalTableID))

		// The writer holds an exclusive row lock on the HISTORICAL table's
		// scanned row; the ordinary table's row stays uncontended.
		exec(writer, "begin")
		writerCommitted := false
		defer func() {
			if !writerCommitted {
				rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer rollbackCancel()
				_, _ = writer.ExecContext(rollbackCtx, "rollback")
			}
		}()
		exec(writer, fmt.Sprintf("update h set v = %d where id = %d", bumpedValue, touchedID))

		// Lock-result synchronization (round 26): scoped to the HISTORICAL
		// table and installed after the writer's own row locks, so the
		// first captured result IS the mixed statement's historical row
		// acquisition. The captured flags are the same ones doLock's wait
		// resolution uses to choose today's terminal legacy conflict
		// against the defensive early-proceed, so this member grades the
		// one execution it runs against what that acquisition actually
		// observed — with no repeat trials to hide behind.
		historicalResults := make(chan lockpb.Result, 8)
		testingContext := moruntime.MustGetTestingContext(cn.ServiceID())
		// The runtime's GetBeforeLockFunc returns beforeLockFunc but guards
		// on the adjustLockResultFunc state (pkg/common/runtime/testing.go),
		// so a non-nil adjust hook must be paired with a non-nil before hook
		// or every doLock entry calls a nil function value. Every other hook
		// consumer in the repository pairs the two the same way.
		testingContext.SetBeforeLockFunc(func(_ []byte, _ uint64) {})
		defer testingContext.SetBeforeLockFunc(nil)
		testingContext.SetAdjustLockResultFunc(func(_ []byte, lockedTable uint64, result *lockpb.Result) {
			if lockedTable == historicalTableID {
				select {
				case historicalResults <- *result:
				default:
				}
			}
		})
		defer testingContext.SetAdjustLockResultFunc(nil)

		exec(reader, "set session transaction isolation level repeatable read")
		exec(reader, "begin")

		// The mixed locking read: the historical occurrence of h scans
		// its named snapshot and locks row touchedID; the ordinary
		// occurrence of g current-reads and locks its matched row.
		done := make(chan int64, 1)
		readErr := make(chan error, 1)
		go func() {
			var v sql.NullInt64
			err := reader.QueryRowContext(ctx,
				fmt.Sprintf("select g.v from h %s join g on h.id = g.id where h.id = %d lock in share mode",
					histTable, touchedID)).Scan(&v)
			if err != nil {
				readErr <- err
				return
			}
			done <- v.Int64
		}()
		readerDone := false
		defer func() {
			if !readerDone {
				_, _ = reader.ExecContext(context.WithoutCancel(ctx), "rollback")
			}
		}()

		// Observed wait: the HISTORICAL table's shared row lock really
		// waits on the writer's exclusive row lock before the commit
		// releases it.
		require.Eventually(t, func() bool {
			return issueLockWaiters30955(c, historicalTableID) > 0
		}, 30*time.Second, 10*time.Millisecond,
			"the historical target's shared row lock never waited on the writer")

		// The writer commits WHILE the historical target's lock waits.
		exec(writer, "commit")
		writerCommitted = true

		// The synchronized lock result: what the mixed statement's own
		// historical row acquisition resolved to, observed before the
		// statement can report anything.
		var resolved lockpb.Result
		select {
		case resolved = <-historicalResults:
		case <-ctx.Done():
			t.Fatal("the historical target's lock acquisition never resolved")
		}

		if resolved.HasConflict && resolved.HasPrevCommit {
			// The mid-wait commit, observed on this execution's lock
			// result: today's terminal legacy conflict is the ONLY
			// admissible outcome. A silently returned value here — the
			// historical target re-marked into the retry cohort — is
			// exactly the regression this member pins, and it fails this
			// execution with no later trial to hide behind.
			select {
			case v := <-done:
				t.Fatalf("the mixed statement silently returned %v after a wait-resolved conflict on the historical target (HasConflict=true, HasPrevCommit=true, commit ts %v): the historical target was re-marked into the retry cohort and this acquisition suppressed the observable mid-wait conflict",
					v, resolved.Timestamp)
			case err := <-readErr:
				require.Error(t, err)
				if strings.Contains(err.Error(), "invalid locking-read retry frontier") {
					t.Fatalf("the marked machinery engaged the historical target and its strict-progress retry fence terminated the mixed statement: %v", err)
				}
				require.Contains(t, err.Error(), "SI not support retry",
					"the historical target keeps today's terminal legacy conflict")
				require.Contains(t, err.Error(), "w-w conflict")
			case <-ctx.Done():
				t.Fatal("the mixed historical/current locking read did not finish")
			}
		} else {
			// The pre-existing stale-flag window of the wait resolution,
			// OBSERVED on this execution's lock result rather than merely
			// claimed (round 26): doLock's defensive early-proceed serves
			// the acquisition, and the ONLY admissible outcome is the
			// pre-commit join view (the writer's mid-wait row change stays
			// invisible to the historical occurrence) — exactly that value,
			// and no error.
			select {
			case v := <-done:
				require.Equal(t, int64(100), v,
					"the observed stale-flag window can only complete silently on the pre-commit join view")
			case err := <-readErr:
				t.Fatalf("the mixed historical/current locking read failed even though the historical target's acquisition resolved without a confirmed committed conflict (lock result: HasConflict=%v, HasPrevCommit=%v): %v",
					resolved.HasConflict, resolved.HasPrevCommit, err)
			case <-ctx.Done():
				t.Fatal("the mixed historical/current locking read did not finish")
			}
		}

		// A best-effort rollback clears the reader.
		_, _ = reader.ExecContext(context.WithoutCancel(ctx), "rollback")
		readerDone = true

		// The eligible current-read control retained: in a fresh RR
		// transaction the same row, WITHOUT the mo_ts clause, has NO
		// engaging snapshot and the ordinary locked read current-reads the
		// committed value; the historical view itself stays intact.
		exec(reader, "begin")
		var currentV int64
		require.NoError(t, reader.QueryRowContext(ctx,
			"select v from h where id = ? lock in share mode", touchedID).Scan(&currentV))
		require.Equal(t, bumpedValue, currentV,
			"the eligible (no mo_ts) locked read keeps the admitted current-read contract")
		var histV int64
		require.NoError(t, reader.QueryRowContext(ctx,
			fmt.Sprintf("select v from h %s where id = %d", histTable, touchedID)).Scan(&histV))
		require.Equal(t, int64(100), histV,
			"the historical view at H is preserved")
		exec(reader, "commit")
		readerDone = true

		// A fresh transaction sees the committed value.
		var fresh int64
		require.NoError(t, sqlDB.QueryRowContext(ctx,
			fmt.Sprintf("select v from `%s`.h where id = ?", dbName), touchedID).Scan(&fresh))
		require.Equal(t, bumpedValue, fresh)
	})
}

// issueLockWaiters30955 counts waiting lock requests on the given table
// across every CN lock service, so tests can OBSERVE a real waiter before a
// mid-wait commit.
func issueLockWaiters30955(c embed.Cluster, tableID uint64) int {
	waiters := 0
	c.ForeachServices(func(svc embed.ServiceOperator) bool {
		if svc.ServiceType() != metadata.ServiceType_CN {
			return true
		}
		lockservice.GetLockServiceByServiceID(svc.ServiceID()).IterLocks(
			func(lockedTableID uint64, _ [][]byte, lock lockservice.Lock) bool {
				if lockedTableID == tableID {
					lock.IterWaiters(func(lockpb.WaitTxn) bool {
						waiters++
						return true
					})
				}
				return true
			})
		return true
	})
	return waiters
}
