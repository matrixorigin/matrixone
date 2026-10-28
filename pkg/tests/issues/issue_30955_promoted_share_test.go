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

// #30955, pre-PR review round 10, finding 1: promoted lock options must
// derive from EFFECTIVE per-execution occurrence eligibility. A locked
// occurrence with an ENGAGING explicit snapshot (H nonzero and strictly
// earlier than the executing SnapshotTS — mo_ts) is withheld from this
// execution's current read entirely (D-1/R8-F3: it reads its named snapshot
// exactly as today with its lock target conserved), yet its plan target
// still carries bind-time presence, so the promoted Shared table lock took
// the MARKED pre-pipeline options even though NO occurrence current-reads:
// a writer committing while that promoted historical LOCK IN SHARE MODE
// waits was silently served (the marked acquisition suppresses the
// mid-wait committed conflict) instead of surfacing today's terminal
// "SI not support retry" legacy conflict — D-1's historical exemption
// broken exactly at the promoted acquisition. This member pins the
// deterministic promotion-plus-historical reproduction the review demanded:
// 100000 rows over the embedded 10000 MaxLockRowCount budget force the
// table-lock promotion, the literal mo_ts point sits after the bulk commit
// and strictly before the reader transaction's snapshot, the wait is
// observed on the real lock service, the commit lands mid-wait, and the
// declined historical statement keeps today's legacy conflict while the
// historical view itself stays intact and the eligible current-read shape
// (the same row, no mo_ts, a fresh transaction) keeps the round-8
// current-read contract.
func TestPromotedShareHistoricalStaysLegacyOnMidWaitCommitUnderRR(t *testing.T) {
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
		const touchedID = 7
		const bumpedValue = int64(1000000000)

		// The explicit historical point: taken AFTER the bulk commit has
		// settled (the { mo_ts } entry validates no catalog snapshot and
		// the probe receipt probe-mo-ts-fresh-read pins fresh-data
		// readability) and strictly BEFORE the reader transaction opens,
		// so the locked occurrence engages the historical cohort for that
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

		// The reader opens its RR transaction here: its SnapshotTS is later
		// than H by the sleep margins above, so the mo_ts occurrence below
		// stays historical for this whole execution.
		exec(reader, "set session transaction isolation level repeatable read")
		exec(reader, "begin")

		// The promoted historical LOCK IN SHARE MODE runs in the reader's
		// transaction; its promoted shared table lock waits on the writer's
		// row lock.
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
		// historical statement must surface today's terminal legacy
		// conflict — its acquisition keeps today's options because no
		// occurrence of the statement current-reads.
		exec(writer, "commit")
		writerCommitted = true

		select {
		case sum := <-done:
			t.Fatalf("the historical promoted shared lock took the marked options and silently suppressed the mid-wait commit (returned %v)", sum)
		case err := <-readErr:
			require.Error(t, err)
			require.Contains(t, err.Error(), "SI not support retry",
				"the historical locked statement keeps today's terminal legacy conflict")
			require.Contains(t, err.Error(), "w-w conflict")
		case <-ctx.Done():
			t.Fatal("the promoted historical locking read did not finish")
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

// #30955, pre-PR review round 11: the historical exemption must reach the
// ROW-LOCK activation of a MIXED statement. The scored shape: a row-scale
// LOCK IN SHARE MODE joining a HISTORICAL table (its own { mo_ts = H }
// clause, H strictly earlier than the executing SnapshotTS) with an
// ORDINARY current-read table. The ordinary occurrence is admitted and
// stamps this attempt's frontier; the historical occurrence is withheld
// from this execution's current read (compile's admission refuses the
// engaging H<S scan) — but its plan target still carries bind-time
// presence, and the stamped frontier activated that target's marked
// revalidation, so a writer committing while the HISTORICAL target's
// shared row lock waits was served through the marked suppression/retry
// branches instead of surfacing today's terminal "SI not support retry"
// legacy conflict — the exact occurrence whose read stays historical. This
// member pins the deterministic mixed reproduction the review demanded:
// row scale (no promotion), the writer's row inside the HISTORICAL
// occurrence's scan only, an observed wait on the historical table, a
// mid-wait commit, the historical target's LEGACY conflict required, and
// the eligible (no mo_ts) current-read control retained in a fresh
// transaction afterwards.
func TestMixedHistoricalShareKeepsLegacyConflictOnHistoricalTargetUnderRR(t *testing.T) {
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

		// Row scale on purpose: no promotion, so the historical target's
		// own row-lock acquisition is directly observable.
		exec(writer, "create table h(id int primary key, v int)")
		exec(writer, "insert into h values (1, 100), (2, 100)")
		exec(writer, "create table g(id int primary key, v int)")
		exec(writer, "insert into g values (1, 100), (2, 100)")

		const touchedID = 2
		const bumpedValue = int64(999)

		// The explicit historical point: after both tables' commits have
		// settled and strictly before the reader transaction opens (the
		// round-10 probe receipt grounds fresh-data readability at a mo_ts
		// point seconds after its commit), so the historical occurrence
		// ENGAGES the cohort for that execution.
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

		exec(reader, "set session transaction isolation level repeatable read")
		exec(reader, "begin")

		// The mixed locking read: the historical occurrence of h scans its
		// named snapshot and locks row touchedID; the ordinary occurrence of
		// g current-reads and locks its matched row.
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

		// Observed wait: the HISTORICAL table's shared row lock really waits
		// on the writer's exclusive row lock before the commit releases it.
		require.Eventually(t, func() bool {
			return issueLockWaiters30955(c, historicalTableID) > 0
		}, 30*time.Second, 10*time.Millisecond,
			"the historical target's shared row lock never waited on the writer")

		// The writer commits WHILE the historical target's lock waits.
		exec(writer, "commit")
		writerCommitted = true

		select {
		case v := <-done:
			t.Fatalf("the historical target inherited the marked revalidation and the mixed statement silently returned %v", v)
		case err := <-readErr:
			require.Error(t, err)
			require.Contains(t, err.Error(), "SI not support retry",
				"the historical target keeps today's terminal legacy conflict")
			require.Contains(t, err.Error(), "w-w conflict")
		case <-ctx.Done():
			t.Fatal("the mixed historical/current locking read did not finish")
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
