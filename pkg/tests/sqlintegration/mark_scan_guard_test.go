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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestMarkScanGuardSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) { runMarkScanGuardSQL(t, cluster) })
}

func runMarkScanGuardSQL(t *testing.T, cluster embed.Cluster) {
	t.Helper()
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(3)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	schema := strings.ToLower(testutils.GetDatabaseName(t))
	exec := func(q string) { t.Helper(); _, e := db.ExecContext(ctx, q); require.NoError(t, e, q) }
	exec("create database " + schema)
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		_, e := db.ExecContext(cleanup, "drop database if exists "+schema)
		if e != nil {
			t.Errorf("drop test database: %v", e)
		}
	}()
	executions := schema + ".executions"
	cases := schema + ".cases"
	exec("create table " + executions + " (id varchar(64) not null primary key, workspace_id varchar(64) not null, workflow_id varchar(64) not null, status int not null, case_id varchar(64) not null, payload longtext, index idx_workspace (workspace_id,workflow_id), index idx_case (case_id))")
	exec("create table " + cases + " (id varchar(36) not null primary key, workspace_id varchar(64) not null, execution_id varchar(64) not null)")
	exec("insert into " + executions + " select concat('e',cast(result as varchar(64))), 'w', 'wf', 1, case when result=10 then 'c1' else concat('other',cast(result as varchar(64))) end, repeat('x',256) from generate_series(1,16000) g")
	exec("insert into " + cases + " values ('c1','w','e10')")
	exec("select mo_ctl('dn','flush','" + executions + "')")
	clock, _ := cn.RawService().(cnservice.Service).GetClock().Now()
	_, err = cn.RawService().(cnservice.Service).GetTxnClient().WaitLogTailAppliedAt(ctx, clock)
	require.NoError(t, err)
	predicate := " from " + executions + " where workspace_id='w' and (id in (select execution_id from " + cases + " where workspace_id='w' and id='c1' and execution_id <> '') or case_id='c1') for update"
	checkPredicate := func(projection, where string, want ...string) {
		t.Helper()
		rows, e := db.QueryContext(ctx, "select "+projection+where)
		require.NoError(t, e)
		defer rows.Close()
		cols, e := rows.Columns()
		require.NoError(t, e)
		values := make([]sql.RawBytes, len(cols))
		args := make([]any, len(cols))
		for i := range values {
			args[i] = &values[i]
		}
		var got []string
		for rows.Next() {
			require.NoError(t, rows.Scan(args...))
			got = append(got, string(values[0]))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.ElementsMatch(t, want, got)
	}
	check := func(projection string, want ...string) {
		t.Helper()
		checkPredicate(projection, predicate, want...)
	}
	check("*", "e10")
	// Reusing a prepared statement with different arguments must not reuse an
	// optional filter derived for a previous execution.
	preparedSQL := "select id from " + executions + " where workspace_id=? and (id in (select execution_id from " + cases + " where workspace_id=? and id=? and execution_id <> '') or case_id=?) for update"
	stmt, err := db.PrepareContext(ctx, preparedSQL)
	require.NoError(t, err)
	defer stmt.Close()
	for _, tc := range []struct {
		caseID string
		want   []string
	}{
		{"c1", []string{"e10"}},
		{"missing", nil},
		{"c1", []string{"e10"}},
	} {
		rows, e := stmt.QueryContext(ctx, "w", "w", tc.caseID, tc.caseID)
		require.NoError(t, e)
		defer rows.Close()
		var got []string
		for rows.Next() {
			var id string
			require.NoError(t, rows.Scan(&id))
			got = append(got, id)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.ElementsMatch(t, tc.want, got)
	}
	require.NoError(t, stmt.Close())

	// A constant alternative and a parameter only in the build side can
	// produce an eligible wide plan. Re-execute on the same connection as the
	// subquery alternates between a hit, an empty set, and a different hit.
	exec("insert into " + cases + " values ('c2','w','e20')")
	preparedConn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer preparedConn.Close()
	widePreparedSQL := "select id, payload from " + executions + " where workspace_id='w' and (id in (select execution_id from " + cases + " where workspace_id='w' and id=? and execution_id <> '') or case_id='c1') for update"
	wideStmt, err := preparedConn.PrepareContext(ctx, widePreparedSQL)
	require.NoError(t, err)
	defer wideStmt.Close()
	for _, tc := range []struct {
		caseID string
		want   []string
	}{
		{"c1", []string{"e10"}},
		{"missing", []string{"e10"}},
		{"c2", []string{"e10", "e20"}},
		{"c1", []string{"e10"}},
	} {
		rows, e := wideStmt.QueryContext(ctx, tc.caseID)
		require.NoError(t, e)
		defer rows.Close()
		var got []string
		for rows.Next() {
			var id, payload string
			require.NoError(t, rows.Scan(&id, &payload))
			got = append(got, id)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.ElementsMatch(t, tc.want, got)
	}
	require.NoError(t, wideStmt.Close())
	require.NoError(t, preparedConn.Close())
	exec("delete from " + cases + " where id='c2'")
	// The final filter must receive only the matched row, proving the guard was
	// actually installed; checking the result alone would also pass on a full scan.
	explained, err := testutils.QueryTextResult(ctx, db, "explain analyze select *"+predicate)
	require.NoError(t, err)
	require.Contains(t, explained.Text, "Join Type: MARK")
	filterRows := regexp.MustCompile(`(?m)->\s+Filter\s*\n\s*Analyze:[^\n]*inputRows=(\d+)`).FindStringSubmatch(explained.Text)
	require.Len(t, filterRows, 2, explained.Text)
	require.Equal(t, "1", filterRows[1], explained.Text)
	// With a narrow projection the scan's raw column positions differ from the
	// MARK output positions. An empty IN input must still preserve the other arm.
	exec("delete from " + cases)
	check("id", "e10")
	// This projection keeps a wide value but remaps raw scan column positions.
	check("id, payload", "e10")
	remapped, err := testutils.QueryTextResult(ctx, db, "explain analyze select id, payload"+predicate)
	require.NoError(t, err)
	remappedRows := regexp.MustCompile(`(?m)->\s+Filter\s*\n\s*Analyze:[^\n]*inputRows=(\d+)`).FindStringSubmatch(remapped.Text)
	require.Len(t, remappedRows, 2, remapped.Text)
	require.Equal(t, "1", remappedRows[1], remapped.Text)
	// The row selected only by the OR alternative must still be locked. A
	// third connection observes the actual waiter rather than a client timeout.
	func() {
		locker, err := db.Conn(ctx)
		require.NoError(t, err)
		defer locker.Close()
		contender, err := db.Conn(ctx)
		require.NoError(t, err)
		defer contender.Close()
		observer, err := db.Conn(ctx)
		require.NoError(t, err)
		defer observer.Close()
		var tableID string
		err = observer.QueryRowContext(ctx, `select cast(rel_id as char) from mo_catalog.mo_tables
		where account_id=0 and reldatabase=? and relname='executions'`, schema).Scan(&tableID)
		require.NoError(t, err)
		tx, err := locker.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		locked, err := tx.QueryContext(ctx, "select id, payload"+predicate)
		require.NoError(t, err)
		defer locked.Close()
		var lockedIDs []string
		for locked.Next() {
			var id, payload string
			require.NoError(t, locked.Scan(&id, &payload))
			lockedIDs = append(lockedIDs, id)
		}
		require.NoError(t, locked.Err())
		require.NoError(t, locked.Close())
		require.Equal(t, []string{"e10"}, lockedIDs)
		type updateOutcome struct {
			result sql.Result
			err    error
		}
		waitCtx, stop := context.WithTimeout(ctx, 30*time.Second)
		updateDone := make(chan updateOutcome, 1)
		go func() {
			result, updateErr := contender.ExecContext(waitCtx, "update "+executions+" set status=status+1 where id='e10'")
			updateDone <- updateOutcome{result: result, err: updateErr}
		}()
		drained := false
		defer func() {
			stop()
			_ = tx.Rollback()
			if !drained {
				select {
				case <-updateDone:
				case <-time.After(10 * time.Second):
					t.Error("competing update did not exit after cancellation and rollback")
				}
			}
		}()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		var waiters int
		for waiters == 0 {
			select {
			case outcome := <-updateDone:
				drained = true
				t.Fatalf("competing update completed before lock waiter was observed: %v", outcome.err)
			case <-waitCtx.Done():
				t.Fatalf("lock waiter was not observed: %v", waitCtx.Err())
			case <-ticker.C:
				err = observer.QueryRowContext(waitCtx, `select count(*) from mo_catalog.mo_locks
				where table_id=? and txn_id<>'' and lock_wait<>''`, tableID).Scan(&waiters)
				require.NoError(t, err)
			}
		}
		require.NoError(t, tx.Rollback())
		select {
		case outcome := <-updateDone:
			drained = true
			require.NoError(t, outcome.err)
			affected, updateErr := outcome.result.RowsAffected()
			require.NoError(t, updateErr)
			require.Equal(t, int64(1), affected)
		case <-waitCtx.Done():
			t.Fatalf("competing update did not complete after rollback: %v", waitCtx.Err())
		}
	}()
	exec("insert into " + cases + " values ('c1','w','e20')")
	check("id", "e10", "e20")
	check("id, payload", "e10", "e20")
	exec("update " + executions + " set case_id='other10' where id='e10'")
	check("id", "e20")
	exec("delete from " + cases)
	check("id")
	exec("update " + executions + " set case_id='c1' where id='e10'")
	exec("drop table " + cases)
	exec("create table " + cases + " (id varchar(36), workspace_id varchar(64), execution_id varchar(64))")
	exec("insert into " + cases + " values ('c1','w',null),('c1','w',''),('c1','other','e20')")
	check("id, payload", "e10")
	// Retain NULL in the subquery so MARK must preserve SQL three-valued
	// semantics; the issue predicate above intentionally removes NULL first.
	nullPredicate := strings.Replace(predicate, " and execution_id <> ''", "", 1)
	exec("delete from " + cases)
	exec("insert into " + cases + " values ('c1','w',null)")
	checkPredicate("id, payload", nullPredicate, "e10")
	exec("update " + executions + " set case_id='other10' where id='e10'")
	checkPredicate("id, payload", nullPredicate)
	exec("insert into " + cases + " values ('c1','w','e20')")
	checkPredicate("id, payload", nullPredicate, "e20")
	exec("update " + executions + " set case_id='c1' where id='e10'")
	checkPredicate("id, payload", nullPredicate, "e10", "e20")
	exec("delete from " + cases)
	exec("insert into " + cases + " values ('c1','w','e10'),('c1','w','e20'),('c1','w','e20')")
	check("id, payload", "e10", "e20")
	exec("delete from " + cases)
	exec("insert into " + cases + " select 'c1','w',concat('e',cast(result as varchar(64))) from generate_series(1,200) g")
	want := make([]string, 200)
	for i := range want {
		want[i] = fmt.Sprintf("e%d", i+1)
	}
	check("id, payload", want...)
	// Compare guarded IN against the engine's ordinary equality semantics for
	// case, trailing spaces, UTF-8 and embedded NUL, instead of assuming a
	// particular collation implementation.
	exec("delete from " + cases)
	exec("update " + executions + " set case_id='other10' where id='e10'")
	_, err = db.ExecContext(ctx, "insert into "+executions+" values (?, 'w', 'wf', 1, 'other-accent', 'payload')", "é")
	require.NoError(t, err)
	for _, key := range []string{"e10", "E10", "e10 ", "é", "e10\x00"} {
		exec("delete from " + cases)
		_, err = db.ExecContext(ctx, "insert into "+cases+" values ('c1','w',?)", key)
		require.NoError(t, err)
		rows, e := db.QueryContext(ctx, "select id from "+executions+" where workspace_id='w' and id=?", key)
		require.NoError(t, e)
		defer rows.Close()
		var expected []string
		for rows.Next() {
			var id string
			require.NoError(t, rows.Scan(&id))
			expected = append(expected, id)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		check("id, payload", expected...)
	}
}
