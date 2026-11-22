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

package embed

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeParallelVisibilityAndFailure(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db := recoverySQLClient(t, cn)
		defer db.Close()
		ctx, cancel := context.WithTimeout(defines.AttachAccountId(t.Context(), 0), time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(q string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, q)
			require.NoError(t, err, q)
		}
		const table = "analyze_worker_test.worker_visibility"
		exec("create database analyze_worker_test")
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 10*time.Second)
			defer done()
			_, err := conn.ExecContext(cleanup, "drop database analyze_worker_test")
			require.NoError(t, err)
		}()
		// Exercise the parallel path even in a one-P test process. Restore
		// both limits (they need not originally match).
		if n, procs := system.GoMaxProcs(), runtime.GOMAXPROCS(0); min(n, procs) < 2 {
			system.SetGoMaxProcs(2)
			defer func() { system.SetGoMaxProcs(n); runtime.GOMAXPROCS(procs) }()
		}
		// Use the minimum 32 regular persisted blocks required for two workers.
		svc := cn.RawService().(cnservice.Service)
		exec("create table " + table + "(id int primary key, v int)")
		exec("insert into " + table + " select result, if(result=262144,99,result%4) from generate_series(1,262144) g")
		exec("select mo_ctl('dn','flush','" + table + "')")
		exec("delete from " + table + " where id=262144")
		exec("select mo_ctl('dn','flush','" + table + "')")
		exec("insert into " + table + " values (262145,77),(262146,null)")
		stats := func() [4]int {
			t.Helper()
			var v [4]int
			err := conn.QueryRowContext(ctx, "select table_cnt, cast(json_extract(ndv_map,'$.v') as signed), cast(json_extract(null_cnt_map,'$.v') as signed), cast(json_extract(max_val_map,'$.v') as signed) from table_stats('"+table+"','get','normal') g").Scan(&v[0], &v[1], &v[2], &v[3])
			require.NoError(t, err)
			return v
		}
		const analyze = "analyze table " + table + "(v)"
		exec("set session max_dop=1")
		exec(analyze)
		want := [4]int{262145, 5, 1, 77}
		require.Equal(t, want, stats())
		exec("set session max_dop=2")
		exec(analyze)
		require.Equal(t, want, stats())

		// Give the previous publication a distinguishable marker. Only a
		// real parallel worker can trigger this failure after opening a reader.
		exec(`select table_cnt from table_stats('` + table + `','patch','{"ndv_map":{"v":3}}') g`)
		prior := stats()
		require.Equal(t, 3, prior[1])
		func() {
			if fault.Enable() {
				defer fault.Disable()
			}
			const point = "analyze_parallel_read_error"
			require.NoError(t, fault.AddFaultPoint(ctx, point, ":::", "echo", 0, "worker_visibility", false))
			defer fault.RemoveFaultPoint(ctx, point)
			_, err = conn.ExecContext(ctx, analyze)
			require.ErrorContains(t, err, "injected parallel ANALYZE reader failure")
			// Exercise the same error against an isolated statement/CN ledger,
			// this time with row sampling and more than one column pass.
			b := process.MustNewExecutionResourceBudget(32<<20, 32<<20)
			g, err := b.OpenGeneration(1)
			require.NoError(t, err)
			stmt := testutil.NewProcess(t)
			require.NoError(t, stmt.SetExecutionResourceBudgetForTesting(g))
			stmt.SetResolveVariableFunc(func(string, bool, bool) (any, error) { return int64(2), nil })
			err = svc.GetSQLExecutor().ExecTxn(ctx, func(tx executor.TxnExecutor) error {
				db, err := svc.GetEngine().Database(ctx, "analyze_worker_test", tx.Txn())
				if err != nil {
					return err
				}
				rel, err := db.Relation(ctx, "worker_visibility", nil)
				if err != nil {
					return err
				}
				tombstones, err := rel.CollectTombstones(ctx, 0, engine.Policy_CollectAllTombstones)
				if err != nil {
					return err
				}
				require.True(t, tombstones.HasAnyTombstoneFile(), "exercise persisted delete visibility")
				_, err = rel.(engine.AnalyzableRelation).AnalyzeTable(ctx, engine.AnalyzeTableRequest{
					ResourceProcess: stmt, Columns: []string{"v", "id"}, TargetRows: 1000,
				})
				return err
			}, executor.Options{})
			require.ErrorContains(t, err, "injected parallel ANALYZE reader failure")
			require.Greater(t, g.Peak(), uint64(12<<20))
			require.Zero(t, g.Used())
			require.Zero(t, b.AggregateUsed())
		}()
		require.Equal(t, prior, stats())
		exec(analyze)
		require.Equal(t, want, stats())
	})
}

func TestPreparedAnalyzeOverMySQLProtocol(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		exec := func(statement string) {
			_, execErr := conn.ExecContext(ctx, statement)
			require.NoError(t, execErr, statement)
		}
		prepare := func(statement string) *sql.Stmt {
			stmt, prepareErr := conn.PrepareContext(ctx, statement)
			require.NoError(t, prepareErr, statement)
			return stmt
		}
		type expectedAnalyzeRow struct {
			tableName       string
			columnsAnalyzed uint64
			populationRows  uint64
		}
		assertResultSet := func(rows *sql.Rows, expectedRows []expectedAnalyzeRow) {
			columns, columnsErr := rows.Columns()
			require.NoError(t, columnsErr)
			require.Equal(t, []string{
				"table_name", "mode", "coverage", "columns_analyzed", "population_rows",
				"population_exact", "sample_rows", "sample_blocks", "sample_bytes", "status", "message",
			}, columns)
			for _, expected := range expectedRows {
				var (
					tableName       string
					mode            string
					coverage        string
					columnsAnalyzed uint64
					populationRows  uint64
					populationExact bool
					sampleRows      uint64
					sampleBlocks    uint64
					sampleBytes     uint64
					status          string
					message         string
				)
				require.True(t, rows.Next())
				require.NoError(t, rows.Scan(
					&tableName, &mode, &coverage, &columnsAnalyzed, &populationRows,
					&populationExact, &sampleRows, &sampleBlocks, &sampleBytes, &status, &message))
				require.Equal(t, expected.tableName, tableName)
				require.Equal(t, "AUTO", mode)
				require.Equal(t, "SNAPSHOT_VISIBLE_V1", coverage)
				require.Equal(t, expected.columnsAnalyzed, columnsAnalyzed)
				require.Equal(t, expected.populationRows, populationRows)
				require.True(t, populationExact)
				require.Equal(t, populationRows, sampleRows)
				require.NotZero(t, sampleBlocks)
				require.NotZero(t, sampleBytes)
				require.Equal(t, "OK", status)
				require.Equal(t, "q=1/1", message)
			}
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
		}
		querySingle := func(stmt *sql.Stmt, expectedRows ...expectedAnalyzeRow) {
			rows, queryErr := stmt.QueryContext(ctx)
			require.NoError(t, queryErr)
			defer func() {
				require.NoError(t, rows.Close())
			}()
			assertResultSet(rows, expectedRows)
			require.False(t, rows.NextResultSet())
			require.NoError(t, rows.Err())
		}
		queryText := func(name string, expectedRows ...expectedAnalyzeRow) {
			rows, queryErr := conn.QueryContext(ctx, "execute "+name)
			require.NoError(t, queryErr)
			defer func() {
				require.NoError(t, rows.Close())
			}()
			assertResultSet(rows, expectedRows)
			require.False(t, rows.NextResultSet())
			require.NoError(t, rows.Err())
		}
		queryError := func(stmt *sql.Stmt, expected string) {
			rows, queryErr := stmt.QueryContext(ctx)
			if rows != nil {
				defer func() {
					require.NoError(t, rows.Close())
				}()
				require.NoError(t, rows.Err())
			}
			require.ErrorContains(t, queryErr, expected)
		}
		queryTextError := func(name string, expected string) {
			rows, queryErr := conn.QueryContext(ctx, "execute "+name)
			if rows != nil {
				defer func() {
					require.NoError(t, rows.Close())
				}()
				require.NoError(t, rows.Err())
			}
			require.ErrorContains(t, queryErr, expected)
		}
		assertCurrentDatabase := func(expected string) {
			var currentDatabase string
			require.NoError(t, conn.QueryRowContext(ctx, "select database()").Scan(&currentDatabase))
			require.Equal(t, expected, currentDatabase)
		}

		exec("drop database if exists prepared_analyze_test")
		exec("drop database if exists prepared_analyze_other")
		exec("create database prepared_analyze_test")
		exec("create database prepared_analyze_other")
		defer conn.ExecContext(context.Background(), "drop database if exists prepared_analyze_test")
		defer conn.ExecContext(context.Background(), "drop database if exists prepared_analyze_other")
		exec("use prepared_analyze_test")
		exec("create table t (a int, b varchar(20))")
		exec("insert into t values (1, 'x'), (1, 'y'), (2, 'y')")

		explicit := prepare("analyze table t(a, b)")
		defer explicit.Close()
		for range 2 {
			querySingle(explicit, expectedAnalyzeRow{
				tableName: "prepared_analyze_test.t", columnsAnalyzed: 2, populationRows: 3})
		}

		implicit := prepare("analyze table t")
		defer implicit.Close()
		querySingle(implicit, expectedAnalyzeRow{
			tableName: "prepared_analyze_test.t", columnsAnalyzed: 2, populationRows: 3})

		exec("create table t_aux (a int, b varchar(20))")
		exec("insert into t_aux values (4, 'u'), (5, 'v')")
		multi := prepare("analyze table t(a), t_aux(b, a)")
		defer multi.Close()
		querySingle(multi,
			expectedAnalyzeRow{tableName: "prepared_analyze_test.t", columnsAnalyzed: 1, populationRows: 3},
			expectedAnalyzeRow{tableName: "prepared_analyze_test.t_aux", columnsAnalyzed: 2, populationRows: 2})

		exec("prepare analyze_stmt_form from analyze table t(a), t_aux(b, a)")
		defer exec("deallocate prepare analyze_stmt_form")
		exec("prepare analyze_string_form from 'analyze table t(a), t_aux(b, a)'")
		defer exec("deallocate prepare analyze_string_form")

		exec("use prepared_analyze_other")
		exec("create table t (a int, b varchar(20))")
		exec("insert into t values (9, 'other'), (9, 'other')")
		exec("create table t_aux (a int, b varchar(20))")
		exec("insert into t_aux values (9, 'other')")
		expectedTextRows := []expectedAnalyzeRow{
			{tableName: "prepared_analyze_test.t", columnsAnalyzed: 1, populationRows: 3},
			{tableName: "prepared_analyze_test.t_aux", columnsAnalyzed: 2, populationRows: 2},
		}
		queryText("analyze_stmt_form", expectedTextRows...)
		assertCurrentDatabase("prepared_analyze_other")
		queryText("analyze_string_form", expectedTextRows...)
		assertCurrentDatabase("prepared_analyze_other")
		querySingle(explicit, expectedAnalyzeRow{
			tableName: "prepared_analyze_test.t", columnsAnalyzed: 2, populationRows: 3})
		assertCurrentDatabase("prepared_analyze_other")
		exec("use prepared_analyze_test")

		exec("create table snapshot_text (old_col int)")
		exec("insert into snapshot_text values (1), (2)")
		exec("create snapshot prepared_analyze_snapshot for account")
		defer exec("drop snapshot prepared_analyze_snapshot")
		exec("prepare analyze_snapshot_stmt from analyze table snapshot_text {snapshot = 'prepared_analyze_snapshot'}")
		defer exec("deallocate prepare analyze_snapshot_stmt")
		exec("prepare analyze_snapshot_string from 'analyze table snapshot_text {snapshot = ''prepared_analyze_snapshot''}'")
		defer exec("deallocate prepare analyze_snapshot_string")
		exec("alter table snapshot_text add column current_only int")
		exec("use prepared_analyze_other")
		queryTextError("analyze_snapshot_stmt", "does not support historical snapshots")
		assertCurrentDatabase("prepared_analyze_other")
		queryTextError("analyze_snapshot_string", "does not support historical snapshots")
		assertCurrentDatabase("prepared_analyze_other")
		exec("use prepared_analyze_test")

		exec("create table drift (x int)")
		exec("insert into drift values (1), (2)")
		drift := prepare("analyze table drift")
		defer drift.Close()
		exec("alter table drift add column y int")
		exec("update drift set y = x")
		querySingle(drift, expectedAnalyzeRow{
			tableName: "prepared_analyze_test.drift", columnsAnalyzed: 2, populationRows: 2})
		exec("drop table drift")
		exec("create table drift (x int)")
		exec("insert into drift values (7), (7)")
		querySingle(drift, expectedAnalyzeRow{
			tableName: "prepared_analyze_test.drift", columnsAnalyzed: 1, populationRows: 2})

		exec("begin")
		exec("insert into t values (3, 'z')")
		queryError(explicit, "cannot run inside an active user transaction")
		exec("rollback")

		exec("create table later_missing (v int)")
		exec("insert into later_missing values (1)")
		failsAfterPrepare := prepare("analyze table t(a), later_missing(v)")
		defer failsAfterPrepare.Close()
		exec("drop table later_missing")
		func() {
			failedRows, queryErr := failsAfterPrepare.QueryContext(ctx)
			if failedRows != nil {
				defer func() {
					require.NoError(t, failedRows.Close())
				}()
				require.NoError(t, failedRows.Err())
			}
			require.ErrorContains(t, queryErr, "no such table prepared_analyze_test.later_missing")
		}()
		var stillUsable int
		require.NoError(t, conn.QueryRowContext(ctx, "select 7").Scan(&stillUsable))
		require.Equal(t, 7, stillUsable)

		exec("drop user if exists prepared_analyze_user")
		exec("drop role if exists prepared_analyze_role")
		exec("create role prepared_analyze_role")
		exec("grant connect on account * to prepared_analyze_role")
		exec("grant select on table prepared_analyze_test.t to prepared_analyze_role")
		exec("create user prepared_analyze_user identified by '111' default role prepared_analyze_role")
		defer conn.ExecContext(context.Background(), "drop user if exists prepared_analyze_user")
		defer conn.ExecContext(context.Background(), "drop role if exists prepared_analyze_role")

		userDSN := fmt.Sprintf("prepared_analyze_user:111@tcp(127.0.0.1:%d)/prepared_analyze_test",
			cn.GetServiceConfig().CN.Frontend.Port)
		userDB, err := sql.Open("mysql", userDSN)
		require.NoError(t, err)
		defer userDB.Close()
		userConn, err := userDB.Conn(ctx)
		require.NoError(t, err)
		defer userConn.Close()
		userStmt, err := userConn.PrepareContext(ctx, "analyze table t(a)")
		require.NoError(t, err)
		defer userStmt.Close()
		exec("revoke select on table prepared_analyze_test.t from prepared_analyze_role")
		func() {
			unauthorizedRows, queryErr := userStmt.QueryContext(ctx)
			if unauthorizedRows != nil {
				defer func() {
					require.NoError(t, unauthorizedRows.Close())
				}()
				require.NoError(t, unauthorizedRows.Err())
			}
			require.Error(t, queryErr)
		}()
	})
}
